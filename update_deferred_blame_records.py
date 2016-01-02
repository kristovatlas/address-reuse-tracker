"""Resolve deferred blame records to blame actual Bitcoin clients and services.

This information was originally deferred because the information is only
accessible via remote API, and slower to fetch.
"""

import os #get name of this script for using os.path.basename
import traceback
import sys

import address_reuse.db
import address_reuse.config
import address_reuse.benchmark.block_reader_benchmark
import address_reuse.blockchain_reader
import address_reuse.block_processor
import address_reuse.logger

ENABLE_MULTITHREADING = True

#Set to None to disable. If set, will not process blocks above this height.
HIGHEST_BLOCK_HEIGHT_TO_PROCESS = 282706

NUM_CONSECUTIVE_BLOCKS_TO_CLAIM = 500

def main():
    """Main function."""
    my_pid = os.getpid()

    #Determine max number of blocks to process. -1 blocks = infinity
    config = address_reuse.config.Config(
        blockchain_mode=address_reuse.config.BlockchainMode.BITCOIND_RPC)
    blocks_remaining = config.MAX_NUM_BLOCKS_TO_PROCESS_PER_RUN

    db = address_reuse.db.Database(
        blockchain_mode=address_reuse.config.BlockchainMode.BITCOIND_RPC)

    coord_db = None
    if ENABLE_MULTITHREADING:
        coord_db = address_reuse.db.BlameResolverCoordinationDatabase()
        dead_blocks = coord_db.get_list_of_block_heights_with_possibly_crashed_workers()
        if len(dead_blocks) > 0:
            print ("Error: The following blocks were claimed by workers that "
                   "appear to have crashed, and must be manually reclaimed. "
                   "This can be done with the `repair_unclaim_dead_blocks_for_"
                   "deferred_records.py` script.")
            for block in dead_blocks:
                print "\t%d" % int(block)
            sys.exit()

    current_height_iterated = 0
    deque_of_claimed_blocks = None
    if ENABLE_MULTITHREADING:
        lowest_height_with_def_records = db.get_lowest_block_height_with_deferred_records()
        deque_of_claimed_blocks = coord_db.get_list_of_next_block_heights_available(
            starting_height=lowest_height_with_def_records,
            num_to_claim=NUM_CONSECUTIVE_BLOCKS_TO_CLAIM)
        try:
            current_height_iterated = deque_of_claimed_blocks.popleft()
        except IndexError:
            current_height_iterated = None
    else:
        current_height_iterated = db.get_lowest_block_height_with_deferred_records()
    if current_height_iterated is None:
        print("No deferred records found in database. TODO: Implement data "
              "subscription.")
        sys.exit()

    if HIGHEST_BLOCK_HEIGHT_TO_PROCESS is None:
        #process until we hit the current blockchain height
        api_reader = address_reuse.blockchain_reader.ThrottledBlockchainReader(db)
        max_blockchain_height = int(api_reader.get_current_blockchain_block_height())
        api_reader = None #Done with API lookups :>
    else:
        max_blockchain_height = HIGHEST_BLOCK_HEIGHT_TO_PROCESS

    blockchain_reader = address_reuse.blockchain_reader.LocalBlockchainRPCReader(db)

    benchmarker = address_reuse.benchmark.block_reader_benchmark.Benchmark()
    last_completed_height = None
    try:
        while (current_height_iterated < max_blockchain_height and
               blocks_remaining):
            print(("DEBUG: update_deferred_blame_records.py: max block height to "
                   "process is %d, last block processed in db is %d, %d "
                   "remaining blocks to process in this run.") %
                  (max_blockchain_height, current_height_iterated,
                   blocks_remaining))

            block_processor = address_reuse.block_processor.BlockProcessor(
                blockchain_reader, db)
            block_processor.process_block_after_deferred_blaming(
                current_height_iterated, benchmarker)

            #Log successful processing of this block
            address_reuse.logger.log_status(('Processed deferred blame for block '
                                             '%d. (PID %d)') %
                                            (current_height_iterated, my_pid))

            if ENABLE_MULTITHREADING:
                #record completion of this block height
                coord_db.mark_block_complete(current_height_iterated)

            print("Completed processing of block at height %d. (PID %d)" %
                  (current_height_iterated, my_pid))

            #Get next block height to process
            if ENABLE_MULTITHREADING:
                last_completed_height = current_height_iterated
                try:
                    current_height_iterated = deque_of_claimed_blocks.popleft()
                except IndexError:
                    #claim moar blocks
                    deque_of_claimed_blocks = coord_db.get_list_of_next_block_heights_available(
                        starting_height=current_height_iterated + 1,
                        num_to_claim=NUM_CONSECUTIVE_BLOCKS_TO_CLAIM)

                if current_height_iterated > last_completed_height + 1:
                    msg = (('Deferred blame processor skipping from block %d to '
                            '%d. (PID %d)') %
                           (last_completed_height, current_height_iterated, my_pid))
                    address_reuse.logger.log_status(msg)
            else:
                current_height_iterated = current_height_iterated + 1

    except Exception:
        traceback.print_exc()
    finally:
        #rollback claim on all blocks we've claimed so they aren't frozen
        if ENABLE_MULTITHREADING and isinstance(current_height_iterated, int):
            assert coord_db is not None
            for height in deque_of_claimed_blocks:
                if height != last_completed_height:
                    coord_db.unclaim_block_height(height)
            if current_height_iterated != last_completed_height:
                coord_db.unclaim_block_height(current_height_iterated)

        #whether it finishes normally or is interrupted by ^C, print stats
        #   before exiting
        benchmarker.stop()
        benchmarker.print_stats()

        #TODO: roll back records upon early exit to safe point as with other
        #   "update" scripts. Actually, I don't think there's any rollback to
        #   do.

if __name__ == "__main__":
    main()
