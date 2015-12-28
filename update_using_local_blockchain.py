#Update the reuse database using the local blockchain.
#During development, this will write to its own database tables indicated by
#   <regular table name>_local. Tests can then compare the two for veracity.

####################
# INTERNAL IMPORTS #
####################

import address_reuse.db
import address_reuse.config
import address_reuse.benchmark.block_reader_benchmark
import address_reuse.blockchain_reader
import address_reuse.logger
import address_reuse.block_processor

####################
# EXTERNAL IMPORTS #
####################

import os #get name of this script for check_int_and_die() using os.path.basename
import traceback

#############
# CONSTANTS #
#############

THIS_FILE = os.path.basename(__file__)

################
# BEGIN SCRIPT #
################

def main():
    db = address_reuse.db.Database(
        blockchain_mode = address_reuse.config.BlockchainMode.BITCOIND_RPC)

    #Determine the current furthest block out in the blockchain according to remote 
    #   API
    api_reader = address_reuse.blockchain_reader.ThrottledBlockchainReader(db)
    current_blockchain_height = int(api_reader.get_current_blockchain_block_height())
    api_reader = None #Done with API lookups :>

    blockchain_reader = address_reuse.blockchain_reader.LocalBlockchainRPCReader(db)

    #Determine the last block I've updated in the db
    last_height_in_db = db.get_last_block_height_in_db()
    num_blocks_processed = 0
    if last_height_in_db is None:
        current_height_iterated = 0
    else:
        current_height_iterated = last_height_in_db + 1

    #TODO: deal with orphans

    #Determine max number of blocks to process. -1 blocks = infinity
    config = address_reuse.config.Config(
        blockchain_mode = address_reuse.config.BlockchainMode.BITCOIND_RPC)
    MAX_NUM_BLOCKS_TO_PROCESS_PER_RUN = config.MAX_NUM_BLOCKS_TO_PROCESS_PER_RUN
    num_blocks_remaining_to_process = MAX_NUM_BLOCKS_TO_PROCESS_PER_RUN

    last_block_height_processed = None

    benchmarker = address_reuse.benchmark.block_reader_benchmark.Benchmark()
    block_processor = address_reuse.block_processor.BlockProcessor(
        blockchain_reader, db)
    try:
        #Process blocks until we're caught up, or have hit the max # blocks to 
        #   process in this run.
        while (current_height_iterated < current_blockchain_height and \
               num_blocks_remaining_to_process):
            print("DEBUG: update_using_local_blockchain.py: current block height of blockchain is %d, last block processed in db is %d, %d remaining blocks to process in this run." % (current_blockchain_height, current_height_iterated, num_blocks_remaining_to_process))

            block_processor.process_block(current_height_iterated, benchmarker, 
                                          defer_blaming = True)
            print("Completed processing of block at height %d." % current_height_iterated)
             #Log successful processing of this block
            address_reuse.logger.log_status('Processed block %d with RPC.' % current_height_iterated)

            last_block_height_processed = current_height_iterated
            current_height_iterated = current_height_iterated + 1

            #continue going through blocks until there are no more or hit MAX_NUM_BLOCKS_TO_PROCESS_PER_RUN
            num_blocks_remaining_to_process = num_blocks_remaining_to_process - 1
            address_reuse.validate.check_int_and_die(
                num_blocks_remaining_to_process, 'num_blocks_remaining_to_process', 
                THIS_FILE)
    except Exception as e:
        traceback.print_exc()
    finally:
        #whether it finishes normally or is interrupted by ^C, print stats before 
        #   exiting
        benchmarker.stop()
        benchmarker.print_stats()

        #handle safe rollback
        if last_block_height_processed is None and current_height_iterated > 0:
            #We didn't complete processing any blocks in this run, but we may
            #   have partially processed one. Need to roll back to the height
            #   we completed before starting this run
            last_block_height_processed = current_height_iterated - 1

        if last_block_height_processed is not None:
            db.rollback_seen_addresses_cache_to_block_height(
                last_block_height_processed)
            print(("Due to early exit, rolled 'seen addresses' table back to the "
                   "last block we finished processing at height %d") % 
                  last_block_height_processed)
            #TODO: This method of rollback is crappy because we may want to
            #   allow users in the future to run multiple instances of this
            #   script to process multiple block heights simultaneously, but
            #   this triggers a DELETE that would devestate any other threads
            #   working at a higher block height. For now, though, it doesnt'
            #   matter.
            db.rollback_blame_stats_to_block_height(last_block_height_processed)
            print(("Due to early exit, rolled 'blame stats' table back to the "
                   "last block we finished processing at height %d") % 
                  last_block_height_processed)

if __name__ == "__main__":
    main()
