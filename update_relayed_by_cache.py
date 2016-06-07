################################################################################
#Downloads all of the 'relayed by' fields for all transactions in the blockchain
#   from remote API.

####################
# INTERNAL IMPORTS #
####################

import address_reuse.db
import address_reuse.config
import address_reuse.blockchain_reader
import address_reuse.block_processor
import address_reuse.benchmark.block_reader_benchmark

####################
# EXTERNAL IMPORTS #
####################

import traceback

################
# BEGIN SCRIPT #
################
if address_reuse.config.OFFLINE_MODE=0:
    db = address_reuse.db.Database(
        blockchain_mode = address_reuse.config.BlockchainMode.REMOTE_API)
else:
    db = address_reuse.db.Database(
        blockchain_mode = address_reuse.config.BlockchainMode.LOCAL_API)

#determine height to begin querying for
heighest_block_in_cache = db.get_highest_relayed_by_height()
current_height_iterated = 0
if heighest_block_in_cache is not None:
    current_height_iterated = heighest_block_in_cache + 1
    
#Determine the current furthest block out in the blockchain according to remote 
#   API
if address_reuse.config.OFFLINE_MODE=0:
    api_reader = address_reuse.blockchain_reader.ThrottledBlockchainReader(db)
else:
    api_reader = address_reuse.blockchain_reader.LocalBlockchainRPCReader(db)

current_blockchain_height = int(api_reader.get_current_blockchain_block_height())

num_blocks_remaining_to_process = current_blockchain_height - current_height_iterated

block_processor = address_reuse.block_processor.BlockProcessor(api_reader, db)

benchmarker = address_reuse.benchmark.block_reader_benchmark.Benchmark()

last_block_height_processed = None
try:
    while (current_height_iterated < current_blockchain_height):
        print(("DEBUG: update_relayed_by_cache.py: current block height of "
               "blockchain is %d, last block processed in db is %d, %d "
               "remaining blocks to process in this run.") % 
              (current_blockchain_height, current_height_iterated, 
               num_blocks_remaining_to_process))
        
        block_processor.cache_relayed_by_fields_for_block_only(
            current_height_iterated, benchmarker)
        
        print("Completed processing of block at height %d." % 
              current_height_iterated)
        last_block_height_processed = current_height_iterated
        current_height_iterated = current_height_iterated + 1
        
        num_blocks_remaining_to_process = num_blocks_remaining_to_process - 1
        
         #Log successful processing of this block
        address_reuse.logger.log_status('Cached relayed-by field for block %d.' % 
                                        last_block_height_processed)
        
except Exception as e:
    traceback.print_exc()
finally:
    #whether it finishes normally or is interrupted by ^C, print stats before 
    #   exiting
    benchmarker.stop()
    benchmarker.print_stats()
    
    if last_block_height_processed is not None:
        db.rollback_relayed_by_cache_to_block_height(last_block_height_processed)
        print(("Rolled database's relayed by cache back to maximum block "
               "height of %d") % last_block_height_processed)
        #TODO: this can sometimes deadlock with other processes, apply the
        #   repeated retried connection here:
        #http://stackoverflow.com/questions/2740806/python-sqlite-database-is-locked
        
