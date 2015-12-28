################################################################################
#Downloads all of the output addresses from bitcoind's database via local RPC
#   interface. Primarily useful for pre-filling information in the database
#   before running update_using_local_blockchain.py.
################################################################################

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

db = address_reuse.db.Database(
    sqlite_db_filename = 'address_reuse_txoutcache.db')

#determine height to begin querying for
heighest_block_in_cache = db.get_highest_output_address_cached_height()
current_height_iterated = 0
if heighest_block_in_cache is not None:
    current_height_iterated = heighest_block_in_cache + 1
    
#Determine the current furthest block out in the blockchain according to remote 
#   API
api_reader = address_reuse.blockchain_reader.ThrottledBlockchainReader(db)
current_blockchain_height = int(api_reader.get_current_blockchain_block_height())

num_blocks_remaining_to_process = current_blockchain_height - current_height_iterated

local_block_reader = address_reuse.blockchain_reader.LocalBlockchainRPCReader(db)
block_processor = address_reuse.block_processor.BlockProcessor(local_block_reader, 
                                                               db)

benchmarker = address_reuse.benchmark.block_reader_benchmark.Benchmark()

last_block_height_processed = None
num_blocks_processed = 0
try:
    while (current_height_iterated < current_blockchain_height):
        print(("DEBUG: update_txout_cache.py: current block height of "
               "blockchain is %d, last block processed in db is %d, %d "
               "remaining blocks to process in this run.") % 
              (current_blockchain_height, current_height_iterated, 
               num_blocks_remaining_to_process))
        
        block_processor.cache_tx_output_addresses_for_block_only(
            current_height_iterated, benchmarker)
        
        print("Completed processing of block at height %d." % 
              current_height_iterated)
        last_block_height_processed = current_height_iterated
        current_height_iterated = current_height_iterated + 1
        
        num_blocks_remaining_to_process = num_blocks_remaining_to_process - 1
        num_blocks_processed = num_blocks_processed + 1
        
         #Log successful processing of this block
        address_reuse.logger.log_status('Cached all tx output addreses for block %d.' % 
                                        last_block_height_processed)

except KeyboardInterrupt:
    pass #Nothing to do here, already commits to db once per block
except Exception as e:
    traceback.print_exc()
finally:
    #whether it finishes normally or is interrupted by ^C, print stats before 
    #   exiting
    benchmarker.stop()
    benchmarker.print_stats()
