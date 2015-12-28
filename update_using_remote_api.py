#Description: A script used to update our database with address reuse information for blocks

####################
# INTERNAL IMPORTS #
####################

import address_reuse.db
import address_reuse.config
import address_reuse.blockchain_reader
import address_reuse.block_processor
import address_reuse.logger
import address_reuse.validate
import address_reuse.benchmark.block_reader_benchmark

####################
# EXTERNAL IMPORTS #
####################

import sys
import os   #get name of this script for check_int_and_die() using os.path.basename
import traceback

#############
# CONSTANTS #
#############

THIS_FILE = os.path.basename(__file__)

#############
# FUNCTIONS #
#############

################
# BEGIN SCRIPT #
################

#Determine the last block I've updated in the db
db = address_reuse.db.Database(
    blockchain_mode = address_reuse.config.BlockchainMode.REMOTE_API)
last_height_in_db = db.get_last_block_height_in_db()

#Determine the current furthest block out in the blockchain according to API
blockchain_reader = address_reuse.blockchain_reader.ThrottledBlockchainReader(db)
current_blockchain_height = int(blockchain_reader.get_current_blockchain_block_height())

last_height_iterated = last_height_in_db
num_blocks_processed = 0
current_height_iterated = None # this will track the current block height we are iterating on
if last_height_iterated is None:
    current_height_iterated = 0
else:
    current_height_iterated = last_height_iterated + 1

#TODO: deal with orphans if I want the accuracy to be solid
#Determine max number of blocks to process. -1 blocks = infinity, as all negative values are True in python, and this will continue to be decremented, at least until it hits some minimum
config = address_reuse.config.Config()
MAX_NUM_BLOCKS_TO_PROCESS_PER_RUN = config.MAX_NUM_BLOCKS_TO_PROCESS_PER_RUN
num_blocks_remaining_to_process = MAX_NUM_BLOCKS_TO_PROCESS_PER_RUN

benchmarker = address_reuse.benchmark.block_reader_benchmark.Benchmark()
try:
    #Process blocks until we're caught up, or have hit the max # blocks to process in this run.
    while (current_height_iterated < current_blockchain_height and num_blocks_remaining_to_process):
        print("DEBUG: update.py: current block height of blockchain is %d, last block processed in db is %d, %d remaining blocks to process in this run." % (current_blockchain_height, current_height_iterated, num_blocks_remaining_to_process))
        #instantiate a processor object to compile stats on this block and store them in the db
        block_processor = address_reuse.block_processor.BlockProcessor(blockchain_reader, db)
        block_processor.process_block(current_height_iterated, benchmarker)
        print("Completed processing of block at height %d." % current_height_iterated)
        
        #Log successful processing of this block
        address_reuse.logger.log_status('Processed block %d.' % current_height_iterated)
        
        current_height_iterated = current_height_iterated + 1

        #continue going through blocks until there are no more or hit MAX_NUM_BLOCKS_TO_PROCESS_PER_RUN
        num_blocks_remaining_to_process = num_blocks_remaining_to_process - 1
        address_reuse.validate.check_int_and_die(num_blocks_remaining_to_process, 'num_blocks_remaining_to_process', THIS_FILE)

        
except Exception as e:
    traceback.print_exc()
finally:
    #whether it finishes normally or is interrupted by ^C, print stats before exiting
    benchmarker.stop()
    benchmarker.print_stats()
    
    #TODO: roll back records upon early exit to safe point as with other
    #   "update" scripts.
