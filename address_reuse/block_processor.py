#http://stackoverflow.com/questions/1267869/how-can-i-force-division-to-be-floating-point-in-python
from __future__ import division # make division of two ints return float
#SyntaxError: from __future__ imports must occur at the beginning of the file

####################
# INTERNAL IMPORTS #
####################

import logger
import validate
import db
import tx_blame
import block_state

#TODO: for debugging only
import time_debug

####################
# EXTERNAL IMPORTS #
####################

import os #get name of this script for check_int_and_die() using os.path.basename

#############
# CONSTANTS #
#############

ENABLE_DEBUG_PRINT = True

THIS_FILE = os.path.basename(__file__)
#This is the approximate height at which Blockchain.info started collecting 
#   'Relayed By' information for transactions. In order to speed things up for 
#   that many blocks, set the next flag to True
SKIP_CLIENT_LOOKUP_BEFORE_BLOCK_HEIGHT = 168085 
DO_SKIP_CLIENT_LOOKUP_BELOW_FIRST_BLOCK = False

#See: e.g. https://www.blocktrail.com/BTC/tx/e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468
WEIRD_TXS_TO_SKIP_FOR_RELAYED_BY_CACHING = {
    #tx hash => block height
    #Below 2 txs are duplicates, later rendered impossible by BIP30.
    'd5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599': 91842,
    'e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468': 91880
}

''' deprecated
#These are blocks so oddly formatted that we should skip processing them
#   entirely.
#See: e.g. https://blockchain.info/tx/e411dbebd2f7d64dafeef9b14b5c59ec60c36779d43f850e5e347abee1e1a455
# and https://www.blocktrail.com/BTC/tx/e411dbebd2f7d64dafeef9b14b5c59ec60c36779d43f850e5e347abee1e1a455
WEIRD_TXS_TO_SKIP_FOR_PROCESSING = {
    'e411dbebd2f7d64dafeef9b14b5c59ec60c36779d43f850e5e347abee1e1a455': True,
    '2a0597e665ac3d1cabeede95cedf907934db7f639e477b3c77b242140d8cf728': True,
    'a288fec5559c3f73fd3d93db8e8460562ebfe2fcf04a5114e8d0f2920a6270dc': True,
    '5492a05f1edfbd29c525a3dbf45f654d0fc45a805ccd620d0a4dff47de63f90b': True,
    'cee16a9b222f636cd27d734da0a131cee5dd7a1d09cb5f14f4d1330b22aaa38e': True,
    '0adfc9f9ace87a2956626777c2e2637c789ca4919a77c314d53ffc1d0bc8ad38': True
}
'''

#deprecated for now: when output address can't be decoded, 'addr' field
#    is simply not set. See get_bci_like_tuple_for_tx_id() in blockchain_reader.
WEIRD_TXS_TO_SKIP_FOR_PROCESSING = {}

#TODO: Split the various kinds of block processing functions into subclasses
#   of BlockProcessor.
class BlockProcessor:
    
    block_reader    = None # API reader
    database        = None # database connector
    blamer          = None
    
    def __init__(self, block_reader, database_connector = None):
        #TODO: declare a class of block reader that other classes can extend, 
        # then check here to make sure it is a subclass
        self.block_reader = block_reader
        
        if database_connector is None:
            self.database = db.Database()
        else:
            self.database = database_connector
        self.blamer = tx_blame.Blamer(self.database)
    
    '''
    To process a block we must:
      * fetch a list of txs
      * foreach tx, go through lists of input and output addresses
      * observe reuse between inputs and outputs
      * foreach address, get tx history and determine whether the address has a 
        history prior to this tx
    Note about malleability: Since we are only concerned with confirmed 
      transactions, tx id malleability should be an unusual case for orphaned 
      blocks or non-SIGHASH_ALL locking scripts. TODO: deal with these?
    Information stored in db about the block:
      * total number of transactions
      * nuber of transactions with send-back reuse
      * number of transactions with outputs that have prior tx history
      * compute percentage of transactions with send-back reuse
      * compute percentage of transactions with prior tx history outputs
    '''
    #param0: block_height: The height of the block you want to process
    #param1: benchmarker (optional): a block_reader_benchmark.Benchmark object
    #param2: defer_blaming (optional): A flag that decides whether to
    #   defer attributing instances of address reuse to a particular party
    #   such as a wallet client or address cluster. We may want to do this when
    #   processing the blockchain locally so that one thread can focus on
    #   parsing the blockchain, and another can focus on remote API lookups.
    #   Default: False
    #param3: use_tx_out_addr_cache_only (Optional): When looking up addresses
    #   for previous transactions, ONLY refer to cache in SQLite database,
    #   rather than slower option of using RPC interface. If set to True,
    #   process will sleep until the data is available in the cache. Default:
    #   False.
    def process_block(self, block_height, benchmarker = None, 
                      defer_blaming = False, 
                      use_tx_out_addr_cache_only = False):
        validate.check_int_and_die(block_height, 'block_height', THIS_FILE)
        current_block_state = block_state.BlockState(block_height) # block stats collector
        debug_timer = time_debug.Timer(purpose='get_tx_list @ block %d' % block_height)
        tx_list = self.block_reader.get_tx_list(block_height, 
                                                use_tx_out_addr_cache_only)
        debug_timer.stop()
        
        debug_timer = time_debug.Timer(purpose='process_tx for all txs @ block %d' % block_height)
        for tx in tx_list:
            self.process_tx(tx, current_block_state, block_height, benchmarker, 
                            defer_blaming)
        debug_timer.stop()
        
        debug_timer = time_debug.Timer(purpose='write_stored_blame @ block %d' % block_height)
        #Per requirements of db.store_blame(), call write_stored_blame() to
        #   write the records cached in Python memory to the database as a
        #   block-sized batch
        if db.INSERT_BLAME_STATS_ONCE_PER_BLOCK:
            self.database.write_stored_blame()
            dprint("Committed stored blame stats to db.")
        debug_timer.stop()
            
        if benchmarker is not None:
            benchmarker.increment_blocks_processed()
        
        debug_timer = time_debug.Timer(purpose='record_block_stats @ block %d' % block_height)
        current_block_state.update_sendback_reuse_pct()
        current_block_state.update_receiver_histoy_pct()
        self.database.record_block_stats(current_block_state)
        debug_timer.stop()
    
    def get_input_address_list_from_txObj(self, txObj):
        input_address_list = []
        try:
            for btc_input in txObj['inputs']:
                if 'prev_out' in btc_input and 'addr' in btc_input['prev_out']:
                    input_address_list.append(btc_input['prev_out']['addr'])
            return input_address_list
        except IndexError as e: #TODO: I think this is a KeyError
            logger.log_and_die("Missing index in txObj: '%s" % str(e))
    
    #This function is called to fill in blame statistics for address reuse 
    #   after data from the blockchain has already been processed. This is 
    #   necessary, for example, if the blockchain analysis was based on a local 
    #   copy of the blockchain but all blame data is accessible only via remote 
    #   APIs.
    def process_block_after_deferred_blaming(self, block_height, 
                                             benchmarker = None):
        outer_timer = time_debug.Timer(
            purpose=('process_block_after_deferred_blaming() @ block %d' % 
                     block_height))
        placeholder_rowid = self.database.get_blame_id_for_deferred_blame_placeholder()
        
        debug_timer = time_debug.Timer(purpose='get_all_deferred_blame_records_at_height @ block %d' % block_height)
        blame_records = self.database.get_all_deferred_blame_records_at_height(
            block_height)
        debug_timer.stop()
        
        dprint("Retrieved %d deferred blame records from db @ height %d" %
              (len(blame_records), block_height))
        
        #Rules for processing each record:
        #   1. If the blame_role is CLIENT, obtain the wallet client used from
        #       a local cache of a remote API call or the remote API call if
        #       not yet cached. If it cannot be obtained, delete the 
        #       record. Otherwise, update it.
        #   2. If the blame_role is SENDER and reuse_type is SENDBACK, delete 
        #       the record as redundant for the RECEIVER record.
        #   3. If rule 2 doesn't apply and the blame_role is SENDER or 
        #       RECEIVER, use  the update_blame_record function to update the 
        #       BlameRecord information. Update the record in the database with 
        #       the new information.
        debug_timer = time_debug.Timer(
            purpose=('update all fetched deferred blame records @ block %d' % 
                     block_height))
        debug_i = 0
        for blame_record in blame_records:
            if blame_record.address_reuse_role == db.AddressReuseRole.CLIENT:
                inner_debug_timer = time_debug.Timer(
                    purpose=('process_deferred_client_blame_record @ block %d record %d' % 
                             (block_height, debug_i)))
                self.process_deferred_client_blame_record(blame_record)
                inner_debug_timer.stop
            if blame_record.address_reuse_type == db.AddressReuseType.SENDBACK and \
                    blame_record.address_reuse_role == db.AddressReuseRole.RECEIVER:
                inner_debug_timer = time_debug.Timer(
                    purpose=('delete_deferred_sendback_receiver_record @ block %d record %d' % 
                             (block_height, debug_i)))
                self.delete_deferred_sendback_receiver_record(blame_record)
                inner_debug_timer.stop()
            else:
                inner_debug_timer = time_debug.Timer(
                    purpose=('get_single_wallet_label @ block %d record %d' % 
                             (block_height, debug_i)))
                blame_record.blame_label = self.blamer.get_single_wallet_label(
                    blame_record.relevant_address)
                inner_debug_timer.stop()
                dprint(("Attempting to update record with new blame "
                       "label %s") % blame_record.blame_label)
                inner_debug_timer = time_debug.Timer(
                    purpose=('update_blame_record @ block %d record %d' % 
                             (block_height, debug_i)))
                self.database.update_blame_record(blame_record)
                inner_debug_timer.stop()
            debug_i = debug_i + 1
        
        if db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK:
            self.database.write_deferred_blame_record_resolutions()
        debug_timer.stop()
        
        if benchmarker is not None:
            benchmarker.increment_blocks_processed()
        
        outer_timer.stop()
    
    #Use remote API query to obtain the wallet client used. If it cannot be 
    #   obtained, delete the record. Otherwise, update it. If in-memory caching
    #   is enlabed per-block in the db module, the caller of this function
    #   should later manually commit these update/deletes once the whole
    #   block has been processed.
    def process_deferred_client_blame_record(self, blame_record):
        assert isinstance(blame_record, tx_blame.BlameRecord)
        assert blame_record.address_reuse_role == db.AddressReuseRole.CLIENT
        
        dprint("Processing record: " + str(blame_record))
        
        client = None
        if DO_SKIP_CLIENT_LOOKUP_BELOW_FIRST_BLOCK and \
            blame_record.block_height < SKIP_CLIENT_LOOKUP_BEFORE_BLOCK_HEIGHT:
                #don't bother looking up client info
                pass
        else:
            client_record = self.blamer.get_wallet_client_blame_record_by_tx_id(
                blame_record.tx_id)
        if client_record is None:
            dprint("No client information, must delete this record.")
            self.database.delete_blame_record(blame_record.row_id)
        else:
            client_label = client_record.blame_label
            dprint("Will update record with client information " + client_label)
            blame_record.blame_label = client_label
            self.database.update_blame_record(blame_record)
    
    def delete_deferred_sendback_receiver_record(self, blame_record):
        assert isinstance(blame_record, tx_blame.BlameRecord)
        
        if blame_record.address_reuse_role != db.AddressReuseRole.RECEIVER or \
            blame_record.address_reuse_type != db.AddressReuseType.SENDBACK:
            msg = ("blame_record argument to "
                   "delete_deferred_sendback_receiver_record() has wrong "
                   "values: %s") % str(blame_record)
            logger.log_and_die(msg)
        
        self.database.delete_blame_record(blame_record.row_id)
    
    #stores the 'relayed by' field in database for all transactions for a
    #   given block
    def cache_relayed_by_fields_for_block_only(self, block_height, 
                                               benchmarker = None):
        tx_list = self.block_reader.get_tx_list(block_height)
        for txObj in tx_list:
            tx_id = txObj['hash']
            relayed_by = txObj['relayed_by']
            if tx_id in WEIRD_TXS_TO_SKIP_FOR_RELAYED_BY_CACHING and \
               WEIRD_TXS_TO_SKIP_FOR_RELAYED_BY_CACHING[tx_id] == block_height:
                pass
            else:
                self.database.record_relayed_by(tx_id, block_height, relayed_by)
            if benchmarker is not None:
                benchmarker.increment_transactions_processed()
        if benchmarker is not None:
            benchmarker.increment_blocks_processed()
    
    #stores transaction output addresses in database for all transactions for a 
    #   given block. Later they can be queried when resolving input addresses.
    #   This should only be used with the local RPC blockchain reader.
    def cache_tx_output_addresses_for_block_only(self, block_height, 
                                                 benchmarker = None):
        debug_timer = time_debug.Timer(purpose='fetch all tx output info @ block %d' % block_height)
        tx_id_list = self.block_reader.get_tx_ids_at_height(block_height)
        for tx_id in tx_id_list:
            rpc_style_tx_json = self.block_reader.get_decoded_tx(tx_id)
            address_list = self.block_reader.get_output_addresses(
                rpc_style_tx_json)
            for output_pos in range(0, len(address_list)):
                address = address_list[output_pos]
                self.database.add_output_address_to_mem_cache(block_height, 
                                                              tx_id, 
                                                              output_pos, 
                                                              address)
            if benchmarker is not None:
                benchmarker.increment_transactions_processed()
        debug_timer.stop()
        
        debug_timer = time_debug.Timer(purpose='write_stored_output_addresses @ block %d' % block_height)
        self.database.write_stored_output_addresses() #write db file per block
        debug_timer.stop()
        if benchmarker is not None:
            benchmarker.increment_blocks_processed()
    
    #Looks for instances of address reuse in the specified tx, and stores
    #   records in the database for those instances of address reuse.
    #param0: txObj should be a transaction object decoded from the JSON output 
    #   of the block explorer API
    #param1: a BlockState object that we'll update
    #param2: benchmarker (optional): a block_reader_benchmark.Benchmark object
    #param3: defer_blaming (optional): A flag that decides whether to
    #   defer attributing instances of address reuse to a particular party
    #   such as a wallet client or address cluster. We may want to do this when
    #   processing the blockchain locally so that one thread can focus on
    #   parsing the blockchain, and another can focus on remote API lookups.
    #   Default: False
    #TODO: This function is too long and indented, break into smaller pieces
    def process_tx(self, txObj, current_block_state, block_height, 
                   benchmarker = None, defer_blaming = False): 
        current_block_state.incr_total_tx_num()
        tx_contains_sendback_reuse = False
        tx_contains_receiver_with_history = False
        output_addr_list = dict() #currently updated but not logically used
        
        tx_id = txObj['hash']
        dprint("tx_id = %s" % tx_id)
        
        if tx_id in WEIRD_TXS_TO_SKIP_FOR_PROCESSING:
            dprint("We skipped this weird transaction %s" % tx_id)
            if benchmarker is not None:
                benchmarker.increment_transactions_processed()
            return None
        
        #Compile a list of input addresses that various callees will need
        input_address_list = self.get_input_address_list_from_txObj(txObj)
        
        #Look through inputs to see if it matches any addresses in outputs
        for btc_input in txObj['inputs']:
            #if this input has an address, see if it's also in the outputs    
            if 'prev_out' in btc_input and 'addr' in btc_input['prev_out']:
                assert not isinstance(btc_input['prev_out'], list)
                input_addr = btc_input['prev_out']['addr']
                #dprint("input_addr: %s" % input_addr)
                for btc_output in txObj['out']:
                    if 'addr' in btc_output:
                        output_addr = btc_output['addr']
                        #dprint("output_addr: %s" % output_addr)
                        #now let's see if the input we're iterating on matches 
                        #   an output address
                        if input_addr == output_addr:
                            #Found an instance of send-back address reuse. Find 
                            #   parties to blame and store that in the db
                            dprint("Address '%s' is found in both inputs and outputs of transaction '%s' at block height '%d'." % (input_addr, tx_id, current_block_state.block_num))                          
                            
                            blame_records = self.blamer.get_wallet_blame_list(
                                tx_id, input_address_list, input_addr, 
                                benchmarker, defer_blaming)
                            for blame_record in blame_records:
                                self.database.store_blame(
                                    blame_record.blame_label, 
                                    db.AddressReuseType.SENDBACK, 
                                    blame_record.address_reuse_role, 
                                    blame_record.data_source, 
                                    current_block_state.block_num, 
                                    tx_id, 
                                    input_addr)
                            
                            if not tx_contains_sendback_reuse:
                                tx_contains_sendback_reuse = True
                                current_block_state.incr_sendback_reuse() # count only once per tx
        
        #Look through outputs to see if any of them have a tx history PRIOR to 
        #   this tx
        for btc_output in txObj['out']:
            if 'addr' in btc_output:
                output_addr = btc_output['addr']
                if self.does_output_have_prior_tx_history(output_addr, tx_id, 
                                                          block_height, 
                                                          benchmarker):
                    #Found an instance of send-back address reuse. Find parties 
                    #   to blame and store that in the db
                    dprint("Address '%s' was sent to despite a prior tx history in transaction '%s' in block at height '%d'" % (output_addr, tx_id, current_block_state.block_num))
                    blame_records = self.blamer.get_wallet_blame_list(tx_id, 
                                                                      input_address_list, 
                                                                      output_addr, 
                                                                      benchmarker, 
                                                                      defer_blaming)
                    for blame_record in blame_records:
                        self.database.store_blame(blame_record.blame_label, 
                                                  db.AddressReuseType.TX_HISTORY, 
                                                  blame_record.address_reuse_role, 
                                                  blame_record.data_source, 
                                                  current_block_state.block_num, 
                                                  tx_id, 
                                                  output_addr)
                        
                    if not tx_contains_receiver_with_history:
                        tx_contains_receiver_with_history = True
                        current_block_state.incr_receiver_tx_history_reuse() # count only once per tx
                        
        #Done looking through inputs and outputs for this tx
        dprint("Completed processing tx '%s'" % tx_id)
        if benchmarker is not None:
            benchmarker.increment_transactions_processed()
            
    #Helper function for does_output_have_prior_tx_history()
    def does_output_have_prior_tx_history(self, addr, current_tx_id, 
                                          block_height, benchmarker = None):
        dprint("Address to be validated: %s" % addr)
        validate.check_address_and_die(addr, THIS_FILE)
        if self.block_reader.is_first_transaction_for_address(addr, 
                                                              current_tx_id,
                                                              block_height,
                                                              benchmarker):
            return False
        else:
            return True

#############
# FUNCTIONS #
#############

def dprint(str):
    if ENABLE_DEBUG_PRINT:
        print("DEBUG: %s" % str)
