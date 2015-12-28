# Unit tests for block_processor.py

#Covers these classes and functions:
#   BlockProcessor:
#       process_block
#       process_block_after_deferred_blaming(block_height,benchmarker)

####################
# INTERNAL IMPORTS #
####################

import block_processor
import blockchain_reader
import db

####################
# EXTERNAL IMPORTS #
####################

import unittest
import os

#############
# CONSTANTS #
#############

TEMP_DB_FILENAME = 'address_reuse.db-temp'
DB_DEFERRED_BLAME_PLACEHOLDER = 'DB_DEFERRED_BLAME_PLACEHOLDER'

class BlockProcessorWithBlockchainAPITestCase(unittest.TestCase):
    def setUp(self):
        try:
            os.remove(TEMP_DB_FILENAME)
        except OSError:
            pass
        self.temp_db = db.Database(TEMP_DB_FILENAME)
        self.blockchain_reader = blockchain_reader.ThrottledBlockchainReader(
            self.temp_db)
    
    def tearDown(self):
        None
    
    #This block was incorrectly parsed in the past when collecting stastics
    # about transactions that send to addresses with a prior tx history
    def test_process_block_92879_blame_stats(self):
        processor = block_processor.BlockProcessor(
            self.blockchain_reader, self.temp_db)
        self.assertIsNotNone(processor)
        
        processor.process_block(92879)
        
        top_address_reuser_ids = self.temp_db.get_top_address_reuser_ids(10)
        
        self.assertEqual(len(top_address_reuser_ids), 2, 
                         "There should be 2 reuser ids for this block: " +
                         "%d" % len(top_address_reuser_ids))
        
        stats = self.temp_db.get_blame_stats_for_block_height(92879, 
                                                        top_address_reuser_ids)
        
        self.assertIsNotNone(stats)
        
        #some expected stats:
        #219 txs
        #1 transaction (68068cfe19e) reuses 14mUbjiofYY2F6h3ZGUSoTo3kxdqtajVTp 
        #   (wallet b3f4d8d8b9bcefb2)
        #113 transactions send from 14mUbjiofYY2F6h3ZGUSoTo3kxdqtajVTp back to 
        #   itself (113 of 219 tx is 51.60%)
        #101 transactions send from 1NNVFX7SiJF44fkqB27QbARXojhRSotF2o back to 
        #   itself (wallet 7be8795b4b011b0c) (101 of 219 tx is 46.12%)
        #the coinbase tx does not involve reuse
        #tx a769847dd does not involve reuse
        #tx ffbd9fa63 does not involve reuse
        #tx 19e641e59 does not involve reuse
        
        self.assertEqual(stats.block_height, 92879,
                         'Block height is wrong: %d' % stats.block_height)
        self.assertEqual(stats.num_tx_total, 219, 'Wrong number of ' +
                         'transactions: %d' % stats.num_tx_total)
        self.assertEqual(stats.pct_tx_with_sendback_reuse,
                         '97.72', '214 of 219 tx or 97.72% send back to ' +
                         'inputs, instead: %s' % str(
                            stats.pct_tx_with_sendback_reuse))
        self.assertEqual(stats.pct_tx_with_history_reuse,
                         '98.17','215 of 219 tx or 98.17% reuse addresses, ' +
                         'instead: %s' % str(stats.pct_tx_with_history_reuse))
        self.assertEqual(len(stats.top_reuser_labels), 2, 
                         "There should be 2 reuser labels for this block: " +
                        "%d" % len(stats.top_reuser_labels))
        self.assertIn('b3f4d8d8b9bcefb2', stats.top_reuser_labels, 'Wallet ' +
                      'b3f4d8d8b9bcefb2 is missing from the top reuser labels.')
        self.assertIn('7be8795b4b011b0c', stats.top_reuser_labels, 'Wallet ' +
                      '7be8795b4b011b0c is missing from the top reuser labels.')
 
        self.assertEqual(len(stats.party_label_to_pct_sendback_map), 2, 
                         'The list of sendback reuse percentages should be ' +
                         ' of size 2: %d' % len(
                            stats.party_label_to_pct_sendback_map))
        self.assertEqual(len(stats.party_label_to_pct_history_map), 2, 
                         'The list of address reuse percentages should be ' +
                         ' of size 2: %d' % len(
                            stats.party_label_to_pct_history_map))
        
        self.assertEqual(stats.party_label_to_pct_sendback_map[
                'b3f4d8d8b9bcefb2'], '51.60', 'The send-back reuse for ' +
                         'wallet b3f4d8d8b9bcefb2 should be 51.60%: ' + 
                         '%s' % str(stats.party_label_to_pct_sendback_map[
                                                        'b3f4d8d8b9bcefb2']))
        self.assertEqual(stats.party_label_to_pct_history_map[
                '7be8795b4b011b0c'], '46.12', 'The address reuse for ' +
                         'wallet 7be8795b4b011b0c should be 46.12%: ' + 
                         '%s' % str(stats.party_label_to_pct_history_map[
                                                        '7be8795b4b011b0c']))
        
        self.assertEqual(stats.party_label_to_pct_history_map[
                'b3f4d8d8b9bcefb2'], '52.05', 'The address reuse for ' +
                         'wallet b3f4d8d8b9bcefb2 should be 52.05%: ' + 
                         '%s' % str(stats.party_label_to_pct_history_map[
                                                        'b3f4d8d8b9bcefb2']))
        
        self.assertEqual(stats.party_label_to_pct_history_map[
                '7be8795b4b011b0c'], '46.12', 'The address reuse for ' +
                         'wallet 7be8795b4b011b0c should be 46.12%: ' + 
                         '%s' % str(stats.party_label_to_pct_history_map[
                                                        '7be8795b4b011b0c']))
    
    #def test_process_genesis_block(self):
    #    self.assertEqual(1,2, 
    #                     "test_process_genesis_block() not yet implemented")
    
    #TODO: Find a block where it reuses an address for the first time within the
    #   same block, and write a test for that.

#Tests functionality for processing blockchain with RPC lookups and then filling
#   in blame labels using remote API lookups later on.
class BlockProcessorDeferredBlameTestCase(unittest.TestCase):
    
    database_connector  = None
    reader              = None
    processor           = None
    
    def setUp(self):
        try:
            os.remove(TEMP_DB_FILENAME)
        except OSError:
            pass
        self.database_connector = db.Database(TEMP_DB_FILENAME)
        self.reader = blockchain_reader.LocalBlockchainRPCReader(
            self.database_connector)
        self.processor = block_processor.BlockProcessor(
            self.reader, self.database_connector)
    
    def tearDown(self):
        self.reader = None
    
    def test_process_block_after_deferred_blaming(self):
        #process block 170 with deferred blaming
        for block_height in range(0,171):
            self.processor.process_block(block_height, 
                                               defer_blaming = True)
            
        deferred_id = self.database_connector.get_blame_id_for_deferred_blame_placeholder()
        expected_blame_label = '67c35f9e5da6beab'
        
        self.assertIsNotNone(deferred_id)
        
        stmt = 'SELECT blame_recipient_id FROM ' + db.SQL_TABLE_NAME_BLAME_STATS + ' WHERE block_height = ? LIMIT 1'
        arglist = (170,)
        caller = 'test_process_block_after_deferred_blaming'
        column_name = 'blame_recipient_id'
        initial_id = self.database_connector.fetch_query_single_int(stmt, 
                                                                    arglist, 
                                                                    caller, 
                                                                    column_name)
        self.assertIsNotNone(initial_id)
        self.assertEqual(deferred_id, initial_id, 
                         (('blame id of row queried should match id for deferred '
                          'blame. Expected %d received %d') % 
                          (deferred_id, initial_id)))
        
        
        self.processor.process_block_after_deferred_blaming(170)
        new_blame_id = self.database_connector.fetch_query_single_int(stmt, 
                                                                    arglist, 
                                                                    caller, 
                                                                    column_name)
        self.assertNotEqual(new_blame_id, initial_id, 
                            ('blame id should have changed due to new '
                             'processing.'))
        #check blame label
        new_label = self.database_connector.get_blame_label_for_blame_id(
            new_blame_id)
        self.assertEqual(new_label, expected_blame_label, 
                         ('Expected blame label %s received %s' % 
                          (expected_blame_label, new_label)))
        
suite1 = unittest.TestLoader().loadTestsFromTestCase(
    BlockProcessorWithBlockchainAPITestCase)
suite2 = unittest.TestLoader().loadTestsFromTestCase(
    BlockProcessorDeferredBlameTestCase)
