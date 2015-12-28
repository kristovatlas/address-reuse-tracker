# Unit tests for block_processor.py

#Covers these classesand functions:
#   BlockProcessor:
#       cache_tx_output_addresses_for_block_only(block_height, benchmarker)
#       process_deferred_client_blame_record()

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

class BlockProcessorForTxOutputAddrCacheTest(unittest.TestCase):
    def setUp(self):
        try:
            os.remove(TEMP_DB_FILENAME)
        except OSError:
            pass
        self.temp_db = db.Database(TEMP_DB_FILENAME)
        self.blockchain_reader = blockchain_reader.LocalBlockchainRPCReader(
            self.temp_db)
    
    def tearDown(self):
        pass
    
    def test_cache_tx_output_addresses_for_block_only_at_height_170(self):
        processor = block_processor.BlockProcessor(
            self.blockchain_reader, self.temp_db)
        self.assertIsNotNone(processor)
        processor.cache_tx_output_addresses_for_block_only(170)
        
        stmt = 'SELECT * FROM ' + db.SQL_TABLE_NAME_TX_OUTPUT_CACHE
        caller = 'test_cache_tx_output_addresses_for_block_only_at_height_170'
        
        rows = self.temp_db.fetch_query_and_handle_errors(stmt, [], 
                                                                     caller)
        
        self.assertEqual(len(rows), 3, 
                         ('There should be 3 outputs at block height 170, saw %d' % 
                          len(rows)))
        
        self.assertEqual(rows[0]['block_height'], 170)
        self.assertEqual(rows[0]['tx_id'], ('b1fea52486ce0c62bb442b530a3f0132b8'
                                            '26c74e473d1f2c220bfa78111c5082'))
        self.assertEqual(rows[0]['output_pos'], 0)
        self.assertEqual(rows[0]['address'], '1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc')
        
        self.assertEqual(rows[1]['block_height'], 170)
        self.assertEqual(rows[1]['tx_id'], ('f4184fc596403b9d638783cf57adfe4c75'
                                            'c605f6356fbc91338530e9831e9e16'))
        self.assertEqual(rows[1]['output_pos'], 0)
        self.assertEqual(rows[1]['address'], '1Q2TWHE3GMdB6BZKafqwxXtWAWgFt5Jvm3')
        
        self.assertEqual(rows[2]['block_height'], 170)
        self.assertEqual(rows[2]['tx_id'], ('f4184fc596403b9d638783cf57adfe4c75'
                                            'c605f6356fbc91338530e9831e9e16'))
        self.assertEqual(rows[2]['output_pos'], 1)
        self.assertEqual(rows[2]['address'], '12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S')
        
    #TODO: A test for what happens when an address can't be decoded. Find such 
    #   a block.
    
    def test_process_deferred_client_blame_record_for_uncached_and_remotely_null_client(self):
        #test with a record for which there is no cached client info in the
        #   database. The function should trigger a query to the remote API, 
        #   and it will still be unable to find a client. As a result, the 
        #   record should be deleted.
        def_id = self.temp_db.get_blame_id_for_label_and_insert_if_new(
            DB_DEFERRED_BLAME_PLACEHOLDER)
        
        stmt = ('INSERT INTO ' 
                '' + db.SQL_TABLE_NAME_BLAME_STATS + ' '
                '(blame_recipient_id, address_reuse_type, role, data_source, '
                'block_height, confirmed_tx_id, relevant_address) VALUES '
                '(?,?,?,?,?,?,?)')
        
        address_reuse_type = db.AddressReuseType.SENDBACK
        role = db.AddressReuseRole.CLIENT
        data_source = db.DataSource.BLOCKCHAIN_INFO
        block_height = 170
        confirmed_tx_id = ('b1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        #not actually address reuse, just for sake of test
        relevant_address = '1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc'
        
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.temp_db.run_statement(stmt, arglist)
        
        records = self.temp_db.get_blame_records_for_blame_id(def_id)
        
        self.assertEqual(len(records), 1)
        
        processor = block_processor.BlockProcessor(
            self.blockchain_reader, self.temp_db)
        processor.process_deferred_client_blame_record(records[0])
        
        if db.DELETE_BLAME_STATS_ONCE_PER_BLOCK:
            self.temp_db.write_deferred_blame_record_resolutions()
        
        #expected outcome: no relayed-by information for this txid in the db,
        #   so it will do a quick fetch from remote API, determine the value
        #   is 0.0.0.0, and therefore delete it from the database.
        
        stmt = ('SELECT 1 AS one FROM ' + db.SQL_TABLE_NAME_BLAME_STATS + ' '
                'WHERE blame_recipient_id = ?')
        arglist = (def_id,)
        caller = ('test_process_deferred_client_blame_record_for_uncached_and_'
                  'remotely_null_client')
        column_name = 'one'
        result = self.temp_db.fetch_query_single_int(stmt, arglist, caller, 
                                                     column_name)
        
        self.assertIsNone(result)
    
    def test_process_deferred_client_blame_record_for_uncached_and_remotely_non_null_client(self):
        #test with a record for which there is no cached client info in the
        #   database. The function should trigger a query to the remote API, 
        #   and it will find a client record there. As a result, the 
        #   record should be deleted.
        def_id = self.temp_db.get_blame_id_for_label_and_insert_if_new(
            DB_DEFERRED_BLAME_PLACEHOLDER)
        
        stmt = ('INSERT INTO ' 
                '' + db.SQL_TABLE_NAME_BLAME_STATS + ' '
                '(blame_recipient_id, address_reuse_type, role, data_source, '
                'block_height, confirmed_tx_id, relevant_address) VALUES '
                '(?,?,?,?,?,?,?)')
        
        #i picked this tx as a BCI tx that involves sendback reuse
        address_reuse_type = db.AddressReuseType.SENDBACK
        role = db.AddressReuseRole.CLIENT
        data_source = db.DataSource.BLOCKCHAIN_INFO
        block_height = 300000
        confirmed_tx_id = ('3184aa6ccaed5f3e41fc34045970cee7501b68795c235108deb'
                           'd1c9a5dfec1a4')
        relevant_address = '1Aj68cCt2UqhPpRrjtQhnFwezCzCebGfvv'
        
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.temp_db.run_statement(stmt, arglist)
        
        records = self.temp_db.get_blame_records_for_blame_id(def_id)
        
        self.assertEqual(len(records), 1)
        
        processor = block_processor.BlockProcessor(
            self.blockchain_reader, self.temp_db)
        processor.process_deferred_client_blame_record(records[0])
        if db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK:
            self.temp_db.write_deferred_blame_record_resolutions()
        
        #expected outcome: no relayed-by information for this txid in the db,
        #   so it will do a quick fetch from remote API, determine the value
        #   is 127.0.0.1, and update the record to point at Blockchain.info
        #   as the blamed party.
        
        stmt = ('SELECT rowid FROM ' + db.SQL_TABLE_NAME_BLAME_IDS + ' WHERE '
                'label = ?')
        arglist = ('Blockchain.info',)
        caller = ('test_process_deferred_client_blame_record_for_uncached_and_'
                  'remotely_non_null_client')
        column_name = 'rowid'
        bci_rowid = self.temp_db.fetch_query_single_int(stmt, arglist, caller, 
                                                        column_name)
        self.assertEqual(bci_rowid, 2)
        
        stmt = ('SELECT * FROM ' + db.SQL_TABLE_NAME_BLAME_STATS + ' '
                'WHERE block_height = ?')
        arglist = (block_height,)
        
        records = self.temp_db.fetch_query_and_handle_errors(stmt, arglist, 
                                                             caller)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['block_height'], 300000)
        self.assertEqual(records[0]['blame_recipient_id'], bci_rowid)
    
    def test_process_deferred_client_blame_record_for_cached_non_null_client(self):
        #test with a record for which there is a 'relayed by' field cached in
        #   the database. The end result is that the deferred record should be
        #   updated.
        block_height = 170
        confirmed_tx_id = ('b1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        
        stmt = ('INSERT INTO ' + db.SQL_TABLE_NAME_RELAYED_BY_CACHE + ' '
                '(block_height, tx_id, relayed_by) VALUES (?,?,?)')
        arglist = (170, confirmed_tx_id, '127.0.0.1')
        self.temp_db.run_statement(stmt, arglist)
        
        def_id = self.temp_db.get_blame_id_for_label_and_insert_if_new(
            DB_DEFERRED_BLAME_PLACEHOLDER)
        
        stmt = ('INSERT INTO ' 
                '' + db.SQL_TABLE_NAME_BLAME_STATS + ' '
                '(blame_recipient_id, address_reuse_type, role, data_source, '
                'block_height, confirmed_tx_id, relevant_address) VALUES '
                '(?,?,?,?,?,?,?)')
        
        address_reuse_type = db.AddressReuseType.SENDBACK
        role = db.AddressReuseRole.CLIENT
        data_source = db.DataSource.BLOCKCHAIN_INFO
        #not actually address reuse, just for sake of test
        relevant_address = '1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc'
        
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.temp_db.run_statement(stmt, arglist)
        
        records = self.temp_db.get_blame_records_for_blame_id(def_id)
        
        self.assertEqual(len(records), 1)
        
        processor = block_processor.BlockProcessor(
            self.blockchain_reader, self.temp_db)
        processor.process_deferred_client_blame_record(records[0])
        
        if db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK:
            self.temp_db.write_deferred_blame_record_resolutions()
        
        #expected outcome: relayed-by information for this txid cached in the 
        #   db, resulting in updating the record to blame BCI as the client.
        
        stmt = ('SELECT rowid FROM ' + db.SQL_TABLE_NAME_BLAME_IDS + ' WHERE '
                'label = ?')
        arglist = ('Blockchain.info',)
        caller = ('test_process_deferred_client_blame_record_for_cached_non_'
                  'null_client')
        column_name = 'rowid'
        bci_rowid = self.temp_db.fetch_query_single_int(stmt, arglist, caller, 
                                                        column_name)
        self.assertEqual(bci_rowid, 2)
        
        stmt = ('SELECT * FROM ' + db.SQL_TABLE_NAME_BLAME_STATS + ' '
                'WHERE block_height = ?')
        arglist = (block_height,)
        
        records = self.temp_db.fetch_query_and_handle_errors(stmt, arglist, 
                                                             caller)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['block_height'], block_height)
        self.assertEqual(records[0]['blame_recipient_id'], bci_rowid)
        
    def test_process_deferred_client_blame_record_for_cached_null_client(self):
        #test with a record for which there is a 'relayed by' field cached in
        #   the database with null value '1.1.1.1'. The end result is that the 
        #   deferred record should be deleted.
        block_height = 170
        confirmed_tx_id = ('b1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        
        stmt = ('INSERT INTO ' + db.SQL_TABLE_NAME_RELAYED_BY_CACHE + ' '
                '(block_height, tx_id, relayed_by) VALUES (?,?,?)')
        arglist = (170, confirmed_tx_id, '1.1.1.1')
        self.temp_db.run_statement(stmt, arglist)
        
        def_id = self.temp_db.get_blame_id_for_label_and_insert_if_new(
            DB_DEFERRED_BLAME_PLACEHOLDER)
        
        stmt = ('INSERT INTO ' 
                '' + db.SQL_TABLE_NAME_BLAME_STATS + ' '
                '(blame_recipient_id, address_reuse_type, role, data_source, '
                'block_height, confirmed_tx_id, relevant_address) VALUES '
                '(?,?,?,?,?,?,?)')
        
        address_reuse_type = db.AddressReuseType.SENDBACK
        role = db.AddressReuseRole.CLIENT
        data_source = db.DataSource.BLOCKCHAIN_INFO
        #not actually address reuse, just for sake of test
        relevant_address = '1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc'
        
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.temp_db.run_statement(stmt, arglist)
        
        records = self.temp_db.get_blame_records_for_blame_id(def_id)
        
        self.assertEqual(len(records), 1)
        
        processor = block_processor.BlockProcessor(
            self.blockchain_reader, self.temp_db)
        processor.process_deferred_client_blame_record(records[0])
        
        if db.DELETE_BLAME_STATS_ONCE_PER_BLOCK:
            self.temp_db.write_deferred_blame_record_resolutions()
        
        #expected outcome: relayed-by information cached in db doesn't match any 
        #   known client, so the deferred record is deleted from the database.
        
        stmt = ('SELECT 1 AS one FROM ' + db.SQL_TABLE_NAME_BLAME_STATS + ' '
                'LIMIT 1')
        arglist = []
        caller = ('test_process_deferred_client_blame_record_for_cached_null_'
                  'client')
        column_name = 'one'
        result = self.temp_db.fetch_query_single_int(stmt, arglist, caller, 
                                                     column_name)
        
        self.assertIsNone(result)

unittest.TestLoader().loadTestsFromTestCase(
    BlockProcessorForTxOutputAddrCacheTest)
