#Covers these classes and functions:
#   BlameResolverCoordinationDatabase:
#       run_statement(stmt, arglist, execute_many)
#       fetch_query_and_handle_errors(stmt, arglist, caller)
#       mark_block_complete(block_height)
#       claim_block_height(block_height)
#       unclaim_block_height(block_height)
#       mark_blocks_completed_up_through_height(block_height)
#       get_list_of_block_heights_with_possibly_crashed_workers()
#       get_next_block_height_available(starting_height, claim_it, jump_minimum)
#
#   Database:
#       update_blame_record(blame_record)
#       get_blame_id_for_deferred_blame_placeholder
#       get_blame_records_for_blame_id(blame_id)
#       delete_blame_record(row_id)
#       write_stored_blame()
#       get_lowest_block_height_with_deferred_records()
#       get_all_deferred_blame_records_at_height(block_height)
#       cache_blame_label_for_btc_address(btc_address, label)
#       update_blame_label_for_btc_address(btc_address, label)
#       write_deferred_blame_record_resolutions()
#       fetch_more_deferred_records_for_cache() #TODO
#
#   TODO for Database:
#       ####### BLOCK STATS FUNCTIONS #######
#       get_last_block_height_in_db()
#       record_block_stats(block_state)
#       get_block_stats(block_height)
#       get_block_stats_for_span(min_block_height, max_block_height)
#
#       ####### BLAME DB FUNCTIONS ##########
#       get_blamed_address_list_for_role(role)
#       get_blame_id_for_label(blame_label)
#       get_blame_id_for_label_and_insert_if_new(blame_label)
#       get_blame_id_for_role_and_address(role, relevant_address)
#       get_blame_label_for_blame_id(blame_party_id)
#       add_blame_party(blame_label)
#       add_blame_record(blame_party_id, address_reuse_type, role, data_source, 
#           block_height, confirmed_tx_id, relevant_address)
#       store_blame(blame_label, address_reuse_type, role, data_source, 
#           block_height, confirmed_tx_id, relevant_address)
#       get_all_distinct_addresses_from_blame_records()
#       get_top_address_reuser_ids(num_reusers, min_block_height, 
#           max_block_height)
#       get_num_records_across_block_span(blame_party_id, address_reuse_type, 
#                                         min_block_height, max_block_height)
#       get_blame_stats_for_block_height(block_height, blame_party_ids)
#       get_blame_records_for_blame_id(blame_id, block_height)
#       rollback_blame_stats_to_block_height(max_block_height)
#
#       ####### BLAME CACHE FUNCTIONS ########
#       get_blame_label_for_btc_address(btc_address)
#
#       ####### SEEN ADDRESSES CACHE FUNCTIONS ######
#       has_address_been_seen_cache_if_not(btc_address, block_height_first_seen)
#       rollback_seen_addresses_cache_to_block_height(max_block_height)
#
#       ####### RELAYED-BY CACHE FUNCTIONS ####
#       get_cached_relayed_by(tx_id)
#       record_relayed_by(tx_id, block_height, relayed_by)
#       rollback_relayed_by_cache_to_block_height(max_block_height)
#       get_highest_relayed_by_height()

####################
# INTERNAL IMPORTS #
####################

import address_reuse.db
import address_reuse.tx_blame
import address_reuse.custom_errors

####################
# EXTERNAL IMPORTS #
####################

import unittest
import os
import sqlite3

#############
# CONSTANTS #
#############

TEMP_DB_FILENAME = 'address_reuse.db-temp'
TEMP_COORD_DB_FILENAME = 'coordination.db-temp'
DB_DEFERRED_BLAME_PLACEHOLDER = 'DB_DEFERRED_BLAME_PLACEHOLDER'

#####################
# TEST CASE CLASSES #
#####################

#TODO: make this class more specific, must decide how specific
class DatabaseTestCase(unittest.TestCase):
    database_connector = None
    reader = None
    
    def setUp(self):
        try:
            os.remove(TEMP_DB_FILENAME)
        except OSError:
            pass
        self.database_connector = address_reuse.db.Database(TEMP_DB_FILENAME)
        self.reader = address_reuse.blockchain_reader.LocalBlockchainRPCReader(
            self.database_connector)
        
    def tearDown(self):
        self.database_connector.close()
    
    #Callers of this test must set these blame_record fields:
    #   * address_reuse_type
    #   * address_reuse_role
    #   * data_source
    #   * block_height
    #   * tx_id
    #   * relevant_address
    
    def do_update_blame_record_without_batching(self, blame_record):
        old_val = address_reuse.db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK
        address_reuse.db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK = False
        
        blame_record.blame_label = 'DB_DEFERRED_BLAME_PLACEHOLDER'
        new_id = self.database_connector.get_blame_id_for_label_and_insert_if_new(
            'DB_DEFERRED_BLAME_PLACEHOLDER')
        
        #simulate call to db.add_blame_record()
        stmt = ('INSERT INTO ' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ''
                ' (blame_recipient_id, address_reuse_type, role, data_source, '
                'block_height, confirmed_tx_id, relevant_address) VALUES '
                '(?, ?, ? , ?, ?, ?, ?)')
        blame_recipient_id  = new_id    #I expect this is 1, but not important
                                        #  to this test
        address_reuse_type  = blame_record.address_reuse_type
        role                = blame_record.address_reuse_role
        data_source         = blame_record.data_source
        block_height        = blame_record.block_height
        confirmed_tx_id     = blame_record.tx_id
        relevant_address    = blame_record.relevant_address
        arglist = (blame_recipient_id, address_reuse_type, role, data_source, 
                   block_height, confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        
        #simulate call to db.cache_blame_label_for_btc_address()
        stmt = ('INSERT INTO ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE + ' '
                '(btc_address, label) VALUES (?,?)')
        arglist = (relevant_address, blame_record.blame_label)
        self.database_connector.run_statement(stmt, arglist)
        
        blame_record.blame_label = 'RESERVED_NEW_BLAME_LABEL'
        blame_record.row_id = 1 #the rowid of the row we inserted
        self.database_connector.update_blame_record(blame_record)
        
        #ensure that blame label was updated
        stmt = ('SELECT blame_recipient_id FROM '
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ''
                ' WHERE rowid = ?')
        arglist = (1,) #the rowid of the row we inserted
        caller = 'do_update_blame_record_without_batching'
        updated_blame_id = self.database_connector.fetch_query_single_int(
            stmt, arglist, caller, 'blame_recipient_id')
        self.assertNotEqual(updated_blame_id, 1, 
                            "New blame ID should not be 1: %d" % 
                                updated_blame_id)
        updated_label = self.database_connector.get_blame_label_for_blame_id(
            updated_blame_id)
        self.assertEqual(blame_record.blame_label, updated_label, 
                         'Blame label was not updated.')
        
        #ensure that blame label was also updated in blame label cache
        stmt = ('SELECT label FROM ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE + ' '
                'WHERE btc_address = ?')
        arglist = (relevant_address,)
        column_name = 'label'
        cached_label = self.database_connector.fetch_query_single_str(
            stmt, arglist, caller, column_name)
        
        self.assertIsNotNone(cached_label)
        self.assertEqual(cached_label, updated_label)
        
        address_reuse.db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK = old_val
        
    def do_update_blame_record_with_batching(self, blame_record):
        old_val = address_reuse.db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK
        address_reuse.db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK = True
        
        blame_record.blame_label = 'DB_DEFERRED_BLAME_PLACEHOLDER'
        new_id = self.database_connector.get_blame_id_for_label_and_insert_if_new(
            'DB_DEFERRED_BLAME_PLACEHOLDER')
        
        #simulate call to db.add_blame_record()
        stmt = ('INSERT INTO ' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ''
                ' (blame_recipient_id, address_reuse_type, role, data_source, '
                'block_height, confirmed_tx_id, relevant_address) VALUES '
                '(?, ?, ? , ?, ?, ?, ?)')
        blame_recipient_id  = new_id    #I expect this is 1, but not important
                                        #  to this test
        address_reuse_type  = blame_record.address_reuse_type
        role                = blame_record.address_reuse_role
        data_source         = blame_record.data_source
        block_height        = blame_record.block_height
        confirmed_tx_id     = blame_record.tx_id
        relevant_address    = blame_record.relevant_address
        arglist = (blame_recipient_id, address_reuse_type, role, data_source, 
                   block_height, confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        
        #simulate call to db.cache_blame_label_for_btc_address()
        stmt = ('INSERT INTO ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE + ' '
                '(btc_address, label) VALUES (?,?)')
        original_label = blame_record.blame_label
        arglist = (relevant_address, original_label)
        self.database_connector.run_statement(stmt, arglist)
        
        blame_record.blame_label = 'RESERVED_NEW_BLAME_LABEL'
        blame_record.row_id = 1 #the rowid of the row we inserted
        self.database_connector.update_blame_record(blame_record)
        
        #ensure that nothing has been updated, but rather records are sitting
        #   in memory cache waiting to be batch committed.
        self.assertEqual(
            len(self.database_connector.in_memory_updated_blame_record_cache), 
            1)
        self.assertEqual(
            len(self.database_connector.in_memory_update_blame_label_cache_cache), 
            1)
        
        stmt = 'SELECT * FROM ' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS
        arglist = []
        caller = 'do_update_blame_record_with_batching'
        rows = self.database_connector.fetch_query_and_handle_errors(stmt, 
                                                                     arglist, 
                                                                     caller)
        self.assertEqual(len(rows), 1)
        #The blame label hasn't been updated yet
        self.assertEqual(rows[0]['blame_recipient_id'], new_id)
        
        stmt = 'SELECT * FROM ' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE
        arglist = []
        caller = 'do_update_blame_record_with_batching'
        rows = self.database_connector.fetch_query_and_handle_errors(stmt, 
                                                                     arglist, 
                                                                     caller)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['label'], original_label)
        
        #push commits to database
        self.database_connector.write_deferred_blame_record_resolutions()
        
        #caches should be empty now
        self.assertEqual(
            len(self.database_connector.in_memory_updated_blame_record_cache), 
            0)
        self.assertEqual(
            len(self.database_connector.in_memory_update_blame_label_cache_cache), 
            0)
        
        #ensure that blame label was updated
        stmt = ('SELECT blame_recipient_id FROM '
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ''
                ' WHERE rowid = ?')
        arglist = (1,) #the rowid of the row we inserted
        
        updated_blame_id = self.database_connector.fetch_query_single_int(
            stmt, arglist, caller, 'blame_recipient_id')
        self.assertNotEqual(updated_blame_id, 1, 
                            "New blame ID should not be 1: %d" % 
                                updated_blame_id)
        updated_label = self.database_connector.get_blame_label_for_blame_id(
            updated_blame_id)
        self.assertEqual(blame_record.blame_label, updated_label, 
                         'Blame label was not updated.')
        
        #ensure that blame label was also updated in blame label cache
        stmt = ('SELECT label FROM ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE + ' '
                'WHERE btc_address = ?')
        arglist = (relevant_address,)
        column_name = 'label'
        cached_label = self.database_connector.fetch_query_single_str(
            stmt, arglist, caller, column_name)
        
        self.assertIsNotNone(cached_label)
        self.assertEqual(cached_label, updated_label)
        
        address_reuse.db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK = old_val
    
    def test_update_blame_record_for_block_height_170_and_no_batching(self):
        blame_label = None
        address_reuse_role = address_reuse.db.AddressReuseRole.SENDER
        address_reuse_type = address_reuse.db.AddressReuseType.SENDBACK
        data_source = address_reuse.db.DataSource.WALLET_EXPLORER
        block_height = 170
        tx_id = 'f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16'
        relevant_address = '12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S'
        
        blame_record = address_reuse.tx_blame.BlameRecord(
            blame_label, address_reuse_role, data_source, tx_id = tx_id, 
            address_reuse_type = address_reuse_type, 
            relevant_address = relevant_address, block_height = block_height)
        self.do_update_blame_record_without_batching(blame_record)
        
    def test_update_blame_record_for_block_height_170_and_batching(self):
        blame_label = None
        address_reuse_role = address_reuse.db.AddressReuseRole.SENDER
        address_reuse_type = address_reuse.db.AddressReuseType.SENDBACK
        data_source = address_reuse.db.DataSource.WALLET_EXPLORER
        block_height = 170
        tx_id = 'f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16'
        relevant_address = '12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S'
        
        blame_record = address_reuse.tx_blame.BlameRecord(
            blame_label, address_reuse_role, data_source, tx_id = tx_id, 
            address_reuse_type = address_reuse_type, 
            relevant_address = relevant_address, block_height = block_height)
        self.do_update_blame_record_with_batching(blame_record)
    
    def test_get_blame_id_for_deferred_blame_placeholder(self):
        stmt = ('INSERT INTO ' + address_reuse.db.SQL_TABLE_NAME_BLAME_IDS + ''
                ' (label) VALUES (?)')
        arglist = ('FOO',)
        self.database_connector.run_statement(stmt, arglist)
        
        arglist = (DB_DEFERRED_BLAME_PLACEHOLDER,)
        self.database_connector.run_statement(stmt, arglist)
        
        arglist = ('BAR',)
        self.database_connector.run_statement(stmt, arglist)
        
        blame_id = self.database_connector.get_blame_id_for_deferred_blame_placeholder()
        self.assertEqual(blame_id, 2, 
                         ("Expected placeholder at rowid 2 but instead saw it "
                          "at: %d") % blame_id)
    
    def test_get_blame_records_for_blame_id(self):
        blame_label = 'GET_BLAME_RECORDS_FOR_ID_TEST'
        
        #create ID for test label
        blame_recipient_id = self.database_connector.get_blame_id_for_label_and_insert_if_new(
            blame_label)
        address_reuse_type = address_reuse.db.AddressReuseType.SENDBACK
        role = address_reuse.db.AddressReuseRole.SENDER
        data_source = address_reuse.db.DataSource.BLOCKCHAIN_INFO
        block_height = 170
        confirmed_tx_id = ('f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338'
                           '530e9831e9e16')
        relevant_address = '12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S'
        stmt = ('INSERT INTO ' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ''
                '(blame_recipient_id, address_reuse_type, role, data_source, '
                'block_height, confirmed_tx_id, relevant_address) VALUES '
                '(?, ?, ?, ?, ?, ?, ?)')
        arglist = (blame_recipient_id, address_reuse_type, role, data_source, 
                   block_height, confirmed_tx_id, relevant_address)
        
        #insert one record
        self.database_connector.run_statement(stmt, arglist)
        
        #insert a second record
        role = address_reuse.db.AddressReuseRole.RECEIVER
        arglist = (blame_recipient_id, address_reuse_type, role, data_source, 
                   block_height, confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        
        blame_records = self.database_connector.get_blame_records_for_blame_id(
            blame_recipient_id)
        self.assertEqual(len(blame_records), 2, 
                         ('Expected 2 blame records, received %d' % 
                            len(blame_records)))
        self.assertEqual(blame_records[0].blame_label, blame_label, 
                         ('Expected blame label %s received %s' % 
                          (blame_label, blame_records[0].blame_label)))
        self.assertEqual(blame_records[1].blame_label, blame_label, 
                         ('Expected blame label %s received %s' % 
                          (blame_label, blame_records[1].blame_label)))
        self.assertEqual(blame_records[0].address_reuse_role, 
                         address_reuse.db.AddressReuseRole.SENDER, 
                         'Wrong address reuse role.')
        self.assertEqual(blame_records[1].address_reuse_role, 
                         address_reuse.db.AddressReuseRole.RECEIVER, 
                         'Wrong address reuse role.')
        self.assertEqual(blame_records[0].row_id, 1, 'Expected rowid of 1')
        self.assertEqual(blame_records[1].row_id, 2, 'Expected rowid of 2')
        self.assertEqual(blame_records[0].tx_id, confirmed_tx_id, 
                         'tx ID doesn not match: %s' % blame_records[0].tx_id)
        self.assertEqual(blame_records[1].tx_id, confirmed_tx_id, 
                         'tx ID doesn not match: %s' % blame_records[1].tx_id)
        self.assertEqual(blame_records[0].address_reuse_type, 
                         address_reuse_type, 'Wrong address reuse type.')
        self.assertEqual(blame_records[1].address_reuse_type, 
                         address_reuse_type, 'Wrong address reuse type.')
        self.assertEqual(blame_records[0].relevant_address, 
                         relevant_address, 'Wrong address.')
        self.assertEqual(blame_records[1].relevant_address, 
                         relevant_address, 'Wrong address.')
        self.assertEqual(blame_records[0].block_height, block_height, 
                         ('Wrong block height: %d' % 
                            blame_records[0].block_height))
        self.assertEqual(blame_records[1].block_height, block_height, 
                         ('Wrong block height: %d' % 
                            blame_records[1].block_height))
        
        #test optional block_height parameter by inserting a record for
        #   a different block height.
        block_height = 171
        arglist = (blame_recipient_id, address_reuse_type, role, data_source, 
                   block_height, confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        blame_records = self.database_connector.get_blame_records_for_blame_id(
            blame_recipient_id, block_height = 170)
        
        self.assertEqual(len(blame_records), 2, 
                         ('Expected 2 blame records, received %d' % 
                            len(blame_records)))
        
    
    def test_delete_blame_record_with_batching(self):
        orig_val = address_reuse.db.DELETE_BLAME_STATS_ONCE_PER_BLOCK
        address_reuse.db.DELETE_BLAME_STATS_ONCE_PER_BLOCK = True
        
        blame_label = 'DELETE_BLAME_RECORD_TEST'
        
        #create ID for test label
        blame_recipient_id = self.database_connector.get_blame_id_for_label_and_insert_if_new(
            blame_label)
        address_reuse_type = address_reuse.db.AddressReuseType.SENDBACK
        role = address_reuse.db.AddressReuseRole.SENDER
        data_source = address_reuse.db.DataSource.BLOCKCHAIN_INFO
        block_height = 170
        confirmed_tx_id = ('f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338'
                           '530e9831e9e16')
        relevant_address = '12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S'
        stmt = ('INSERT INTO ' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ''
                '(blame_recipient_id, address_reuse_type, role, data_source, '
                'block_height, confirmed_tx_id, relevant_address) VALUES '
                '(?, ?, ?, ?, ?, ?, ?)')
        arglist = (blame_recipient_id, address_reuse_type, role, data_source, 
                   block_height, confirmed_tx_id, relevant_address)
        
        #insert one record
        self.database_connector.run_statement(stmt, arglist)
        
        self.database_connector.delete_blame_record(1)
        blame_records = self.database_connector.get_blame_records_for_blame_id(
            blame_recipient_id)
        
        #not yet commited as a batch
        self.assertEqual(len(blame_records), 1)
        
        #one delete item in cache
        self.assertEqual(
            len(self.database_connector.in_memory_deleted_blame_record_cache), 
            1)
        
        self.database_connector.write_deferred_blame_record_resolutions()
        
        blame_records = self.database_connector.get_blame_records_for_blame_id(
            blame_recipient_id)
        self.assertEqual(len(blame_records), 0)
        
        self.assertEqual(
            len(self.database_connector.in_memory_deleted_blame_record_cache), 
            0)
        
        address_reuse.db.DELETE_BLAME_STATS_ONCE_PER_BLOCK = orig_val

    def test_delete_blame_record_without_batching(self):
        orig_val = address_reuse.db.DELETE_BLAME_STATS_ONCE_PER_BLOCK
        address_reuse.db.DELETE_BLAME_STATS_ONCE_PER_BLOCK = False
        
        blame_label = 'DELETE_BLAME_RECORD_TEST'
        
        #create ID for test label
        blame_recipient_id = self.database_connector.get_blame_id_for_label_and_insert_if_new(
            blame_label)
        address_reuse_type = address_reuse.db.AddressReuseType.SENDBACK
        role = address_reuse.db.AddressReuseRole.SENDER
        data_source = address_reuse.db.DataSource.BLOCKCHAIN_INFO
        block_height = 170
        confirmed_tx_id = ('f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338'
                           '530e9831e9e16')
        relevant_address = '12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S'
        stmt = ('INSERT INTO ' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ''
                '(blame_recipient_id, address_reuse_type, role, data_source, '
                'block_height, confirmed_tx_id, relevant_address) VALUES '
                '(?, ?, ?, ?, ?, ?, ?)')
        arglist = (blame_recipient_id, address_reuse_type, role, data_source, 
                   block_height, confirmed_tx_id, relevant_address)

        #insert one record
        self.database_connector.run_statement(stmt, arglist)
        
        self.database_connector.delete_blame_record(1)
        blame_records = self.database_connector.get_blame_records_for_blame_id(
            blame_recipient_id)
        self.assertEqual(blame_records, [], 
                         "Expected empty list after deleting record.")
        
        address_reuse.db.DELETE_BLAME_STATS_ONCE_PER_BLOCK = orig_val
    
    #Ensure that Python memory caching works when 
    #   db.INSERT_BLAME_STATS_ONCE_PER_BLOCK flag is set to True
    def test_write_stored_blame(self):
        blame_label1 = 'WRITE_STORED_BLAME_TEST_1'
        blame_label2 = 'WRITE_STORED_BLAME_TEST_2'
        
        blame_recipient_id1 = self.database_connector.get_blame_id_for_label_and_insert_if_new(
            blame_label1)
        blame_recipient_id2 = self.database_connector.get_blame_id_for_label_and_insert_if_new(
            blame_label1)
        address_reuse_type = address_reuse.db.AddressReuseType.SENDBACK
        role = address_reuse.db.AddressReuseRole.SENDER
        data_source = address_reuse.db.DataSource.BLOCKCHAIN_INFO
        block_height = 170
        confirmed_tx_id1 = ('b1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c22'
                            '0bfa78111c5082')
        confirmed_tx_id2 = ('f4184fc596403b9d638783cf57adfe4c75c605f6356fbc9133'
                            '8530e9831e9e16')
        
        #not actually address reuse, just for sake of test
        relevant_address1 = '1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc'

        relevant_address2 = '12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S'
        
        #Set INSERT_BLAME_STATS_ONCE_PER_BLOCK flag
        self.database_connector.INSERT_BLAME_STATS_ONCE_PER_BLOCK = True
        
        self.database_connector.store_blame(blame_label1, address_reuse_type, 
                                            role, data_source, block_height, 
                                            confirmed_tx_id1, relevant_address1)
        self.database_connector.store_blame(blame_label1, address_reuse_type, 
                                            role, data_source, block_height, 
                                            confirmed_tx_id1, relevant_address2)
        
        #verify no records written to database yet
        stmt = ('SELECT 1 AS one FROM ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ' '
                'LIMIT 1')
        result = self.database_connector.fetch_query_single_int(stmt, [], 
                                                       'test_write_stored_blame', 
                                                       'one')
        self.assertIsNone(result)
        
        self.database_connector.write_stored_blame()
        stmt = ('SELECT * FROM ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + '')
        result = self.database_connector.fetch_query_and_handle_errors(stmt, [], 
                                                       'test_write_stored_blame')
        self.assertEqual(len(result), 2)
        
    #Ensure that, if the db has to create multiple batches of INSERTs due to
    #   SQLite limits on compound SELECT statements, we still end up with the
    #   correct number of records inserted.
    def test_write_stored_blame_with_more_than_one_batch(self):
        NUM_RECORDS_TO_CREATE = address_reuse.db.SQLITE_MAX_COMPOUND_SELECT + 3
        for i in range(0, NUM_RECORDS_TO_CREATE):
            blame_label = 'blame' + str(i)
            address_reuse_type = address_reuse.db.AddressReuseType.SENDBACK
            role = address_reuse.db.AddressReuseRole.SENDER
            data_source = address_reuse.db.DataSource.BLOCKCHAIN_INFO
            block_height = i
            confirmed_tx_id = 'tx' + str(i)
            relevant_address = '1abcd' + str(i)
            self.database_connector.store_blame(blame_label, 
                                                address_reuse_type, role, 
                                                data_source, block_height, 
                                                confirmed_tx_id, 
                                                relevant_address)
        
        #verify no records written to database yet
        stmt = ('SELECT 1 AS one FROM ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ' '
                'LIMIT 1')
        result = self.database_connector.fetch_query_single_int(
            stmt, [], 'test_write_stored_blame_with_more_than_one_batch', 'one')
        self.assertIsNone(result)
        
        self.database_connector.write_stored_blame()
        stmt = ('SELECT * FROM ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + '')
        result = self.database_connector.fetch_query_and_handle_errors(
            stmt, [], 'test_write_stored_blame_with_more_than_one_batch')
        self.assertEqual(len(result), NUM_RECORDS_TO_CREATE)
        
    def test_get_lowest_block_height_with_deferred_records(self):
        lowest = self.database_connector.get_lowest_block_height_with_deferred_records()
        self.assertIsNone(lowest)
        
        def_id = self.database_connector.get_blame_id_for_label_and_insert_if_new(
            DB_DEFERRED_BLAME_PLACEHOLDER)
        
        stmt = ('INSERT INTO ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ' '
                '(blame_recipient_id, address_reuse_type, role, data_source, '
                'block_height, confirmed_tx_id, relevant_address) VALUES '
                '(?,?,?,?,?,?,?)')
        
        address_reuse_type = address_reuse.db.AddressReuseType.SENDBACK
        role = address_reuse.db.AddressReuseRole.SENDER
        data_source = address_reuse.db.DataSource.BLOCKCHAIN_INFO
        block_height = 170
        confirmed_tx_id = ('b1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        #not actually address reuse, just for sake of test
        relevant_address = '1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc'
        
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        
        lowest = self.database_connector.get_lowest_block_height_with_deferred_records()
        self.assertEqual(lowest, 170)
        
        block_height = 171
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        lowest = self.database_connector.get_lowest_block_height_with_deferred_records()
        self.assertEqual(lowest, 170)
        
        block_height = 169
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        lowest = self.database_connector.get_lowest_block_height_with_deferred_records()
        self.assertEqual(lowest, 169)
        
    def test_get_all_deferred_blame_records_at_height_with_no_records(self):
        #test: there are no deferred records in the db
        records = self.database_connector.get_all_deferred_blame_records_at_height(1)
        self.assertEqual(len(records), 0)
    
    def test_get_all_deferred_blame_records_at_height_with_cache_already_complete(self):
        #Test what happens when all of the records needed for requested block 
        #   height are already in the cache, and there are no more records to 
        #   get from the cache.
        
        #override this value with smaller to make tests a lot faster
        address_reuse.db.FETCH_N_DEFERRED_RECORDS_IN_BATCH = 2
        
        def_id = self.database_connector.get_blame_id_for_label_and_insert_if_new(
            DB_DEFERRED_BLAME_PLACEHOLDER)
        
        stmt = ('INSERT INTO ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ' '
                '(blame_recipient_id, address_reuse_type, role, data_source, '
                'block_height, confirmed_tx_id, relevant_address) VALUES '
                '(?,?,?,?,?,?,?)')
        
        address_reuse_type = address_reuse.db.AddressReuseType.SENDBACK
        role = address_reuse.db.AddressReuseRole.SENDER
        data_source = address_reuse.db.DataSource.BLOCKCHAIN_INFO
        block_height = 170
        confirmed_tx_id = ('b1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        #not actually address reuse, just for sake of test
        relevant_address = '1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc'
        
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        
        #pre-fill cache
        self.database_connector.fetch_more_deferred_records_for_cache(def_id)
        cache_size = len(self.database_connector.in_memory_deferred_record_cache)
        self.assertEqual(cache_size, 1)
        
        records = self.database_connector.get_all_deferred_blame_records_at_height(170)
        
        new_cache_size = len(self.database_connector.in_memory_deferred_record_cache)
        self.assertEqual(new_cache_size, 0)
        
        #one record should have been returned
        self.assertEqual(len(records), 1)
        
        #verify the attributes of the record returned
        self.assertEqual(records[0].blame_label, DB_DEFERRED_BLAME_PLACEHOLDER)
        self.assertEqual(records[0].address_reuse_role, role)
        self.assertEqual(records[0].data_source, data_source)
        self.assertEqual(records[0].row_id, 1)
        self.assertEqual(records[0].tx_id, confirmed_tx_id)
        self.assertEqual(records[0].address_reuse_type, address_reuse_type)
        self.assertEqual(records[0].relevant_address, relevant_address)
        self.assertEqual(records[0].block_height, block_height)
    
    def test_get_all_deferred_blame_records_at_height_with_cache_incomplete(self):
        #some of the records needed in the cache are in there, but not all.
        #   There will be 3 records total for the block height, but only 2
        #   in the first fetch.
        
        #override this value with smaller to make tests a lot faster
        address_reuse.db.FETCH_N_DEFERRED_RECORDS_IN_BATCH = 2
        
        def_id = self.database_connector.get_blame_id_for_label_and_insert_if_new(
            DB_DEFERRED_BLAME_PLACEHOLDER)
        
        stmt = ('INSERT INTO ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ' '
                '(blame_recipient_id, address_reuse_type, role, data_source, '
                'block_height, confirmed_tx_id, relevant_address) VALUES '
                '(?,?,?,?,?,?,?)')
        
        address_reuse_type = address_reuse.db.AddressReuseType.SENDBACK
        role = address_reuse.db.AddressReuseRole.SENDER
        data_source = address_reuse.db.DataSource.BLOCKCHAIN_INFO
        block_height = 170
        confirmed_tx_id = ('b1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        #not actually address reuse, just for sake of test
        relevant_address = '1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc'
        
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        
        confirmed_tx_id = ('c1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        confirmed_tx_id = ('d1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        
        #pre-fill cache with only 2 records
        self.database_connector.fetch_more_deferred_records_for_cache(def_id)
        cache_size = len(self.database_connector.in_memory_deferred_record_cache)
        self.assertEqual(cache_size, 2)
        
        records = self.database_connector.get_all_deferred_blame_records_at_height(170)
        
        new_cache_size = len(self.database_connector.in_memory_deferred_record_cache)
        self.assertEqual(new_cache_size, 0)
        
        #three records should have been returned
        self.assertEqual(len(records), 3)
        
        #verify the attributes of the last record returned
        self.assertEqual(records[2].blame_label, DB_DEFERRED_BLAME_PLACEHOLDER)
        self.assertEqual(records[2].address_reuse_role, role)
        self.assertEqual(records[2].data_source, data_source)
        self.assertEqual(records[2].row_id, 3)
        self.assertEqual(records[2].tx_id, confirmed_tx_id)
        self.assertEqual(records[2].address_reuse_type, address_reuse_type)
        self.assertEqual(records[2].relevant_address, relevant_address)
        self.assertEqual(records[2].block_height, block_height)
        
    def test_get_all_deferred_blame_records_at_height_with_cache_incomplete(self):
        #Initially, there will be no items in the cache. The function will have
        #   to make multiple passes to get all 3 records requested, since it
        #   will only gather in batches of 2.
        
        #override this value with smaller to make tests a lot faster
        address_reuse.db.FETCH_N_DEFERRED_RECORDS_IN_BATCH = 2
        
        def_id = self.database_connector.get_blame_id_for_label_and_insert_if_new(
            DB_DEFERRED_BLAME_PLACEHOLDER)
        
        stmt = ('INSERT INTO ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ' '
                '(blame_recipient_id, address_reuse_type, role, data_source, '
                'block_height, confirmed_tx_id, relevant_address) VALUES '
                '(?,?,?,?,?,?,?)')
        
        address_reuse_type = address_reuse.db.AddressReuseType.SENDBACK
        role = address_reuse.db.AddressReuseRole.SENDER
        data_source = address_reuse.db.DataSource.BLOCKCHAIN_INFO
        block_height = 170
        confirmed_tx_id = ('b1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        #not actually address reuse, just for sake of test
        relevant_address = '1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc'
        
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        
        confirmed_tx_id = ('c1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        confirmed_tx_id = ('d1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        
        records = self.database_connector.get_all_deferred_blame_records_at_height(170)
        
        new_cache_size = len(self.database_connector.in_memory_deferred_record_cache)
        self.assertEqual(new_cache_size, 0)
        
        #three records should have been returned
        self.assertEqual(len(records), 3)
        
        #verify the attributes of the last record returned
        self.assertEqual(records[2].blame_label, DB_DEFERRED_BLAME_PLACEHOLDER)
        self.assertEqual(records[2].address_reuse_role, role)
        self.assertEqual(records[2].data_source, data_source)
        self.assertEqual(records[2].row_id, 3)
        self.assertEqual(records[2].tx_id, confirmed_tx_id)
        self.assertEqual(records[2].address_reuse_type, address_reuse_type)
        self.assertEqual(records[2].relevant_address, relevant_address)
        self.assertEqual(records[2].block_height, block_height)
        
    def test_get_all_deferred_blame_records_at_height_with_records_leftover_in_cache(self):
        #Test what happens when there are leftovers in the cache for the next
        #   block height. Function ought to only fetch the relevant records
        #   for this height and leave the rest in the cache.
        
        #override this value with smaller to make tests a lot faster
        address_reuse.db.FETCH_N_DEFERRED_RECORDS_IN_BATCH = 2
        
        def_id = self.database_connector.get_blame_id_for_label_and_insert_if_new(
            DB_DEFERRED_BLAME_PLACEHOLDER)
        
        stmt = ('INSERT INTO ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ' '
                '(blame_recipient_id, address_reuse_type, role, data_source, '
                'block_height, confirmed_tx_id, relevant_address) VALUES '
                '(?,?,?,?,?,?,?)')
        
        address_reuse_type = address_reuse.db.AddressReuseType.SENDBACK
        role = address_reuse.db.AddressReuseRole.SENDER
        data_source = address_reuse.db.DataSource.BLOCKCHAIN_INFO
        block_height = 170
        confirmed_tx_id = ('b1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        #not actually address reuse, just for sake of test
        relevant_address = '1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc'
        
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        
        confirmed_tx_id = ('c1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        confirmed_tx_id = ('d1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        block_height = 1000
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        
        records = self.database_connector.get_all_deferred_blame_records_at_height(170)
        
        #should be one leftover record in cache
        new_cache_size = len(self.database_connector.in_memory_deferred_record_cache)
        self.assertEqual(new_cache_size, 1)
        
        #three records should have been returned
        self.assertEqual(len(records), 3)
        
        #verify the attributes of the last record returned
        self.assertEqual(records[2].blame_label, DB_DEFERRED_BLAME_PLACEHOLDER)
        self.assertEqual(records[2].address_reuse_role, role)
        self.assertEqual(records[2].data_source, data_source)
        self.assertEqual(records[2].row_id, 3)
        self.assertEqual(records[2].tx_id, confirmed_tx_id)
        self.assertEqual(records[2].address_reuse_type, address_reuse_type)
        self.assertEqual(records[2].relevant_address, relevant_address)
        self.assertEqual(records[2].block_height, 170)
        
        records = self.database_connector.get_all_deferred_blame_records_at_height(171)
        self.assertEqual(len(records), 0)
        
        records = self.database_connector.get_all_deferred_blame_records_at_height(1000)
        self.assertEqual(len(records), 1)
        
        #verify the attributes of the last record returned
        self.assertEqual(records[0].blame_label, DB_DEFERRED_BLAME_PLACEHOLDER)
        self.assertEqual(records[0].address_reuse_role, role)
        self.assertEqual(records[0].data_source, data_source)
        self.assertEqual(records[0].row_id, 4)
        self.assertEqual(records[0].tx_id, confirmed_tx_id)
        self.assertEqual(records[0].address_reuse_type, address_reuse_type)
        self.assertEqual(records[0].relevant_address, relevant_address)
        self.assertEqual(records[0].block_height, 1000)
    
    def test_cache_blame_label_for_btc_address_when_not_already_present(self):
        btc_address = '1Q6YQHqjC1d6AkPieGgBHwwkCx2ZtcWVQC'
        label = 'MtGoxAndOthers'
        self.database_connector.cache_blame_label_for_btc_address(btc_address, 
                                                                  label)
        stmt = ('SELECT * FROM '
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE)
        arglist = []
        caller = ('test_cache_blame_label_for_btc_address_when_not_already_'
                  'present')
        rows = self.database_connector.fetch_query_and_handle_errors(stmt, 
                                                                     arglist, 
                                                                     caller)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['btc_address'], btc_address)
        self.assertEqual(rows[0]['label'], label)
    
    def test_cache_blame_label_for_btc_address_when_blame_previously_deferred(self):
        btc_address = '1Q6YQHqjC1d6AkPieGgBHwwkCx2ZtcWVQC'
        label = 'MtGoxAndOthers'
        stmt = ('INSERT INTO '
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE + ' '
                '(btc_address, label) VALUES (?,?)')
        arglist = (btc_address, DB_DEFERRED_BLAME_PLACEHOLDER)
        self.database_connector.run_statement(stmt, arglist)
        
        stmt = ('SELECT * FROM '
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE)
        arglist = []
        caller = ('test_cache_blame_label_for_btc_address_when_not_already_'
                  'present')
        rows = self.database_connector.fetch_query_and_handle_errors(stmt, 
                                                                     arglist, 
                                                                     caller)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['btc_address'], btc_address)
        self.assertEqual(rows[0]['label'], DB_DEFERRED_BLAME_PLACEHOLDER)
        
        self.database_connector.cache_blame_label_for_btc_address(btc_address, 
                                                                  label)
        
        stmt = ('SELECT * FROM '
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE)
        arglist = []
        caller = ('test_cache_blame_label_for_btc_address_when_not_already_'
                  'present')
        rows = self.database_connector.fetch_query_and_handle_errors(stmt, 
                                                                     arglist, 
                                                                     caller)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['btc_address'], btc_address)
        self.assertEqual(rows[0]['label'], label)
    
    def test_update_blame_label_for_btc_address_with_no_batching(self):
        init_val = address_reuse.db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK
        address_reuse.db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK = False
        
        def_id = self.database_connector.get_blame_id_for_label_and_insert_if_new(
            DB_DEFERRED_BLAME_PLACEHOLDER)
        
        label = DB_DEFERRED_BLAME_PLACEHOLDER
        btc_address = '1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc'
        
        stmt = ('INSERT INTO ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE + ' '
                '(btc_address, label) VALUES (?,?)')
        
        arglist = (btc_address, label)
        self.database_connector.run_statement(stmt, arglist)
        
        new_label = 'MY_NEW_LABEL'
        self.database_connector.update_blame_label_for_btc_address(
            btc_address, new_label)
        
        stmt = 'SELECT * FROM ' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE
        arglist = []
        caller = 'test_update_blame_label_for_btc_address_with_no_batching'
        rows = self.database_connector.fetch_query_and_handle_errors(stmt, 
                                                                     arglist, 
                                                                     caller)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['btc_address'], btc_address)
        self.assertEqual(rows[0]['label'], new_label)
        self.assertEqual(
            len(self.database_connector.in_memory_update_blame_label_cache_cache),
            0) #no use of cache when batching turned off
        
        address_reuse.db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK = init_val
        
    def test_update_blame_label_for_btc_address_with_batching(self):
        init_val = address_reuse.db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK
        address_reuse.db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK = True
        
        def_id = self.database_connector.get_blame_id_for_label_and_insert_if_new(
            DB_DEFERRED_BLAME_PLACEHOLDER)
        
        label = DB_DEFERRED_BLAME_PLACEHOLDER
        btc_address = '1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc'
        
        stmt = ('INSERT INTO ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE + ' '
                '(btc_address, label) VALUES (?,?)')
        
        arglist = (btc_address, label)
        self.database_connector.run_statement(stmt, arglist)
        
        new_label = 'MY_NEW_LABEL'
        self.database_connector.update_blame_label_for_btc_address(
            btc_address, new_label)
        
        stmt = 'SELECT * FROM ' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE
        arglist = []
        caller = 'test_update_blame_label_for_btc_address_with_no_batching'
        rows = self.database_connector.fetch_query_and_handle_errors(stmt, 
                                                                     arglist, 
                                                                     caller)
        self.assertEqual(len(rows), 1)
        #label shouldn't be updated yet
        self.assertEqual(rows[0]['label'], label)
        self.assertEqual(
            len(self.database_connector.in_memory_update_blame_label_cache_cache),
            1) #there should be one item in the cache
        
        self.database_connector.write_deferred_blame_record_resolutions()
        
        rows = self.database_connector.fetch_query_and_handle_errors(stmt, 
                                                                     arglist, 
                                                                     caller)

        self.assertEqual(
            len(self.database_connector.in_memory_update_blame_label_cache_cache),
            0) #cache should be empty now

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['btc_address'], btc_address)
        self.assertEqual(rows[0]['label'], new_label)
        
        address_reuse.db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK = init_val
        
    def test_fetch_more_deferred_records_for_cache(self):
        stmt = (('INSERT INTO %s (blame_recipient_id, address_reuse_type, role,'
                ' data_source, block_height, confirmed_tx_id, relevant_address)'
                ' VALUES (?,?,?,?,?,?,?)') % 
                address_reuse.db.SQL_TABLE_NAME_BLAME_STATS)
        blame_recipient_id = 1
        address_reuse_type = 1
        role = 1
        data_source = 1
        block_height = 1
        confirmed_tx_id = ('0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a'
                           '74b1efd512098')
        relevant_address = '12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX'
        arglist = (blame_recipient_id, address_reuse_type, role, data_source,
                  block_height, confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        
        #what hapepns when the min block height is above all records? expected
        #   result: 0 records fetched
        with self.assertRaises(address_reuse.custom_errors.NoDeferredRecordsRemaining):
            self.database_connector.fetch_more_deferred_records_for_cache(
                deferred_id=1, min_block_height=2)
            self.assertEqual(
                len(self.database_connector.in_memory_deferred_record_cache),
                0)
        
        #if min block height is at the level of the only record? expected
        #   result: 1 record fetched
        self.database_connector.fetch_more_deferred_records_for_cache(
            deferred_id=1, min_block_height=1)
        self.assertEqual(len(self.database_connector.in_memory_deferred_record_cache),
                         1)
        
    def test_is_deferred_record_at_height(self):
        res = self.database_connector.is_deferred_record_at_height(2)
        self.assertFalse(res)
        
        def_id = self.database_connector.get_blame_id_for_label_and_insert_if_new(
            DB_DEFERRED_BLAME_PLACEHOLDER)
        
        stmt = ('INSERT INTO ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ' '
                '(blame_recipient_id, address_reuse_type, role, data_source, '
                'block_height, confirmed_tx_id, relevant_address) VALUES '
                '(?,?,?,?,?,?,?)')
        
        address_reuse_type = address_reuse.db.AddressReuseType.SENDBACK
        role = address_reuse.db.AddressReuseRole.SENDER
        data_source = address_reuse.db.DataSource.BLOCKCHAIN_INFO
        block_height = 170
        confirmed_tx_id = ('b1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        #not actually address reuse, just for sake of test
        relevant_address = '1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc'
        
        arglist = (def_id, address_reuse_type, role, data_source, block_height, 
                   confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        
        res = self.database_connector.is_deferred_record_at_height(170)
        self.assertTrue(res)
        
        address_reuse_type = address_reuse.db.AddressReuseType.SENDBACK
        role = address_reuse.db.AddressReuseRole.SENDER
        data_source = address_reuse.db.DataSource.BLOCKCHAIN_INFO
        block_height = 200
        confirmed_tx_id = ('b1fea52486ce0c62bb442b530a3f0132b826c74e473d1f2c220'
                           'bfa78111c5082')
        #not actually address reuse, just for sake of test
        relevant_address = '1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc'
        
        blame_id_no_deferred = 2
        
        arglist = (blame_id_no_deferred, address_reuse_type, role, data_source, 
                   block_height, confirmed_tx_id, relevant_address)
        self.database_connector.run_statement(stmt, arglist)
        
        res = self.database_connector.is_deferred_record_at_height(200)
        self.assertFalse(res)
        

class BlameResolverCoordinationDatabaseTestCase(unittest.TestCase):
    
    def setUp(self):
        try:
            os.remove(TEMP_COORD_DB_FILENAME)
        except OSError:
            pass
        self.coord_db = address_reuse.db.BlameResolverCoordinationDatabase(
            filename_override=TEMP_COORD_DB_FILENAME)
        
    def tearDown(self):
        self.coord_db.close()
    
    def test_run_statement_single_stmt_and_fetch_single_row(self):
        #self, stmt, arglist, execute_many = False):
        block_height = 42
        pid = '1337'
        completed = 1
        stmt = ('INSERT INTO '
                '' + address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER + ' '
                '(block_height, pid_of_claimer, completed) VALUES (?,?,?)')
        arglist = (block_height, pid, completed)
        self.coord_db.run_statement(stmt, arglist)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        caller = 'test_run_statement_single_stmt'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['block_height'], block_height)
        self.assertEqual(res[0]['pid_of_claimer'], int(pid))
        self.assertEqual(res[0]['completed'], completed)

    def test_run_statement_exec_many_and_fetch_multiple_rows(self):
        block_height_1 = 42
        block_height_2 = 24
        pid = '1337'
        completed_1 = 1
        completed_2 = 0
        
        stmt = ('INSERT INTO '
                '' + address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER + ' '
                '(block_height, pid_of_claimer, completed) VALUES (?,?,?)')
        arglist_1 = (block_height_1, pid, completed_1)
        arglist_2 = (block_height_2, pid, completed_2)
        arglist = []
        arglist.append(arglist_1)
        arglist.append(arglist_2)
        self.coord_db.run_statement(stmt, arglist, execute_many=True)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        caller = 'test_run_statement_single_stmt'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]['block_height'], block_height_1)
        self.assertEqual(res[0]['pid_of_claimer'], int(pid))
        self.assertEqual(res[0]['completed'], completed_1)
        self.assertEqual(res[1]['block_height'], block_height_2)
        self.assertEqual(res[1]['pid_of_claimer'], int(pid))
        self.assertEqual(res[1]['completed'], completed_2)
        
    def test_run_invalid_statement(self):
        orig = address_reuse.db.NUM_ATTEMPTS_UPON_DB_ERROR
        address_reuse.db.NUM_ATTEMPTS_UPON_DB_ERROR = 1

        stmt = 'INSERT INTO tblDoesntExist (kunfoo) VALUES (panda)'
        arglist = []
        with self.assertRaises(address_reuse.custom_errors.TooManyDatabaseErrors):
            self.coord_db.run_statement(stmt, arglist)

        address_reuse.db.NUM_ATTEMPTS_UPON_DB_ERROR = orig
    
    def test_fetch_empty_result(self):
        stmt = ('SELECT * FROM %s WHERE 1=2' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        caller = 'test_fetch_empty_result'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        self.assertEqual(len(res), 0)
    
    def test_fetch_invalid_query(self):
        orig = address_reuse.db.NUM_ATTEMPTS_UPON_DB_ERROR
        address_reuse.db.NUM_ATTEMPTS_UPON_DB_ERROR = 1
        
        stmt = 'SELECT panda FROM tblDoesntExist'
        arglist = []
        caller = 'test_fetch_invalid_query'
        with self.assertRaises(address_reuse.custom_errors.TooManyDatabaseErrors):
            res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[],
                                                              caller=caller)

        address_reuse.db.NUM_ATTEMPTS_UPON_DB_ERROR = orig
    
    def test_mark_block_complete(self):
        block_height = 17
        self.coord_db.mark_block_complete(block_height)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_mark_block_complete'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        pid = os.getpid()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['block_height'], block_height)
        self.assertEqual(res[0]['pid_of_claimer'], pid)
        self.assertEqual(res[0]['completed'], 1)
        
    def test_claim_block_height(self):
        block_height = 17
        self.coord_db.claim_block_height(block_height)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_claim_block_height'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        pid = os.getpid()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['block_height'], block_height)
        self.assertEqual(res[0]['pid_of_claimer'], pid)
        self.assertEqual(res[0]['completed'], 0)
        
    def test_mark_block_claimed_block_complete(self):
        block_height = 17
        self.coord_db.claim_block_height(block_height)
        self.coord_db.mark_block_complete(block_height)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_mark_block_claimed_block_complete'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        pid = os.getpid()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['block_height'], block_height)
        self.assertEqual(res[0]['pid_of_claimer'], pid)
        self.assertEqual(res[0]['completed'], 1)
        
    def test_unclaim_block_height_previously_completed(self):
        #This may happen in the case of db repair where a block is marked as
        #   completed when it shouldn't have been.
        self.coord_db.mark_block_complete(2)
        self.coord_db.unclaim_block_height(2)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_mark_block_claimed_block_complete'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['block_height'], 2)
        self.assertEqual(res[0]['pid_of_claimer'], None)
        self.assertEqual(res[0]['completed'], None)
        
    def test_unclaim_block_height(self):
        block_height = 17
        self.coord_db.claim_block_height(block_height)
        self.coord_db.unclaim_block_height(block_height)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_claim_block_height'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        pid = os.getpid()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['block_height'], block_height)
        self.assertIsNone(res[0]['pid_of_claimer'])
        self.assertIsNone(res[0]['completed']) #REPLACE op results in NULL val
        
    def test_mark_blocks_completed_up_through_height(self):
        block_height = 1
        self.coord_db.mark_blocks_completed_up_through_height(block_height)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_mark_block_claimed_block_complete'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        pid = os.getpid()
        
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]['block_height'], 0)
        self.assertEqual(res[0]['pid_of_claimer'], pid)
        self.assertEqual(res[0]['completed'], 1)
        self.assertEqual(res[1]['block_height'], 1)
        self.assertEqual(res[1]['pid_of_claimer'], pid)
        self.assertEqual(res[1]['completed'], 1)
    
    def test_get_list_of_block_heights_with_possibly_crashed_workers(self):
        #insert 2 claimed blocks, one very old one and one new one
        stmt = (('INSERT INTO %s (block_height, pid_of_claimer, '
                 'timestamp_claimed, completed) VALUES (0, 12, '
                 'CURRENT_TIMESTAMP, 0)') % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        self.coord_db.run_statement(stmt, arglist)
        stmt = (('INSERT INTO %s (block_height, pid_of_claimer, '
                 'timestamp_claimed, completed) VALUES (11, 42, '
                 '0, 0)') % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        self.coord_db.run_statement(stmt, arglist)
        lst = self.coord_db.get_list_of_block_heights_with_possibly_crashed_workers()
        self.assertEqual(len(lst), 1)
        self.assertEqual(lst[0], 11)
        
        stmt = (('UPDATE %s SET timestamp_claimed=CURRENT_TIMESTAMP WHERE '
                 'block_height = ?') % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = (11,)
        self.coord_db.run_statement(stmt, arglist)
        
        lst = self.coord_db.get_list_of_block_heights_with_possibly_crashed_workers()
        self.assertEqual(len(lst), 0)
    
    def test_get_next_block_height_available_with_no_blocks(self):
        #test what the funtion does when you there are no blocks heights in the
        #   db yet, and we don't try to claim the block.
        
        next_block_height_avail = self.coord_db.get_next_block_height_available(
            starting_height=0)
        self.assertEqual(next_block_height_avail, 0)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_get_next_block_height_available_with_no_blocks'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['block_height'], 0)
        self.assertEqual(res[0]['pid_of_claimer'], None)
        self.assertEqual(res[0]['completed'], None)
    
    def test_get_next_block_height_available_with_no_blocks_and_claim(self):
        #test what the funtion does when you there are blocks heights in the
        #   db yet, and we try to claim the block.
        
        next_block_height_avail = self.coord_db.get_next_block_height_available(
            starting_height=0, claim_it=True)
        self.assertEqual(next_block_height_avail, 0)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_get_next_block_height_available_with_no_blocks'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        
        pid = os.getpid()
        
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['block_height'], 0)
        self.assertEqual(res[0]['pid_of_claimer'], pid)
        self.assertEqual(res[0]['completed'], False)

    def test_get_next_block_height_not_claimed_when_genesis_block_only_claimed(self):
        stmt = (('INSERT INTO %s (block_height, pid_of_claimer, completed) '
                 'VALUES (0, 1337, NULL)') % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        self.coord_db.run_statement(stmt, arglist)
        
        #expected result: all blocks in db are claimed or completed, incr to 1
        next_block_height_avail = self.coord_db.get_next_block_height_available(
            starting_height=0, claim_it=False)
        self.assertEqual(next_block_height_avail, 1)
        
    def test_get_next_block_height_not_claimed_when_gensis_block_only_completed_and_claim_it(self):
        stmt = (('INSERT INTO %s (block_height, pid_of_claimer, completed) '
                 'VALUES (0, 1337, 1)') % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        self.coord_db.run_statement(stmt, arglist)
        
        #expected result: all blocks in db are claimed or completed, incr to 1
        next_block_height_avail = self.coord_db.get_next_block_height_available(
            starting_height=0, claim_it=True)
        self.assertEqual(next_block_height_avail, 1)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_get_next_block_height_available_with_no_blocks'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        
        pid = os.getpid()
        
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]['block_height'], 0)
        self.assertEqual(res[0]['pid_of_claimer'], 1337)
        self.assertEqual(res[0]['completed'], True)
        self.assertEqual(res[1]['block_height'], 1)
        self.assertEqual(res[1]['pid_of_claimer'], pid)
        self.assertEqual(res[1]['completed'], False)
        
    def test_get_next_block_height_not_claimed_when_first_two_blocks_claimed_and_claim_it(self):
        stmt = (('INSERT INTO %s (block_height, pid_of_claimer, completed) '
                 'VALUES (0, 1337, 0)') % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        self.coord_db.run_statement(stmt, arglist)
        stmt = (('INSERT INTO %s (block_height, pid_of_claimer, completed) '
                 'VALUES (1, 1337, 0)') % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        self.coord_db.run_statement(stmt, arglist)
        
        #expected result: all blocks in db are claimed or completed, incr to 2
        next_block_height_avail = self.coord_db.get_next_block_height_available(
            starting_height=0, claim_it=True)
        self.assertEqual(next_block_height_avail, 2)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_get_next_block_height_available_with_no_blocks'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        
        pid = os.getpid()
        
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0]['block_height'], 0)
        self.assertEqual(res[0]['pid_of_claimer'], 1337)
        self.assertEqual(res[0]['completed'], False)
        self.assertEqual(res[1]['block_height'], 1)
        self.assertEqual(res[1]['pid_of_claimer'], 1337)
        self.assertEqual(res[1]['completed'], False)
        self.assertEqual(res[2]['block_height'], 2)
        self.assertEqual(res[2]['pid_of_claimer'], pid)
        self.assertEqual(res[2]['completed'], False)
        
    def test_get_next_block_height_not_claimed_multiple_times(self):
        next_block_height_avail = self.coord_db.get_next_block_height_available(
            starting_height=0, claim_it=False)
        self.assertEqual(next_block_height_avail, 0)
        next_block_height_avail = self.coord_db.get_next_block_height_available(
            starting_height=0, claim_it=True)
        self.assertEqual(next_block_height_avail, 0)
        next_block_height_avail = self.coord_db.get_next_block_height_available(
            starting_height=0, claim_it=True)
        self.assertEqual(next_block_height_avail, 1)

    def test_get_next_block_height_not_claimed_when_a_block_is_claimed_and_unclaimed(self):
        self.coord_db.claim_block_height(2)
        self.coord_db.unclaim_block_height(2)
        next_block_height_avail = self.coord_db.get_next_block_height_available(
            starting_height=0, claim_it=False)
        self.assertEqual(next_block_height_avail, 0)
        next_block_height_avail = self.coord_db.get_next_block_height_available(
            starting_height=2, claim_it=False)
        self.assertEqual(next_block_height_avail, 2)
    
    def test_get_next_block_height_not_claimed_when_start_height_is_high(self):
        self.coord_db.claim_block_height(2)
        self.coord_db.unclaim_block_height(2)
        next_block_height_avail = self.coord_db.get_next_block_height_available(
            starting_height=100, claim_it=True)
        self.assertEqual(next_block_height_avail, 100)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_get_next_block_height_not_claimed_when_start_height_is_high'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        
        pid = os.getpid()
        
        self.assertEqual(len(res), 101)
        self.assertEqual(res[0]['block_height'], 2)
        self.assertEqual(res[0]['pid_of_claimer'], None)
        self.assertEqual(res[0]['completed'], None)
        self.assertEqual(res[100]['block_height'], 100)
        self.assertEqual(res[100]['pid_of_claimer'], pid)
        self.assertEqual(res[100]['completed'], False)

    '''deprecated
    def test_get_next_block_height_available_with_jump(self):
        jump_minimum = 100
        starting_height = 0
        first_available_too_low = 20
        first_available_block_height = 120
        
        stmt = (('INSERT INTO %s (block_height, pid_of_claimer, completed) '
                'VALUES (?, NULL, NULL)') %
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist1 = (first_available_too_low,)
        arglist2 = (first_available_block_height,)
        arglist = [arglist1, arglist2]
        self.coord_db.run_statement(stmt, arglist, execute_many=True)
        
        next_avail = self.coord_db.get_next_block_height_available(
            starting_height=0, claim_it=False, jump_minimum=jump_minimum)
        self.assertEqual(next_avail, 120)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_get_next_block_height_not_claimed_when_start_height_is_high'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]['block_height'], 20)
        self.assertEqual(res[0]['pid_of_claimer'], None)
        self.assertEqual(res[0]['completed'], None)
        self.assertEqual(res[1]['block_height'], 120)
        self.assertEqual(res[1]['pid_of_claimer'], None)
        self.assertEqual(res[1]['completed'], None)
        
        next_avail = self.coord_db.get_next_block_height_available(
            starting_height=0, claim_it=True, jump_minimum=jump_minimum)
        self.assertEqual(next_avail, 120)
        
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        
        pid = os.getpid()
        
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]['block_height'], 20)
        self.assertEqual(res[0]['pid_of_claimer'], None)
        self.assertEqual(res[0]['completed'], None)
        self.assertEqual(res[1]['block_height'], 120)
        self.assertEqual(res[1]['pid_of_claimer'], pid)
        self.assertEqual(res[1]['completed'], 0)
    '''

    def test_get_list_of_next_block_heights_available_simple(self):
        #try to claim 3 consecutive blocks
        claimed = self.coord_db.get_list_of_next_block_heights_available(
            starting_height=0, num_to_claim=3)
        self.assertEqual(len(claimed), 3)
        self.assertIn(0, claimed)
        self.assertIn(1, claimed)
        self.assertIn(2, claimed)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_get_next_block_height_not_claimed_when_start_height_is_high'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        my_pid = os.getpid()
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0]['block_height'], 0)
        self.assertEqual(res[0]['pid_of_claimer'], my_pid)
        self.assertEqual(res[0]['completed'], 0)
        self.assertEqual(res[1]['block_height'], 1)
        self.assertEqual(res[1]['pid_of_claimer'], my_pid)
        self.assertEqual(res[1]['completed'], 0)
        self.assertEqual(res[2]['block_height'], 2)
        self.assertEqual(res[2]['pid_of_claimer'], my_pid)
        self.assertEqual(res[2]['completed'], 0)

    
    def test__initialize_block_span_simple(self):
        self.coord_db._initialize_block_span(max_block_height=2)
        
        self.assertEqual(self.coord_db.last_block_initialized, 2)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_get_next_block_height_not_claimed_when_start_height_is_high'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0]['block_height'], 0)
        self.assertEqual(res[0]['pid_of_claimer'], None)
        self.assertEqual(res[0]['completed'], None)
        self.assertEqual(res[1]['block_height'], 1)
        self.assertEqual(res[1]['pid_of_claimer'], None)
        self.assertEqual(res[1]['completed'], None)
        self.assertEqual(res[2]['block_height'], 2)
        self.assertEqual(res[2]['pid_of_claimer'], None)
        self.assertEqual(res[2]['completed'], None)
        
        self.coord_db._initialize_block_span(max_block_height=5, 
                                             min_block_height=4)
        
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        self.assertEqual(len(res), 5)
        self.assertEqual(res[0]['block_height'], 0)
        self.assertEqual(res[0]['pid_of_claimer'], None)
        self.assertEqual(res[0]['completed'], None)
        self.assertEqual(res[1]['block_height'], 1)
        self.assertEqual(res[1]['pid_of_claimer'], None)
        self.assertEqual(res[1]['completed'], None)
        self.assertEqual(res[2]['block_height'], 2)
        self.assertEqual(res[2]['pid_of_claimer'], None)
        self.assertEqual(res[2]['completed'], None)
        self.assertEqual(res[3]['block_height'], 4)
        self.assertEqual(res[3]['pid_of_claimer'], None)
        self.assertEqual(res[3]['completed'], None)
        self.assertEqual(res[4]['block_height'], 5)
        self.assertEqual(res[4]['pid_of_claimer'], None)
        self.assertEqual(res[4]['completed'], None)
        
        #The highest consecutive block from 0 that has been initialized now
        #   is 2, since there is a gap at 3.
        self.assertEqual(self.coord_db.last_block_initialized, 2)
        
    def test__initialize_block_span_with_existing_records(self):
        self.coord_db.claim_block_height(1)
        self.coord_db.mark_block_complete(2)
        
        self.coord_db._initialize_block_span(max_block_height=3)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_get_next_block_height_not_claimed_when_start_height_is_high'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        
        my_pid = os.getpid()
        
        self.assertEqual(len(res), 4)
        self.assertEqual(res[0]['block_height'], 1)
        self.assertEqual(res[0]['pid_of_claimer'], my_pid)
        self.assertEqual(res[0]['completed'], 0)
        self.assertEqual(res[1]['block_height'], 2)
        self.assertEqual(res[1]['pid_of_claimer'], my_pid)
        self.assertEqual(res[1]['completed'], 1)
        self.assertEqual(res[2]['block_height'], 0)
        self.assertEqual(res[2]['pid_of_claimer'], None)
        self.assertEqual(res[2]['completed'], None)
        self.assertEqual(res[3]['block_height'], 3)
        self.assertEqual(res[3]['pid_of_claimer'], None)
        self.assertEqual(res[3]['completed'], None)
        
        self.assertEqual(self.coord_db.last_block_initialized, 3)
    
    def test_get_list_of_next_block_heights_available_interrupted_by_complete_block(self):
        stmt = (('INSERT INTO %s (block_height, pid_of_claimer, completed) '
                 'VALUES (1, 1337, 1)') % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        self.coord_db.run_statement(stmt, arglist=[])
        
        claimed = self.coord_db.get_list_of_next_block_heights_available(
            starting_height=0, num_to_claim=3)
        
        self.assertEqual(len(claimed), 1)
        self.assertIn(0, claimed)
        self.assertNotIn(2, claimed)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_get_next_block_height_not_claimed_when_start_height_is_high'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        my_pid = os.getpid()
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]['block_height'], 1)
        self.assertEqual(res[0]['pid_of_claimer'], 1337)
        self.assertEqual(res[0]['completed'], 1)
        self.assertEqual(res[1]['block_height'], 0)
        self.assertEqual(res[1]['pid_of_claimer'], my_pid)
        self.assertEqual(res[1]['completed'], 0)
        
    def test_get_list_of_next_block_heights_available_interrupted_by_claimed_block(self):
        stmt = (('INSERT INTO %s (block_height, pid_of_claimer, completed) '
                 'VALUES (1, 1337, 0)') % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        self.coord_db.run_statement(stmt, arglist=[])
        
        claimed = self.coord_db.get_list_of_next_block_heights_available(
            starting_height=0, num_to_claim=3)
        
        self.assertEqual(len(claimed), 1)
        self.assertIn(0, claimed)
        self.assertNotIn(2, claimed)
        
        stmt = ('SELECT * FROM %s' % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        caller= 'test_get_next_block_height_not_claimed_when_start_height_is_high'
        res = self.coord_db.fetch_query_and_handle_errors(stmt, arglist=[], 
                                                          caller=caller)
        my_pid = os.getpid()
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]['block_height'], 1)
        self.assertEqual(res[0]['pid_of_claimer'], 1337)
        self.assertEqual(res[0]['completed'], 0)
        self.assertEqual(res[1]['block_height'], 0)
        self.assertEqual(res[1]['pid_of_claimer'], my_pid)
        self.assertEqual(res[1]['completed'], 0)
        
    def test_is_block_height_claimed(self):
        claimed = self.coord_db.is_block_height_claimed(1)
        self.assertFalse(claimed)
        
        stmt = (('INSERT OR REPLACE INTO %s (block_height, pid_of_claimer, '
                 'completed) VALUES (1, 1337, 0)') % 
                address_reuse.db.SQL_TABLE_NAME_COORDINATION_REGISTER)
        self.coord_db.run_statement(stmt, arglist=[]) 
        
        claimed = self.coord_db.is_block_height_claimed(1)
        
suite = unittest.TestLoader().loadTestsFromTestCase(DatabaseTestCase)
suite2 = unittest.TestLoader().loadTestsFromTestCase(
    BlameResolverCoordinationDatabaseTestCase)
