#Simulates the full processing of address reuse using mutiple threads

#Covers these classes and functions:
#   BlockDataProductionAnnouncer:
#       increment_announced_block_available()
#
#   BlockDataSubscriber:
#       #do_sleep_until_producers_ready()

####################
# INTERNAL IMPORTS #
####################

import address_reuse.data_subscription
import address_reuse.block_processor
import address_reuse.db

####################
# EXTERNAL IMPORTS #
####################

import unittest
import os
from multiprocessing import Process, Queue

#############
# CONSTANTS #
#############

TEMP_DB_FILENAME = 'address_reuse.db-temp'

class DataSubscriptionSimulateProcessingTestCase(unittest.TestCase):
    
    database_connector =  None
    
    def setUp(self):
        try:
            os.remove(TEMP_DB_FILENAME)
        except OSError:
            pass
        self.database_connector = address_reuse.db.Database(TEMP_DB_FILENAME)
        
    def tearDown(self):
        pass
    
    def relayed_by_cacher(self):
        try:
            database_connector = address_reuse.db.Database(TEMP_DB_FILENAME)
            
            relayed_by_producer_identity = address_reuse.data_subscription.DataProducer.BLOCK_RELAYED_BY_CACHED_IN_DB
            relayed_by_announcer = address_reuse.data_subscription.BlockDataProductionAnnouncer(
                relayed_by_producer_identity, database_connector)
            
            api_reader = address_reuse.blockchain_reader.ThrottledBlockchainReader(
                database_connector)
            block_processor = address_reuse.block_processor.BlockProcessor(
                api_reader, database_connector)
            for i in range(0, 171):
                print("DEBUG: relayed_by_cacher @ height %d" % i)
                block_processor.cache_relayed_by_fields_for_block_only(i)
                print(("DEBUG: relayed_by_cacher @ height %d: Cached fields "
                       "for this block." % i))
                relayed_by_announcer.increment_announced_block_available()
        except Exception as e:
            print(str(e))
    
    def tx_out_cacher(self):
        print("DEBUG: Started tx_out_cacher process.")
        database_connector = address_reuse.db.Database(TEMP_DB_FILENAME)
        local_block_reader = address_reuse.blockchain_reader.LocalBlockchainRPCReader(
            database_connector)
        block_processor = address_reuse.block_processor.BlockProcessor(
            local_block_reader, database_connector)
        for i in range(0, 171):
            print("DEBUG: tx_out_cacher @ height %d" % i)
            block_processor.cache_tx_output_addresses_for_block_only(i)
            
    def address_reuse_finder(self):
        print("DEBUG: Started address_reuse_finder process.")
        database_connector = address_reuse.db.Database(TEMP_DB_FILENAME)
        local_block_reader = address_reuse.blockchain_reader.LocalBlockchainRPCReader(
            database_connector)
        block_processor = address_reuse.block_processor.BlockProcessor(
            local_block_reader, database_connector)
        
        deferred_blame_reuse_producer_identity = address_reuse.data_subscription.DataProducer.BLOCK_ADDRESS_REUSE_WITH_DEFERRED_BLAME
        announcer = address_reuse.data_subscription.BlockDataProductionAnnouncer(
            deferred_blame_reuse_producer_identity, database = database_connector)
        
        for i in range(0, 171):
            #print("DEBUG: address_reuse_finder @ height %d. May sleep now..." % i)
            print("DEBUG: address_reuse_finder @ height %d." % i)
            #relayed_by_subscription.do_sleep_until_producers_ready()
            #print("DEBUG: address_reuse_finder @ height %d. Produers are now ready." % i)
            block_processor.process_block(i, defer_blaming = True, 
                                          use_tx_out_addr_cache_only = True)
            announcer.increment_announced_block_available()
            print("DEBUG: address_reuse_finder @ height %d. Announcing completion of block." % i)
            #relayed_by_subscription.increment_next_block_needed()
    
    def deferred_blame_resolver(self):
        print("DEBUG: Started deferred_blame_resolver process.")
        database_connector = address_reuse.db.Database(TEMP_DB_FILENAME)
        blockchain_reader = address_reuse.blockchain_reader.LocalBlockchainRPCReader(
            database_connector)
        block_processor = address_reuse.block_processor.BlockProcessor(
            blockchain_reader, database_connector)
        
        subscriptions = address_reuse.data_subscription.BlockDataSubscriber(
            database = database_connector, next_block_needed = 0)
        subscriptions.add_subscription(
            address_reuse.data_subscription.DataProducer.BLOCK_RELAYED_BY_CACHED_IN_DB)
        subscriptions.add_subscription(
            address_reuse.data_subscription.DataProducer.BLOCK_ADDRESS_REUSE_WITH_DEFERRED_BLAME)
        
        ready = subscriptions.are_producers_ready()
        self.assertFalse(ready)
        
        for i in range(0, 171):
            print("DEBUG: deferred_blame_resolver @ height %d. May sleep now..." % i)
            subscriptions.do_sleep_until_producers_ready()
            print("DEBUG: deferred_blame_resolver @ height %d. Produers are now ready." % i)
            block_processor.process_block_after_deferred_blaming(i)
            subscriptions.increment_next_block_needed()
            
    def select_all(self, caller, table_name):
        arglist = []
        
        stmt = 'SELECT * FROM ' + table_name
        rows = self.database_connector.fetch_query_and_handle_errors(stmt, 
                                                                     arglist, 
                                                                     caller)
        return rows
        
    #Create several processes to handle various kinds of work. They should get
    #   blocked and sleep when stuff isn't ready. By the end, the database
    #   should be a speficic state.
    def test_simulate_through_block_170(self):
        #Cache all 'relayed by' fields blocks at heights 0 through 170
        #   subscriptions: None
        #Cache all transaction outputs at heights 0 through 170
        #   subscriptions: None
        #Find all records of address reuse at heights 0 through 170
        #   subscriptions: tx output cacher
        #Resolve all address reuse records from deferred blame to particular
        #   parties at heights 0 through 170.
        #   subscriptions: address reuse record creator, relayed-by cacher
        
        tx_out_process = Process(target = self.tx_out_cacher)
        tx_out_process.start()
        
        relayed_by_process = Process(target = self.relayed_by_cacher)
        relayed_by_process.start()
        
        address_reuse_process = Process(target = self.address_reuse_finder)
        address_reuse_process.start()
        
        #run last as a regular function instead of a process so it will block
        self.deferred_blame_resolver()
        
        #check final state of database
        caller = 'test_simulate_through_block_170'
        
        rows = self.select_all(caller, 
                               address_reuse.db.SQL_TABLE_NAME_BLAME_IDS)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['label'], 'DB_DEFERRED_BLAME_PLACEHOLDER')
        self.assertEqual(rows[1]['label'], '67c35f9e5da6beab') 
        
        rows = self.select_all(caller, 
                               address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['btc_address'], 
                         '12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S')
        self.assertEqual(rows[0]['label'], '67c35f9e5da6beab')
        
        rows = self.select_all(caller, 
                               address_reuse.db.SQL_TABLE_NAME_BLAME_STATS)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]['blame_recipient_id'], 2)
        self.assertEqual(rows[1]['blame_recipient_id'], 2)
        self.assertEqual(rows[2]['blame_recipient_id'], 2)
        
        self.assertEqual(rows[0]['address_reuse_type'], 1)
        self.assertEqual(rows[1]['address_reuse_type'], 2)
        self.assertEqual(rows[2]['address_reuse_type'], 2)
        
        self.assertEqual(rows[0]['role'], 1)
        self.assertEqual(rows[1]['role'], 1)
        self.assertEqual(rows[2]['role'], 2)
        
        self.assertEqual(rows[0]['data_source'], 2)
        self.assertEqual(rows[1]['data_source'], 2)
        self.assertEqual(rows[2]['data_source'], 2)
        
        self.assertEqual(rows[0]['block_height'], 170)
        self.assertEqual(rows[1]['block_height'], 170)
        self.assertEqual(rows[2]['block_height'], 170)
        
        self.assertEqual(rows[0]['confirmed_tx_id'], ('f4184fc596403b9d638783cf'
                                                      '57adfe4c75c605f6356fbc91'
                                                      '338530e9831e9e16'))
        self.assertEqual(rows[1]['confirmed_tx_id'], ('f4184fc596403b9d638783cf'
                                                      '57adfe4c75c605f6356fbc91'
                                                      '338530e9831e9e16'))
        self.assertEqual(rows[2]['confirmed_tx_id'], ('f4184fc596403b9d638783cf'
                                                      '57adfe4c75c605f6356fbc91'
                                                      '338530e9831e9e16'))

        self.assertEqual(rows[0]['relevant_address'], ('12cbQLTFMXRnSzktFkuoG3e'
                                                       'HoMeFtpTu3S'))
        self.assertEqual(rows[1]['relevant_address'], ('12cbQLTFMXRnSzktFkuoG3e'
                                                       'HoMeFtpTu3S'))
        self.assertEqual(rows[2]['relevant_address'], ('12cbQLTFMXRnSzktFkuoG3e'
                                                       'HoMeFtpTu3S'))
        
        rows = self.select_all(
            caller, 
            address_reuse.db.SQL_TABLE_NAME_BLOCK_DATA_PRODUCTION_STATUS)
        
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['producer_id'], 1)
        self.assertEqual(rows[1]['producer_id'], 2)
        
        self.assertEqual(rows[0]['top_block_height_available'], 170)
        self.assertEqual(rows[1]['top_block_height_available'], 170)
        
        rows = self.select_all(caller, 
                               address_reuse.db.SQL_TABLE_NAME_BLOCK_STATS)
        self.assertEqual(len(rows), 171)
        for i in range(0,170):
            self.assertEqual(rows[i]['block_num'], i)
            self.assertEqual(rows[i]['tx_total_num'], 1)
            self.assertEqual(rows[i]['tx_sendback_reuse_num'], 0)
            self.assertEqual(rows[i]['tx_receiver_has_tx_history_num'], 0)
            self.assertEqual(rows[i]['tx_sendback_reuse_pct'], 0)
            self.assertEqual(rows[i]['tx_receiver_has_tx_history_pct'], 0)
            self.assertEqual(rows[i]['process_type_version_num'], 1)
        
        self.assertEqual(rows[170]['block_num'], 170)
        self.assertEqual(rows[170]['tx_total_num'], 2)
        self.assertEqual(rows[170]['tx_sendback_reuse_num'], 1)
        self.assertEqual(rows[170]['tx_receiver_has_tx_history_num'], 1)
        self.assertEqual(rows[170]['tx_sendback_reuse_pct'], 50)
        self.assertEqual(rows[170]['tx_receiver_has_tx_history_pct'], 50)
        self.assertEqual(rows[170]['process_type_version_num'], 1)
        
        rows = self.select_all(caller, 
                             address_reuse.db.SQL_TABLE_NAME_RELAYED_BY_CACHE)
        self.assertEqual(len(rows), 172)
        for i in range(0, 171):
            self.assertEqual(rows[i]['block_height'], i)
            self.assertEqual(len(rows[i]['tx_id']), 64)
            self.assertTrue(rows[i]['tx_id'].isalnum())
            self.assertEqual(rows[i]['relayed_by'], '0.0.0.0')
        
        self.assertEqual(rows[171]['block_height'], 170)
        self.assertEqual(rows[171]['tx_id'], ('f4184fc596403b9d638783cf57adfe4c'
                                              '75c605f6356fbc91338530e9831e9e1'
                                              '6'))
        self.assertEqual(rows[171]['relayed_by'], '0.0.0.0')
        
        rows = self.select_all(caller, 
                               address_reuse.db.SQL_TABLE_NAME_ADDRESSES_SEEN) 
        self.assertEqual(len(rows), 172)
        for i in range(0,171):
            self.assertEqual(rows[i]['block_height_first_seen'], i)
            self.assertTrue(rows[i]['address'].isalnum())
            self.assertTrue(
                len(rows[i]['address']) == 33 or len(rows[i]['address']) == 34)
            
        self.assertEqual(rows[171]['block_height_first_seen'], 170)
        self.assertEqual(rows[171]['address'], ('1Q2TWHE3GMdB6BZKafqwxXtWAWgFt5'
                                                'Jvm3'))
        
        rows =self.select_all(caller, 
                              address_reuse.db.SQL_TABLE_NAME_TX_OUTPUT_CACHE)
        self.assertEqual(len(rows), 173)
        for i in range(0, 170):
            self.assertEqual(rows[i]['block_height'], i)
            self.assertEqual(len(rows[i]['tx_id']), 64)
            self.assertTrue(rows[i]['tx_id'].isalnum())
            self.assertEqual(rows[i]['output_pos'], 0)
            self.assertTrue(
                len(rows[i]['address']) == 33 or len(rows[i]['address']) == 34)
            self.assertTrue(rows[i]['address'].isalnum())
        
        self.assertEqual(rows[170]['block_height'], 170)
        self.assertEqual(rows[171]['block_height'], 170)
        self.assertEqual(rows[172]['block_height'], 170)
        
        self.assertEqual(rows[170]['tx_id'], ('b1fea52486ce0c62bb442b530a3f0132'
                                              'b826c74e473d1f2c220bfa78111c508'
                                              '2'))
        self.assertEqual(rows[171]['tx_id'], ('f4184fc596403b9d638783cf57adfe4c'
                                              '75c605f6356fbc91338530e9831e9e1'
                                              '6'))
        self.assertEqual(rows[172]['tx_id'], ('f4184fc596403b9d638783cf57adfe4c'
                                              '75c605f6356fbc91338530e9831e9e1'
                                              '6'))
        
        self.assertEqual(rows[170]['output_pos'], 0)
        self.assertEqual(rows[171]['output_pos'], 0)
        self.assertEqual(rows[172]['output_pos'], 1)
        
        self.assertEqual(rows[170]['address'], ('1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQ'
                                                'DDWc'))
        self.assertEqual(rows[171]['address'], ('1Q2TWHE3GMdB6BZKafqwxXtWAWgFt5'
                                                'Jvm3'))
        self.assertEqual(rows[172]['address'], ('12cbQLTFMXRnSzktFkuoG3eHoMeFtp'
                                                'Tu3S'))
        
    def test_subscriber_and_producer_relayed_by_cache_when_ready(self):
        producer_identity = address_reuse.data_subscription.DataProducer.BLOCK_RELAYED_BY_CACHED_IN_DB
        relayed_by_producer = address_reuse.data_subscription.BlockDataProductionAnnouncer(
            producer_identity, self.database_connector)
        relayed_by_subscriber = address_reuse.data_subscription.BlockDataSubscriber(
            database = self.database_connector, next_block_needed = 0)
        relayed_by_subscriber.add_subscription(
            address_reuse.data_subscription.DataProducer.BLOCK_RELAYED_BY_CACHED_IN_DB)
        
        ready = relayed_by_subscriber.are_producers_ready()
        self.assertFalse(ready)
        
        relayed_by_producer.increment_announced_block_available()
        ready = relayed_by_subscriber.are_producers_ready()
        self.assertTrue(ready)
        
suite = unittest.TestLoader().loadTestsFromTestCase(
    DataSubscriptionSimulateProcessingTestCase)