#Covers these classes and functions:
#   BlockDataProductionAnnouncer:
#       increment_announced_block_available()
#
#   BlockDataSubscriber:
#       #are_producers_ready()

####################
# INTERNAL IMPORTS #
####################

import address_reuse.data_subscription

####################
# EXTERNAL IMPORTS #
####################

import unittest
import os

#############
# CONSTANTS #
#############

TEMP_DB_FILENAME = 'address_reuse.db-temp'

class DataSubscriptionTestCase(unittest.TestCase):
    
    database_connector =  None
    
    def setUp(self):
        try:
            os.remove(TEMP_DB_FILENAME)
        except OSError:
            pass
        self.database_connector = address_reuse.db.Database(TEMP_DB_FILENAME)
        
    def tearDown(self):
        pass
    
    def test_subscribe_to_relayed_by_cache_and_check_readiness_when_empty(self):
        relayed_by_subscriber = address_reuse.data_subscription.BlockDataSubscriber(
            database = self.database_connector, next_block_needed = 0)
        relayed_by_subscriber.add_subscription(
            address_reuse.data_subscription.DataProducer.BLOCK_RELAYED_BY_CACHED_IN_DB)
        ready = relayed_by_subscriber.are_producers_ready()
        self.assertFalse(ready)
    
    def test_subscriber_and_producer_relayed_by_cache_when_ready(self):
        producer_identity = address_reuse.data_subscription.DataProducer.BLOCK_RELAYED_BY_CACHED_IN_DB
        relayed_by_producer = address_reuse.data_subscription.BlockDataProductionAnnouncer(
            producer_identity, self.database_connector)
        relayed_by_subscriber = address_reuse.data_subscription.BlockDataSubscriber(
            self.database_connector, next_block_needed = 0)
        relayed_by_subscriber.add_subscription(
            address_reuse.data_subscription.DataProducer.BLOCK_RELAYED_BY_CACHED_IN_DB)
        
        ready = relayed_by_subscriber.are_producers_ready()
        self.assertFalse(ready)
        
        relayed_by_producer.increment_announced_block_available()
        ready = relayed_by_subscriber.are_producers_ready()
        self.assertTrue(ready)
        
suite = unittest.TestLoader().loadTestsFromTestCase(DataSubscriptionTestCase)