#A data producer processes blockchain information and stores the results. Data
#   subscribers use this data to produce their own data. This module
#   synchronizes acitivity of multiple processes/threads based on what they
#   need next in order to perform their own duties.
#Data producers signal to subscribers that data is ready by storing the height 
#   span of the blocks they've processed in the database. For example, if one
#   process is responsible for identifying the client for an instance of
#   address reuse at block height x, but needs to wait for another process to
#   finish identifying all instances of address reuse at block height x, it
#   can use this class to find out when that data is available.

####################
# INTERNAL IMPORTS #
####################

import db
import custom_errors

####################
# EXTERNAL IMPORTS #
####################

from enum import IntEnum
from time import sleep

#############
# CONSTANTS #
#############

DEFAULT_SLEEP_TIME_IN_SEC = 10.0   #float

#########
# ENUMS #
#########

class DataProducer(IntEnum):
    BLOCK_ADDRESS_REUSE_WITH_DEFERRED_BLAME = 1
    BLOCK_RELAYED_BY_CACHED_IN_DB           = 2
    TX_OUTPUT_ADDRESS_CACHED_IN_DB          = 3

###########
# CLASSES #
###########

#Processes that produce data and want to announce that new data is available to
#   subscribers.
class DataProductionAnnouncer:
    
    producer_identity   = None
    database            = None
    
    def __init__(self, producer_identity, database):
        assert isinstance(producer_identity, DataProducer)
        assert isinstance(database, db.Database)
        self.producer_identity = producer_identity
        self.database = database

class BlockDataProductionAnnouncer(DataProductionAnnouncer):
    
    current_block_available = None
    
    def __init__(self, producer_identity, database, 
                 current_block_available = -1):
        DataProductionAnnouncer.__init__(self, producer_identity, database) #super
        self.current_block_available = current_block_available
        
    def increment_announced_block_available(self):
        self.current_block_available = self.current_block_available + 1
        self.database.increment_top_block_height_available(
            self.producer_identity)

class TxOutputAddressCacheAnnouncer(DataProductionAnnouncer):
    
    def __init__(self, producer_identity, database):
        assert producer_identity == DataProducer.TX_OUTPUT_ADDRESS_CACHED_IN_DB
        DataProductionAnnouncer.__init__(self, producer_identity, database) #super

class DataSubscriber:
    
    sleep_time      = 0.0
    subscriptions   = None
    database        = None
    
    def __init__(self, database, sleep_time = DEFAULT_SLEEP_TIME_IN_SEC):
        assert isinstance(database, db.Database)
        self.sleep_time = sleep_time
        self.subscriptions = []
        self.database = database
        
    def add_subscription(self, producer):
        assert isinstance(producer, DataProducer)
        self.subscriptions.append(producer)
        
    def are_producers_ready(self):
        raise NotImplementedError #override me
    
    def do_sleep_until_producers_ready(self):
        while True:
            if self.are_producers_ready():
                break
            else:
                print("DEBUG: sleeping for %f seconds..." % self.sleep_time)
                sleep(self.sleep_time)

class BlockDataSubscriber(DataSubscriber):
    
    next_block_needed   = None
    
    def __init__(self, database, next_block_needed = -1, 
                 sleep_time = DEFAULT_SLEEP_TIME_IN_SEC):
        DataSubscriber.__init__(self, database, sleep_time) #super
        self.next_block_needed = next_block_needed
    
    #Returns whether data for the next required block height is available from
    #   all data producers this is subscribed to.
    def are_producers_ready(self):
        if len(self.subscriptions) == 0:
            raise custom_errors.NoSubscriptionsAdded
        for producer in self.subscriptions:
            height = self.database.get_top_block_height_available(producer)
            if height is None:
                print("DEBUG: Producer %d we're subscribed to has not yet announced any completed blocks. We need block %d." %
                     (producer, self.next_block_needed))
                return False
            if height < self.next_block_needed:
                print("DEBUG: Producer %d we're subscribed to is not ready. Its height is %d and we need %d." %
                     (producer, height, self.next_block_needed))
                return False
        return True
    
    def increment_next_block_needed(self):
        self.next_block_needed = self.next_block_needed + 1

class TxOutputAddressCacheSubscriber(DataSubscriber):
    
    next_tx_id_needed               = None
    next_prev_tx_ouput_pos_needed   = None
    
    def __init__(self, database, sleep_time = DEFAULT_SLEEP_TIME_IN_SEC, 
                 next_tx_id_needed = None, 
                 next_prev_tx_ouput_pos_needed = None):
        DataSubscriber.__init__(self, database = database, 
                                sleep_time = sleep_time) #super
        
        self.next_tx_id_needed = next_tx_id_needed
        self.next_prev_tx_ouput_pos_needed = next_prev_tx_ouput_pos_needed
    
    #Returns whether data for the next required tx is available from all data
    #   producers this is subscribed to.
    def are_producers_ready(self):
        addr = self.get_output_address(self.next_tx_id_needed, 
                                       self.next_prev_tx_ouput_pos_needed)
        if addr is None:
            return False
        else:
            return True
    
    #Returns output address if available, otherwise returns None
    def get_output_address(self, tx_id, output_pos):
        addr = self.database.get_output_address(tx_id, output_pos)
        return addr
    