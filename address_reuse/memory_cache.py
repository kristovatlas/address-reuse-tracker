#A module for caching stuff in memory for quicker lookup

#TODO: write tests

from collections import OrderedDict
from types import IntType
from array import array

#debugging
#import time

#############
# CONSTANTS #
#############

#TODO: Consider setting this in config file and have caller override in 
#   constructor
MAX_TX_IN_MEM_DEFAULT = 10000

class TransactionOutputCache:
    #A dictionary object, with keys as tx ids pointing to an indexed, ordered 
    #   list of output addresses for that transaction:
    #  {tx_id => [address0, address1, ...]}
    #The dictionary will have a max size of MAX_TX_IN_MEM transactions.
    memory_store        = None
    
    blockchain_reader   = None
    max_tx_in_mem       = None
    
    num_cache_hits      = 0
    num_cache_misses    = 0
    
    def __init__(self, blockchain_reader, 
                 max_tx_in_mem = MAX_TX_IN_MEM_DEFAULT):
        self.memory_store       = OrderedDict()
        self.blockchain_reader  = blockchain_reader
        self.max_tx_in_mem      = max_tx_in_mem
    
    #param0: tx_id: A trasaction hash
    #param1: output_addresses: An ordered, indexed list of output addresses
    #   corresponding to the specified transaction.
    def store_tx_outputs(self, tx_id, output_addresses):
        if len(self.memory_store) == self.max_tx_in_mem:
            self.memory_store.popitem(last=False) #FIFO
        self.memory_store[tx_id] = output_addresses
    
    def get_output_address_at_position(self, tx_id, output_pos):
        assert type(output_pos) is IntType
        #start = time.time()
        if tx_id in self.memory_store:
            #commented out: this slows things way down
            #assert len(self.memory_store[tx_id]) > output_pos
            addr = self.memory_store[tx_id][output_pos]
            self.num_cache_hits = self.num_cache_hits + 1
            #elapsed = time.time() - start
            #print("DEBUG: Found tx %s in memory cache, %d hits so far. Address was %s. Time elapsed: %f" % (tx_id, self.num_cache_hits, addr, elapsed))
            return addr
        else:
            #query reader for the address and cache ALL output addresses for
            #   that transaction.
            tx_json = self.blockchain_reader.get_decoded_tx(tx_id)
            output_addresses = self.blockchain_reader.get_output_addresses(
                tx_json)
            self.store_tx_outputs(tx_id, output_addresses)
            self.num_cache_misses = self.num_cache_misses + 1
            addr = output_addresses[output_pos]
            #elapsed = time.time() - start
            #print("DEBUG: tx %s was not in memory cache, %d misses so far. Address was %s. Time elapsed: %f" % (tx_id, self.num_cache_misses, addr, elapsed))
            return addr
