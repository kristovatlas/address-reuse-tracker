# Stores stats of interest about a block. Does not directly access DB

####################
# INTERNAL IMPORTS #
####################

import logger

#####################
# PACKAGE CONSTANTS #
#####################

NUM_DECIMAL_PLACES = 2 # TODO: move to config file?
DECIMAL_FORMAT = '{0:.' + str(NUM_DECIMAL_PLACES) + 'f}'

###################
# PACKAGE CLASSES #
###################

class BlockState:
    
    ###################
    # CLASS CONSTANTS #
    ###################
    
    #This integer defines what processing logic was used to process a block. Therefore,
    # if we want to process blocks again in the future, we can detect that previous
    # processing is outdated, and update the processing for that block.
    PROCESS_TYPE_VERSION_NUM        = 1  #TODO: Move this to config file?
    
    block_num                       = None
    tx_total_num                    = None
    tx_sendback_reuse_num           = None
    tx_receiver_has_tx_history_num  = None
    tx_sendback_reuse_pct           = None
    tx_receiver_has_tx_history_pct  = None
    
    def __init__(self, block_height):
        self.block_num                          = block_height
        self.tx_total_num                       = 0
        self.tx_sendback_reuse_num              = 0
        self.tx_receiver_has_tx_history_num     = 0
        self.tx_sendback_reuse_pct              = ''
        self.tx_receiver_has_tx_history_pct     = ''
    
    def incr_total_tx_num(self):
        self.tx_total_num = self.tx_total_num + 1
        
    def incr_sendback_reuse(self):
        self.tx_sendback_reuse_num = self.tx_sendback_reuse_num +1
    
    def incr_receiver_tx_history_reuse(self):
        self.tx_receiver_has_tx_history_num = self.tx_receiver_has_tx_history_num + 1
    
    #once the other stats are done accumulating, calculate percentage. There should not be a divby0 issue, since all blocks should contain at least a coinbase tx.
    def update_sendback_reuse_pct(self):
        pct = '' 
        try:
            pct = DECIMAL_FORMAT.format(100.0 * self.tx_sendback_reuse_num / self.tx_total_num)
        except ZeroDivisionError:
            logger.log_and_die("Tried to update sendback reuse % but got divby0 for block height " + str(self.block_num))
        self.tx_sendback_reuse_pct = pct
        print("DEBUG: Updated tx_sendback_reuse_pct for block %d is '%s'" % (self.block_num, self.tx_sendback_reuse_pct))
    
    #once the other stats are done accumulating, calculate percentage. There should not be a divby0 issue, since all blocks should contain at least a coinbase tx.
    def update_receiver_histoy_pct(self):
        pct = '' 
        try:
            pct = DECIMAL_FORMAT.format(100.0 * self.tx_receiver_has_tx_history_num / self.tx_total_num)
        except ZeroDivisionError:
            logger.log_and_die("Tried to update receiver history % but got divby0 for block height " + str(self.block_num))
        self.tx_receiver_has_tx_history_pct = pct
        print("DEBUG: Updated tx_receiver_has_tx_history_pct for block %d is '%s'" % (self.block_num, self.tx_receiver_has_tx_history_pct))
