####################
# INTERNAL IMPORTS #
####################

import validate
import logger

####################
# EXTERNAL IMPORTS #
####################

import json #for 'pretty' printing contents of BlameStatsPerBlock instance

#####################
# PACKAGE CONSTANTS #
#####################

NUM_DECIMAL_PLACES = 2 # TODO: move to config file?
DECIMAL_FORMAT = '{0:.' + str(NUM_DECIMAL_PLACES) + 'f}'

#Defines a set of data for address reuse stats for a given block in the 
# blockchain. This data can later be visualized in a graph.
class BlameStatsPerBlock:
    block_height                    = None
    num_tx_total                    = None
    pct_tx_with_sendback_reuse      = None
    pct_tx_with_history_reuse       = None
    party_label_to_pct_sendback_map = None #dict
    party_label_to_pct_history_map  = None #dict
    top_reuser_labels               = None #list
    
    def __init__(self, block_height, num_tx_total, pct_tx_with_sendback_reuse, 
                 pct_tx_with_history_reuse):
        validate.check_int_and_die(block_height, 'block_height', '__init__')
        validate.check_int_and_die(num_tx_total, 'num_tx_total', '__init__')
        validate.check_float_and_die(pct_tx_with_sendback_reuse, 
                                     'pct_tx_with_sendback_reuse', '__init__')
        validate.check_float_and_die(pct_tx_with_history_reuse,
                                     'pct_tx_with_history_reuse', '__init__')
        
        self.block_height = int(block_height)
        self.num_tx_total = int(num_tx_total)
        self.pct_tx_with_sendback_reuse = DECIMAL_FORMAT.format(
            pct_tx_with_sendback_reuse)
        self.pct_tx_with_history_reuse = DECIMAL_FORMAT.format(
            pct_tx_with_history_reuse)
        self.party_label_to_pct_sendback_map = dict()
        self.party_label_to_pct_history_map  = dict()
        self.top_reuser_labels = []
    
    def add_sendback_reuse_blamed_party(self, blame_label, 
                                        num_tx_with_sendback_reuse):
        if blame_label not in self.top_reuser_labels:
            self.top_reuser_labels.append(blame_label)
        if blame_label in self.party_label_to_pct_sendback_map:
             logger.log_and_die(("Label '%s' has already been added as "
                                "send-back reuser to this stats object for "
                                "block height %d: %s") % 
                                (blame_label, self.block_height, str(self)))
        pct = DECIMAL_FORMAT.format(
            100.0 * num_tx_with_sendback_reuse / self.num_tx_total)
        self.party_label_to_pct_sendback_map[blame_label] = pct
    
    def add_history_reuse_blamed_party(self, blame_label, 
                                       num_tx_with_history_reuse):
        if blame_label not in self.top_reuser_labels:
            self.top_reuser_labels.append(blame_label)
        if blame_label in self.party_label_to_pct_history_map:
            logger.log_and_die(("Label '%s' has already been added as sender "
                                "to address with prior history to this stats "
                                "object for block height %d: %s") % 
                                (blame_label, self.block_height, str(self)))
        pct = DECIMAL_FORMAT.format(
                    (100.0 * num_tx_with_history_reuse / self.num_tx_total))
        self.party_label_to_pct_history_map[blame_label] = pct
    
    def to_json_string(self):
        #http://stackoverflow.com/questions/17043860/python-dump-dict-to-json-file
        d = {'Block Height':self.block_height,
            'Total # tx': self.num_tx_total,
            '%% tx w/ send-back reuse': self.pct_tx_with_sendback_reuse,
            '%% tx that send to addresses w/ tx history': 
                self.pct_tx_with_history_reuse,
            'Send-back Reusers:':[{'Name':key, "%% of block":value} for 
                key, value in self.party_label_to_pct_sendback_map.items()],
            'Tx History Reusers:':[{'Name':key, "% of block":value} for
                key, value in self.party_label_to_pct_history_map.items()]}
        return json.dumps(d)
    
    def __str__(self):
        return self.to_json_string()
