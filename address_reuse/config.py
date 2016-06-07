#Description: Reading and storing configuration from a config file

####################
# INTERNAL IMPORTS #
####################

####################
# EXTERNAL IMPORTS #
####################

import ConfigParser         # Configuration file
import sys                  # sys.exit
from enum import IntEnum    #

#TODO: only used for logging. We can remove these if we figure out how to
#  import the logger module without circular imports
import syslog
import time
import datetime

#############
# CONSTANTS #
#############

CONFIG_FILENAME = 'address_reuse.cfg'

#########
# ENUMS #
#########

class BlockchainMode(IntEnum):
    REMOTE_API          = 1
    BITCOIND_RPC        = 2


###########
# CLASSES #
###########

#From: http://stackoverflow.com/questions/3220670/read-all-the-contents-in-ini-file-into-dictionary-with-python
class DictConfigParser(ConfigParser.ConfigParser):
    def as_dict(self):
        d = dict(self._sections)
        for k in d:
            d[k] = dict(self._defaults, **d[k])
            d[k].pop('__name__', None)
        return d

class Config:
    SQLITE_DB_FILENAME                  = None
    SQLITE_DB_REMOTE_FILENAME           = None
    SQLITE_DB_LOCAL_FILENAME            = None
    BLOCKCHAIN_INFO_API_KEY             = None
    WALLETEXPLORER_API_KEY              = None
    API_NUM_SEC_SLEEP                   = None #Sleep this many seconds between HTTP requests
    MAX_NUM_BLOCKS_TO_PROCESS_PER_RUN   = None
    RPC_USERNAME                        = None
    RPC_PASSWORD                        = None
    RPC_HOST                            = None
    RPC_PORT                            = None
    OFFLINE_MODE                        = 0 #Default to online, don't change behavior.
    
    config_parser                       = None
    
    #arg0: sqlite_db_filename (optional): Overrides all other settings for the 
    #   database filename.
    #arg1: blockchain_mode (optional): Selects a database filename from the 
    #   config file based on whether we will retrieve blockchain data from a 
    #   remote API or local bitcoind RPC.
    def __init__(self, sqlite_db_filename = None, 
                 blockchain_mode = BlockchainMode.REMOTE_API):
        if not isinstance(blockchain_mode, BlockchainMode):
            msg = "Blockchain source must be a valid enum value: '%s'" % str(blockchain_mode)
            log_and_die(msg)
        self.config_parser = DictConfigParser()
        try:
            self.config_parser.readfp(open(CONFIG_FILENAME))
        except ConfigParser.Error:
            print_and_log_alert("Could not read or parse in config file '%s'" % CONFIG_FILENAME)
        self.config_parser.read(CONFIG_FILENAME)
        self.read_constants()
        
        if blockchain_mode == BlockchainMode.REMOTE_API:
            self.SQLITE_DB_FILENAME = self.SQLITE_DB_REMOTE_FILENAME
        elif blockchain_mode == BlockchainMode.BITCOIND_RPC:
            self.SQLITE_DB_FILENAME = self.SQLITE_DB_LOCAL_FILENAME
        if sqlite_db_filename is not None:
            self.SQLITE_DB_FILENAME = sqlite_db_filename #overide
    
    def read_constants(self):
        try:
            for section_name in self.config_parser.sections():
                if section_name == 'Database':
                    self.SQLITE_DB_REMOTE_FILENAME = self.config_parser.get(
                        'Database','sqlite_db_remote_filename')
                    self.SQLITE_DB_LOCAL_FILENAME = self.config_parser.get(
                        'Database','sqlite_db_local_filename')
                
                elif section_name == 'API':
                    self.BLOCKCHAIN_INFO_API_KEY = self.config_parser.get(
                        'API','blockchain_info_api_key')
                    self.WALLETEXPLORER_API_KEY = self.config_parser.get(
                        'API','walletexplorer_api_key')
                    self.API_NUM_SEC_SLEEP = self.config_parser.get(
                        'API','api_num_sec_sleep')
                    
                elif section_name == 'RPC':
                    self.RPC_USERNAME = self.config_parser.get('RPC',
                                                               'rpc_username')
                    self.RPC_PASSWORD = self.config_parser.get('RPC',
                                                               'rpc_password')
                    self.RPC_HOST = self.config_parser.get('RPC','rpc_host')
                    self.RPC_PORT = self.config_parser.get('RPC','rpc_port')
                    
                elif section_name == 'General':
                    try:
                        self.MAX_NUM_BLOCKS_TO_PROCESS_PER_RUN = int(
                            self.config_parser.get('General',
                                                   'max_num_blocks_to_process_per_run'))
                    except ValueError:
                        msg = ('Invalid format for max_num_blocks_to_process_'
                               'per_run in config file.')
                        log_and_die(msg)
                    try: #Add parsing for Offline Mode selection.
                        self.OFFLINE_MODE = int(
                            self.config_parser.get('General',
                                                   'offline'))
                    except ValueError:
                        msg = ('Invalid selection for offline mode')
                        log_and_die(msg)
        except ConfigParser.NoOptionError as e:
            log_and_die("Invalid config file: '%s'" % str(e))
        
#############
# FUNCTIONS #
#############

def get_current_timestamp():
    return datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

#TODO: figure out how to refactor so that we don't need to copy/paste these from logger, but don't have circular importing
def log_alert(message):
    timestamp = get_current_timestamp()
    msg_with_stamp = "[%s]: %s" % (timestamp, message)
    syslog.syslog(syslog.LOG_ALERT, msg_with_stamp)

def print_and_log_alert(message):
    print(message)
    log_alert(message)

def log_and_die(message):
    log_alert(message)
    sys.exit(message)
