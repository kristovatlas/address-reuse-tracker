#Description: Get information about blocks and transactions in the Bitcoin
#   blockchain

#TODO: It might be wise to import modules conditionally based on the config
#   file so that people don't have to install the RPC stuff if they only want
#   to use remote APIs.

####################
# INTERNAL IMPORTS #
####################

import http
import config
import logger
import validate
import tx_blame
import db
import custom_errors
import data_subscription

####################
# EXTERNAL IMPORTS #
####################

from time import sleep, clock   #for throttling HTTP requests
import json
import os  #get name of this script for check_int_and_die using os.path.basename

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
import decimal #to help json parser

#############
# CONSTANTS #
#############

#Flag determines whether to consult SQL db for cached transaction output
#   addresses before querying bitcoind via RPC interface.
USE_TX_OUTPUT_ADDR_CACHE_FIRST = True

ENABLE_DEBUG_PRINT = True

#If using an API reader, this flag determines whether we will cache in which
#   block we have seen an address so that, in the future, when we need to
#   determine whether the address has received bitcoin before, we can just
#   refer to the local database cache and not do another HTTP lookup
USE_LOCAL_LABEL_CACHE = True #TODO: Move me to config file

#How many times to retry a remote API query before giving up
MAX_NUM_RETRIES = 10 #TODO: move to config file?

#With this flag set to true, every time we look for the label for a sender,
#   we will aggressively cache this label for every input address. This is
#   how WalletExplorer.com appears to work, so we may as well make it happen
#   locally, as well, to reduce the number of API requests we make for future
#   transactions.
CACHE_LOCALLY_FOR_ALL_INPUT_ADDRESSES = True #TODO: Move me to config file

#With this flag set to true, every time we do a remote lookup to determine
#   the wallet label for a sender, we'll also lookup all of the other addresses
#   assigned to that wallet and cache that data as well. This assumes that, if
#   one wallet's address is involved in address reuse, the remaining addresses
#   in the wallet are also likeyl to be involved in address reuse.
CACHE_ALL_WALLET_ADDRESSES = True #TODO: Move me to config file

THIS_FILE = os.path.basename(__file__)

DB_DEFERRED_BLAME_PLACEHOLDER = 'DB_DEFERRED_BLAME_PLACEHOLDER'

WALLET_EXPLORER_NOT_FOUND_PLACEHOLDER = 'WE_NOT_FOUND_PLACEHOLDER'

#THESE ADDRESSES FOR VARIOUS REASONS WERE NOT INDEXED BY WALLETEXPLORER.COM
'''
ADDRESSES_TO_SKIP_FOR_WALLET_EXPLORER_COM = {
    #address => label to use
    '1NdB761LmTmrJixxp93nz7pEiCx5cKPW44': '1NdB761LmTmrJixxp93nz7pEiCx5cKPW44',
    '157Mbu4yJVVeNhj9Hs4K5KSvTsYCZPFEPb': '157Mbu4yJVVeNhj9Hs4K5KSvTsYCZPFEPb',
    '14yo1jSgQkUDaiKj7bL5JtThTrAymthBew': '14yo1jSgQkUDaiKj7bL5JtThTrAymthBew',
    '17iGxMvWwLKhNhkTZfV9daa3ubqvQqwNyN': '17iGxMvWwLKhNhkTZfV9daa3ubqvQqwNyN',
    '13gu4DCGXjXCscDWStvFVRTbMA2uFtxdHV': '13gu4DCGXjXCscDWStvFVRTbMA2uFtxdHV',
    '1BUiZkSWnvg465viEKJVeghmUtUvdKs2DF': '1BUiZkSWnvg465viEKJVeghmUtUvdKs2DF',
    '1FGmR2qmo1g37LbtXt6WLWSrXEUyNtkn4R': '1FGmR2qmo1g37LbtXt6WLWSrXEUyNtkn4R',
    '1N6zpHfgm8LDZWV6AHLM1ownhrPy4hm323': '1N6zpHfgm8LDZWV6AHLM1ownhrPy4hm323'
}
'''

#If we query WalletExplorer.com and find that this many address in a row haven't
#   been clustered, kill this process. This is meant to detect massive failure
#   of the remote API as opposed to reasonable holes.
NUM_CONSECUTIVE_API_MISSES_TO_DIE = 100;

class BlockchainInfoURLBuilder:

    #empty unless specified in constructor
    api_key_str = ''

    def __init__(self):
        None

    def __init__(self, api_key):
        self.api_key_str = "&api_code=%s" % api_key

    def get_current_height_url(self):
        return 'https://blockchain.info/latestblock?' + \
            self.api_key_str.lstrip('&')

    #This should include all of the transactions for this block; results for
    # /block-height are not broken into pages
    def get_block_at_height_url(self, height):
        validate.check_int_and_die(height,'height', THIS_FILE)
        return ("https://blockchain.info/block-height/%d?format=json%s" %
            (height, self.api_key_str))

    def get_tx_for_address_at_offset(self, address, offset):
        validate.check_address_and_die(address, THIS_FILE)
        validate.check_int_and_die(offset, 'offset', THIS_FILE)
        return ("https://blockchain.info/address/%s?format=json&offset=%d%s" %
            (address, offset, self.api_key_str))

    def get_tx_info(self, tx_id):
        return ("https://blockchain.info/tx/%s?format=json%s" %
            (tx_id, self.api_key_str))

    def get_number_of_transactions_for_address(self, addr):
        return ("https://blockchain.info/address/%s?format=json&limit=0%s" %
            (addr, self.api_key_str))

class WalletExplorerURLBuilder:

    def __init__(self):
        None

    def get_tx_info(self, tx_id, api_key):
        return ("https://www.walletexplorer.com/api/1/tx?txid=%s&caller=%s" %
            (tx_id, api_key))

    def get_address_info(self, addr, api_key):
        return ("https://www.walletexplorer.com/api/1/address?address=%s&"
                "caller=%s&from=0&count=100") % (addr, api_key)

    #API only lists 100 addresses at a time
    def get_wallet_addresses_at_offset(self, wallet_label, offset, api_key):
        return ("http://www.walletexplorer.com/api/1/wallet-addresses?wallet=%s"
                "&from=%d&count=100&caller=%s") % (wallet_label, offset,
                                                   api_key)

#Parent class for all block explorers that defines the functions that should be
#   overriden
class BlockExplorerReader:

    config = None
    database_connector = None

    def __init__(self, database_connector = None):
        if database_connector is None:
            self.database_connector = db.Database() #Create new db conn
        else:
            self.database_connector = database_connector #Use existing db conn
        #Get config from database class so we don't overide a chosen database
        #   filename with whatever is default in the config class.
        self.config = self.database_connector.config_store

    def get_current_blockchain_block_height(self):
        raise NotImplementedError

    def get_tx_list(self, block_height, use_tx_out_addr_cache_only = False):
        raise NotImplementedError

    def is_first_transaction_for_address(self, addr, tx_id, block_height,
                                         benchmarker = None):
        raise NotImplementedError

#TODO: Do more accurate sleeping by sleeping for difference in elapsed time
#   since last request vs the expected throttle, using time.clock(). Then we
#   can allow the throttle to be a float rather than an int. Alternatively, just
#   deprecate the sleep since we are using an API key, and will timeout
#   and/or retry when necessary, which acts as a natural throttling mechanism.
#Uses the Blockchain.info API remotely
class ThrottledBlockchainReader(BlockExplorerReader):

    config = None
    database_connector = None

    #sleep a bit before fetching data from API to avoid bombarding it
    def throttled_fetch_url(self, url):
        if self.config.API_NUM_SEC_SLEEP > 0:
            sleep(float(self.config.API_NUM_SEC_SLEEP))
        return http.fetch_url(url)

    def get_current_blockchain_block_height(self):
        urlbuilder = BlockchainInfoURLBuilder(
            self.config.BLOCKCHAIN_INFO_API_KEY)
        url = urlbuilder.get_current_height_url()

        response = self.throttled_fetch_url(url)
        try:
            jsonObj = json.loads(response)
            height = jsonObj['height']
            try:
                height_as_int = int(height)
                return height_as_int
            except ValueError:
                msg = "Invalid block height returned from API: %s" % str(height)
                logger.log_and_die(msg)
        except ValueError as e:
            #Something weird came back from API despite HTTP 200 OK, panic
            msg = ("Expected JSON response, instead received: '%s'" %
                str(response))
            logger.log_and_die(msg)

    #Return list of transactions for the block at specified height. The
    #   transction list will be the json object decoded from the block explorer
    #   API:
    #https://blockchain.info/api/blockchain_api
    def get_tx_list(self, block_height, use_tx_out_addr_cache_only = False):
        assert not use_tx_out_addr_cache_only #not applicable to remote reader

        urlbuilder = BlockchainInfoURLBuilder(
            self.config.BLOCKCHAIN_INFO_API_KEY)
        url = urlbuilder.get_block_at_height_url(block_height) #validates height

        response = self.throttled_fetch_url(url)
        current_num_retries = 0
        while current_num_retries <= MAX_NUM_RETRIES:
            try:
                jsonObj = json.loads(response)
                #BCI stores the main chain block in addition to orphaned blocks,
                #   so iterate through blocks at this height until we find the
                #   non-orphaned ("main chain") block.
                for i in range(0, len(jsonObj['blocks'])):
                    if jsonObj['blocks'][i]['main_chain'] == True:
                        blockObj = jsonObj['blocks'][i]

                        tx_list = blockObj['tx']
                        return tx_list
                msg = (("Could not find the main chain block in blocks listed "
                        "by remote API at block height %d") % block_height)
                logger.log_and_die(msg)

            except ValueError as e:
                #Something weird came back from API despite HTTP 200 OK, try a
                #   few more times before giving up. For example, sometimes BCI
                #   API will return 'No Free Cluster Connection' when
                #   overloaded.
                current_num_retries = current_num_retries + 1

        #Exceeded maximum time we're willing to wait for the API, time to give
        #   up. Examples of irreconcilable return values: 'Unknown Error
        #   Fetching Blocks From Database' means we tried to grab a block at a
        #   height that doesn't exist yet.
        msg = ("Expected JSON response for block at height %d, instead "
               "received '%s'") % (block_height, str(response))
        logger.log_and_die(msg)

    def get_number_of_transactions_for_address(self, addr):
        validate.check_address_and_die(addr, THIS_FILE)
        urlbuilder = BlockchainInfoURLBuilder(
            self.config.BLOCKCHAIN_INFO_API_KEY)
        url = urlbuilder.get_number_of_transactions_for_address(addr)

        response = self.throttled_fetch_url(url)
        try:
            jsonObj = json.loads(response)
            n_tx = jsonObj['n_tx']
            if not n_tx:
                #panic
                msg = ("Expected 'n_tx' from JSON response but found none: "
                       "'%s'") % (n_tx, str(response))
                logger.log_and_die(msg)
            try:
                int(n_tx)
            except ValueError:
                msg = ("Expected integer value for 'n_tx' in JSON response but "
                       "found: '%s'") % (str(response))
                logger.log_and_die(msg)
            return n_tx
        except ValueError as e:
            #Something weird came back from the API despite HTTP 200 OK, panic
            msg = ("Expected a JSON response for address '%s', instead "
                   "received '%s'") % (addr, str(response))
            logger.log_and_die(msg)

    def is_first_transaction_for_address(self, addr, tx_id, block_height,
                                         benchmarker = None):
        validate.check_address_and_die(addr, THIS_FILE)

        #First, check the local database cache for this address. If it's not
        #   there, add it to the cache as an address that has been seen, and
        #   then do API lookups to determine whether this tx is the address's
        #   first.
        if self.database_connector.has_address_been_seen_cache_if_not(addr,
                                                                      block_height):
            if benchmarker is not None:
                benchmarker.increment_blockchain_info_queries_avoided_by_caching()
                benchmarker.increment_blockchain_info_queries_avoided_by_caching()
            return False

        n_tx = self.get_number_of_transactions_for_address(addr)
        offset = int(n_tx) - 1

        urlbuilder = BlockchainInfoURLBuilder(
            self.config.BLOCKCHAIN_INFO_API_KEY)
        url = urlbuilder.get_tx_for_address_at_offset(addr, offset)

        response = self.throttled_fetch_url(url)
        try:
            jsonObj = json.loads(response)
            tx_list = jsonObj['txs']
            if not tx_list:
                #address has no history
                return True
            if tx_list[0]:
                #Check if the first tx is the tx in question
                first_tx_id = tx_list[0]['hash']
                if first_tx_id == tx_id:
                    return True
                else:
                    return False
            else:
                #Something wrong, this tx list seems to be neither empty nor
                #   contains a first tx, panic!
                msg = ("Expected zero or one transactions for address '%s'" %
                    addr)
                logger.log_and_die(msg)
        except ValueError as e:
            #Something weird came back from the API despite HTTP 200 OK, panic
            msg = ("Expected a JSON response for address '%s', instead "
                   "received '%s'") % (addr, str(response))
            logger.log_and_die(msg)

    #Retrieves 'relayed by' field from BCI API. First, it will check if this
    #   information has been locally cached in the database. If not, it
    #   will be retrieved via HTTP and the helper function
    #   get_tx_relayed_by_using_txObj() will cache the result for future
    #   queries.
    def get_tx_relayed_by_using_tx_id(self, tx_id, txObj = None,
                                      benchmarker = None):
        cached_relayed_by = self.database_connector.get_cached_relayed_by(tx_id)
        dprint("DB Cached relayed-by field for tx %s is: %s" %
               (tx_id, str(cached_relayed_by)))
        if cached_relayed_by is not None:
            if benchmarker is not None:
                benchmarker.increment_blockchain_info_queries_avoided_by_caching()
            return cached_relayed_by

        urlbuilder = BlockchainInfoURLBuilder(
            self.config.BLOCKCHAIN_INFO_API_KEY)
        url = urlbuilder.get_tx_info(tx_id)
        response = self.throttled_fetch_url(url)

        try:
            jsonObj = json.loads(response)
            return self.get_tx_relayed_by_using_txObj(jsonObj)
        except ValueError as e:
            #Something went wrong, panic
            msg = ("Expected JSON response for tx id '%s', instead received "
                   "'%s'") % (str(tx_id), str(response))
            logger.log_and_die(msg)

    #Using JSON data retrieved from remote API call, get the 'relayed by'
    #   field from BCI's remote API, and then cache it in the databse.
    def get_tx_relayed_by_using_txObj(self, txObj):
        try:
            relayed_by = txObj['relayed_by']
            block_height = txObj['block_height']
            tx_id = txObj['hash']
            self.database_connector.record_relayed_by(tx_id, block_height,
                                                      relayed_by)
            return relayed_by
        except IndexError as e:
            msg = ("relayed_by field missing from tx JSON object: %s" %
                str(txObj))
            logger.log_and_die(msg)

#Helps JSON encoder cope with floating point values in JSON
#http://stackoverflow.com/questions/1960516/python-json-serialize-a-decimal-object
class DecimalEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

#Uses bitcoind's RPC interface to query about the state of the blockchain as
#   reflected locally. This (hopefully) is much faster than querying a remote
#   API.
#TODO: Raise a custom error if trying to fetch information not yet in bitcoind's DB
class LocalBlockchainRPCReader(BlockExplorerReader):

    rpc_connection              = None
    #transaction_output_cache    = None ''' deprecated '''

    def __init__(self, database_connector = None):
        BlockExplorerReader.__init__(self, database_connector) #super

        self.rpc_connection = AuthServiceProxy(
            "http://%s:%s@%s:%s" % (self.config.RPC_USERNAME,
                                    self.config.RPC_PASSWORD,
                                    self.config.RPC_HOST,
                                    self.config.RPC_PORT))

    def get_current_blockchain_block_height(self):
        return self.rpc_connection.getblockcount() #TODO: Error checking? - This should already be an integer.

    #Retreives a list of transactions at specified block height. Each tx
    #   will be formatted as a BCI-like tuple per
    #   get_bci_like_tuple_for_tx_id().
    #param0: block_height: Height at which to get a list of txs for.
    #param1: use_tx_out_addr_cache_only (Optional): When looking up addresses
    #   for previous transactions, ONLY refer to cache in SQLite database,
    #   rather than slower option of using RPC interface. If set to True,
    #   process will sleep until the data is available in the cache. Default:
    #   False.
    def get_tx_list(self, block_height, use_tx_out_addr_cache_only = False):
        ids = self.get_tx_ids_at_height(block_height)

        txs = []
        for tx_id in ids:
            bci_like_tuple = self.get_bci_like_tuple_for_tx_id(
                tx_id, use_tx_out_addr_cache_only)
            txs.append(bci_like_tuple)
        return txs

    #Checks if the specified transaction is the first time the specified address
    #   has received funds. If it is, it will cache this for the specified
    #   block height in the database so subsequent lookups will answer
    #   correctly. IMPORTANT: This function assumes that that blocks are being
    #   processed in a complete, monotonically-increasing fashion from the
    #   genesis block. Otherwise, correct results not guaranteed! It is the
    #   caller's responsibility to ensure that enough blocks have been
    #   processed.
    def is_first_transaction_for_address(self, addr, tx_id, block_height,
                                         benchmarker = None):
        if self.database_connector.has_address_been_seen_cache_if_not(addr,
                                                                      block_height):
            dprint("Address %s at block height %d was already seen." %
                (addr, block_height))
            return False
        else:
            dprint("Address %s at block height %d has no prior tx history." %
                  (addr, block_height))
            return True

    def get_block_hash_at_height(self, block_height):
        return self.rpc_connection.getblockhash(block_height)

    def get_tx_json_for_block_hash(self, block_hash):
        return self.rpc_connection.getblock(block_hash)

    def get_tx_ids_at_height(self, block_height):
        block_hash = self.get_block_hash_at_height(block_height)
        tx_json = self.get_tx_json_for_block_hash(block_hash)
        tx_ids = []
        for tx_id in tx_json['tx']:
            tx_ids.append(tx_id)
        return tx_ids

    #Returns the transaction in raw format. If the requested transaction is
    #   the sole transaction of the genesis block, bitcoind's RPC interface
    #   will throw an error 'No information available about transaction
    #   (code -5)' so we preempt this by raising a custom error that callers
    #   should handle; iterating callers should just move onto the next tx.
    #throws: NoDataAvailableForGenesisBlockError
    def get_raw_tx(self, tx_id):
        if tx_id == ('4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7af'
                     'deda33b'):
            raise custom_errors.NoDataAvailableForGenesisBlockError()
        else:
            return self.rpc_connection.getrawtransaction(tx_id)

    #Gets a human-readable string of the transaction in JSON format.
    def get_decoded_tx(self, tx_id):
        try:
            return self.rpc_connection.decoderawtransaction(
                self.get_raw_tx(tx_id))
        except custom_errors.NoDataAvailableForGenesisBlockError:
            #bitcoind won't generate this, but here's what it would look like
            genesis_json = {
                'txid':    ('4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2'
                            '127b7afdeda33b'),
                'version':  1,
                'locktime': 0,
                'vin': [{
                    "sequence":4294967295,
                    'coinbase': ('04ffff001d0104455468652054696d65732030332f4a6'
                                 '16e2f32303039204368616e63656c6c6f72206f6e2062'
                                 '72696e6b206f66207365636f6e64206261696c6f75742'
                                 '0666f722062616e6b73')
                }],
                'vout': [
                    {
                        'value': 50.00000000,
                        'n': 0,
                        'scriptPubKey': {
                            'asm': ('04678afdb0fe5548271967f1a67130b7105cd6a828'
                                    'e03909a67962e0ea1f61deb649f6bc3f4cef38c4f3'
                                    '5504e51ec112de5c384df7ba0b8d578a4c702b6bf1'
                                    '1d5f OP_CHECKSIG'),
                            'hex': ('4104678afdb0fe5548271967f1a67130b7105cd6a8'
                                    '28e03909a67962e0ea1f61deb649f6bc3f4cef38c4'
                                    'f35504e51ec112de5c384df7ba0b8d578a4c702b6b'
                                    'f11d5fac'),
                            'reqSigs': 1,
                            'type': 'pubkey',
                            'addresses': ['1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa']
                        }
                    }
                ]
            }
            return genesis_json

    #Converts required infromation from local bitcoind RPC into a format similar
    #   to that returned by Blockchain.info's API. This helps to make the code
    #   more agnostic as to the source of blockchain data.
    #Note: When an output address cannot be decoded, BCI excludes the "addr"
    #   field from the JSON returned. Therefore, this function will do the same.
    #   See:
    #   https://blockchain.info/tx/cee16a9b222f636cd27d734da0a131cee5dd7a1d09cb5f14f4d1330b22aaa38e
    #Note: When a previous output address for an input cannot be decoded, BCI
    #   excludes the "addr" field from the JSON returned. Therefore, this
    #   function will do the same. See:
    #   https://blockchain.info/tx/8ebe1df6ebf008f7ec42ccd022478c9afaec3ca0444322243b745aa2e317c272
    #param0: tx_id: Specified transaction hash
    #param1: use_tx_out_addr_cache_only (Optional): When looking up addresses
    #   for previous transactions, ONLY refer to cache in SQLite database,
    #   rather than slower option of using RPC interface. If set to True,
    #   process will sleep until the data is available in the cache. Default:
    #   False.
    def get_bci_like_tuple_for_tx_id(self, tx_id,
                                     use_tx_out_addr_cache_only = False):
        json_tuple = {}
        json_tuple['hash'] = tx_id
        json_tuple['inputs'] = []
        json_tuple['out'] = []

        subscription = None
        if use_tx_out_addr_cache_only:
            subscription = data_subscription.TxOutputAddressCacheSubscriber(
                database = self.database_connector)

        tx_json = self.get_decoded_tx(tx_id)

        #populate input addresses
        for vin in tx_json['vin']:
            #look up address based on its previous transaction
            prev_txid = None
            if 'txid' in vin:
                prev_txid = vin['txid']
            prev_vout = None
            if 'vout' in vin:
                prev_vout_num = vin['vout'] #yes, this RPC field is poorly named
                prev_out = {'n': prev_vout_num}
                try:
                    if use_tx_out_addr_cache_only:
                        #flag specifies that we will wait for cache to catch up
                        #   before continuing this operation. Process/thread
                        #   will sleep until then.
                        subscription.next_tx_id_needed = prev_txid
                        subscription.next_prev_tx_ouput_pos_needed = prev_vout_num
                        dprint(("get_bci_like_tuple_for_tx_id: May sleep until "
                                "tx output address is cached..."))
                        subscription.do_sleep_until_producers_ready()

                    address = self.get_output_address(prev_txid, prev_vout_num)
                    prev_out['addr'] = address
                except custom_errors.PrevOutAddressCannotBeDecodedError:
                    pass
                current_input = {'prev_out': prev_out}
                json_tuple['inputs'].append(current_input)
            else:
                #If there's no index specifying the txo from prev tx, there's
                #   probably nothing to do here. Should only come up for
                #   coinbase transactions.
                continue

        #populate output addresses
        for vout in tx_json['vout']:
            output_index = vout['n']
            current_output = {'n':output_index}
            if 'scriptPubKey' in vout and 'addresses' in vout['scriptPubKey']:
                address = vout['scriptPubKey']['addresses'][0]
                current_output['addr'] = address
            json_tuple['out'].append(current_output)

        return json_tuple

    #Returns an ordered list of output addresses for the specified transaction
    #   JSON as returned by the bitcoind RPC interface. If an address cannot be
    #   decoded for one of the outputs, a value of None will be inserted
    #   at that position in the list.
    #TODO: This does not properly handle multisig outputs that list multiple
    #   addresses per output. See:
    #   http://bitcoin.stackexchange.com/questions/4687/can-a-scriptpubkey-have-multiple-addresses
    #   When support for this is added, make sure to add a test case.
    def get_output_addresses(self, tx_json):
        assert 'vout' in tx_json
        output_addresses = []
        for vout in tx_json['vout']:
            assert 'scriptPubKey' in vout
            if 'addresses' in vout['scriptPubKey']:
                ouput_address = vout['scriptPubKey']['addresses'][0]
                output_addresses.append(ouput_address)
            else:
                output_addresses.append(None)
        return output_addresses

    #Raises: custom_errors.PrevOutAddressCannotBeDecoded
    #TODO: This does not properly handle multisig outputs that list multiple
    #   addresses per output.
    def get_output_address(self, tx_id, output_index, tx_json = None):
        if USE_TX_OUTPUT_ADDR_CACHE_FIRST:
            addr = self.database_connector.get_output_address(tx_id,
                                                              output_index)
            if addr is not None:
                return addr

        #not in cache, fall back to querying RPC interface
        if tx_json is None:
            tx_json = self.get_decoded_tx(tx_id)

        if 'vout' in tx_json and len(tx_json['vout']) > output_index and \
                'scriptPubKey' in tx_json['vout'][output_index]:
            if 'addresses' not in tx_json['vout'][output_index]['scriptPubKey']:
                raise custom_errors.PrevOutAddressCannotBeDecodedError
            else:
                return tx_json['vout'][output_index]['scriptPubKey'][
                    'addresses'][0]
        else:
            msg = ("Missing element for vout in get_output_address() with tx "
                   "id %s and output index %d") % (tx_id, output_index)
            logger.log_and_die(msg)

#Queries WalletExplorer.com for cluster analysis information about addresses.
#   This information can be used to blame particular parties for address reuse.
class WalletExplorerReader:

    config = None
    database_connector = None
    consecutive_lookup_misses = None

    def __init__(self, database_connector = None):
        self.config = config.Config()
        if database_connector is None:
            self.database_connector = db.Database()      #Create new db conn
        else:
            self.database_connector = database_connector #Use existing db conn
        self.consecutive_lookup_misses = 0

    #sleep a bit before fetching data from API to avoid bombarding it
    def fetch_url(self, url):
        return http.fetch_url(url)

    #Returns the label for a bitcoin address if it can retreived from the
    #   cache, otherwise returns None
    def get_label_from_cache(self, address):
        if not USE_LOCAL_LABEL_CACHE:
            return None
        else:
            cached_label = self.database_connector.get_blame_label_for_btc_address(
                address)
            if cached_label == 'DB_DEFERRED_BLAME_PLACEHOLDER':
                return None
            else:
                return cached_label

    #Returns the label for a sender's bitcoin address based on the JSON
    #   returned by WE.com. Returns None if no label is specified by WE.com.
    def get_sender_label_from_json(self, remote_json):
        if remote_json is None:
            logger.log_and_die(("Called get_sender_label_from_json() with a "
                                "'remote_json' value of None."))
        try:
            if remote_json['is_coinbase']:
                return None #There is no "sender" in a coinbase transaction, only a receiver.
            elif remote_json['wallet_id'] is not None and \
                    remote_json['wallet_id']:
                return remote_json['wallet_id']
            else:
                #WE.com has no label for this address, for some reason that I
                #   am not currently curious about :>
                return None
        except IndexError as e:
            msg = (("One or more expected fields in the tx JSON object are "
                   "missing: '%s'. Exception: '%s'") %
                    (str(remote_json), str(e)))
            logger.log_and_die(msg)

    def get_receiver_label_from_json(self, remote_json, receiver_address):
        if remote_json is None:
            logger.log_and_die(("Called get_receiver_label_from_json() with a "
                                "'remote_json' value of None."))

        #find the address in the outputs
        receiver = ''
        try:
            for btc_output in remote_json['out']:
                out_addr = btc_output['address']
                if out_addr == receiver_address:
                    return btc_output['wallet_id']
            if not receiver:
                #didn't find a matching address, panic
                msg = ("Looked for addr '%s' in outputs but couldn't find the "
                       "matching output") % receiver_address
                logger.log_and_die(msg)
        except IndexError as e:
            msg = (("One or more expected fields in the tx JSON object are "
                   "missing: '%s'. Exception: '%s'") %
                    (str(remote_json), str(e)))
            logger.log_and_die(msg)

    #Whereas get_wallet_labels() will grab wallet labels for many inputs and
    #   outputs of a transaction, this funtion fetches the label for only a
    #   single bitcoin address. Used when updating specific blame records in
    #   the database after deferring blaming during local RPC blockchain
    #   processing.
    def get_wallet_label_for_single_address(self, addr):
        label = None

        #if addr in ADDRESSES_TO_SKIP_FOR_WALLET_EXPLORER_COM:
        #    return ADDRESSES_TO_SKIP_FOR_WALLET_EXPLORER_COM[addr]

        if USE_LOCAL_LABEL_CACHE:
            label = self.get_label_from_cache(addr)

        if label is None:
            #Must query remote API via HTTP
            try:
                remote_json = self.get_address_json_net(addr)
            except custom_errors.NotFoundAtRemoteAPIError:
                #A small percentage of addresses are not clustered. For those,
                #   just use a placeholder for now.
                return WALLET_EXPLORER_NOT_FOUND_PLACEHOLDER

            if 'label' in remote_json:
                label = remote_json['label']
            if 'wallet_id' not in remote_json:
                return None
            if label is None:
                #If no label field set, use remote API's wallet_id field
                #   instead, which is just a random alphanum string
                label = remote_json['wallet_id']

            cached = self.database_connector.cache_blame_label_for_btc_address(
                addr, label)
            if cached:
                pass
            else:
                pass #TODO can handle this differently

            if CACHE_ALL_WALLET_ADDRESSES:
                #Aggressively query and cache all addresses in this wallet
                addresses = self.get_addresses_for_wallet_label_net(label)
                for addr in addresses:
                    #TODO: Need to create an executemany version of this, very
                    #   slow to insert one record at a time for big wallets
                    cached = self.database_connector.cache_blame_label_for_btc_address(
                        addr, label)
                    if cached:
                        pass
                    else:
                        #WE.com sometimes has weirdly formatted addresses
                        #   returned for this API call such as
                        #   '#multisig_a74f4a173bb335f7_1'. These will be
                        #   rejected by a validation check, result in a False
                        #   return value. We can safely ignore this.
                        pass

        return label

    #Helper function for get_addresses_for_wallet_label_net() extracts addresses
    #   from JSON retrieved from remote API.
    def get_address_list_from_json(self, address_list_json):
        addresses = []
        for item in address_list_json['addresses']:
            address = item['address']
            addresses.append(address)
        return addresses

    def get_label_for_wallet_id_net(self, wallet_id):
        api_key = self.config.WALLETEXPLORER_API_KEY
        urlbuilder = WalletExplorerURLBuilder()
        url = urlbuilder.get_wallet_addresses_at_offset(
            wallet_label = wallet_id, offset = 0, api_key = api_key)
        json = None
        try:
            json = self.get_json_net(url)
        except custom_errors.NotFoundAtRemoteAPIError:
            return None
        try:
            label  = json['label']
            return label
        except KeyError:
            return None

    #Does a remote API lookup to fetch all addresses that belong to the wallet
    #   with the specified label such as 'BTC-e.com' or '637e58bb505ab93d'
    def get_addresses_for_wallet_label_net(self, blame_label):
        offset = 0

        api_key = self.config.WALLETEXPLORER_API_KEY
        urlbuilder = WalletExplorerURLBuilder()
        url = urlbuilder.get_wallet_addresses_at_offset(blame_label, offset,
                                                        api_key)
        json = self.get_json_net(url)

        addresses = self.get_address_list_from_json(json)

        addresses_count = 0
        try:
            addresses_count = json['addresses_count']
        except KeyError as e:
            logger.log_and_die(str(e))

        #start by retrieving up to 100 addresses, continue until all fetched.
        addresses_remaining = addresses_count - 100
        offset = 100
        while addresses_remaining > 0:
            url = urlbuilder.get_wallet_addresses_at_offset(blame_label,
                                                            offset, api_key)
            json = self.get_json_net(url)
            addresses.extend(self.get_address_list_from_json(json))

            addresses_remaining = addresses_remaining - 100
            offset = offset + 100

        return addresses

    #Looks in local database cache for all labels that we need. If not all of
    #   the labels we need are cached, a single HTTP request is made to look
    #   them up remotely, and then we cache relevant info for future queries.
    #   The purpose of this is to try to avoid remote lookups whenever possible
    #parm0: tx_id: The transaction ID of the transaction in question containing
    #   address reuse
    #param1: sender_address_list: A list of input addresses for the transaction.
    #   We need this information to determine whether we have any information
    #   locally cached for the sender.
    #param2: reused_output_address: A bitcoin address that has been implicated
    #   as an output in an instance of address reuse. If this is send-back
    #   address reuse, then the address will also be listed in the inputs of
    #   the transaction.
    #TODO: This function is a bit long, consider breaking it up
    def get_wallet_labels(self, tx_id, input_address_list,
                          reused_output_address, benchmarker = None,
                          defer_blaming = False):
        if tx_id == ('4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7af'
                     'deda33b'):
            return [] #The sole tx of the genesis block. Return empty list

        blame_records = []
        remote_json = None #Only fill this in with an HTTP request if we don't have everything cached.

        #Add record for sender
        sender_label = None
        if defer_blaming:
            sender_label = DB_DEFERRED_BLAME_PLACEHOLDER
        else:
            if USE_LOCAL_LABEL_CACHE:
                for input_address in input_address_list:
                    cached_label = self.get_label_from_cache(input_address)
                    if cached_label is not None:
                        #Sender label is cached
                        sender_label = cached_label
            else:
                #The label for the input addresses has never been cached. Resort to
                #   HTTP query.
                remote_json = self.get_transaction_json_net(tx_id)
                sender_label = self.get_sender_label_from_json(remote_json)

        if sender_label is not None:
            #Obtanied sender label either from local cache or remote query. Add
            #   to blame records.
            blame_record = tx_blame.BlameRecord(sender_label, db.AddressReuseRole.SENDER, db.DataSource.WALLET_EXPLORER)
            blame_records.append(blame_record)

            if CACHE_LOCALLY_FOR_ALL_INPUT_ADDRESSES:
                for input_address in input_address_list:
                    cached = self.database_connector.cache_blame_label_for_btc_address(
                        input_address, sender_label)
                    if cached:
                        pass
                    else:
                        pass #TODO: can modify this later if we wish


        #Add record for receiver
        receiver_label = None
        if USE_LOCAL_LABEL_CACHE:
            receiver_label = self.get_label_from_cache(reused_output_address)

        if receiver_label is None:
            if defer_blaming:
                receiver_label = DB_DEFERRED_BLAME_PLACEHOLDER
            else:
                #The label for the output address has never been cached. Resort
                #   to HTTP query.
                if remote_json is None: #Don't make more than one HTTP query.
                    remote_json = self.get_transaction_json_net(tx_id)
                receiver_label = self.get_receiver_label_from_json(remote_json,
                                                                   reused_output_address)

        if receiver_label is not None:
            #Obtained receiver label either from local cache or remote query.
            #   Add to blame records.
            blame_record = tx_blame.BlameRecord(receiver_label,
                                                db.AddressReuseRole.RECEIVER,
                                                db.DataSource.WALLET_EXPLORER)
            blame_records.append(blame_record)
            cached = self.database_connector.cache_blame_label_for_btc_address(
                reused_output_address, receiver_label)
            if cached:
                pass
            else:
                pass #TODO: can handle this differently

        if remote_json is None and benchmarker is not None:
            benchmarker.increment_wallet_explorer_queries_avoided_by_caching()

        return blame_records

    #Fetch a JSON reponse for given url belong to WalletExplorer.com via HTTP.
    #Error conditions handled: WE.com returns found:false in the JSON.
    #If object cannot be found at remote API for consecutive calls, a counter
    #   is incremented until it reaches NUM_CONSECUTIVE_API_MISSES_TO_DIE,
    #   otherwise just raises raises a NotFoundAtRemoteAPIError.
    def get_json_net(self, url):
        response = self.fetch_url(url)
        try:
            jsonObj = json.loads(response)
        except ValueError as e:
            #Something went wrong with JSON response from API, panic
            msg = (("Expected JSON response from '%s' instead received '%s'") %
                (url, str(response)))
            logger.log_and_die(msg)

        if jsonObj['found']:
            self.consecutive_lookup_misses = 0
            return jsonObj
        else:
            #Some addresses are not clustered due to limitations of the
            #   WalletExplorer.com API; for example, m-of-n escrow
            #   addresses.
            self.consecutive_lookup_misses = (
                self.consecutive_lookup_misses + 1)
            dprint("Not found for url: %s. %d consecutive misses so far." %
                   (url, self.consecutive_lookup_misses))
            if (self.consecutive_lookup_misses ==
                    NUM_CONSECUTIVE_API_MISSES_TO_DIE):
                msg = ("Error: Encountered %d consecutive misses to "
                       "WalletExplorer.com API. Please investigate and "
                       "increase the package constant "
                       "NUM_CONSECUTIVE_API_MISSES_TO_DIE if everything's "
                       "okay.") % NUM_CONSECUTIVE_API_MISSES_TO_DIE
                logger.log_and_die(msg)
            else:
                raise custom_errors.NotFoundAtRemoteAPIError

    #Fetch information for a given address from WalletExplorer.com via HTTP
    #If informationf or the specified address is not found at the remote API,
    #   raises custom_errors.NotFoundAtRemoteAPIError.
    def get_address_json_net(self, addr):
        api_key = self.config.WALLETEXPLORER_API_KEY
        urlbuilder = WalletExplorerURLBuilder()
        url = urlbuilder.get_address_info(addr, api_key)
        try:
            json = self.get_json_net(url)
            return json
        except custom_errors.NotFoundAtRemoteAPIError:
            raise

    #Fetch information for a given transaction from WalletExplorer.com via HTTP
    #Error conditions handled: WE.com returns found:false in the JSON.
    def get_transaction_json_net(self, tx_id):
        api_key = self.config.WALLETEXPLORER_API_KEY
        urlbuilder = WalletExplorerURLBuilder()
        url = urlbuilder.get_tx_info(tx_id, api_key)
        return self.get_json_net(url)

#############
# FUNCTIONS #
#############

def dprint(str):
    if ENABLE_DEBUG_PRINT:
        print("DEBUG: %s" % str)
