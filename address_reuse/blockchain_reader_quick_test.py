#Covers these classes and functions:
#   LocalBlockchainRPCReader:
#       get_block_hash_at_height
#       get_tx_json_for_block_hash
#       get_tx_ids_at_height
#       get_raw_tx(tx_id)
#       get_decoded_tx(tx_id)
#       get_bci_like_tuple_for_tx_id(tx_id)
#       get_output_addresses(tx_json)
#       get_output_address(tx_id, output_index, [tx_json])
#       get_tx_list(block_height)
#       is_first_transaction_for_address(addr, tx_id, block_height, benchmarker)
#
#   ThrottledBlockchainReader:
#       get_tx_relayed_by_using_tx_id(tx_id, txObj, benchmarker)
#           * only tests whether cache is used, not remote API lookup
#
#   WalletExplorerReader:
#       get_address_list_from_json(address_list_json)

####################
# INTERNAL IMPORTS #
####################

import address_reuse.blockchain_reader
import address_reuse.custom_errors
import address_reuse.block_processor
import address_reuse.benchmark.block_reader_benchmark
import address_reuse.db

####################
# EXTERNAL IMPORTS #
####################

import unittest
import os
import json

#############
# CONSTANTS #
#############

GENESIS_TX_AS_BCI_LIKE_TUPLE = {
            'hash': ('4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7af'
                     'deda33b'),
            'inputs': [],
            'out': [
                {
                    'n': 0,
                    'addr': '1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa'
                }
            ]
        }

FIRST_TX_BLOCK_HEIGHT_187_AS_BCI_LIKE_TUPLE = {
            'hash': ('70587f1780ccd2ebbace28a7b33d83d19f4362f10ff7a4ad88f8c4138'
                     '83f94b7'),
            'inputs': [],
            'out': [
                {
                    'n': 0,
                    'addr': '1FDMwEo8qNa9icVcooBUoGvA6NriePtJJ3'
                }
            ]
        }

SECOND_TX_BLOCK_HEIGHT_187_AS_BCI_LIKE_TUPLE = {
            'hash': ('4385fcf8b14497d0659adccfe06ae7e38e0b5dc95ff8a13d7c6203599'
                     '4a0cd79'),
            'inputs': [
                {
                    'prev_out': {
                        'addr': '13HtsYzne8xVPdGDnmJX8gHgBZerAfJGEf',
                        'n': 0
                    }
                }
            ],
            'out': [
                {
                    'n': 0,
                    'addr': '15NUwyBYrZcnUgTagsm1A7M2yL2GntpuaZ'
                }
            ]
        }

SAMPLE_ADDRESS_LIST_JSON_AS_STR = '''
{
    "found": true,
    "wallet_id": "07a838551a4aaa2f",
    "addresses_count": 2,
    "addresses": [
        {
            "address": "1NbrsBgcktga92XFbcNuCGaPc1BAJoydKP",
            "balance": 3.8341741412878,
            "incoming_txs": 132,
            "last_used_in_block": 388071
        },
        {
            "address": "1AsiYDrudPY3yTGZ4ArdYeiCdP7297niqe",
            "balance": 2.4305099584162,
            "incoming_txs": 228,
            "last_used_in_block": 388311
        }
    ],
    "updated_to_block": 388447
}
'''


TEMP_DB_FILENAME = 'address_reuse.db-temp'

#####################
# TEST CASE CLASSES #
#####################

class LocalBlockchainRPCReaderTestCase(unittest.TestCase):
    
    database_connector  = None
    reader              = None
    
    def setUp(self):
        try:
            os.remove(TEMP_DB_FILENAME)
        except OSError:
            pass
        self.database_connector = address_reuse.db.Database(TEMP_DB_FILENAME)
        
        self.reader = address_reuse.blockchain_reader.LocalBlockchainRPCReader(
            self.database_connector)
        
    def tearDown(self):
        self.reader = None
    
    def do_get_block_hash_at_height(self, block_height, expected_hash):
        block_hash = self.reader.get_block_hash_at_height(block_height)
        self.assertEqual(block_hash, expected_hash, 
                         'Block hash at height %d is incorrect. Expected %s, recieved %s' % (block_height, expected_hash, block_hash))
        
    def test_get_block_hash_at_height_zero(self):
        expected_hash = '000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f'
        self.do_get_block_hash_at_height(0, expected_hash)
    
    def test_get_block_hash_at_height_100(self):
        expected_hash = '000000007bc154e0fa7ea32218a72fe2c1bb9f86cf8c9ebf9a715ed27fdb229a'
        self.do_get_block_hash_at_height(100, expected_hash)
    
    def do_get_tx_json_for_block_hash(self, block_height, block_hash, expected_fields):
        tx_json = self.reader.get_tx_json_for_block_hash(block_hash)
        self.assertEqual(len(tx_json), len(expected_fields), 
                         'Expected a list of %d elments but received %d. at block height %d' % (len(expected_fields), len(tx_json), block_height))
        for field in expected_fields:
            self.assertIn(field, tx_json, 
                          'Could not find expected field %s in received tx JSON at block height %d' % (field, block_height))
        self.assertEqual(tx_json['hash'], block_hash, 'Block hashes do not match. Expected %s, received %s' % (block_hash, tx_json['hash']))
        self.assertEqual(tx_json['height'], block_height, 'Block height does not match. Expected %s, received %s' % (block_height, tx_json['height']))
    
    def test_get_tx_json_for_block_hash_at_height_zero(self):
        block_height = 0
        block_hash = '000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f'
        expected_fields = ['hash', 'confirmations', 'size', 'height', 'version', 
                           'merkleroot', 'tx', 'time', 'nonce', 'bits', 
                           'difficulty','chainwork', 'nextblockhash']
        self.do_get_tx_json_for_block_hash(block_height, block_hash, 
                                           expected_fields)
     
    def test_get_tx_json_for_block_hash_at_height_187(self):
        block_height = 187
        block_hash = '00000000b2cde2159116889837ecf300bd77d229d49b138c55366b54626e495d'
        expected_fields = ['hash', 'confirmations', 'size', 'height', 'version', 
                           'merkleroot', 'tx', 'time', 'nonce', 'bits', 
                           'difficulty','chainwork', 'previousblockhash', 
                           'nextblockhash']
        self.do_get_tx_json_for_block_hash(block_height, block_hash, 
                                           expected_fields)
    
    def do_get_tx_ids_at_height(self, block_height, expected_ids):
        tx_ids = self.reader.get_tx_ids_at_height(block_height)
        self.assertEqual(len(tx_ids), len(expected_ids), 
                         'Expected a list of %d tx ids but received %d. at block height %d' % (len(expected_ids), len(tx_ids), block_height))
        for expected_id in expected_ids:
            self.assertIn(expected_id, tx_ids, 
                          'Could not find expected tx id %s in received tx ids at block height %d' % (expected_id, block_height))
    
    def test_get_tx_ids_at_height_zero(self):
        block_height = 0
        expected_ids = ['4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b']
        self.do_get_tx_ids_at_height(block_height, expected_ids)
        
    def test_get_tx_ids_at_height_187(self):
        block_height = 187
        expected_ids = ['70587f1780ccd2ebbace28a7b33d83d19f4362f10ff7a4ad88f8c413883f94b7',
                        '4385fcf8b14497d0659adccfe06ae7e38e0b5dc95ff8a13d7c62035994a0cd79']
        self.do_get_tx_ids_at_height(block_height, expected_ids)
    
    def do_get_raw_tx(self, tx_id, expected_raw_tx):
        raw_tx = self.reader.get_raw_tx(tx_id)
        self.assertEqual(expected_raw_tx, raw_tx, 'For tx %s expected raw tx %s but received %s.' % (tx_id, expected_raw_tx, raw_tx))
    
    def test_get_raw_tx_at_height_zero(self):
        tx_id = '4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b'
        with self.assertRaises(address_reuse.custom_errors.NoDataAvailableForGenesisBlockError):
            self.do_get_raw_tx(tx_id, None)

    def test_get_second_raw_tx_at_height_187(self):
        tx_id = '4385fcf8b14497d0659adccfe06ae7e38e0b5dc95ff8a13d7c62035994a0cd79'
        raw_tx = ('0100000001ba91c1d5e55a9e2fab4e41f55b862a73b24719aad13a527d16'
                  '9c1fad3b63b5120000000048473044022041d56d649e3ca8a06ffc10dbc6'
                  'ba37cb958d1177cc8a155e83d0646cd5852634022047fd6a02e26b00de9f'
                  '60fb61326856e66d7a0d5e2bc9d01fb95f689fc705c04b01ffffffff0100'
                  'e1f50500000000434104fe1b9ccf732e1f6b760c5ed3152388eeeadd4a07'
                  '3e621f741eb157e6a62e3547c8e939abbd6a513bf3a1fbe28f9ea85a4e64'
                  'c526702435d726f7ff14da40bae4ac00000000')
        self.do_get_raw_tx(tx_id, raw_tx)
    
    def do_get_decoded_tx(self, tx_id, expected_fields_map):
        decoded_tx = self.reader.get_decoded_tx(tx_id)
        
        for key, value in expected_fields_map.iteritems():
            self.assertIn(key, decoded_tx, 'Transaction %s missing field %s' % (tx_id, key))
            self.assertEqual(value, decoded_tx[key], 'Expected %s in tx %s to be %s, received %s' % (key, tx_id, value, decoded_tx[key]))
        self.assertEqual(decoded_tx, expected_fields_map, 'Transaction %s mismatch.' % tx_id)
    
    def test_get_decoded_tx_at_height_zero(self):
        tx_id = '4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b'
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
        self.do_get_decoded_tx(tx_id, genesis_json)
    
    def test_get_second_decoded_tx_at_height_187(self):
        tx_id = '4385fcf8b14497d0659adccfe06ae7e38e0b5dc95ff8a13d7c62035994a0cd79'
        expected_json = {
            'txid': ('4385fcf8b14497d0659adccfe06ae7e38e0b5dc95ff8a13d7c6203599'
                     '4a0cd79'),
            'version': 1,
            'locktime': 0,
            'vin': [
                {
                    'txid': ('12b5633bad1f9c167d523ad1aa1947b2732a865bf5414eab2'
                             'f9e5ae5d5c191ba'),
                    'vout': 0,
                    'scriptSig': {
                        'asm': ('3044022041d56d649e3ca8a06ffc10dbc6ba37cb958d11'
                                '77cc8a155e83d0646cd5852634022047fd6a02e26b00de'
                                '9f60fb61326856e66d7a0d5e2bc9d01fb95f689fc705c0'
                                '4b01'),
                        'hex': ('473044022041d56d649e3ca8a06ffc10dbc6ba37cb958d'
                                '1177cc8a155e83d0646cd5852634022047fd6a02e26b00'
                                'de9f60fb61326856e66d7a0d5e2bc9d01fb95f689fc705'
                                'c04b01')
                    },
                    'sequence': 4294967295,
                }
            ],
            'vout': [
                {
                    'value': 1.00000000,
                    'n': 0,
                    'scriptPubKey': {
                        'asm': ('04fe1b9ccf732e1f6b760c5ed3152388eeeadd4a073e62'
                                '1f741eb157e6a62e3547c8e939abbd6a513bf3a1fbe28f'
                                '9ea85a4e64c526702435d726f7ff14da40bae4 '
                                'OP_CHECKSIG'),
                        'hex': ('4104fe1b9ccf732e1f6b760c5ed3152388eeeadd4a073e'
                                '621f741eb157e6a62e3547c8e939abbd6a513bf3a1fbe2'
                                '8f9ea85a4e64c526702435d726f7ff14da40bae4ac'),
                        'reqSigs': 1,
                        'type': 'pubkey',
                        'addresses': ['15NUwyBYrZcnUgTagsm1A7M2yL2GntpuaZ']
                    }
                }
            ]
        }
        self.do_get_decoded_tx(tx_id, expected_json)
    
    #Expected fields for tx:
    #   hash (string)
    #   inputs (list of tuples)
    #       (tuple elt)
    #           prev_out (tuple) (only present if not coinbase tx)
    #              addr (string)
    #               n (integer)
    #   out (list of tuples)
    #       (tuple elt)
    #           n (integer)
    #           addr (string)
    def do_get_bci_like_tuple_for_tx_id(self, tx_id, expected_tuple):
        bci_like_tuple = self.reader.get_bci_like_tuple_for_tx_id(tx_id)
        self.assertEqual(bci_like_tuple, expected_tuple, 'Mismatching tuple for tx %s. Expected %s discovered %s')
    
    def test_get_bci_like_tuple_for_tx_id_at_height_zero(self):
        tx_id = '4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b'
        expected_tuple = GENESIS_TX_AS_BCI_LIKE_TUPLE
        self.do_get_bci_like_tuple_for_tx_id(tx_id, expected_tuple)
    
    def test_get_bci_like_tuple_for_second_tx_id_at_height_187(self):
        tx_id = '4385fcf8b14497d0659adccfe06ae7e38e0b5dc95ff8a13d7c62035994a0cd79'
        expected_tuple = SECOND_TX_BLOCK_HEIGHT_187_AS_BCI_LIKE_TUPLE
        self.do_get_bci_like_tuple_for_tx_id(tx_id, expected_tuple)
        
    def do_get_output_address(self, tx_id, output_index, 
                              expected_output_address, 
                              tx_json = None, 
                              skip_fetching_tx_json = False):
        if tx_json is None and not skip_fetching_tx_json:
            tx_json = self.reader.get_decoded_tx(tx_id)
        
        output_address = self.reader.get_output_address(tx_id, output_index, 
                                                                  tx_json)
        self.assertEqual(output_address, expected_output_address, 
                         'Output address does not match. Expected %s received %s' % (expected_output_address, output_address))
    
    def test_get_output_address_at_block_height_zero(self):
        tx_id = '4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b'
        index = 0
        expected_address = '1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa'
        self.do_get_output_address(tx_id, index, expected_address)
    
    def test_get_output_address_at_block_height_187_second_tx(self):
        tx_id = '4385fcf8b14497d0659adccfe06ae7e38e0b5dc95ff8a13d7c62035994a0cd79'
        index = 0
        expected_address = '15NUwyBYrZcnUgTagsm1A7M2yL2GntpuaZ'
        self.do_get_output_address(tx_id, index, expected_address)

    def test_get_output_address_at_block_height_170_second_tx(self):
        tx_id = 'f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16'
        index = 1
        expected_address = '12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S'
        self.do_get_output_address(tx_id, index, expected_address)
    
    def test_get_output_address_at_block_height_187_second_tx_after_cached(self):
        #make the tx get cached in db.SQL_TABLE_NAME_TX_OUTPUT_CACHE and then
        #   make sure that get_output_address() still returns it properly
        block_height = 187
        tx_id = '4385fcf8b14497d0659adccfe06ae7e38e0b5dc95ff8a13d7c62035994a0cd79'
        index = 0
        expected_address = '15NUwyBYrZcnUgTagsm1A7M2yL2GntpuaZ'
        stmt = ('INSERT INTO ' + address_reuse.db.SQL_TABLE_NAME_TX_OUTPUT_CACHE + ''
                ' (block_height, tx_id, output_pos, address) '
                'VALUES (?, ?, ?, ?)')
        arglist = (block_height, tx_id, index, expected_address)
        self.database_connector.run_statement(stmt, arglist)
                
        #temporarily break the RPC interface to ensure that result is returned
        #   from DB cache instead >:-D
        temp = self.reader.rpc_connection
        self.reader.rpc_connection = None
        
        self.do_get_output_address(tx_id, index, expected_address, 
                                   skip_fetching_tx_json = True)
        
        self.reader.rpc_connection = temp
        
    def do_get_tx_list(self, block_height, expected_txs_as_bci_tuples):
        txs_as_bci_tuples = self.reader.get_tx_list(block_height)
        self.assertIsNotNone(txs_as_bci_tuples, 
                             'Received list of txs for block height %d is None.' % block_height)
        self.assertEqual(len(txs_as_bci_tuples), 
                         len(expected_txs_as_bci_tuples), 
                         'Wrong number of txs at block height %d. Expected %d, received %d.' % (block_height, len(expected_txs_as_bci_tuples), len(txs_as_bci_tuples)))
        for expected_tx in expected_txs_as_bci_tuples:
            self.assertIn(expected_tx, txs_as_bci_tuples, 
                          'Missing transaction %s from transaction list returned at block height %d.' % (expected_tx['hash'], block_height))
    
    def test_get_tx_list_at_block_height_zero(self):
        block_height = 0
        txs = [GENESIS_TX_AS_BCI_LIKE_TUPLE]
        self.do_get_tx_list(block_height, txs)
        
    def test_get_tx_list_at_block_height_187(self):
        block_height = 187
        txs = [FIRST_TX_BLOCK_HEIGHT_187_AS_BCI_LIKE_TUPLE, 
              SECOND_TX_BLOCK_HEIGHT_187_AS_BCI_LIKE_TUPLE]
        self.do_get_tx_list(block_height, txs)
    
    def do_is_first_transaction_for_address(self, addr, tx_id, block_height, 
                                            expected_result):
        result = self.reader.is_first_transaction_for_address(addr, tx_id, 
                                                              block_height)
        self.assertEqual(result, expected_result, 
                         'Incorrect answer to whether %s first received in tx %s. Expected %s received %s' % (addr, tx_id, str(expected_result), str(result)))
    
    #Test the is_first_transaction_for_address() function when no blocks
    #   have been processed
    def test_is_first_transaction_for_output_address_in_genesis_block(self):
        block_height = 0
        tx_id = '4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b'
        addr = '1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa'
        self.do_is_first_transaction_for_address(addr, tx_id, block_height, 
                                                 True)
    
    def test_is_first_transaction_for_reused_address(self):
        block_height = 170
        
        #in order for the results at block height 170 to be accurate, we must
        #   first process the first 170 blocks.
        block_processor = address_reuse.block_processor.BlockProcessor(
            self.reader, self.database_connector)
        benchmarker = address_reuse.benchmark.block_reader_benchmark.Benchmark()
        #We don't need to blame particular parties for this test, so skip
        #   the blaming functions that would require remote HTTP requests.
        for height in range(0,170):
            block_processor.process_block(height, benchmarker, 
                                          defer_blaming=True)
        benchmarker.stop()
        benchmarker.print_stats()
        tx_id = 'f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16'
        addr = '12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S'
        self.do_is_first_transaction_for_address(addr, tx_id, block_height, 
                                                 False)
    
    #simulate caching of the 'relayed by' field for one transation, and then
    #   ensure that the get_tx_relayed_by_using_tx_id() is referencing
    #   the cache rather than doing a remote API lookup. This can be tested
    #   by using a benchmarker object that counts the number of API lookups
    #   saved by using the cache.
    def test_get_tx_relayed_by_using_tx_id_uses_cache(self):
        tx_id = ('0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512'
                 '098')
        block_height = 1
        relayed_by = '0.0.0.0'
        stmt = ('INSERT INTO '
                '' + address_reuse.db.SQL_TABLE_NAME_RELAYED_BY_CACHE + ''
                ' (block_height, tx_id, relayed_by) VALUES (?,?,?)')
        arglist = (block_height, tx_id, relayed_by)
        self.database_connector.run_statement(stmt, arglist)
        
        benchmarker = address_reuse.benchmark.block_reader_benchmark.Benchmark()
        api_reader = address_reuse.blockchain_reader.ThrottledBlockchainReader(
            self.database_connector)
        relayed_by = api_reader.get_tx_relayed_by_using_tx_id(tx_id, 
                                                 benchmarker = benchmarker)
        self.assertEqual(relayed_by, '0.0.0.0', 
                         'Expected 0.0.0.0, received %s' % relayed_by)
        queries_avoided = benchmarker.blockchain_info_queries_avoided_by_caching
        self.assertEqual(queries_avoided, 1, 
                         ('Expected to avoid 1 query to BCI, instead %d' % 
                          queries_avoided))
    
    def do_get_output_addresses(self, tx_id, expected_output_addresses):
        print("asdf1")
        tx_json = self.reader.get_decoded_tx(tx_id)
        print("asdf2")
        output_addresses = self.reader.get_output_addresses(tx_json)
        print("asdf3")
        a = len(expected_output_addresses)
        b = len(output_addresses)
        self.assertEqual(a, b, ("Expected %d output addresses, recieved %d." % 
                                (a, b)))
        for i in range(0, len(output_addresses)):
            print("asdf4")
            a = output_addresses[i]
            b = expected_output_addresses[i]
            self.assertEqual(a, b, 
                             ("%d th output address wrong, expected %s received %s" %
                              (i, a, b)))

    def test_get_output_addresses_of_second_tx_block_170(self):
        print("Entered test_get_output_addresses_of_second_tx_block_170()")
        tx_id = 'f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16'
        expected_address = ['1Q2TWHE3GMdB6BZKafqwxXtWAWgFt5Jvm3',
                           '12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S']
        self.do_get_output_addresses(tx_id, expected_address)
        print("Done with test_get_output_addresses_of_second_tx_block_170()")

class WalletExplorerReaderTestCase(unittest.TestCase):
    
    database_connector  = None
    reader              = None
    
    def setUp(self):
        try:
            os.remove(TEMP_DB_FILENAME)
        except OSError:
            pass
        self.database_connector = address_reuse.db.Database(TEMP_DB_FILENAME)
        
        self.reader = address_reuse.blockchain_reader.WalletExplorerReader(
            database_connector = self.database_connector)
        
    def tearDown(self):
        self.reader = None
    
    def test_get_address_list_from_json(self):
        addresses_json = json.loads(SAMPLE_ADDRESS_LIST_JSON_AS_STR)
        addresses = self.reader.get_address_list_from_json(addresses_json)
        self.assertEqual(len(addresses), 2)
        self.assertIn('1NbrsBgcktga92XFbcNuCGaPc1BAJoydKP', addresses)
        self.assertIn('1AsiYDrudPY3yTGZ4ArdYeiCdP7297niqe', addresses)

suite = unittest.TestLoader().loadTestsFromTestCase(
    LocalBlockchainRPCReaderTestCase)
suite2 = unittest.TestLoader().loadTestsFromTestCase(
    WalletExplorerReaderTestCase)
