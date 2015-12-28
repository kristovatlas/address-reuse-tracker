#Covers these classes and functions:
#   WalletExplorerReader:
#       get_addresses_for_wallet_label_net(blame_label)
#       get_wallet_label_for_single_address(addr)
#       get_label_for_wallet_id_net(wallet_id)

####################
# INTERNAL IMPORTS #
####################

import address_reuse.blockchain_reader
import address_reuse.db

####################
# EXTERNAL IMPORTS #
####################

import unittest
import os

#############
# CONSTANTS #
#############

TEMP_DB_FILENAME = 'address_reuse.db-temp'

#####################
# TEST CASE CLASSES #
#####################

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
        
    def test_get_addresses_for_wallet_label_net_one_page(self):
        wallet_label = '07a838551a4aaa2f'
        addresses = self.reader.get_addresses_for_wallet_label_net(wallet_label)
        
        #this wallet may add new addresses in the future, so just check the
        #   lower bound and the addresses we know about
        self.assertGreaterEqual(len(addresses), 2)
        self.assertIn('1NbrsBgcktga92XFbcNuCGaPc1BAJoydKP', addresses)
        self.assertIn('1AsiYDrudPY3yTGZ4ArdYeiCdP7297niqe', addresses)
    
    def test_get_addresses_for_wallet_label_net_multi_page(self):
        #Remote API will only return 100 addresses at a time; test ability
        #   to find correct addresses in middle 'pages'.
        
        wallet_label = 'Zyado.com-old' #239 addresses, unlikely to change
        addresses = self.reader.get_addresses_for_wallet_label_net(wallet_label)
        self.assertGreaterEqual(len(addresses), 239)
        self.assertLess(len(addresses), 500)
        self.assertIn('1JuNUnx3JQe7Zb4trkWQUS7QQi8iQYH4g3', addresses) #first page
        self.assertIn('1BxaUa8UkXYT6cNdb59zYWbSe1nSbYJBBo', addresses) #middle
        self.assertIn('1JNBqxJLVHiASA8jdGFJ8sGqkNcJjT6f36', addresses) #last
    
    def test_get_wallet_label_for_single_address_for_wallet_with_label(self):
        #This wallet should have the 'label' field set at WalleteExplorer.com
        addr = '1BxaUa8UkXYT6cNdb59zYWbSe1nSbYJBBo'
        label = self.reader.get_wallet_label_for_single_address(addr)
        self.assertEqual(label, 'Zyado.com-old')
        
    def test_get_wallet_label_for_single_address_for_wallet_without_label(self):
        #This wallet should not have the 'label' field set at WalleteExplorer.com,
        #   and fall back on the 'wallet_id' field.
        addr = '1LxozJ6tALJdurh9aWjrenjPMgqF7csmvc'
        label = self.reader.get_wallet_label_for_single_address(addr)
        self.assertEqual(label, '45c64a40eb51918f')
    
    def test_get_wallet_label_for_single_address_with_full_wallet_caching_off(self):
        address_reuse.blockchain_reader.CACHE_ALL_WALLET_ADDRESSES = False
        addr = '1JuNUnx3JQe7Zb4trkWQUS7QQi8iQYH4g3'
        
        label = self.reader.get_wallet_label_for_single_address(addr)
        #expected result: db.cache_blame_label_for_btc_address() is called to
        #   cache a single record in SQL_TABLE_NAME_BLAME_LABEL_CACHE.
        
        stmt = ('SELECT * FROM ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE)
        arglist = []
        caller = ('test_get_wallet_label_for_single_address_with_full_wallet_'
                  'caching_off')
        rows = self.database_connector.fetch_query_and_handle_errors(stmt, 
                                                                     arglist, 
                                                                     caller)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['btc_address'], addr)
        self.assertEqual(rows[0]['label'], 'Zyado.com-old')
    
    def test_get_wallet_label_for_single_address_with_full_wallet_caching_on(self):
        address_reuse.blockchain_reader.CACHE_ALL_WALLET_ADDRESSES = True
        addr = '1JuNUnx3JQe7Zb4trkWQUS7QQi8iQYH4g3'
        
        label = self.reader.get_wallet_label_for_single_address(addr)
        #espected result: not only will label for this address be cached, but
        #   for all addresses belonging to this wallet. This wallet is unlikely
        #   to be updated, so we'll just expect a specific number of addresses
        #   to be cached.
        
        stmt = ('SELECT * FROM ' 
                '' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE)
        arglist = []
        caller = ('test_get_wallet_label_for_single_address_with_full_wallet_'
                  'caching_off')
        rows = self.database_connector.fetch_query_and_handle_errors(stmt, 
                                                                     arglist, 
                                                                     caller)
        self.assertEqual(len(rows), 239)
        
    def test_get_label_for_wallet_id_net_no_label_available(self):
        wallet_id = '45c64a40eb51918f'
        label = self.reader.get_label_for_wallet_id_net(wallet_id)
        self.assertIsNone(label)
        
    def test_get_label_for_wallet_id_net_label_available(self):
        wallet_id = '0000041af1456771'
        label = self.reader.get_label_for_wallet_id_net(wallet_id)
        self.assertEqual(label, 'BTC-e.com')
        
suite = unittest.TestLoader().loadTestsFromTestCase(
    WalletExplorerReaderTestCase)
