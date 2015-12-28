#Try processing the same 100 blocks' worth of deferred blame records with 
#   various flags that impact caching of remote API data, to benchmark which 
#   flags make it go the fastest during this limited data set.

####################
# INTERNAL IMPORTS #
####################

import address_reuse.blockchain_reader
import address_reuse.time_debug
import address_reuse.db
import address_reuse.block_processor

####################
# EXTERNAL IMPORTS #
####################

import csv
import sqlite3
import os

#############
# CONSTANTS #
#############

BENCHMARK_DB_FILENAME = 'address_reuse_benchmark.db-temp'

CSV_FILENAME = 'deferred_records_block_150k.csv'

DB_DEFERRED_BLAME_PLACEHOLDER = 'DB_DEFERRED_BLAME_PLACEHOLDER'

################
# BEGIN SCRIPT #
################

def get_new_db():
    try:
        os.remove(BENCHMARK_DB_FILENAME)
    except OSError:
        pass
        
    database = address_reuse.db.Database(
        sqlite_db_filename = BENCHMARK_DB_FILENAME)
    many_args = []
    
    with open(CSV_FILENAME,'rb') as fin: # `with` statement available in 2.5+
        # csv.DictReader uses first line in file for column headings by default
        dr = csv.DictReader(fin) # comma is default delimiter
        for row in dr:
            blame_recipient_id = row['blame_recipient_id']
            address_reuse_type = row['address_reuse_type']
            role = row['role']
            data_source = row['data_source']
            block_height = row['block_height']
            confirmed_tx_id = row['confirmed_tx_id']
            relevant_address = row['relevant_address']

            arglist = (blame_recipient_id, address_reuse_type, role, 
                       data_source, block_height, confirmed_tx_id, 
                       relevant_address)
            many_args.append(arglist)
    
    stmt = ('INSERT INTO ' 
                    '' + address_reuse.db.SQL_TABLE_NAME_BLAME_STATS + ' '
                    '(blame_recipient_id, address_reuse_type, role, '
                    'data_source, block_height, confirmed_tx_id, '
                    'relevant_address) VALUES (?,?,?,?,?,?,?);')
    database.run_statement(stmt, many_args, execute_many = True)
        
    #setup indices
    
    stmt = ('CREATE INDEX indBlameStats ON tblBlameStats (blame_recipient_id, '
            'address_reuse_type, role, data_source, block_height, '
            'confirmed_tx_id, relevant_address);')
    database.run_statement(stmt, [])
    
    stmt = ('CREATE INDEX indBlameLabelCache ON tblBlameLabelCache '
            '(btc_address, label)')
    database.run_statement(stmt, [])
    
    
    database.get_blame_id_for_label_and_insert_if_new(
        DB_DEFERRED_BLAME_PLACEHOLDER)
    
    return database

def get_new_processor(cache_all_wallet_addresses, cache_locally_for_all_input_addresses):
    #override global settings for blockchain_reader module
    reader_mod = address_reuse.blockchain_reader
    reader_mod.CACHE_ALL_WALLET_ADDRESSES = cache_all_wallet_addresses
    reader_mod.CACHE_LOCALLY_FOR_ALL_INPUT_ADDRESSES = cache_locally_for_all_input_addresses
    
    database = get_new_db()
    blockchain_reader = reader_mod.LocalBlockchainRPCReader(database)
    block_processor = address_reuse.block_processor.BlockProcessor(
        block_reader = blockchain_reader, database_connector = database)
    return block_processor
    
def main():
    purpose_to_elapsed = {}
    
    for cache_all_wallet_addresses in [True, False]:
        for cache_locally_for_all_input_addresses in [True, False]:
            block_processor = get_new_processor(cache_all_wallet_addresses, 
                                          cache_locally_for_all_input_addresses)
            purpose = ('cache_all=%s, cache_input=%s' % 
                       (str(cache_all_wallet_addresses), 
                        str(cache_locally_for_all_input_addresses)))
            timer = address_reuse.time_debug.Timer(purpose)
            for block_height in range(150000, 150100):
                block_processor.process_block_after_deferred_blaming(
                    block_height)
            elapsed = timer.stop()
            
            purpose_to_elapsed[purpose] = elapsed
    
    for purpose, elapsed in purpose_to_elapsed:
        print("%s: %s" % (purpose, str(elapsed)))

if __name__ == "__main__":
    main()
