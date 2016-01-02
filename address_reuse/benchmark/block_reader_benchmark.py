####################
# EXTERNAL IMPORTS #
####################

import time
from datetime import datetime, timedelta

#############
# CONSTANTS #
#############

#source: https://blockchain.info/charts/n-transactions-total
CURRENT_APPROX_TX_HISTORICAL = 95000000

#source: https://blockchain.info/
CURRENT_APPROX_BLOCK_HEIGHT = 381297

#############
# FUNCTIONS #
#############

#From:
#http://stackoverflow.com/questions/4048651/python-function-to-convert-seconds-into-minutes-hours-and-days
def get_time(sec):
    sec = timedelta(seconds=int(sec))
    d = datetime(1,1,1) + sec
    return ("%d days, %d hours, %d, minutes, %d seconds" %
            (d.day-1, d.hour, d.minute, d.second))

###########
# CLASSES #
###########

class Benchmark:
    first = None
    last = None
    block_count = 0
    tx_count = 0
    record_count = 0
    wallet_explorer_queries_avoided_by_caching = 0
    blockchain_info_queries_avoided_by_caching = 0

    #Timer is started when class is instantiated
    def __init__(self):
        self.first = time.time()

    def increment_blocks_processed(self):
        self.block_count = self.block_count + 1

    def increment_transactions_processed(self):
        self.tx_count = self.tx_count + 1

    def increment_records_processed(self):
        self.record_count = self.record_count + 1

    def increment_wallet_explorer_queries_avoided_by_caching(self):
        self.wallet_explorer_queries_avoided_by_caching = (
            self.wallet_explorer_queries_avoided_by_caching + 1)

    def increment_blockchain_info_queries_avoided_by_caching(self):
        self.blockchain_info_queries_avoided_by_caching = (
            self.blockchain_info_queries_avoided_by_caching + 1)

    def stop(self):
        self.last = time.time()

    def print_stats(self):
        sec_elapsed = self.last - self.first
        blocks_per_sec = 1.0 * self.block_count / sec_elapsed
        tx_per_sec = 1.0 * self.tx_count / sec_elapsed
        records_per_sec = 1.0 * self.record_count / sec_elapsed
        expected_sec_to_process_all_tx = 0
        try:
            expected_sec_to_process_all_tx = (CURRENT_APPROX_TX_HISTORICAL *
                                              1.0 / tx_per_sec)
        except ZeroDivisionError:
            pass

        expected_time_to_process_all_tx = get_time(
            expected_sec_to_process_all_tx)

        print (("Processed %d blocks, %d transactions, and %d records in %d "
                "seconds. Average: %.6f blocks/sec %.6f tx/sec %.6f "
                "records/sec. At this rate, it would take %s to process all of "
                "the transactions in the blockchain. Avoided making %d call(s) "
                "to WalletExplorer.com and %d call(s) to Blockchain.info "
                "through the use of caching.") %
               (self.block_count, self.tx_count, self.record_count, sec_elapsed,
                blocks_per_sec, tx_per_sec, records_per_sec,
                expected_time_to_process_all_tx,
                self.wallet_explorer_queries_avoided_by_caching,
                self.blockchain_info_queries_avoided_by_caching))
