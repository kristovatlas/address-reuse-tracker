#Description: Goes through the table containing blame stats and back-fills the table containing a cache of every unique Bitcoin address seen.
#Updates database with local processing only -- no network access required.

#TODO: move my file location to a utilities directory

####################
# INTERNAL IMPORTS #
####################

import address_reuse.db
import address_reuse.logger

database_connector = address_reuse.db.Database()
address_list = database_connector.get_all_distinct_addresses_from_blame_records()

num_added = 0
for address in address_list:
    if database_connector.has_address_been_seen_cache_if_not(address):
        address_reuse.logger.log_and_die('Somehow we have already seen address %s' % address)
    num_added = num_added +1

print("Done. Added %d addresses." % num_added)
