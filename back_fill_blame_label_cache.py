#Description: Goes through the table containing blame stats and back-fills the table containing a cache of blame labels for any given address.
#Updates database with local processing only -- no network access required.

#TODO: move my file location to a utilities directory

####################
# INTERNAL IMPORTS #
####################

import address_reuse.db
import address_reuse.logger

database_connector = address_reuse.db.Database()

#There's only enough information in the database to back-fill the label cache for reuse records in which the BTC address was the receiver. We don't know the address of any of the senders.
address_list = database_connector.get_blamed_address_list_for_role(address_reuse.db.AddressReuseRole.RECEIVER)

print("Retrieved a list of %d addresses, the labels of which we can back-fill in the cache." % len(address_list))

for address in address_list:
    print("Trying to cache label for address '%s'..." % address)
    blame_id = database_connector.get_blame_id_for_role_and_address(address_reuse.db.AddressReuseRole.RECEIVER, address)
    label = database_connector.get_blame_label_for_blame_id(blame_id)
    cached_label = database_connector.get_blame_label_for_btc_address(address)
    if cached_label is not None:
        address_reuse.logger.log_and_die("Label '%s' appears to already be cached for address '%s'" % (cached_label, address))
    database_connector.cache_blame_label_for_btc_address(address, label)

print("Done.")