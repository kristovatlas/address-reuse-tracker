#Go through blame IDs stored in db, check if WalletExplorer.com provides a
#   'label' field for each instead of 'wallet_id'. If so, use that instead for
#   all existing records.


# I need this script to repair my database, as I was accidentally only fetching
#   the 'wallet_id' field from WalletExplorer.com for a while.

#TODO: move me to tools directory

import address_reuse.db
import address_reuse.config

STARTING_ROWID = 1400

def main():
    rowid = STARTING_ROWID
    database = address_reuse.db.Database(
        blockchain_mode=address_reuse.config.BlockchainMode.BITCOIND_RPC)
    wallet_api_reader = address_reuse.blockchain_reader.WalletExplorerReader(
        database_connector=database)

    stmt1 = ('UPDATE ' + address_reuse.db.SQL_TABLE_NAME_BLAME_IDS + ' SET '
             'label=? WHERE rowid=?')
    arglist_many1 = []

    stmt2 = ('UPDATE ' + address_reuse.db.SQL_TABLE_NAME_BLAME_LABEL_CACHE + ' '
             'SET label=? WHERE label=?')
    arglist_many2 = []

    num_records_updated = 0

    while True:
        print "Status: Checking rowid %d." % rowid
        label = database.get_blame_label_for_blame_id(blame_party_id=rowid)
        if label is None:
            break #ran out of rows
        else:
            new_label = wallet_api_reader.get_label_for_wallet_id_net(label)
            if new_label is None:
                #Either there is no 'label' field for this, or the string we
                #   looked up was already the 'label' field in the database,
                #   rather than the 'wallet_id' field. Either way, nothing to
                #   do here.
                pass
            else:
                print("Status: Need to update records: %s => %s" %
                      (label, new_label))
                num_records_updated = num_records_updated + 1

                #Update SQL_TABLE_NAME_BLAME_IDS for this rowid with new_label
                #TODO: Probably should write functions in the db class, but too
                #   lazy right now since this may be a use-once script.

                arglist1 = (new_label, rowid)
                arglist_many1.append(arglist1)

                #Update all references to the old lable in
                #   SQL_TABLE_NAME_BLAME_LABEL_CACHE with the new_label
                arglist2 = (new_label, label)
                arglist_many2.append(arglist2)

        if rowid % 100 == 0:
            #execute mass batch of update statements
            if len(arglist_many1) > 0:
                assert len(arglist_many2) > 0
                database.run_statement(stmt1, arglist_many1, execute_many=True)
                arglist_many1 = []
                database.run_statement(stmt2, arglist_many2, execute_many=True)
                arglist_many2 = []
            print "Completed updates up thru rowid %d " % rowid

        rowid = rowid + 1

    print "Tried to update %d items." % num_records_updated

if __name__ == "__main__":
    main()
