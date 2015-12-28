#TODO: move me to ./tools/

#Deprecated: No need for this tool ever, safe to delete in a git commit.

import address_reuse.db

MAX_HEIGHT_IN_DB_TO_CONSIDER = 200000

#find the height with the lowest deferred blame records

db = address_reuse.db.Database(
    blockchain_mode = address_reuse.config.BlockchainMode.BITCOIND_RPC)
coord_db = address_reuse.db.BlameResolverCoordinationDatabase()
lowest_height_with_def_records = db.get_lowest_block_height_with_deferred_records()

for height in range(lowest_height_with_def_records, MAX_HEIGHT_IN_DB_TO_CONSIDER + 1):
    if db.is_deferred_record_at_height(height) and coord_db.is_block_height_claimed(height):
        coord_db.unclaim_block_height(height)
        print "Unclaimed height %d that was incorrectly marked as complete."

print "Done."
