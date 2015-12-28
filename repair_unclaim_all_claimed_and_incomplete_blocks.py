#WARNING: DO NOT RUN THIS SCRIPT WHILE OTHER PROCESSES ARE OPERATING ON DB,
#   PARTICULARLY update_deferred_blame_records.py. It is only meant to be run
#   when the db is otherwise quiet to clean out blocks claimed by workers
#   that have recently crashed without unclaiming the blocks.

#TODO: move me to ./tools/

import address_reuse.db

address_reuse.db.DEFERRED_BLAME_RESOLVER_WARNING_AFTER_N_SEC = 0

coord_db = address_reuse.db.BlameResolverCoordinationDatabase()
dead_blocks = coord_db.get_list_of_block_heights_with_possibly_crashed_workers()
for block in dead_blocks:
    coord_db.unclaim_block_height(block)
    print("Unclaimed block at height %d." % block)

print("Done.")
