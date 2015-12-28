#TODO: move me to ./tools/

import address_reuse.db

#This will clean up frozen block heights when an instance of
#   update_deferred_blame_records.py crashed. It's probably a bad idea to run
#   this while other instances of update_deferred_blame_records.py are still
#   running -- close them first.

coord_db = address_reuse.db.BlameResolverCoordinationDatabase()
dead_blocks = coord_db.get_list_of_block_heights_with_possibly_crashed_workers()
for dead_block in dead_blocks:
    coord_db.unclaim_block_height(dead_block)
    print("Unclaimed dead block %d" % dead_block)
print("Done.")
