#TODO: move me to ./tools/

import address_reuse.db

#processed by update_deferred_blame_records.py
TOP_HEIGHT_ALREADY_PROCESSED = 198170

coord_db = address_reuse.db.BlameResolverCoordinationDatabase()
coord_db.mark_blocks_completed_up_through_height(TOP_HEIGHT_ALREADY_PROCESSED)
