####################
# INTERNAL IMPORTS #
####################

import config
import logger
import validate
import blame_stats
import string
import tx_blame
import data_subscription
import custom_errors

####################
# EXTERNAL IMPORTS #
####################

import sqlite3
from enum import IntEnum
from collections import OrderedDict, deque
from cgi import escape
from copy import deepcopy
from time import sleep
from os import getpid

#############
# CONSTANTS #
#############

DEFERRED_BLAME_RESOLVER_WARNING_AFTER_N_SEC = 18000 #300min

FETCH_DEFERRED_RECORDS_IN_BATCH     = True  #TODO: move flag to config file?
#This should be a large value that doesn't consume "too much" memory. Consider
#   these benchmarks:
#   $ time sqlite3 addres_reuse_local.db 'SELECT rowid, role, data_source,
#   confirmed_tx_id, address_reuse_type, relevant_address, block_height FROM
#   tblBlameStats WHERE rowid > -1 AND blame_recipient_id = 1 ORDER BY rowid
#   LIMIT 10;'
#   real	5m16.102s, user	3m43.451s, sys	0m29.772s
#   LIMIT 10000:
#   real	6m33.113s, user	5m9.649s, sys	0m29.318s
#   LIMIT 20000:
#   real	6m37.439s, user	5m13.238s, sys	0m29.716s
#   LIMIT 100000:
#   real	6m52.341s, user	5m31.745s, sys	0m28.268s
FETCH_N_DEFERRED_RECORDS_IN_BATCH   = 200000 #TODO: move setting to config file?

#If sqlite3 encounters an exception, try again this many times. Each time, the
#   waiting function will sleep for an incremented number of seconds a total of
#   n^2/2 seconds. This value is high because there may be multiple
#   threads/processes operating on the same database file, and some query
#   operations take a long time (up to 7 min) resulting in a locked database
#   for other workers that want to modify that table.
NUM_ATTEMPTS_UPON_DB_ERROR = 50

#Batch INSERT blame stats only once per block as a batch, rather than once per
#   TX
INSERT_BLAME_STATS_ONCE_PER_BLOCK = True

#Batch UPDATE blame stats only once per block as a batch, rather than once per
#   TX.
UPDATE_BLAME_STATS_ONCE_PER_BLOCK = True

#Batch DELETE blame stats only once per block as a batch, rather tha once per
#   TX.
DELETE_BLAME_STATS_ONCE_PER_BLOCK = True

#SQLite limits the number of terms in a compound SELECT statement:
#   http://www.sqlite.org/limits.html
SQLITE_MAX_COMPOUND_SELECT = 500

ENABLE_DEBUG_PRINT = True

DB_DEFERRED_BLAME_PLACEHOLDER = 'DB_DEFERRED_BLAME_PLACEHOLDER'

HTML_ESCAPE_TABLE = {
    "&": "&amp;",
    '"': "&quot;",
    "'": "&apos;",
    ">": "&gt;",
    "<": "&lt;"     }

############################## TABLE DEFINITIONS ###############################

SQL_TABLE_NAME_BLOCK_STATS      = 'tblBlockStats'

#This is ugly, but normal Python dictionaries are not ordered and even the
#   OrderedDict() won't retain order if the items aren't added sequentially.
SQL_SCHEMA_BLOCK_STATS = OrderedDict()
SQL_SCHEMA_BLOCK_STATS['block_num']                         = 'INTEGER'
SQL_SCHEMA_BLOCK_STATS['tx_total_num']                      = 'INTEGER'
SQL_SCHEMA_BLOCK_STATS['tx_sendback_reuse_num']             = 'INTEGER'
SQL_SCHEMA_BLOCK_STATS['tx_receiver_has_tx_history_num']    = 'INTEGER'
SQL_SCHEMA_BLOCK_STATS['tx_sendback_reuse_pct']             = 'DECIMAL(5,2)'
SQL_SCHEMA_BLOCK_STATS['tx_receiver_has_tx_history_pct']    = 'DECIMAL(5,2)'
SQL_SCHEMA_BLOCK_STATS['process_type_version_num']          = 'INTEGER'

#TODO: This table not yet used.
SQL_TABLE_NAME_LAST_N_BLOCKS    = 'tblStatsLastNBlocks'

SQL_SCHEMA_LAST_N_BLOCKS = OrderedDict()
SQL_SCHEMA_LAST_N_BLOCKS['last_n_blocks']                   = 'INTEGER'
SQL_SCHEMA_LAST_N_BLOCKS['tx_total_num']                    = 'INTEGER'
SQL_SCHEMA_LAST_N_BLOCKS['tx_sendback_reuse_num']           = 'INTEGER'
SQL_SCHEMA_LAST_N_BLOCKS['tx_receiver_has_tx_history_num']  = 'INTEGER'
SQL_SCHEMA_LAST_N_BLOCKS['tx_sendback_reuse_pct']           = 'DECIMAL(5,2)'
SQL_SCHEMA_LAST_N_BLOCKS['tx_receiver_has_tx_history_pct']  = 'DECIMAL(5,2)'
SQL_SCHEMA_LAST_N_BLOCKS['process_type_version_num']        = 'INTEGER'

SQL_ALIAS_HIGHEST_BLOCK_NUM = 'highest_block_num'

SQL_TABLE_NAME_BLAME_STATS = 'tblBlameStats'

SQL_SCHEMA_BLAME_STATS = OrderedDict()
SQL_SCHEMA_BLAME_STATS['blame_recipient_id']                = 'INTEGER'
SQL_SCHEMA_BLAME_STATS['address_reuse_type']                = 'INTEGER' #1 = sendback, 2 = tx history
SQL_SCHEMA_BLAME_STATS['role']                              = 'INTEGER' #1 = sender, 2 = receiver, 3 = client
SQL_SCHEMA_BLAME_STATS['data_source']                       = 'INTEGER' #1 = BCI, 2 = WalletExplorer.com, 3 = BCI 'relayed by'
SQL_SCHEMA_BLAME_STATS['block_height']                      = 'INTEGER'
SQL_SCHEMA_BLAME_STATS['confirmed_tx_id']                   = 'TEXT'    #the tx hash in confirmed block
SQL_SCHEMA_BLAME_STATS['relevant_address']                  = 'TEXT'    #can be null

SQL_TABLE_NAME_BLAME_IDS        = 'tblBlameIds'

SQL_SCHEMA_BLAME_IDS = OrderedDict()
#No ID field is needed because ROWID is automatically included in SQLite
SQL_SCHEMA_BLAME_IDS['label']                               = 'TEXT'

#A local cache of blame labels for addreses
SQL_TABLE_NAME_BLAME_LABEL_CACHE   = 'tblBlameLabelCache'
SQL_SCHEMA_BLAME_LABEL_CACHE = OrderedDict()
SQL_SCHEMA_BLAME_LABEL_CACHE['btc_address']                 = 'TEXT'
SQL_SCHEMA_BLAME_LABEL_CACHE['label']                       = 'TEXT'

#TODO: This is a somewhat awkward way to express the UNIQUE contraing for this
#   table
SQL_SCHEMA_BLAME_LABEL_CACHE_WITH_CONSTRAINTS = deepcopy(
    SQL_SCHEMA_BLAME_LABEL_CACHE)
#This is UNIQUE so that subsequent INSERT OR IGNORE statements work as intended.
#Addresses never get more than one wallet cluster label.
SQL_SCHEMA_BLAME_LABEL_CACHE_WITH_CONSTRAINTS['UNIQUE (btc_address)'] = ''

#A stateful list of addresses that we've seen so far while processing the
#   blockchain. This is used as a fast way to determine whether an output
#   address we are considering has a prior tx history.
#TODO: Reconsider if we should also inlcude the block height
#   at which we first saw the given address to provide more context, or whether
#   this will slow inserts and lookups down too much.
SQL_TABLE_NAME_ADDRESSES_SEEN   =   'tblSeenAddresses'
SQL_SCHEMA_ADDRESSES_SEEN = OrderedDict()
SQL_SCHEMA_ADDRESSES_SEEN['block_height_first_seen']        = 'INTEGER'
SQL_SCHEMA_ADDRESSES_SEEN['address']                        = 'TEXT'

#TODO: This is a somewhat awkward way to express the UNIQUE contraing for this
#   table
SQL_SCHEMA_ADDRESSES_SEEN_WITH_CONSTRAINTS = deepcopy(SQL_SCHEMA_ADDRESSES_SEEN)
SQL_SCHEMA_ADDRESSES_SEEN_WITH_CONSTRAINTS['UNIQUE (address)'] = ''

SQL_TABLE_NAME_RELAYED_BY_CACHE = 'tblRelayedByCache'
SQL_SCHEMA_RELAYED_BY_CACHE = OrderedDict()
SQL_SCHEMA_RELAYED_BY_CACHE['block_height']                 = 'INTEGER'
SQL_SCHEMA_RELAYED_BY_CACHE['tx_id']                        = 'TEXT'
SQL_SCHEMA_RELAYED_BY_CACHE['relayed_by']                   = 'TEXT'

#Make a local copy of the 'relayed by' field whenever we query it remotely
SQL_SCHEMA_RELAYED_BY_CACHE_WITH_CONSTRAINTS = deepcopy(
    SQL_SCHEMA_RELAYED_BY_CACHE)
SQL_SCHEMA_RELAYED_BY_CACHE_WITH_CONSTRAINTS['UNIQUE (tx_id)'] = ''

#A place to cache transation output addresses
SQL_TABLE_NAME_TX_OUTPUT_CACHE = 'tblTxOutputCache'
SQL_SCHEMA_TX_OUTPUT_CACHE = OrderedDict()
SQL_SCHEMA_TX_OUTPUT_CACHE['block_height']                  = 'INTEGER'
SQL_SCHEMA_TX_OUTPUT_CACHE['tx_id']                         = 'TEXT'
SQL_SCHEMA_TX_OUTPUT_CACHE['output_pos']                    = 'INTEGER'
SQL_SCHEMA_TX_OUTPUT_CACHE['address']                       = 'TEXT'

#TODO: This is a somewhat awkward way to express the UNIQUE contraing for this
#   table
SQL_SCHEMA_TX_OUTPUT_CACHE_WITH_CONSTRAINTS = deepcopy(
    SQL_SCHEMA_TX_OUTPUT_CACHE)
SQL_SCHEMA_TX_OUTPUT_CACHE_WITH_CONSTRAINTS[
    'UNIQUE (tx_id, output_pos, address)'] = ''

#Used to coordinate efforts between producers and subscribers of block-related
#   data.
SQL_TABLE_NAME_BLOCK_DATA_PRODUCTION_STATUS = 'tblBlockDataProductionStatus'
SQL_SCHEMA_BLOCK_DATA_PRODUCTION_STATUS = OrderedDict()
#Referenced by Enum in data_subscription module
SQL_SCHEMA_BLOCK_DATA_PRODUCTION_STATUS['producer_id']      = 'INTEGER'
SQL_SCHEMA_BLOCK_DATA_PRODUCTION_STATUS['top_block_height_available'] = 'INTEGER'

############################ END TABLE DEFINITIONS #############################

## SPECIAL DATABASE FOR COORDINATING MULTIPLE DEFERRED BLAME RESOLVER THREADS ##

"""
    TODO: This concept should eventually be merged with the data subscription
    model. In the mean time, this is a purpose-specific and relatively small
    database file that helps coordinate deferred blame resolver threads, since
    this is the most time-consuming process and which is in most dire need
    of multiple simultaneous workers.
"""

BLAME_RESOLVER_COORDINATION_DB_FILENAME = 'coordination.db'

SQL_TABLE_NAME_COORDINATION_REGISTER = 'tblCoordinationRegister'
SQL_SCHEMA_COORDINATION_REGISTER = OrderedDict()
SQL_SCHEMA_COORDINATION_REGISTER['block_height'] = 'INTEGER'
SQL_SCHEMA_COORDINATION_REGISTER['pid_of_claimer'] = 'INTEGER' #null = unclaimed
SQL_SCHEMA_COORDINATION_REGISTER['timestamp_claimed'] = ('DATETIME DEFAULT '
                                                         'CURRENT_TIMESTAMP')
SQL_SCHEMA_COORDINATION_REGISTER['completed'] = 'INTEGER' #bool
SQL_SCHEMA_SQL_SCHEMA_COORDINATION_REGISTER_WITH_CONSTRAINTS = deepcopy(
    SQL_SCHEMA_COORDINATION_REGISTER)
#this constraint required because we do an INSERT OR REPLACE instead of two
#   separate SELECT and UPDATE transactions.
SQL_SCHEMA_SQL_SCHEMA_COORDINATION_REGISTER_WITH_CONSTRAINTS[
    'UNIQUE (block_height)'] = ''

############################# END SPECIAL DATABASE #############################

#########
# ENUMS #
#########

class AddressReuseType(IntEnum):
    SENDBACK            = 1
    TX_HISTORY          = 2

class AddressReuseRole(IntEnum):
    SENDER              = 1
    RECEIVER            = 2
    CLIENT              = 3

class DataSource(IntEnum):
    BLOCKCHAIN_INFO     = 1
    WALLET_EXPLORER     = 2

###########
# CLASSES #
###########

class BlameResolverCoordinationDatabase(object):
    """Helps multiple threads coordinate their blockchain processing.

    Args:
        filename_override (Optional[str]): Override the default database
            filename for, e.g. unit testing.

    Attributes:
        con (sqlite3.Connection): Connection to the sqlite3 database.
        cursor (sqlite3.Cursor): Maintains and updates state for database.
        db_filename (str): The name of the database file in use.
        last_block_initialized (int): The highest block height at which this
            object knows for sure that there is an entry for all block hegihts
            between 0 and `last_block_initialized`. Various functions must
            keep track of this to ensure there aren't gaps, otherwise bugs
            will arise.
    """

    def __init__(self, filename_override = None):
        try:
            if filename_override is not None:
                self.con = sqlite3.connect(filename_override)
                self.db_filename = filename_override
            else:
                self.con = sqlite3.connect(
                    BLAME_RESOLVER_COORDINATION_DB_FILENAME)
                self.db_filename = BLAME_RESOLVER_COORDINATION_DB_FILENAME

            self.con.row_factory = sqlite3.Row # permit accessing results by col name
            self.cursor = self.con.cursor()
        except Exception as e:
            if self.con is not None:
                self.con.close()
            msg = "Could not connect to database: " + str(e)
            logger.log_and_die(msg)

        stmt = get_conditional_create_stmt(
            SQL_TABLE_NAME_COORDINATION_REGISTER,
            SQL_SCHEMA_SQL_SCHEMA_COORDINATION_REGISTER_WITH_CONSTRAINTS)
        self.run_statement(stmt, arglist=[])
        self.last_block_initialized = -1

    def close(self):
        """Closes the database connection."""
        self.con.close()

    def run_statement(self, stmt, arglist, execute_many = False):
        """Execute a SQL statement that returns no results.

        The statement is handled as a single SQLite transaction.

        Args:
            stmt (str): The statement to execute.
            arglist (List): A list of parameters for the SQL statement, or a
                list of a list of parameters if `execute_many` is set to True.
            execute_many (Optional[bool]): When False (default), only a single
                list of parameters is exected. If set to True, this function
                expects a list of a list of parameters and repeatedly submits
                them using the SQL statement by using the `sqlite3` module's
                `executemany` function.

        Raises:
            custom_errors.TooManyDatabaseErrors: If database errors are
                repeatedly encountered until the function gives up, the problem
                is logged and this error is raised.
        """

        dprint("Statement: " + stmt)
        dprint("Arglist: " + str(arglist))
        last_error = None
        for sec_wait in range(1, NUM_ATTEMPTS_UPON_DB_ERROR):
            try:
                if execute_many:
                    self.cursor.executemany(stmt, arglist)
                else:
                    self.cursor.execute(stmt, arglist)
                self.con.commit()

                #TODO: This should return a value indicating whether the
                #   statement executed successfully or not
                return
            except Exception as e:
                last_error = e
                if sec_wait != NUM_ATTEMPTS_UPON_DB_ERROR:
                    #Could you give it a second? It's going to space.
                    print(("WARNING: Experienced error executing db statement, "
                           "trying again in %f seconds.") % float(sec_wait))
                    sleep(float(sec_wait))
        if self.con is not None:
            self.con.close()
        msg = (("Could not execute database statement after %d tries. Last "
                "error: %s") % (NUM_ATTEMPTS_UPON_DB_ERROR, str(last_error)))
        logger.log_alert(msg)
        raise custom_errors.TooManyDatabaseErrors

    def get_mark_block_complete_stmt(self):
        """ SQL statement for marking a block complete."""

        return ('INSERT OR REPLACE INTO '
                '' + SQL_TABLE_NAME_COORDINATION_REGISTER + '(block_height, '
                'pid_of_claimer, completed) VALUES (?, ? , 1)')

    def mark_block_complete(self, block_height):
        """Signals to other workers that this block is complete."""

        pid = getpid()
        stmt = self.get_mark_block_complete_stmt()
        arglist = (block_height, pid)
        self.run_statement(stmt, arglist)

    def claim_block_height(self, block_height):
        """Signals to other workers that this block is being worked on."""

        assert isinstance(block_height, int)

        pid = getpid()

        stmt = ('INSERT OR REPLACE INTO '
                '' + SQL_TABLE_NAME_COORDINATION_REGISTER + ' (block_height, '
                'pid_of_claimer, completed) VALUES (?, ?, 0)')
        arglist = (block_height, pid)
        self.run_statement(stmt, arglist)

    def is_block_height_claimed(self, block_height):
        """Returns whether the block is currently marked as claimed."""

        assert isinstance(block_height, int)

        self._initialize_block_span(max_block_height=block_height,
                                    min_block_height=0)

        stmt = (('SELECT 1 AS one FROM %s WHERE block_height = ? AND '
                 'pid_of_claimer IS NOT NULL LIMIT 1') %
                SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = (block_height,)
        caller = 'is_block_height_claimed'
        res = self.fetch_query_and_handle_errors(stmt, arglist, caller)
        if res is None or len(res) == 0:
            return False
        else:
            return True

    def unclaim_block_height(self, block_height):
        """Mark this block as unclaimed and not complete."""

        assert isinstance(block_height, int)

        stmt = ('INSERT OR REPLACE INTO '
                '' + SQL_TABLE_NAME_COORDINATION_REGISTER + ' (block_height, '
                'pid_of_claimer, completed) VALUES (?, NULL, NULL)')
        arglist = (block_height,)
        self.run_statement(stmt, arglist)

    def unclaim_block_heights(self, block_height_list):
        """Unclaim a list of block heights."""

        for height in block_height_list:
            self.unclaim_block_height(height)

    def mark_blocks_completed_up_through_height(self, block_height):
        """Helps set up a new database by marking blocks complete.

        Will cover all blocks from genesis block to specified height.
        """

        assert isinstance(block_height, int)

        pid = getpid()
        stmt = self.get_mark_block_complete_stmt()
        arglist = []

        for height in range(0, block_height + 1):
            args = (height, pid)
            arglist.append(args)

        self.run_statement(stmt, arglist, execute_many=True)

    def get_list_of_block_heights_with_possibly_crashed_workers(self):
        """List block heights being processed by possibly crashed workers.

        Crashing is evidenced by a lack of completion for processing a block
            since the time that it was claimed. The time threshold for this is
            set by constant `DEFERRED_BLAME_RESOLVER_WARNING_AFTER_N_SEC`.
        """

        stmt = ('SELECT block_height FROM '
                '' + SQL_TABLE_NAME_COORDINATION_REGISTER + ' WHERE '
                '(completed IS NULL OR completed != 1) AND pid_of_claimer IS '
                'NOT NULL AND '
                'CAST(strftime("%s", CURRENT_TIMESTAMP) AS integer) - '
                'CAST(strftime("%s", timestamp_claimed) AS integer) > ?')
        arglist = (DEFERRED_BLAME_RESOLVER_WARNING_AFTER_N_SEC,)
        caller = 'get_list_of_block_heights_with_possibly_crashed_workers'
        records = self.fetch_query_and_handle_errors(stmt, arglist, caller)
        if len(records) == 0:
            return []
        else:
            height_list = []
            for record in records:
                height_list.append(record['block_height'])
            return height_list

    def fetch_query_and_handle_errors(self, stmt, arglist, caller):
        """Select info from and deal with issues that might come up.

        Args:
            stmt (str): The statement to execute.
            arglist (List[str]): A list of parameters for the SQL statement.
            caller (str): Name of calling function, for inclusion in error
                messages.

        Returns:
            The results from `sqlite3.Cursor.execute`. If the results are an
                empty list, an empty list is returned.
        """
        dprint(stmt)
        dprint(str(arglist))
        results = None

        last_error = None
        for sec_wait in range(1, NUM_ATTEMPTS_UPON_DB_ERROR):
            try:
                self.cursor.execute(stmt, arglist)
                results = self.cursor.fetchall()
                if results is None:
                    msg = ('Received null value from Database query in %s().' %
                           caller)
                    logger.log_and_die(msg)
            except Exception as e:
                dprint(str(e))
                last_error = e
                if sec_wait != NUM_ATTEMPTS_UPON_DB_ERROR:
                    #Could you give it a second? It's going to space.
                    print(("WARNING: Experienced error executing db statement, "
                           "trying again in %f seconds.") % float(sec_wait))
                    sleep(float(sec_wait))

        if results is None:
            msg = (("Could not fetch from database. Statement was '%s' error "
                    "is '%s' database filename is '%s'") %
                   (stmt, str(last_error), self.db_filename))
            if self.con is not None:
                self.con.close()
            logger.log_alert(msg)
            raise custom_errors.TooManyDatabaseErrors
        else:
            return results

    def _initialize_block_span(self, max_block_height, min_block_height=0):
        """Initialize a span of blocks as unclaimed and incomplete.

        This will not overwrite any existing records due to the use of
        `INSERT OR IGNORE`.

        Args:
            max_block_height (int): Max block height to intialize through.
            min_block_height (Optional[int]): Lowest lock height to start
                initializing. Default: Genesis block height.
        """

        assert isinstance(min_block_height, int)
        assert isinstance(max_block_height, int)

        if max_block_height <= self.last_block_initialized:
            #already intitalized all of these blocks!
            return

        stmt = (('INSERT OR IGNORE INTO %s (block_height, pid_of_claimer, '
                 'completed) VALUES (?, NULL, NULL)') %
                SQL_TABLE_NAME_COORDINATION_REGISTER)
        arglist = []
        lowest = 0
        if self.last_block_initialized > 0:
            lowest =  self.last_block_initialized
        for i in range(lowest, max_block_height + 1):
            arg = [i]
            arglist.append(arg)
        self.run_statement(stmt, arglist, execute_many=True)

        if min_block_height == 0 or min_block_height <= self.last_block_initialized:
            self.last_block_initialized = max_block_height

    def _claim_block_if_available(self, block_height):
        """Claims a block if it's not already claimed.

        Returns:
            bool: True if block was available and was claimed, False otherwise.
        """
        assert isinstance(block_height, int)

        #Need to lock the database to make this claiming process atomic
        #TODO: Not entirely positive that this prevents TOCTOU issues
        #http://stackoverflow.com/questions/8828495/how-to-lock-a-sqlite3-database-in-python
        with self.con:

            self._initialize_block_span(max_block_height=block_height,
                                        min_block_height=0)

            stmt = (('SELECT block_height FROM %s WHERE block_height = ? AND '
                     '(completed = 1 OR pid_of_claimer IS NOT NULL);') %
                    SQL_TABLE_NAME_COORDINATION_REGISTER)
            arglist = (block_height,)
            caller = '_claim_block_if_available'
            res = self.fetch_query_and_handle_errors(stmt, arglist, caller)
            assert len(res) < 2
            if len(res) == 1 and res[0]['block_height'] == block_height:
                return False
            else:
                self.claim_block_height(block_height)
                return True

    def get_list_of_next_block_heights_available(self, starting_height=0,
                                                 num_to_claim=1):
        """ Claim up to `num` of the lowest consecutive blocks that are avilable.

            Args:
                starting_height (Optional[int]): Lowest block to consider.
                    Default: genesis block.
                num_to_claim (Optional[int]): Claim consecutive blocks until we
                    hit a block height that is claimed by another worker, or
                    have claimed `num` consecutive blocks. Default: Only
                    claim 1.

            Returns:
                deque[int]: The consecutive block heights that were claimed.
        """

        assert isinstance(starting_height, int)
        assert isinstance(num_to_claim, int)

        self._initialize_block_span(max_block_height=starting_height,
                                    min_block_height=0)

        #Need to lock the database to make this claiming process atomic
        #TODO: Not entirely positive that this prevents TOCTOU issues
        #http://stackoverflow.com/questions/8828495/how-to-lock-a-sqlite3-database-in-python
        with self.con:

            next_available_height = self._get_next_block_height_not_claimed_start_at(
                starting_height=starting_height, claim_it=True)
            claimed_blocks = deque()
            claimed_blocks.append(next_available_height)
            for height in range(next_available_height + 1,
                                next_available_height + num_to_claim):
                claimed = self._claim_block_if_available(height)
                if claimed:
                    claimed_blocks.append(height)
                else:
                    break
            return claimed_blocks

    #TODO: this is now a pointless wrapper, remove it
    def get_next_block_height_available(self, starting_height=0,
                                        claim_it=False):
        """Get next block height not claimed by another worker.

        Args:
            starting_height (Optional[int]): The lowest block height to
                consider. This is needed, for example, if the worker first
                looks up the lowest height that contains the lowest deferred
                blame record.
            claim_it (Optional[bool]): Defaults to False. If set to true, the
                next unclaimed block will also be marked as claimed.
        """

        assert isinstance(starting_height, int)
        assert isinstance(claim_it, bool)

        self._initialize_block_span(max_block_height=starting_height,
                                    min_block_height=0)

        next_available_height = self._get_next_block_height_not_claimed_start_at(
            starting_height, claim_it)
        return next_available_height

    def _get_next_block_height_not_claimed_start_at(self, starting_height=0,
                                                    claim_it=False):
        """Helper function for `get_next_block_height_available`.

        Args:
            starting_height (Optional[int]): The lowest block height to
                consider. This is needed, for example, if the worker first
                looks up the lowest height that contains the lowest deferred
                blame record.
            claim_it (Optional[bool]): Defaults to False. If set to true, the
                next unclaimed block will also be marked as claimed.
        """

        assert isinstance(starting_height, int)

        self._initialize_block_span(max_block_height=starting_height,
                                    min_block_height=0)

        caller = '_get_next_block_height_not_claimed_start_at'

        #SQL is weird and apparently a NULL value is both != 1 and also not
        #   equal to 1
        stmt = ('SELECT MIN(block_height) AS min FROM '
                '' + SQL_TABLE_NAME_COORDINATION_REGISTER + ' WHERE '
                'pid_of_claimer IS NULL AND '
                '(completed IS NULL OR completed != 1) AND '
                'block_height >= ?;')
        arglist = (starting_height,)
        records = self.fetch_query_and_handle_errors(stmt, arglist, caller)
        assert len(records) < 2
        next_available_block = None
        try:
            next_available_block = records[0]['min']
            assert isinstance(next_available_block, int)
            if claim_it:
                self.claim_block_height(next_available_block)
            dprint("%s: Lowest unclaimed block height is %d" %
                   (caller, next_available_block))
            return next_available_block
        #Using error handling for conditional logic feels so wrong, but Python.
        #   `\_(''/)_/`
        except (IndexError, KeyError, AssertionError):
            #all blocks in this table so far are claimed or completed, so get
            #   the highest block completed/claimed and add 1
            stmt = ('SELECT MAX(block_height) AS max FROM '
                    '' + SQL_TABLE_NAME_COORDINATION_REGISTER + ' WHERE '
                    'block_height >= ?')
            arglist = (starting_height,)
            records = self.fetch_query_and_handle_errors(stmt, arglist, caller)
            try:
                if records[0]['max'] is not None:
                    next_available_block = int(records[0]['max']) + 1
                assert isinstance(next_available_block, int)
                if claim_it:
                    self.claim_block_height(next_available_block)
                dprint("%s: All blocks in db claimed, incremented to %d" %
                       (caller, next_available_block))
                return next_available_block
            except (IndexError, KeyError, AssertionError):
                #there are no blocks in the db yet. since the
                #   starting_height is not claimed nor complete, it is
                #   available.
                if claim_it:
                    self.claim_block_height(starting_height)
                dprint(("%s: There were no blocks in the coordination db at "
                        "or above the starting height. Returning starting "
                        "height %d" % (caller, starting_height)))
                return starting_height

#TODO: Consider making this a with-compatible class. See:
#   http://stackoverflow.com/questions/865115/how-do-i-correctly-clean-up-a-python-object
class Database:

    config_store                            = None
    con                                     = None #Connection to database
    cursor                                  = None #DB connection cursor

    #Only used when flag INSERT_BLAME_STATS_ONCE_PER_BLOCK is set to True
    in_memory_blame_cache                   = None #deque

    #This is an INSERT cache that builds up new records to insert into the
    #   tx output cache in the database so INSERTs can be performed in
    #   large batches.
    in_memory_tx_output_cache               = None #list of tuples

    #Only used when flag FETCH_DEFERRED_RECORDS_IN_BATCH is set to True
    #The first var is a deque containing row objects returned by
    #   fetch_query_and_handle_errors().
    in_memory_deferred_record_cache         = None
    #Keeps track of the last batch of records we fetched.
    last_fetched_deferred_record_rowid      = None

    deferred_blame_placeholder_rowid = None #fetch one and store in mem

    #TODO: Create general mechanism for caching and batch commiting SQL
    #   statements rather than maintaining all these separate vars and
    #   functions. It would be nice if we could at the same time remove
    #   responsibility from the caller to make push the batch.

    #This is an UPDATE cache that builds up new UPDATE statements to commit
    #   to the database so that the UPDATEs can be performed in large batches.
    #   Used only when UPDATE_BLAME_STATS_ONCE_PER_BLOCK is set to True.
    in_memory_updated_blame_record_cache = None

    #This is an UPDATE cache that builds up new UPDATE statements to commit to
    #   the database -- it is specific to updating the blame label cache table.
    #   Used only when UPDATE_BLAME_STATS_ONCE_PER_BLOCK is set to True.
    in_memory_update_blame_label_cache_cache = None

    #This is a DELETE cache taht builds up new DELETE statements to commit to
    #   the database sot hat the DELETEs can be performed in large batches.
    #   Used only when DELETE_BLAME_STATS_ONCE_PER_BLOCK is set to True.
    in_memory_deleted_blame_record_cache = None

    ############################ GENERAL FUNCTIONS #############################

    #Database constructor.
    #arg0: sqlite_db_filename (optional): Overrides all filenames set by
    #   config file
    #arg1: blockchain_mode (optional): Selects either the filename designated
    #   in the config file for remote API blockchain lookups, or the filename
    #   designated for bitcoind RPC blockchain lookups.
    def __init__(self, sqlite_db_filename = None,
                 blockchain_mode = config.BlockchainMode.REMOTE_API):
        if not isinstance(blockchain_mode, config.BlockchainMode):
            msg = ("Blockchain source must be a valid enum value: '%s'" %
                   str(blockchain_mode))
            logger.log_and_die(msg)
        self.config_store = config.Config(sqlite_db_filename, blockchain_mode)
        self.in_memory_blame_cache = deque()
        self.in_memory_deferred_record_cache = deque()
        self.last_fetched_deferred_record_rowid = -1
        self.in_memory_tx_output_cache = []
        self.in_memory_updated_blame_record_cache = []
        self.in_memory_update_blame_label_cache_cache = []
        self.in_memory_deleted_blame_record_cache = []

        ####### must be called last in __init__() #######
        self.db_init()

    def make_table(self, table_name, schema_as_dict):
        stmt = get_conditional_create_stmt(table_name, schema_as_dict)
        arglist = []
        self.run_statement(stmt, arglist)

    def db_init(self):
        print("Attepmting to initialize database connection..")
        try:
            self.con = sqlite3.connect(self.config_store.SQLITE_DB_FILENAME)
            self.con.row_factory = sqlite3.Row # permit accessing results by col name
            self.cursor = self.con.cursor()
        except Exception as e:
            if self.con is not None:
                self.con.close()
            msg = "Could not connect to database: " + str(e)
            logger.log_and_die(msg)
        print("Connected to database.")

        #Seems to resolve I/O issues with multiple processes accessing same db
        #   file at once.
        self.run_statement('PRAGMA journal_mode = TRUNCATE', [])

        self.make_table(SQL_TABLE_NAME_BLOCK_STATS, SQL_SCHEMA_BLOCK_STATS)
        self.make_table(SQL_TABLE_NAME_LAST_N_BLOCKS, SQL_SCHEMA_LAST_N_BLOCKS)
        self.make_table(SQL_TABLE_NAME_BLAME_STATS, SQL_SCHEMA_BLAME_STATS)
        self.make_table(SQL_TABLE_NAME_BLAME_IDS, SQL_SCHEMA_BLAME_IDS)
        self.make_table(SQL_TABLE_NAME_BLAME_LABEL_CACHE,
                        SQL_SCHEMA_BLAME_LABEL_CACHE_WITH_CONSTRAINTS)
        self.make_table(SQL_TABLE_NAME_ADDRESSES_SEEN,
                        SQL_SCHEMA_ADDRESSES_SEEN_WITH_CONSTRAINTS)
        self.make_table(SQL_TABLE_NAME_RELAYED_BY_CACHE,
                        SQL_SCHEMA_RELAYED_BY_CACHE_WITH_CONSTRAINTS)
        self.make_table(SQL_TABLE_NAME_TX_OUTPUT_CACHE,
                        SQL_SCHEMA_TX_OUTPUT_CACHE_WITH_CONSTRAINTS)
        self.make_table(SQL_TABLE_NAME_BLOCK_DATA_PRODUCTION_STATUS,
                        SQL_SCHEMA_BLOCK_DATA_PRODUCTION_STATUS)

    def run_statement(self, stmt, arglist, execute_many = False):
        """Execute a SQL statement that returns no results.

        The statement is handled as a single SQLite transaction.

        Args:
            stmt (str): The statement to execute.
            arglist (List): A list of parameters for the SQL statement, or a
                list of a list of parameters if `execute_many` is set to True.
            execute_many (Optional[bool]): When False (default), only a single
                list of parameters is exected. If set to True, this function
                expects a list of a list of parameters and repeatedly submits
                them using the SQL statement by using the `sqlite3` module's
                `executemany` function.
        """

        dprint("Statement: " + stmt)
        dprint("Arglist: " + str(arglist))
        last_error = None
        for sec_wait in range(1, NUM_ATTEMPTS_UPON_DB_ERROR):
            try:
                if execute_many:
                    self.cursor.executemany(stmt, arglist)
                else:
                    self.cursor.execute(stmt, arglist)
                self.con.commit()

                #TODO: This should return a value indicating whether the
                #   statement executed successfully or not
                return
            except Exception as e:
                last_error = e
                if sec_wait != NUM_ATTEMPTS_UPON_DB_ERROR:
                    #Could you give it a second? It's going to space.
                    print(("WARNING: Experienced error executing db statement, "
                           "trying again in %f seconds.") % float(sec_wait))
                    sleep(float(sec_wait))
        if self.con is not None:
            self.con.close()
        msg = (("Could not execute database statement after %d tries. Last "
                "error: %s") % (NUM_ATTEMPTS_UPON_DB_ERROR, str(last_error)))
        logger.log_and_die(msg)

    def manual_commit(self):
        self.con.commit()

    def close(self):
        self.con.close()

    def fetch_query(self, stmt, arglist):
        dprint("Attempting to fetch from database...")
        dprint("Statement: " + stmt)
        dprint("Arglist: " + str(arglist))

        last_error = None
        for sec_wait in range(1, NUM_ATTEMPTS_UPON_DB_ERROR):
            try:
                self.cursor.execute(stmt, arglist)
                fetched = self.cursor.fetchall()
                #con.close()
                for row in fetched:
                    dprint(str(row))
                dprint("Done fetching from database, fetched %d records." %
                       len(fetched))
                return fetched
            except Exception as e:
                last_error = e
                if sec_wait != NUM_ATTEMPTS_UPON_DB_ERROR:
                    #Could you give it a second? It's going to space.
                    print(("WARNING: Experienced error executing db statement, "
                           "trying again in %f seconds.") % float(sec_wait))
                    sleep(float(sec_wait))

        msg = ("Could not fetch from database. Statement was '%s' error is "
               "'%s' database filename is '%s'" %
               (stmt, str(last_error), self.config_store.SQLITE_DB_FILENAME))
        if self.con is not None:
            self.con.close()
        logger.log_and_die(msg)

    #arg2: caller is the name of the function calling this, not including parens
    #returns the records fetched from the DB query, or None if records are empty
    def fetch_query_and_handle_errors(self, stmt, arglist, caller):
        records = self.fetch_query(stmt, arglist)

        if records is None:
            msg = 'Received null value from Database query in %s().' % caller
            logger.log_and_die(msg)
        if not records:
            return None #empty
        return records

    #Fetch a query that ought to return only a single integer or string value.
    #   If the query returns an empty result set, return None.
    #param0: stmt: The SQL statement to execute
    #param1: arglist: The arguments to pass to the parameterized SQL statement
    #param2: caller: Name of the function calling this one
    #param3: colName The column name or 'AS' SQL alias of the datum being
    #   fetched. If you are fetching something other than a column name, use an
    #   SQL alias in the statement with the 'AS' specifier.
    #param4: data_type: 'int' or 'string'
    def fetch_query_single_datum(self, stmt, arglist, caller, column_name,
                                 data_type):
        if data_type is not 'int' and data_type is not 'string':
            msg = ("Invalid data_type in fetch_query_single_datum by caller '"
                   "" + caller + "().")
            logger.log_and_die(msg)
        records = self.fetch_query_and_handle_errors(stmt, arglist, caller)
        if records is None or len(records) == 0:
            return None
        if column_name not in records[0] and records[0][column_name] is None:
            #empty result set, return None
            #TODO: not sure why column_name is not in records here, but
            #   that seems to be the case for get_last_block_height_in_db()
            #   and I don't feel like investigating why.
            return None
        if len(records) > 1:
            msg = ("Expected a single record returned from db in '"
                   "" + caller + "()' but received '" + str(records) + "'")
            logger.log_and_die(msg)
        try:
            if data_type is 'int':
                int_val = int(records[0][column_name])
                return int_val
            elif data_type is 'string':
                str_val = str(records[0][column_name])
                return str_val
        except IndexError:
            msg = ("Malformed response returned from database in '"
                   "" + caller + "()'. Contents: '" + str(records) + "'")
            logger.log_and_die(msg)
        except TypeError as type_error:
            if records[0] is None:
                return None
            else:
                msg = ("Unknown type error parsing '" + str(records[0]) + ""
                       "' as a " + data_type + " in '" + caller + "()': '"
                       "" + str(type_error) + "'")
                logger.log_and_die(msg)
        except ValueError:
            msg = ("Expected a " + data_type + " returned in '"
                   "" + caller + "()' but received instead: '"
                   "" + str(records[0]) + "'")
            logger.log_and_die(msg)

    #Fetch a query that ought to return only a single integer value. If the
    #   query returns an empty result set, return None.
    def fetch_query_single_int(self, stmt, arglist, caller, column_name):
        return self.fetch_query_single_datum(stmt, arglist, caller,
                                             column_name, 'int')

     #Fetch a query that ought to return only a single string value. If the
    #   query returns an empty result set, return None.
    def fetch_query_single_str(self, stmt, arglist, caller, column_name):
        return self.fetch_query_single_datum(stmt, arglist, caller,
                                             column_name, 'string')

    def reset_in_memory_deferred_record_cache(self):
        """ Clear the cache of records we've previously fetched.

        This can be called, for example, if a block processor finishes a block
        at a certain height and needs to skip to a higher block height than
        the next consecutive block; in this case, there will likely be a bunch
        of records that are no longer relevant to that block processor.
        Since rowids should be strictly increasing, we don't need to change
        `last_fetched_deferred_record_rowid`.
        """

        self.in_memory_deferred_record_cache = deque()

    ########################## BLOCK STATS FUNCTIONS ###########################

    #Returns the integer height of the last block stored processed in the db.
    #   If no block has been processed, returns None.
    def get_last_block_height_in_db(self):
        stmt = ' SELECT MAX(block_num) AS ' + SQL_ALIAS_HIGHEST_BLOCK_NUM + \
            ' FROM ' + SQL_TABLE_NAME_BLOCK_STATS
        arglist = []
        caller = 'get_last_block_height_in_db'
        column_name = SQL_ALIAS_HIGHEST_BLOCK_NUM
        return self.fetch_query_single_int(stmt, arglist, caller, column_name)

    def record_block_stats(self, block_state):
        col_names = get_comma_separated_list_of_col_names(
            SQL_SCHEMA_BLOCK_STATS)
        stmt = ('INSERT INTO ' + SQL_TABLE_NAME_BLOCK_STATS + '('
                '' + col_names + ') VALUES (?,?,?,?,?,?,?)')
        arglist = (block_state.block_num,
                   block_state.tx_total_num,
                   block_state.tx_sendback_reuse_num,
                   block_state.tx_receiver_has_tx_history_num,
                   block_state.tx_sendback_reuse_pct,
                   block_state.tx_receiver_has_tx_history_pct,
                   block_state.PROCESS_TYPE_VERSION_NUM)
        self.run_statement(stmt, arglist)

    def get_block_stats(self, block_height):
        var_name = 'block_height'
        caller = 'get_block_stats'
        validate.check_int_and_die(block_height, var_name, caller)

        stmt = ('SELECT tx_total_num AS tx_total_num, tx_sendback_reuse_pct AS '
                'tx_sendback_reuse_pct, tx_receiver_has_tx_history_pct AS '
                'tx_receiver_has_tx_history_pct FROM '
                '' + SQL_TABLE_NAME_BLOCK_STATS + ' WHERE block_num = ?')
        arglist=(block_height,)
        records = self.fetch_query_and_handle_errors(stmt, arglist, caller)
        if len(records) == 0:
            return []
        elif records is None:
            msg = (("Expected non-None result in %s() for block height %d") %
                   (caller, block_height))
            logger.log_and_die(msg)
        elif len(records) > 1:
            msg = (("Expected a single row in %s() for block height %d") %
                   (caller, block_height))
            logger.log_and_die(msg)

        tx_total_num = records[0]['tx_total_num']
        tx_sendback_reuse_pct = records[0]['tx_sendback_reuse_pct']
        tx_receiver_has_tx_history_pct = records[0]['tx_receiver_has_tx_history_pct']
        stats = blame_stats.BlameStatsPerBlock(block_height, tx_total_num,
                                               tx_sendback_reuse_pct,
                                               tx_receiver_has_tx_history_pct)

        return stats

    #Returns address reuse stats for the specified span of blocks. If there
    #   are no records for that span, returns emtpy list.
    def get_block_stats_for_span(self, min_block_height,
                                       max_block_height):
        caller = 'get_block_stats_for_span'
        validate.check_int_and_die(min_block_height, 'min_block_height', caller)
        validate.check_int_and_die(max_block_height, 'max_block_height', caller)

        #TODO: probably can remove all of the 'AS' clauses
        stmt = ('SELECT block_num AS block_num, tx_total_num AS tx_total_num, '
                'tx_sendback_reuse_pct AS tx_sendback_reuse_pct, '
                'tx_receiver_has_tx_history_pct AS '
                'tx_receiver_has_tx_history_pct FROM '
                '' + SQL_TABLE_NAME_BLOCK_STATS + ' WHERE block_num <= ? AND '
                'block_num >= ?')
        arglist = (max_block_height, min_block_height,)
        records = self.fetch_query_and_handle_errors(stmt, arglist, caller)
        if records is None:
            msg = (("Expected non-None result in %s() for blocks %d through "
                    "%d") % (caller, min_block_height, max_block_height))
            logger.log_and_die(msg)
        all_stats = []
        for record in records:
            block_height = record['block_num']
            tx_total_num = record['tx_total_num']
            tx_sendback_reuse_pct = record['tx_sendback_reuse_pct']
            tx_receiver_has_tx_history_pct = record[
                'tx_receiver_has_tx_history_pct']
            stats = blame_stats.BlameStatsPerBlock(block_height, tx_total_num,
                                                   tx_sendback_reuse_pct,
                                                   tx_receiver_has_tx_history_pct)
            all_stats.append(stats)
        return all_stats

    ############################ BLAME DB FUNCTIONS ############################

    #Given an address reuse role, get a list of all of the unique bitcoin
    #   addresses that have been blamed.
    def get_blamed_address_list_for_role(self, role):
        if not isinstance(role, AddressReuseRole):
            death_msg = ("Addr reuse role is not a proper enum object: '%s'" %
                         str(role))

        stmt = ('SELECT DISTINCT relevant_address FROM '
                ''+ SQL_TABLE_NAME_BLAME_STATS + ' WHERE role = ?')
        arglist = (role,)
        records = self.fetch_query(stmt, arglist)

        if records is None:
            msg = ('Received null value from Database query in '
                   'get_blamed_address_list_for_role().')
            logger.log_and_die(msg)
        if not records:
            return []
        else:
            address_list = []
            for record in records:
                address_list.append(record['relevant_address'])
            return address_list

    #Returns integer value of the id ('rowid' col) for the specified
    #   blame_label in the database. If missing from the database, returns None.
    def get_blame_id_for_label(self, blame_label):
        stmt = ('SELECT rowid FROM ' + SQL_TABLE_NAME_BLAME_IDS + ''
                ' WHERE label = ? LIMIT 1')
        arglist = (blame_label,)
        caller = 'get_blame_id_for_label'
        column_name = 'rowid'
        return self.fetch_query_single_int(stmt, arglist, caller, column_name)

    def get_blame_id_for_label_and_insert_if_new(self, blame_label):
        #Note: We don't need to encode 'blame_label', because we won't store it
        #   in the DB in this function.
        #get the ROWID for this blamed party
        blame_id = self.get_blame_id_for_label(blame_label)
        if blame_id is None or not blame_id:
            #This blamed party not in db yet and therefore is new
            self.add_blame_party(blame_label)
            blame_id = self.get_blame_id_for_label(blame_label)
            assert blame_id is not None
        return blame_id

    #Returns integer value of the id ('rowid' col) for the specified address
    #   reuse role and relevant BTC address in a blame record in the database.
    #   If missing from the database, returns None.
    def get_blame_id_for_role_and_address(self, role, relevant_address):
        caller = 'get_blame_id_for_role_and_address'
        validate.check_address_and_die(relevant_address, caller)
        if not isinstance(role, AddressReuseRole):
            death_msg = ("Address reuse role is not a proper enum object: '%s'" %
                         str(role))
        stmt = ('SELECT blame_recipient_id FROM '
                '' + SQL_TABLE_NAME_BLAME_STATS + ' WHERE role = ? AND '
                'relevant_address = ? LIMIT 1')
        arglist = (role, relevant_address)
        caller = 'get_blame_id_for_role_and_address'
        column_name = 'blame_recipient_id'
        return self.fetch_query_single_int(stmt, arglist, caller, column_name)

    #Returns wallet cluster label for id ('rowid' col) of the party blamed for
    #   one or more instances of address reuse.
    def get_blame_label_for_blame_id(self, blame_party_id):
        assert isinstance(blame_party_id, int)
        #validate.check_int_and_die(blame_party_id, 'blame_party_id', 'get_blame_label_for_blame_id')
        stmt = ('SELECT label FROM ' + SQL_TABLE_NAME_BLAME_IDS + ' WHERE '
                'rowid = ? LIMIT 1')
        arglist = (blame_party_id,)
        caller = 'get_blame_label_for_blame_id'
        column_name = 'label'
        return self.fetch_query_single_str(stmt, arglist, caller, column_name)

    #param0: blame_label: The string that presents the wallet that the address
    #   belongs to. It wil be HTML encoded before being stored.
    def add_blame_party(self, blame_label):
        label_escaped = html_escape(blame_label)
        col_names = get_comma_separated_list_of_col_names(SQL_SCHEMA_BLAME_IDS)
        stmt = ('INSERT INTO ' +  SQL_TABLE_NAME_BLAME_IDS + '('
                '' + col_names + ') VALUES(?)')
        arglist = (label_escaped,)
        self.run_statement(stmt, arglist)

    #helper function for store_blame() that actually performs the INSERT using
    #   the blame_id that store_blame queried from the database.
    def add_blame_record(self, blame_party_id, address_reuse_type, role,
                         data_source, block_height, confirmed_tx_id,
                         relevant_address):
        #Validate arguments
        death_msg = ''
        assert isinstance(blame_party_id, int)
        assert isinstance(address_reuse_type, AddressReuseType)
        assert isinstance(role, AddressReuseRole)
        assert isinstance(data_source, DataSource)
        assert isinstance(block_height, int)
        validate.check_hex_and_die(confirmed_tx_id, 'add_blame_record')
        validate.check_address_and_die(relevant_address, 'add_blame_record')

        col_names = get_comma_separated_list_of_col_names(SQL_SCHEMA_BLAME_STATS)

        stmt = ('INSERT INTO ' + SQL_TABLE_NAME_BLAME_STATS + '('
                '' + col_names + ') VALUES (?,?,?,?,?,?,?)')
        arglist = (blame_party_id, address_reuse_type, role, data_source,
                   block_height, confirmed_tx_id, relevant_address)
        self.run_statement(stmt, arglist)

    #Store a blame record in the database. If the
    #   db.INSERT_BLAME_STATS_ONCE_PER_BLOCK flag is set to True, the caller
    #   must call write_stored_blame() in order to actually wite to the
    #   database, since only the caller knows how many transactions are in the
    #   block being processed. TODO: This is awkward, take away this
    #   responisibility from the caller somehow.
    def store_blame(self, blame_label, address_reuse_type, role, data_source,
                    block_height, confirmed_tx_id, relevant_address):
        if not INSERT_BLAME_STATS_ONCE_PER_BLOCK:
            blame_id = self.get_blame_id_for_label_and_insert_if_new(
                blame_label)

            #Store this blame record into db using the fetched id
            self.add_blame_record(blame_id, address_reuse_type, role,
                                  data_source, block_height, confirmed_tx_id,
                                  relevant_address)
        else:
            blame_record_tuple = (blame_label, address_reuse_type, role,
                                  data_source, block_height, confirmed_tx_id,
                                  relevant_address)
            self.in_memory_blame_cache.append(blame_record_tuple)
            dprint("Added blame tuple to cache: " + str(blame_record_tuple))

    #This must be called by any users of this class that uses the store_blame()
    #   function when the db.INSERT_BLAME_STATS_ONCE_PER_BLOCK flag is set to
    #   True in order to write the blame stats from Python memory to the
    #   database in one batch. It should be called, e.g. once per block.
    def write_stored_blame(self):
        #We will create two batches of INSERT statements: One to create a new
        #   blame id (rowid) for the blame label of each stored blame record if
        #   it is not already present. A second to insert the records
        #   themselves into the database. The first batch of INSERTs must be
        #   done first because the second will reference the rowid.

        #this is the executemany-style arglist for first INSERT, the actual
        #   INSERT statement is defined in push_batch_stored_blame_inserts().
        insert_if_new_arglist = []

        #second INSERT
        record_insert_stmt_build = string.StringBuilder()
        record_insert_stmt_build.append(self.get_blame_stats_insert_header())
        record_insert_arglist = []

        num_select_terms = 0 #counter keeps track of batch
        blame_record_tuple = None
        while True:
            try:
                blame_record_tuple = self.in_memory_blame_cache.popleft() #FIFO
            except IndexError:
                break #went through all the records cached in deque. done.

            blame_label         = blame_record_tuple[0]
            address_reuse_type  = blame_record_tuple[1]
            role                = blame_record_tuple[2]
            data_source         = blame_record_tuple[3]
            block_height        = blame_record_tuple[4]
            confirmed_tx_id     = blame_record_tuple[5]
            relevant_address    = blame_record_tuple[6]

            insert_if_new_arglist.append((blame_label, blame_label))

            blame_recipient_id_select = ('(SELECT rowid FROM '
                                         '' + SQL_TABLE_NAME_BLAME_IDS + ' '
                                         'WHERE label = ? LIMIT 1)')
            record_insert_arglist.append(blame_label)
            row = '(' + blame_recipient_id_select + ', ?, ?, ?, ?, ?, ?),'
            record_insert_stmt_build.append(row)
            record_insert_arglist.append(address_reuse_type)
            record_insert_arglist.append(role)
            record_insert_arglist.append(data_source)
            record_insert_arglist.append(block_height)
            record_insert_arglist.append(confirmed_tx_id)
            record_insert_arglist.append(relevant_address)

            num_select_terms = num_select_terms + 1
            #each compound SELECT statement can only contain at most
            #   SQLITE_MAX_COMPOUND_SELECT terms, forcing us to INSERT in
            #   batches.
            if num_select_terms == SQLITE_MAX_COMPOUND_SELECT:
                self.push_batch_stored_blame_inserts(
                    record_insert_stmt_build, record_insert_arglist,
                    insert_if_new_arglist)

                #reset for next batch
                record_insert_stmt_build = string.StringBuilder()
                record_insert_stmt_build.append(
                    self.get_blame_stats_insert_header())
                record_insert_arglist = []
                insert_if_new_arglist = []
                num_select_terms = 0

        if num_select_terms > 0: #last batch
            self.push_batch_stored_blame_inserts(record_insert_stmt_build,
                                                 record_insert_arglist,
                                                 insert_if_new_arglist)

    def get_blame_stats_insert_header(self):
        #We're doing two non-obvious SQL things here with the second INSERT
        #   statement: 1) the first column inserted is the result of a SELECT
        #   statement, creating a compound SELECT statement. Secondly, we are
        #   inserting multiple rows at a time.
        return ('INSERT INTO ' + SQL_TABLE_NAME_BLAME_STATS + ''
                '(blame_recipient_id, address_reuse_type, role, '
                'data_source, block_height, confirmed_tx_id, '
                'relevant_address) VALUES ')

    #Actually pushes a bunch of INSERTs to the database. Helper function for
    #   write_stored_blame()
    def push_batch_stored_blame_inserts(self, record_insert_stmt_build,
                                        record_insert_arglist,
                                        insert_if_new_arglist):

        #TODO: this can be simplified to INSERT OR IGNORE INTO if the
        #   `label` field is made UNIQUE as a constraint.
        #http://stackoverflow.com/questions/19337029/insert-if-not-exists-statement-in-sqlite
        insert_if_new_stmt = ('INSERT INTO ' + SQL_TABLE_NAME_BLAME_IDS + ' '
                              '(label) SELECT ? WHERE NOT EXISTS (SELECT 1 '
                              'FROM ' + SQL_TABLE_NAME_BLAME_IDS + ' WHERE '
                              'label = ?)')
        self.run_statement(insert_if_new_stmt, insert_if_new_arglist,
                           execute_many = True)

        record_insert_stmt = str(record_insert_stmt_build)
        record_insert_stmt = record_insert_stmt.rstrip(',') #remove trailing comma
        self.run_statement(record_insert_stmt, record_insert_arglist)

    def get_all_distinct_addresses_from_blame_records(self):
        stmt = ('SELECT DISTINCT relevant_address FROM '
                '' + SQL_TABLE_NAME_BLAME_STATS + '')
        arglist = []
        records = self.fetch_query(stmt, arglist)

        if records is None:
            msg = ('Received null value from Database query in '
                   'get_all_distinct_addresses_from_blame_records().')
            logger.log_and_die(msg)
        if not records:
            return []
        else:
            address_list = []
            for record in records:
                address_list.append(record['relevant_address'])
            return address_list

    #returns a list of top blamed parties as referenced by their integer rowid
    #   in the blame ID table
    #param0: num_reusers: The number of parties that should be included in the
    #   list. If there aren't that many distinct parties in the databse, we'll
    #   just get fewer back.
    #param1: min_block_height (Optional): The lowest block height to consider.
    #   Default will be block zero.
    #param2: max_block_height (Optional): The highest block height to consider.
    #   Default will be whatever the highest block height that we've processed
    #   and stored in the database so far.
    def get_top_address_reuser_ids(self, num_reusers, min_block_height = None,
                                   max_block_height = None):

        #in all cases, we should ignore the DB_DEFERRED_BLAME_PLACEHOLDER.
        def_id = self.get_blame_id_for_deferred_blame_placeholder()
        if def_id is None:
            #Some DB tests will not have DB_DEFERRED_BLAME_PLACEHOLDER set;
            #   rather than requiring them to, just set this to an impossible
            #   rowid so that the SQL statement doesn't choke on None.
            def_id = -1

        #specify_min_or_max = False

        if min_block_height is not None:
            validate.check_int_and_die(min_block_height, 'min_block_height',
                                       'get_top_address_reuser_ids')
            #specify_min_or_max = True
        if max_block_height is not None:
            validate.check_int_and_die(max_block_height, 'max_block_height',
                                       'get_top_address_reuser_ids')
            #specify_min_or_max = True
        validate.check_int_and_die(num_reusers, 'num_reusers',
                                   'get_top_address_reuser_ids')


        stmt_build = string.StringBuilder()
        arglist = []
        stmt_build.append(('SELECT DISTINCT blame_recipient_id FROM '
                           '' + SQL_TABLE_NAME_BLAME_STATS + ' '))

        #omit DB_DEFERRED_BLAME_PLACEHOLDER from results
        stmt_build.append('WHERE blame_recipient_id != ? ')
        arglist.append(def_id)

        if min_block_height is not None:
            stmt_build.append('AND block_height >= ? ')
            arglist.append(min_block_height)
        if max_block_height is not None:
            stmt_build.append('AND block_height <= ? ')
            arglist.append(max_block_height)

        stmt_build.append(('GROUP BY blame_recipient_id ORDER BY COUNT(*) DESC '
                           'LIMIT ?'))
        arglist.append(num_reusers)

        stmt = str(stmt_build)

        records = self.fetch_query_and_handle_errors(
            stmt, arglist, 'get_top_address_reuser_ids')
        if records is None:
            #Possibly something wrong here, panic
            msg = (("Expected non-None result in get_top_address_reuser_ids() "
                   "for min_block_height=%s max_block_height=%s num_reusers=%s") %
                    (str(min_block_height), str(max_block_height),
                     str(num_reusers)))
            logger.log_and_die(msg)
        if len(records) == 0:
            return []

        blamed_party_ids = []
        for row in records:
            blamed_party_id = row['blame_recipient_id']
            blamed_party_ids.append(blamed_party_id)
        return blamed_party_ids

    #Returns a list of records from the database including the block height
    #   and number of records for that address_reuse type per block for the
    #   specified blame_party_id. In other words, "give me the number of
    #   instances of SomeBitcoinCompany sending to addresses with a prior tx
    #   history, per block between block heights X and Y."
    #param0: blame_party_id: The address reuser you want to query data for.
    #param1: address_reuse_type: The type of address reuse you want to query
    #   data for.
    #param2: min_block_height (Optional): The minimum block height to consider
    #   data for. Default: genesis block.
    #param3: max_block_height (Optional): The maximum block height to consider
    #   data for. Default: Highest block currently in the database.
    #Returns list of records. Each record is a tuple containing these fields:
    #   * block_height => integer
    #   * count => integer
    def get_num_records_across_block_span(self, blame_party_id,
                                          address_reuse_type,
                                          min_block_height = 0,
                                          max_block_height = None):
        if not isinstance(address_reuse_type, AddressReuseType):
            msg = ("Address reuse type is not a proper enum object: '%s'" %
                   str(address_reuse_type))
            logger.log_and_die(msg)
        caller = 'get_num_records_across_block_span'
        validate.check_int_and_die(blame_party_id, 'blame_party_id', caller)
        validate.check_int_and_die(min_block_height, 'min_block_height', caller)
        validate.check_int_and_die(max_block_height, 'max_block_height', caller)

        stmt = ('SELECT block_height,  COUNT(DISTINCT confirmed_tx_id) AS '
                'count FROM ' + SQL_TABLE_NAME_BLAME_STATS + ' WHERE '
                'address_reuse_type = ? AND blame_recipient_id = ? AND '
                'block_height <= ? AND block_height >= ? GROUP BY '
                'block_height, blame_recipient_id')
        arglist = (address_reuse_type, blame_party_id, max_block_height,
                   min_block_height)
        records = self.fetch_query_and_handle_errors(stmt, arglist, caller)
        if records is None:
            no_records = []
            for block_height in range(min_block_height, max_block_height + 1):
                no_records.append({'block_height': block_height, 'count': 0})
            return no_records
        else:
            return records

    def get_num_records(self, address_reuse_type, blame_party_id, block_height):
        assert isinstance(address_reuse_type, AddressReuseType)
        assert isinstance(blame_party_id, int)
        assert isinstance(block_height, int)

        stmt = ('SELECT COUNT(DISTINCT confirmed_tx_id) AS count FROM '
                '' + SQL_TABLE_NAME_BLAME_STATS + ''
                ' WHERE address_reuse_type = ? AND blame_recipient_id = ? '
                'AND block_height = ? GROUP BY blame_recipient_id')
        arglist = (address_reuse_type, blame_party_id, block_height)
        caller = 'get_num_records_for_address_reuse_type_and_id'
        column_name = 'count'
        num_records = self.fetch_query_single_int(stmt, arglist, caller,
                                                  column_name)
        if num_records is None:
            return 0
        else:
            return num_records

    #param0: block_height: The block that you want the stats for.
    #param1: blame_party_ids: The stats will include some stats specific to
    #   particular parties. This parameter specifies which parties should be
    #   included. Each int in the list should be the rowid of the party in the
    #   blame ID table.
    #Returns: A list of BlameStatsPerBlock objects
    def get_blame_stats_for_block_height(self, block_height, blame_party_ids):
        stats = self.get_block_stats(block_height)

        for blame_party_id in blame_party_ids:
            blame_label = self.get_blame_label_for_blame_id(blame_party_id)
            num_tx_with_sendback_reuse = self.get_num_records(
                AddressReuseType.SENDBACK, blame_party_id, block_height)
            stats.add_sendback_reuse_blamed_party(blame_label,
                                                  num_tx_with_sendback_reuse)

            num_tx_with_history_reuse = self.get_num_records(
                AddressReuseType.TX_HISTORY, blame_party_id, block_height)
            stats.add_history_reuse_blamed_party(blame_label,
                                                 num_tx_with_history_reuse)

        return stats

    #wrapper for get_blame_stats_for_block_span that consolidates stats
    #   into averages at a specified integer resolution > 1. This will iterate
    #   over data again after the wrapped function does, so it is not very
    #   efficient. #TODO: consolidate.
    #TODO: function is long and looks like it got beat by an ugly stick
    def get_blame_stats_for_block_span_and_resolution(self, blame_party_ids,
                                                      min_block_height = 0,
                                                      max_block_height = None,
                                                      csv_dump_filename = None,
                                                      block_resolution=1):
        assert isinstance(block_resolution, int)
        assert block_resolution > 0
        all_stats = self.get_blame_stats_for_block_span(blame_party_ids,
                                                        min_block_height,
                                                        max_block_height,
                                                        csv_dump_filename)
        if block_resolution == 1:
            return all_stats
        else:
            dprint(("Fetched %d stats, now breaking into pieces of %d "
                    "records.") % (len(all_stats), block_resolution))
            #break into pieces and get averages for each piece
            averaged_stats = []
            for i in range(0, len(all_stats), block_resolution):
                dprint(("get_blame_stats_for_block_span_and_resolution: Trying "
                        "to consolidate %d blocks of records starting at "
                        "record # %d of %d" % (block_resolution, i,
                                               len(all_stats))))
                #let the first block represent the height for the range
                block_height = all_stats[i].block_height
                num_tx_total = 0
                total_pct_tx_with_sendback_reuse = 0.0
                total_pct_tx_with_history_reuse = 0.0
                party_label_to_total_pct_sendback_map = dict()
                party_label_to_total_pct_history_map = dict()
                top_reuser_labels = all_stats[i].top_reuser_labels
                dprint(("get_blame_stats_for_block_span_and_resolution: Found "
                        "list of %d top reuser labels at record # %d" %
                        (len(top_reuser_labels), i)))
                stats_in_this_piece = get_up_to_n_items(all_stats,
                                                        start_index=i,
                                                        n=block_resolution)
                num_stats_in_this_piece = len(stats_in_this_piece)

                for top_reuser_label in top_reuser_labels:
                    party_label_to_total_pct_sendback_map[top_reuser_label] = 0.0
                    party_label_to_total_pct_history_map[top_reuser_label] = 0.0
                for j in range(0, len(stats_in_this_piece)):
                    num_tx_total = num_tx_total + stats_in_this_piece[j].num_tx_total
                    total_pct_tx_with_sendback_reuse = total_pct_tx_with_sendback_reuse + \
                        float(stats_in_this_piece[j].pct_tx_with_sendback_reuse)
                    total_pct_tx_with_history_reuse = total_pct_tx_with_history_reuse + \
                        float(stats_in_this_piece[j].pct_tx_with_history_reuse)
                    #TODO: holy crap this is ugly
                    for top_reuser_label in top_reuser_labels:
                        party_label_to_total_pct_sendback_map[
                            top_reuser_label] = party_label_to_total_pct_sendback_map[
                                                top_reuser_label] + float(
                                                stats_in_this_piece[
                                                j].party_label_to_pct_sendback_map[
                                                top_reuser_label])
                        party_label_to_total_pct_history_map[
                            top_reuser_label] = party_label_to_total_pct_history_map[
                                                top_reuser_label] + float(
                                                stats_in_this_piece[
                                                j].party_label_to_pct_history_map[
                                                top_reuser_label])
                avg_pct_tx_with_sendback_reuse = total_pct_tx_with_sendback_reuse / num_stats_in_this_piece
                avg_pct_tx_with_history_reuse = total_pct_tx_with_history_reuse / num_stats_in_this_piece

                piece_stats = blame_stats.BlameStatsPerBlock(
                    block_height, num_tx_total, avg_pct_tx_with_sendback_reuse,
                    avg_pct_tx_with_history_reuse)
                piece_stats.top_reuser_labels = top_reuser_labels

                for top_reuser_label in top_reuser_labels:
                    avg_sendback_for_reuser = party_label_to_total_pct_sendback_map[
                        top_reuser_label] / num_stats_in_this_piece
                    avg_history_for_reuser = party_label_to_total_pct_history_map[
                        top_reuser_label] / num_stats_in_this_piece

                    piece_stats.party_label_to_pct_sendback_map[
                        top_reuser_label] = avg_sendback_for_reuser
                    piece_stats.party_label_to_pct_history_map[
                        top_reuser_label] = avg_history_for_reuser

                averaged_stats.append(piece_stats)

            dprint("Conslidated into %d records." % len(averaged_stats))
            return averaged_stats

    #param0: blame_party_ids: The stats will include some stats specific to
    #   particular parties. This parameter specifies which parties should be
    #   included. Each int in the list should be the rowid of the party in the
    #   blame ID table.
    #param1: min_block_height (Optional): Minimum block height we want stats
    #   for. Default = 0
    #param2: max_block_height (Optional): Maximum block height we want stats
    #   for. Default = highest block in DB
    #param3: csv_dump_filename (Optional): If set, all of the data accumulated
    #   by this function will be dumped to a file in Comma-Separated-Value
    #   format, so it can be easily loaded later, or exported for other
    #   purposes.
    #Returns: A list of BlameStatsPerBlock objects
    #TODO: function is long, refactor it
    def get_blame_stats_for_block_span(self, blame_party_ids,
                                       min_block_height = 0,
                                       max_block_height = None,
                                       csv_dump_filename = None):
        if csv_dump_filename is not None:
            raise NotImplementedError #TODO

        #fetch generic address reuse stats for each block in the span
        #note: get_block_stats_for_span validates block_height params
        stats_over_span = self.get_block_stats_for_span(min_block_height,
                                                         max_block_height)

        dprint("stats_over_span length %d " % len(stats_over_span))
        if max_block_height is not None:
            assert len(stats_over_span) == (max_block_height - \
                                            min_block_height + 1)

        #fetch address reuse stats about particular entities in the span
        #Note: This will be slow if there are many blame_party_ids:
        for blame_party_id in blame_party_ids:
            blame_label = self.get_blame_label_for_blame_id(blame_party_id)

            sendback_record_rows = self.get_num_records_across_block_span(
                blame_party_id, AddressReuseType.SENDBACK, min_block_height,
                max_block_height)
            tx_history_record_rows = self.get_num_records_across_block_span(
                blame_party_id, AddressReuseType.TX_HISTORY, min_block_height,
                max_block_height)
            #assertions: a given party might not have records for each block
            #   in the span, but we shouldn't get back MORE rows than 1 per
            #   block height.
            assert len(stats_over_span) >= len(sendback_record_rows)
            assert len(stats_over_span) >= len(tx_history_record_rows)

            #Fill in the address reuse stats for this address reuser in the
            #   list of per-block stat's we're returning. This blame party
            #   may not have stats for every block in the span, so fill those
            #   in with zeroes.
            sendback_iter = 0
            tx_history_iter = 0

            for i in range(0, len(stats_over_span)):
                block_height_to_fill = min_block_height + i
                assert block_height_to_fill == stats_over_span[i].block_height

                num_tx_with_sendback_reuse = 0
                try:
                    sendback_cur_height = sendback_record_rows[
                                                sendback_iter]['block_height']
                    if sendback_cur_height == block_height_to_fill:
                        num_tx_with_sendback_reuse = sendback_record_rows[
                                                        sendback_iter]['count']
                        sendback_iter = sendback_iter + 1
                except IndexError:
                    pass

                num_tx_with_history_reuse = 0
                try:
                    tx_history_cur_height = tx_history_record_rows[
                                                tx_history_iter]['block_height']
                    if tx_history_cur_height == block_height_to_fill:
                        num_tx_with_history_reuse = tx_history_record_rows[
                                                        tx_history_iter]['count']
                        tx_history_iter = tx_history_iter + 1
                except IndexError:
                    pass

                stats_over_span[i].add_sendback_reuse_blamed_party(
                    blame_label, num_tx_with_sendback_reuse)
                stats_over_span[i].add_history_reuse_blamed_party(
                    blame_label, num_tx_with_history_reuse)

        return stats_over_span

    def get_lowest_block_height_with_deferred_records(self):
        """Get lowest block height with deferred blame records.

        This will help identify where to start updating deferred records with
        actual blame records. If there are no records in the database with
        defeferred blame, returns None.
        """
        deferred_id = self.get_blame_id_for_deferred_blame_placeholder()
        if deferred_id is None:
            return None
        else:
            stmt = ('SELECT MIN(block_height) AS min FROM '
                    '' + SQL_TABLE_NAME_BLAME_STATS + ' WHERE '
                    'blame_recipient_id = ?')
            arglist = (deferred_id,)
            caller = 'get_lowest_block_height_with_deferred_records'
            column_name = 'min'
            min_height = self.fetch_query_single_int(stmt, arglist, caller,
                                                  column_name)
            dprint("%s: min_height for deferred record is %d." % (caller,
                                                                  min_height))
            return min_height

    def is_deferred_record_at_height(self, block_height):
        """Does db have any deferred blame records at the specified height? """

        assert isinstance(block_height, int)

        deferred_id = self.get_blame_id_for_deferred_blame_placeholder()
        if deferred_id is None:
            return False
        stmt = ('SELECT 1 AS one FROM %s WHERE block_height = ? AND '
                'blame_recipient_id = ? LIMIT 1') % SQL_TABLE_NAME_BLAME_STATS
        arglist = (block_height, deferred_id)
        caller = 'is_deferred_record_at_height'
        column_name = 'one'
        result = self.fetch_query_single_int(stmt, arglist, caller, column_name)
        if result is None:
            return False
        else:
            return True

    def fetch_more_deferred_records_for_cache(self, deferred_id,
                                              min_block_height=None):
        """Adds records to the `in_memory_deferred_record_cache` from the db.

        This query will not consider records with a rowid below
        `last_fetched_deferred_record_rowid`, which is the point where the
        previous fetch operation left off.

        Args:
            deferred_id (int): The `rowid` of the deferred blame placeholder
                in the db.
            min_block_height (Optional[int]): The minimum block height at which
                to consider fetching records. By default, no mimimum is set,
                which effectively equates to a minimum of zero.

        Raises:
            custom_errors.NoDeferredRecordsRemaining: If there are no more
                records to be fetched in the database, this error is raised.
        """
        assert isinstance(deferred_id, int)
        if min_block_height is not None:
            assert isinstance(min_block_height, int)

        stmt_build = string.StringBuilder()
        stmt_build.append('SELECT rowid, role, data_source, confirmed_tx_id, '
                          'address_reuse_type, relevant_address, block_height '
                          'FROM ' + SQL_TABLE_NAME_BLAME_STATS + ' WHERE rowid '
                          '> ?  AND blame_recipient_id = ? ')
        if min_block_height is not None:
            stmt_build.append('AND block_height >= ? ')

        stmt_build.append('ORDER BY rowid LIMIT ?')
        stmt = str(stmt_build)

        arglist = [self.last_fetched_deferred_record_rowid, deferred_id]
        if min_block_height is not None:
            arglist.append(min_block_height)
        arglist.append(FETCH_N_DEFERRED_RECORDS_IN_BATCH)

        caller = 'fetch_more_deferred_records_for_cache'
        deferred_records = self.fetch_query_and_handle_errors(stmt, arglist,
                                                              caller)

        if deferred_records is None:
            dprint("%s: Fetched 0 records." % caller)
            raise custom_errors.NoDeferredRecordsRemaining
        else:
            self.in_memory_deferred_record_cache.extend(deferred_records)
            last_row_id = deque_right_peek(
                self.in_memory_deferred_record_cache)['rowid']
            self.last_fetched_deferred_record_rowid = last_row_id
            dprint(("fetch_more_deferred_records_for_cache(): Fetched %d "
                    "records.") % len(self.in_memory_deferred_record_cache))

    def get_blame_record_obj_from_row(self, row, blame_label):
        address_reuse_role = AddressReuseRole(row['role'])
        data_source = DataSource(row['data_source'])
        row_id = row['rowid']
        tx_id = row['confirmed_tx_id']
        address_reuse_type = AddressReuseType(row['address_reuse_type'])
        relevant_address = row['relevant_address']
        block_height = row['block_height']

        blame_record = tx_blame.BlameRecord(blame_label, address_reuse_role,
                                      data_source, row_id, tx_id,
                                      address_reuse_type, relevant_address,
                                      block_height)
        return blame_record

    #Returns all deferred blame records as a list of BlameRecord objects at the
    #   specified block height. If there are none, return an empty list.
    #TODO: This is a tad long, consider breaking up into multiple functions
    def get_all_deferred_blame_records_at_height(self, block_height):
        assert isinstance(block_height, int)

        deferred_id = self.get_blame_id_for_deferred_blame_placeholder()
        if deferred_id is None:
            return []
        if not FETCH_DEFERRED_RECORDS_IN_BATCH:
            return self.get_blame_records_for_blame_id(deferred_id,
                                                       block_height)
        else:
            dprint(("get_all_deferred_blame_records_at_height(): Using mem "
                    "cache to efficiently grab deferred records @ height %d") %
                   block_height)
            #get records more efficiently in batches using a deque. First, cache
            #   all records we need for this block height from the DB, since
            #   that lookup is a time consuming operation.

            if len(self.in_memory_deferred_record_cache) == 0:
                #deque is empty, continue fetching records until we have all
                #   records at this block height.
                try:
                    self.fetch_more_deferred_records_for_cache(
                        deferred_id, min_block_height=block_height)
                except custom_errors.NoDeferredRecordsRemaining:
                    #If there ought to be more deferred records available in the
                    #   db, this should have been handled through a data
                    #   subscription relationship, so assume there are no
                    #   records at this block height to be fetched.
                    dprint(("get_all_deferred_blame_records_at_height: No more "
                           "records at height %d to fetch.") % block_height)
                    return []
            highest_height_in_cache = deque_right_peek(
                self.in_memory_deferred_record_cache)['block_height']
            dprint(("get_all_deferred_blame_records_at_height(): Highest "
                    "height in cache currently is %d") %
                   highest_height_in_cache)
            while highest_height_in_cache <= block_height:
                #some of the records needed for this block height are in the
                #   cache, but we can't say for sure that all of them are.
                #   continue extending.
                try:
                    self.fetch_more_deferred_records_for_cache(
                        deferred_id, min_block_height=block_height)
                    highest_height_in_cache = deque_right_peek(
                        self.in_memory_deferred_record_cache)['block_height']
                except custom_errors.NoDeferredRecordsRemaining:
                    #We've fetched all of the deferred records available in this
                    #   database. If there ought to be more, this should have
                    #   been handled through a data subscription relationship,
                    #   so assume for now that we've simply finished going
                    #   through all of the records we need
                    break

            #all records needed for this block height are now in the cache.
            #   Return them in the expected format.
            deferred_records = []
            while True:
                record = None
                try:
                    record = self.in_memory_deferred_record_cache.popleft()
                except IndexError:
                    #deque empty, done appending to deferred_records
                    dprint(("get_all_deferred_blame_records_at_height: "
                            "Returning %d records at height %d.") %
                            (len(deferred_records), block_height))
                    return deferred_records

                if record['block_height'] < block_height:
                    #This can happen if a block processor is working at one
                    #   height, fills the cache, and then needs to skip to a
                    #   higher, non-consecutive height in order to avoid bumping
                    #   into other workers. In this case, just let all of those
                    #   lower block height records that we skipped over just
                    #   pop right out and ignore them.
                    pass
                if record['block_height'] > block_height:
                    #done appending to deferred_records, push last item back on
                    self.in_memory_deferred_record_cache.appendleft(record)
                    dprint(("get_all_deferred_blame_records_at_height: "
                            "Returning %d records at height %d.") %
                            (len(deferred_records), block_height))
                    return deferred_records

                if record['block_height'] == block_height:
                    blame_label = DB_DEFERRED_BLAME_PLACEHOLDER
                    blame_record_obj = self.get_blame_record_obj_from_row(
                        record, blame_label)
                    deferred_records.append(blame_record_obj)

    #Return all blame records for the specified blame ID. Each record is
    #   encapsulated in a tx_blame.BlameRecord object. If there are no
    #   records matching the blame ID, returns an empty list.
    #arg0: blame_id: The blame ID in the database that we want to retrieve records for
    #arg1: block_height (optional): If specified, we will only retrieve records at
    #   this block height.
    def get_blame_records_for_blame_id(self, blame_id, block_height = None):
        assert isinstance(blame_id, int)

        if block_height is not None:
            assert isinstance(blame_id, int)

        blame_label = self.get_blame_label_for_blame_id(blame_id)
        if blame_label is None:
            msg = ("Could not find blame label to match blame id in "
                   "get_blame_records_for_blame_id(). ID was: %d") % blame_id
            logger.log_and_die(msg)

        stmt_build = string.StringBuilder()
        stmt_build.append('SELECT role, data_source, rowid, confirmed_tx_id, '
                          'address_reuse_type, relevant_address, block_height '
                          'FROM ' + SQL_TABLE_NAME_BLAME_STATS + ' WHERE '
                          'blame_recipient_id = ?')
        if block_height is not None:
            stmt_build.append(' AND block_height = ?')
        stmt = str(stmt_build)

        arglist = [blame_id]
        if block_height is not None:
            arglist.append(block_height)

        caller = 'get_blame_records_for_blame_id'
        records = self.fetch_query_and_handle_errors(stmt, arglist, caller)
        if records is None:
            return []

        blame_records = []
        for record in records:
            blame_record = self.get_blame_record_obj_from_row(record,
                                                              blame_label)
            blame_records.append(blame_record)
        return blame_records

    def get_blame_id_for_deferred_blame_placeholder(self):
        if self.deferred_blame_placeholder_rowid is not None:
            return self.deferred_blame_placeholder_rowid
        else:
            stmt = ('SELECT rowid FROM ' + SQL_TABLE_NAME_BLAME_IDS  + ' WHERE '
                    'label = ? LIMIT 1')
            arglist = (DB_DEFERRED_BLAME_PLACEHOLDER,)
            caller = 'get_blame_id_for_deferred_blame_placeholder'
            column_name = 'rowid'
            rowid = self.fetch_query_single_int(stmt, arglist, caller,
                                                column_name)
            self.deferred_blame_placeholder_rowid = rowid
            return rowid

    def get_update_blame_record_sql_statement(self):
        """Helper function for update_blame_record()."""

        #ideally this would end with LIMIT 1, but this is not supported by my
        #   version of SQLite. :<
        return ('UPDATE ' + SQL_TABLE_NAME_BLAME_STATS + ' SET '
                'blame_recipient_id = ?, '
                'address_reuse_type = ?, '
                'role = ?, '
                'data_source = ?, '
                'block_height = ?, '
                'confirmed_tx_id = ?, '
                'relevant_address = ? '
                'WHERE ROWID = ?')

    #Updates a single blame record (row) in the database, and also any
    #   corresponding entries in the "blame label" cache table. The latter must
    #   updated so that future encounters with the relevant address resolve to
    #   the correct (non-deferred) blame label.
    #param0: blame_record: The record to be updated as a BlameRecord object.
    #   Must contain rowid that of the row that will be updated, as well as all
    #   pertient information for that row. If the blamed party in the database
    #   before the update is a deferred blame, a new blame party will be
    #   added to the database and the deferred blame placeholder will be
    #   replaced.
    def update_blame_record(self, blame_record):
        assert blame_record.blame_label is not None

        #Determine the blame_recipient_id that is appropriate. If not already
        #   in the database, add it.
        blame_recipient_id = self.get_blame_id_for_label_and_insert_if_new(
            blame_record.blame_label)
        address_reuse_type = blame_record.address_reuse_type
        role = blame_record.address_reuse_role
        data_source = blame_record.data_source
        block_height = blame_record.block_height
        confirmed_tx_id = blame_record.tx_id
        relevant_address = blame_record.relevant_address
        rowid = blame_record.row_id

        arglist = (blame_recipient_id, address_reuse_type, role, data_source,
                   block_height, confirmed_tx_id, relevant_address, rowid)

        if UPDATE_BLAME_STATS_ONCE_PER_BLOCK:
            #caller responsible for calling
            #   write_deferred_blame_record_resolutions()
            self.in_memory_updated_blame_record_cache.append(arglist)
        else:
            #update database with only a single record (slower)
            stmt = self.get_update_blame_record_sql_statement()
            self.run_statement(stmt, arglist)

        if blame_record.blame_label != DB_DEFERRED_BLAME_PLACEHOLDER:
            #TODO: if this doesn't complete, perhaps we should rollback the
            #   previous UPDATE statement.
            self.update_blame_label_for_btc_address(relevant_address,
                                                    blame_record.blame_label)

    #Writes the changes that have been cached to memory that UPDATE or DELETE
    #   deferred blame records in the database.
    def write_deferred_blame_record_resolutions(self):
        stmt1 = self.get_update_blame_record_sql_statement()
        arglist = self.in_memory_updated_blame_record_cache
        if len(arglist) > 0:
            self.run_statement(stmt1, arglist, execute_many=True)
            self.in_memory_updated_blame_record_cache = []

        stmt2 = self.get_sql_blame_label_update_stmt()
        arglist = self.in_memory_update_blame_label_cache_cache
        if len(arglist) > 0:
            self.run_statement(stmt2, arglist, execute_many=True)
            self.in_memory_update_blame_label_cache_cache = []

        stmt3 = self.get_delete_blame_record_sql_stmt()
        arglist = self.in_memory_deleted_blame_record_cache
        if len(arglist) > 0:
            self.run_statement(stmt3, arglist, execute_many=True)
            self.in_memory_deleted_blame_record_cache = []

    def get_delete_blame_record_sql_stmt(self):
        #No support for a LIMIT clause for DELETE in my version of sqlite :<
        return 'DELETE FROM ' + SQL_TABLE_NAME_BLAME_STATS + ' WHERE rowid = ?'

    def delete_blame_record(self, row_id):
        assert isinstance(row_id, int)

        arglist = (row_id,)

        if DELETE_BLAME_STATS_ONCE_PER_BLOCK:
            self.in_memory_deleted_blame_record_cache.append(arglist)
        else:
            stmt = self.get_delete_blame_record_sql_stmt()
            self.run_statement(stmt, arglist)

    #In the event that something goes wrong while updating the database and
    #   we need to rollback partial results, specfiy the maximum block height
    #   at which to retain data. Beyond that height, cached data is deleted.
    def rollback_blame_stats_to_block_height(self, max_block_height):
        var_name = 'max_block_height'
        caller = 'rollback_blame_stats_to_block_height'
        validate.check_int_and_die(max_block_height, var_name, caller)

        stmt = ('DELETE FROM ' + SQL_TABLE_NAME_BLAME_STATS + ''
                ' WHERE block_height > ?')
        arglist = (max_block_height,)
        self.run_statement(stmt, arglist)

    ########################## BLAME CACHE FUNCTIONS ###########################

    #Get the wallet cluster label for the specified BTC address. If it's not
    #   cached, this will return None.
    def get_blame_label_for_btc_address(self, btc_address):
        validate.check_address_and_die(btc_address,
                                       'get_blame_label_for_btc_address')
        stmt = ('SELECT label FROM ' + SQL_TABLE_NAME_BLAME_LABEL_CACHE + ''
                ' WHERE btc_address = ? LIMIT 1')
        arglist = (btc_address,)
        caller = 'get_blame_label_for_btc_address'
        column_name = 'label'
        return self.fetch_query_single_str(stmt, arglist, caller, column_name)

    #Stores label for an address. If it has already been cached and is not a
    #   deferred blame, this will be updated. The update occurs so that, if we
    #   are trying to pre-fetch a label for multiple addresses in the same
    #   wallet, we can update them all in one batch.
    #param0: btc_address: The bitcoin address
    #param1: label: The string that presents the wallet that the address
    #   belongs to. It wil be HTML encoded before being stored.
    #Returns: Whether or not the address and label were cached. Will not cache
    #   if address is malformed.
    def cache_blame_label_for_btc_address(self, btc_address, label):
        if validate.looks_like_address(btc_address):

            label_escaped = html_escape(label)
            col_names = get_comma_separated_list_of_col_names(
                SQL_SCHEMA_BLAME_LABEL_CACHE)
            stmt_build = string.StringBuilder()
            stmt_build.append('INSERT OR ')
            if label == DB_DEFERRED_BLAME_PLACEHOLDER:
                #I'm not sure if this would ever happen, but it woudl be a
                #   resolved blame record with a deferred one, so prevent that.
                stmt_build.append('IGNORE ')
            else:
                stmt_build.append('REPLACE ')
            stmt_build.append(('INTO ' + SQL_TABLE_NAME_BLAME_LABEL_CACHE + '('
                               '' + col_names + ') VALUES (?,?)'))
            stmt = str(stmt_build)
            arglist = (btc_address, label_escaped)
            self.run_statement(stmt, arglist)
            #TODO: return value should be based on return val of run_statement
            return True
        else:
            return False

    def get_sql_blame_label_update_stmt(self):
        return ('UPDATE ' + SQL_TABLE_NAME_BLAME_LABEL_CACHE + ' SET label = ? '
                'WHERE btc_address = ?')

    #Updates label for an address. For example, if the label was set to a
    #   placeholder while deferring setting it a particular label earlier, we
    #   can now update it.
    def update_blame_label_for_btc_address(self, btc_address, label):
        caller = 'update_blame_label_for_btc_address'
        validate.check_address_and_die(btc_address, caller)

        arglist = (label, btc_address)

        label_escaped = html_escape(label)
        if UPDATE_BLAME_STATS_ONCE_PER_BLOCK:
            self.in_memory_update_blame_label_cache_cache.append(arglist)
        else:
            stmt = self.get_sql_blame_label_update_stmt()
            self.run_statement(stmt, arglist)

    ###################### SEEN ADDRESSES CACHE FUNCTIONS ######################

    def has_address_been_seen_cache_if_not(self,
                                           btc_address,
                                           block_height_first_seen = None):
        validate.check_address_and_die(btc_address,
                                       'get_blame_label_for_btc_address')
        if block_height_first_seen is not None:
            validate.check_int_and_die(block_height_first_seen,
                                       'block_height_first_seen',
                                       'has_address_been_seen_cache_if_not')

        #http://stackoverflow.com/questions/9755860/valid-query-to-check-if-row-exists-in-sqlite3
        stmt = ('SELECT EXISTS(SELECT 1 FROM '
                '' + SQL_TABLE_NAME_ADDRESSES_SEEN + ''
                ' WHERE address=? LIMIT 1) AS is_first')
        arglist = (btc_address,)
        caller = 'has_address_been_seen_cache_if_not'
        column_name = 'is_first'
        result = self.fetch_query_single_int(stmt, arglist, caller, column_name)
        dprint("result: %s" % str(result))
        if result == 0:
            if block_height_first_seen is None:
                stmt = ('INSERT INTO ' + SQL_TABLE_NAME_ADDRESSES_SEEN + ''
                        '(address) VALUES (?)')
                arglist = (btc_address,)
            else:
                stmt = ('INSERT INTO ' + SQL_TABLE_NAME_ADDRESSES_SEEN + ''
                        ' (block_height_first_seen, address) VALUES (?,?)')
                arglist = (block_height_first_seen, btc_address,)
            self.run_statement(stmt, arglist)
            return False
        else:
            return True

    #In the event that something goes wrong while updating the database and
    #   we need to rollback partial results, specfiy the maximum block height
    #   at which to retain data. Beyond that height, cached data is deleted.
    def rollback_seen_addresses_cache_to_block_height(self, max_block_height):
        var_name = 'max_block_height'
        caller = 'rollback_seen_addresses_cache_to_block_height'
        validate.check_int_and_die(max_block_height, var_name, caller)

        stmt = ('DELETE FROM ' + SQL_TABLE_NAME_ADDRESSES_SEEN + ''
                ' WHERE block_height_first_seen > ?')
        arglist = (max_block_height,)
        self.run_statement(stmt, arglist)

    ######################## RELAYED-BY CACHE FUNCTIONS ########################

    #retrieves the relayed-by field for the specified transaction.
    def get_cached_relayed_by(self, tx_id):

        stmt = ('SELECT relayed_by FROM ' + SQL_TABLE_NAME_RELAYED_BY_CACHE + ''
                ' WHERE tx_id = ?')
        arglist = (tx_id,)
        caller = 'get_cached_relayed_by'
        column_name = 'relayed_by'
        relayed_by = self.fetch_query_single_str(stmt, arglist, caller,
                                                 column_name)
        if relayed_by is None:
            return None
        else:
            return relayed_by

    def record_relayed_by(self, tx_id, block_height, relayed_by):
        col_names = get_comma_separated_list_of_col_names(
            SQL_SCHEMA_RELAYED_BY_CACHE)
        stmt = ('INSERT INTO ' + SQL_TABLE_NAME_RELAYED_BY_CACHE + ''
                '(' + col_names + ') VALUES (?,?,?)')
        arglist = (block_height, tx_id, relayed_by)
        self.run_statement(stmt, arglist)

    #In the event that something goes wrong while updating the database and
    #   we need to rollback partial results, specfiy the maximum block height
    #   at which to retain data. Beyond that height, cached data is deleted.
    def rollback_relayed_by_cache_to_block_height(self, max_block_height):
        var_name = 'max_block_height'
        caller = 'rollback_relayed_by_cache_to_block_height'
        validate.check_int_and_die(max_block_height, var_name, caller)

        stmt = ('DELETE FROM ' + SQL_TABLE_NAME_RELAYED_BY_CACHE + ''
                ' WHERE block_height > ?')
        arglist = (max_block_height,)
        self.run_statement(stmt, arglist)

    #Returns the highest block height in the cache. Returns None if information
    #   for no transactions has been cached.
    def get_highest_relayed_by_height(self):
        stmt = ('SELECT MAX(block_height) AS max_block_height FROM '
                '' + SQL_TABLE_NAME_RELAYED_BY_CACHE + '')
        arglist = []
        caller = 'get_highest_relayed_by_height'
        column_name = 'max_block_height'
        highest = self.fetch_query_single_int(stmt, arglist, caller,
                                              column_name)
        if highest is None:
            return None
        else:
            return highest

    ######################### TX OUTPUT CACHE FUNCTIONS ########################

    #Cache address in memory
    def add_output_address_to_mem_cache(self, block_height, tx_id, output_pos,
                                        address):
        tup = (block_height, tx_id, output_pos, address)
        self.in_memory_tx_output_cache.append(tup)

    #Write cache to disk and clear memory
    def write_stored_output_addresses(self):
        stmt = ('INSERT OR IGNORE INTO ' + SQL_TABLE_NAME_TX_OUTPUT_CACHE + ' '
                '(block_height, tx_id, output_pos, address) VALUES (?,?,?,?)')
        self.run_statement(stmt, self.in_memory_tx_output_cache,
                           execute_many = True)
        self.in_memory_tx_output_cache = []

    #Queries from DB cache the output address for specified tx and output. If
    #   not present in DB, returns None
    def get_output_address(self, tx_id, output_pos):
        stmt = ('SELECT address FROM ' + SQL_TABLE_NAME_TX_OUTPUT_CACHE + ' '
                'WHERE tx_id = ? AND output_pos = ? LIMIT 1')
        arglist = (tx_id, output_pos)
        caller = 'get_output_address'
        column_name = 'address'
        addr = self.fetch_query_single_str(stmt, arglist, caller, column_name)
        return addr

    #Returns the highest block height in the cache. Returns None if information
    #   for no transactions has been cached.
    def get_highest_output_address_cached_height(self):
        stmt = ('SELECT MAX(block_height) AS max_block_height FROM '
                '' + SQL_TABLE_NAME_TX_OUTPUT_CACHE + '')
        arglist = []
        caller = 'get_highest_output_address_cached_height'
        column_name = 'max_block_height'
        highest = self.fetch_query_single_int(stmt, arglist, caller,
                                              column_name)
        if highest is None:
            return None
        else:
            return highest

    ##################### BLOCK DATA PRODUCTION FUNCTIONS ######################

    #Returns the highest block height at which the specified block producer has
    #   advertised complete data for. If not present, returns None.
    def get_top_block_height_available(self, block_producer):
        assert isinstance(block_producer, data_subscription.DataProducer)
        stmt = ('SELECT top_block_height_available FROM '
                '' + SQL_TABLE_NAME_BLOCK_DATA_PRODUCTION_STATUS + ' WHERE '
                'producer_id = ?')
        arglist = (block_producer,)
        caller = 'get_top_block_height_available'
        column_name = 'top_block_height_available'
        return self.fetch_query_single_int(stmt, arglist, caller, column_name)

    def set_top_block_height_available(self, block_producer, block_height):
        assert isinstance(block_producer, data_subscription.DataProducer)
        var_name = 'block_height'
        caller = 'set_top_block_height_available'
        validate.check_int_and_die(block_height, var_name, caller)

        stmt = ('INSERT OR REPLACE INTO '
                '' + SQL_TABLE_NAME_BLOCK_DATA_PRODUCTION_STATUS + ' '
                '(producer_id, top_block_height_available) VALUES (?,?)')
        arglist =  (block_producer, block_height)
        self.run_statement(stmt, arglist)

    def increment_top_block_height_available(self, block_producer):
        assert isinstance(block_producer, data_subscription.DataProducer)

        current_height = self.get_top_block_height_available(block_producer)
        if current_height is None:
            #incrementing for the first time means we just processed the genesis
            #   block.
            self.set_top_block_height_available(block_producer, 0)
        else:
            stmt = ('UPDATE ' + SQL_TABLE_NAME_BLOCK_DATA_PRODUCTION_STATUS + ''
                    ' SET top_block_height_available = '
                    'top_block_height_available + 1  WHERE producer_id = ?')
            arglist = (block_producer,)
            self.run_statement(stmt, arglist)

#End Database class

#Creates and deletes SQL indices on demand.
class IndexManager:
    #TODO
    None

    #This is just an example, we don't actually need an index for this since one
    #   is automatically created due to UNIQUE constraint
    #CREATE UNIQUE INDEX indTxOutputCache ON tblTxOutputCache (tx_id, output_pos, address);

    #Covering index for query blame records
    #CREATE INDEX indBlameStats ON tblBlameStats (blame_recipient_id, address_reuse_type, role, data_source, block_height, confirmed_tx_id, relevant_address);

    #Covering index for querying cached blame labels
    #CREATE INDEX indBlameLabelCache ON tblBlameLabelCache (btc_address, label)

#############################
# GENERAL PACKAGE FUNCTIONS #
#############################

def get_conditional_create_stmt(table_name, schema_as_dict):
    stmt = 'CREATE TABLE IF NOT EXISTS %s (' % table_name
    for varname, datatype in schema_as_dict.iteritems():
        stmt = stmt + "%s %s," % (varname, datatype)
    stmt = stmt.rstrip(',') #remove trailing comma
    stmt = stmt + ');'
    return stmt

def get_comma_separated_list_of_col_names(schema_as_dict):
    col_names = ''
    for varname, datatype in schema_as_dict.iteritems():
        col_names = col_names + varname + ","
    col_names = col_names.rstrip(',') #remove trailing comma
    return col_names

#From: https://wiki.python.org/moin/EscapingHtml
def html_escape(text):
    return ''.join(HTML_ESCAPE_TABLE.get(c, c) for c in text)

def dprint(str):
    if ENABLE_DEBUG_PRINT:
        print("DEBUG: %s" % str)

def deque_right_peek(deq):
    return deq[-1]

def get_up_to_n_items(a_list, start_index, n):
    return a_list[start_index:start_index + n]
