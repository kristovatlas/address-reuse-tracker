"""Process blocks in the blockchain in various ways to identify address reuse.
"""

#http://stackoverflow.com/questions/1267869/how-can-i-force-division-to-be-floating-point-in-python
from __future__ import division # make division of two ints return float
#SyntaxError: from __future__ imports must occur at the beginning of the file

# pylint: disable=bad-whitespace

####################
# EXTERNAL IMPORTS #
####################

#get name of this script for `check_int_and_die` using `os.path.basename`
import os

####################
# INTERNAL IMPORTS #
####################

import logger
import validate
import db
import tx_blame
import block_state
import blockchain_reader

#############
# CONSTANTS #
#############

ENABLE_DEBUG_PRINT = True

THIS_FILE = os.path.basename(__file__)
#This is the approximate height at which Blockchain.info started collecting
#   'Relayed By' information for transactions. In order to speed things up for
#   that many blocks, set the next flag to True
SKIP_CLIENT_LOOKUP_BEFORE_BLOCK_HEIGHT = 168085
DO_SKIP_CLIENT_LOOKUP_BELOW_FIRST_BLOCK = False

#See: e.g. https://www.blocktrail.com/BTC/tx/e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468
WEIRD_TXS_TO_SKIP_FOR_RELAYED_BY_CACHING = {
    #tx hash => block height
    #Below 2 txs are duplicates, later rendered impossible by BIP30.
    'd5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599': 91842,
    'e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468': 91880
}

############################################
# GLOBAL CONSTANTS TO SHORTEN DOT NOTATION #
############################################

CLIENT = db.AddressReuseRole.CLIENT
RECEIVER = db.AddressReuseRole.RECEIVER
SENDBACK = db.AddressReuseType.SENDBACK
TX_HISTORY = db.AddressReuseType.TX_HISTORY

#TODO: Split the various kinds of block processing functions into subclasses
#   of BlockProcessor.
class BlockProcessor(object):
    """Processes a block in the blockchain.

    Args:
        block_reader (`blockchain_reader.BlockExplorerReader`): An object that
            answers queries about the state of the blockchain.
        database_connector (Optional[`db.Database`]): Manages connection to
            database to make queries and store updates. If no existing
            connection is specified, a new one will be created using the
            defaults specified by the `Database` class.

    Attributes:
        block_reader (`blockchain_reader.BlockExplorerReader`): An object that
            answers queries about the state of the blockchain.
        database (`db.Database`): Manages connection to database to make queries
            and store updates.
        blamer (`tx_blame.Blamer`): Assigns blame for address reuse to
            particular entitties.
    """

    def __init__(self, block_reader, database_connector = None):
        assert isinstance(block_reader, blockchain_reader.BlockExplorerReader)
        assert database_connector is None or isinstance(database_connector,
                                                        db.Database)

        self.block_reader = block_reader

        if database_connector is None:
            self.database = db.Database()
        else:
            self.database = database_connector
        self.blamer = tx_blame.Blamer(self.database)

    def process_block(self, block_height, benchmarker = None,
                      defer_blaming = False,
                      use_tx_out_addr_cache_only = False):
        """A jack-of-all-trades function that does all address reuse processing.

        To process a block we must:
            * fetch a list of txs
            * foreach tx, go through lists of input and output addresses
            * observe reuse between inputs and outputs
            * foreach address, get tx history and determine whether the address
                has a history prior to this tx
        Note about malleability: Since we are only concerned with confirmed
        transactions, tx id malleability should be an unusual case for orphaned
        blocks or non-SIGHASH_ALL locking scripts. TODO: deal with these?

        Information stored in db about the block:
            * total number of transactions
            * nuber of transactions with send-back reuse
            * number of transactions with outputs that have prior tx history
            * compute percentage of transactions with send-back reuse
            * compute percentage of transactions with prior tx history outputs

        Args:
            block_height (int): The height of the block to be processed.
            benchmarker (Optional[`block_reader_benchmark.Benchmark`]):
                Evaluates the speed of this function's various tasks.
            defer_blaming (Optional[bool]): A flag that decides whether to defer
                attributing instances of address reuse to a particular party
                such as a wallet client or address cluster. We may want to do
                this when processing the blockchain locally so that one thread
                can focus on parsing the blockchain, and another can focus on
                remote API lookups. Default: False
            use_tx_out_addr_cache_only (Optional[bool]): When looking up
                addresses for previous transactions, this flag instructs the
                function to ONLY refer to the cache in our SQLite database,
                rather than slower option of using RPC interface. If set to
                True, the process will sleep until the data is available in the
                cache. Default: False.
        """

        assert isinstance(block_height, int)

        current_block_state = block_state.BlockState(block_height) # block stats collector

        tx_list = self.block_reader.get_tx_list(block_height,
                                                use_tx_out_addr_cache_only)

        for tx in tx_list:
            self.process_tx(tx, current_block_state, block_height, benchmarker,
                            defer_blaming)

        #Per requirements of db.store_blame(), call write_stored_blame() to
        #   write the records cached in Python memory to the database as a
        #   block-sized batch
        if db.INSERT_BLAME_STATS_ONCE_PER_BLOCK:
            self.database.write_stored_blame()
            dprint("Committed stored blame stats to db.")

        if benchmarker is not None:
            benchmarker.increment_blocks_processed()

        current_block_state.update_sendback_reuse_pct()
        current_block_state.update_receiver_histoy_pct()
        self.database.record_block_stats(current_block_state)

    #TODO: http://pylint-messages.wikidot.com/messages:r0201
    def _get_input_address_list(self, tx_obj):
        """Helper for `process_tx`, gets input addr list from tx JSON."""

        input_address_list = []
        try:
            for btc_input in tx_obj['inputs']:
                if 'prev_out' in btc_input and 'addr' in btc_input['prev_out']:
                    input_address_list.append(btc_input['prev_out']['addr'])
            return input_address_list
        except KeyError as err:
            logger.log_and_die("Missing element in tx_obj: '%s" % str(err))

    #This function is called to fill in blame statistics for address reuse
    #   after data from the blockchain has already been processed. This is
    #   necessary, for example, if the blockchain analysis was based on a local
    #   copy of the blockchain but all blame data is accessible only via remote
    #   APIs.
    def process_block_after_deferred_blaming(self, block_height,
                                             benchmarker=None):
        """Fetches address reuse records with deferred blame and resolves blame.

        This function is called to fill in blame statistics for address reuse
        after data from the blockchain has already been processed. This is
        necessary, for example, if the blockchain analysis was based on a local
        copy of the blockchain but all blame data is accessible only via remote
        APIs. Each record is processed according to one of three rules:
        1. If the `blame_role` is `CLIENT`, obtain the wallet client used from a
            local cache of a remote API call or the remote API call if not yet
            cached. If it cannot be obtained, delete the record. Otherwise,
            update it.
        2. If the `blame_role` is `SENDER` and `reuse_type` is `SENDBACK`,
            delete the record as redundant for the `RECEIVER` record.
        3. If rule 2 doesn't apply and the `blame_role` is `SENDER` or
            `RECEIVER`, use the `update_blame_record` function to update the
            `BlameRecord` information. Update the record in the database with
            the new information.
        """

        blame_records = self.database.get_all_deferred_blame_records_at_height(
            block_height)

        dprint("Retrieved %d deferred blame records from db @ height %d" %
               (len(blame_records), block_height))

        for blame_record in blame_records:
            if blame_record.address_reuse_role == CLIENT:
                self.process_deferred_client_blame_record(blame_record)

            if (blame_record.address_reuse_type == SENDBACK
                    and blame_record.address_reuse_role == RECEIVER):
                self.delete_deferred_sendback_receiver_record(blame_record)

            else:
                blame_record.blame_label = self.blamer.get_single_wallet_label(
                    blame_record.relevant_address)
                dprint(("Attempting to update record with new blame label %s") %
                       blame_record.blame_label)
                self.database.update_blame_record(blame_record)

        if db.UPDATE_BLAME_STATS_ONCE_PER_BLOCK:
            self.database.write_deferred_blame_record_resolutions()

        if benchmarker is not None:
            benchmarker.increment_blocks_processed()

    def process_deferred_client_blame_record(self, blame_record):
        """Determine wallet client or delete the record.

        Uses previously cached or new remote API query to obtain the wallet
        client used. If it cannot be obtained, deletes the record. Otherwise,
        updates it.

        If in-memory caching per-block is enlabed in the `db` module, the caller
        of this function should later manually commit these update/deletes after
        the whole block has been processed.

        Args:
            blame_record (`tx_blame.BlameRecord`): The record that has no
                resolved wallet client name yet.
        """

        assert isinstance(blame_record, tx_blame.BlameRecord)
        assert blame_record.address_reuse_role == CLIENT

        dprint("Processing record: " + str(blame_record))

        client_record = None
        if (DO_SKIP_CLIENT_LOOKUP_BELOW_FIRST_BLOCK and
                blame_record.block_height <
                SKIP_CLIENT_LOOKUP_BEFORE_BLOCK_HEIGHT):
            #don't bother looking up client info
            pass
        else:
            client_record = self.blamer.get_wallet_client_blame_record(
                blame_record.tx_id)
        if client_record is None:
            dprint("No client information, must delete this record.")
            self.database.delete_blame_record(blame_record.row_id)
        else:
            client_label = client_record.blame_label
            dprint("Will update record with client information " + client_label)
            blame_record.blame_label = client_label
            self.database.update_blame_record(blame_record)

    def delete_deferred_sendback_receiver_record(self, blame_record):
        """Deletes the record of send-back address reuse as a duplicate."""

        assert isinstance(blame_record, tx_blame.BlameRecord)
        assert blame_record.address_reuse_role is RECEIVER
        assert blame_record.address_reuse_type is SENDBACK

        self.database.delete_blame_record(blame_record.row_id)

    def cache_relayed_by_fields_for_block_only(self, block_height,
                                               benchmarker=None):
        """Cache 'relayed by' field in db for all txs in the block."""

        tx_list = self.block_reader.get_tx_list(block_height)
        for tx_obj in tx_list:
            tx_id = tx_obj['hash']
            try:
                relayed_by = tx_obj['relayed_by']
            except KeyError as err: #When fetching locally, it doesn't have this field
				relayed_by = ''
				pass
            if (tx_id in WEIRD_TXS_TO_SKIP_FOR_RELAYED_BY_CACHING and
                    WEIRD_TXS_TO_SKIP_FOR_RELAYED_BY_CACHING[tx_id] ==
                    block_height):
                pass
            else:
                self.database.record_relayed_by(tx_id, block_height, relayed_by)
            if benchmarker is not None:
                benchmarker.increment_transactions_processed()
        if benchmarker is not None:
            benchmarker.increment_blocks_processed()

    def cache_tx_output_addresses_for_block_only(self, block_height,
                                                 benchmarker = None):
        """Cache output addresses for all txs in the block.

        This informations required at a later stage when resolving input
        addresses when utilizing a local RPC blockchain reader instead of a
        block explorer remote API.
        """

        tx_id_list = self.block_reader.get_tx_ids_at_height(block_height)
        for tx_id in tx_id_list:
            rpc_style_tx_json = self.block_reader.get_decoded_tx(tx_id)
            address_list = self.block_reader.get_output_addresses(
                rpc_style_tx_json)
            for output_pos in range(0, len(address_list)):
                address = address_list[output_pos]
                self.database.add_output_address_to_mem_cache(
                    block_height, tx_id, output_pos, address)
            if benchmarker is not None:
                benchmarker.increment_transactions_processed()

        self.database.write_stored_output_addresses() #write db file per block
        if benchmarker is not None:
            benchmarker.increment_blocks_processed()

    #TODO: This function is too long and indented, break into smaller pieces
    def process_tx(self, tx_obj, current_block_state, block_height,
                   benchmarker=None, defer_blaming=False):
        """Finds address reuse in speicfied tx and stores records in db.

        Args:
            tx_obj (tuple): A transaction decoded from the JSON output of the
                block reader.
            current_block_state (`block_state.BlockState): The state that needs
                to be updated destructively.
            benchmarker (Optional[`block_reader_benchmark.Benchmark`]):
                Evaluates the speed of this function's various tasks.
            defer_blaming (Optional[bool]): A flag that decides whether to defer
                attributing instances of address reuse to a particular party
                such as a wallet client or address cluster. We may want to do
                this when processing the blockchain locally so that one thread
                can focus on parsing the blockchain, and another can focus on
                remote API lookups. Default: False
        """
        current_block_state.incr_total_tx_num()
        tx_contains_sendback_reuse = False
        tx_contains_receiver_with_history = False

        tx_id = tx_obj['hash']
        dprint("tx_id = %s" % tx_id)

        #Compile a list of input addresses that various callees will need
        input_address_list = self._get_input_address_list(tx_obj)

        #Look through inputs to see if it matches any addresses in outputs
        for btc_input in tx_obj['inputs']:
            #if this input has an address, see if it's also in the outputs
            if 'prev_out' in btc_input and 'addr' in btc_input['prev_out']:
                assert not isinstance(btc_input['prev_out'], list)
                input_addr = btc_input['prev_out']['addr']

                for btc_output in tx_obj['out']:
                    if 'addr' in btc_output:
                        output_addr = btc_output['addr']

                        #now let's see if the input we're iterating on matches
                        #   an output address
                        if input_addr == output_addr:
                            #Found an instance of send-back address reuse. Find
                            #   parties to blame and store that in the db
                            blame_records = self.blamer.get_wallet_blame_list(
                                tx_id, input_address_list, input_addr,
                                benchmarker, defer_blaming)
                            for blame_record in blame_records:
                                self.database.store_blame(
                                    blame_record.blame_label,
                                    SENDBACK,
                                    blame_record.address_reuse_role,
                                    blame_record.data_source,
                                    current_block_state.block_num,
                                    tx_id,
                                    input_addr)

                            if not tx_contains_sendback_reuse:
                                tx_contains_sendback_reuse = True
                                # count only once per tx
                                current_block_state.incr_sendback_reuse()

        #Look through outputs to see if any of them have a tx history PRIOR to
        #   this tx
        for btc_output in tx_obj['out']:
            if 'addr' in btc_output:
                output_addr = btc_output['addr']
                if self._does_output_have_prior_tx_history(
                        output_addr, tx_id, block_height, benchmarker):
                    #Found an instance of send-back address reuse. Find parties
                    #   to blame and store that in the db
                    blame_records = self.blamer.get_wallet_blame_list(
                        tx_id, input_address_list, output_addr, benchmarker,
                        defer_blaming)
                    for blame_record in blame_records:
                        self.database.store_blame(
                            blame_record.blame_label, TX_HISTORY,
                            blame_record.address_reuse_role,
                            blame_record.data_source,
                            current_block_state.block_num, tx_id, output_addr)

                    if not tx_contains_receiver_with_history:
                        tx_contains_receiver_with_history = True
                        # count only once per tx
                        current_block_state.incr_receiver_tx_history_reuse()

        #Done looking through inputs and outputs for this tx
        dprint("Completed processing tx '%s'" % tx_id)
        if benchmarker is not None:
            benchmarker.increment_transactions_processed()

    def _does_output_have_prior_tx_history(self, addr, current_tx_id,
                                           block_height, benchmarker=None):
        """#Helper function for `process_tx`."""

        dprint("Address to be validated: %s" % addr)
        validate.check_address_and_die(addr, THIS_FILE)
        if self.block_reader.is_first_transaction_for_address(
                addr, current_tx_id, block_height, benchmarker):
            return False
        else:
            return True

####################
# MODULE FUNCTIONS #
####################

def dprint(msg):
    """Prints a message when debugging mode is enabled."""
    if ENABLE_DEBUG_PRINT:
        print "DEBUG: %s" % msg
