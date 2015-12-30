"""Attributes address reuse blame to a wallet or service.

Blame is determined as follows:
    1. Was the tx first relayed by Blockchain.info? If so, BCI wallet or API was
        likely used.
    2. Is there a distinct label for the sender according to WalletExplorer.com?
        If so, they get blamed
    3. Is there a distinct label for the receiver according to WE.com? If so,
        they get blamed.
There can be multiple parties blamed for address reuse in a given transaction:
sender, reciever, and wallet client.
"""

import blockchain_reader
import db

DB_DEFERRED_BLAME_PLACEHOLDER = 'DB_DEFERRED_BLAME_PLACEHOLDER'

class BlameRecord(object):
    """Records how address reuse was attributed to a specific party.

    Optional fields can be set in constructor to help identify a paticular db
    record or external lookup identifier in case the record needs to be updated.

    Args:
        blame_label (str): The label that uniquely identifies this blamed party.
        address_reuse_role (`db.AddressReuseRole`): Role of party in reuse.
        data_source (`db.DataSource`): Source of data used to blame party.
        row_id (Optional[int]): Pointer to row in database.
        tx_id (Optional[str]): The ID of the transaction in which an address was
            reused.
        address_reuse_type (Optional[`db.AddressReuseType`]): Type of address
            reuse.
        relevant_address (Optional[str]): The reused address.
        block_height (Optional[int]): Height of block in which address was
            reused.

    Attributes:
        blame_label (str): The label that uniquely identifies this blamed party.
        address_reuse_role (`db.AddressReuseRole`): Role of party in reuse.
        data_source (`db.DataSource`): Source of data used to blame party.
        row_id (int): Pointer to row in database.
        address_reuse_type (`db.AddressReuseType`): Type of address reuse.
        relevant_address (str): The reused address.
        block_height (int): Height of block in which address was reused.
    """

    def __init__(self, blame_label, address_reuse_role, data_source,
                 row_id=None, tx_id=None, address_reuse_type=None,
                 relevant_address=None, block_height=None):
        assert isinstance(address_reuse_role, db.AddressReuseRole)
        assert isinstance(data_source, db.DataSource)
        self.blame_label = blame_label
        self.address_reuse_role = address_reuse_role
        self.data_source = data_source
        self.row_id = row_id
        self.tx_id = tx_id
        self.address_reuse_type = address_reuse_type
        self.relevant_address = relevant_address
        self.block_height = block_height

    def __str__(self):
        """Print object for debugging."""
        return (('blame_label %s address_reuse_role %s data_source %s row_id %s'
                 ' tx_id %s address_reuse_type %s relevant_address %s '
                 'block_height %s') %
                (str(self.blame_label), str(self.address_reuse_role),
                 str(self.data_source), str(self.row_id), str(self.tx_id),
                 str(self.address_reuse_type), str(self.relevant_address),
                 str(self.block_height)))

    def __repr__(self):
        return self.__str__()

class Blamer(object):
    """Contains functions for determining blame of address reuse.

    These functions are encapsulated in an object so that a single database
    connection can be used.

    Args:
        database_connector (Optional[`db.Database`]): Manages connection to
            database to make queries and store updates. If not specified, a new
            connection is created using the defaults of the `Database` class.

    Attributes:
        bci_blockchain_reader (`blockchain_reader.ThrottledBlockchainReader`):
            Connector to the remote Blockchain.info blockchain data API.
        walletexplorer_reader (`blockchain_reader.WalletExplorerReader`):
            Connector to the remote WalletExplorer.com API for address
            clustering.
        database_connector (`db.Database`): Manages connection to database to
            make queries and store updates.
    """

    def __init__(self, database_connector=None):
        if database_connector is None:
            self.database_connector = db.Database() #Create new db conn
        else:
            self.database_connector = database_connector #Use existing db conn

        self.blockchain_reader = blockchain_reader.ThrottledBlockchainReader(
            self.database_connector)

        self.walletexplorer_reader = blockchain_reader.WalletExplorerReader(
            self.database_connector)

    def _get_wallet_client_by_tx_id(self, tx_id, benchmarker=None):
        """Look up wallet client or API that created the tx using tx id.

        Returns:
            str or None: Either returns the wallet client or None if it could
            not be determined.
        """

        relayed_by = self.blockchain_reader.get_tx_relayed_by_using_tx_id(
            tx_id, benchmarker=benchmarker)
        relayed_by = relayed_by.strip() #remove trailing or leaidng whitespace

        if relayed_by == '127.0.0.1' or relayed_by == 'Blockchain.info':
            return 'Blockchain.info'
        else:
            return None

    def get_wallet_client_blame_record(self, tx_id, defer_blaming=False,
                                       benchmarker=None):
        """Look up the wallet client or API that created the tx using tx id.

        Args:
            tx_id (str): The ID of the transaction in which an address was
                reused.
            defer_blaming (Optional[bool]): Disabled by default. If set to
                `True`, a placeholder will be used instead of looking up
                the actual client name.
            benchmarker (Optional[`block_reader_benchmark.Benchmark`]): Measures
                the speed of this function.

        Returns:
            `BlameRecord`: A record containing the name of the client used to
                create the transaction containing adddress reuse. If the
                `defer_blaming` argument is set to `True`, a placeholder
                value will be set instead of the actual client name.
        """
        CLIENT = db.AddressReuseRole.CLIENT
        BCI = db.DataSource.BLOCKCHAIN_INFO

        if defer_blaming:
            blame_record = BlameRecord(
                DB_DEFERRED_BLAME_PLACEHOLDER, CLIENT, BCI)
            return blame_record
        else:
            client = self._get_wallet_client_by_tx_id(
                tx_id, benchmarker=benchmarker)
            if client is None:
                return None
            else:
                blame_record = BlameRecord(client, CLIENT, BCI)
                return blame_record

    def get_wallet_blame_list(self, tx_id, input_address_list, address,
                              benchmarker=None, defer_blaming=False):
        """For an address-reusing tx, get a blame record for each input addr.

        Since WalletExplorer.com is currently the sole source of clustering
        analysis and it treats all inputs as belonging to the same wallet
        cluster, this could be condensed into returning one blame record.
        However, in principle, you could have a different blamed party for
        each input address.

        Since address reuse stats are grouped by distinct transaction ID
        in `db.Database.get_num_records_across_block_span`, it's harmless to
        create multiple blame records for the same transaction.

        Args:
            tx_id (str): Transaction hash id for transaction involving address
                reuse.
            input_address_list (List[str]): The Bitcoin addresses that are
                inputs to the transaction involving address reuse.
            address (str): The reused address in the transaction.
            benchmarker (Optional[`block_reader_benchmark.Benchmark`]): Measures
                the speed of this function.
            defer_blaming (Optional[bool]): Specifies whether the blamed party
                should be resolved now through remote API lookups (Default), or
                can be resolved later by another processing function.
        Returns:
            List[`BlameRecord`]: One blame record object for each input address
                for the transaction specified.
        """

        blame_list = []
        blame_client_record = self.get_wallet_client_blame_record(
            tx_id, defer_blaming, benchmarker=benchmarker)
        if blame_client_record is not None and blame_client_record:
            blame_list.append(blame_client_record)

        clustered_blame_records = self.get_wallet_label_records(
            tx_id, input_address_list, address, benchmarker, defer_blaming)
        if (clustered_blame_records is not None and
                len(clustered_blame_records) > 0):
            blame_list.extend(clustered_blame_records)

        return blame_list

    def get_wallet_label_records(self, tx_id, input_address_list, address,
                                 benchmarker=None, defer_blaming=False):
        """For the specified tx ID, get a list of addr reuse blame records.

        Each record contains a label for the blamed party, and specifies whether
        the party was a sender or receiver of funds.

        Args:
            tx_id (str): Transaction hash id for transaction involving address
                reuse.
            input_address_list (List[str]): The Bitcoin addresses that are
                inputs to the transaction involving address reuse.
            address (str): The reused address in the transaction.
            benchmarker (Optional[`block_reader_benchmark.Benchmark`]): Measures
                the speed of this function.
            defer_blaming (Optional[bool]): Specifies whether the blamed party
                should be resolved now through remote API lookups (Default), or
                can be resolved later by another processing function.
        Returns:
            List[`BlameRecord`]: Blame record objects for address reuse in
                this transaction.
        """

        return self.walletexplorer_reader.get_wallet_labels(
            tx_id, input_address_list, address, benchmarker, defer_blaming)

    def get_single_wallet_label(self, addr):
        """Get the wallet label for a single Bitcoin address from remote API."""

        return self.walletexplorer_reader.get_wallet_label_for_single_address(
            addr)
