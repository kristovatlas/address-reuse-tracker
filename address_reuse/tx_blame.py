#Given a particular transaction, this code will attempt to attribute it to a 
#   particular wallet client or a service, e.g. through a distinct client 
#   fingerprint or address clustering

#How to determine blame:
#   1. Was the tx first relayed by Blockchain.info? If so, BCI wallet or API was 
#       likely used.
#   2. Is there a distinct label for the sender according to WalletExplorer.com? 
#       If so, they get blamed
#   3. Is there a distinct label for the receiver according to WE.com? If so, 
#       they get blamed.
# There can be multiple parties blamed for address reuse in a given transaction 
#   (both sender and receiver).

####################
# INTERNAL IMPORTS #
####################

import blockchain_reader
import db

#############
# CONSTANTS #
#############

DB_DEFERRED_BLAME_PLACEHOLDER = 'DB_DEFERRED_BLAME_PLACEHOLDER'

###########
# CLASSES #
###########

class BlameRecord:
    
    #These fields are always set by constructor
    blame_label         = None
    address_reuse_role  = None #Of type AddressReuseRole enum
    data_source         = None
    
    #Optional fields that can be set in the constructor that help identify 
    #   a particular database record or external lookup identifier in case we 
    #   need to update that record in the future.
    row_id              = None
    tx_id               = None
    address_reuse_type  = None
    relevant_address    = None
    block_height        = None
    
    def __init__(self, blame_label, address_reuse_role, data_source, 
                 row_id = None, tx_id = None, address_reuse_type = None, 
                 relevant_address = None, block_height = None):
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
        return (('blame_label %s address_reuse_role %s data_source %s row_id %s '
                'tx_id %s address_reuse_type %s relevant_address %s '
                'block_height %s') % 
                (str(self.blame_label), str(self.address_reuse_role), 
                 str(self.data_source), str(self.row_id), str(self.tx_id), 
                 str(self.address_reuse_type), str(self.relevant_address), 
                 str(self.block_height)))

class Blamer:

    bci_blockchain_reader = None
    walletexplorer_reader = None
    database_connector = None
    
    def __init__(self, database_connector = None):
        if database_connector is None:
            self.database_connector = db.Database() #Create new db conn
        else:
            self.database_connector = database_connector #Use existing db conn
        
        self.blockchain_reader = blockchain_reader.ThrottledBlockchainReader(
            self.database_connector)
            
        self.walletexplorer_reader = blockchain_reader.WalletExplorerReader(
            self.database_connector)
        
    #Returns name of wallet client or API that created the transaction. If not 
    #   determined, returns None.
    def get_wallet_client_by_txObj(self, txObj):
        relayed_by = self.blockchain_reader.get_tx_relayed_by_using_txObj(txObj)
        relayed_by = relayed_by.strip() #remove trailing or leading whitespace

        if relayed_by == '127.0.0.1' or relayed_by == 'Blockchain.info':
            return 'Blockchain.info'
        elif relayed_by == '0.0.0.0':
            #BCI has no 'relayed by' data for this transaction, sometimes 
            #   because it predates BCI
            return None
        else:
            return None
    
    def get_wallet_client_by_tx_id(self, tx_id, benchmarker = None):
        relayed_by = self.blockchain_reader.get_tx_relayed_by_using_tx_id(
            tx_id, benchmarker = benchmarker)
        relayed_by = relayed_by.strip() #remove trailing or leaidng whitespace
        
        if relayed_by == '127.0.0.1' or relayed_by == 'Blockchain.info':
            return 'Blockchain.info'
        else:
            return None
        
    #wrapper for get_wallet_client_by_tx_id() that turns it into a BlameRecord 
    #   obj
    #Returns a BlameRecord object or None
    def get_wallet_client_blame_record_by_tx_id(self, tx_id, 
                                                defer_blaming = False,
                                                benchmarker = None):
        if defer_blaming:
            blame_record = BlameRecord(DB_DEFERRED_BLAME_PLACEHOLDER, 
                                       db.AddressReuseRole.CLIENT, 
                                       db.DataSource.BLOCKCHAIN_INFO)
            return blame_record
        else:
            client = self.get_wallet_client_by_tx_id(tx_id, 
                                                     benchmarker = benchmarker)
            if client is None:
                return None
            else:
                blame_record = BlameRecord(client, db.AddressReuseRole.CLIENT, 
                                           db.DataSource.BLOCKCHAIN_INFO)
                return blame_record

    #returns a list of BlameRecord objects
    def get_wallet_blame_list(self, tx_id, input_address_list, address, 
                              benchmarker = None, defer_blaming = False):
        blame_list = []
        blame_client_record = self.get_wallet_client_blame_record_by_tx_id(
            tx_id, defer_blaming, benchmarker = benchmarker)
        if blame_client_record is not None and blame_client_record:
            blame_list.append(blame_client_record)

        clustered_blame_records = self.get_wallet_label_records(tx_id, 
                                                                input_address_list, 
                                                                address, 
                                                                benchmarker,
                                                                defer_blaming)
        if clustered_blame_records is not None and \
                len(clustered_blame_records) > 0:
            blame_list.extend(clustered_blame_records)

        #print("DEBUG: Found %d parties to blame for address reuse for tx '%s' and address '%s'" % (len(clustered_blame_records), tx_id, address))
        return blame_list

    #Returns a list of BlameRecord objects, each record including the wallet 
    #   cluster label assigned by WalletExplorer.com and specifying whether 
    #   the blamed party was a sender or receiver
    def get_wallet_label_records(self, tx_id, input_address_list, address, 
                                 benchmarker = None, defer_blaming = False):
        return self.walletexplorer_reader.get_wallet_labels(tx_id, 
                                                            input_address_list, 
                                                            address, 
                                                            benchmarker,
                                                            defer_blaming)
    
    #returns the wallet label for a single bitcoin address
    def get_single_wallet_label(self, addr):
        return self.walletexplorer_reader.get_wallet_label_for_single_address(
            addr)
