# address-reuse-tracker
Maintains stats on address reuse in Bitcoin network, displays charts

## Dependencies

* IntEnum from enum34. `pip install enum34`
* GChartWrapper. `easy_install -U GChartWrapper`
* [bitcoinrpc](https://github.com/jgarzik/python-bitcoinrpc) `[sudo] python setup.py install`
* SQLite version 3.7.11 or higher, due to the use of [inserting multiple rows without `SELECT` and `UNION` clauses](http://stackoverflow.com/questions/1609637/is-it-possible-to-insert-multiple-rows-at-a-time-in-an-sqlite-database).

## Choosing a data source

This tool collects two types of information:
* Blockchain data
* Blame data relevant to address reuse via remote APIs.

Blockchain data can either be obtained via [Blockchain.info's API](https://blockchain.info/api) or from bitcoind via RPC interface. The latter is much faster but requires a local, indexed copy of the blockchain and installation of bitcoind/Bitcoin-Qt.

## Setup for using bitcoind's RPC interface

You will need the following entries at a minimum in your `bitcoin.conf` file if you choose to use bitcoind rather than remote API.

```ini
# server=1 tells Bitcoin-QT to accept JSON-RPC commands.
server=1

# You must set rpcuser and rpcpassword to secure the JSON-RPC api
rpcuser=my_fabulous_username_CHANGEME
rpcpassword=my_secret_password_CHANGEME

# Listen for RPC connections on this TCP port:
rpcport=8332

#Maintain a full transaction index, used by the getrawtransaction rpc call (default: 0)
txindex=1
```

## Configuring

Copy `address_reuse_example.cfg` to `address_reuse.cfg`. Modify the contents of this new file based on your API keys, RPC configurations, etc.

## Running

The tool can be used to populate the database based on blockchain analysis and remote API calls, and then to generate graphs.

### Populating the database

If you want to use only the remote API calls, run: `python update_using_remote_api.py`.

If you want to bitcoind's local copy of the blockchain, you will need to process things in two stages:

1. Run `python update_using_local_blockchain.py`.
2. Run `python update_deferred_blame_records.py`.

### Generating visualizations

This tool uses the Google graph library for visualization of address reuse. Once the database is populated to your liking, edit the constants in `graph-generator.py` and run:

`python graph-generator.py`

### Tests

There are two levels of tests: Suites that require remote API calls (slow) and those that don't (quick). Respectively, run `./run_quick_tests.sh` or `./run_slow_tests.sh`.

## Feedback

You may email all of the authors below or submit a GitHub issue.

## Primary Authors

Kristov Atlas (email: firstname @ openbitcoinprivacyproject.org)

## Acknowledgments

Thanks to LaurentMT, Jameson Lopp, and Justus Ranvier for their input.
