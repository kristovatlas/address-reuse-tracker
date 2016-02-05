'''Functions related to BIP-69.

See: https://github.com/bitcoin/bips/blob/master/bip-0069.mediawiki
'''

import sys #sys.maxint

#https://en.bitcoin.it/wiki/Controlled_supply
MAX_SATOSHIS = 1968750000000000
SATOSHIS_PER_BTC = 100000000

def sort_inputs(inputs):
    '''Given a list of inputs, get the list back sorted according to BIP-69.
    Args:
        inputs (List[(str, int)]): List of input tuples consisting of txid and
            index of previous output.

    Returns:
        List[(str, int)]: Sorted list of inputs.
    '''

    return sorted(inputs, key=lambda tup: (tup[0], tup[1]))

def sort_outputs(outputs):
    '''Given a list of outputs, get the list back sorted according to BIP-69.
    Args:
        outputs (List[str, int]):

    Returns:
        List[(str, int)]: Sorted list of outputs.
    '''
    assert sys.maxint > MAX_SATOSHIS #satoshis can be large integers
    return sorted(outputs, key=lambda tup: (tup[1], tup[0]))

def get_inputs_from_rpc_json(rpc_tx_json):
    '''Get just the inputs as a list of tuples of (txid, index).
    Args:
        rpc_tx_json: The object returned by calling `json.loads` on
            `decoderawtransaction`.
    Returns:
        List[(str, int)]: List of inputs.
    '''
    if 'vin' not in rpc_tx_json:
        return []
    inputs = []
    for vin in rpc_tx_json['vin']:
        if 'txid' not in vin or 'vout' not in vin:
            continue
        txid = str(vin['txid'])
        index = int(vin['vout'])

        inputs.append((txid, index))
    return inputs

def get_outputs_from_rpc_json(rpc_tx_json):
    '''Get just the outputs as list of tuples of (scriptPubKey, amt).
    Args:
        rpc_tx_json: The object returned by calling `json.loads` on
            `decoderawtransaction`.
    Returns:
        List[(str, int)]: List of outputs.
    '''
    assert sys.maxint > MAX_SATOSHIS #satoshis can be large integers

    if 'vout' not in rpc_tx_json:
        return []
    outputs = []
    for vout in rpc_tx_json['vout']:
        if ('scriptPubKey' not in vout or 'hex' not in vout['scriptPubKey'] or
                'value' not in vout):
            continue
        script_pub_key = vout['scriptPubKey']['hex']
        satoshis = int(round(vout['value'] * SATOSHIS_PER_BTC))

        outputs.append((script_pub_key, satoshis))
    return outputs

def is_bip69(json_tx_json):
    '''Determine whether transaction is BIP-69 compliant.
    Args:
        rpc_tx_json: The object returned by calling `json.loads` on
            `decoderawtransaction`.

    Returns:
        bool: Whether transaction is BIP-69 compliant.
    '''
    inputs = get_inputs_from_rpc_json(json_tx_json)
    sorted_inputs = sort_inputs(inputs)
    if inputs != sorted_inputs:
        return False
    outputs = get_outputs_from_rpc_json(json_tx_json)
    sorted_outputs = sort_outputs(outputs)
    return outputs == sorted_outputs
