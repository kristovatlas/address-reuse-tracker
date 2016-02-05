'''Unit tests for `bip69` module.'''

import unittest
import os
import json

import bip69

#TODO: move me to a .json in a test directory
TX_0a6a357e = '''
{
    "txid": "0a6a357e2f7796444e02638749d9611c008b253fb55f5dc88b739b230ed0c4c3",
    "version": 1,
    "locktime": 0,
    "vin": [
        {
            "txid": "643e5f4e66373a57251fb173151e838ccd27d279aca882997e005016bb53d5aa",
            "vout": 0,
            "scriptSig": {
                "asm": "304402205438cedd30ee828b0938a863e08d810526123746c1f4abee5b7bc2312373450c02207f26914f4275f8f0040ab3375bacc8c5d610c095db8ed0785de5dc57456591a601 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "47304402205438cedd30ee828b0938a863e08d810526123746c1f4abee5b7bc2312373450c02207f26914f4275f8f0040ab3375bacc8c5d610c095db8ed0785de5dc57456591a601210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        },
        {
            "txid": "28e0fdd185542f2c6ea19030b0796051e7772b6026dd5ddccd7a2f93b73e6fc2",
            "vout": 0,
            "scriptSig": {
                "asm": "3045022100f81d98c1de9bb61063a5e6671d191b400fda3a07d886e663799760393405439d0220234303c9af4bad3d665f00277fe70cdd26cd56679f114a40d9107249d29c979401 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "483045022100f81d98c1de9bb61063a5e6671d191b400fda3a07d886e663799760393405439d0220234303c9af4bad3d665f00277fe70cdd26cd56679f114a40d9107249d29c979401210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        },
        {
            "txid": "f0a130a84912d03c1d284974f563c5949ac13f8342b8112edff52971599e6a45",
            "vout": 0,
            "scriptSig": {
                "asm": "304402202310b00924794ef68a8f09564fd0bb128838c66bc45d1a3f95c5cab52680f166022039fc99138c29f6c434012b14aca651b1c02d97324d6bd9dd0ffced0782c7e3bd01 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "47304402202310b00924794ef68a8f09564fd0bb128838c66bc45d1a3f95c5cab52680f166022039fc99138c29f6c434012b14aca651b1c02d97324d6bd9dd0ffced0782c7e3bd01210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        },
        {
            "txid": "0e53ec5dfb2cb8a71fec32dc9a634a35b7e24799295ddd5278217822e0b31f57",
            "vout": 0,
            "scriptSig": {
                "asm": "3045022100d276251f1f4479d8521269ec8b1b45c6f0e779fcf1658ec627689fa8a55a9ca50220212a1e307e6182479818c543e1b47d62e4fc3ce6cc7fc78183c7071d245839df01 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "483045022100d276251f1f4479d8521269ec8b1b45c6f0e779fcf1658ec627689fa8a55a9ca50220212a1e307e6182479818c543e1b47d62e4fc3ce6cc7fc78183c7071d245839df01210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        },
        {
            "txid": "381de9b9ae1a94d9c17f6a08ef9d341a5ce29e2e60c36a52d333ff6203e58d5d",
            "vout": 1,
            "scriptSig": {
                "asm": "30450221008768eeb1240451c127b88d89047dd387d13357ce5496726fc7813edc6acd55ac022015187451c3fb66629af38fdb061dfb39899244b15c45e4a7ccc31064a059730d01 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "4830450221008768eeb1240451c127b88d89047dd387d13357ce5496726fc7813edc6acd55ac022015187451c3fb66629af38fdb061dfb39899244b15c45e4a7ccc31064a059730d01210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        },
        {
            "txid": "f320832a9d2e2452af63154bc687493484a0e7745ebd3aaf9ca19eb80834ad60",
            "vout": 0,
            "scriptSig": {
                "asm": "30450221009be4261ec050ebf33fa3d47248c7086e4c247cafbb100ea7cee4aa81cd1383f5022008a70d6402b153560096c849d7da6fe61c771a60e41ff457aac30673ceceafee01 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "4830450221009be4261ec050ebf33fa3d47248c7086e4c247cafbb100ea7cee4aa81cd1383f5022008a70d6402b153560096c849d7da6fe61c771a60e41ff457aac30673ceceafee01210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        },
        {
            "txid": "de0411a1e97484a2804ff1dbde260ac19de841bebad1880c782941aca883b4e9",
            "vout": 1,
            "scriptSig": {
                "asm": "30450221009bc40eee321b39b5dc26883f79cd1f5a226fc6eed9e79e21d828f4c23190c57e022078182fd6086e265589105023d9efa4cba83f38c674a499481bd54eee196b033f01 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "4830450221009bc40eee321b39b5dc26883f79cd1f5a226fc6eed9e79e21d828f4c23190c57e022078182fd6086e265589105023d9efa4cba83f38c674a499481bd54eee196b033f01210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        },
        {
            "txid": "3b8b2f8efceb60ba78ca8bba206a137f14cb5ea4035e761ee204302d46b98de2",
            "vout": 0,
            "scriptSig": {
                "asm": "304402200fb572b7c6916515452e370c2b6f97fcae54abe0793d804a5a53e419983fae1602205191984b6928bf4a1e25b00e5b5569a0ce1ecb82db2dea75fe4378673b53b9e801 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "47304402200fb572b7c6916515452e370c2b6f97fcae54abe0793d804a5a53e419983fae1602205191984b6928bf4a1e25b00e5b5569a0ce1ecb82db2dea75fe4378673b53b9e801210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        },
        {
            "txid": "54ffff182965ed0957dba1239c27164ace5a73c9b62a660c74b7b7f15ff61e7a",
            "vout": 1,
            "scriptSig": {
                "asm": "304402206bc218a925f7280d615c8ea4f0131a9f26e7fc64cff6eeeb44edb88aba14f1910220779d5d67231bc2d2d93c3c5ab74dcd193dd3d04023e58709ad7ffbf95161be6201 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "47304402206bc218a925f7280d615c8ea4f0131a9f26e7fc64cff6eeeb44edb88aba14f1910220779d5d67231bc2d2d93c3c5ab74dcd193dd3d04023e58709ad7ffbf95161be6201210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        },
        {
            "txid": "bafd65e3c7f3f9fdfdc1ddb026131b278c3be1af90a4a6ffa78c4658f9ec0c85",
            "vout": 0,
            "scriptSig": {
                "asm": "3044022047df98cc26bd2bfdc5b2b97c27aead78a214810ff023e721339292d5ce50823d02205fe99dc5f667908974dae40cc7a9475af7fa6671ba44f64a00fcd01fa12ab52301 02ca46fa75454650afba1784bc7b079d687e808634411e4beff1f70e44596308a1",
                "hex": "473044022047df98cc26bd2bfdc5b2b97c27aead78a214810ff023e721339292d5ce50823d02205fe99dc5f667908974dae40cc7a9475af7fa6671ba44f64a00fcd01fa12ab523012102ca46fa75454650afba1784bc7b079d687e808634411e4beff1f70e44596308a1"
            },
            "sequence": 4294967295
        },
        {
            "txid": "a5e899dddb28776ea9ddac0a502316d53a4a3fca607c72f66c470e0412e34086",
            "vout": 0,
            "scriptSig": {
                "asm": "304402205566aa84d3d84226d5ab93e6f253b57b3ef37eb09bb73441dae35de86271352a02206ee0b7f800f73695a2073a2967c9ad99e19f6ddf18ce877adf822e408ba9291e01 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "47304402205566aa84d3d84226d5ab93e6f253b57b3ef37eb09bb73441dae35de86271352a02206ee0b7f800f73695a2073a2967c9ad99e19f6ddf18ce877adf822e408ba9291e01210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        },
        {
            "txid": "7a1de137cbafb5c70405455c49c5104ca3057a1f1243e6563bb9245c9c88c191",
            "vout": 0,
            "scriptSig": {
                "asm": "3045022100df61d45bbaa4571cdd6c5c822cba458cdc55285cdf7ba9cd5bb9fc18096deb9102201caf8c771204df7fd7c920c4489da7bc3a60e1d23c1a97e237c63afe53250b4a01 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "483045022100df61d45bbaa4571cdd6c5c822cba458cdc55285cdf7ba9cd5bb9fc18096deb9102201caf8c771204df7fd7c920c4489da7bc3a60e1d23c1a97e237c63afe53250b4a01210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        },
        {
            "txid": "26aa6e6d8b9e49bb0630aac301db6757c02e3619feb4ee0eea81eb1672947024",
            "vout": 1,
            "scriptSig": {
                "asm": "3044022031501a0b2846b8822a32b9947b058d89d32fc758e009fc2130c2e5effc925af70220574ef3c9e350cef726c75114f0701fd8b188c6ec5f84adce0ed5c393828a5ae001 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "473044022031501a0b2846b8822a32b9947b058d89d32fc758e009fc2130c2e5effc925af70220574ef3c9e350cef726c75114f0701fd8b188c6ec5f84adce0ed5c393828a5ae001210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        },
        {
            "txid": "402b2c02411720bf409eff60d05adad684f135838962823f3614cc657dd7bc0a",
            "vout": 1,
            "scriptSig": {
                "asm": "3045022100a6ac110802b699f9a2bff0eea252d32e3d572b19214d49d8bb7405efa2af28f1022033b7563eb595f6d7ed7ec01734e17b505214fe0851352ed9c3c8120d53268e9a01 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "483045022100a6ac110802b699f9a2bff0eea252d32e3d572b19214d49d8bb7405efa2af28f1022033b7563eb595f6d7ed7ec01734e17b505214fe0851352ed9c3c8120d53268e9a01210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        },
        {
            "txid": "7d037ceb2ee0dc03e82f17be7935d238b35d1deabf953a892a4507bfbeeb3ba4",
            "vout": 1,
            "scriptSig": {
                "asm": "3045022100ebc77ed0f11d15fe630fe533dc350c2ddc1c81cfeb81d5a27d0587163f58a28c02200983b2a32a1014bab633bfc9258083ac282b79566b6b3fa45c1e6758610444f401 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "483045022100ebc77ed0f11d15fe630fe533dc350c2ddc1c81cfeb81d5a27d0587163f58a28c02200983b2a32a1014bab633bfc9258083ac282b79566b6b3fa45c1e6758610444f401210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        },
        {
            "txid": "6c1d56f31b2de4bfc6aaea28396b333102b1f600da9c6d6149e96ca43f1102b1",
            "vout": 1,
            "scriptSig": {
                "asm": "3044022010f8731929a55c1c49610722e965635529ed895b2292d781b183d465799906b20220098359adcbc669cd4b294cc129b110fe035d2f76517248f4b7129f3bf793d07f01 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "473044022010f8731929a55c1c49610722e965635529ed895b2292d781b183d465799906b20220098359adcbc669cd4b294cc129b110fe035d2f76517248f4b7129f3bf793d07f01210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        },
        {
            "txid": "b4112b8f900a7ca0c8b0e7c4dfad35c6be5f6be46b3458974988e1cdb2fa61b8",
            "vout": 0,
            "scriptSig": {
                "asm": "304402207328142bb02ef5d6496a210300f4aea71f67683b842fa3df32cae6c88b49a9bb022020f56ddff5042260cfda2c9f39b7dec858cc2f4a76a987cd2dc25945b04e15fe01 0391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210",
                "hex": "47304402207328142bb02ef5d6496a210300f4aea71f67683b842fa3df32cae6c88b49a9bb022020f56ddff5042260cfda2c9f39b7dec858cc2f4a76a987cd2dc25945b04e15fe01210391064d5b2d1c70f264969046fcff853a7e2bfde5d121d38dc5ebd7bc37c2b210"
            },
            "sequence": 4294967295
        }
    ],
    "vout": [
        {
            "value": 4.00057456,
            "n": 0,
            "scriptPubKey": {
                "asm": "OP_DUP OP_HASH160 4a5fba237213a062f6f57978f796390bdcf8d015 OP_EQUALVERIFY OP_CHECKSIG",
                "hex": "76a9144a5fba237213a062f6f57978f796390bdcf8d01588ac",
                "reqSigs": 1,
                "type": "pubkeyhash",
                "addresses": [
                    "17nFgS1YaDPnXKMPQkZVdNQqZnVqRgBwnZ"
                ]
            }
        },
        {
            "value": 400,
            "n": 1,
            "scriptPubKey": {
                "asm": "OP_DUP OP_HASH160 5be32612930b8323add2212a4ec03c1562084f84 OP_EQUALVERIFY OP_CHECKSIG",
                "hex": "76a9145be32612930b8323add2212a4ec03c1562084f8488ac",
                "reqSigs": 1,
                "type": "pubkeyhash",
                "addresses": [
                    "19Nrc2Xm226xmSbeGZ1BVtX7DUm4oCx8Pm"
                ]
            }
        }
    ]
}
'''

class Bip69Test(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_inputs_from_rpc_json_0a6a357e(self):
        tx_0a6a357e = json.loads(TX_0a6a357e)
        inputs = bip69.get_inputs_from_rpc_json(tx_0a6a357e)

        self.assertEqual(len(inputs), 17)
        self.assertEqual(inputs[0], (('643e5f4e66373a57251fb173151e838ccd27d279'
                                      'aca882997e005016bb53d5aa'), 0))
        self.assertEqual(inputs[15], (('6c1d56f31b2de4bfc6aaea28396b333102b1f60'
                                       '0da9c6d6149e96ca43f1102b1'), 1))

    def test_sort_inputs_0a6a357e(self):
        tx_0a6a357e = json.loads(TX_0a6a357e)
        inputs = bip69.get_inputs_from_rpc_json(tx_0a6a357e)
        bip69_inputs = bip69.sort_inputs(inputs)
        self.assertEqual(bip69_inputs[0], (('0e53ec5dfb2cb8a71fec32dc9a634a35b7'
                                            'e24799295ddd5278217822e0b31f57'),
                                            0))
        self.assertEqual(bip69_inputs[10], (('7d037ceb2ee0dc03e82f17be7935d238b'
                                             '35d1deabf953a892a4507bfbeeb3ba4'),
                                           1))

    def test_get_outputs_from_rpc_json_0a6a357e(self):
        tx_0a6a357e = json.loads(TX_0a6a357e)
        outputs = bip69.get_outputs_from_rpc_json(tx_0a6a357e)

        self.assertEqual(len(outputs), 2)
        self.assertEqual(outputs[0], (('76a9144a5fba237213a062f6f57978f796390bd'
                                       'cf8d01588ac'), 400057456))
        self.assertEqual(outputs[1], (('76a9145be32612930b8323add2212a4ec03c156'
                                       '2084f8488ac'), 40000000000))

    def test_sort_outputs_0a6a357e(self):

        tx_0a6a357e = json.loads(TX_0a6a357e)
        outputs = bip69.get_outputs_from_rpc_json(tx_0a6a357e)
        bip69_outputs = bip69.sort_outputs(outputs)
        self.assertEqual(bip69_outputs[0], (('76a9144a5fba237213a062f6f57978f79'
                                             '6390bdcf8d01588ac'), 400057456))
        self.assertEqual(bip69_outputs[1], (('76a9145be32612930b8323add2212a4ec'
                                             '03c1562084f8488ac'), 40000000000))

    def test_is_bip69_0a6a357e(self):
        tx_0a6a357e = json.loads(TX_0a6a357e)
        self.assertFalse(bip69.is_bip69(tx_0a6a357e))

    def test_is_bip69_with_properly_sorted_inputs_and_outputs(self):
        BIP_69_TX_JSON = """
        {
            "vin": [
                {
                    "txid": "28e0fdd185542f2c6ea19030b0796051e7772b6026dd5ddccd7a2f93b73e6fc2",
                    "vout": 0
                },
                {
                    "txid": "643e5f4e66373a57251fb173151e838ccd27d279aca882997e005016bb53d5aa",
                    "vout": 0
                }
            ],
            "vout": [
                {
                    "value": 4.00057456,
                    "n": 0,
                    "scriptPubKey": {
                        "hex": "76a9144a5fba237213a062f6f57978f796390bdcf8d01588ac"
                    }
                },
                {
                    "value": 400,
                    "n": 1,
                    "scriptPubKey": {
                        "hex": "76a9145be32612930b8323add2212a4ec03c1562084f8488ac"
                    }
                }
            ]
        }
        """
        txn = json.loads(BIP_69_TX_JSON)
        self.assertTrue(bip69.is_bip69(txn))

unittest.TestLoader().loadTestsFromTestCase(Bip69Test)