from unittest import TestCase

from mtc2kafka.core import mtc_key_deserializer
from mtc2kafka.core import mtc_value_deserializer


class TestMTC2KafkaDeserializers(TestCase):
    """
    Defines tests for the MTConnect deserializers:
     - mtc_key_deserializer
     - mtc_value_deserializer
    """

    def test_mtc_key_deserializer(self):
        actual = mtc_key_deserializer(b"test_uuid")
        expected = 'test_uuid'
        self.assertEqual(actual, expected)

    def test_mtc_value_deserializer(self):
        actual = mtc_value_deserializer(b'{"id": "Zact", "tag": "Position", "attributes": {"name": "Zact", "subType": "ACTUAL", "timestamp": "2022-11-06T21:40:21.587353Z"}, "value": "-0.328"}')
        expected = {'id': 'Zact', 'tag': 'Position', 'attributes': {'name': 'Zact', 'subType': 'ACTUAL', 'timestamp': '2022-11-06T21:40:21.587353Z'}, 'value': '-0.328'}
        self.assertEqual(actual, expected)
