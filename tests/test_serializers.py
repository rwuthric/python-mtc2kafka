from unittest import TestCase
import xml.etree.ElementTree as ET

from mtc2kafka.core import mtc_dataItem_key_serializer
from mtc2kafka.core import mtc_dataItem_value_serializer


class TestMTC2KafkaSerializers(TestCase):
    """
    Defines tests for the MMP serializers:
     - mtc_dataItem_key_serializer
     - mtc_dataItem_value_serializer
    """
    
    def test_mtc_dataItem_key_serializer(self):
        actual = mtc_dataItem_key_serializer("test_uuid")
        expected = b'test_uuid'
        self.assertEqual(actual, expected)
        
    def test_mtc_dataItem_value_serializer(self):
        dataItem = ET.fromstring('<Position dataItemId="Zact" name="Zact" sequence="18059" subType="ACTUAL" timestamp="2022-11-06T21:40:21.587353Z">-0.328</Position>')
        actual = mtc_dataItem_value_serializer(dataItem)
        expected = b"{'id': 'Zact', 'tag': 'Position', 'attributes': {'name': 'Zact', 'subType': 'ACTUAL', 'timestamp': '2022-11-06T21:40:21.587353Z'}, 'value': '-0.328'}"
        self.assertEqual(actual, expected)
