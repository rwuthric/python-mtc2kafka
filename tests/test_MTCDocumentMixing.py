from unittest import TestCase
import xml.etree.ElementTree as ET

from mtc2kafka.core.mtcdocument import MTCDocumentMixing


class TestMTCDocumentMixing(TestCase):
    """
    Tests for MTCDocumentMixing
    """
    
    def setUp(self):
        """ Loads MTC Connect stream mockup data """
        class TestMTCDoc(MTCDocumentMixing):
            mtc_namespace = 'urn:mtconnect.org:MTConnectStreams:2.0'
        self.testMTCDoc = TestMTCDoc()
        self.mtc_stream = ET.parse('tests/MTConnectDataMockUp/MTConnectStreams.xml')
    
    def test_MTCDocumentMixing_mtc_ns_default(self):
        """ Tests if mtc_ns gets correct default value """
        mtcdoc = MTCDocumentMixing()
        self.assertEqual(mtcdoc.mtc_ns, {'mtc': 'urn:mtconnect.org:MTConnectStreams:1.7'})
        
    def test_MTCDocumentMixing_mtc_ns_custom(self):
        """ Tests if mtc_ns gets correct custom value """
        class MTCDoc(MTCDocumentMixing):
            mtc_namespace = 'urn:mtconnect.org:MTConnectStreams:2.0'
        mtcdoc = MTCDoc()
        self.assertEqual(mtcdoc.mtc_ns, {'mtc': 'urn:mtconnect.org:MTConnectStreams:2.0'})
        
    def test_get_get_mtc_header(self):
        """ Tests if MTConnect header is found in MTC Connect stream """
        header = self.testMTCDoc.get_mtc_header(self.mtc_stream)
        self.assertEqual(header.tag, '{urn:mtconnect.org:MTConnectStreams:2.0}Header')
        self.assertEqual(header.attrib, {'creationTime': '2023-06-18T15:55:53Z', 'sender': '50168b9130e2', 'instanceId': '1685383424', 'version': '2.0.0.12', 'deviceModelChangeTime': '2023-05-29T18:03:44.033375Z', 'bufferSize': '131072', 'nextSequence': '87', 'firstSequence': '1', 'lastSequence': '5727'})
        
    def test_get_mtc_DeviceStreams(self):
        """ Tests if MTConnect Devices are found in MTC Connect stream """
        devices = self.testMTCDoc.get_mtc_DeviceStreams(self.mtc_stream)
        self.assertEqual(devices[0].tag, '{urn:mtconnect.org:MTConnectStreams:2.0}DeviceStream')
        self.assertEqual(devices[0].attrib, {'name': 'Agent', 'uuid': 'e8d21b67-fd02-51d1-93f4-848479bdde2c'})
        self.assertEqual(devices[1].tag, '{urn:mtconnect.org:MTConnectStreams:2.0}DeviceStream')
        self.assertEqual(devices[1].attrib, {'name': 'Zaix-4', 'uuid': 'ZAIX-4-003'})
        
    def test_get_get_mtc_header(self):
        """ Tests if MTConnect header is found in MTC Connect stream """
        device = self.testMTCDoc.get_mtc_DeviceStreams(self.mtc_stream)[1]
        item = self.testMTCDoc.get_dataItems(device)
        self.assertEqual(item[0].tag, '{urn:mtconnect.org:MTConnectStreams:2.0}Block')
        self.assertEqual(item[0].attrib, {'dataItemId': 'block', 'name': 'block', 'sequence': '83', 'timestamp': '2023-06-07T19:17:15.169357Z'})
        self.assertEqual(item[0].text, 'Z-5 F100')
        self.assertEqual(item[1].tag, '{urn:mtconnect.org:MTConnectStreams:2.0}Block')
        self.assertEqual(item[1].attrib, {'dataItemId': 'block', 'name': 'block', 'sequence': '85', 'timestamp': '2023-06-07T19:17:29.426247Z'})
        self.assertEqual(item[1].text, 'Z-1 F100')