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
            mtc_streams_namespace = 'urn:mtconnect.org:MTConnectStreams:2.0'
            mtc_devices_namespace = 'urn:mtconnect.org:MTConnectDevices:2.0'
        self.testMTCDoc = TestMTCDoc()
        self.mtc_stream = ET.parse('tests/MTConnectDataMockUp/MTConnectStreams.xml')
        self.mtc_device = ET.parse('tests/MTConnectDataMockUp/MTConnectProbe.xml')

    def test_MTCDocumentMixing_mtc_ns_default(self):
        """ Tests if xml namespaces gets correct default value """
        mtcdoc = MTCDocumentMixing()
        self.assertEqual(mtcdoc.mtc_streams, {'mtc': 'urn:mtconnect.org:MTConnectStreams:1.7'})
        self.assertEqual(mtcdoc.mtc_devices, {'mtc': 'urn:mtconnect.org:MTConnectDevices:1.7'})

    def test_MTCDocumentMixing_mtc_ns_custom(self):
        """ Tests if xml namespaces gets correct custom value """
        class MTCDoc(MTCDocumentMixing):
            mtc_streams_namespace = 'urn:mtconnect.org:MTConnectStreams:2.0'
            mtc_devices_namespace = 'urn:mtconnect.org:MTConnectDevices:2.0'
        mtcdoc = MTCDoc()
        self.assertEqual(mtcdoc.mtc_streams, {'mtc': 'urn:mtconnect.org:MTConnectStreams:2.0'})
        self.assertEqual(mtcdoc.mtc_devices, {'mtc': 'urn:mtconnect.org:MTConnectDevices:2.0'})

    def test_MTCDocumentMixing_get_mtc_header_devices(self):
        """ Tests if MTConnect header is found in MTC Connect stream """
        header = self.testMTCDoc.get_mtc_header_devices(self.mtc_device)
        self.assertEqual(header.tag, '{urn:mtconnect.org:MTConnectDevices:2.0}Header')
        self.assertEqual(header.attrib, {'creationTime': '2023-06-19T00:30:30Z', 'sender': '50168b9130e2', 'instanceId': '1685383424', 'version': '2.0.0.12', 'deviceModelChangeTime': '2023-05-29T18:03:44.033375Z', 'assetBufferSize': '1024', 'assetCount': '0', 'bufferSize': '131072'})

    def test_MTCDocumentMixing_get_mtc_header_streams(self):
        """ Tests if MTConnect header is found in MTC Connect stream """
        header = self.testMTCDoc.get_mtc_header_streams(self.mtc_stream)
        self.assertEqual(header.tag, '{urn:mtconnect.org:MTConnectStreams:2.0}Header')
        self.assertEqual(header.attrib, {'creationTime': '2023-06-18T15:55:53Z', 'sender': '50168b9130e2', 'instanceId': '1685383424', 'version': '2.0.0.12', 'deviceModelChangeTime': '2023-05-29T18:03:44.033375Z', 'bufferSize': '131072', 'nextSequence': '87', 'firstSequence': '1', 'lastSequence': '5727'})

    def test_MTCDocumentMixing_get_mtc_DevicesStreams(self):
        """ Tests if MTConnect Devices are found in MTConnect stream """
        devices = self.testMTCDoc.get_mtc_DevicesStreams(self.mtc_stream)
        self.assertEqual(devices[0].tag, '{urn:mtconnect.org:MTConnectStreams:2.0}DeviceStream')
        self.assertEqual(devices[0].attrib, {'name': 'Agent', 'uuid': 'e8d21b67-fd02-51d1-93f4-848479bdde2c'})
        self.assertEqual(devices[1].tag, '{urn:mtconnect.org:MTConnectStreams:2.0}DeviceStream')
        self.assertEqual(devices[1].attrib, {'name': 'Zaix-4', 'uuid': 'ZAIX-4-003'})

    def test_MTCDocumentMixing_get_mtc_Devices(self):
        """ Tests if MTConnect Devices are found in MTConnect device file """
        devices = self.testMTCDoc.get_mtc_Devices(self.mtc_device)
        self.assertEqual(devices[0].tag, '{urn:mtconnect.org:MTConnectDevices:2.0}Agent')
        self.assertEqual(devices[0].attrib, {'id': 'agent_e8d21b67', 'mtconnectVersion': '2.0', 'name': 'Agent', 'uuid': 'e8d21b67-fd02-51d1-93f4-848479bdde2c'})
        self.assertEqual(devices[1].tag, '{urn:mtconnect.org:MTConnectDevices:2.0}Device')
        self.assertEqual(devices[1].attrib, {'id': 'device', 'name': 'Zaix-4', 'uuid': 'ZAIX-4-003'})

    def test_MTCDocumentMixing_get_dataItems(self):
        """ Tests if MTConnect dataItems are found in an MTC Connect stream """
        device = self.testMTCDoc.get_mtc_DevicesStreams(self.mtc_stream)[1]
        item = self.testMTCDoc.get_dataItems(device)
        self.assertEqual(item[0].tag, '{urn:mtconnect.org:MTConnectStreams:2.0}Block')
        self.assertEqual(item[0].attrib, {'dataItemId': 'block', 'name': 'block', 'sequence': '83', 'timestamp': '2023-06-07T19:17:15.169357Z'})
        self.assertEqual(item[0].text, 'Z-5 F100')
        self.assertEqual(item[1].tag, '{urn:mtconnect.org:MTConnectStreams:2.0}Block')
        self.assertEqual(item[1].attrib, {'dataItemId': 'block', 'name': 'block', 'sequence': '85', 'timestamp': '2023-06-07T19:17:29.426247Z'})
        self.assertEqual(item[1].text, 'Z-1 F100')
