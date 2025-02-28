from unittest import TestCase
from unittest.mock import patch
from requests.models import Response

from mtc2kafka.core import MTCAgent
from mtc2kafka.core import ImproperlyConfigured


def mocked_requests_get(*args, **kwargs):
    """ mocked requests.get method """
    response_content = None
    request_url = args[0]
    if request_url == 'http://test_agent:5000/probe':
        f = open('tests/MTConnectDataMockUp/MTConnectProbe.xml')
        response_content = f.read()
        f.close()
    elif request_url == 'http://test_agent:5000/current':
        f = open('tests/MTConnectDataMockUp/MTConnectCurrent.xml')
        response_content = f.read()
        f.close()
    elif request_url == 'http://test_agent:5000':
        f = open('tests/MTConnectDataMockUp/MTConnectProbe.xml')
        response_content = f.read()
        f.close()
    response = Response()
    response.status_code = 200
    response._content = str.encode(response_content)
    return response


class TestMTCAgent(TestCase):
    """
    Tests for MTCAgent
    """

    def test_MTCAgent_mtc_agent_none(self):
        """ Tests if ImproperlyConfigured error raised when mtc_agent is not defined """
        self.assertRaises(ImproperlyConfigured, MTCAgent)

    @patch("requests.get", side_effect=mocked_requests_get)
    def test_MTCAgent_mtc_agent_namespaces(self, mock_requests_get):
        """ Tests if XML namespace is extracted from /probe and /current requests """
        class TestMTCAgent(MTCAgent):
            mtc_agent = 'http://test_agent:5000'
        agent = TestMTCAgent()
        self.assertEqual(agent.mtc_devices, {'mtc': 'urn:mtconnect.org:MTConnectDevices:2.0'})
        self.assertEqual(agent.mtc_streams, {'mtc': 'urn:mtconnect.org:MTConnectStreams:2.0'})

    @patch("requests.get", side_effect=mocked_requests_get)
    def test_MTCAgent_get_agent_baseUrl(self, mock_requests_get):
        """ Tests if mtc_agent is used to form the base url """
        class TestMTCAgent(MTCAgent):
            mtc_agent = 'http://test_agent:5000'
        agent = TestMTCAgent()
        self.assertEqual(agent.get_agent_baseUrl(), 'http://test_agent:5000')

    @patch("requests.get", side_effect=mocked_requests_get)
    def test_MTCAgent_get_agent_uuid(self, mock_requests_get):
        """ Tests if the MTConnect Agent UUID is returned """
        class TestMTCAgent(MTCAgent):
            mtc_agent = 'http://test_agent:5000'
        agent = TestMTCAgent()
        self.assertEqual(agent.get_agent_uuid(), 'e8d21b67-fd02-51d1-93f4-848479bdde2c')

    @patch("requests.get", side_effect=mocked_requests_get)
    def test_MTCAgent_get_agent_instanceId(self, mock_requests_get):
        """ Tests if the MTConnect Agent instanceId is returned """
        class TestMTCAgent(MTCAgent):
            mtc_agent = 'http://test_agent:5000'
        agent = TestMTCAgent()
        self.assertEqual(agent.get_agent_instanceId(), 1685383424)

    @patch("requests.get", side_effect=mocked_requests_get)
    def test_MTCAgent_get_agent_devices(self, mock_requests_get):
        """ Tests if MTConnect Devices are found in a MTConnect device file """
        class TestMTCAgent(MTCAgent):
            mtc_agent = 'http://test_agent:5000'
        agent = TestMTCAgent()
        devices = agent.get_agent_devices()
        self.assertEqual(devices[0].tag, '{urn:mtconnect.org:MTConnectDevices:2.0}Agent')
        self.assertEqual(devices[0].attrib, {'id': 'agent_e8d21b67', 'mtconnectVersion': '2.0', 'name': 'Agent', 'uuid': 'e8d21b67-fd02-51d1-93f4-848479bdde2c'})
        self.assertEqual(devices[1].tag, '{urn:mtconnect.org:MTConnectDevices:2.0}Device')
        self.assertEqual(devices[1].attrib, {'id': 'device', 'name': 'Zaix-4', 'uuid': 'ZAIX-4-003'})

    @patch("requests.get", side_effect=mocked_requests_get)
    def test_MTCAgent_get_agent_adapters(self, mock_requests_get):
        """ Tests if MTConnect Adapters are found in a MTConnect device file """
        class TestMTCAgent(MTCAgent):
            mtc_agent = 'http://test_agent:5000'
        agent = TestMTCAgent()
        adapts = agent.get_agent_adapters()
        self.assertEqual(adapts[0].tag, '{urn:mtconnect.org:MTConnectDevices:2.0}Adapter')
        self.assertEqual(adapts[0].attrib, {'id': '_d8b297ff1b', 'name': '123.345.6.789:7879'})

    @patch("requests.get", side_effect=mocked_requests_get)
    def test_MTCAgent_get_agent_adapters_id(self, mock_requests_get):
        """ Tests if MTConnect Adapters IDs are found in a MTConnect device file """
        class TestMTCAgent(MTCAgent):
            mtc_agent = 'http://test_agent:5000'
        agent = TestMTCAgent()
        adapts = agent.get_agent_adapters_id()
        self.assertEqual(adapts, ['_d8b297ff1b'])
