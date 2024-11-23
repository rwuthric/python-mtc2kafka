import unittest
from unittest.mock import patch, MagicMock
from mtc2kafka.connectors import MTCSourceConnector
from mtc2kafka.core import ImproperlyConfigured


class MockedMTCSourceConnector(MTCSourceConnector):
    mtc_agent = 'mocked_agent'
    mtc_agent_uuid = 'mocked_agent_uuid'
    bootstrap_servers = ['localhost:9092']
    kafka_producer_uuid = 'mocked_producer_uuid'
    mtconnect_devices_topic = 'mtc_devices'


class TestMTCSourceConnector(unittest.TestCase):
    """
    Tests for MTCSourceConnector
    """

    @patch.object(MTCSourceConnector, 'get_agent_uuid', return_value='mocked_agent_uuid')
    def test_init(self, mock_get_agent_uuid):
        """ Test constructor """

        # Mock attributes
        MTCSourceConnector.mtc_agent = 'mocked_agent'
        MTCSourceConnector.bootstrap_servers = ['localhost:9092']
        MTCSourceConnector.kafka_producer_uuid = 'mocked_producer_uuid'

        # Test initialization without exceptions
        connector = MTCSourceConnector()
        self.assertEqual(connector.mtc_agent_uuid, 'mocked_agent_uuid')

        # Test exception when bootstrap_servers is not set
        MTCSourceConnector.bootstrap_servers = None
        with self.assertRaises(ImproperlyConfigured):
            MTCSourceConnector()

        # Test exception when kafka_producer_uuid is not set
        MTCSourceConnector.bootstrap_servers = ['localhost:9092']
        MTCSourceConnector.kafka_producer_uuid = None
        with self.assertRaises(ImproperlyConfigured):
            MTCSourceConnector()

    @patch('mtc2kafka.connectors.mtcsourceconnector.KafkaProducer')
    @patch('mtc2kafka.connectors.mtcsourceconnector.DataItem')
    def test_send_agent_availability(self, MockDataItem, MockKafkaProducer):
        """ Tests sending of Agent Availability """

        connector = MockedMTCSourceConnector()
        connector.mtc_dataItem_key_serializer = MagicMock()
        connector.mtc_dataItem_value_serializer = MagicMock()

        # Mock the DataItem instance
        mock_data_item_instance = MockDataItem.return_value
        mock_data_item_instance.tag = None
        mock_data_item_instance.attrib = {}
        mock_data_item_instance.text = None

        # Mock the KafkaProducer instance and its methods
        mock_producer_instance = MockKafkaProducer.return_value
        mock_producer_instance.send.return_value = MagicMock()

        # Call the method
        connector.send_agent_availability("MOCK AVAILABLE")

        # Check format of DataItem
        MockDataItem.assert_called_once()
        self.assertEqual(mock_data_item_instance.tag, "Availability")
        self.assertEqual(mock_data_item_instance.attrib['dataItemId'], "agent_avail")
        self.assertEqual(mock_data_item_instance.text, "MOCK AVAILABLE")
        self.assertEqual(mock_data_item_instance.type, "Events")

        # Check if correct Kafka producer used
        MockKafkaProducer.assert_called_once_with(
            bootstrap_servers=['localhost:9092'],
            key_serializer=connector.mtc_dataItem_key_serializer,
            value_serializer=connector.mtc_dataItem_value_serializer
        )

        # Check sending of correct Kafka message
        mock_producer_instance.send.assert_called_once()
        mock_producer_instance = MockKafkaProducer.return_value
        self.assertEqual(mock_producer_instance.send.call_args[0], ('mtc_devices',))
        call_args = mock_producer_instance.send.call_args[1]
        self.assertEqual(call_args['key'], 'mocked_agent_uuid')
        self.assertEqual(call_args['value'], mock_data_item_instance)

    @patch('mtc2kafka.connectors.mtcsourceconnector.KafkaProducer')
    @patch('mtc2kafka.connectors.mtcsourceconnector.DataItem')
    def test_send_producer_availability(self, MockDataItem, MockKafkaProducer):
        """ Tests sending of Producer Availability """

        connector = MockedMTCSourceConnector()
        connector.mtc_dataItem_key_serializer = MagicMock()
        connector.mtc_dataItem_value_serializer = MagicMock()

        # Mock the DataItem instance
        mock_data_item_instance = MockDataItem.return_value
        mock_data_item_instance.tag = None
        mock_data_item_instance.attrib = {}
        mock_data_item_instance.text = None

        # Mock the KafkaProducer instance and its methods
        mock_producer_instance = MockKafkaProducer.return_value
        mock_producer_instance.send.return_value = MagicMock()

        # Call the method
        connector.send_producer_availability("MOCK AVAILABLE")

        # Check format of DataItem
        MockDataItem.assert_called_once()
        self.assertEqual(mock_data_item_instance.tag, "Availability")
        self.assertEqual(mock_data_item_instance.attrib['dataItemId'], "avail")
        self.assertEqual(mock_data_item_instance.text, "MOCK AVAILABLE")
        self.assertEqual(mock_data_item_instance.type, "Events")

        # Check if correct Kafka producer used
        MockKafkaProducer.assert_called_once_with(
            bootstrap_servers=['localhost:9092'],
            key_serializer=connector.mtc_dataItem_key_serializer,
            value_serializer=connector.mtc_dataItem_value_serializer
        )

        # Check sending of correct Kafka message
        mock_producer_instance.send.assert_called_once()
        mock_producer_instance = MockKafkaProducer.return_value
        self.assertEqual(mock_producer_instance.send.call_args[0], ('mtc_devices',))
        call_args = mock_producer_instance.send.call_args[1]
        self.assertEqual(call_args['key'], 'mocked_producer_uuid')
        self.assertEqual(call_args['value'], mock_data_item_instance)

    @patch('mtc2kafka.connectors.mtcsourceconnector.KafkaProducer')
    @patch('mtc2kafka.connectors.mtcsourceconnector.DataItem')
    def test_send_producer_software_version(self, MockDataItem, MockKafkaProducer):
        """ Tests sending of Producer Software Version """

        connector = MockedMTCSourceConnector()
        connector.mtc_dataItem_key_serializer = MagicMock()
        connector.mtc_dataItem_value_serializer = MagicMock()

        # Mock the DataItem instance
        mock_data_item_instance = MockDataItem.return_value
        mock_data_item_instance.tag = None
        mock_data_item_instance.attrib = {}
        mock_data_item_instance.text = None

        # Mock the KafkaProducer instance and its methods
        mock_producer_instance = MockKafkaProducer.return_value
        mock_producer_instance.send.return_value = MagicMock()

        # Call the method
        connector.send_producer_software_version()

        # Check format of DataItem
        MockDataItem.assert_called_once()
        self.assertEqual(mock_data_item_instance.tag, "ProducerSoftwareVersion")
        self.assertEqual(mock_data_item_instance.attrib['dataItemId'], "producer_software_version")
        self.assertEqual(mock_data_item_instance.text, connector.kafka_producer_version)
        self.assertEqual(mock_data_item_instance.attrib['type'], "Events")

        # Check if correct Kafka producer used
        MockKafkaProducer.assert_called_once_with(
            bootstrap_servers=['localhost:9092'],
            key_serializer=connector.mtc_dataItem_key_serializer,
            value_serializer=connector.mtc_dataItem_value_serializer
        )

        # Check sending of correct Kafka message
        mock_producer_instance.send.assert_called_once()
        mock_producer_instance = MockKafkaProducer.return_value
        self.assertEqual(mock_producer_instance.send.call_args[0], ('mtc_devices',))
        call_args = mock_producer_instance.send.call_args[1]
        self.assertEqual(call_args['key'], 'mocked_producer_uuid')
        self.assertEqual(call_args['value'], mock_data_item_instance)

    def test_get_agent_instance_file(self):
        """ Test agent instance file """
        connector = MockedMTCSourceConnector()
        connector.mtc_agent = "mocked_agent"
        expected_filename = ".mocked_agent"
        self.assertEqual(connector.get_agent_instance_file(), expected_filename)

        # check correct handling of ':'
        connector.mtc_agent = "mocked:agent:with:special:characters"
        expected_filename = ".mocked_agent_with_special_characters"
        self.assertEqual(connector.get_agent_instance_file(), expected_filename)

    @patch('builtins.open')
    @patch('os.path.isfile', return_value=True)
    @patch.object(MTCSourceConnector, 'get_agent_instance_file', return_value='mocked_filename')
    def test_get_latest_stored_agent_instance(self, mock_get_agent_instance_file, mock_isfile, mock_open):
        """ Test fetching of latest storded agent instance """
        # Mock the file content
        mock_open.return_value.readline.return_value = "123 456"

        connector = MockedMTCSourceConnector()
        instanceId, sequence = connector.get_latest_stored_agent_instance()

        # Assertions
        self.assertEqual(instanceId, 123)
        self.assertEqual(sequence, 456)

        # Test case when file does not exist
        mock_isfile.return_value = False
        instanceId, sequence = connector.get_latest_stored_agent_instance()
        self.assertEqual(instanceId, 0)
        self.assertEqual(sequence, 0)

    @patch('builtins.open')
    @patch.object(MTCSourceConnector, 'get_latest_stored_agent_instance', return_value=('123', '456'))
    def test_store_agent_instance(self, mock_get_latest_stored_agent_instance, mock_open):
        """ Test storing of latest agent instance """
        # Mock the mtc_header
        mtc_header = MagicMock()
        mtc_header.attrib = {'instanceId': '123', 'lastSequence': '456'}

        connector = MockedMTCSourceConnector()

        # Test case when instanceId and lastSequence are same
        connector.store_agent_instance(mtc_header)
        assert not mock_open.called

        # Test cases when instanceId and lastSequence are different
        mock_open.reset_mock()
        mtc_header.attrib = {'instanceId': '1234', 'lastSequence': '456'}
        connector.store_agent_instance(mtc_header)
        mock_open.assert_called_once()

        mock_open.reset_mock()
        mtc_header.attrib = {'instanceId': '123', 'lastSequence': '4567'}
        connector.store_agent_instance(mtc_header)
        mock_open.assert_called_once()
