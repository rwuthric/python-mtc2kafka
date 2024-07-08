import unittest
from datetime import datetime, timezone
from unittest.mock import patch
from mtc2kafka.connectors.mtcsourceconnector import DataItem


class TestDataItem(unittest.TestCase):
    """
    Tests for DataItem
    """

    @patch('mtc2kafka.connectors.mtcsourceconnector.datetime')
    def test_init(self, mock_datetime):
        # Mock the current time
        mocked_now = datetime(2024, 7, 8, 12, 0, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = mocked_now
        mock_datetime.now.side_effect = lambda tz: mocked_now if tz else datetime.now()

        data_item = DataItem()

        # Check if tag and text are initialized correctly
        self.assertEqual(data_item.tag, '')
        self.assertEqual(data_item.text, '')

        # Check if attrib dictionary is initialized correctly
        self.assertEqual(data_item.attrib['dataItemId'], '')
        self.assertEqual(data_item.attrib['sequence'], '')

        # Check if timestamp is set correctly
        expected_timestamp = mocked_now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        self.assertEqual(data_item.attrib['timestamp'], expected_timestamp)

    @patch('mtc2kafka.connectors.mtcsourceconnector.datetime')
    def test_set_timestamp(self, mock_datetime):
        """ Test if timestamp set correctly """
        data_item = DataItem()

        # Mock the current time
        mocked_now = datetime(2024, 7, 8, 12, 0, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = mocked_now

        data_item.set_timestamp()

        # Check if timestamp is updated correctly
        expected_timestamp = mocked_now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        actual_timestamp = data_item.attrib['timestamp']
        self.assertEqual(actual_timestamp, expected_timestamp)
