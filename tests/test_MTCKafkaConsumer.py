from unittest import TestCase
from unittest.mock import patch

from mtc2kafka.core import MTCKafkaConsumer


class TestMTCKafkaConsumer(TestCase):
    """
    Tests for MTCKafkaConsumer:
    """
           
    @patch("kafka.KafkaConsumer.__init__", return_value=None)
    def test_MTCKafkaConsumer_key_deserializer(self, mock_KafkaConsumer_init):
        """ Tests if key_deserializer is correctly defined """
        con = MTCKafkaConsumer()
        args = mock_KafkaConsumer_init.call_args
        assert(args[1]['key_deserializer'] == con.mtc_key_deserializer)
        
    @patch("kafka.KafkaConsumer.__init__", return_value=None)
    def test_MTCKafkaConsumer_value_deserializer(self, mock_KafkaConsumer_init):
        """ Tests if value_deserializer is correctly defined """
        con = MTCKafkaConsumer()
        args = mock_KafkaConsumer_init.call_args
        assert(args[1]['value_deserializer'] == con.mtc_value_deserializer)