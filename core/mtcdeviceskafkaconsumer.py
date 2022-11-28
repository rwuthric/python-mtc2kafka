from kafka import KafkaConsumer
from mtc2kafka.core import MTCDeserializersMixin


class MTCDevicesKafkaConsumer(KafkaConsumer, MTCDeserializersMixin):
    """ KafkaConsumer for the MTConnect Devices stream """

    def __init__(self, *topic, **configs):
        """ Constructor """
        super(MTCDevicesKafkaConsumer, self).__init__("mtc_devices", **configs,
                                                      key_deserializer=self.mtc_key_deserializer,
                                                      value_deserializer=self.mtc_value_deserializer)