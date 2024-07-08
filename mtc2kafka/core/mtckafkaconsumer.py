from kafka import KafkaConsumer
from mtc2kafka.core import MTCDeserializersMixin


class MTCKafkaConsumer(KafkaConsumer, MTCDeserializersMixin):
    """ KafkaConsumer with MTConnect deserializer """

    def __init__(self, *topic, **configs):
        """ Constructor """
        super(MTCKafkaConsumer, self).__init__(*topic, **configs,
                                               key_deserializer=self.mtc_key_deserializer,
                                               value_deserializer=self.mtc_value_deserializer)
