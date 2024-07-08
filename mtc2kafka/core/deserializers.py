import json

"""
Kafka deserializers for MTConnect DataItems
"""


def mtc_key_deserializer(key):
    """ Kafka key deserializer for MTConnect DataItems """
    return key.decode('utf-8')


def mtc_value_deserializer(value):
    """ Kafka value deserializer for MTConnect DataItems """
    return json.loads(value.decode('utf-8'))


"""
Mixin for Kafka deserializers of MTConnect DataItems
"""


class MTCDeserializersMixin():

    def mtc_key_deserializer(self, key):
        """ Kafka key deserializer for MTConnect DataItems """
        return mtc_key_deserializer(key)

    def mtc_value_deserializer(self, value):
        """ Kafka value serializer for MTConnect DataItems """
        return mtc_value_deserializer(value)
