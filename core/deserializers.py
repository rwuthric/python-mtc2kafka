import ast

class MTCDeserializersMixin():
    """ A mixin for Kafka MTConnect DataItems deserializers """
    
    def mtc_key_deserializer(self, key):
        """ Kafka key deserializer for MTConnect DataItems """
        return key.decode('utf-8')

    def mtc_value_deserializer(self, value):
        """ Kafka value serializer for MTConnect DataItems """
        return ast.literal_eval(value.decode('utf-8'))