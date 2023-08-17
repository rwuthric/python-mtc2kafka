"""
Kafka MTConnect DataItems serializers
"""

import json


def mtc_dataItem_key_serializer(key):
    """  Kafka key serializer for MTConnect DataItems """
    return str.encode(key)


def mtc_dataItem_value_serializer(dataItem):
    """ Kafka value serializer for MTConnect DataItems """
    tmp_dic = dataItem.attrib.copy()
    del tmp_dic['dataItemId']
    del tmp_dic['sequence']
    ret_dic = {}
    ret_dic['id'] = dataItem.attrib['dataItemId']
    ret_dic['tag'] = dataItem.tag
    ret_dic['attributes'] = tmp_dic
    ret_dic['value'] = dataItem.text
    return str.encode(json.dumps(ret_dic))


"""
Mixin for Kafka MTConnect DataItems serializers
"""

class MTCSerializersMixin():
    """ A mixin for Kafka MTConnect DataItems serializers """

    def mtc_dataItem_key_serializer(self, uuid):
        """ Kafka key serializer for MTConnect DataItems """
        return mtc_dataItem_key_serializer(uuid)

    def mtc_dataItem_value_serializer(self, dataItem):
        """ Kafka value serializer for MTConnect DataItems """
        return mtc_dataItem_value_serializer(dataItem)