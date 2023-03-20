class MTCSerializersMixin():
    """ A mixin for Kafka MTConnect DataItems serializers """

    def mtc_dataItem_key_serializer(self, uuid):
        """ Kafka key serializer for MTConnect DataItems """
        return str.encode(uuid)

    def mtc_dataItem_value_serializer(self, dataItem):
        """ Kafka value serializer for MTConnect DataItems """
        tmp_dic = dataItem.attrib.copy()
        del tmp_dic['dataItemId']
        del tmp_dic['sequence']
        ret_dic = {}
        ret_dic['id'] = dataItem.attrib['dataItemId']
        ret_dic['tag'] = dataItem.tag
        ret_dic['attributes'] = tmp_dic
        ret_dic['value'] = dataItem.text
        return str.encode(str(ret_dic))