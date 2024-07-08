class MTCDocumentMixing():
    """
    A mixin for reading MTConnect documents

    Children can redefine the XML namespaces mtc_streams_namespace and mtc_devices_namespace if needed
    """

    mtc_streams_namespace = 'urn:mtconnect.org:MTConnectStreams:1.7'
    mtc_devices_namespace = 'urn:mtconnect.org:MTConnectDevices:1.7'

    def __init__(self):
        """ constructor """
        self.mtc_streams = {'mtc': self.mtc_streams_namespace}
        self.mtc_devices = {'mtc': self.mtc_devices_namespace}

    def sortChildrenBy(self, parent, attr):
        """ sorts children in parent by the attribute attr """
        parent[:] = sorted(parent, key=lambda child: child.get(attr))

    def get_mtc_header_devices(self, xml_root):
        """ returns MTConnect header from a MTConnect device file """
        return xml_root.find("mtc:Header", self.mtc_devices)

    def get_mtc_header_streams(self, xml_root):
        """ returns MTConnect header from a MTConnect stream """
        return xml_root.find("mtc:Header", self.mtc_streams)

    def get_mtc_DevicesStreams(self, xml_root):
        """ returns all MTConnect Devices from an MTConnect Streams document """
        return xml_root.find("mtc:Streams", self.mtc_streams)

    def get_mtc_Devices(self, xml_root):
        """ returns all MTConnect Devices from an MTConnect Device document """
        return xml_root.find("mtc:Devices", self.mtc_devices)

    def get_dataItems(self, xml_node):
        """
        Returns MTConnect DataItems below xml_node
        Sorts the DataItems by sequence
        """
        dataItems = xml_node.findall(".//*[@sequence]")
        self.sortChildrenBy(dataItems, 'sequence')
        return dataItems
