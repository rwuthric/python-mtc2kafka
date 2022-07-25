import xml.etree.ElementTree as ET


class MTCDocumentMixing():
    """
    A mixin for reading MTConnect documents
    
    Children can redefine the XML namespace mtc_namespace if needed
    """
    
    mtc_namespace = 'urn:mtconnect.org:MTConnectStreams:1.7'
    
    def __init__(self):
        """ constructor """
        self.mtc_ns = {'mtc' : self.mtc_namespace}
    
    def sortChildrenBy(self, parent, attr):
        """ sorts children in parent by the attribute attr """
        parent[:] = sorted(parent, key=lambda child: child.get(attr))
    
    def get_mtc_header(self, xml_root):
        """ returns MTConnect header """
        return xml_root.find("mtc:Header", self.mtc_ns)
    
    def get_mtc_DeviceStreams(self, xml_root):
        """ returns all MTConnect Devices from an MTConnect document """
        return xml_root.find("mtc:Streams", self.mtc_ns)
    
    def get_dataItems(self, xml_node):
        """
        Returns MTConnect DataItems below xml_node
        Sorts the DataItems by sequence
        """
        dataItems = xml_node.findall(".//mtc:*[@sequence]", self.mtc_ns)
        self.sortChildrenBy(dataItems, 'sequence')
        return dataItems