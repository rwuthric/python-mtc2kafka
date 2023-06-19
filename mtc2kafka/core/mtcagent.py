import xml.etree.ElementTree as ET
import requests
from mtc2kafka.core import MTCDocumentMixing
from mtc2kafka.core import ImproperlyConfigured


class MTCAgent(MTCDocumentMixing):
    """
    Python class to communicate with an MTConnect agent

    Children have to define the following attributes:

     mtc_agent = 'my_agent:5000'                 # MTConnect agent address

    """

    mtc_agent = None

    def __init__(self):
        """ Constructor """
        super(MTCAgent, self).__init__()
        # Configuration validations
        if self.mtc_agent is None:
            raise ImproperlyConfigured("MTCAgent requires the attribute 'mtc_agent' to be defined")
            
        # extracts XML devices namespace from /probe and /current request
        try:
            xml_probe = requests.get(self.get_agent_baseUrl() + '/probe').content
            xml_current = requests.get(self.get_agent_baseUrl() + '/current').content
        except requests.exceptions.ConnectionError:
            print("ERROR - Could not connect to agent")
            return '-1'
        root = ET.fromstring(xml_probe)
        start = root.tag.find('{') + 1
        end = root.tag.find('}', start)
        self.mtc_devices = {'mtc': root.tag[start:end]}
        root = ET.fromstring(xml_current)
        start = root.tag.find('{') + 1
        end = root.tag.find('}', start)
        self.mtc_streams = {'mtc': root.tag[start:end]}

    def get_agent_baseUrl(self):
        """ returns MTConnect agent base URL """
        return "http://" + self.mtc_agent

    def get_agent_uuid(self):
        """
        Returns the MTConnect agent uuid
        or -1 if connection could not be established or no agent found
        """
        try:
            xml_data = requests.get(self.get_agent_baseUrl() + '/probe').content
        except requests.exceptions.ConnectionError:
            print("ERROR - Could not connect to agent")
            return '-1'
        for device in self.get_agent_devices():
            if device.attrib['name'] == 'Agent':
                return device.attrib['uuid']
        return '-1'

    def get_agent_instanceId(self):
        """
        Returns the current MTConnect agent instanceId
        or -1 if connection could not be established
        """
        try:
            xml_data = requests.get(self.get_agent_baseUrl() + '/probe').content
        except requests.exceptions.ConnectionError:
            print("ERROR - Could not connect to agent")
            return -1
        root = ET.fromstring(xml_data)
        return int(self.get_mtc_header_devices(root).attrib['instanceId'])

    def get_agent_devices(self):
        """
        Returns devices (ElementTree) handled by the MTConnect agent
        or -1 if connection could not be established
        """
        try:
            xml_data = requests.get(self.get_agent_baseUrl() + '/probe').content
        except requests.exceptions.ConnectionError:
            print("ERROR - Could not connect to agent")
            return -1
        root = ET.fromstring(xml_data)
        return self.get_mtc_Devices(root)

    def get_agent_adapters(self):
        """
        Returns the adapters (ElementTree) connected to the MTConnect agent
        or -1 if connection could not be established
        """
        try:
            xml_data = requests.get(self.get_agent_baseUrl() + '/probe').content
        except requests.exceptions.ConnectionError:
            print("ERROR - Could not connect to agent")
            return '-1'
        root = ET.fromstring(xml_data)
        agent = root.find("mtc:Devices", self.mtc_devices).find("mtc:Agent", self.mtc_devices)
        return agent.find("mtc:Components", self.mtc_devices).find("mtc:Adapters", self.mtc_devices ).find("mtc:Components", self.mtc_devices)

    def get_agent_adapters_id(self):
        """
        Returns a list of ID of the adapters connected to the MTConnect agent
        or -1 if connection could not be established
        """
        try:
            requests.get(self.get_agent_baseUrl()).content
        except requests.exceptions.ConnectionError:
            print("ERROR - Could not connect to agent")
            return '-1'
        adapter_ids = []
        for adapter in self.get_agent_adapters():
            adapter_ids.append(adapter.attrib['id'])
        return adapter_ids