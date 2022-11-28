import xml.etree.ElementTree as ET
import requests
import os
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import TopicPartition
from kafka.errors import KafkaError
from mtc2kafka.core import MTCDocumentMixing
from mtc2kafka.core import MTCSerializersMixin
from mtc2kafka.core import ImproperlyConfigured


class MTCSourceConnector(MTCSerializersMixin, MTCDocumentMixing):
    """
    Kafka Source Connector to MTConnect
    Streams MTConnect data to Kafka topics

    Children have to define the following attributes:

     bootstrap_servers = ['kafka_server1:9092']  # List of Kafka bootstrap servers
     mtc_agent = 'my_agent:5000'                 # MTConnect agent

    Children can optionnaly redifine the following attributes:

     mtconnect_devices_topic = 'mtc_devices'

    """

    bootstrap_servers = None
    mtc_agent = None

    mtconnect_devices_topic = 'mtc_devices'

    # colors for print
    END = '\033[0m'
    DEVICE = '\033[34m'

    def __init__(self):
        """ Constructor """
        super(MTCSourceConnector, self).__init__()
        # Configuration validations
        if self.bootstrap_servers is None:
            raise ImproperlyConfigured("MTCSourceConnector requires the attribute 'bootstrap_servers' to be defined")
        if self.mtc_agent is None:
            raise ImproperlyConfigured("MTCSourceConnector requires the attribute 'mtc_agent' to be defined")

    def get_agent_baseUrl(self):
        """ returns MTConnect agent base URL """
        return "http://" + self.mtc_agent
 
    def get_agent_instanceId(self):
        """
        Returns the current MTConnect agent instanceId
        or -1 if connection could not be established
        """
        try:
            xml_data = requests.get(self.get_agent_baseUrl() + '/current').content
        except requests.exceptions.ConnectionError:
            print("ERROR - Could not connect to agent")
            return -1
        root = ET.fromstring(xml_data)
        return int(self.get_mtc_header(root).attrib['instanceId'])

    def get_agent_devices(self):
        """ Returns a list of devices handled by the MTConnect agent """
        try:
            xml_data = requests.get(self.get_agent_baseUrl() + '/current').content
        except requests.exceptions.ConnectionError:
            print("ERROR - Could not connect to agent")
            return -1
        root = ET.fromstring(xml_data)
        return self.get_mtc_DeviceStreams(root)

    def get_agent_instance_file(self):
        """ Returns the local file used to store latest agent instance and sequence """
        return "." + self.mtc_agent.replace(":", "_")

    def get_latest_stored_agent_instance(self):
        """
        Returns the latest agent instanceId and sequence stored in Kafka
        If no entry is found returns (0, 0)
        """
        filename = self.get_agent_instance_file()
        if not os.path.isfile(filename):
            return 0, 0
        f = open(self.get_agent_instance_file(), "r")
        line = f.readline()
        instanceId, sequence = line.split(' ')
        f.close()
        return int(instanceId), int(sequence)
        
    def store_agent_instance(self, mtc_header):
        """
        Stores agent instanceId and lastSequence to local file
        Stores information only if different than latest stored info in Kafka
        """
        instanceId = mtc_header.attrib['instanceId']
        lastSequence = mtc_header.attrib['lastSequence']
        prev_instanceId, prev_lastSequence = self.get_latest_stored_agent_instance()
        if (instanceId != prev_instanceId) or (lastSequence != str(prev_lastSequence)):
            f = open(self.get_agent_instance_file(), "w")
            f.write("%s %s\n" % (instanceId, lastSequence))
            f.close()

    def stream_mtc_dataItems_to_Kafka(self, interval=1000):
        """
        Streams MTConnect DataItems to Kafka
        Keys are the MTConnect UUID of the device

        Streams from the sequence of the agent that was last stored in Kafka
        In case the agent was restarded since, will stream from the first sequence
        """
        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                 key_serializer=self.mtc_dataItem_key_serializer,
                                 value_serializer=self.mtc_dataItem_value_serializer)

        # Computes start_sequence to stream from
        instanceID, sequence = self.get_latest_stored_agent_instance()
        if self.get_agent_instanceId() == instanceID:
            start_sequence = sequence + 1
        else:
            start_sequence = 1
            instanceID = self.get_agent_instanceId()

        agent_url = self.get_agent_baseUrl()
        print("Streaming from " + agent_url + '&from=' + str(start_sequence))
        print("to Kafka       " + str(self.bootstrap_servers))
        print("Topic          " + self.mtconnect_devices_topic)
        req = requests.get(agent_url + '/sample?interval=' + str(interval) +
                           '&from=' + str(start_sequence), stream=True)

        # Determines devices handled by MTConnect agent
        print("Devices handled by MTConnect agent:")
        for device in self.get_agent_devices():
            if "uuid" not in device.attrib:
                continue
            print(self.DEVICE, device.tag, device.attrib, self.END)
            uuid = device.attrib['uuid']

        for line in req.iter_lines(delimiter=b"</MTConnectStreams>"):
            if line:
                resp = line.decode()
                ind = resp.find("<?xml")
                xml_data = resp[ind:] + "</MTConnectStreams>"
                root = ET.fromstring(xml_data)
                for device in self.get_mtc_DeviceStreams(root):
                    print(self.DEVICE, device.tag, device.attrib, self.END)
                    if "uuid" not in device.attrib:
                        continue
                    uuid = device.attrib['uuid']
                    for item in self.get_dataItems(device):
                        print("  " + self.mtc_dataItem_value_serializer(item).decode('utf-8'))
                        header = [("agent", str.encode(self.mtc_agent)),
                                  ("instanceID", str.encode(str(instanceID))),
                                  ("sequence", str.encode(item.attrib['sequence']))]
                        future = producer.send(self.mtconnect_devices_topic,
                                               headers=header,
                                               key=uuid,
                                               value=item)
                        try:
                            record_metadata = future.get(timeout=10)
                        except KafkaError:
                            # Decide what to do if request failed
                            log.exception()
                            pass
                self.store_agent_instance(self.get_mtc_header(root))

        producer.close()