import xml.etree.ElementTree as ET
import requests
import rsa
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
     privateKeyFile ='path/to/privateKey.em'     # Private key to sign Kafka headers
     publicKeyFile ='path/to/publicKey.em'       # Private key to validate KafkaHeaders

    """

    bootstrap_servers = None
    mtc_agent = None
    privateKeyFile = None
    publicKeyFile = None

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
        if self.privateKeyFile is None:
            raise ImproperlyConfigured("MTCSourceConnector requires the attribute 'privateKeyFile' to be defined")
        if self.publicKeyFile is None:
            raise ImproperlyConfigured("MTCSourceConnector requires the attribute 'publicKeyFile' to be defined")
        with open(self.privateKeyFile, mode='rb') as keyFile:
            self.privateKey = rsa.PrivateKey.load_pkcs1(keyFile.read())
        with open(self.publicKeyFile, mode='rb') as keyFile:
            self.publicKey = rsa.PublicKey.load_pkcs1(keyFile.read())

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
        return self.get_mtc_header(root).attrib['instanceId']

    def get_agent_devices(self):
        """ Returns a list of devices handled by the MTConnect agent """
        try:
            xml_data = requests.get(self.get_agent_baseUrl() + '/current').content
        except requests.exceptions.ConnectionError:
            print("ERROR - Could not connect to agent")
            return -1
        root = ET.fromstring(xml_data)
        return self.get_mtc_DeviceStreams(root)

    def get_agent_topic(self):
        """ Returns the Kafka topic of the MTConnect agent """
        return self.mtc_agent.replace(":", "_")

    def get_latest_stored_kafka_offset(self, topic):
        """
        Returns offset of latest recored stored in kafka
        Returns 0 if none was ever stored
        """
        part = TopicPartition(topic=topic, partition=0)
        consumer = KafkaConsumer(topic, bootstrap_servers=self.bootstrap_servers)

        # checks if topic actualy exists
        if topic not in consumer.topics():
            return 0
        # Checks if topic is an empty topic
        if consumer.beginning_offsets([part])[part] == consumer.end_offsets([part])[part]:
            return 0

        # gets latest offset
        consumer.poll()
        try:
            consumer.seek(part, consumer.end_offsets([part])[part]-1)
            last_message = next(consumer)
            offset = last_message.offset
        except AssertionError:
            offset = 0
        consumer.close()
        return offset

    def get_latest_stored_agent_instance(self):
        """
        Returns the latest agent instanceId and sequence stored in Kafka
        If no entry is found returns (0, 0)
        """
        agent_topic = self.get_agent_topic()
        part = TopicPartition(topic=agent_topic, partition=0)
        consumer = KafkaConsumer(agent_topic, bootstrap_servers=self.bootstrap_servers)
        if agent_topic not in consumer.topics():
            prod = KafkaProducer(bootstrap_servers=self.bootstrap_servers)
            future = prod.send(self.get_agent_topic(), partition=0,
                               key=str.encode('0'),
                               value=str.encode('0'))
            try:
                future.get(timeout=10)
            except KafkaError:
                # Decide what to do if request failed
                log.exception()
                pass
            prod.close()
            return 0, 0

        # Checks if agent_topic is an empty topic
        if consumer.beginning_offsets([part])[part] == consumer.end_offsets([part])[part]:
            return 0, 0

        # poll() is needed in order to force assigning partitions
        # Reason: KafkaConsumer constructor is asynchronous. When calling seek()
        #         it is likely that the partition is not yet assigned
        consumer.poll()
        try:
            consumer.seek(part, consumer.end_offsets([part])[part]-1)
            last_message = next(consumer)
            instanceId = last_message.key.decode('utf-8')
            sequence = int(last_message.value.decode('utf-8'))
        except AssertionError:
            instanceId = 0
            sequence = 0
        consumer.close()
        return instanceId, sequence

    def store_agent_instance(self, mtc_header):
        """
        Stores agent instanceId and lastSequence to Kafka
        Stores information only if different than latest stored info in Kafka
        Forces the use of partition 0 in the agent topic
        """
        instanceId = mtc_header.attrib['instanceId']
        lastSequence = mtc_header.attrib['lastSequence']
        prev_instanceId, prev_lastSequence = self.get_latest_stored_agent_instance()
        if (instanceId != prev_instanceId) or (lastSequence != str(prev_lastSequence)):
            prod = KafkaProducer(bootstrap_servers=self.bootstrap_servers)
            future = prod.send(self.get_agent_topic(), partition=0,
                               key=str.encode(instanceId),
                               value=str.encode(lastSequence))
            try:
                future.get(timeout=10)
            except KafkaError:
                # Decide what to do if request failed
                log.exception()
                pass
            prod.close()

    def stream_mtc_dataItems_to_Kafka(self, interval=1000):
        """
        Streams MTConnect DataItems to Kafka to their respective topics
        Topic is the MTConnect UUID of the device
        Forces the use of partition 0 in the topic
        Signs the message

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
        req = requests.get(agent_url + '/sample?interval=' + str(interval) +
                           '&from=' + str(start_sequence), stream=True)

        # Determines devices handled by MTConnect agent
        print("Devices handled by MTConnect agent:")
        last_offset = {}
        for device in self.get_agent_devices():
            if "uuid" not in device.attrib:
                continue
            print(self.DEVICE, device.tag, device.attrib, self.END)
            uuid = device.attrib['uuid']
            last_offset[uuid] = str(self.get_latest_stored_kafka_offset(uuid)).encode()
            print(last_offset[uuid])

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
                        # message = str.encode(self.mtc_agent) + str.encode(str(instanceID)) + str.encode(item.attrib['sequence'])
                        header = [("agent", str.encode(self.mtc_agent)),
                                  ("instanceID", str.encode(str(instanceID))),
                                  ("sequence", str.encode(item.attrib['sequence'])),
                                  ("signature", rsa.sign(last_offset[uuid], self.privateKey, 'SHA-1'))]
                        future = producer.send(uuid, partition=0,
                                               headers=header,
                                               key=item,
                                               value=item)
                        try:
                            record_metadata = future.get(timeout=10)
                            last_offset[uuid] = str(record_metadata.offset).encode()
                            print(last_offset[uuid])
                        except KafkaError:
                            # Decide what to do if request failed
                            log.exception()
                            pass
                self.store_agent_instance(self.get_mtc_header(root))

        producer.close()