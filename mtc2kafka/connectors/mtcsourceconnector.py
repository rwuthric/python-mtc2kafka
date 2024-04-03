import xml.etree.ElementTree as ET
import requests
import os
from datetime import timezone
from datetime import datetime
from time import sleep
from kafka import KafkaProducer
from kafka.errors import KafkaError
from mtc2kafka.core import MTCAgent
from mtc2kafka.core import MTCDocumentMixing
from mtc2kafka.core import MTCSerializersMixin
from mtc2kafka.core import ImproperlyConfigured


class DataItem():
    """
    An MTConnect DataItem
    """
    attrib = {}


class MTCSourceConnector(MTCAgent, MTCSerializersMixin, MTCDocumentMixing):
    """
    Kafka Source Connector to MTConnect
    Streams MTConnect data to Kafka topics

    Children have to define the following attributes:

     bootstrap_servers = ['kafka_server1:9092']  # List of Kafka bootstrap servers
     mtc_agent = 'my_agent:5000'                 # MTConnect agent
     kafka_producer_uuid                         # Kafka producer UUID

    Children can optionnaly redefine the following attributes:

     mtc_agent_uuid = none                       # UUID to be assigned to the agent (if none, does not reasing UUID)
     mtconnect_devices_topic = 'mtc_devices'     # Kafka topic to which messages are written
     max_attempts = 3                            # Maximal number of attemps in trying to reconnect to the MTConnect Agent
     attempt_delay = 10                          # Time delay (in sec) bettween reconnection attempts

    """

    agent_uuid = None
    bootstrap_servers = None
    mtc_agent_uuid = None
    kafka_producer_uuid = None

    mtconnect_devices_topic = 'mtc_devices'
    max_attempts = 3
    attempt_delay = 10

    # Producer software version
    kafka_producer_version = '1.1.0'

    # colors for print
    END = '\033[0m'
    DEVICE = '\033[34m'

    def __init__(self):
        """ Constructor """
        super(MTCSourceConnector, self).__init__()
        # Configuration validations
        if self.bootstrap_servers is None:
            raise ImproperlyConfigured("MTCSourceConnector requires the attribute 'bootstrap_servers' to be defined")
        if self.kafka_producer_uuid is None:
            raise ImproperlyConfigured("MTCSourceConnector requires the attribute 'kafka_producer_uuid' to be defined")
        if self.mtc_agent_uuid is None:
            self.mtc_agent_uuid = self.get_agent_uuid()

    def send_agent_availability(self, availability):
        """
        Sends Agent Availability message to Kafka
        """
        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                 key_serializer=self.mtc_dataItem_key_serializer,
                                 value_serializer=self.mtc_dataItem_value_serializer)

        dt_now = datetime.now(timezone.utc)
        item = DataItem()
        item.attrib['id'] = "agent_avail"
        item.attrib['tag'] = "Availability"
        item.attrib['attributes'] = {"timestamp": dt_now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}
        item.attrib['value'] = availability

        future = producer.send(self.mtconnect_devices_topic,
                               key=self.mtc_agent_uuid,
                               value=item)
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if request failed
            log.exception()
            pass

        producer.close()

    def send_producer_availability(self, availability):
        """
        Sends Kafka Producer Availability message to Kafka
        """
        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                 key_serializer=self.mtc_dataItem_key_serializer,
                                 value_serializer=self.mtc_dataItem_value_serializer)

        dt_now = datetime.now(timezone.utc)
        item = DataItem()
        item.attrib['id'] = "avail"
        item.attrib['tag'] = "Availability"
        item.attrib['attributes'] = {"timestamp": dt_now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}
        item.attrib['value'] = availability

        future = producer.send(self.mtconnect_devices_topic,
                               key=self.kafka_producer_uuid,
                               value=item)
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if request failed
            log.exception()
            pass

        producer.close()

    def send_producer_software_version(self):
        """
        Sends Kafka Producer Software Version Message to Kafka
        """
        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                 key_serializer=self.mtc_dataItem_key_serializer,
                                 value_serializer=self.mtc_dataItem_value_serializer)

        dt_now = datetime.now(timezone.utc)
        item = DataItem()
        item.attrib['id'] = "producer_software_version"
        item.attrib['tag'] = "ProducerSoftwareVersion"
        item.attrib['attributes'] = {"timestamp": dt_now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}
        item.attrib['value'] = self.kafka_producer_version

        future = producer.send(self.mtconnect_devices_topic,
                               key=self.kafka_producer_uuid,
                               value=item)
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if request failed
            log.exception()
            pass

        producer.close()

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

    def __stream(self, producer, verbose=True, interval=1000):
        """
        Private method to stream MTConnect DataItems to Kafka
        """
        # Computes start_sequence to stream from
        instanceID, sequence = self.get_latest_stored_agent_instance()
        if self.get_agent_instanceId() == instanceID:
            start_sequence = sequence + 1
        else:
            start_sequence = 1
            instanceID = self.get_agent_instanceId()

        agent_url = self.get_agent_baseUrl()
        if verbose:
            print("Streaming from " + agent_url + '&from=' + str(start_sequence))
            print("to Kafka       " + str(self.bootstrap_servers))
            print("Topic          " + self.mtconnect_devices_topic)
        req = requests.get(agent_url + '/sample?interval=' + str(interval) +
                           '&from=' + str(start_sequence), stream=True)

        # Determines devices handled by MTConnect agent
        if verbose:
            print("Devices handled by MTConnect agent:")
        for device in self.get_agent_devices():
            if "uuid" not in device.attrib:
                continue
            if verbose:
                print(self.DEVICE, device.tag, device.attrib, self.END)
        original_agent_uuid = self.get_agent_uuid()

        for line in req.iter_lines(delimiter=b"</MTConnectStreams>"):
            if line:
                resp = line.decode()
                ind = resp.find("<?xml")
                xml_data = resp[ind:] + "</MTConnectStreams>"
                root = ET.fromstring(xml_data)
                for device in self.get_mtc_DevicesStreams(root):
                    if verbose:
                        print(self.DEVICE, device.tag, device.attrib, self.END)
                    if "uuid" not in device.attrib:
                        continue
                    uuid = device.attrib['uuid']
                    if uuid == original_agent_uuid:
                        uuid = self.mtc_agent_uuid
                    for item in self.get_dataItems(device):
                        if verbose:
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
                    self.store_agent_instance(self.get_mtc_header_streams(root))

    def stream_mtc_dataItems_to_Kafka(self, verbose=True, interval=1000):
        """
        Streams MTConnect DataItems to Kafka
        Keys are the MTConnect UUID of the device

        Streams from the sequence of the agent that was last stored in Kafka
        In case the agent was restarted, will stream from the first sequence
        """
        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                 key_serializer=self.mtc_dataItem_key_serializer,
                                 value_serializer=self.mtc_dataItem_value_serializer)

        stream = True
        self.send_producer_software_version()
        self.send_producer_availability('AVAILABLE')
        while (stream):
            try:
                self.__stream(producer, verbose, interval)

            except requests.exceptions.ChunkedEncodingError:
                if verbose:
                    print("ERROR - MTConnect Agent got disconnected")
                self.send_agent_availability('UNAVAILABLE')
                attempts = 0
                while attempts < self.max_attempts:
                    if verbose:
                        print("  Trying again in %s sec" % self.attempt_delay)
                    sleep(self.attempt_delay)
                    try:
                        requests.get(self.get_agent_baseUrl())
                        if verbose:
                            print("Agent is back online")
                        # Note: Agent sends itself an 'AVAILABLE' status
                        # Do not call
                        # self.send_agent_availability('AVAILABLE')
                        attempts = self.max_attempts
                    except requests.exceptions.ConnectionError:
                        attempts += 1
                        if attempts == self.max_attempts:
                            print("Giving up")
                            stream = False

            except Exception as e:
                if verbose:
                    print("ERROR ", e.__class__.__name__)
                stream = False

        producer.close()
        self.send_producer_availability('UNAVAILABLE')