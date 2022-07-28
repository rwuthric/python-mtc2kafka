import xml.etree.ElementTree as ET
import requests
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import TopicPartition
from mtc2kafka.core import MTCDocumentMixing, MTCSerializersMixin, ImproperlyConfigured

 

class MTCSourceConnector(MTCSerializersMixin, MTCDocumentMixing):
    """
    Kafka Source Connector to MTConnect
    Streams MTConnect data to Kafka topics
    
    Children have to define the following attributes:
    
     bootstrap_servers = ['kafka_server1:9092']  # List of Kafka bootstrap servers
     mtc_agent = 'my_agent:5000'                 # MTConnect agent
    
    """
    
    bootstrap_servers = None
    mtc_agent = None
    
    # colors for print
    END = '\033[0m'
    DEVICE = '\033[34m'
    
    
    def __init__(self):
        """ Constructor """
        super(MTCSourceConnector, self).__init__()
        # Configuration validations
        if self.bootstrap_servers == None:
            raise ImproperlyConfigured("MTCSourceConnector requires the attribute 'bootstrap_servers' to be defined")
        if self.mtc_agent == None:
            raise ImproperlyConfigured("MTCSourceConnector requires the attribute 'mtc_agent' to be defined")
        
                 
    def get_agent_baseUrl(self):
        """ returns MTConnect agent base URL """
        return "http://" + self.mtc_agent
 
 
    def get_agent_instanceId(self):
        """ Returns the current MTConnect agent instanceId or -1 if no connection could be established """
        try:
            xml_data = requests.get(self.get_agent_baseUrl() + '/current').content
        except requests.exceptions.ConnectionError:
            print("ERROR - Could not connect to agent")
            return -1
        root = ET.fromstring(xml_data)
        return self.get_mtc_header(root).attrib['instanceId']
  
  
    def get_agent_topic(self):
        """ Returns the Kafka topic of the MTConnect agent """
        return self.mtc_agent.replace(":", "_")
    
    
    def get_latest_stored_agent_instance(self):
        """
        Returns the latest agent instanceId and sequence stored in Kafka
        If no entry is found returns (0, 0)
        """
        agent_topic = self.get_agent_topic()
        part = TopicPartition(topic=agent_topic, partition=0)
        consumer = KafkaConsumer(agent_topic, bootstrap_servers=self.bootstrap_servers)
        # poll() is needed in order to force assigning partitions
        # Reason: KafkaConsumer constructor is asynchronous. When calling seek() it is likely
        #         the partition is not yet assigned
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
        """
        instanceId = mtc_header.attrib['instanceId']
        lastSequence = mtc_header.attrib['lastSequence']
        prev_instanceId, prev_lastSequence = self.get_latest_stored_agent_instance()
        if (instanceId!=prev_instanceId) or (lastSequence!=str(prev_lastSequence)):
            prod = KafkaProducer(bootstrap_servers=self.bootstrap_servers)
            future = prod.send(self.get_agent_topic(), partition=0,
                               key=str.encode(instanceId),
                               value=str.encode(lastSequence))
            try:
                record_metadata = future.get(timeout=10)
            except KafkaError:
                # Decide what to do if produce request failed...
                log.exception()
                pass
            prod.close()
            
            
    def stream_mtc_dataItems_to_Kafka(self, interval=1000):
        """ Streams MTConnect DataItems to Kafka to their respective topics """
        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                 key_serializer=self.mtc_dataItem_key_serializer,
                                 value_serializer=self.mtc_dataItem_value_serializer)
        
        # computes start_sequence
        instanceID, sequence = self.get_latest_stored_agent_instance()
        if self.get_agent_instanceId()==instanceID:
            start_sequence = sequence + 1
        else:
            start_sequence = 1
            
        agent_url = self.get_agent_baseUrl()
        print("Streaming from " + agent_url + '&from=' + str(start_sequence))
        print("to Kafka       " + str(self.bootstrap_servers))
        req = requests.get(agent_url + '/sample?interval=' + str(interval) +
                           '&from=' + str(start_sequence), stream=True)
        
        for line in req.iter_lines(delimiter=b"</MTConnectStreams>"):
            if line:
                resp = line.decode()
                ind = resp.find("<?xml")
                xml_data = resp[ind:] + "</MTConnectStreams>"
                root = ET.fromstring(xml_data)
                for device in self.get_mtc_DeviceStreams(root):
                    print(self.DEVICE, device.tag, device.attrib, self.END)
                    if not "uuid" in device.attrib:
                        continue
                    uuid = device.attrib['uuid']
                    for item in self.get_dataItems(device):
                        print("  " + self.mtc_dataItem_value_serializer(item).decode('utf-8'))
                        header = [("agent" , str.encode(self.mtc_agent)),
                                  ("instanceID" , str.encode(str(instanceID))),
                                  ("sequence" , str.encode(item.attrib['sequence']))]
                        future = producer.send(uuid, partition=0, headers=header, key=item, value=item)
                        try:
                            record_metadata = future.get(timeout=10)
                        except KafkaError:
                            # Decide what to do if produce request failed...
                            log.exception()
                            pass
                self.store_agent_instance(self.get_mtc_header(root))
                
        producer.close()
    