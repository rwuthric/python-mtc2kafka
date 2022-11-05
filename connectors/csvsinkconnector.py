from pathlib import Path
import os.path
import ast
from kafka import KafkaConsumer, TopicPartition
from mtc2kafka.core import MTCDeserializersMixin, ImproperlyConfigured


class CSVSinkConnector(MTCDeserializersMixin):
    """
    Kafka sink connector to CSV files
    Saves Kafka messages with a key in 'keys' to CSV files in 'storageFolder'

    Children have to define the following attributes:

     bootstrap_servers = ['kafka_server1:9092']   # List of Kafka bootstrap servers
     topic = 'my_topic'                           # Kafka topic to read from
     keys = ['key1' , 'key2']                     # List of keys that will be saved
     meta_data = ['key3', 'key4']                 # List of keys that will be saved as meta data
     storageFolder = '/path/to/storage/folder'    # Full path where the data should be saved to
     start_key = 'start'                          # Key that will trigger start of saving
     stop_key = 'stop'                            # Key that will trigger stop of saving


    Children can optionnaly define the following attributes:

     history_file = 'name_of_file'                # Full name (including path) of file for history data
                                                  # If none is defined will assign one based on class name in current path
    """

    # Attributes to be defined by children
    bootstrap_servers = None
    topic = None
    keys = None
    meta_data = None
    storageFolder = None
    start_key = None
    stop_key = None
    history_file = None

    # Internal constants
    SAVE_MODE_SINGLE_FILE = 1
    SAVE_MODE_MULTIPLE_FILES = 2

    def __init__(self):
        """ Constructor """
        # Configuration validations
        if self.bootstrap_servers is None:
            raise ImproperlyConfigured("CSVSinkConnector requires the attribute 'bootstrap_servers' to be defined")
        if self.topic is None:
            raise ImproperlyConfigured("CSVSinkConnector requires the attribute 'topic' to be defined")
        if self.storageFolder is None:
            raise ImproperlyConfigured("CSVSinkConnector requires the attribute 'storageFolder' to be defined")
        if self.start_key is None:
            raise ImproperlyConfigured("CSVSinkConnector requires the attribute 'start_key' to be defined")
        if self.stop_key is None:
            raise ImproperlyConfigured("CSVSinkConnector requires the attribute 'topic' to be defined")
        # Initial configurations
        self.files = None
        if self.history_file is None:
            self.history_file = type(self).__name__
        self.load_current()
        print(str(self.current))

    def store_to_single_file(self, mode='w'):
        """ Stores DataItems with a key in keys to a single file in storageFolder """       
        self.store(self.SAVE_MODE_SINGLE_FILE, mode)
    
    def store_to_multiple_files(self, mode='w'):
        """ Stores DataItems with a key in keys to mutiple files in storageFolder """           
        self.store(self.SAVE_MODE_MULTIPLE_FILES, mode)

    def save_current(self):
        """ Saves current to history_file """
        f = open(self.history_file, 'w')
        f.write(str(self.current))
        f.close()
        
    def load_current(self):
        """ Loads current from history_file """
        self.current = {}
        if os.path.isfile(self.history_file):
            f = open(self.history_file, 'r')
            self.current = ast.literal_eval(f.read())
            f.close()
        for key in self.meta_data:
            if key not in self.current:
                self.current[key] = 'UNAVAILABLE'
        for key in self.keys:
            if key not in self.current:
                self.current[key] = 'UNAVAILABLE'
        if 'timestamp' not in self.current:
            self.current['timestamp'] = 'UNAVAILABLE'
        self.previous = self.current.copy()

    def get_storage_folder(self):
        """ Returns folder where data will be stored and creates it if needed """
        folder = os.path.join(self.storageFolder, self.current[self.start_key])
        Path(folder).mkdir(parents=True, exist_ok=True)
        return folder

    def open_save_files(self, save_mode, mode='w'):
        """ Opens files for saving """
        if save_mode == self.SAVE_MODE_SINGLE_FILE:
            self.files = open(self.get_storage_folder() + '/data.csv', mode)
            if mode == 'w':
                # Adds header
                self.files.write("timestamp")
                for key in self.keys:
                    self.files.write(",%s" % (key))
                self.files.write("\n")
                self.files.flush()
        if save_mode == self.SAVE_MODE_MULTIPLE_FILES:
            self.files = {}
            for key in self.keys:
                f = open(self.get_storage_folder() + '/' + key + '.csv', mode)
                self.files[key] = f
                if mode == 'w':
                    # Adds header
                    self.files[key].write("%s,%s\n" % ("timestamp", key))
                self.files[key].write("%s,%s\n" % (self.current['timestamp'], self.current[key]))
                self.files[key].flush()
                    
    def close_save_files(self, save_mode):
        """ Closes files for saving """
        if self.files is None:
            # files were never opened or already closed once
            return
        if save_mode == self.SAVE_MODE_SINGLE_FILE:
            self.files.write(self.current['timestamp'])
            for key in self.keys:
                self.files.write(",%s" % (self.current[key]))
            self.files.write("\n")
            self.files.close()
        if save_mode == self.SAVE_MODE_MULTIPLE_FILES:
            for key in self.keys:
                self.files[key].close()
        self.files = None

    def save_meta_data(self, mode='w'):
        """ Saves meta_data """
        f = open(os.path.join(self.get_storage_folder(), 'meta_data.csv'), mode)
        for key in self.meta_data:
            f.write("%s,%s\n" % (key, self.current[key]))
        f.close()

    def store(self, save_mode, mode='w'):
        """ Stores DataItems with a key in keys to the storageFolder """
        print("Storing dataItems " + str(self.keys) + " from topic " + self.topic)
        print("Destination : " + self.storageFolder)

        part = TopicPartition(topic=self.topic, partition=0)
        consumer = KafkaConsumer(self.topic,
                                 bootstrap_servers=self.bootstrap_servers,
                                 auto_offset_reset='latest',
                                 key_deserializer=self.mtc_key_deserializer,
                                 value_deserializer=self.mtc_value_deserializer)

        if 'kafka_offset' in self.current:
            consumer.topics()
            consumer.poll()
            consumer.seek(part, self.current['kafka_offset']+1)

        record = False
        for message in consumer:
            print("%s\t%s=%s" % (message.headers, message.key, message.value['value']))

            # keeps history of meta_data and data
            if message.key in self.meta_data or message.key in self.keys:
                self.previous = self.current.copy()
                self.current[message.key] = message.value['value']
                self.current['kafka_offset'] = message.offset
                self.current['timestamp'] = message.value['attributes']['timestamp']
                self.save_current()

            # signal for stop recording recieved
            if message.key == self.stop_key:
                print("Stop storing")
                self.close_save_files(save_mode)
                record = False

            # signal for start recording recieved
            if message.key == self.start_key and message.value['value'] != 'UNAVAILABLE':
                print("Start storing")
                self.save_meta_data(mode)
                self.open_save_files(save_mode, mode)
                record = True

            # records data
            if message.key in self.keys and record:
                if save_mode == self.SAVE_MODE_MULTIPLE_FILES:
                    self.files[message.key].write("%s,%s\n" % (message.value['attributes']['timestamp'], message.value['value']))
                    self.files[message.key].flush()
                if save_mode == self.SAVE_MODE_SINGLE_FILE:
                    if self.previous['timestamp'] != message.value['attributes']['timestamp']:
                        self.files.write(self.previous['timestamp'])
                        for key in self.keys:
                            self.files.write(",%s" % (self.previous[key]))
                        self.files.write("\n")
                        self.files.flush()
