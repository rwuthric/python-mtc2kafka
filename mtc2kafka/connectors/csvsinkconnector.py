from pathlib import Path
import os.path
import ast
from kafka import KafkaConsumer, TopicPartition
from mtc2kafka.core import MTCDeserializersMixin, ImproperlyConfigured


class CSVSinkConnector(MTCDeserializersMixin):
    """
    Kafka sink connector to CSV files
    Saves Kafka messages containing MTConnect DataItems
    with a key in 'keys' to CSV files in 'storageFolder'

    Children have to define the following attributes:

     bootstrap_servers = ['kafka_server1:9092']   # List of Kafka bootstrap servers
     deviceUUID = 'device_uuid'                   # MTConnect Device UUID to read from
     ids = ['id1' , 'id2']                        # List of ids of MTConnect DataItems that will be saved
     meta_data = ['id3', 'id4']                   # List of ids of MTConnect DataItems that will be saved as meta data
     storageFolder = '/path/to/storage/folder'    # Full path where the data will be saved to
     start_id = 'start'                           # ID that will trigger start of saving
                                                  # Its value is used to create a sub-folder 
                                                  # within 'storageFolder'
     stop_id = 'stop'                             # ID that will trigger stop of saving


    Children can optionnaly define the following attributes:

     history_file = 'name_of_file'                # Full name (including path) of file for history data
                                                  # If none is defined will assign one in current path based on class name
    """

    # Attributes to be defined by children
    bootstrap_servers = None
    deviceUUID = None
    ids = None
    meta_data = None
    storageFolder = None
    start_id = None
    stop_id = None
    history_file = None

    # Internal constants
    SAVE_MODE_SINGLE_FILE = 1
    SAVE_MODE_MULTIPLE_FILES = 2

    def __init__(self):
        """ Constructor """
        # Configuration validations
        if self.bootstrap_servers is None:
            raise ImproperlyConfigured("CSVSinkConnector requires the attribute 'bootstrap_servers' to be defined")
        if self.deviceUUID is None:
            raise ImproperlyConfigured("CSVSinkConnector requires the attribute 'deviceUUID' to be defined")
        if self.storageFolder is None:
            raise ImproperlyConfigured("CSVSinkConnector requires the attribute 'storageFolder' to be defined")
        if self.start_id is None:
            raise ImproperlyConfigured("CSVSinkConnector requires the attribute 'start_id' to be defined")
        if self.stop_id is None:
            raise ImproperlyConfigured("CSVSinkConnector requires the attribute 'stop_id' to be defined")
        # Initial configurations
        self.files = None
        if self.history_file is None:
            self.history_file = type(self).__name__
        self.load_current()
        print(str(self.current))

    def store_to_single_file(self, mode='w'):
        """
        Stores DataItems with an ID in ids to a single file in storageFolder
        If mode='w' (default) an new file is created
        If mode='a' data are appended to existing file
        """
        self.store(self.SAVE_MODE_SINGLE_FILE, mode)

    def store_to_multiple_files(self, mode='w'):
        """
        Stores DataItems with an ID in ids to mutiple files in storageFolder
        If mode='w' (default) an new file is created for each ID
        If mode='a' data are appended to existing files
        """
        self.store(self.SAVE_MODE_MULTIPLE_FILES, mode)

    def print_message(self, message):
        """
        Prints information in message
        Can be overiden by children to presonalize format
        """
        print("%s\t%s=%s" % (message.headers, message.value['id'], message.value['value']))

    def load_current(self):
        """
        Loads 'current' from history_file
        'current' contains the state of the MTConnect device in form of:
         - the current values of all ids in 'ids'
         - the timestamp of this state
        """
        self.current = {}
        if os.path.isfile(self.history_file):
            f = open(self.history_file, 'r')
            self.current = ast.literal_eval(f.read())
            f.close()
        for ID in self.meta_data:
            if ID not in self.current:
                self.current[ID] = 'UNAVAILABLE'
        for ID in self.ids:
            if ID not in self.current:
                self.current[ID] = 'UNAVAILABLE'
        if 'timestamp' not in self.current:
            self.current['timestamp'] = 'UNAVAILABLE'
        self.previous = self.current.copy()

    def save_current(self):
        """ Saves 'current' to history_file """
        f = open(self.history_file, 'w')
        f.write(str(self.current))
        f.close()

    def get_storage_folder(self):
        """ 
        Returns folder where data will be stored and creates it if needed
        The recieved value of the 'start_id' is used to create
        a sub-folder within 'storageFolder'
        """
        folder = os.path.join(self.storageFolder, self.current[self.start_id])
        Path(folder).mkdir(parents=True, exist_ok=True)
        return folder

    def open_save_files(self, save_mode, mode='w'):
        """ Opens files for saving """
        if save_mode == self.SAVE_MODE_SINGLE_FILE:
            self.files = open(self.get_storage_folder() + '/data.csv', mode)
            if mode == 'w':
                # Adds header
                self.files.write("timestamp")
                for ID in self.ids:
                    self.files.write(",%s" % (ID))
                self.files.write("\n")
                self.files.flush()
        if save_mode == self.SAVE_MODE_MULTIPLE_FILES:
            self.files = {}
            for ID in self.ids:
                f = open(self.get_storage_folder() + '/' + ID + '.csv', mode)
                self.files[ID] = f
                if mode == 'w':
                    # Adds header
                    self.files[ID].write("%s,%s\n" % ("timestamp", ID))
                self.files[ID].write("%s,%s\n" % (self.current['timestamp'], self.current[ID]))
                self.files[ID].flush()

    def close_save_files(self, save_mode):
        """ Closes files for saving """
        if self.files is None:
            # files were never opened or already closed once
            return
        if save_mode == self.SAVE_MODE_SINGLE_FILE:
            self.files.write(self.current['timestamp'])
            for ID in self.ids:
                self.files.write(",%s" % (self.current[ID]))
            self.files.write("\n")
            self.files.close()
        if save_mode == self.SAVE_MODE_MULTIPLE_FILES:
            for ID in self.ids:
                self.files[ID].close()
        self.files = None

    def save_meta_data(self, mode='w'):
        """ Saves meta_data """
        f = open(os.path.join(self.get_storage_folder(), 'meta_data.csv'), mode)
        for ID in self.meta_data:
            f.write("%s,%s\n" % (ID, self.current[ID]))
        f.close()

    def store(self, save_mode, mode='w'):
        """ Stores DataItems with their key in keys to the storageFolder """
        print("Storing dataItems " + str(self.ids) + " from MTConnect Device " + self.deviceUUID)
        print("Destination : " + self.storageFolder)

        consumer = KafkaConsumer('mtc_devices',
                                 bootstrap_servers=self.bootstrap_servers,
                                 auto_offset_reset='latest',
                                 key_deserializer=self.mtc_key_deserializer,
                                 value_deserializer=self.mtc_value_deserializer)

        record = False
        for message in consumer:
            if message.key != self.deviceUUID:
                continue
            self.print_message(message)

            # copy 'current' to 'previous', updates current state and saves it
            if message.value['id'] in self.meta_data or message.value['id'] in self.ids:
                self.previous = self.current.copy()
                self.current[message.value['id']] = message.value['value']
                #self.current['kafka_offset'] = message.offset
                self.current['timestamp'] = message.value['attributes']['timestamp']
                self.save_current()

            # signal for stop recording recieved
            if message.value['id'] == self.stop_id:
                print("Stop storing")
                self.close_save_files(save_mode)
                record = False

            # signal for start recording recieved
            if message.value['id'] == self.start_id and message.value['value'] != 'UNAVAILABLE':
                print("Start storing")
                self.save_meta_data(mode)
                self.open_save_files(save_mode, mode)
                record = True

            # records data
            if message.value['id'] in self.ids and record:
                if save_mode == self.SAVE_MODE_MULTIPLE_FILES:
                    self.files[message.value['id']].write("%s,%s\n" % (message.value['attributes']['timestamp'], message.value['value']))
                    self.files[message.value['id']].flush()
                if save_mode == self.SAVE_MODE_SINGLE_FILE:
                    if self.previous['timestamp'] != message.value['attributes']['timestamp']:
                        self.files.write(self.previous['timestamp'])
                        for ID in self.ids:
                            self.files.write(",%s" % (self.previous[ID]))
                        self.files.write("\n")
                        self.files.flush()
