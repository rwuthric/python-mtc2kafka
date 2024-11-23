# python-mtc2kafka
[![Unit tests](https://github.com/rwuthric/python-mtc2kafka/actions/workflows/unittests.yml/badge.svg)](https://github.com/rwuthric/python-mtc2kafka/actions/workflows/unittests.yml)

Python library to stream MTConnect data to Apache Kafka

Each MTConnect DataItem is streamed to the single Kafka topic `'mtc_devices'`.
Each message key is the MTConnect `UUID` of the device.

## Kafka Message Structure
* The kafka ```message.topic``` is `'mtc_devices'`
* The kafka ```message.key``` is the MTConnect `UUID` of the device
* The kafka ```message.value``` is a JSON string containing the following fields:
  * `'id'` : the MTConnect DataItem ID
  * `'tag'` : the xml tag of the MTConnect DataItem
  * `'attributes'` : the attributes of the MTConnect DataItem in JSON format
  * `'value'` : the MTConnect value of the MTConnect DataItem
  * `'type'` : the MTConnect DataItem category (either `Samples` or `Events`)
  
mtc2kafka converts all JSON formats to python dictionnaries to simplify their manipulation.

## Example
Consider a MTConnect DataItem defined in the device file (which contains the MTConnect Device Information Model) as follows:
```
<DataItem category="SAMPLE" id="Zact" name="Zact" nativeUnits="MILLIMETER" subType="ACTUAL" type="POSITION" units="MILLIMETER"/>
```
If the device with `UUID` equal to `ZAIX-4-001` streamed data in MTConnect format as (to understand how MTConnect fills data into MTConnectStreams based on the defintion of the 
MTConnect Device Information Model, read the [offical definition of the MTConnect standard](https://www.mtconnect.org/standard-download20181)):
```
<Samples> 
   <Position dataItemId="Zact" name="Zact" sequence="18059" subType="ACTUAL" timestamp="2022-11-06T21:40:21.587353Z">-0.328</Position>  
</Samples>
```
then the following kafka message fields will be available:
* `message.key` equal to `ZAIX-4-001`
* `message.value['id']` equal to `Zact`
* `message.value['tag']` equal to `Position`
* `message.value['attributes']` equal to ```{'name': 'Zact', 'subType': 'ACTUAL', 'timestamp': '2022-11-06T21:40:21.587353Z'}```
* `message.value['value']` equal to `-0.328`
* `message.value['type']` equal to `Samples`

The MTConnect attributes can be accsed in python as a dictionnary. For example the `'timestamp'` attribute would be accessible like so:
```
message.value['attributes']['timestamp']
```

## Installation
Create a local clone of this repository and in the root directory of the cloned code run:
```
pip install .
```

## Uninstall
Use
```
pip uninstall mtc2kafka
```

## Dependencies
The following Python libraries are required (installation via `pip` handles them automatically)

1. [kafka-python](https://kafka-python.readthedocs.io/en/master/)
```
python -m pip install kafka-python
```
2. [requests](https://pypi.org/project/requests/)
```
python -m pip install requests
```

## Unit tests
The test suite is described [here](https://github.com/rwuthric/python-mtc2kafka/tree/main/tests)
