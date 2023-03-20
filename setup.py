# mtc2kafka package build script

from setuptools import setup, find_packages

setup(
    name='mtc2kafka',
    version='0.1.0',    
    description='Python library to stream MTConnect data to Apache Kafka',
    url='https://github.com/rwuthric/mtc2kafka',
    author='Rolf Wuthrich',
    author_email='rolf.wuthrich@concordia.ca',
    license='BSD 3-Clause License',
    packages=find_packages(),
    install_requires=['requests', 'kafka-python'],
)