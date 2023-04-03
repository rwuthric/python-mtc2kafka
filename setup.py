# mtc2kafka package build script

from setuptools import setup, find_packages
import mtc2kafka

setup(
    name='mtc2kafka',
    version=mtc2kafka.__version__,    
    description='Python library to stream MTConnect data to Apache Kafka',
    url='https://github.com/rwuthric/mtc2kafka',
    author=mtc2kafka.__author__,
    author_email=mtc2kafka.__email__,
    license='BSD 3-Clause License',
    packages=find_packages(),
    install_requires=['requests', 'kafka-python'],
)