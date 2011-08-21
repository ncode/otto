#!/usr/bin/python

from setuptools import setup

setup(name='otto',
      version='0.0.1',
      description='S3 Clone on top of cyclone'
      author='Juliano Martinez',
      author_email='juliano@martinez.io',
      url='https://github.com/ncode/otto'
      install_requires=['cyclone','txriak'],
      packages=['otto'],
     )
