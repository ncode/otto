#!/usr/bin/python

from setuptools import setup

setup(name='otto',
    version='0.0.2',
    description='S3 Clone on top of cyclone',
    author='Juliano Martinez',
    author_email='juliano@martinez.io',
    url='https://github.com/ncode/otto',
    install_requires=['cyclone','txriak'],
    packages=['otto', 'otto.storage'],
    package_dir = {
        'otto': 'src/lib',
        'otto.storage': 'src/storage',
    },
    data_files=[
        ('/etc', ['src/config/otto.cfg']),
        ('/etc/default', ['src/default/otto']),
        ('/etc/init.d', ['src/init/otto.debian']),
        ('/etc', ['src/config/otto.cfg']),
        ('/usr/sbin', ['src/otto.tac']),
    ]
)
