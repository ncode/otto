# vim:set ft=python:

import os
import sys
import otto
import ConfigParser
from twisted.application import service, internet

config_file = 'config/otto.cfg'
if not os.path.isfile(config_file):
    print "Problem: %s not found" % config_file
    sys.exit(1)

config = ConfigParser.RawConfigParser()
config.read(config_file)


Port = config.getint('otto', 'Port')
ObjectStorage = config.get('otto', 'ObjectStorage')
storage_config = {}

if config.has_section(ObjectStorage):
	for key, value in config.items(ObjectStorage):
		storage_config[key] = value

application = service.Application("Otto Daemon")
srv = internet.TCPServer(Port, otto.S3Application(ObjectStorage, storage_config), )
srv.setServiceParent(application)
