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
Port = config.getint('otto','Port')
ObjectStorage = config.get('otto', 'ObjectStorage')
tmp_directory = config.get('otto', 'tmp_directory')

application = service.Application("Otto Daemon")
srv = internet.TCPServer(Port, otto.S3Application(tmp_directory=tmp_directory, ObjectStorage=ObjectStorage))
srv.setServiceParent(application)
