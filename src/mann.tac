#!/usr/bin/env python
# coding: utf-8
# twistd -ny s3.tac
# gleicon moraes (http://zenmachine.wordpress.com | http://github.com/gleicon)

SERVER_PORT = 4000

import otto
from twisted.application import service, internet

application = service.Application("Otto Daemon")
srv = internet.TCPServer(SERVER_PORT, otto.S3Application(tmp_directory="/tmp/otto"))
srv.setServiceParent(application)
