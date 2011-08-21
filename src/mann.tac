SERVER_PORT = 4000
import otto
from twisted.application import service, internet
application = service.Application("Otto Daemon")
srv = internet.TCPServer(SERVER_PORT, otto.S3Application(tmp_directory="/tmp/otto"))
srv.setServiceParent(application)
