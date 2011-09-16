# Otto - S3 Clone on top of cyclone
* supporting fs and riak backends for object storage

## Requirements:

* cyclone - https://github.com/fiorix/cyclone
* txriak - https://bitbucket.org/asi/txriak

## Usage:
### Configuringa otto:

    $ vim otto.cfg
    [otto]
    Port = 4000
    #ObjectStorage = storage.FsObjectStorage
    ObjectStorage = storage.RiakObjectStorage
    #tmp_directory = /tmp/otto

### Running otto:

    $ twistd -ny mann.tac

### Write:

    $ curl --request PUT "http://localhost:4000/otto/"
    $ curl --data-binary "@otto.py" --request PUT --header "Content-Type: text/plain" "http://localhost:4000/otto/otto.py"

### Read:

    $ curl "http://localhost:4000/otto/"
    $ curl "http://localhost:4000/otto/otto.py"

### Delete:

    $ curl --request DELETE "http://localhost:4000/otto/otto.py"
    $ curl --request DELETE "http://localhost:4000/otto/"
    
# Project Status: Almost testing

Base on https://github.com/gleicon/3s and
http://github.com/facebook/tornado/raw/master/tornado/s3server.py
