# Otto - An S3 Clone on top of cyclone

## Requirements:

cyclone - https://github.com/fiorix/cyclone.git
txriak - https://github.com/fiorix/cyclone.git

## Usage:
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
    
# Project Status: Experimental

Base on https://github.com/gleicon/3s and
http://github.com/facebook/tornado/raw/master/tornado/s3server.py
