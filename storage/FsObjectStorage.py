import os
import datetime
from twisted.python import log

class ObjectStorage(object):
    def __init__(self):
        log.msg('FsObjectStorage.ObjectStorage loaded')
        self.directory = '/tmp/s3'
        pass

    def __is_bucket__(self, bucket_name):
        pass

    def __is_object__(self, bucket_name, object_name):
        pass

    def list_buckets(self):
        buckets = []
        for name in os.listdir(self.directory):
            path = os.path.join(self.directory, name)
            info = os.stat(path)
            buckets.append({
                "Name": name,
                "CreationDate": datetime.datetime.utcfromtimestamp(info.st_ctime),
            })
        return buckets

    def create_bucket(self, bucket_name):
        pass

    def delete_bucket(self, bucket_name):
        pass

    def list_objects(self, bucket_name):
        pass

    def write_object(self, bucket_name, object_name):
        pass

    def delete_object(self, bucket_name, object_name):
        pass
