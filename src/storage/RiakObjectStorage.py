import os
import bisect
import datetime
from txriak import riak
from twisted.python import log
from twisted.internet import defer

class ObjectStorage(object):
    def __init__(self):
        log.msg('RiakObjectStorage.ObjectStorage loaded')
        self.client = riak.RiakClient()

    @defer.inlineCallbacks
    def is_bucket(self, bucket_name, object_name = None):
        _bucket = yield self.client.list_buckets()
        if (bucket_name in _bucket):
            if not object_name:
                defer.returnValue(True)
            
            bucket = client.bucket(bucket_name)
            obj = yield bucket.get_binary(object_name)
            if obj.exists():
                defer.returnValue(True)
        defer.returnValue(False)
    
    @defer.inlineCallbacks
    def is_object(self, bucket_name, object_name):
        bucket = client.bucket(bucket_name)
        obj = yield bucket.get_binary(object_name)
        if obj.exists():
            defer.returnValue(True)

    def _return(self, value):
        log.msg(value)
        return value

    @defer.inlineCallbacks
    def list_buckets(self):
        bucket_list = []
        _bucket = yield self.client.list_buckets()
        log.msg(_bucket)
        for bucket in _bucket:
            bucket_list.append({
                'Name': bucket,
                'CreationDate': datetime.datetime.now(),
            })
        defer.returnValue(bucket_list)

    @defer.inlineCallbacks
    def create_bucket(self, bucket_name):
        bucket = client.bucket(bucket_name)
        obj = bucket.new_binary('__CreationDate__', datetime.datetime.now())
        yield obj.store()
        del(obj)
        log.msg('Created bucket %s' % bucket_name)

    def delete_bucket(self, bucket_name):
        pass

    def list_objects(self, bucket_name, marker = None, prefix = None, max_keys = 5000, terse = None):
        objects = []
        start_pos = 0
        truncated = False
        [ objects.append(file_name) for root, dirs, files in os.walk(self.__object_path__(bucket_name)) for file_name in files ]
        if marker:
            start_pos = bisect.bisect_right(objects, marker, start_pos)
        if prefix:
            start_pos = bisect.bisect_left(objects, prefix, start_pos)

        contents = []
        for _object in objects[start_pos:]:
            if not _object.startswith(prefix):
                break
            if len(contents) >= max_keys:
                truncated = True
                break
            content = {'Key': _object}
            if not terse:
                _stat = os.stat(self.__object_path__(bucket_name, _object))
                content.update({
                    'LastModified': datetime.datetime.utcfromtimestamp(_stat.st_mtime),
                    'Size': _stat.st_size,
                })
            contents.append(content)
            marker = _object

        yield { 
                    'Name': bucket_name, 
                    'Prefix': prefix, 
                    'Marker': marker, 
                    'MaxKeys': max_keys, 
                    'IsTruncated': truncated, 
                    'Contents': contents
                }

    def stat_object(self, bucket_name, object_name):
        _stat = os.stat(self.__object_path__(bucket_name, object_name))
        yield {
                    'LastModified': datetime.datetime.utcfromtimestamp(_stat.st_mtime), 
                    'CreationDate': datetime.datetime.utcfromtimestamp(_stat.st_ctime),
                    'Size': _stat.st_size
               }

    def read_object(self, bucket_name, object_name):
        yield open(self.__object_path__(bucket_name, object_name)).read()

    def write_object(self, bucket_name, object_name, content):
        open(self.__object_path__(bucket_name, object_name), 'w').write(content)
        log.msg('Created object %s on bucket %s' % (object_name, bucket_name))

    def delete_object(self, bucket_name, object_name):
        _object = self.__object_path__(bucket_name, object_name)
        if os.path.isfile(_object):
            os.unlink(_object)
            log.msg('Created object %s on bucket %s' % (object_name, bucket_name))
