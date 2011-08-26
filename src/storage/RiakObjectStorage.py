import os
import time
import bisect
import datetime
from txriak import riak
from twisted.python import log
from twisted.internet import defer

class ObjectStorage(object):
    def __init__(self):
        log.msg('RiakObjectStorage.ObjectStorage loaded')
        self.client = riak.RiakClient()
        self._private = ['luwak_node']

    @defer.inlineCallbacks
    def __object_path__(self, bucket_name, object_name):
        result = yield 'luwak/%s/%s' % (bucket_name, object_name)
        defer.returnValue(result)

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
        bucket = self.client.bucket(bucket_name)
        obj = yield bucket.get_binary(object_name)
        if obj.exists():
            defer.returnValue(True)

    @defer.inlineCallbacks
    def list_buckets(self):
        bucket_list = []
        _bucket = yield self.client.list_buckets()
        log.msg(_bucket)
        for bucket in _bucket:
            if not bucket in self._private:
                bucket_list.append({
                    'Name': bucket,
                    'CreationDate': datetime.datetime.now(),
                })
        defer.returnValue(bucket_list)

    @defer.inlineCallbacks
    def create_bucket(self, bucket_name):
        bucket = self.client.bucket(bucket_name)
        obj = bucket.new('__CreationDate__', str(time.mktime(datetime.datetime.now().timetuple())))
        yield obj.store()
        del(obj)
        log.msg('Created bucket %s' % bucket_name)

    @defer.inlineCallbacks
    def delete_bucket(self, bucket_name):
        pass

    def list_objects(self, bucket_name, marker = None, prefix = None, max_keys = 5000, terse = None):
        start_pos = 0
        truncated = False
        objects = yield bucket.list_keys()
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
                _stat = yield self.stat_object(bucket_name, object_name_
                content.update({
                    'LastModified': _stat['LastModified'],
                    'Size': _stat['Size'],
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
        bucket = self.client.bucket(bucket_name)
        _object = yield bucket.get_binary(object_name)
        if _object.exists():
            defer.returnValue(False)
            _stat = _object.get_data
            yield {
                        'LastModified': datetime.datetime.utcfromtimestamp(_stat.st_mtime), 
                        'CreationDate': datetime.datetime.utcfromtimestamp(_stat.st_ctime),
                        'Size': _stat.st_size
                }

    def read_object(self, bucket_name, object_name):
        yield open(self.__object_path__(bucket_name, object_name)).read()

    def write_object(self, bucket_name, object_name, content):
        bucket = self.client.bucket(bucket_name)
        _object = yield self.__object_path__(bucket_name, object_name)
        stat = { 
                    'CreationDate': str(time.mktime(datetime.datetime.now().timetuple())),
                    'ObjectPath': _object
               }
        obj = bucket.new_binary(object_name, json.dumps(stat))
        yield obj.store()
        del(obj)
        log.msg('Created object %s on bucket %s' % (object_name, bucket_name))
        defer.returnValue(True)

    def delete_object(self, bucket_name, object_name):
        bucket = self.client.bucket(bucket_name)
        obj = yield bucket.get_binary(object_name)
        if obj.exists():
            obj.delete()
            log.msg('Deleted object %s on bucket %s' % (object_name, bucket_name))
            defer.returnValue(True)
        defer.returnValue(False)
