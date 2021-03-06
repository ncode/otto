import os
import sys
import json
import time
import bisect
import logging
import datetime
from txriak import riak
from cyclone import httpclient
from twisted.python import log
from twisted.internet import defer

class ObjectStorage(object):
    def __init__(self, storage_config = {}):
        log.msg('RiakObjectStorage.ObjectStorage loaded')
        self.riak_client = riak.RiakClient()
        self._private = ['luwak_node', 'deleted_files']

    @defer.inlineCallbacks
    def __object_path__(self, bucket_name, object_name):
        result = yield 'luwak/%s::%s' % (bucket_name, object_name)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def is_bucket(self, bucket_name, object_name = None):
        _bucket = yield self.riak_client.list_buckets()
        if (bucket_name in _bucket):
            if not object_name:
                bucket = self.riak_client.bucket(bucket_name)
                obj = yield bucket.get_binary('__CreationDate__')
                if obj.exists():
                    log.msg('Found a bucket with name %s' % bucket_name, logLevel=logging.DEBUG)
                    defer.returnValue(True)
                defer.returnValue(False)

            bucket = self.riak_client.bucket(bucket_name)
            obj = yield bucket.get_binary("%s/%s" % (bucket_name, object_name))
            if obj.exists():
                log.msg('Found a bucket with name %s/%s' % (bucket_name, object_name), logLevel=logging.DEBUG)
                defer.returnValue(True)
        defer.returnValue(False)
    
    @defer.inlineCallbacks
    def is_object(self, bucket_name, object_name):
        bucket = self.riak_client.bucket(bucket_name)
        obj = yield bucket.get_binary(object_name)
        if obj.exists():
            log.msg('Found object %s on bucket %s' % (object_name, bucket_name), logLevel=logging.DEBUG)
            defer.returnValue(obj)
        defer.returnValue(False)

    @defer.inlineCallbacks
    def list_buckets(self):
        bucket_list = []
        _bucket = yield self.riak_client.list_buckets()
        for bucket in _bucket:
            if bucket in self._private:
                continue

            obj = yield self.is_object(bucket, '__CreationDate__')
            if obj:
                bucket_list.append({
                    'Name': bucket,
                    'CreationDate': datetime.datetime.fromtimestamp(float(json.loads(obj.get_data()))),
                })
        defer.returnValue(bucket_list)

    @defer.inlineCallbacks
    def create_bucket(self, bucket_name):
        bucket = self.riak_client.bucket(bucket_name)
        obj = bucket.new_binary('__CreationDate__', str(time.mktime(datetime.datetime.now().timetuple())))
        yield obj.store()
        del(obj)
        log.msg('Created bucket %s' % bucket_name)

    @defer.inlineCallbacks
    def delete_bucket(self, bucket_name):
        obj = yield self.delete_object(bucket_name, '__CreationDate__')
        if obj:
            log.msg('Removed bucket %s' % bucket_name)
            defer.returnValue(True)
        defer.returnValue(False)

    @defer.inlineCallbacks
    def list_objects(self, bucket_name, marker = None, prefix = None, max_keys = 5000, terse = None):
        start_pos = 0
        truncated = False
        bucket = self.riak_client.bucket(bucket_name)
        objects = yield bucket.list_keys()
        if marker:
            start_pos = bisect.bisect_right(objects, marker, start_pos)
        if prefix:
            start_pos = bisect.bisect_left(objects, prefix, start_pos)

        contents = []
        for _object in objects[start_pos:]:
            if not _object == '__CreationDate__':
                if not _object.startswith(prefix):
                    break
                if len(contents) >= max_keys:
                    truncated = True
                    break
                content = {'Key': _object}
                if not terse:
                    _stat = yield self.stat_object(bucket_name, _object)
                    content.update({
                        'LastModified': _stat['LastModified'],
                        'Size': _stat['Size'],
                    })
                contents.append(content)
                log.msg('Listing object %s from bucket %s' % (_object, bucket_name))
                marker = _object

        defer.returnValue({ 
                    'Name': bucket_name, 
                    'Prefix': prefix, 
                    'Marker': marker, 
                    'MaxKeys': max_keys, 
                    'IsTruncated': truncated, 
                    'Contents': contents
                })

    @defer.inlineCallbacks
    def stat_object(self, bucket_name, object_name):
        bucket = self.riak_client.bucket(bucket_name)
        _object = yield bucket.get_binary(object_name)
        if _object.exists():
            log.msg('Stating object %s fron bucket %s' % (object_name, bucket_name))
            _object = json.loads(_object.get_data())
            log.msg(_object)
            _stat =  yield {
                                'LastModified': datetime.datetime.fromtimestamp(float(_object['LastModified'])),
                                'CreationDate': datetime.datetime.fromtimestamp(float(_object['CreationDate'])),
                                'Size': _object['Size']
                           }
            defer.returnValue(_stat)
        defer.returnValue(False)

    @defer.inlineCallbacks
    def read_object(self, bucket_name, object_name):
        bucket = self.riak_client.bucket(bucket_name)
        _object = yield bucket.get_binary(object_name)
        _object = json.loads(_object.get_data())
        content = yield httpclient.fetch('http://127.0.0.1:8098/%s' % _object['ObjectPath'])
        defer.returnValue(content.body)

    @defer.inlineCallbacks
    def write_object(self, bucket_name, object_name, content):
        _object = yield self.__object_path__(bucket_name, object_name)
        _stat_obj = yield self.is_object(bucket_name, object_name)
        creation_date = None
        if _stat_obj:
            _stat_obj = json.loads(_stat_obj.get_data())
            creation_date = _stat_obj['CreationDate']

        status = yield httpclient.fetch('http://127.0.0.1:8098/luwak', method="POST", postdata=content, headers={"Content-Type": ["application/unknown"]})
        log.msg('Response Headers from luwak %s' % status.headers)
        log.msg('Response Body from luwak %s' % status.body)
        log.msg('Status Code from luwak %s' % status.code)
        log.msg('Status Length from luwak %s' % status.length)
        _object = status.headers['Location'][0] 
        log.msg('Object location on luwak %s' % _object)
        stat = { 
                    'CreationDate': creation_date or str(time.mktime(datetime.datetime.now().timetuple())),
                    'LastModified': str(time.mktime(datetime.datetime.now().timetuple())),
                    'ObjectPath': _object,
                    'Size': sys.getsizeof(content)
               }
        bucket = self.riak_client.bucket(bucket_name)
        obj = bucket.new_binary(object_name, json.dumps(stat))
        yield obj.store()
        del(obj)
        log.msg('Created object %s on bucket %s' % (object_name, bucket_name))
        if _stat_obj:
            bucket = self.riak_client.bucket('deleted_objects')
            stat = {
                        'DeletetionDate': creation_date or str(time.mktime(datetime.datetime.now().timetuple())),
                        'ObjectPath': _stat_obj['ObjectPath'].split('/')[-1],
                        'FromBucket': bucket_name
                   }
            log.msg('Old object location from bucket %s in luwak %s' % (bucket_name, _stat_obj['ObjectPath']))
            obj = bucket.new_binary(_stat_obj['ObjectPath'].split('/')[-1], json.dumps(stat))
            yield obj.store()
            del(obj)   
            status = yield httpclient.fetch('http://127.0.0.1:8098/%s' % _stat_obj['ObjectPath'], method="DELETE")
        defer.returnValue(True)

    @defer.inlineCallbacks
    def delete_object(self, bucket_name, object_name):
        bucket = self.riak_client.bucket(bucket_name)
        obj = yield bucket.get_binary(object_name)
        if obj.exists():
            _object = json.loads(obj.get_data())
            if 'ObjectPath' in object_name: 
                httpclient.fetch('http://127.0.0.1:8098/%s' % _object['ObjectPath'], method="DELETE")
                log.msg('Deleted object %s from bucket %s on %s' % (object_name, bucket_name, _object['ObjectPath']))
            else:
                log.msg('Deleted object %s from bucket %s' % (object_name, bucket_name))
            obj.delete()
            defer.returnValue(True)
        defer.returnValue(False)
