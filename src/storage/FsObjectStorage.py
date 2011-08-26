import os
import bisect
import datetime
from twisted.python import log
from twisted.internet import defer

class ObjectStorage(object):
    def __init__(self):
        log.msg('FsObjectStorage.ObjectStorage loaded')
        self.directory = '/tmp/s3'

    @defer.inlineCallbacks
    def __object_path__(self, bucket_name, object_name = None):
        if object_name:
            result = yield os.path.abspath(os.path.join(self.directory, bucket_name, object_name))
            defer.returnValue(result)
        result = yield os.path.abspath(os.path.join(self.directory, bucket_name))
        defer.returnValue(result)

    @defer.inlineCallbacks
    def is_bucket(self, bucket_name, object_name = None):
        _bucket = yield self.__object_path__(bucket_name, object_name)
        if os.path.isdir(_bucket):
            defer.returnValue(True)
        else:
            defer.returnValue(False)

    @defer.inlineCallbacks
    def is_object(self, bucket_name, object_name):
        _object = yield self.__object_path__(bucket_name, object_name)
        if os.path.isfile(_object):
            defer.returnValue(True)
        else:
            defer.returnValue(False)

    @defer.inlineCallbacks
    def list_buckets(self):
        buckets = []
        for bucket_name in os.listdir(self.directory):
            _bucket = yield self.__object_path__(bucket_name)
            buckets.append({
                'Name': bucket_name,
                'CreationDate': datetime.datetime.utcfromtimestamp(os.stat(_bucket).st_ctime),
            })
        defer.returnValue(buckets)

    @defer.inlineCallbacks
    def create_bucket(self, bucket_name):
        _bucket = yield self.__object_path__(bucket_name)
        if not os.path.isdir(_bucket):
            os.makedirs(_bucket)
            log.msg('Created bucket %s' % bucket_name)
            defer.returnValue(True)
        else:
            defer.returnValue(False)

    @defer.inlineCallbacks
    def delete_bucket(self, bucket_name):
        _bucket = yield self.__object_path__(bucket_name)
        if os.path.isdir(_bucket):
            os.rmdir(_bucket)
            log.msg('Delete bucket %s' % bucket_name)
            defer.returnValue(True)
        else:
            defer.returnValue(False)

    @defer.inlineCallbacks
    def list_objects(self, bucket_name, marker = None, prefix = None, max_keys = 5000, terse = None):
        objects = []
        start_pos = 0
        truncated = False
        _bucket = yield self.__object_path__(bucket_name)
        [ objects.append(file_name) for root, dirs, files in os.walk(_bucket) for file_name in files ]
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
                _object_path = yield self.__object_path__(bucket_name, _object)
                _stat = os.stat(_object_path)
                content.update({
                    'LastModified': datetime.datetime.utcfromtimestamp(_stat.st_mtime),
                    'Size': _stat.st_size,
                })
            contents.append(content)
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
        _object = yield self.__object_path__(bucket_name, object_name)
        _stat = os.stat(_object)
        defer.returnValue({
                            'LastModified': datetime.datetime.utcfromtimestamp(_stat.st_mtime), 
                            'CreationDate': datetime.datetime.utcfromtimestamp(_stat.st_ctime),
                            'Size': _stat.st_size
                          })

    @defer.inlineCallbacks
    def read_object(self, bucket_name, object_name):
        _object = yield self.__object_path__(bucket_name, object_name)
        defer.returnValue(open(_object).read())

    @defer.inlineCallbacks
    def write_object(self, bucket_name, object_name, content):
        _object = yield self.__object_path__(bucket_name, object_name)
        open(_object, 'w').write(content)
        log.msg('Created object %s on bucket %s' % (object_name, bucket_name))
        defer.returnValue(True)

    @defer.inlineCallbacks
    def delete_object(self, bucket_name, object_name):
        _object = yield self.__object_path__(bucket_name, object_name)
        if os.path.isfile(_object):
            os.unlink(_object)
            log.msg('Deleted object %s on bucket %s' % (object_name, bucket_name))
            defer.returnValue(True)
        else:
            defer.returnValue(False)
