import os
import bisect
import datetime
from twisted.python import log
from twisted.internet import defer
from twisted.internet import fdesc


class ObjectStorage(object):
    def __init__(self):
        log.msg('FsObjectStorage.ObjectStorage loaded')
        self.directory = '/tmp/s3'

    def __object_path__(self, bucket_name, object_name = None):
        if object_name:
            return os.path.abspath(os.path.join(self.directory, bucket_name, object_name))
        return os.path.abspath(os.path.join(self.directory, bucket_name))

    @defer.inlineCallbacks
    def is_bucket(self, bucket_name, object_name = None):
        if os.path.isdir(self.__object_path__(bucket_name, object_name)):
            defer.returnValue(True)
        defer.returnValue(False)

    @defer.inlineCallbacks
    def is_object(self, bucket_name, object_name):
        if os.path.isfile(self.__object_path__(bucket_name, object_name)):
            defer.returnValue(True)
        defer.returnValue(False)

    @defer.inlineCallbacks
    def list_buckets(self):
        buckets = []
        for bucket in os.listdir(self.directory):
            buckets.append({
                'Name': bucket,
                'CreationDate': datetime.datetime.utcfromtimestamp(os.stat(self.__object_path__(bucket)).st_ctime),
            })
        defer.returnValue(buckets)

    def create_bucket(self, bucket_name):
        _bucket = self.__object_path__(bucket_name)
        if not os.path.isdir(_bucket):
            os.makedirs(_bucket)
            log.msg('Created bucket %s' % bucket_name)

    def delete_bucket(self, bucket_name):
        _bucket = self.__object_path__(bucket_name)
        if os.path.isdir(_bucket):
            os.rmdir(_bucket)
            log.msg('Delete bucket %s' % bucket_name)

    @defer.inlineCallbacks
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
        _stat = os.stat(self.__object_path__(bucket_name, object_name))
        defer.returnValue({
                            'LastModified': datetime.datetime.utcfromtimestamp(_stat.st_mtime), 
                            'CreationDate': datetime.datetime.utcfromtimestamp(_stat.st_ctime),
                            'Size': _stat.st_size
                          })

    @defer.inlineCallbacks
    def read_object(self, bucket_name, object_name):
        defer.returnValue(open(self.__object_path__(bucket_name, object_name)).read())

    @defer.inlineCallbacks
    def write_object(self, bucket_name, object_name, content):
        file = open(self.__object_path__(bucket_name, object_name), 'w')
        fdesc.writeToFD(file, content)
        log.msg('Created object %s on bucket %s' % (object_name, bucket_name))

    def delete_object(self, bucket_name, object_name):
        _object = self.__object_path__(bucket_name, object_name)
        if os.path.isfile(_object):
            os.unlink(_object)
            log.msg('Created object %s on bucket %s' % (object_name, bucket_name))
