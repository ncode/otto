import os
import bisect
import datetime
from twisted.python import log

class ObjectStorage(object):
    def __init__(self):
        log.msg('FsObjectStorage.ObjectStorage loaded')
        self.directory = '/tmp/s3'

    def __object_path__(self, bucket_name, object_name = None):
        if object_name:
            return os.path.abspath(os.path.join(self.directory, bucket_name, object_name))
        return os.path.abspath(os.path.join(self.directory, bucket_name))

    def is_bucket(self, bucket_name):
        if os.path.isdir(self.__object_path__(bucket_name)):
            return True
        return False

    def is_object(self, bucket_name, object_name):
        if os.path.isfile(self.__object_path__(bucket_name, object_name)):
            return True

    def list_buckets(self):
        buckets = []
        for bucket in os.listdir(self.directory):
            info = os.stat(self.__object_path__(bucket))
            buckets.append({
                "Name": bucket,
                "CreationDate": datetime.datetime.utcfromtimestamp(info.st_ctime),
            })
        return buckets

    def create_bucket(self, bucket_name):
        pass

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
            content = {"Key": _object}
            if not terse:
                _stat = os.stat(self.__object_path__(bucket, _object))
                c.update({
                    "LastModified": datetime.datetime.utcfromtimestamp(_stat.st_mtime),
                    "Size": _stat.st_size,
                })
            contents.append(content)
            marker = _object

        return {'Name': bucket_name, 'Prefix': prefix, 'Marker': marker, 'MaxKeys': max_keys, 'IsTruncated': truncated, 'Contents': contents}

    def write_object(self, bucket_name, object_name):
        pass

    def delete_object(self, bucket_name, object_name):
        pass
