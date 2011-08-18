#!/usr/bin/env python
#
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# port to cyclone: took out ioloop initialization, fixed imports and created a .tac file
# gleicon 04/10

"""Implementation of an S3-like storage server based on local files.

Useful to test features that will eventually run on S3, or if you want to
run something locally that was once running on S3.

We don't support all the features of S3, but it does work with the
standard S3 client for the most basic semantics. To use the standard
S3 client with this module:

    c = S3.AWSAuthConnection("", "", server="localhost", port=8888,
                             is_secure=False)
    c.create_bucket("mybucket")
    c.put("mybucket", "mykey", "a value")
    print c.get("mybucket", "mykey").body

"""

from storage import FsObjectStorage
from twisted.python import log
from cyclone import escape
from cyclone import web
import datetime
import urllib
import sys
import os

class S3Application(web.Application):
    """Implementation of an S3-like storage server based on local files.

    If bucket depth is given, we break files up into multiple directories
    to prevent hitting file system limits for number of files in each
    directories. 1 means one level of directories, 2 means 2, etc.
    """
    def __init__(self, root_directory, bucket_depth=0):
        web.Application.__init__(self, [
            (r"/", RootHandler),
            (r"/([^/]+)/(.+)", ObjectHandler),
            (r"/([^/]+)/", BucketHandler),
        ])
        self.directory = os.path.abspath(root_directory)
        self.tmp = '/tmp/cancer'
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        if not os.path.exists(self.tmp):
            os.makedirs(self.tmp)
        self.bucket_depth = bucket_depth
        self.storage = FsObjectStorage.ObjectStorage()

class BaseRequestHandler(web.RequestHandler):
#    SUPPORTED_METHODS = ("PUT", "GET", "DELETE", "HEAD")
    def render_xml(self, value):
        assert isinstance(value, dict) and len(value) == 1
        self.set_header("Content-Type", "application/xml; charset=UTF-8")
        name = value.keys()[0]
        parts = []
        parts.append('<%s xmlns="http://doc.s3.amazonaws.com/2006-03-01">' % escape.utf8(name))
        self._render_parts(value.values()[0], parts)
        parts.append('</%s>' % escape.utf8(name))
        self.finish('<?xml version="1.0" encoding="UTF-8"?>\n %s' % ''.join(parts))

    def _render_parts(self, value, parts=[]):
        if isinstance(value, basestring):
            parts.append(escape.xhtml_escape(value))
        elif isinstance(value, int) or isinstance(value, long):
            parts.append(str(value))
        elif isinstance(value, datetime.datetime):
            parts.append(value.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
        elif isinstance(value, dict):
            for name, subvalue in value.iteritems():
                if not isinstance(subvalue, list):
                    subvalue = [subvalue]
                for subsubvalue in subvalue:
                    parts.append('<%s>' % escape.utf8(name))
                    self._render_parts(subsubvalue, parts)
                    parts.append('</%s>' % escape.utf8(name))
        else:
            raise Exception("Unknown S3 value type %r", value)

    def _object_path(self, bucket, object_name):
        return os.path.abspath(os.path.join(self.application.directory, bucket, object_name))

class RootHandler(BaseRequestHandler):
    def get(self):
        log.msg('Accessing root directory')
        self.render_xml({"ListAllMyBucketsResult": {
            "Buckets": {"Bucket": self.application.storage.list_buckets()},
        }})

class BucketHandler(BaseRequestHandler):
    def get(self, bucket_name):
        log.msg('Accessing bucket %s' % bucket_name)
        prefix = self.get_argument("prefix", u"")
        marker = self.get_argument("marker", u"")
        max_keys = int(self.get_argument("max-keys", 50000))
        terse = int(self.get_argument("terse", 0))
        if not self.application.storage.is_bucket(bucket_name):
            raise web.HTTPError(404)
        self.render_xml({"ListBucketResult": self.application.storage.list_objects(bucket_name, marker, prefix, max_keys, terse)})

    def put(self, bucket_name):
        log.msg('Creating bucket %s' % bucket_name)
        if self.application.storage.is_bucket(bucket_name):
            raise web.HTTPError(403)
        self.application.storage.create_bucket(bucket_name)
        self.finish()

    def delete(self, bucket_name):
        log.msg('Deleting bucket %s' % bucket_name)
        if not self.application.storage.is_bucket(bucket_name):
            raise web.HTTPError(404)
        if len(self.application.storage.list_objects(bucket_name)['Contents']) > 0:
            raise web.HTTPError(403)
        self.application.storage.delete_bucket(bucket_name)
        self.set_status(204)
        self.finish()

class ObjectHandler(BaseRequestHandler):
    def get(self, bucket_name, object_name):
        log.msg('Accessing object %s from bucket %s' % (object_name, bucket_name))
        object_name = urllib.unquote(object_name)
        if not self.application.storage.is_object(bucket_name, object_name):
            raise web.HTTPError(404)
        self.set_header("Content-Type", "application/unknown")
        self.set_header("Last-Modified", self.application.storage.stat_object(bucket_name, object_name)['LastModified'])
        self.finish(self.application.storage.read_object(bucket_name, object_name))

    def put(self, bucket_name, object_name):
        log.msg('Writing object %s on bucket %s' % (object_name, bucket_name))
        object_name = urllib.unquote(object_name)
        if not self.application.storage.is_bucket(bucket_name):
            raise web.HTTPError(404)
        if self.application.storage.is_bucket(bucket_name, object_name):
            raise web.HTTPError(403)
        self.application.storage.write_object(bucket_name, object_name, self.request.body)
        self.finish()

    def delete(self, bucket_name, object_name):
        log.msg('Removing object %s from bucket %s' % (object_name, bucket_name))
        object_name = urllib.unquote(object_name)
        if not self.application.storage.is_object(bucket_name, object_name):
            raise web.HTTPError(404)
        self.application.storage.delete_object(bucket_name, object_name)
        self.set_status(204)
        self.finish()
