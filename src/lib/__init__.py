#!/usr/bin/env python

__version__ = '0.0.1'
VERSION = tuple(map(int, __version__.split('.')))
__all__ = ['otto']
__author__ = 'Juliano Martinez <juliano@martinez.io>'

from twisted.internet import defer
from twisted.python import log
from cyclone import escape
from cyclone import web
from cyclone import redis

import datetime, urllib, sys, os, base64
from hashlib import md5
import functools

class S3Application(web.Application):
    def __init__(self, storage, storage_config = {}):
        web.Application.__init__(self, [
            (r"/", RootHandler),
            (r"/([^/]+)/(.+)", ObjectHandler),
            (r"/([^/]+)[/]?", BucketHandler),
        ])
        exec("from otto.storage import %s as ObjectStorage" % storage)
        self.storage = ObjectStorage.ObjectStorage(storage_config)
        # TODO: split from redis into generic authorization class, expose config settings
        self.auth_db = redis.lazyConnectionPool() 

class BaseRequestHandler(web.RequestHandler):
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
    
    def _calculate_hash(self):
        pass

    def _sign_string(self):
        pass
    
    def _authenticate(self, access_key, signed_string):
        return True

def Authenticator(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            auth_hdr = self.request.headers["Authorization"]
            if auth_hdr is not None:
                auth_type, auth_data = auth_hdr.split()
            assert auth_type == "AWS"
            access_key, signed_hash = auth_data.split(":", 1)
            log.msg(self._authenticate(access_key, signed_hash))
        except Exception, e:
            log.msg(e)
            # All arguments are optional. Defaults are:
            # log_message=None, auth_type="Basic", realm="Restricted Access"
            raise cyclone.web.HTTPAuthenticationRequired(
                                log_message="Authentication failed!",
                                auth_type="AWS", realm="otto")
        else:
            #self._current_user = usr
            return method(self, *args, **kwargs)
    return wrapper

class RootHandler(BaseRequestHandler):
    @defer.inlineCallbacks
    @web.asynchronous
    def get(self):
        log.msg('Accessing root directory')
        bucket_list = yield self.application.storage.list_buckets()
        self.render_xml({"ListAllMyBucketsResult": {
            "Buckets": {"Bucket": bucket_list},
        }})

class BucketHandler(BaseRequestHandler):
    @defer.inlineCallbacks
    @web.asynchronous
    def get(self, bucket_name):
        log.msg('Accessing bucket %s' % bucket_name)
        prefix = self.get_argument("prefix", u"")
        marker = self.get_argument("marker", u"")
        max_keys = int(self.get_argument("max-keys", 50000))
        terse = int(self.get_argument("terse", 0))
        status = yield self.application.storage.is_bucket(bucket_name)
        if not status:
            raise web.HTTPError(404)
        _objects = yield self.application.storage.list_objects(bucket_name, marker, prefix, max_keys, terse)
        self.render_xml({"ListBucketResult": _objects})

    @defer.inlineCallbacks
    @web.asynchronous
    @Authenticator
    def put(self, bucket_name):
        log.msg('Creating bucket %s' % bucket_name)
        status = yield self.application.storage.is_bucket(bucket_name)
        if status:
            raise web.HTTPError(403)
        self.application.storage.create_bucket(bucket_name)
        self.finish()

    @defer.inlineCallbacks
    @web.asynchronous
    def delete(self, bucket_name):
        log.msg('Deleting bucket %s' % bucket_name)
        status = yield self.application.storage.is_bucket(bucket_name)
        if not status:
            raise web.HTTPError(404)
        contents = yield self.application.storage.list_objects(bucket_name)
        if len(contents['Contents']) > 0:
            raise web.HTTPError(403)
        self.application.storage.delete_bucket(bucket_name)
        self.set_status(204)
        self.finish()

class ObjectHandler(BaseRequestHandler):
    @defer.inlineCallbacks
    @web.asynchronous
    def get(self, bucket_name, object_name):
        log.msg('Accessing object %s from bucket %s' % (object_name, bucket_name))
        object_name = urllib.unquote(object_name)
        status = yield self.application.storage.is_object(bucket_name, object_name)
        if not status:
            raise web.HTTPError(404)
        self.set_header("Content-Type", "application/unknown")
        _stat = yield self.application.storage.stat_object(bucket_name, object_name)
        self.set_header("Last-Modified", _stat['LastModified'])
        _object = yield self.application.storage.read_object(bucket_name, object_name)
        self.finish(_object)

    @defer.inlineCallbacks
    def put(self, bucket_name, object_name):
        log.msg('Writing object %s on bucket %s' % (object_name, bucket_name))
        object_name = urllib.unquote(object_name)
        body = self.request.body
        m = md5(body)
        etag = m.hexdigest()
        self.set_header("ETag", '"%s"' % etag)
        status = yield self.application.storage.is_bucket(bucket_name)
        if not status:
            raise web.HTTPError(404)
        status = yield self.application.storage.is_bucket(bucket_name, object_name)
        if status:
            raise web.HTTPError(403)
        self.application.storage.write_object(bucket_name, object_name, body)
        self.finish()

    @defer.inlineCallbacks
    @web.asynchronous
    def delete(self, bucket_name, object_name):
        log.msg('Deleting object %s from bucket %s' % (object_name, bucket_name))
        object_name = urllib.unquote(object_name)
        status = yield self.application.storage.is_object(bucket_name, object_name)
        if not status:
            raise web.HTTPError(404)
        self.application.storage.delete_object(bucket_name, object_name)
        self.set_status(204)
        self.finish()

