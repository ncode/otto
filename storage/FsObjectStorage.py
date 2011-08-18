class ObjectStorage(object):

    def __init__(self, engine = 'file'):
        pass

    def __is_bucket__(self, bucket_name):
        pass

    def __is_object__(self, bucket_name, object_name):
        pass

    def list_buckets(self, bucket_name):
        pass

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
