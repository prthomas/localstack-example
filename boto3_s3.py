"""Module for Boto3S3"""
import codecs
import os

from boto3_resource import get_boto3_resource


class Boto3S3:
    """Boto3S3 has functions `upload` and `stream`"""
    def __init__(self):
        self.rsrc = get_boto3_resource('s3')

    def upload(self, filename, bucket, relative_path=None):
        """
        upload accepts `filename`, `bucket`, and `relative_path` arguments
            if `bucket` does not exists it will be created
            `relative_path` defaults to None
            if `relative_path` is passed, that is the path within the S3 bucket

            throws exception if file does not exists
        """
        basename = os.path.basename(filename)
        if relative_path:
            key = f"{relative_path}/{basename}"
        else:
            key = basename

        s3obj = self.rsrc.Object(bucket, key)
        with open(filename, 'rb') as filestream:
            s3obj.put(Body=filestream)

    def stream(self, bucket, key, encoding='utf-8'):
        """
        stream accepts `bucket`, `key`, and `encoding` arguments
        return stream for S3 object
        """
        body = self.rsrc.Object(bucket, key).get()['Body']
        return codecs.getreader(encoding)(body)
