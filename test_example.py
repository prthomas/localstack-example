"""
Module to run tests for example
"""
import os
import unittest

import botocore

from localstack.constants import DEFAULT_SERVICE_PORTS
from localstack.services import infra

from boto3_s3 import Boto3S3
from example import get_boto3_resource
from example import s3_to_dynamodb


class TestExample(unittest.TestCase):
    """
    TestExample class includes tests for testing implemented code
        This spins up a localstack process to test AWS services
    """
    @classmethod
    def setUpClass(cls):
        cls.startservices = ['s3', 'dynamodb']
        infra.start_infra(asynchronous=True, apis=cls.startservices)
        os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
        for service in cls.startservices:
            os.environ['{}_endpoint_url'.format(service)] = ''.join([
                'http://',
                '127.0.0.1:',
                str(DEFAULT_SERVICE_PORTS[service])])

    @classmethod
    def tearDownClass(cls):
        infra.stop_infra()

    def test_boto3s3(self):
        """Test s3_to_dynamodb function"""
        boto3s3 = Boto3S3()
        s3rsrc = boto3s3.rsrc
        s3bucket = s3rsrc.Bucket('my-bucket')
        self.assertFalse(s3bucket.creation_date)
        s3bucket.create()
        self.assertTrue(s3bucket.creation_date)

        s3obj = s3rsrc.Object('my-bucket', 'gdata.csv')
        with self.assertRaises(botocore.exceptions.ClientError):
            s3obj.load()

        boto3s3.upload('gdata.csv', 'my-bucket')
        self.assertEqual(s3obj.content_length, 1410)

        with open('gdata.csv', 'r') as actualfile:
            with boto3s3.stream('my-bucket', 'gdata.csv') as s3stream:
                for (a_v, e_v) in zip(actualfile, s3stream):
                    self.assertEqual(a_v, e_v)

        s3obj.delete()
        with self.assertRaises(botocore.exceptions.ClientError):
            s3obj.load()

        s3obj2 = s3rsrc.Object('my-bucket', 'relpath/gdata.csv')
        with self.assertRaises(botocore.exceptions.ClientError):
            s3obj2.load()

        boto3s3.upload('gdata.csv', 'my-bucket', 'relpath')
        self.assertEqual(s3obj2.content_length, 1410)
        s3obj2.delete()
        with self.assertRaises(botocore.exceptions.ClientError):
            s3obj2.load()

    def test_s3_to_dynamodb(self):
        """Test s3_to_dynamodb function"""
        boto3s3 = Boto3S3()
        boto3s3.upload('gdata.csv', 'my-bucket')
        dynamodb = get_boto3_resource('dynamodb')
        dynamodb.create_table(
            TableName='gdata',
            KeySchema=[
                {
                    'AttributeName': 'sales',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'business',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'sales',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'business',
                    'AttributeType': 'S'
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )

        s3_to_dynamodb()
        dynamotbl = dynamodb.Table('gdata')
        self.assertEqual(dynamotbl.item_count, 30)
        self.assertEqual(
            dynamotbl.get_item(
                Key={
                    'sales': '147327000000',
                    'business': '44000'
                },
                AttributesToGet=['line']
            )['Item']['line'],
            'Seasonally Adjusted,44000,1992.03,147327000000')
        self.assertFalse('Item' in dynamotbl.get_item(
            Key={
                'sales': 'notpresentsale',
                'business': '44000'
            },
            AttributesToGet=['line']
        ))

if __name__ == '__main__':
    unittest.main()
