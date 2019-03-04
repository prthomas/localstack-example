import boto3
import os
import unittest

from example import Boto3Client
from example import s3ToDynamodb
from localstack.constants import DEFAULT_SERVICE_PORTS
from localstack.services import infra

class TestExample(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.startservices = ['s3', 'dynamodb']
        infra.start_infra(asynchronous=True, apis=self.startservices)
        os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
        for service in self.startservices:
            os.environ['{}_endpoint_url'.format(service)] = ''.join(['http://',
                       '127.0.0.1:',
                       str(DEFAULT_SERVICE_PORTS[service])])

    @classmethod
    def tearDownClass(self):
        infra.stop_infra()
    
    def test_1(self):
        s3 = Boto3Client('s3').client
        s3.create_bucket(Bucket='my-bucket')
        with open('gdata.csv', 'rb') as data:
            s3.upload_fileobj(data, 'my-bucket', 'gdata.csv')
        for key in s3.list_objects(Bucket='my-bucket')['Contents']:
            self.assertTrue(key['Key'] == 'gdata.csv')

        dynamodb = Boto3Client('dynamodb').client
        dynamodb.create_table(
            TableName='gdata',
            KeySchema=[
                {
                    'AttributeName': 'sales',
                    'KeyType': 'HASH'  #Partition key
                },
                {
                    'AttributeName': 'business',
                    'KeyType': 'RANGE'  #Sort key
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

        s3ToDynamodb()
        self.assertTrue(dynamodb.describe_table(TableName='gdata')['Table']['ItemCount'] == 30)


if __name__ == '__main__':
    unittest.main()
