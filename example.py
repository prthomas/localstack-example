import boto3
import codecs
import os

class Boto3Client:
    def __init__(self, Service, Resource=False):
        endpointurl = os.environ.get("{}_endpoint_url".format(Service))
        if(not Resource):
            if(endpointurl):    
                self.client = boto3.client(Service, endpoint_url=endpointurl)
            else:
                self.client = boto3.client(Service)
        else:
            if(endpointurl):
                self.client = boto3.resource(Service, endpoint_url=endpointurl)
            else:
                self.client = boto3.resource(Service)

        

def s3ToDynamodb():
    print('Within s3ToDynamodb function')
    s3 = Boto3Client('s3').client
    dynamodb = Boto3Client('dynamodb', Resource=True).client
    table = dynamodb.Table('gdata')
    s3file = s3.get_object(Bucket='my-bucket', Key='gdata.csv')
    with table.batch_writer() as gdatawriter:
        with codecs.getreader('utf-8')(s3file['Body']) as filestream:
            for line in filestream:
                cols = line.split(',')
                gdatawriter.put_item({
                    'sales': cols[3],
                    'business': cols[1],
                    'line': line
                })
