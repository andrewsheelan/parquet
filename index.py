from __future__ import print_function

import json
import urllib
import boto3

print('Loading function')

s3 = boto3.client('s3')

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import csv

columns = ['uid', 'controller', 'action', 'params', 'format', 'method', 'path', 'status', 'controller_path', 'created_at', 'duration', 'session', 'headers', 'body', 'api_email']

maxInt = sys.maxsize
decrement = True

while decrement:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.

    decrement = False
    try:
        csv.field_size_limit(maxInt)
    except OverflowError:
        maxInt = int(maxInt/10)
        decrement = True

def convert_file(file_path, converted_file_path):
    print(file_path)

    data = collections.OrderedDict()
    for column in columns:
        data[column] = []

    with gzip.open(os.path.join(dirpath, name),'rb') as tsvin:
        rows = csv.reader(tsvin, delimiter='|')
        for row in rows:
            for index, item in enumerate(row):
                data[columns[index]].append(item)

        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, converted_file_path, compression='gzip')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        # Get the object from the event and show its content type
        bucket = record['s3']['bucket']['name']
        key = urllib.unquote_plus(record['s3']['object']['key'].encode('utf8'))
        try:
            file_name = key.split('/')[-1]
            download_path = '/tmp/{}'.format(file_name)
            upload_path = '/tmp/{}'.format(file_name.split('.')[0] + '.parquet')
            s3.download_file(bucket, key, download_path)
            convert_file(download_path, upload_path)
            s3_client.upload_file(upload_path, '{}resized'.format(bucket), key)
            upload_key = 'dataset/month=' + key.split('/')[-4] + '/day=' + key.split('/')[-3] + '/hour=' + key.split('/')[-2] + '/' + key.split('/')[-1].split('.')[0] + '.parquet'
            response = s3_client.upload_file(upload_path, 's3_requests_converted', upload_key)
            print("response: " + response)
            print("upload_key: " + upload_key)
            return True
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e
