from __future__ import print_function

import json
import urllib
import boto3

s3 = boto3.client('s3')
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import csv
import sys
import os
import gzip
import collections

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
        if column == 'params':
          data['assigned_date'] = []

    with gzip.open(file_path ,'rb') as tsvin:
        rows = csv.reader(tsvin, delimiter='|')
        for row in rows:
            for index, item in enumerate(row):
                data[columns[index]].append(item)
                if columns[index] == 'params':
                    try:
                        parsed_params = json.loads(item)
                        if parsed_params.has_key('assigned_date'):
                            # dt = pd.to_datetime(parsed_params['assigned_date'], format="%Y-%m-%d")
                            data['assigned_date'].append(parsed_params['assigned_date'])
                        else:
                            data['assigned_date'] = ''
                    except:
                        data['assigned_date'] = ''

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
	    print("environment variable: {}".format(os.environ['DATASETPATH']))
	    print("Key variable: {}".format(key))
	    upload_key = "{}/dataset/month={}/day={}/hour={}/{}.parquet".format(os.environ['DATASETPATH'], key.split('/')[-4], key.split('/')[-3], key.split('/')[-2], file_name.split('.')[0])
            print("upload_key: " + upload_key)
            s3.upload_file(upload_path, bucket, upload_key)
            return True
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e
