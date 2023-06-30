from pymongo import MongoClient
import pandas as pd
import boto3
import os

mongocli = MongoClient('mongodb://mongoadmin:**7@116.203.49.147:27017/')
client = boto3.client('s3', aws_access_key_id='AKIAT34F3EZ454WXW3NL', region_name='us-east-1', aws_secret_access_key='**Ctyn15kykdT')
BUCKET = 'miami-dade-sdc'

def upload_dump_to_s3(tmp_name, name):
    # print("Starting upload to Object Storage")
    year = name.split('.')[0]
    bucketkey = '{}/{}'.format(year, name)
    client.upload_file(
        Filename=tmp_name,
        Bucket=BUCKET,
        Key=bucketkey)
    print("Uploaded")

def remove_temp_files(filename):
    os.remove(filename)

def run(year):
    db = mongocli.miamidude
    col = db[year]
    cursor = col.find({})
    df = pd.DataFrame(list(cursor))

    name = '{}.csv.gz'.format(year)
    tmp_name = '/tmp/{}'.format(name)
    df.to_csv(tmp_name, index=False, compression='gzip')

    upload_dump_to_s3(tmp_name, name)
    remove_temp_files(tmp_name)

def main():
    for year in range(2007, 2011):
        print(year)
        run(str(year))

if __name__ == '__main__':
    main()
