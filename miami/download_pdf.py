import json
import os
import sys
import base64
import requests
import boto3
import decimal
import csv
import sentry_sdk
import random
import pdb

from sentry_sdk.integrations.aws_lambda import \
    AwsLambdaIntegration

from lxml import etree

from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')

BUCKET = 'miami-dade-sdc'
#DYNAMODB_TABLE = 'miamidadeclerk'

client = boto3.client('s3', aws_access_key_id='W3NL', region_name='us-west-1', aws_secret_access_key='+nNW4Ctyn15kykdT')
sentry_sdk.init("http://00/11", integrations=[AwsLambdaIntegration()])

def upload_dump_to_s3(tmp_name, name):
    # print("Starting upload to Object Storage")
    year = name.split('-')[0]
    bucketkey = '{}/{}'.format(year, name)
    client.upload_file(
        Filename=tmp_name,
        Bucket=BUCKET,
        Key=bucketkey)
    print("Uploaded")

def get_db():
    dynamo_dbs = ['miamidadeclerk', 'miamidadeclerk_2', 'miamidadeclerk_3', 'miamidadeclerk_3', 'miamidadeclerk_3', 'miamidadeclerk_3']
    db = random.choice(dynamo_dbs)
    return db

def remove_temp_files(filename):
    os.remove(filename)
    print("That's all!")

def save_to_dynamo(data):
    dbname = get_db()
    db = boto3.client('dynamodb', aws_access_key_id='AKIAYJCUALZWFGZS6Z6S', aws_secret_access_key='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')
    db.put_item(Item=data, TableName=dbname)
    return True

def save_to_mongo(data):
    print(":: DATA :: ", data)
    name = data.get('filename')
    year = name.split('-')[0]
    collection = 'collection_{}'.format(year)

    db = client.miamidude
    col = db[collection]
    col.insert_one(data).inserted_id
    return True

def get_ua():
    user_agent_list = ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.3 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/601.7.8 (KHTML, like Gecko) Version/11.1.2 Safari/605.3.8",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/604.5.6 (KHTML, like Gecko) Version/11.0.3 Safari/604.5.6",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/604.3.5 (KHTML, like Gecko) Version/11.0.1 Safari/604.3.5",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.1 Safari/603.1.30",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.3538.77 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4083.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4083.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4078.2 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.43 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.136 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.106 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.106 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:75.0) Gecko/20100101 Firefox/75.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:75.0) Gecko/20100101 Firefox/75.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:74.0) Gecko/20100101 Firefox/74.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:74.0) Gecko/20100101 Firefox/74.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:74.0) Gecko/20100101 Firefox/74.0",
        "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0",
        "Mozilla/5.0 (Windows NT 10.0; rv:76.0) Gecko/20100101 Firefox/76.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0",
        "Mozilla/5.0 (Windows NT 6.2; rv:74.0) Gecko/20100101 Firefox/74.0",
        "Mozilla/5.0 (Windows NT 10.0; ) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4086.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; ) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.4 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.6 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.5 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.2 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.2 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.6 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4083.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4068.4 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.62 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.62 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.62 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36"]

    user_agent = random.choice(user_agent_list)
    return user_agent


def download_pdf(valcfn, count, s, cfn):
    print(':: satart download pdf :: ')
    url = 'https://onlineservices.miami-dadeclerk.com/OfficialRecords/CFNDetailsPDF.aspx/GetData'
    ua = get_ua()
    headers = {
        'User-Agent': ua,
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'uk-UA,uk;q=0.8,en-US;q=0.5,en;q=0.3',
        'Content-Type': 'application/json; charset=utf-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://onlineservices.miami-dadeclerk.com',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
    }

    data = '{"valCfn":%s,"valRed":"False"}' % valcfn
    cookies = {'ASP.NET_SessionId': s}
    response = requests.post(url, headers=headers, data=data, cookies=cookies)

    jsondata=json.loads(response.content).get('d')
    blobdata=base64.b64decode(jsondata)
    tmp_name = f'/tmp/{cfn}-{count}.pdf'
    name = f'{cfn}-{count}.pdf'

    with open(tmp_name, 'wb') as file:
        file.write(blobdata) # or aa=base64.b64decode(json.loads(response.content).get('d'))
    file.close()
    
    pdb.set_trace()
    upload_dump_to_s3(tmp_name, name)
    remove_temp_files(tmp_name)

    return name

def nolambda_handler(qs, r, s, cfn):
    print("qs, r, s, cfn >>>", qs, r, s, cfn)
    ua = get_ua()
    headers = {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'uk-UA,uk;q=0.8,en-US;q=0.5,en;q=0.3',
        'Referer': 'https://onlineservices.miami-dadeclerk.com/officialrecords/StandardSearch.aspx',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
        }
    # TODO implement

    cookies = {'ASP.NET_SessionId': s}
    url = f'https://onlineservices.miami-dadeclerk.com/officialrecords/CFNDetailsPDF.aspx?QS={qs}'
    response = requests.get(url, headers=headers, cookies=cookies)

    data = dict()
    html = etree.HTML(response.content)
    valCfn = html.xpath('//input[@id="hfCfn"]/@value')[0] if html.xpath('//input[@id="hfCfn"]/@value') else None
    if not valCfn:
        
        info = html.xpath('//span[@id="lblError"]/text()')
        if info:
            error_msg = info[0]
            if error_msg.startswith('There seems to be a problem accessing this document'):
                return {'status': 'error', 'message': 'need update session'},
            else:
                return {'status': 'error', 'message': error_msg}
        
        return {'status': 'error', 'message': 'missing cfn'}


    filename = download_pdf(valcfn=valCfn, count=r, s=s, cfn=cfn)
    if not filename:
        return {'status': 'error', 'message': 'file not finded'}



    data['filename'] = filename
    for tr in html.xpath('//table')[0].xpath('./tr'):
        if len(tr.xpath('td')) == 8:
            t1, t2, t3, t4, t5, t6, t7, t8 = tr.xpath('td')
            data[t1.xpath('strong/text()')[0]] = t2.xpath('text()')[0] if t2.xpath('text()') else '' 
            data[t3.xpath('strong/text()')[0]] = t4.xpath('text()')[0] if t4.xpath('text()') else ''
            data[t5.xpath('strong/text()')[0]] = t6.xpath('text()')[0] if t6.xpath('text()') else ''
            data[t7.xpath('strong/text()')[0]] = t8.xpath('text()')[0] if t8.xpath('text()') else ''

        if len(tr.xpath('td')) == 6:
            t1, t2, t3, t4, t5, t6 = tr.xpath('td')
            data[t1.xpath('strong/text()')[0]] = t2.xpath('text()')[0] if t2.xpath('text()') else ''
            data[t3.xpath('strong/text()')[0]] = t4.xpath('text()')[0] if t4.xpath('text()') else ''
            data[t5.xpath('strong/text()')[0]] = t6.xpath('text()')[0] if t6.xpath('text()') else ''

    # is_file_saved_to_db = save_to_dynamo(data)
    is_file_saved_to_db = save_to_mongo(data)
    if is_file_saved_to_db:
        return {'status': 'success'}
    return {'status': 'error', 'message': 'file not saved'}








