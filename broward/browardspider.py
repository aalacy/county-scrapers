from lxml import etree
from datetime import datetime, timedelta
from io import BytesIO
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from utils.useragents import get_random_ua

import requests
import os
import json
import base64
import csv
import re
import string
import threading
import boto3
import secrets
import random
import names
import time
import dateparser

class Spider(threading.Thread):
    """docstring for Spider"""
    thread_spider_status = dict()
    scraped_items = 0
    scraped_items_all = 0
    # lambda_version = 3

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.session = requests.Session()
        self.queue = queue
        self.response = None
        self.thread_id = 0
        self.BUCKET = 'aws-sdc-usage'
        self.s3cli = boto3.client('s3', aws_access_key_id='AKIAT34F3EZ454WXW3NL', region_name='us-east-1', aws_secret_access_key='**Ctyn15kykdT')
        # self.s3cli = boto3.client('s3', aws_access_key_id='AKIAYJCUALZWFGZS6Z6S', region_name='us-east-2', aws_secret_access_key='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')
        self.mongocli = MongoClient('mongodb://mongoadmin:**7@116.203.49.147:27017/')

    # def _change_lambda_api(self):
    #     if self.__class__.lambda_version < 8:
    #         self.__class__.lambda_version += 1
    #     else:
    #         self.__class__.lambda_version = 1

    def _create_new_session(self):
        self.session = requests.Session()
        self.session.proxies = {'http': 'http://127.0.0.1:8119', 'https': 'http://127.0.0.1:8119'}
        self.need_create_account = True
        return True

    def _open(self, url, method='GET', data={}):
        ua = get_random_ua()
        headers = {
            'User-Agent': ua,
            'Accept': 'text/html, */*; q=0.01',
            'Accept-Language': 'uk-UA,uk;q=0.8,en-US;q=0.5,en;q=0.3',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
            'Referer': 'https://officialrecords.broward.org/AcclaimWeb/Details/',
        }

        if method == 'GET':
            self.response = self.session.get(url, headers=headers)
        if method == 'POST':
            self.response = self.session.post(url, headers=headers, data=data)

        return self.response

    def _get_thread_status(self):
        value = ''
        if len(set(self.thread_spider_status.values())) == 1:
            value = list(set(self.thread_spider_status.values()))[0]       
        return value

    def _write_error_download_ids(self, r):
        with open('error_broward_files.txt', 'a') as file:
            file.write(f'{r}\n')
        file.close()

    def run(self):
        #time.sleep(random.choice(list(range(1, 30))))

        while True:
            r = self.queue.get()
            self._try_search(r, 2)
            self.queue.task_done()

    def _try_search(self, r, i):
        c = 0
        print(':::r>> ', r)
        while c < i:
            #try:
            resp = self._start(r)
            #print('resp1>>>>', resp)
            #except:
            #    print('except')
            #    resp = None

            #finally:
            #    time.sleep(10)
            #    c += 1

            if resp:
                break
            c += 1

        if c >= i:
            pass
            #time.sleep(5)
            #self._write_error_download_ids(r)

    def _start(self, r):
        #40560488
        url = f'https://officialrecords.broward.org/AcclaimWeb/Image/DocumentPdfAllPages/{r}'
        ua = get_random_ua()
        headers = {
            'User-Agent': ua,
        }
        
        while True:
            try:
                self.response = self.session.get(url, headers=headers)
                break
            except:
                print("R>>>>>", r)
                time.sleep(60)

        if self.response.status_code == 503:
            time.sleep(600)
            print(f':: WARNING :: {url} Status 503')
            self._write_error_download_ids(r)
            return False

        self.__class__.scraped_items_all += 1
        if self.response.headers.get('Content-Type') == 'application/pdf':
            self.thread_spider_status[self.thread_id] = 'ok'
 
            self.__class__.scraped_items += 1
            data = self.parse_html(r)
            self.save_to_mongo(data)
            self.download_file(data['filename'])

            print(f':: OK :: {self.__class__.scraped_items} :: {self.__class__.scraped_items_all} :: {url}, :: DATA > {data}')
            return True

        elif self.response.headers.get('Content-Type') == 'text/html; charset=utf-8':
            print(":: INFO ::", self.response.content)
            return '2002-302' in self.response.content.decode('utf-8')

        else:
            print('error')
            return False

    def download_file(self, name):
        tmp_name = f"/tmp/ramdisk/{name}"
        with open(tmp_name, 'wb') as file: 
            file.write(self.response.content) 
        file.close()

        self.upload_dump_to_s3(tmp_name, name)
        self.remove_temp_files(tmp_name)

    def upload_dump_to_s3(self, tmp_name, name):
        year = name.split('-')[0]
        bucketkey = 'broward/{}/{}'.format(year, name)
        self.s3cli.upload_file(
            Filename=tmp_name,
            Bucket=self.BUCKET,
            Key=bucketkey)

    def remove_temp_files(self, filename):
        os.remove(filename)


    def _clean_value_data(self, data):
        if not data:
            return ''

        if len(data) > 1:
            data = ' \n'.join(data)
        else:
            data = data[0] 

        clean_data = [t.strip().replace(':', '') for t in data.splitlines() 
            if t.strip().replace(':', '')]

        if len(clean_data) > 1:
            clean_data = ' \n'.join(clean_data)
        elif len(clean_data) == 0:
            clean_data = ''
        else:
            clean_data = clean_data[0]
        
        return clean_data

    def _clean_key_data(self, data):
        if not data:
            return ''

        if len(data) > 1:
            data = ' \n'.join(data)
        else:
            data = data[0] 

        clean_data = [t.strip().replace(':', '').replace('.', '').replace('#', '') 
            for t in data.splitlines()
            if t.strip().replace(':', '').replace('.', '').replace('#', '')]

        return clean_data[0] if clean_data else None


    def parse_html(self, r):
        url = f'https://officialrecords.broward.org/AcclaimWeb/details/documentdetails/{r}'
        response = self.session.get(url)
        # print('resp2:: ', response.headers)
        html = etree.HTML(response.content)
        data = dict()
        year = None
        instrument = None
        for i in html.xpath('//div[@class="docDetailRow"]'):
            key = self._clean_key_data(i.xpath('div[@class="detailLabel"]/text()'))
            value = self._clean_value_data(
                i.xpath('div[@class="formInput"]/a/text()') or \
                i.xpath('div[@class="formInput"]/span/text()') or \
                i.xpath('div[@class="formInput"]/text()') or \
                i.xpath('div[@class="listDocDetails"]/a/text()') or \
                i.xpath('div[@class="listDocDetails"]/span/text()') or \
                i.xpath('div[@class="listDocDetails"]/text()')             
                )
            if not key:
                continue
            else:
                key = key.strip()

            if key == 'Book Type':
                value =  ' '.join([v.strip() for v in value.splitlines()])
            if key == 'Record Date':
                #dt = dateparser.parse(value)
                #print("value>>", value[:-10], '  dt>> ',  dt)
                #dt = datetime.strptime('12/31/1997 41200 PM', '%m/%d/%Y %I%M%S %p')
                if len(value.strip()) > 10:
                    dt = datetime.strptime(value.strip(), '%m/%d/%Y %I%M%S %p')
                else:
                    dt = datetime.strptime(value.strip(), '%m/%d/%Y')
                year = dt.year
            if key == 'Instrument':
                instrument = value

            data[key] = value
        data['filename'] = f'{year}-{instrument}.pdf'
        return data

    def save_to_mongo(self, data):
        # print(":: DATA :: ", data)
        name = data.get('filename')
        year = name.split('-')[0]
        collection = '{}'.format(year)

        db = self.mongocli['broward']
        col = db[collection]
        col.create_index("filename", unique=True)
        try:
            col.insert_one(data).inserted_id
        except DuplicateKeyError:
            pass
        return True
