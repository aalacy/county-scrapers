# -*- coding: utf-8 -*-
import json
import math
import os
import scrapy
from datetime import datetime, timedelta
from urllib.parse import urlencode
import img2pdf
from boto3.s3.transfer import S3Transfer
import boto3
#dl_before = set()
access_key = '**L5G5Y'
secret_key = '**+6GmR0HjyZ4oznpWVCj/n'
client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
transfer = S3Transfer(client)
with open('completed.txt') as f:
    dl_before= set(f.read().split('\n'))

def get_url(url, num):
    payload = {'api_key': '92975321be6c2765177bcc543bd86790', 'url': url, 'session_number': num, 'keep_headers': 'true'}
    proxy_url = 'http://api.scraperapi.com/?' + urlencode(payload)
    return proxy_url


def get_url1(url):
    payload = {'api_key': '92975321be6c2765177bcc543bd86790', 'url': url, 'keep_headers': 'true'}
    proxy_url = 'http://api.scraperapi.com/?' + urlencode(payload)
    return proxy_url


t_post = 'https://recording.charlotteclerk.com/Render/GetDocumentView'
start_date = datetime(year=1921, month=1, day=1)
end_date = datetime.today()
to_scrape = [start_date]
while True:
    start_date += timedelta(days=8)
    to_scrape.append(start_date)
    if start_date > end_date:
        break
to_scrape = to_scrape


class ExampleSpider(scrapy.Spider):
    name = 'first'

    def start_requests(self):
        url = 'https://recording.charlotteclerk.com/Search/DocumentType/'
        yield scrapy.Request(get_url1(url), callback=self.parse,)

    def parse(self, response):
        cok = response.headers.get('Set-Cookie').decode().split(';')[0]
        headers = {
            'authority': 'recording.charlotteclerk.com',
            'accept': '*/*',
            'x-requested-with': 'XMLHttpRequest',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': 'https://recording.charlotteclerk.com',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'accept-language': 'en-US,en;q=0.9',
            'cookie': cok,
        }

        for dt in to_scrape:
            data = {
                'sort': '',
                'page': '1',
                'pageSize': '50',
                'group': '',
                'filter': '',
                'inFromSearch': 'DOCUMENTTYPE',
                'inDirectReverse': '',
                'inFilterCriteria': '',
                'inCompressedName': '',
                'inBookType': '',
                'inBook': '',
                'inPage': '',
                'inStartDate': dt.strftime('%m/%d/%Y'),
                'inEndDate': (dt + timedelta(days=7)).strftime('%m/%d/%Y'),
                'inCaseNumber': '',
                'inInstrumentNumber': '',
                'inLegal': '',
                'inDocumentTypeIds': '2,30,31',
                'inLot': '',
                'inBlock': '',
                'inSection': '',
                'inTownship': '',
                'inRange': '',
                'inUnit': '',
                'inBuilding': '',
                'inWeek': '',
                'inPhase': '',
                'inSubCondo': ''
            }
            yield scrapy.FormRequest(url=get_url1(t_post), callback=self.p2, formdata=data, headers=headers,
                                     meta={'Follow': True, 'data': data, 'headers': headers})


    def p2(self, response):
        follow = response.meta['Follow']
        data = response.meta['data']
        headers = response.meta['headers']
        di = json.loads(response.text)
        if di['Total'] > 50 and follow:
            print(data['inStartDate'], data['inEndDate'], di['Total'])
            for i in range(2, math.ceil(di['Total'] / 50)+1):
                data['page'] = str(i)
                yield scrapy.FormRequest(url=get_url1(t_post), callback=self.p2, formdata=data,
                                         meta={'Follow': False, 'headers': headers, 'data': data}, headers=headers)
        for i in di['Data']:
            doc_id = i['DocumentId']
            if doc_id in dl_before:
                continue
            p_url = 'https://recording.charlotteclerk.com/Render/GetDocumentById?inDocumentId=' + str(doc_id)
            fn = f"{doc_id}_{i['ClerkFileNumber']}_"
            yield scrapy.Request(get_url1(p_url), callback=self.p3,
                                 meta={'fn': fn, 'id': doc_id, 'headers': headers}, headers=headers)

    def p3(self, response):
        fn = response.meta['fn']
        doc_id = response.meta['id']
        headers = response.meta['headers']
        di = json.loads(response.text)
        num_of_pages = di['PageCount']
        dd = {'Request': 'GetDocument', 'App': 'OR', 'DocumentId': doc_id,
              'StartPage': '1', 'EndPage': num_of_pages, 'TotalPages':
                  num_of_pages, 'inIsAuthenticated': 'N'}
        url = 'https://recording.charlotteclerk.com/AjaxHandler.ashx?' + urlencode(dd)
        yield scrapy.Request(get_url1(url), callback=self.p4,
                             meta={'fn': fn, 'num_of_pages': num_of_pages,
                                   'headers': headers, 'doc_id': doc_id, 'item': di}, headers=headers)

    def p4(self, response):
        num_of_pages = response.meta['num_of_pages']
        fn = response.meta['fn']
        doc_id = response.meta['doc_id']
        item = response.meta['item']
        my_json = json.loads(response.text)
        item['fileName'] = fn[:-1] + ".pdf"

        img_di = {'ataladocpage': 0,
                  'atala_docurl': my_json['returnPath'],
                  'atala_doczoom': 0.999,
                  'atala_thumbpadding': 'false'}
        images = []
        for i in range(num_of_pages):
            fn1 = f"{fn}_{i}.png"
            img_di['ataladocpage'] = i
            url = 'https://recording.charlotteclerk.com/WebDocViewerHandler.ashx?' + urlencode(img_di)
            s = f"""curl '{url}' -H 'User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0' -H 'Accept: image/webp,*/*' -H 'Accept-Language: en-US,en;q=0.5' --compressed -H 'DNT: 1' -H 'Connection: keep-alive' -H 'Cookie: {response.request.headers.get('Cookie')}' -H 'Pragma: no-cache' -H 'Cache-Control: no-cache' -H 'TE: Trailers' > {fn1}"""
            os.system(s)
            images.append(fn1)
        with open(item['fileName'], "wb") as f:
            f.write(img2pdf.convert(images))
        transfer.upload_file(item['fileName'], 'amjadupwork', "charlotte1" + "/" + item['fileName'])
        for i in images:
            os.remove(i)
        os.remove(item['fileName'])
        dl_before.add(doc_id)
        yield item

    def close(self, reason):
        with open('completed.txt', 'w') as f:
            for i in dl_before:
                f.write(f"{i}\n")
