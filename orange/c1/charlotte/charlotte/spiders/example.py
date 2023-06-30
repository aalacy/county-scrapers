# -*- coding: utf-8 -*-
import scrapy
import json
import math, os
from urllib.parse import urlencode
from datetime import datetime, timedelta
from boto3.s3.transfer import S3Transfer
import boto3
import img2pdf
if os.path.isfile('completed.txt'):
    with open('completed.txt') as f:
        dl_before = set(f.read().split('\n'))
else:
    dl_before = set()
access_key = '**L5G5Y'
secret_key = '**+6GmR0HjyZ4oznpWVCj/n'

client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
transfer = S3Transfer(client)



t_post = 'https://recording.charlotteclerk.com/Render/GetDocumentView'
start_date = datetime(year=1921, month=1, day=1)
end_date = datetime.today()
to_scrape = [start_date]
while True:
    start_date += timedelta(days=8)
    to_scrape.append(start_date)
    if start_date > end_date:
        break
to_scrape = to_scrape[4500:]
my_errs = set()


def get_url(url, num):
    payload = {'api_key': '92975321be6c2765177bcc543bd86790', 'url': url, 'session_number': num, 'keep_headers': 'true'}
    proxy_url = 'http://api.scraperapi.com/?' + urlencode(payload)
    return proxy_url


class ExampleSpider(scrapy.Spider):
    name = 'example'

    def start_requests(self):
        url = 'https://recording.charlotteclerk.com/Search/DocumentType/'
        for i in range(len(to_scrape) + 1):
            yield scrapy.Request(get_url(url, i + 1), callback=self.parse, dont_filter=True,
                                 meta={'ses': i + 1, 'dt': to_scrape[i]})

    def parse(self, response):
        ses = response.meta['ses']
        dt = response.meta['dt']
        try:
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
            yield scrapy.FormRequest(url=get_url(t_post, ses), callback=self.p2,
                                     meta={'data': data, 'headers': headers, 'ses': ses, 'Follow': True}, formdata=data,
                                     headers=headers)
        except AttributeError:
            my_errs.add(dt.strftime('%m/%d/%Y'))

    def p2(self, response):
        follow = response.meta['Follow']
        if follow:
            data = response.meta['data']
        headers = response.meta['headers']
        ses = response.meta['ses']
        di = json.loads(response.text)
        x = 50
        if di['Total'] > x and follow:
            for i in range(2, math.ceil(di['Total'] / 50) + 1):
                data['page'] = str(i)
                yield scrapy.FormRequest(url=get_url(t_post, ses), callback=self.p2,
                                         meta={'Follow': False, 'ses': ses}, formdata=data, headers=headers)
        for i in di['Data']:
            item = {'Status': i['Status'], 'Direct': i['DirectReverse'], 'Reverse': i['Reverse'],
                    'Book': i['BookNumber'],
                    'Page': i['PageNumber'], 'FileNumber': i['ClerkFileNumber'], 'RecordData': i['RecordDate'],
                    'Description': i['DocTypeDescription'], 'Legal': i['Legal']}
            # yield item
            doc_id = i['DocumentId']
            if doc_id in dl_before:
                continue
            p_url = 'https://recording.charlotteclerk.com/Render/GetDocumentById?inDocumentId=' + str(doc_id)
            fn = f"{doc_id}_{i['ClerkFileNumber']}_"
            yield scrapy.Request(get_url(p_url, ses), callback=self.p3,
                                 meta={'fn': fn, 'id': doc_id, 'ses': ses, 'headers': headers}, headers=headers)

    def p3(self, response):
        fn = response.meta['fn']
        doc_id = response.meta['id']
        ses = response.meta['ses']
        headers = response.meta['headers']
        di = json.loads(response.text)
        item = di
        num_of_pages = di['PageCount']
        dd = {'Request': 'GetDocument', 'App': 'OR', 'DocumentId': doc_id,
              'StartPage': '1', 'EndPage': num_of_pages, 'TotalPages':
                  num_of_pages, 'inIsAuthenticated': 'N'}

        url = 'https://recording.charlotteclerk.com/AjaxHandler.ashx?' + urlencode(dd)
        yield scrapy.Request(get_url(url, ses), callback=self.p4,
                             meta={'fn': fn, 'num_of_pages': num_of_pages, 'ses': ses,
                                   'headers': headers, 'doc_id': doc_id, 'item': item}, headers=headers)

    def p4(self, response):
        num_of_pages = response.meta['num_of_pages']
        fn = response.meta['fn']
        doc_id = response.meta['doc_id']
        item = response.meta['item']
        my_json = json.loads(response.text)
        item['fileName'] = fn[:-1] + ".pdf"
        yield item
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

        with open(fn[:-1] + ".pdf", "wb") as f:
            f.write(img2pdf.convert(images))
        transfer.upload_file(item['fileName'], 'amjadupwork', "charlotte" + "/" + item['fileName'])
        for i in images:
            os.remove(i)
        os.remove(fn[:-1] + ".pdf")
        dl_before.add(doc_id)

    def close(self, reason):
        with open('my_errors.txt', 'w') as f:
            for i in my_errs:
                f.write(f"{i}\n")

        with open('completed.txt', 'w') as f:
            for i in dl_before:
                f.write(f"{i}\n")
