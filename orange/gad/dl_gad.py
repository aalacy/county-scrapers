import json
import img2pdf
import uuid
import os
from tqdm import tqdm
from scraper_api import ScraperAPIClient
from boto3.s3.transfer import S3Transfer
import boto3
import requests
com_file_name = 'completed.txt'
try:
    with open(com_file_name) as f:
        dl_before = set(f.read().split('\n'))
except FileNotFoundError:
    dl_before = set()
client = ScraperAPIClient('92975321be6c2765177bcc543bd86790')
access_key = '**L5G5Y'
secret_key = '**+6GmR0HjyZ4oznpWVCj/n'
boto_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
transfer = S3Transfer(boto_client)
with open('all_results.json') as f:
    di2 = json.load(f)
headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
}
ses = requests.Session()
ses.headers = headers
ses.proxies['http'] = 'socks5h://localhost:9050'
ses.proxies['https'] = 'socks5h://localhost:9050'
for i in tqdm(di2[9073:]):
    try:
        temp = []
        if not bool(int(i['NumberOfPages'])) or i['InstrumentID'] in dl_before:
            continue
        for i2 in range(1, int(i['NumberOfPages'])+1):
            url = f"http://www.gadsdenclerk.com/publicinquiry/CreateImage.aspx?InstrumentID={i['InstrumentID']}&Page={i2}"
            result = client.get(url=url).text
            image_url = "http://www.gadsdenclerk.com/publicinquiry/" + result
            fn = str(uuid.uuid4()) + ".jpg"
            print(url, image_url)
            with open(fn, 'wb') as f:
                f.write(ses.get(url=image_url).content)
            temp.append(fn)
        oo = i['PDF_fileName'].replace('.json', '.pdf')
        with open(oo, 'wb') as f:
            print(temp)
            f.write(img2pdf.convert(temp))
        for i3 in temp:
            os.remove(i3)
        transfer.upload_file(oo, 'amjadupwork', "gadsden" + "/" + oo)
        os.remove(oo)
        dl_before.add(i['InstrumentID'])

    except:
        pass


with open(com_file_name, 'w') as f:
    for i in dl_before:
        f.write(f'{i}\n')
print("Completed")
