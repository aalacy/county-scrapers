from my_base import my_stuff
import re
import os
import requests
import uuid
import concurrent.futures
import boto3
from boto3.s3.transfer import S3Transfer
main_file_name = my_stuff.file_name
main_folder_name = my_stuff.folder_name
completed_file = my_stuff.completed
folder_to_upload_to = my_stuff.bucket_folder_name
headers = {'User-Agent': "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0"}
access_key = 'AKIAW5BNHBK22MHI5ELI'
secret_key = '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'

client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
transfer = S3Transfer(client)


def download_site(url):
    with requests.get(url['image_url'], headers=headers) as response:
        name = url['name']
        if response.content:
            with open(name, 'wb') as f2:
                f2.write(response.content)
        downloaded.add(url['image_url'])
        if "404 - Not Found" not in response.content:
            transfer.upload_file(name, 'vip001', folder_to_upload_to + "/" + name)
        os.remove(name)

def download_all_sites(sites):
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(download_site, sites)


my_set = set()
with open(main_file_name, 'r') as f:
    for row in f:
        my_set.add(row.split(',')[2].strip())

my_set.remove('image_url')
if os.path.isfile(completed_file):
    with open(completed_file) as f:
        downloaded = f.read().split('\n')[:-1]
else:
    downloaded = []
downloaded = set(downloaded)
seen = set()
h = []
for i in my_set:
    if i.startswith('..'):
        i = "https://www3.wipo.int/designdb"+i[2:]
    if i in downloaded:
        continue
    try:
        my_name = re.search(r'img=\w+/\w+/(.*?)&', i, re.I).groups()[0]
    except AttributeError:
        my_name = ''
    if not my_name:
        try:
            my_name = re.search(r'img=\w+/\w+/(.*?)&?', i, re.I).groups()[0]
        except AttributeError:
            my_name = ''
    if not my_name or not my_name.lower().endswith('.jpg'):
        my_name = str(uuid.uuid4()) + ".jpg"
    while my_name in seen:
        my_name = str(uuid.uuid4()) + ".jpg"
    seen.add(my_name)
    di = {'name': my_name, 'image_url': i}
    h.append(di)


os.makedirs(main_folder_name, exist_ok=True)
os.chdir(main_folder_name)
download_all_sites(h)
os.chdir('..')
with open(completed_file, 'w') as f:
    for i in downloaded:
        f.write(f'{i}\n')
