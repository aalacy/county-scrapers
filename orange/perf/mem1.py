import threading
import requests
import re
import numpy as np
import os
import uuid
import errno
from random import randint
from my_base import my_stuff
main_file_name = my_stuff.file_name
main_folder_name = my_stuff.folder_name
completed_file = my_stuff.completed

headers = {'User-Agent': "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0"}


def download_image(*urls):
    for di in urls:
        try:
            res = requests.get(di['image_url'], stream=True,headers=headers)
        except (requests.exceptions.ConnectionError, requests.exceptions.MissingSchema):
            continue
        try:
            if res.status_code == 200:
                if not res.text:
                    downloaded.add(di['image_url'])
                    continue
                file_name = di['name']
                with open(file_name, 'wb') as f:
                    for chunk in res.iter_content(1024):
                        if chunk:
                            f.write(chunk)
                downloaded.add(di['image_url'])
            elif res.status_code == 404:
                downloaded.add(di['image_url'])
        except Exception as e:
            if e.errno == errno.ENOSPC:
                break



my_set = set()
with open(main_file_name, 'r') as f:
    for row in f:
        my_set.add(row.split(',')[1].strip())
my_set.remove('image_url')

if os.path.isfile(completed_file):
    with open(completed_file) as f:
        downloaded = f.read().split('\n')[:-1]
else:
    downloaded = []
downloaded = set(downloaded)
seen = set()
seen_links = set()
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
        temp = my_name.split('.jpg')
        my_name = temp[0] + str(randint(1, 9)) + '.jpg'
    seen.add(my_name)
    di = {'name': my_name, 'image_url': i}
    h.append(di)
os.makedirs(main_folder_name, exist_ok=True)
os.chdir(main_folder_name)
links = list(filter(None, h))
threads = []
chunks = [i.tolist() for i in np.array_split(links, 40) if i.size > 0]
for lst in chunks:
    threads.append(threading.Thread(target=download_image, args=lst))
for x in threads:
    x.start()
for x in threads:
    x.join()
print('Done')
with open(completed_file, 'w') as f:
    for i in downloaded:
        f.write(f'{i}\n')
