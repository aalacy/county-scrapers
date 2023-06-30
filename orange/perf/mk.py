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
import csv
downloaded=set()

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


with open('new1.csv') as f:
    h = [{k: v for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)]

print(len(h))
os.makedirs(main_folder_name, exist_ok=True)
os.chdir(main_folder_name)
links = list(filter(None, h))
threads = []
chunks = [i.tolist() for i in np.array_split(links, 20) if i.size > 0]
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
