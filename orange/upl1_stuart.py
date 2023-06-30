import boto3
import numpy as np
import os
import threading
from boto3.s3.transfer import S3Transfer


def upload_image(*images):
    for i in images:
        try:
            transfer.upload_file(i, 'amjadupwork', "occompt" + "/" + i)
            os.remove(i)
        except Exception as e:
            print(e)


access_key = '**L5G5Y'
secret_key = '**+6GmR0HjyZ4oznpWVCj/n'

client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
transfer = S3Transfer(client)
os.chdir('pdf_files')
files = os.listdir('.')
links = list(filter(None, files))
threads = []
chunks = [i.tolist() for i in np.array_split(links, 20) if i.size > 0]
for lst in chunks:
    threads.append(threading.Thread(target=upload_image, args=lst))
for x in threads:
   x.start()
for x in threads:
    x.join()
print("Completed")
print(len(files))
