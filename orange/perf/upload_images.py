from boto3.s3.transfer import S3Transfer
import boto3, os, threading
import numpy as np
from my_base import my_stuff
main_folder_name = my_stuff.folder_name
folder_to_upload_to = my_stuff.bucket_folder_name


def upload_image(*images):
    for i in images:
        try:
            transfer.upload_file(i, 'vip001', folder_to_upload_to + "/" + i)
            os.remove(i)
        except Exception as e:
            print(e)


access_key = 'AKIAW5BNHBK22MHI5ELI'
secret_key = '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'

client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
transfer = S3Transfer(client)
os.chdir(main_folder_name)
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
