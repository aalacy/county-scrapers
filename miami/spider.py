import requests
import threading
import time
import sentry_sdk
import argparse

from miami_spider import Spider
from queue import Queue

sentry_sdk.init("http://54d29db6cbd44c38b639b3f4b5a6f833@sentry.codelab.pp.ua:9000/11")
parser = argparse.ArgumentParser(description='Enter spider name <-s>')

def miami(args):
    for date in list(range(1981, 1982)):
        queue = Queue()
        for i in range(1, 80):
            t = Spider(queue)
            if args.l:
                t.relogin = True
            t.startdate = date
            t.thread_id = i
            # t.session.proxies = {'http': 'http://127.0.0.1:8118', 'https': 'http://127.0.0.1:8118'}
            t.setDaemon(True)
            t.start()

            queue.put(i)
        queue.join()

def main():
    parser.add_argument('-s', type=str)
    parser.add_argument('-l', type=str)

    args = parser.parse_args()
    if not args.s:
        print('Enter spider name like as: "python spider.py -s spider_name "')
        return None

    if args.l:
        print("Start spider with login user")

    if args.s == 'miami':
        miami(args)
    else:
        print("Can't find spider '%s'" % args.s)

if __name__ == "__main__":
    main()
