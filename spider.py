import requests
import threading
import time
import sentry_sdk
import argparse

from queue import Queue

sentry_sdk.init("http://54d29db6cbd44c38b639b3f4b5a6f833@sentry.codelab.pp.ua:9000/11")
parser = argparse.ArgumentParser(description='Enter spider name <-s>')

def miami(args):
    from spiders.miamidude.miamidudespider import Spider
    for date in list(range(1951, 1979)):
        queue = Queue()
        for i in range(1, 80):
            t = Spider(queue)
            if args.l:
                t.relogin = True
            t.startdate = date
            t.thread_id = i
            t.session.proxies = {'http': 'http://127.0.0.1:8118', 'https': 'http://127.0.0.1:8118'}
            t.setDaemon(True)
            t.start()

        for r in range(1, 1000001):
            #if date == 1982 and r < 353400:
            #    continue
            queue.put(r)
        queue.join()

def broward():
    from spiders.broward.browardspider import Spider
    queue = Queue()
    for i in range(1, 20):
        t = Spider(queue)
        t.thread_id = i
        #t.session.proxies = {'http': 'http://127.0.0.1:8118', 'https': 'http://127.0.0.1:8118'}
        t.setDaemon(True)
        t.start()

    for r in range(12350059, 20000000):
        # if date == 2007 and r < 400000:
        #     continue
        # print(f'put: {r}')
        queue.put(r)
    print('join')
    queue.join()

def duval():
    from spiders.duval.duvalspider import Spider
    queue = Queue()
    for i in range(1, 25):
        print(i)
        t = Spider(queue)
        t.thread_id = i
        #t.session.proxies = {'http': 'http://127.0.0.1:8118', 'https': 'http://127.0.0.1:8118'}
        t.setDaemon(True)
        t.start()

    for r in range(5593056, 10000000):
        # if date == 2007 and r < 400000:
        #     continue
        # print(f'put: {r}')
        queue.put(r)
    print('join')
    queue.join()

def pinellas():
    from spiders.pinellas.pinellasspider import Spider
    queue = Queue()
    for i in range(1, 25):
        t = Spider(queue)
        t.thread_id = i
        #t.session.proxies = {'http': 'http://127.0.0.1:8118', 'https': 'http://127.0.0.1:8118'}
        t.setDaemon(True)
        t.start()

    for r in range(158500, 10000000):
        # if date == 2007 and r < 400000:
        #     continue
        # print(f'put: {r}')
        queue.put(r)
    print('join')
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
    elif args.s == 'broward':
        broward()
    elif args.s == 'duval':
        duval()
    elif args.s == 'pinellas':
        pinellas()
    else:
        print("Can't find spider '%s'" % args.s)

if __name__ == "__main__":
    main()
