import logging
import pymongo
import os
import scrapy
import ssl
from scrapy.pipelines.files import FilesPipeline
from urllib.parse import urlparse
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class MiamiTaxFilesPipeline(FilesPipeline):

    def file_path(self, request, response=None, info=None, *, item=None):
        print("Inside MiamiTaxFilesPipeline file_path")
        path = urlparse(request.url).path
        res = ''
        for i in range(3):
            path = os.path.dirname(path)
            res = os.path.basename(path) + '_' + res
        return res[:-1]+".pdf"

    def get_media_requests(self, item, info):
        adapter = ItemAdapter(item)
        print("Inside MiamiTaxFilesPipeline get_media_requests")
        for file_url in adapter['data']["latest_annual_bill"]["pdfurls"]:
            print("about to download: ", file_url)
            yield scrapy.Request(file_url)

    def item_completed(self, results, item, info):
        print("Inside MiamiTaxFilesPipeline item_completed")
        print("results: ", results)
        file_paths = [x['path'] for ok, x in results if ok]
        if not file_paths:
            raise DropItem("Item contains no files")
        adapter = ItemAdapter(item)
        adapter['data']["latest_annual_bill"]["pdfs"] = file_paths
        return item


class MongoPipeline(object):

    collection_name = 'property_taxes'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        # pull in information from settings.py
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        # initializing spider
        # opening db connection
        self.client = pymongo.MongoClient(self.mongo_uri,ssl_cert_reqs=ssl.CERT_NONE)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        # clean up when spider is closed
        self.client.close()

    def process_item(self, item, spider):
        # how to handle each post
        # print("process item: ", item)
        if item:
            # self.db[self.collection_name].insert(dict(item))
            self.db[self.collection_name].update(
                {'folio': item['folio']}, dict(item), upsert=True)
        logging.debug("Item added to MongoDB")
        return item
