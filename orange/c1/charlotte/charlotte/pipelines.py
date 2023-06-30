# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import datetime
import re


class CharlottePipeline:
    def process_item(self, item, spider):
        timestamp= re.search(r'\d+', item['RecordData']).group()
        item['RecordData'] = datetime.fromtimestamp(int(int(timestamp)/1000)).strftime('%m/%d/%Y')
        return item
