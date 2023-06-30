import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess
import requests
import os
import base64
import json
import csv
import boto3
from uuid import uuid1
from lxml import etree
from clint.textui import progress
import random
from pymongo import MongoClient
import pdb

class BaySpider(CrawlSpider):
	name = 'Bay'

	meta = {
		"proxy": "37.48.118.90:13042"
	}
	
	base_url = 'https://qpublic.schneidercorp.com'
	url_pattern = 'https://qpublic.schneidercorp.com/Application.aspx?AppID=834&LayerID=15170&PageTypeID=4&PageID=6825&Q=2345234&KeyValue={}'
	NAL_URL = './data/NAL13F202001.csv'
	
	AWS_BUCKET = 'aws-sdc-usage'
	AWS_ACCESS_KEY_ID = 'AKIAT34F3EZ454WXW3NL'
	AWS_SECRET_ACCESS_KEY = '**Ctyn15kykdT'
	AWS_REGION = 'us-west-1'
	
	parcel_ids = []
	data = {}

	def __init__(self):
		super(BaySpider, self).__init__()		

	def _init_session(self):
		self.s3_client = boto3.client('s3', aws_access_key_id=self.AWS_ACCESS_KEY_ID, region_name=self.AWS_REGION, aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY)
		self.session = requests.Session()
		self.proxies = {
			'http': "37.48.118.90:13042",
			'https': "83.149.70.159:13042"
		}
		self.session.proxies = self.proxies
		self.session.mount('https://', requests.adapters.HTTPAdapter(pool_connections=1000000, max_retries=3))

		self.mongocli = MongoClient('mongodb+srv://hbg:**cluster0.uetxd.mongodb.net/:27017')
		self.db = self.mongocli.Bay_County

	def _read_parcel_no(self):
		with open(self.NAL_URL, encoding='utf-8') as csvf: 
			csvReader = csv.DictReader(csvf) 
			for rows in csvReader: 
				self.parcel_ids.append(rows['PARCEL_ID'])

		self.log(f'[Bay] Read Parcel No List. Total {len(self.parcel_ids)}')

	def start_requests(self):
		self._init_session()
		self._read_parcel_no()

		for parcel_id in self.parcel_ids:
			self.cur_parcel_id = parcel_id
			url = self.url_pattern.format(self.cur_parcel_id)
			self.headers = { "User-Agent": self._get_ua() }

			yield scrapy.Request(url=url, callback=self.parse, meta=self.meta, headers=self.headers)
			# yield scrapy.Request(url=url, callback=self.parse, headers=self.headers)


	def _save_to_mongo(self):
		# print(":: DATA :: ", data)
		collection = '_'.join(self.cur_parcel_id.split('-'))

		col = self.db['Tax_Properties']
		try:
			col.insert_one(self.data).inserted_id
		except Exception as E:
			self.log(str(E))
		return True

	def parse(self, response):
		self.data = {
			'folio': self.cur_parcel_id,
			'url': self.url_pattern.format(self.cur_parcel_id)
		}

		# Parcel Summary
		self._parse_parcel_summary(response)

		# Owner Information
		self._parse_owner_info(response)

		# Valuation
		self._parse_valuation(response)

		# Condo Information
		self._parse_condo_info(response)

		# Building Information
		self._parse_building_data(response)

		# Permits
		self._parse_permits(response)

		#  Extra Features
		self._parse_extra_features(response)

		# Sales
		self._parse_sales(response)

		# Area Sales Report
		self._parse_area_sales_report(response)

		# Generate Owner List by Radius
		self._parse_generate_owner_list_by_radius(response)

		# Card View
		self._parse_card_view(response)

		# Assessment Notice
		self._parse_assessment_notice(response)

		# Tax Collector
		self._parse_tax_collector(response)

		# Sketches
		self._download_sketches(response)

		# Map

		# Normalize keys 
		self._normalizeKeys(self.data)

		# Save data to MongoDB
		self._save_to_mongo()

	def _get_ua(self):
		user_agent_list = ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0 Safari/605.1.15",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.3 Safari/605.1.15",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/601.7.8 (KHTML, like Gecko) Version/11.1.2 Safari/605.3.8",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.2 Safari/605.1.15",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/604.5.6 (KHTML, like Gecko) Version/11.0.3 Safari/604.5.6",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/604.3.5 (KHTML, like Gecko) Version/11.0.1 Safari/604.3.5",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.1 Safari/603.1.30",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.3538.77 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4083.0 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4083.0 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4078.2 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.43 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.136 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.106 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.106 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:75.0) Gecko/20100101 Firefox/75.0",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:75.0) Gecko/20100101 Firefox/75.0",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:74.0) Gecko/20100101 Firefox/74.0",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:74.0) Gecko/20100101 Firefox/74.0",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:74.0) Gecko/20100101 Firefox/74.0",
			"Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0",
			"Mozilla/5.0 (Windows NT 10.0; rv:76.0) Gecko/20100101 Firefox/76.0",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0",
			"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0",
			"Mozilla/5.0 (Windows NT 6.2; rv:74.0) Gecko/20100101 Firefox/74.0",
			"Mozilla/5.0 (Windows NT 10.0; ) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4086.0 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; ) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.4 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.6 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.5 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.2 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.2 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4085.6 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4083.0 Safari/537.36",
			"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4068.4 Safari/537.36",
			"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.62 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.62 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.62 Safari/537.36",
			"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
			"Mozilla/5.0 (Windows NT 6.3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36"]

		user_agent = random.choice(user_agent_list)
		return user_agent

	def _extract_validate(self, el):
		res = ''
		try:
			val = el.extract_first()
			if val:
				res =  val.strip()
		except:
			self.log(f'Cannot get the text')

		return res

	def _valid(self, val):
		if val:
			return val.strip()
		else:
			return ''

	def _strip_list(self, arr):
		new_list = []
		for item in arr:
			if item == '\xa0' or item.strip():
				new_list.append(item.strip())

		return new_list

	def _strip_list1(self, arr):
		new_list = []
		for item in arr:
			if item.strip():
				new_list.append(item.strip())

		return new_list

	def _get_viewstategenerator(self, response):
		return response.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').extract_first()

	def _get_viewstate(self, response):
		return response.xpath('//input[@id="__VIEWSTATE"]/@value').extract_first()

	def _get_form_action(self, response):
		action = response.xpath('//form[@id="Form1"]/@action').extract_first()
		return f"{self.base_url}{action[1:]}"

	def _parse_table(self, response, table_id):
		res = []
		# headers
		table = response.xpath(f"//table[@id='{table_id}']")
		temp_headers = self._strip_list(table.xpath('.//thead//text()').extract())
		headers = temp_headers
		
		# values
		value_trs = table.xpath('.//tbody//tr')
		for tr in value_trs:
			values = self._strip_list(tr.xpath('.//text()').extract())
			res.append(dict(zip(headers, values)))

		return res

	def _parse_table_by_xtree(self, response, table_id):
		res = []
		# headers
		table = response.xpath(f"//table[@id='{table_id}']")
		if table:
			temp_headers = self._strip_list1(table[0].xpath(f".//thead//text()"))
			headers = temp_headers
		
			# values
			value_trs = table[0].xpath('.//tbody//tr')
			for tr in value_trs:
				values = self._strip_list(tr.xpath('.//text()'))
				res.append(dict(zip(headers, values)))

		return res

	def _parse_table_by_section_id(self, response, section_id):
		res = []
		table = response.xpath(f"//section[@id='{section_id}']//table")
		if table:
			keys = self._strip_list1(table.xpath('.//th//text()').extract())
			values = self._strip_list1(table.xpath('.//td//text()').extract())

			res.append(dict(zip(keys, values)))

		return res

	def _download_file(self, link, file_name=None):
		vid = self.session.get(link, stream=True)
		try:
			if vid.status_code == 200:
				temp_filename = f'/tmp/bay_{uuid1()}'
				with open(temp_filename, "wb") as f:
					if vid.headers.get('content-length'):
						total_size = int(vid.headers.get('content-length'))
						for chunk in progress.bar(vid.iter_content(chunk_size=1024),
												  expected_size=total_size/1024 + 1):
							if chunk:
								f.write(chunk)
								f.flush()
					else:
						f.write(vid.content)

			name = os.path.basename(link)
			if file_name:
				name = file_name
			self._upload_dump_to_s3(temp_filename, name)
			os.remove(temp_filename)

			return name
		except Exception as E:
			self.log(str(E))

		return ''

	def _upload_dump_to_s3(self, tmp_name, name):
		self.log("Starting upload to Object Storage")
		bucketkey = f'Bay/Tax_Properties/{name}'
		self.s3_client.upload_file(
			Filename=tmp_name,
			Bucket=self.AWS_BUCKET,
			Key=bucketkey)

		self.log(f"[Bay] Uploaded file to s3 {name}")

	def _normalizeKeys(self, obj):
		if type(obj) is list:
			for o in obj:
				self._normalizeKeys(o)
			return
		elif type(obj) is dict:
			keys = list(obj.keys())
			for key in keys:
				newKey = key.lower().replace('.', '').replace(' ', '_').strip()
				obj[newKey] = obj.pop(key)
			for val in obj.values():
				self._normalizeKeys(val)
			return
		else:
			return

	def _parse_section(self, response, sec_id):
		is_exist = False
		section = response.xpath(f'//div[@id="{sec_id}"]/text()').extract()
		if section:
			is_exist = True

		return is_exist

	def _parse_parcel_summary(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl00_lblName"):
			self.log(f'[Bay] Parcel Summary empty')
			return

		self.log(f'[Bay] Parse parcel summary')

		parcel_summary = {}
		tr_list = response.xpath("//section[@id='ctlBodyPane_ctl00_mSection']//table[@class='tabular-data-two-column']//tr")
		try:
			for tr in tr_list:
				children = tr.xpath('.//*[not(*)]//text()').extract()
				if len(children) > 1:
					key = self._valid(children[0])
					value = self._valid(children[1])
					parcel_summary[key] = value
				else:
					parcel_summary[''] = children[0]
		except Exception as E:
			self.log(str(E))

		self.data['parcel_summary'] = parcel_summary

	def _parse_owner_info(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl01_lblName"):
			self.log(f'[Bay] Owner Information empty')
			return

		self.log(f'[Bay] Parse owner information')

		name = self._extract_validate(response.xpath('//span[@id="ctlBodyPane_ctl01_ctl01_lstPrimaryOwner_ctl00_lblPrimaryOwnerName_lblSearch"]//text()'))
		address = self._strip_list1(response.xpath('//span[@id="ctlBodyPane_ctl01_ctl01_lstPrimaryOwner_ctl00_lblPrimaryOwnerAddress"]//text()').extract())
		self.data['owner_info'] = {
			'name': name,
			'address': ' '.join(address)
		}

	def _parse_valuation(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl02_lblName"):
			self.log(f'[Bay] Valuation empty')
			return

		self.log(f'[Bay] Parse valuation')

		valuation = []
		# headers
		table = response.xpath("//table[@id='ctlBodyPane_ctl02_ctl01_grdValuation']")
		headers = table.xpath('.//thead//th/text()').extract()

		# values
		value_trs = table.xpath('.//tbody//tr')
		keys = []
		vals = []
		for tr in value_trs:
			key = self._extract_validate(tr.xpath('./th/text()'))
			keys.append(key)
			main_values = tr.xpath('./td/text()').extract()
			vals.append(dict(zip(headers, main_values)))

		valuation.append(dict(zip(keys, vals)))
		self.data['valuation'] = valuation

	def _download_trim_notice(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl03_lblName"):
			self.log(f'[Bay] TRIM Notice empty')
			return

		self.log(f'[Bay] Parse TRIM notice')

		links = response.xpath("//section[@id='ctlBodyPane_ctl03_mSection']//table//td/input/@onclick").extract()
		trim_pdfs = []
		for link in links:
			_link = link.replace("window.open('", '')[:-3]
			name = self._download_file(_link)
			trim_pdfs.append(name)

		self.data['trim_notice'] = trim_pdfs

	def _parse_land_info(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl06_lblName"):
			self.log(f'[Bay] Lando Information empty')
			return

		self.log(f'[Bay] Lando Information')

		self.data['land_info'] = self._parse_table(response, 'ctlBodyPane_ctl06_ctl01_gvwLand')

	def _parse_condo_info(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl04_lblName"):
			self.log(f'[Bay] Condo Information empty')
			return

		self.log(f'[Bay] Parse condo information')

		self.data['condo_information'] = self._parse_table_by_section_id(response, 'ctlBodyPane_ctl04_mSection')

	def _parse_building_data(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl03_lblName"):
			self.log(f'[Bay] Building Data empty')
			return

		self.log(f'[Bay] Parse building Data')

		self.data['building_data'] = self._parse_table_by_section_id(response, "ctlBodyPane_ctl03_mSection")

	def _parse_permits(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl09_lblName"):
			self.log(f'[Bay] Permits empty')
			return

		self.log(f'[Bay] Parse permits')

		self.data['permits'] = self._parse_table(response, 'ctlBodyPane_ctl09_ctl01_grdPermits')

	def _parse_extra_features(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl05_lblName"):
			self.log(f'[Bay] Extra Features empty')
			return

		self.log(f'[Bay] Extra Features permits')

		self.data['extra_features'] = self._parse_table(response, 'ctlBodyPane_ctl05_ctl01_gvwExtraFeatures')

		
	def _parse_sales(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl07_lblName"):
			self.log(f'[Bay] Sales empty')
			return

		self.log(f'[Bay] Parse sales')

		self.data['sales'] = self._parse_table(response, 'ctlBodyPane_ctl08_ctl01_grdSales')

	def _parse_area_sales_report(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl08_lblName"):
			self.log(f'[Bay] Area Sales Report empty')
			return

		self.log(f'[Bay] Parse area sales report')

		url = self._get_form_action(response)

		VIEWSTATE = self._get_viewstate(response)
		VIEWSTATEGENERATOR = self._get_viewstategenerator(response)

		ua = self._get_ua()
		referer = self.url_pattern.format(self.cur_parcel_id)
		headers = {
			'user-agent': ua,
			'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
			'accept-language': 'en-US,en;q=0.9',
			'referer': referer,
			'content-type': 'application/x-www-form-urlencoded',
			'origin': 'https://qpublic.schneidercorp.com',
			'upgrade-insecure-requests': '1',
			'sec-fetch-dest': 'document',
			'sec-fetch-mode': 'navigate',
			'sec-fetch-site': 'same-origin',
			'sec-fetch-user': '?1'
		}

		form_data = {
			'__EVENTTARGET': '',
			'__EVENTARGUMENT': '',
			'__VIEWSTATE': VIEWSTATE,
			'__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
			'ctlBodyPane$ctl08$ctl01$txtStartDate': '2017-11-06',
			'ctlBodyPane$ctl08$ctl01$txtEndDate': '2020-11-06',
			'ctlBodyPane$ctl08$ctl01$txtDistance': '1500',
			'ctlBodyPane$ctl08$ctl01$ddlUnits': '1',
			'ctlBodyPane$ctl08$ctl01$btnRecentSalesInArea': 'Sales by Distance',
			'ctlBodyPane$ctl08$ctl01$hdnNbhd': '',
			'ctlBodyPane$ctl08$ctl01$hdnSubDiv': '',
			'ctlBodyPane$ctl08$ctl01$hdnLon': '-85.8280940396637',
			'ctlBodyPane$ctl08$ctl01$hdnLat': '30.1886982406327',
			'ctlBodyPane$ctl08$ctl01$hdnParcelIDGroup': '348',
			'ctlBodyPane$ctl08$ctl01$hdnParcelIDGroup2': '',
			'ctlBodyPane$ctl11$ctl01$txtDistance': '100',
			'ctlBodyPane$ctl11$ctl01$ddlUnits': '1',
			'ctlBodyPane$ctl11$ctl01$Address': 'rbShowOwner',
			'ctlBodyPane$ctl11$ctl01$ddlFileTypes': '5160',
			'ctlBodyPane$ctl11$ctl01$chkShowAllOwners': 'on',
			'ctlBodyPane$ctl11$ctl01$txtSkipLabels': '0'
		}

		try:
			res = self.session.post(url, data=form_data, headers=headers)
			url = f"{self.base_url}{res.history[0].headers.get('location')}"
			_response = self.session.get(url, headers=headers).content
			self.data['area_sales_report'] = self._parse_table_by_xtree(etree.HTML(_response), 'ctlBodyPane_ctl00_ctl01_gvwSalesResults')
		except Exception as E:
			self.log(str(E))

	def _parse_generate_owner_list_by_radius(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl11_lblName"):
			self.log(f'[Bay] Generate Owner List by Radius empty')
			return

		self.log(f'[Bay] Parse Generate Owner List by Radius')

		url = self._get_form_action(response)

		VIEWSTATE = self._get_viewstate(response)
		VIEWSTATEGENERATOR = self._get_viewstategenerator(response)

		ua = self._get_ua()
		referer = self.url_pattern.format(self.cur_parcel_id)
		headers = {
			'user-agent': ua,
			'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
			'accept-language': 'en-US,en;q=0.9',
			'referer': referer,
			'content-type': 'application/x-www-form-urlencoded',
			'origin': 'https://qpublic.schneidercorp.com',
			'upgrade-insecure-requests': '1',
			'sec-fetch-dest': 'document',
			'sec-fetch-mode': 'navigate',
			'sec-fetch-site': 'same-origin',
			'sec-fetch-user': '?1'
		}

		form_data = {
			'__EVENTTARGET': '',
			'__EVENTARGUMENT': '',
			'__VIEWSTATE': VIEWSTATE,
			'__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
			'ctlBodyPane$ctl08$ctl01$txtStartDate': '2017-11-06',
			'ctlBodyPane$ctl08$ctl01$txtEndDate': '2020-11-06',
			'ctlBodyPane$ctl08$ctl01$txtDistance': '1500',
			'ctlBodyPane$ctl08$ctl01$ddlUnits': '1',
			'ctlBodyPane$ctl08$ctl01$btnRecentSalesInArea': 'Sales by Distance',
			'ctlBodyPane$ctl08$ctl01$hdnNbhd': '',
			'ctlBodyPane$ctl08$ctl01$hdnSubDiv': '',
			'ctlBodyPane$ctl08$ctl01$hdnLon': '-85.8280940396637',
			'ctlBodyPane$ctl08$ctl01$hdnLat': '30.1886982406327',
			'ctlBodyPane$ctl08$ctl01$hdnParcelIDGroup': '348',
			'ctlBodyPane$ctl08$ctl01$hdnParcelIDGroup2': '',
			'ctlBodyPane$ctl11$ctl01$txtDistance': '100',
			'ctlBodyPane$ctl11$ctl01$ddlUnits': '1',
			'ctlBodyPane$ctl11$ctl01$Address': 'rbShowOwner',
			'ctlBodyPane$ctl11$ctl01$ddlFileTypes': '5160',
			'ctlBodyPane$ctl11$ctl01$chkShowAllOwners': 'on',
			'ctlBodyPane$ctl11$ctl01$txtSkipLabels': '0',
			'ctlBodyPane$ctl11$ctl01$btnDownload': 'Download'
		}

		try:
			res = self.session.post(url, data=form_data, headers=headers)
			link = f"{self.base_url}{res.history[0].headers.get('location')}"
			name = link.split('FileName=')[1]
			self._download_file(link, name)
			self.data['generate_owner_list_by_radius'] = name
		except Exception as E:
			self.log(str(E))

	def _parse_card_view(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl12_lblName"):
			self.log(f'[Bay] Card View empty')
			return

		link = self._extract_validate(response.xpath('//section[@id="ctlBodyPane_ctl12_mSection"]//div[@class="module-content"]//a/@href'))
		name = f"{self.cur_parcel_id}_0.pdf"
		self._download_file(link, name)
		self.data['card_view'] = name

	def _parse_assessment_notice(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl13_lblName"):
			self.log(f'[Bay] Assessment Notice empty')
			return

		link = self._extract_validate(response.xpath('//a[@id="ctlBodyPane_ctl12_ctl01_prtrFiles_ctl00_hlkName"]/@href'))
		name = f"{self.cur_parcel_id}_0.pdf"
		self._download_file(link, name)
		self.data['card_view'] = name

	def _parse_tax_collector(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl11_lblName"):
			self.log(f'[Bay] Tax Collector empty')
			return

		self.log(f'[Bay] Parse tax collector')
		account_summary = ''
		amount_due = ''
		account_history = ''
		try:

			link = self._extract_validate(response.xpath('//a[@id="ctlBodyPane_ctl14_ctl01_hlkWebLink"]/@href'))

			_response = etree.HTML(self.session.get(link).content)

			account_summary = self._parse_tax_account_summary(_response)

			amount_due = self._parse_tax_amount_due(_response)

			account_history = self._parse_tax_account_history(_response)

		except:
			pass

		self.data['tax_collector'] = {
			'account_summary': account_summary,
			'amount_due': amount_due,
			'account_history': account_history
		}

	def _parse_tax_account_summary(self, response):
		account_summary = {
			'Real Estate Account': f"#{' '.join(self.cur_parcel_id.split('-'))}"
		}
		account_details = response.xpath('//div[@class="account-header"]//div[contains(@class, "account-detail")]')
		for detail in account_details:
			key = self._valid(detail.xpath('.//div[@class="label"]/text()')[0])
			value = self._valid(detail.xpath('.//div[@class="value"]//text()')[0])
			account_summary[key] = value

		return account_summary

	def _parse_tax_amount_due(self, response):
		amount_due = {}
		table = response.xpath('//div[contains(@class, "bills")]//table')
		values = []
		try:
			if table:
				trs = table[0].xpath('.//tbody//tr[@class="regular"]')
				for tr in trs:
					values.append({
						'BILL': self._valid(tr.xpath('.//th[@class="description"]/a/text()')[0]),
						'AMOUNT DUE': self._valid(tr.xpath('.//td[contains(@class, "balance")]/text()')[0])
					})
				footer_divs = table[0].xpath('.//tfoot//tr/td[contains(@class, "balance")]//div[contains(@class, "row")]/div//text()')
				key = self._valid(footer_divs[0])
				value = self._valid(footer_divs[1])
				amount_due[key] = value
				amount_due['values'] = values
			else:
				amount_due = ' '.join(self._strip_list1(response.xpath('//div[@class="bills"]/div[@class="row"]//text()')))
		except Exception as E:
			self.log(str(E))

		return amount_due

	def _parse_tax_account_history(self, response):
		account_history = []
		table = response.xpath('//div[contains(@class, "bill-history")]//table')
		if table:
			trs = table[0].xpath('.//tbody/tr[@class="regular"]')
			for tr in trs:
				bill = tr.xpath('.//th/a/text()')[0]
				amount = self._valid(tr.xpath('.//td[@class="balance"]//text()')[0])
				status = ' '.join(self._strip_list1(tr.xpath('.//td[contains(@class, "status")]//text()')))
				as_of = ' '.join(self._strip_list1(tr.xpath('.//td[contains(@class, "as-of")]//text()')))
				message = ' '.join(self._strip_list1(tr.xpath('.//td[contains(@class, "message")]//text()')))
				action = self._valid(tr.xpath('.//td[contains(@class, "actions")]//a/@href')[0])
				year = '-'.join(bill.split(' '))
				file_name = f"Bay-Country-Tax-Collector-Real-Estate-{self.cur_parcel_id}-{year}.pdf"
				name = self._download_file(f"https://bay.county-taxes.com{action}", file_name)
				account_history.append({
					'BILL': bill,
					'AMOUNT DUE': amount,
					'STATUS': status,
					'AS OF': as_of,
					'MESSAGE': message,
					'PDF': name
				})

		return account_history

	def _download_sketches(self, response):
		if not self._parse_section(response, "ctlBodyPane_ctl10_lblName"):
			self.log(f'[Bay] Sketches empty')
			return

		self.log(f'[Bay] Parse sketches')

		links = response.xpath("//section[@id='ctlBodyPane_ctl10_mSection']//div[@class='sketch-thumbnail']//img/@src").extract()
		sketches = []
		for link in links:
			name = f'Bay-Sketch-{os.path.basename(link)}.png'
			self._download_file(link, name)
			sketches.append(name)

		self.data['sketches'] = sketches

	def _download_map(self, response):
		pass


if __name__ == '__main__':
	c = CrawlerProcess({
		'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36',
		'DOWNLOAD_DELAY': '.5',
		'COOKIES_ENABLED': 'False',
		'CONCURRENT_REQUESTS_PER_DOMAIN': '10',
		'CONCURRENT_REQUESTS_PER_IP': '10'
	})
	c.crawl(BaySpider)
	c.start()