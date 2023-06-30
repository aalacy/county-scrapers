from lxml import etree
from datetime import datetime, timedelta
# from spiders.miamidude.download_pdf import nolambda_handler
from io import BytesIO
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

import requests
import os
import json
import base64
import csv
import re
import string
import threading
import boto3
import secrets
import random
import names
import time


DOMAINS = ['gmail.com', 'yahoo.com']
CITIES = ["Aventura","Bal Harbour","Bay Harbor Islands","Biscayne Park",
          "Coral Gables","Cutler Bay","Doral","El Portal","Florida City",
          "Golden Beach","Hialeah","Hialeah Gardens","Homestead",
          "Indian Creek","Key Biscayne","Medley","Miami","Miami Beach",
          "Miami Gardens","Miami Lakes","Miami Shores","Miami Springs",
          "North Bay Village","North Miami","North Miami Beach","Opa-locka",
          "Palmetto Bay","Pinecrest","South Miami","Sunny Isles Beach",
          "Surfside","Sweetwater","Virginia Gardens","West Miami"]
STREETS = ["Main Street","Church Street","Main Street North",
           "Main Street South","Elm Street","High Street","Main Street West",
           "Washington Street","Main Street East","Park Avenue","2nd Street",
           "Walnut Street","Chestnut Street","Maple Avenue","Maple Street",
           "Broad Street","Oak Street","Center Street","Pine Street",
           "River Road","Market Street","Water Street","Union Street",
           "South Street","Park Street","3rd Street","Washington Avenue",
           "Cherry Street","North Street","4th Street","Court Street",
           "Highland Avenue","Mill Street","Franklin Street","Prospect Street",
           "School Street","Spring Street","Central Avenue","1st Street",
           "State Street","Front Street","West Street","Jefferson Street",
           "Cedar Street","Jackson Street","Park Place","Bridge Street",
           "Locust Street","Madison Avenue","Meadow Lane","Spruce Street",
           "Grove Street","Ridge Road","5th Street","Pearl Street","Lincoln Street",
           "Madison Street","Dogwood Drive","Lincoln Avenue","Pennsylvania Avenue",
           "Pleasant Street","4th Street West","Adams Street","Jefferson Avenue",
           "3rd Street West","7th Street","Academy Street","11th Street",
           "2nd Avenue","East Street","Green Street","Hickory Lane","Route 1",
           "Summit Avenue","Virginia Avenue","12th Street","5th Avenue",
           "6th Street","9th Street","Charles Street","Cherry Lane",
           "Elizabeth Street","Hill Street","River Street","10th Street",
           "Colonial Drive","Monroe Street","Valley Road","Winding Way",
           "1st Avenue","Fairway Drive","Liberty Street","2nd Street West",
           "3rd Avenue","Broadway","Church Road","Delaware Avenue","Prospect Avenue",
           "Route 30","Sunset Drive","Vine Street","Woodland Drive","6th Street West",
           "Brookside Drive","Hillside Avenue","Lake Street","13th Street","4th Avenue",
           "5th Street North","College Street","Dogwood Lane","Mill Road","7th Avenue",
           "8th Street","Beech Street","Division Street","Harrison Street",
           "Heather Lane","Lakeview Drive","Laurel Lane","New Street","Oak Lane",
           "Primrose Lane","Railroad Street","Willow Street","4th Street North",
           "5th Street West","6th Avenue","Berkshire Drive","Buckingham Drive",
           "Circle Drive","Clinton Street","George Street","Hillcrest Drive",
           "Hillside Drive","Laurel Street","Park Drive","Penn Street","Railroad Avenue",
           "Riverside Drive","Route 32","Route 6","Sherwood Drive","Summit Street",
           "2nd Street East","6th Street North","Cedar Lane","Creek Road",
           "Durham Road","Elm Avenue","Fairview Avenue","Front Street North",
           "Grant Street","Hamilton Street","Highland Drive","Holly Drive",
           "King Street","Lafayette Avenue","Linden Street","Mulberry Street",
           "Poplar Street","Ridge Avenue","7th Street East","Belmont Avenue",
           "Cambridge Court","Cambridge Drive","Clark Street","College Avenue",
           "Essex Court","Franklin Avenue","Hilltop Road","James Street",
           "Magnolia Drive","Myrtle Avenue","Route 10","Route 29","Shady Lane",
           "Surrey Lane","Walnut Avenue","Warren Street","Williams Street",
           "Wood Street","Woodland Avenue"]

class Spider(threading.Thread):
    """docstring for Spider"""
    thread_spider_status = dict()
    scraped_items = 0
    scraped_items_all = 0
    lambda_version = 1

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.session = requests.Session()
        self.queue = queue
        self.response = None
        self.startdate = 1910
        self.need_create_account = True
        self.iters = self._get_random_iters()
        self.is_logined = False
        self.thread_id = 0 
        self.relogin = False
        self.s3cli = boto3.client('s3', aws_access_key_id='AKIAT34F3EZ454WXW3NL', region_name='us-west-1', aws_secret_access_key='**Ctyn15kykdT')
        self.mongocli = MongoClient('mongodb://mongoadmin:**7@localhost:27017/')
        self.is_finish = False


    def _get_random_iters(self):
        iters = random.choice(list(range(2000, 3000)))
        return iters

    def _change_lambda_api(self):
        if self.__class__.lambda_version < 8:
            self.__class__.lambda_version += 1
        else:
            self.__class__.lambda_version = 1

    def _create_new_session(self):
        self.session = requests.Session()
        self.session.proxies = {'http': 'http://127.0.0.1:8118', 'https': 'http://127.0.0.1:8118'}
        self.need_create_account = True
        return True

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


    def _generate_email(self, first_name, last_name):
        first_name = first_name.replace(' ', '')
        last_name = last_name.replace(' ', '')
        email = "%s-%s%d@%s" % (first_name.lower(), last_name.lower(), random.choice(list(range(1, 100))), random.choice(DOMAINS))
        return email


    def _generate_name(self):
        gender = random.choice(['male', 'female'])
        generatedname = names.get_full_name(gender=gender)
        name = {'sex': gender, 'first_name': generatedname.split()[0], 'last_name': generatedname.split()[1]}
        return name


    def _generate_pass(self):
        alphabet = string.ascii_letters + string.digits
        password = ''.join(secrets.choice(alphabet) for i in range(15))
        return password

    def _generate_num(self, count):
        nums = ''.join(secrets.choice(string.digits) for i in range(count))
        return int(nums) if nums[0] != '0' else int(nums + '0')


    def _get_viewstate(self):
        html = etree.HTML(self.response.content)
        VIEWSTATE = html.xpath('//input[@id="__VIEWSTATE"]/@value')[0]
        return VIEWSTATE

    def _get_viewstategenerator(self):
        html = etree.HTML(self.response.content)
        VIEWSTATEGENERATOR = html.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value')[0]
        return VIEWSTATEGENERATOR

    def _get_eventvalidation(self):
        html = etree.HTML(self.response.content)
        EVENTVALIDATION = html.xpath('//input[@id="__EVENTVALIDATION"]/@value')[0]
        return EVENTVALIDATION

    def _open(self, url, method='GET', data={}):
        ua = self._get_ua()
        headers = {
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'uk-UA,uk;q=0.8,en-US;q=0.5,en;q=0.3',
            'Referer': 'https://www2.miami-dadeclerk.com/PremierServices/registration.aspx',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://www2.miami-dadeclerk.com',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        if method == 'GET':
            self.response = self.session.get(url, headers=headers)
        if method == 'POST':
            self.response = self.session.post(url, headers=headers, data=data)

        return self.response

    def get_user(self):
        # print(":: INFO :: < Get user from list >")
        with open('users.txt', 'r') as f:
            user_string = random.choice(f.readlines())
            user_dict = json.loads(user_string.strip().replace("'", '"'))
            return user_dict

    def login_user(self):
        ua = self._get_ua()
        headers = {
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www2.miami-dadeclerk.com/PremierServices/login.aspx',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://www2.miami-dadeclerk.com',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

        url = 'https://www2.miami-dadeclerk.com/PremierServices/login.aspx'
        self.response = self.session.get(url, headers=headers)

        VIEWSTATE = self._get_viewstate()
        VIEWSTATEGENERATOR = self._get_viewstategenerator()
        EVENTVALIDATION = self._get_eventvalidation()

        user = self.get_user()
        print(f":: INFO :: <relogin response> {self.response} :: Login :: <User {user['username']}>, <Password {user['password']}>, <Email {user['email']}>")
        data = {
          '__EVENTTARGET': '',
          '__EVENTARGUMENT': '',
          '__VIEWSTATE': VIEWSTATE,
          '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
          '__EVENTVALIDATION': EVENTVALIDATION,
          'ctl00$cphPage$txtUserName': user.get('username'),
          'ctl00$cphPage$txtPassword': user.get('password'),
          'ctl00$cphPage$btnLogin': 'Login',
          'ctl00$cphPage$registrationEmail': ''
        }

        self.response = self.session.post(url, headers=headers, data=data)
        self.is_logined = True


    # send userdata step one
    def _reg_new_user_step_one(self, user):
        # print(':: start reg 1 step')
        ua = self._get_ua()
        headers = {
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'uk-UA,uk;q=0.8,en-US;q=0.5,en;q=0.3',
            'Referer': 'https://www2.miami-dadeclerk.com/PremierServices/registration.aspx',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://www2.miami-dadeclerk.com',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        url = 'https://www2.miami-dadeclerk.com/PremierServices/registration.aspx'
        self.response = self.session.get(url, headers=headers)
        # print(':: response reg 1 step, {}'.format(self.response))
        VIEWSTATE = self._get_viewstate()
        VIEWSTATEGENERATOR = self._get_viewstategenerator()
        EVENTVALIDATION = self._get_eventvalidation()

        data = {
          '__EVENTTARGET': 'ctl00$cphPage$btnNext1',
          '__EVENTARGUMENT': '',
          '__LASTFOCUS': '',
          '__VIEWSTATE': VIEWSTATE,
          '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
          '__EVENTVALIDATION': EVENTVALIDATION,
          'ctl00$cphPage$rblRegistrationType': 'Individual',
          'ctl00$cphPage$txtFM1FirstName':  user['first_name'],
          'ctl00$cphPage$txtFM1LastName': user['last_name'],
          'ctl00$cphPage$txtFM1BusinessName': '{}{}'.format(user['first_name'], user['last_name']),
          'ctl00$cphPage$txtFM1Email': user['email'],
          'ctl00$cphPage$txtFM1AreaCode': '{}'.format(self._generate_num(3)),
          'ctl00$cphPage$txtFM1Prefix': '{}'.format(self._generate_num(3)),
          'ctl00$cphPage$txtFM1Phone': '{}'.format(self._generate_num(4)),
          'ctl00$cphPage$txtFM1Ext': '',
          'ctl00$cphPage$txtFM1StreetAddress': '{} {}'.format(random.choice(STREETS) ,self._generate_num(3)),
          'ctl00$cphPage$txtFM1City': random.choice(CITIES),
          'ctl00$cphPage$ddFM1State': 'FL',
          'ctl00$cphPage$txtFM1Zip': '{}'.format(self._generate_num(5))
        }

        self.response = self.session.post('https://www2.miami-dadeclerk.com/PremierServices/registration.aspx', headers=headers, data=data)
        # print(':: response reg 1 step (2), {}'.format(self.response))
        return True

    def _reg_new_user_step_two(self, user):
        # print(':: start reg 2 step')
        ua = self._get_ua()
        url = 'https://www2.miami-dadeclerk.com/PremierServices/registration.aspx'
        headers = {
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'uk-UA,uk;q=0.8,en-US;q=0.5,en;q=0.3',
            'Referer': 'https://www2.miami-dadeclerk.com/PremierServices/registration.aspx',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://www2.miami-dadeclerk.com',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        VIEWSTATE = self._get_viewstate()
        VIEWSTATEGENERATOR = self._get_viewstategenerator()
        EVENTVALIDATION = self._get_eventvalidation()
        data = {
          '__EVENTTARGET': '',
          '__EVENTARGUMENT': '',
          '__LASTFOCUS': '',
          '__VIEWSTATE': VIEWSTATE,
          '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
          '__EVENTVALIDATION': EVENTVALIDATION,
          'ctl00$cphPage$txtAdUserName': user['username'],
          'ctl00$cphPage$txtAdPassword': user['password'],
          'ctl00$cphPage$txtAdConfPassword': user['password'],
          'ctl00$cphPage$txtAdQuestion': 'aaaa',
          'ctl00$cphPage$txtAdAnswer': 'aaaa',
          'ctl00$cphPage$lbtnUserInformation': 'Next'
        }
        self.response = self.session.post(url, headers=headers, data=data)
        # print(':: response reg 2 step, {}'.format(self.response))
        return True

    def _complite_button(self):
        # print(':: complit button')
        ua = self._get_ua()
        url = 'https://www2.miami-dadeclerk.com/PremierServices/registration.aspx'
        headers = {
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'uk-UA,uk;q=0.8,en-US;q=0.5,en;q=0.3',
            'Referer': 'https://www2.miami-dadeclerk.com/PremierServices/registration.aspx',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://www2.miami-dadeclerk.com',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        VIEWSTATE = self._get_viewstate()
        VIEWSTATEGENERATOR = self._get_viewstategenerator()
        EVENTVALIDATION = self._get_eventvalidation()
        data = {
          '__EVENTTARGET': 'ctl00$cphPage$LinkButton1',
          '__EVENTARGUMENT': '',
          '__VIEWSTATE': VIEWSTATE,
          '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
          '__EVENTVALIDATION': EVENTVALIDATION
        }
        self.response = self.session.post(url, headers=headers, data=data)
        # print(':: response comlit button, {}'.format(self.response))
        return True

    def _auth(self, user):
        url='https://www2.miami-dadeclerk.com/PremierServices/login.aspx'
        ua = self._get_ua()
        headers = {
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'uk-UA,uk;q=0.8,en-US;q=0.5,en;q=0.3',
            'Referer': 'https://www2.miami-dadeclerk.com/PremierServices/login.aspx',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://www2.miami-dadeclerk.com',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.response = self.session.get(url, headers=headers)
        VIEWSTATE = self._get_viewstate()
        VIEWSTATEGENERATOR = self._get_viewstategenerator()
        EVENTVALIDATION = self._get_eventvalidation()
        data = {
          '__EVENTTARGET': '',
          '__EVENTARGUMENT': '',
          '__VIEWSTATE': VIEWSTATE,
          '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
          '__EVENTVALIDATION': EVENTVALIDATION,
          'ctl00$cphPage$txtUserName': user['username'],
          'ctl00$cphPage$txtPassword': user['password'],
          'ctl00$cphPage$btnLogin': 'Login',
          'ctl00$cphPage$registrationEmail': ''
        }

        self.response = self.session.post(url, headers=headers, data=data)
        self.is_logined = True
        return True


    def _create_fake_account(self):
        if self.is_logined:
            url = 'https://www2.miami-dadeclerk.com/PremierServices/Logout.aspx'
            self.response = self.session.get(url)
            self.is_logined = False

        if self.relogin:
            self.login_user()
            #self.relogin = False

        else:
            user = self._generate_name()
            user['password'] = self._generate_pass()
            user['email'] = self._generate_email(user['first_name'], user['last_name'])
            user['username'] = '{}{}{}'.format(user['first_name'], user['last_name'], self._generate_num(2))

            print('USER>>', user)

            self._reg_new_user_step_one(user)
            self._reg_new_user_step_two(user)
            response = self._complite_button()
            # response = self._auth(user)
            if response:
                self.is_logined = True
                file = open('users.txt', 'a')
                file.write(f'\n{user}')
            return response

    def _get_thread_status(self):
        value = ''
        if len(set(self.thread_spider_status.values())) == 1:
            value = list(set(self.thread_spider_status.values()))[0]       
        return value

    def _write_error_download_ids(self, id):
        with open('error_files.txt', 'a') as file:
            file.write(f'{self.startdate}:{id}\n')
        file.close()

    def run(self):
        if self.is_finish:
            self.queue.task_done()
        else:
            time.sleep(random.choice(list(range(1, 120))))
  
            while True:
                r = self.queue.get()
                self._try_search(r, 3)
                self.queue.task_done()


    def _try_search(self, r, i):
        c = 0
        while c < i:
            try:
                resp = self._search(r)
            except:
                resp = False
            finally:
                c += 1

            if resp:
                #self.thread_spider_status[self.thread_id] = 'ok'
                break

        if c >= i:
            time.sleep(30)
            self.thread_spider_status[self.thread_id] = 'error'
            #self._write_error_download_ids(r)

    def _search(self, r):
        # print("STATUS: {}".format(self._get_thread_status()))
        if self.__class__.scraped_items > 250000: 
            self._change_lambda_api()
            self.__class__.scraped_items = 1

        if self._get_thread_status() == 'none':
            return True

        time.sleep(random.choice(list(range(1, 4))))

        if self.need_create_account:
            # print('need create account')
            self._create_fake_account()
            self.need_create_account = False

        if self._get_thread_status() == 'error':
            time.sleep(random.choice(list(range(1800, 1900))))
            self.thread_spider_status[self.thread_id] = 'active'
            self.need_create_account = True
            return None

        url = 'https://onlineservices.miami-dadeclerk.com/officialrecords/StandardSearch.aspx'
        ua = self._get_ua()
        headers = {
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'uk-UA,uk;q=0.8,en-US;q=0.5,en;q=0.3',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://onlineservices.miami-dadeclerk.com',
            'Connection': 'keep-alive',
            'Referer': 'https://onlineservices.miami-dadeclerk.com/officialrecords/StandardSearch.aspx',
            'Upgrade-Insecure-Requests': '1',
        }
        self.response = self.session.get(url, headers=headers)
        # print(":: resp 1 :: >>", self.response)
        VIEWSTATE = self._get_viewstate()
        VIEWSTATEGENERATOR = self._get_viewstategenerator()
        EVENTVALIDATION = self._get_eventvalidation()

        if self.iters < 0:
            self._create_fake_account()
            self.iters = self._get_random_iters()
            url = 'https://onlineservices.miami-dadeclerk.com/officialrecords/StandardSearch.aspx'
            ua = self._get_ua()
            headers = {
                'User-Agent': ua,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'uk-UA,uk;q=0.8,en-US;q=0.5,en;q=0.3',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': 'https://onlineservices.miami-dadeclerk.com',
                'Connection': 'keep-alive',
                'Referer': 'https://onlineservices.miami-dadeclerk.com/officialrecords/StandardSearch.aspx',
                'Upgrade-Insecure-Requests': '1',
            }
            self.response = self.session.get(url, headers=headers)
            # print(":: resp 2 :: >>", self.response)
            VIEWSTATE = self._get_viewstate()
            VIEWSTATEGENERATOR = self._get_viewstategenerator()
            EVENTVALIDATION = self._get_eventvalidation()
        
        data = {
          '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$btnCFNSearch',
          '__EVENTARGUMENT': '',
          '__VIEWSTATE': VIEWSTATE,
          '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
          '__EVENTVALIDATION': EVENTVALIDATION,
          'ctl00$ContentPlaceHolder1$hfTab': 'tab2default',
          'ctl00$ContentPlaceHolder1$pfirst_party': '',
          'ctl00$ContentPlaceHolder1$prec_date_from': '',
          'ctl00$ContentPlaceHolder1$prec_date_to': '',
          'ctl00$ContentPlaceHolder1$pdoc_type': '',
          'ctl00$ContentPlaceHolder1$pcfn_year': self.startdate,
          'ctl00$ContentPlaceHolder1$pcfn_seq': f'{r}',
          'ctl00$ContentPlaceHolder1$prec_book': '',
          'ctl00$ContentPlaceHolder1$prec_page': '',
          'ctl00$ContentPlaceHolder1$prec_booktype': 'O',
          'ctl00$ContentPlaceHolder1$pplat_book': '',
          'ctl00$ContentPlaceHolder1$pplat_page': '',
          'ctl00$ContentPlaceHolder1$pblock_no': '',
          'ctl00$ContentPlaceHolder1$party_name': ''
        }

        self.response = self.session.post(url, headers=headers, data=data)
        # print(":: resp 3 :: >>", self.response)
        self.iters -= 1
        #self.__class__.scraped_items += 1
        #self.__class__.scraped_items_all += 1

        # print('INFO ::: scraped items: %s' % self.__class__.scraped_items)

        qs = self.parse_shelf()
        if not qs:
            count_none = list(self.thread_spider_status.values()).count('none')
            count_error = list(self.thread_spider_status.values()).count('error')
            count_ok = list(self.thread_spider_status.values()).count('ok')
            count_active = list(self.thread_spider_status.values()).count('active')
            print("INFO ::: %s, Thread: %s ::: <error: %s, none: %s, ok: %s, active: %s> R:%s" % ("Can't find QS", self.thread_id, count_error, count_none, count_ok, count_active, r))
            #print("INFO ::: %s, <Message: %s>" % (url, "Can't find QS"))
            if (count_none > 78 and count_error == 0 and count_ok == 0 and count_active == 0):
                self.is_finish = True
                return True
            time.sleep(60)
            return True

        # print("!!!!!!!!!!!!!!!!!!!")
        lambda_response = self.run_lambda(qs=qs, r=r)
        # print('lambda_response>>', lambda_response)
        # lambda_response = self.run_nolambda(qs=qs, r=r)
        status = lambda_response.get('status')
        if status == 'success':
            self.__class__.scraped_items_all += 1
            self.__class__.scraped_items += 1
            data = lambda_response.get('data')
            # print("ldata>>", data)
            self.save_to_mongo(data)
            self.thread_spider_status[self.thread_id] = 'ok'
            print("OK :: %s :: %s :: %s"%(self.__class__.scraped_items, self.__class__.scraped_items_all, lambda_response.get('url')))
            return True

        if status == 'error':
            if lambda_response.get('message', '') == 'missing cfn':
                print("INFO ::: %s, <Message: %s>" % (lambda_response.get('url'), lambda_response.get('message', '')))
                return True

            if lambda_response.get('message', '') == 'need update session':
                print("FAIL ::: %s, <Message: %s>" % (lambda_response.get('url'), lambda_response.get('message', '')))
                time.sleep(300)
                self._create_new_session()
                return False

            print("FAIL ::: %s, <Message: %s>" % (lambda_response.get('url'), lambda_response.get('message', '')))
            return False

        return False

    
    def parse_shelf(self):
        html = etree.HTML(self.response.content)
        items = html.xpath('//table[@id="tableSearchResults"]/tbody/tr/@onclick')
        qs = None
        if items:
            resp = items[0]
            reg = r"QS=(.*?)','850px'"
            qs = re.search(reg, resp).group(1)
            if not qs:
                print(">>>>>>>>", resp)
        else:
            self.thread_spider_status[self.thread_id] = 'none'
            # print("resp>", self.response, " items>> ", items)

        return qs

    # def run_nolambda(self, qs, r):
    #     s = self.session.cookies.get_dict().get('ASP.NET_SessionId')
    #     cfn = self.startdate
    #     data = self.nolambda_handler(qs=qs, r=r, s=s, cfn=cfn)
    #     self.count_items += 1
    #     print(":: data :: ", data)
    #     return data

    def run_lambda(self, qs, r):
        s = self.session.cookies.get_dict().get('ASP.NET_SessionId')
        nsc_n, nsc_v = [(k, self.session.cookies.get_dict()[k]) for k in self.session.cookies.get_dict().keys() if k.startswith('NSC_')][0]
        t = 'qwjf7Tds30KndbSi8fqprFs4mfaskjqppmSAA993'
        cfn = self.startdate
        lambda_version = self.__class__.lambda_version
        #url = f"https://m3ngchi12h.execute-api.us-east-2.amazonaws.com/api/l{lambda_version}?qs={qs}&s={s}&t={t}&cfn={cfn}&r={r}&nsc_n={nsc_n}&nsc_v={nsc_v}"
        url = f"https://5ys2upcbd2.execute-api.us-east-1.amazonaws.com/api/l{lambda_version}?qs={qs}&s={s}&t={t}&cfn={cfn}&r={r}&nsc_n={nsc_n}&nsc_v={nsc_v}"
        # print(url)
        response = requests.get(url)

        try:
            data = json.loads(response.content)
            data['url'] = url
        except:
            data = {'status':'error', 'response': response.text, 'url': url}
            # print("error in run_lambda func")
            # print("response code : %s" % response.status_code)
        return data
    
    def save_to_mongo(self, data):
        # print(":: DATA :: ", data)
        name = data.get('filename')
        year = name.split('-')[0]
        collection = '{}'.format(year)

        db = self.mongocli.miamidude
        col = db[collection]
        col.create_index("filename", unique=True)
        try:
            col.insert_one(data).inserted_id
        except DuplicateKeyError:
            pass
        return True
