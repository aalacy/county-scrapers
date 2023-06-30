import scrapy
from scrapy.http import FormRequest
import re
import selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time

class balHarborPermits(scrapy.Spider):
    name = "bal_harbor_permits"
    start_urls=['https://vlg-balharbour-fl.smartgovcommunity.com/Parcels/ParcelSearch?query=1&_conv=1#',]
    pageNum = 1
    def parse(self, response):

        #intialize headless driver
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920x1080")

        # download the chrome driver from https://sites.google.com/a/chromium.org/chromedriver/downloads and put it in the
        # current directory
        chrome_driver = "././chromedriver"
        driver = webdriver.Chrome(chrome_options=chrome_options, executable_path=chrome_driver)
        driver.get(response.url)

        ErrorCount = 0
        # continues to get permits until cant go anymore
        while True:

            #sleeps then looks for next button
            #potential to tune this for decreased latency
            time.sleep(3)
            next = WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((By.XPATH,'/html/body/div[1]/section[2]/div/div/div/div/div/div/div/div[2]/div/form/div[4]/div[12]/ul/li[10]/a'))
                        )
            #click button, and convert selenium url to response for scrapy
            try:
                next.click()
                body = (driver.page_source).encode('utf-8')
                req = scrapy.Request(driver.current_url)
                resp = scrapy.http.HtmlResponse(req.url, body=body, request=req)

                #get all clickable links for scrapy, and forms urls to parse with
                permits = resp.css('div.search-result-title a::attr(onclick)').getall()
                for permit in permits:
                    link = 'https://vlg-balharbour-fl.smartgovcommunity.com/Parcels/ParcelDetail/Index/' + permit[34:-4] +'?_conv=1'
                    yield scrapy.Request(link,self.parse_permits,dont_filter=False) 
                #should only get here if no issues with scrapy/ or selenium
                if True:
                    ErrorCount = 0
            except:
                print("ERROR: Failed to click next")
                ErrorCount+=1
                #if selenium look occurrs break out// probably needs to be better implemented to 
                if ErrorCount >10:
                    break
                else:
                    
                    continue

    def parse_permits(self, response):
        
        try:
            address = ""
            addy = response.css('div.span2 address::text').getall()
            address = address.join(addy)
            addy = re.sub('[^A-Za-z0-9-_,#]+', ' ', address)
        except:
            addy = " "
        try:
            parcelNum = response.css('div.span2 address span::text').get()
            parcelNum = re.sub('[^A-Za-z0-9-_,.#/]+', ' ' ,parcelNum)

            parcel = response.css('div.span2 address div::text').get() 
            parcel = re.sub('[^A-Za-z0-9-_,.#/]+', ' ' ,parcelNum)
        except:
            parcel = ""
            parcelNum=""
        try:
            status = response.css('div.alert div.span1 span::text').getall()
            for s in status:
                s = re.sub('[^A-Za-z0-9-_,.#/]+', ' ' ,s)
        except:
            status=""
        try:
            date = response.css('div.span2.permit-date-container span::text').getall()
            temp=""
            date = temp.join(date)
            date = re.sub('[^A-Za-z0-9-_,.#/]+', ' ' ,date)
        except:
            date=""
        try:
            Inspections = response.css('div.span2.permit-inspections-container div span::text').getall()
            for i in Inspections:
                i = re.sub('[^A-Za-z0-9-_,.#:]+',' ', i)
        except:
            Inspections=""
        try:
            bottomName = response.css('div.span9 span::text').get()
            bottomName = re.sub('[^A-Za-z0-9-_,.#:]+',' ', bottomName)

            value = response.css('div.span9::text').getall() #specifically 2nd elem
            temp=""
            for v in value:
                temp+=v
            temp = re.sub('[^A-Za-z0-9-_,.#:]+',' ', temp)
            value = temp
        except:
            bottomName=""
            value = ""
        yield{
            'Address' : addy,
            'ParcelNum' : parcelNum,
            'Parcel' : parcel,
            'Status' : status,
            'Date' : date,
            'Inspections' : Inspections,
            bottomName : value
        }
       