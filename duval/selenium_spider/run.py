import os 
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select


from datetime import datetime, timedelta


path = os.getcwd()
DRIVER_PATH = path + '/chromedriver'

duval_county_search = 'https://or.duvalclerk.com/search/SearchTypeDocType'
# Set Driver

# Run Head
options = Options()
# options.headless = True
options.add_argument('--ignore-certificate-errors')
options.add_argument('--window-size=1920,1200')


driver = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)

driver.get(duval_county_search)

# Run Headless 
def get_html_in_headless_from(url):
    html = ''
    driver.get(url)
    html = driver.page_source
    # driver.quit()
    return html

# html = get_html_in_headless_from(duval_county_search)
# print(html)

def get_last_month():
    today = datetime.date(datetime.now())
    last_month = today - timedelta(days=30)
    today = today.strftime("%m/%d/%Y")
    last_month = last_month.strftime("%m/%d/%Y")
    return last_month


def click_button(elements):
    '''
    clicks first instance of element found
    '''
    button = driver.find_elements_by_xpath(elements)[0]
    button.click()

def type_text(element, text):
    text_area = driver.find_element_by_id(element)
    text_area.send_keys(text)

def select_from_dropdown_using_text(text):
    select = Select(driver.find_element_by_id('fruits01'))

    # select by visible text
    # select.select_by_visible_text('Banana')

    # select by value 
    # select.select_by_value('1')

duval_disclaimer_button = '//*[@id="btnButton"]'
# '/html/body/div[2]/div/div/div/div[2]/form/div/input'
click_button(duval_disclaimer_button)

# Search Page
doc_type_element = '//*[@id="DocTypesDisplay-input"]'
select = driver.find_element_by_xpath(doc_type_element)
select.send_keys('LIS PENDENS (LP)')


# date_range_dropdown = '/html/body/div[2]/div[1]/div[2]/div/div/form/div[1]/div[1]/div[4]/div'
date_range_dropdown = '//*[@id="DateRangeDropDown"]'
# select = driver.find_element_by_id('DateRangeDropDown')
# select.send_keys('Last Month')
from_record_date = driver.find_element_by_xpath('//*[@id="RecordDateFrom"]')
last_month = get_last_month()

from_record_date.clear()
from_record_date.send_keys(last_month)




# driver.quit()
