import requests
from datetime import datetime, timedelta
from scrapy.selector import Selector
from urllib.parse import urljoin
import os
import csv
from tqdm import tqdm


def clean_up(di2):
    for k, v in di2.items():
        if not v:
            item[k] = ''
        elif '&nbsp' in v:
            item[k] = v.replace('&nbsp', '').strip()
        return item


completed_file = 'occompt_completed.txt'
if os.path.isfile(completed_file):
    with open(completed_file) as f:
        already = f.read().split('\n')
        my_set = set([i.strip() for i in already if i.strip()])
else:
    my_set = set()


def get_new_session():
    i_url = 'https://www.occompt.com/official-records/'
    ses = requests.Session()
    ses.proxies = {'http': 'socks5h://localhost:9050', 'https': 'socks5h://localhost:9050'}
    ses.headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }
    response = ses.get(i_url)
    sel = Selector(response)
    m = sel.xpath('//a[@title="Search for documents in Official Records"]/@href').extract_first()
    url = urljoin(i_url, m)
    response = ses.get(url)
    ses.get('https://or.occompt.com/recorder/web/')
    r = ses.post('https://or.occompt.com/recorder/web/loginPOST.jsp', data={'guest': 'true', 'submit': 'I Accept'})
    return ses


data = {
    'RecordingDateIDStart': '01/16/2018',
    'RecordingDateIDEnd': '01/25/2018',
    'BothNamesIDSearchString': '',
    'BothNamesIDSearchType': 'Wildcard Search',
    'GrantorIDSearchString': '',
    'GrantorIDSearchType': 'Exact Match',
    'GranteeIDSearchString': '',
    'GranteeIDSearchType': 'Exact Match',
    'DocumentID': '',
    'BookPageIDBook': '',
    'BookPageIDPage': '',
    #'PLSSIDSixtyFourthSection': '',
    'PLSSIDSection': '',
    'PLSSIDTownship': '',
    'PLSSIDRange': '',
    'PlattedIDLot': '',
    'PlattedIDBlock': '',
    'PlattedIDTract': '',
    'PlattedIDUnit': '',
    'CaseID': '',
    'DeedDocTaxStart': '',
    'DeedDocTaxEnd': '',
    'MortgageDocTaxStart': '',
    'MortgageDocTaxEnd': '',
    'IntangibleTaxStart': '',
    'IntangibleTaxEnd': '',
    'ParcelID': '',
    'LegalRemarks': '',
    'docTypeTotal': '41',
    '__search_select': 'AFF'
}
dates_scrape = []
x = datetime(2008, 1, 6)
while True:
    dates_scrape.append(x)
    if x > datetime.today():
        break
    x += timedelta(days=30)

date_to_scrape = dates_scrape[0]
found = []
document_types = ['LP', 'AFF']
for document_type in document_types:
    data['__search_select'] = document_type
    for date_to_scrape in tqdm(dates_scrape):
        ses = get_new_session()
        data['RecordingDateIDStart'] = date_to_scrape.strftime('%m/%d/%Y')
        data['RecordingDateIDEnd'] = (date_to_scrape + timedelta(days=30)).strftime('%m/%d/%Y')
        _ = ses.post('https://or.occompt.com/recorder/eagleweb/docSearchPOST.jsp', data=data)
        if "Sorry, I was unable to complete your request" in _.text:
            print("Blocked")
            break
        params = {'searchId': '0'}
        response = ses.get('https://or.occompt.com/recorder/eagleweb/docSearchResults.jsp', params=params)
        page = 1
        while True:
            params = {'searchId': '0', 'page': f'{page}', 'pageSize': '100', 'sort': '', 'sort2': '', 'dir': ''}
            response = ses.get('https://or.occompt.com/recorder/eagleweb/docSearchResults.jsp', params=params)
            sel = Selector(response)
            next_page = sel.xpath('//a[text()="Next"]/@href').extract_first()
            for i in sel.xpath('//table[@id="searchResultsTable"]/tbody/tr'):
                temp = i.xpath('.//strong//text()').extract()
                if temp[1].strip() in my_set:
                    continue
                img = i.xpath('.//a[text()="View Image"]/@href').extract_first()
                dt = i.xpath('.//*[text()="Rec Date: "]/parent::*/text()').extract_first()
                grantor = i.xpath('.//*[text()="Grantor: "]/parent::*/text()').extract_first()
                grantee = i.xpath('.//*[text()="Grantee: "]/parent::*/text()').extract_first()
                legal = i.xpath('.//*[text()="Legal: "]/parent::*/text()').extract_first()
                book_page = i.xpath('.//*[text()="Rec Date: "]/parent::*/text()').extract()[1]
                item = {'image': 'https://or.occompt.com/recorder/eagleweb/' + img,
                        'details': "https://or.occompt.com/recorder" + i.xpath('td[1]//a/@href').extract_first()[2:],
                        'doc_type': temp[0], 'id': temp[1].strip(), 'date': dt, 'Grantor': grantor,'grantee': grantee,
                        'legal': legal, 'book_page': book_page
                        }
                item['pdf_file'] = f'https://or.occompt.com/recorder/eagleweb/downloads/{item["id"]}?id={item["image"].split("id=")[1]}&preview=false&noredirect=true'
                found.append(clean_up(item))
                my_set.add(temp[1].strip())
            if next_page:
                page += 1
                continue
            else:
                break


with open(completed_file, 'w') as f:
    for i in my_set:
        f.write(f'{i}\n')

if found:
    keys = found[0].keys()
    with open('occompt_history.csv', 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(found)
else:
    with open('occompt.txt', 'w') as output_file:
        output_file.write(f"There is nothing to write completed\ncompleted at {datetime.today()}")

print(f"Completed\n{len(found)} were found")
