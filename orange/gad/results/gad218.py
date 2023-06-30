from datetime import datetime, timedelta
import os
import xmltodict
import json
import uuid
# start_date = datetime(year=2020, month=6, day=26).strftime('%Y-%m-%d')
# end_date = datetime(year=2020, month=7, day=23).strftime('%Y-%m-%d')
z = ['5', '65', '218']


def curl_it(dt, doc_id='218'):
    start_date1 = dt.strftime('%Y-%m-%d')
    end_date1 = (dt+timedelta(29)).strftime('%Y-%m-%d')
    s = (
        "curl 'http://api.scraperapi.com/?api_key=92975321be6c2765177bcc543bd86790"
        "&url=http://www.gadsdenclerk.com/publicinquiry/Mobile/MobileService.svc/ProcessXmlMessage'"
        " -H 'User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0' "
        "-H 'Accept: text/plain, */*; q=0.01' -H 'Accept-Language: en-US,en;q=0.5'"
        " --compressed -H 'Content-Type: application/json; charset=utf-8' -H 'X-Requested-With: XMLHttpRequest' "
        "-H 'Origin: http://www.gadsdenclerk.com' -H 'DNT: 1' -H 'Connection: keep-alive' -H "
        "'Referer: http://www.gadsdenclerk.com/publicinquiry/Mobile/Search.htm'"
        " -H 'Pragma: no-cache' -H 'Cache-Control: no-cache' --data-raw '{"
        '"XML":"<?xml version=\\"1.0\\" encoding=\\"utf-8\\"?><DuProcessRequest xmlns=\\'
        '"'
        "http://www.cdscourts.com/xsd/PublicInquiry_SearchInstrument.html\\"
        '"><MetaData><DocumentAction>'
        "PublicInquiry_SearchInstrument</DocumentAction><Version>1.0</Version></MetaData><PublicInquiry_SearchInstrumentDetails><MaxResults>50"
        "</MaxResults><StartDate>"
        f"{start_date1}"
        "</StartDate><EndDate>"
        f"{end_date1}<"
        f"/EndDate><InstrumentTypeID>{doc_id}</InstrumentTypeID><PublicViewableStatus>Published"
        "</PublicViewableStatus><ShowRestricted>false</ShowRestricted></PublicInquiry"
        '_SearchInstrumentDetails></DuProcessRequest>"}\''
    )
    return s


start_date = datetime(year=1970, month=1, day=1)
end_date = datetime.today()
to_scrape = [start_date]
while True:
    start_date += timedelta(days=30)
    to_scrape.append(start_date)
    if start_date > end_date:
        break

found = []
for i in to_scrape:
    z = curl_it(i)
    fn = str(uuid.uuid4()) + ".html"
    os.system(f"{z} > {fn}")
    with open(fn) as f:
        h = f.read()
        di = xmltodict.parse(h)
    os.remove(fn)
    pdf_name = fn.replace('html', 'json')
    my_list = di.get('DuProcessResponse', {}).get('InstrumentCollection', {})
    if not my_list:
        continue
    if not my_list.get('Instrument', {}):
        continue
    my_dicts = [dict(i2) for i2 in my_list.get('Instrument', {})]
    my_ids = set(i['InstrumentID'] for i in my_dicts)
    for ins_id in my_ids:
        ind = 1
        for prod in my_dicts:
            if prod['InstrumentID'] == ins_id:
                if ind == 1:
                    temp_di = {'InstrumentID': ins_id, 'InstrumentNumber': prod['InstrumentNumber'],'From': [],'To': [],
                               'InstrumentType': prod['InstrumentType'], 'DateFiled': prod['DateFiled'],
                               'NumberOfPages': prod['NumberOfPages'], 'Amount': prod['Amount'],
                               'CurrentStatus': prod['CurrentStatus'], 'LegalDescription': prod['LegalDescription'],
                               'PDF_fileName': pdf_name
                               }
                    ind += 1
                if prod['PartyType'] == 'FROM_PARTY':
                    temp_di['From'].append(prod['FullName'])
                else:
                    temp_di['To'].append(prod['FullName'])
        found.append(temp_di)


with open('Results218.json', 'w') as f:
    json.dump(found, f)

