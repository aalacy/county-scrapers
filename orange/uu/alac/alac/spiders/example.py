# -*- coding: utf-8 -*-
import scrapy
from urllib.parse import urlencode

headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'http://isol.alachuaclerk.org',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Referer': 'http://isol.alachuaclerk.org/RealEstate/SearchResults.aspx',
    'Upgrade-Insecure-Requests': '1',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
}

def get_url(url):
    payload = {'api_key': '92975321be6c2765177bcc543bd86790', 'url': url, 'country_code': 'us'}
    proxy_url = 'http://api.scraperapi.com/?' + urlencode(payload)
    return proxy_url


class ExampleSpider(scrapy.Spider):
    name = 'example'
    #allowed_domains = ['example.com']
    start_urls = ['http://isol.alachuaclerk.org/RealEstate/SearchEntry.aspx?e=newSession']

    def parse(self, response):
        data = {
            '__EVENTTARGET': 'ctl00$ctl00$cphNoMargin$cphNoMargin$SearchCriteriaTop$FullCount1',
            '__EVENTARGUMENT': '0',
            '__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
            '__VIEWSTATEGENERATOR': response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
            '__EVENTVALIDATION': response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
            'Header1_WebHDS_clientState': '',
            'Header1_WebDataMenu1_clientState': '[[null,[[[null,[],null],[{},[]],null]],null],[{},[{},{}]],null]',
            'ctl00$cphNoMargin$f$NameSearchMode': 'rdoCombine',
            'cphNoMargin_f_txtParty_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'cphNoMargin_f_txtParty': 'Lastname Firstname',
            'ctl00$cphNoMargin$f$drbPartyType': '',
            'cphNoMargin_f_txtGrantor_clientState': '|0|00||[[[[]],[],[]],[{},[]],"00"]',
            'cphNoMargin_f_txtGrantee_clientState': '|0|00||[[[[]],[],[]],[{},[]],"00"]',
            'cphNoMargin_f_ddcDateFiledFrom_clientState': '|0|011971-8-21-0-0-0-0||[[[[]],[],[]],[{},[]],"011971-8-21-0-0-0-0"]',
            'cphNoMargin_f_ddcDateFiledTo_clientState': '|0|012020-9-20-0-0-0-0||[[[[]],[],[]],[{},[]],"012020-9-20-0-0-0-0"]',
            'cphNoMargin_f_txtInstrumentNoFrom_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'cphNoMargin_f_txtInstrumentNoFrom': '',
            'cphNoMargin_f_txtInstrumentNoTo_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'cphNoMargin_f_txtInstrumentNoTo': '',
            'cphNoMargin_f_txtBook_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'cphNoMargin_f_txtBook': '',
            'cphNoMargin_f_txtPage_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'cphNoMargin_f_txtPage': '',
            'ctl00$cphNoMargin$f$dclDocType$0': 'AFF',
            'ctl00$cphNoMargin$f$dclDocType$1': 'AFFDC',
            'ctl00$cphNoMargin$f$dclDocType$2': 'AFFFAM',
            'ctl00$cphNoMargin$f$dclDocType$29': 'LP',
            'ctl00$cphNoMargin$f$dclDocType$30': 'LPFAM',
            'cphNoMargin_f_txtSubdivision_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'cphNoMargin_f_txtSubdivision': '',
            'cphNoMargin_f_txtLDBlock_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'cphNoMargin_f_txtLDBlock': '',
            'cphNoMargin_f_txtLDLot_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'cphNoMargin_f_txtLDLot': '',
            'cphNoMargin_f_Datatextedit2_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'cphNoMargin_f_Datatextedit2': '',
            'cphNoMargin_f_Datatextedit1_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'cphNoMargin_f_Datatextedit1': '',
            'cphNoMargin_f_Datatextedit10_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'cphNoMargin_f_Datatextedit10': '',
            'cphNoMargin_f_Datatextedit5_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'cphNoMargin_f_Datatextedit5': '',
            'cphNoMargin_f_Datatextedit3_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'cphNoMargin_f_Datatextedit3': '',
            'cphNoMargin_f_Datatextedit4_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'cphNoMargin_f_Datatextedit4': '',
            'ctl00$cphNoMargin$f$DataDropDownList3': '',
            'ctl00$cphNoMargin$f$DataDropDownList2': '',
            'ctl00$cphNoMargin$f$DataDropDownList1': '',
            'cphNoMargin_f_Datatextedit28_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'cphNoMargin_f_Datatextedit28': '',
            'cphNoMargin_dlgPopup_clientState': '[[[[null,3,null,null,null,null,1,1,0,0,null,0]],[[[[[null,"Document Image",null]],[[[[[]],[],null],[null,null],[null]],[[[[]],[],null],[null,null],[null]],[[[[]],[],null],[null,null],[null]]],[]],[{},[]],null],[[[[null,null,null,null]],[],[]],[{},[]],null]],[]],[{},[]],"3,0,,,,,0"]',
            'dlgOptionWindow_clientState': '[[[[null,3,null,null,"700px","550px",1,1,0,0,null,0]],[[[[[null,"Copy Options",null]],[[[[[]],[],null],[null,null],[null]],[[[[]],[],null],[null,null],[null]],[[[[]],[],null],[null,null],[null]]],[]],[{},[]],null],[[[[null,null,null,null]],[],[]],[{},[]],null]],[]],[{},[]],"3,0,,,700px,550px,0"]',
            'RangeContextMenu_clientState': '[[[[null,null,null,null,1]],[[[[[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]],[],[]],[{},[]],null]],null],[{},[{},{}]],null]',
            'LoginForm1_txtLogonName_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'LoginForm1_txtLogonName': '',
            'LoginForm1_txtPassword_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'LoginForm1_txtPassword': '',
            'ctl00$LoginForm1$logonType': 'rdoPubCpu',
            '_ig_def_dp_cal_clientState': '|0|15,2020,09,2020,9,20||[[null,[],null],[{},[]],"11,2020,09,2020,9,20"]',
            'ctl00$_IG_CSS_LINKS_': '~/localization/style.css|~/localization/styleforsearch.css|~/favicon.ico|~/localization/styleFromCounty.css|../ig_res/Default/ig_monthcalendar.css|../ig_res/ElectricBlue/ig_texteditor.css|../ig_res/Default/ig_texteditor.css|../ig_res/Default/ig_datamenu.css|../ig_res/ElectricBlue/ig_dialogwindow.css|../ig_res/ElectricBlue/ig_datamenu.css|../ig_res/ElectricBlue/ig_shared.css|../ig_res/Default/ig_shared.css',
            'ctl00$cphNoMargin$SearchButtons2$btnSearch__10': ':0'
        }
        t_post = 'http://isol.alachuaclerk.org/RealEstate/SearchEntry.aspx?e=newSession'
        yield scrapy.FormRequest(t_post, callback=self.p3, formdata=data)

    def p2(self, response):
        with open('my_test1.html', 'w') as f:
            f.write(response.text)
        print(response.request.headers)
        data = {
            '__EVENTTARGET': 'ctl00$ctl00$cphNoMargin$cphNoMargin$SearchCriteriaTop$FullCount1',
            '__EVENTARGUMENT': response.xpath('//*[@id="__EVENTARGUMENT"]/@value').extract_first(),
            '__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
            '__VIEWSTATEGENERATOR': response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
            '__EVENTVALIDATION': response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
            'Header1_WebHDS_clientState': '',
            'Header1_WebDataMenu1_clientState': '[[null,[[[null,[],null],[{},[]],null]],null],[{},[{},{}]],null]',
            'ctl00$ctl00$cphNoMargin$cphNoMargin$OptionsBar1$txtSelectedItems': '',
            'ctl00$ctl00$cphNoMargin$cphNoMargin$OptionsBar1$ItemList': '1',
            'ctl00$ctl00$cphNoMargin$cphNoMargin$OptionsBar1$ddlSortColumns': '',
            'cphNoMargin_cphNoMargin_g_G1_clientState': '[[[[]],[[[[[0]],[[[[[]],[],null],[null,null],[null]]],[{},{"RowNumber":[[0]],"ImageUrl":[[0]],"PERMANENT_INDEX_STATUS_ABBR":[[0]]}]],[{},[{},{}]],null],[[[[40]],[],null],[null,null],[null]],[[[[null,null,null,null,null,"25px"]],[],[]],[{},[]],null],[[[[null,null,null,null,null,"40px"]],[],[]],[{},[]],null],[[[[null,null,null,null,null,"20px"]],[],[]],[{},[]],null],[[[[1,null,null,null,null,"79px"]],[],[]],[{},[]],null],[[[[null,null,null,null,null,"95px"]],[],[]],[{},[]],null],[[[[1,null,null,null,null,"82px"]],[],[]],[{},[]],null],[[[[1,null,null,null,null,"42px"]],[],[]],[{},[]],null],[[[[1,null,null,null,null,"42px"]],[],[]],[{},[]],null],[[[[null,null,null,null,null,"65px"]],[],[]],[{},[]],null],[[[[null,null,null,null,null,"65px"]],[],[]],[{},[]],null],[[[[null,null,null,null,null,"110px"]],[],[]],[{},[]],null],[[[[1,null,null,null,null,"110px"]],[],[]],[{},[]],null],[[[[null,null,null,null,null,"25%"]],[],[]],[{},[]],null],[[[[1,null,null,null,null,"18px"]],[],[]],[{},[]],null],[[[[1,null,null,null,null,"130px"]],[],[]],[{},[]],null],[[[[1,null,null,null,null,"38px"]],[],[]],[{},[]],null],[[[[1,null,null,null,null,"18px"]],[],[]],[{},[]],null],[[[[1,null,null,null,null,"130px"]],[],[]],[{},[]],null],[[[[1,null,null,null,null,"38px"]],[],[]],[{},[]],null],[[[[null,null,null,null,null,"20%"]],[],[]],[{},[]],null],[[[[null,null,null,null,null,"40px"]],[],[]],[{},[]],null],[[[[1,null,null,null,null,"60px"]],[],[]],[{},[]],null],[[[[1,null,null,null,null,"60px"]],[],[]],[{},[]],null],[[[[1,null,null,null,null,"50px"]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[1,null,null,null,null]],[],[]],[{},[]],null],[[[[0]],[],[]],[{},[]],null]],null],[{},[]],[]]',
            'ctl00_ctl00_cphNoMargin_cphNoMargin_g_G1_ctl00_clientState': '[[[[null,25,null]],[[[[[0]],[],null],[null,null],[null]],[[[[3]],[],null],[null,null],[null]],[[[["Activation",null]],[],[]],[{},[]],null],[[[["Selection",1,null,2,null,null,null]],[],[]],[{},[]],null],[[[["EditingCore"]],[[[[["CellEditing"]],[[[[[0,null,null]],[],[]],[{},[]],null]],[{"ImageUrl":[[]],"INSTRUMENT_NUMBER":[[]],"DOCUMENT_TYPE":[[]],"DATE_RECEIVED":[[]],"LEGAL_DESCRIPTION":[[]],"PERMANENT_INDEX_STATUS_ABBR":[[]]}]],[{},[{}]],null]],[]],[{},[]],null],[[[[]],[],null],[null,null],[null]],[[[[]],[],null],[null,null],[null]]],null],[{},[{},{}]],[[],[],[],[]]]',
            'ctl00_ctl00_cphNoMargin_cphNoMargin_g_G1_ctl00_EditorControl1_clientState': '[[[[null,2,200,null,null,null,null,0,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,"AFFIDAVIT",null,null,null,null,null,null,null,null,null,null,null,null,null,3,null,0,null,-1,0,null,null,null,null,null,null,null]],[],null],[{},[{}]],null]',
            'ctl00$ctl00$cphNoMargin$cphNoMargin$OptionsBar2$txtSelectedItems': '',
            'ctl00$ctl00$cphNoMargin$cphNoMargin$OptionsBar2$ItemList': '1',
            'ctl00$ctl00$cphNoMargin$cphNoMargin$OptionsBar2$ddlSortColumns': '',
            'cphNoMargin_dlgImageWindow_clientState': '[[[[null,3,null,null,"950px","800px",1,1,0,0,null,0]],[[[[[null,"Document Image",null]],[[[[[]],[],null],[null,null],[null]],[[[[]],[],null],[null,null],[null]],[[[[]],[],null],[null,null],[null]]],[]],[{},[]],null],[[[[null,null,null,null]],[],[]],[{},[]],null]],[]],[{},[]],"3,0,,,950px,800px,0"]',
            'dlgOptionWindow_clientState': '[[[[null,3,null,null,"700px","550px",1,1,0,0,null,0]],[[[[[null,"Copy Options",null]],[[[[[]],[],null],[null,null],[null]],[[[[]],[],null],[null,null],[null]],[[[[]],[],null],[null,null],[null]]],[]],[{},[]],null],[[[[null,null,null,null]],[],[]],[{},[]],null]],[]],[{},[]],"3,0,,,700px,550px,0"]',
            'RangeContextMenu_clientState': '[[[[null,null,null,null,1]],[[[[[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]],[],[]],[{},[]],null]],null],[{},[{},{}]],null]',
            'LoginForm1_txtLogonName_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'LoginForm1_txtLogonName': '',
            'LoginForm1_txtPassword_clientState': '|0|01||[[[[]],[],[]],[{},[]],"01"]',
            'LoginForm1_txtPassword': '',
            'ctl00$ctl00$LoginForm1$logonType': 'rdoPubCpu',
            '_ig_def_ep_n_clientState': '|0|01||[[null,[],null],[{},[]],"01"]',
            '_ig_def_ep_n': '',
            '_ig_def_ep_d_clientState': '|0|01||[[null,[],null],[{},[]],"01"]',
            '_ig_def_ep_d': '',
            'ctl00$ctl00$_IG_CSS_LINKS_': '~/localization/style.css|~/localization/styleforsearch.css|~/favicon.ico|~/localization/styleFromCounty.css|../ig_res/ElectricBlue/ig_hierarchicalDataGrid.css|../ig_res/Default/ig_dropDown.css|../ig_res/Default/ig_texteditor.css|../ig_res/Default/ig_datamenu.css|../ig_res/ElectricBlue/ig_dialogwindow.css|../ig_res/ElectricBlue/ig_datamenu.css|../ig_res/ElectricBlue/ig_dataGrid.css|../ig_res/ElectricBlue/ig_shared.css|../ig_res/Default/ig_shared.css'}
        yield scrapy.FormRequest('http://isol.alachuaclerk.org/RealEstate/SearchResults.aspx', formdata=data, headers=headers)

    def p3(self, response):
        with open('my_test.html', 'w') as f:
            f.write(response.text)
        values = response.xpath('//*[@id="cphNoMargin_cphNoMargin_OptionsBar1_ItemList"]/option/@value').extract()
        for val in values:
            if val == '1':
                continue
            url = f'http://isol.alachuaclerk.org/RealEstate/SearchResults.aspx?pg={val}'
