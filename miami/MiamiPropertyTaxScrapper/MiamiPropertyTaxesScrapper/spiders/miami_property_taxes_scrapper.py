import scrapy
import json
import sys
from MiamiPropertyTaxesScrapper.items import MiamipropertytaxesscrapperItem


class InfoSpider(scrapy.Spider):
    name = "miami_property_tax"
    base_link = "https://miamidade.county-taxes.com/public/real_estate/parcels/"

    def start_requests(self):
        folios = list(map(lambda x: x.strip(), list(
            open('folio_nums_miami.txt'))))
        nstart = 450000 
        folios = folios[nstart:]
        # folios = folios[0:10]
        # folios = ["0101000000020"]
        for folio in folios:
            account = '-'.join([folio[:2], folio[2:6], folio[6:9], folio[9:]])
            link = self.base_link + account + "/bills"
            print(link)
            yield scrapy.Request(link, self.parse1, cb_kwargs=dict(folio=folio))

    def parse1(self, response, folio):
        item = MiamipropertytaxesscrapperItem()
        # print("item.fields: ", item.fields)
        # print("folio: ", folio)
        item['folio'] = folio
        item['data'] = {}
        history = self.extractHistoryTable(response)
        item['data'].update({"account_history": history})
        # now go ahead and scrape the next page
        part = response.xpath(
            "//div[contains(@class,'bill-history')]/descendant::h2").xpath('normalize-space()').get()
        path = response.xpath(
            "//div[@class='parcel']/descendant::a/@href").get()
        yield response.follow(path, callback=self.parse2, cb_kwargs=dict(item=item))

    def parse2(self, response, item):
        # Parcel Details
        parcel_details = self.extractParcelDetails(response)
        item['data'].update({"parcel_details": parcel_details})

        # Extract Data of Mailing Address
        mail_label = response.xpath(
            "//div[@class='row mailing-address']/descendant::div[contains(@class, 'label')]").xpath('normalize-space()').get()
        mail_value = response.xpath(
            "//div[@class='row mailing-address']/descendant::div[contains(@class, 'value')]").xpath('normalize-space()').get()
        item['data'].update({"mailing_address": {mail_label: mail_value}})

        # Extract Data of Ad Valorem Taxes
        advtax = self.extractAdTable(response)
        item['data'].update({"ad_alorem_taxes": advtax})

        # Extract Data of Non-Ad Valorem Assessments
        nonadvtax = self.extractNonadTable(response)
        item['data'].update({"nonad_valorem_taxes": nonadvtax})

        # Bill Taxes
        bill = self.extractBillTable(response)
        item['data'].update({"latest_annual_bill": bill})

        # Extract Data of Combined taxes and assessments
        message = response.xpath(
            "//div[@class='row messages']/descendant::div[contains(@class, 'message')]").xpath('normalize-space()').get()
        label = message.split(":")[0]
        value = message.split(":")[1]

        # Combined Taxes and Assessments
        item['data'].update({label: value})
        self.normalizeKeys(item['data'])
        yield item

    def extractParcelDetails(self, response):
        # Extract Data of Owner Info
        keys = []
        vals = []

        classnames = ['owners', 'account-details', 'parcel-values']
        for classname in classnames:
            keys += response.xpath(
                "//div[contains(@class, '{}')]/descendant::div[contains(@class, 'label')]".format(classname)).xpath('normalize-space()').getall()
            vals += response.xpath(
                "//div[contains(@class, '{}')]/descendant::div[contains(@class, 'value')]".format(classname)).xpath('normalize-space()').getall()

        keys = list(map(lambda x: x.split(':')[0], keys))

        # Extract Data of the Latest Annual Bill
        bill_detail = response.xpath("//div[contains(@class, 'bill-details')]")
        bill_name = bill_detail.xpath(
            ".//div[contains(@class, 'header')]").xpath('normalize-space()').get()
        bill_keys = bill_detail.xpath(
            ".//div[contains(@class, 'label')]").xpath('normalize-space()').getall()
        bill_vals = bill_detail.xpath(
            ".//div[contains(@class, 'value')]").xpath('normalize-space()').getall()

        bill_keys = list(map(lambda x: x.split(':')[0], bill_keys))

        keys.append(bill_name)
        vals.append(dict(zip(bill_keys, bill_vals)))

        # Extract Data of Legal Description
        # scrapy.shell.inspect_response(response, self)
        keys.append("legal_description")
        # response.xpath(
        #     "//div[contains(@class, 'legal')]/descendant::*[@class='truncated']").remove()
        # vals.append(response.xpath(
        #     "//div[contains(@class, 'legal')]/descendant::div[contains(@class, 'col-12')]")[1].xpath('normalize-space()').get())
        tmp = response.xpath("//div[contains(@class, 'legal')]/descendant::div[contains(@class, 'col-12')]")[1]
        vals.append(self.strListMerge(tmp.xpath("./text()|./*[@class='expanded']/text()").getall()))

        # Extract Data of Location
        location_keys = response.xpath(
            "//div[@class='row detail']/descendant::div[contains(@class, 'location')]/descendant::div[contains(@class, 'label')]").xpath('normalize-space()').getall()
        location_vals = response.xpath(
            "//div[@class='row detail']/descendant::div[contains(@class, 'location')]/descendant::div[contains(@class, 'value')]").xpath('normalize-space()').getall()

        location_keys = list(map(lambda x: x.split(':')[0],  location_keys))

        keys.append("location")
        vals.append(dict(zip(location_keys, location_vals)))

        return dict(zip(keys, vals))

    def extractAdTable(self, response):
        table = {}
        className = "row advalorem"
        rows = response.xpath(
            "//div[@class='{}']/descendant::tbody".format(className))
        groupName = None
        groupVal = None

        if not rows:
            return {}

        for row in rows:
            if row.attrib['class'] == "district-group":
                if groupName:
                    table.update({groupName: groupVal})
                groupName = row.xpath(
                    "self::*//th[@class='name']").xpath('normalize-space()').get()
                groupVal = []
            else:
                rowEntry = {}
                key = row.attrib['class'].replace("-", " ")
                val = row.xpath('self::*//th').xpath('normalize-space()').get()
                rowEntry.update({key: val})

                for td in row.xpath('self::*//td'):
                    key = td.attrib['class']
                    val = td.xpath('normalize-space()').get()
                    rowEntry.update({key: val})
                groupVal.append(rowEntry)

        table.update({groupName: groupVal})

        total = {}
        for td in response.xpath("//div[@class='{}']/descendant::tfoot/descendant::td".format(className)):
            if 'class' in td.attrib:
                key = td.attrib["class"]
                val = td.xpath('normalize-space()').get()
                total.update({key: val})
        return {'table': table, 'total': total}

    def extractNonadTable(self, response):
        table = {}
        className = "row nonadvalorem"
        rows = response.xpath(
            "//div[@class='{}']/descendant::tbody".format(className))
        attributes = response.xpath(
            "//div[@class='{}']/descendant::thead/descendant::th".format(className)).xpath('normalize-space()').getall()

        if not rows:
            return {}

        for row in rows:
            values = row.xpath(
                'self::*//th|self::*//td').xpath('normalize-space()').getall()
            table.update(dict(zip(attributes, values)))

        total = response.xpath(
            "//div[@class='{}']/descendant::tfoot/descendant::td[contains(@class, 'amount')]".format(className)).xpath('normalize-space()').get()

        return {'table': table, 'total': total}

    def extractBillTable(self, response):
        # print("Inside extractBillTable")
        table = []
        pdfurls = []
        pdfs = []
        className = "table table-hover bills"
        tableSelect = response.xpath(
            "//*[@class='{}']".format(className))
        # print("tableSelect ", tableSelect)
        attributes = tableSelect.xpath(
            "self::*/descendant::thead/descendant::th").xpath('normalize-space()').getall()
        # print("attributes ", attributes)
        rows = tableSelect.xpath("self::*/descendant::tbody")
        # print("rows ", rows)

        if not rows:
            return {}

        for row in rows:
            rowdata = {}
            row = row.xpath(
                "self::*/descendant::tr[not(contains(@class,'mobile'))]")
            values = row.xpath(
                'self::*//th|self::*//td').xpath('normalize-space()').getall()
            vals = values[:4]
            rowdata.update(dict(zip(attributes, vals)))
            pdfpath = row.xpath('self::*/descendant::a/@href').get()
            pdfpath = response.urljoin(pdfpath)
            pdfurls.append(pdfpath)
            table.append(rowdata)

        notices = self.strListMerge(response.xpath(
            "//*[@class='row notices']").xpath('normalize-space()').getall())

        return {'table': table, 'pdfurls': pdfurls, 'pdfs': pdfs, 'notices': notices}

    def extractHistoryTable(self, response):
        result = {}
        className = "content-group bill-history"
        table = response.xpath(
            "//div[@class='{}']/descendant::table".format(className))
        rows = table.xpath("./descendant::tr")

        # from scrapy.shell import inspect_response
        # inspect_response(response, self)

        if not rows:
            return {}
        for row in rows:
            rowclass = row.xpath("./@class").get()
            if not rowclass:
                continue
            if "regular" in rowclass:
                description = row.xpath(
                    "./*[contains(@class,'description')]/a/text()").get()
                result.update(
                    {description: {'status': {'payments': [], 'refund': []}}})
                balance = row.xpath(
                    ".//*[contains(@class,'balance')]/text()").get().strip()
                result[description].update({'amount due': balance})
                currPayment = result[description]['status']['payments']
                currRefund = result[description]['status']['refund']
                paid = self.historyHelper(row, 'Paid')
                time = row.xpath(".//time/text()").get()
                receipt = self.historyHelper(row, 'Receipt')
                currPayment.append(
                    {"paid": paid, "date": time, "receipt_number": receipt})
            elif "partial-payment" in rowclass:
                paid = self.historyHelper(row, 'Payment')
                time = row.xpath(".//time/text()").get()
                receipt = self.historyHelper(row, 'Receipt')
                currPayment.append(
                    {"paid": paid, "date": time, "receipt_number": receipt})
            elif "refund" in rowclass:
                check = row.xpath(
                    "./*[contains(@class,'description')]/text()").get().split()[-1]
                cleard = self.historyHelper(row, "Cleared")
                time = row.xpath(".//td[2]/text()").get().strip()
                recipient = self.strListMerge(
                    row.xpath(".//td[3]/div[1]/text()").getall())
                recipient_address = self.strListMerge(
                    row.xpath(".//td[3]/div[2]/text()").getall())
                currRefund.append(
                    {"cleard": cleard, "date": time, "check_number": check, "recipient": recipient, "recipient_address": recipient_address})
                # print("currRefund: ", currRefund)

        return result

    def historyHelper(self, row, keyword):
        # get fields after a span containing the keyword
        # print(row.xpath(
        #     ".//*[contains(text(), '{}')]/parent::*/text()".format(keyword)).getall())
        return self.strListMerge(row.xpath(".//*[contains(text(), '{}')]/parent::*/text()".format(keyword)).getall())

    def strListMerge(self, strList):
        return str(" ".join(map(lambda x: x.strip(), strList))).strip()

    def normalizeKeys(self, obj):
        if type(obj) is list:
            for o in obj:
                self.normalizeKeys(o)
            return
        elif type(obj) is dict:
            keys = list(obj.keys())
            for key in keys:
                newKey = key.lower().replace(' ', '_').strip()
                obj[newKey] = obj.pop(key)
            for val in obj.values():
                self.normalizeKeys(val)
            return
        else:
            return
