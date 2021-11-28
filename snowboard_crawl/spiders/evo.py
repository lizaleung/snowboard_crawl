import scrapy
import json
import re
import sys

from snowboard_crawl.items import EvoSnowboardItem, EvoSnowboardImageItem

class EvoSpider(scrapy.Spider):
    name = 'evo'
    allowed_domains = ['evo.com']
    start_urls = ['https://www.evo.com/shop/snowboard/snowboards/rpp_400']
    base_url = 'https://www.evo.com'
    folder_name = 'evo/snowboard'

    def parse(self, response):

        # pagination
        if "snowboards/p_" not in response.url :
            pages = set(response.css("div.results-pagination-numerals a::attr(href)").extract())
            if "javascript:void(0);" in pages: pages.remove("javascript:void(0);")
            for eachPage in pages:
                if eachPage[0] == "#" : eachPage = eachPage[1:]
                pagination_url = "%s%s" %(self.base_url , eachPage)
                request = scrapy.Request(url = pagination_url, 
                                     callback=self.parse,
                                     )
                self.log('Pagination crawling %s' % pagination_url)
                # yield request

        # get structured data 
        structured_data = self.parse_structured_data(response)

        # get from css html
        for _item in response.css("div.product-thumb"):
            url = self.base_url + _item.css("a::attr(href)").extract_first()
            item_id = _item.css("div::attr(data-productid)").extract_first()
            unique_id = _item.css("a::attr(data-unique-id)").extract_first()
            img_url = _item.css("img::attr(data-src)").extract_first()
            # print(_item.extract())
            # print(url)
            # print(img_url)
            # print(unique_id)
            # print(structured_data[unique_id]["price"])
            # print(structured_data[unique_id]["name"])
            # print(structured_data[unique_id]["brand"])
            item = { "unique_id": unique_id,
                     "item_id": item_id,
                     "url": url,
                     "img_url": img_url,
                     "price": structured_data[unique_id]["price"],
                     "name": structured_data[unique_id]["name"],
                     "brand": structured_data[unique_id]["brand"],
                   }
            yield scrapy.Request(url = url, callback=self.parse_item_page,
                                 meta={'item': item})
            # break
            





    def parse_structured_data(self, response):

        data_dict = {}
        
        data_dump = re.match(r".*window\.dataLayerManager\.pushImpressions\((.*)\, \'Results", response.text, re.DOTALL).group(1)
        data_json = json.loads(data_dump)
        for element in data_json:
            data_dict[element["_uniqueId"]] = element
        return data_dict

    def mergeList (self, inputList):
        inputList = [s for s in inputList if s != "\n"]
        return " ".join(inputList)

    def parse_item_page(self, response):
        """
        :param response:
        :return:
        """

        item = response.meta["item"]

        try:
            # Product
            description = response.css("div.pdp-details-content p ::text").extract_first()
            item["description"] = description
        except:
            self.log("Error while retrieving parse_item_page Product")
            item["description"] = None

        try:
            # Feature
            feature = {}
            for eachFeature in response.css("div.pdp-feature"):
                a = self.mergeList(eachFeature.css("h5::text").extract())
                b = self.mergeList(eachFeature.css("div.pdp-feature-description p ::text").extract())
                feature[a] = b
            item["feature"] = feature

        except:
            self.log("Error while retrieving parse_item_page Product")
            item["feature"] = None

        try:
            # Specs
            spec = {}
            for eachSpec in response.css("li.pdp-spec-list-item"):
                a = self.mergeList(eachSpec.css("span.pdp-spec-list-title strong::text").extract())
                b = eachSpec.css("span.pdp-spec-list-description ::text").extract()
                spec[a] = b
            item["spec"] = spec

        except:
            self.log("Error while retrieving parse_item_page Specs")
            item["spec"] = None

        try:    
            # Size Table
            size_table_dict = {}
            for eachRow in response.xpath('//*[@class="spec-table table"]//tr'):
                row_name = eachRow.xpath('th//text()').extract_first()
                row_val = eachRow.xpath('td//text()').extract()
                for k,eachRowVal in enumerate(row_val):
                    if k not in size_table_dict: size_table_dict[k] = {} 
                    size_table_dict[k][row_name] = eachRowVal
            item["size_table"] = size_table_dict

        except:
            self.log("Error while retrieving parse_item_page Size Table")
            item["size_table"] = None

        # Post Processing of some fields
        item["name_processed"] = ""
        item["year_processed"] = 0
        item["women_processed"] = False
        item["img_url_processed"] = ""

        tmpName = item["name"].replace(item["brand"],"").lstrip()
        
        try:
            if tmpName.split(" ")[-1] + 0:
                item["year_processed"] = tmpName.split(" ")[-1]
        except:
            item["year_processed"] = 0
        
        try:
            if "Women" in item["name"]:
                item["women_processed"] = True
            if " - " in item["name"]:
                item["name_processed"] = tmpName.split(" - ")[0].replace(" Snowboard","")
            elif "Snowboard" in item["name"]:
                item["name_processed"] = tmpName.split(" Snowboard ")[0]
            else:
                item["name_processed"] = " ".join(tmpName.split(" ")[0:-1])

            item["img_url_processed"] = "gear/" + item["img_url"].split("/")[-1]

            # imageItem = ImageItem()
            # imageItem['image_urls'] = item["img_url"]
            # yield imageItem



        except:
            self.log("Error while Post Processing of some fields")



        yield EvoSnowboardItem(**item)






class EvoImageSpider(scrapy.Spider):
    name = 'evo_image'
    allowed_domains = ['evo.com']
    start_urls = ['https://www.evo.com/shop/snowboard/snowboards/rpp_400']
    base_url = 'https://www.evo.com'
    folder_name = 'evo/snowboard'
    custom_settings = {
        'ITEM_PIPELINES': {
           'snowboard_crawl.pipelines.MyImagesPipeline': 400,
           # 'scrapy.pipelines.images.ImagesPipeline': 400,
        }
    }

    def parse(self, response):

        # pagination
        if "snowboards/p_" not in response.url :
            pages = set(response.css("div.results-pagination-numerals a::attr(href)").extract())
            if "javascript:void(0);" in pages: pages.remove("javascript:void(0);")
            for eachPage in pages:
                if eachPage[0] == "#" : eachPage = eachPage[1:]
                pagination_url = "%s%s" %(self.base_url , eachPage)
                request = scrapy.Request(url = pagination_url, 
                                     callback=self.parse,
                                     )
                self.log('Pagination crawling %s' % pagination_url)
                yield request



        for _item in response.css("div.product-thumb"):
            # print(_item.extract())
            img_url = _item.css("img::attr(src)").extract_first()
            if img_url:
                img_url = img_url.replace("https://images.evo.com/imgp/250","https://images.evo.com/imgp/700")
                item = { 
                         "image_urls": [ img_url ]
                       }
                yield EvoSnowboardImageItem(**item)
            else:
                self.log('Problem crawling image %s' % img_url)

    def parse_structured_data(self, response):

        data_dict = {}
        
        data_dump = re.match(r".*window\.dataLayerManager\.pushImpressions\((.*)\, \'Results", response.text, re.DOTALL).group(1)
        data_json = json.loads(data_dump)
        for element in data_json:
            data_dict[element["_uniqueId"]] = element

        return data_dict
