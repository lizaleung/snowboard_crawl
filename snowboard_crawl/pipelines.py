# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter


# useful for handling different item types with a single interface
import json
import os
import datetime

from slugify import slugify
import logging

from scrapy.pipelines.images import ImagesPipeline
from snowboard_crawl.settings import outdir

import scrapy

from itemadapter import ItemAdapter
from snowboard_crawl import settings
import pymysql
 

from scrapy.exceptions import DropItem


class CustomImagePipeline(ImagesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        basepath = settings.IMAGES_STORE
        name = request.url.split('/')[-1]
        # print("basepath ", basepath)
        print('正在下载：',name)
        return os.path.join(basepath,name)

    def get_media_requests(self, item, info):
        for image_url in item['image_urls']:
            yield scrapy.Request(image_url)

    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            raise DropItem("Item contains no images")
        adapter = ItemAdapter(item)
        adapter['image_paths'] = image_paths
        print(image_paths)
        return item
        

class SnowboardCrawlPipeline:
 
    def __init__(self):
 
        self.connect = pymysql.connect(
            host=settings.MYSQL_HOST,
            db=settings.MYSQL_DATABASE,
            user=settings.MYSQL_USERNAME,
            passwd=settings.MYSQL_PASSWORD,
            charset='utf8'
        )
 
        self.cursor = self.connect.cursor()
 
    def process_item(self, item, spider):

        brand_id = 1
        try:
            sql = "select id from gears_brand where name = '%s'" % item['brand']

            mycursor = self.cursor
            mycursor.execute(sql)
            sqlretval = mycursor.fetchone()
            brand_id = sqlretval[0]
            # self.log(brand_id)
        except:
            self.log("error while looking for brand_id for - %s" %item['brand'])

        
 
        sql = "REPLACE INTO gears_gear(title, \
                                    description, \
                                    year,\
                                    slug, \
                                    image, \
                                    created_at, \
                                    updated_at, \
                                    category_id, \
                                    contributor_id, \
                                    brand_id ) \
                                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
 
        data = (item['name_processed'], \
            item['description'], \
            item['year_processed'],\
            slugify(item['name']), \
            item['image_paths'][0], \
            "2022-01-01", \
            "2022-01-01", \
            1, 1, 
            brand_id) 
 
        
        self.cursor.execute(sql, data)

        # return item
        return
 
    def close_spider(self, spider):
        self.connect.commit()
        self.connect.close()



# from urllib.parse import urlparse


# class MyImagesPipeline(ImagesPipeline):

#     def file_path(self, request, response=None, info=None, *, item=None):
#         return 'files/' + os.path.basename(urlparse(request.url).path)





