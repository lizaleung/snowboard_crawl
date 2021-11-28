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

from scrapy.pipelines.images import ImagesPipeline
from snowboard_crawl.settings import outdir


from itemadapter import ItemAdapter
from snowboard_crawl import settings
import pymysql
 
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

        
 
        sql = "INSERT INTO gears_gear(title, \
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
            item['name'].replace(" ","-").replace("'","").lower(), \
            item['img_url_processed'], \
            "2010-01-01", \
            "2010-01-01", \
            1, 1, 
            brand_id) 
 
        
        self.cursor.execute(sql, data)

        # return item
        return
 
    def close_spider(self, spider):
        self.connect.commit()
        self.connect.close()



from urllib.parse import urlparse


class MyImagesPipeline(ImagesPipeline):

    def file_path(self, request, response=None, info=None, *, item=None):
        return 'files/' + os.path.basename(urlparse(request.url).path)


