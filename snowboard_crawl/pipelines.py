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

# from snowboard_crawl.items import EvoSnowboardItem, EvoSnowboardImageItem
from scrapy.pipelines.images import ImagesPipeline
from snowboard_crawl.settings import outdir


# from sqlalchemy.orm import sessionmaker
# from models import Channels, db_connect, create_channel_table

# class SnowboardCrawlPipeline(object):

#     def __init__(self):
#         #Initializes database connection and sessionmaker.
#         engine = db_connect()
#         create_channel_table(engine)
#         Session = sessionmaker(bind=engine)
#         self.session = Session()

#     def process_item(self, item, spider):
#         # check if item with this title exists in DB
#         item_exists = self.session.query(Channels).filter_by(title=item['title']).first()
#         # if item exists in DB - we just update 'date' and 'subs' columns.
#         if item_exists:
#             item_exists.date = item['date']
#             item_exists.subs = item['subs'] 
#             print('Item {} updated.'.format(item['title']))
#         # if not - we insert new item to DB
#         else:     
#             new_item = Channels(**item)
#             self.session.add(new_item)
#             print('New item {} added to DB.'.format(item['title']))
#         return item    

#     def close_spider(self, spider):
#         # We commit and save all items to DB when spider finished scraping.
#         try:
#             self.session.commit()
#         except:
#             self.session.rollback()
#             raise
#         finally:
#             self.session.close()



# # -*- coding: utf-8 -*-
# import logging
# from scrapy import signals

# class SnowboardCrawlPipeline(object):

#     def __init__(self):
#         self.items=[]

#     def process_item(self, item, spider):
            
#         self.placeholders = ', '.join(['%s'] * len(item))
#         self.columns = ', '.join(item.keys())
#         self.query = "INSERT INTO %s ( %s ) VALUES ( %s )" % ("table_name", self.columns, self.placeholders)
        
#         self.items.extend([item.values()])
        
#         if len(self.items) >= 50:

#             try:
#                 spider.cursor.executemany(self.query, self.items)
#                 self.items = []    
#             except Exception as e:
#                 if 'MySQL server has gone away' in str(e):
#                     spider.connectDB()
#                     spider.cursor.executemany(self.query, self.items)
#                     self.items = []    
#                 else:   
#                     raise e
#         return item



#         def close_spider(self, spider):
#             try:
#                 spider.cursor.executemany(self.query, self.items)
#                 self.items = []    
#             except Exception as e:
#                     if 'MySQL server has gone away' in str(e):
#                         spider.connectDB()
#                         spider.cursor.executemany(self.query, self.items)
#                         self.items = []    
#                     else:   
#                         raise e


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


# class MyImagesPipeline(ImagesPipeline):

#     #Name download version
#     def file_path(self, request, response=None, info=None):
#         #item=request.meta['item'] # Like this you can use all from item, not just url.
#         image_guid = request.url.split('/')[-1]
#         return 'full/%s' % (image_guid)

#     #Name thumbnail version
#     def thumb_path(self, request, thumb_id, response=None, info=None):
#         image_guid = thumb_id + response.url.split('/')[-1]
#         return 'thumbs/%s/%s.jpg' % (thumb_id, image_guid)

#     def get_media_requests(self, item, info):
#         #yield Request(item['images']) # Adding meta. I don't know, how to put it in one line :-)
#         for image in item['images']:
#             yield Request(image)