
import os
import datetime

from slugify import slugify
import logging

import scrapy
from scrapy.pipelines.images import ImagesPipeline

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from snowboard_crawl import settings
import pymysql

from scrapy.exceptions import DropItem


class CustomImagePipeline(ImagesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        basepath = "gear"
        name = request.url.split('/')[-1]
        # print("basepath ", basepath)
        # print('正在下载：',name)
        if name == "clone.jpg": name = slugify(item['name']) + ".jpg"
        logging.info('正在下载： %s' %name)
        return os.path.join(basepath,name)

    def get_media_requests(self, item, info):
        for image_url in item['image_urls']:
            yield scrapy.Request(image_url)

    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            raise DropItem("Item contains no images")
        else:
            logging.info(image_paths)
        adapter = ItemAdapter(item)
        adapter['image_paths'] = image_paths
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

        slug_name = slugify(item['name'])

        brand_id = 1
        try:
            sql = "select id from gears_brand where name = '%s'" % item['brand']
            mycursor = self.cursor
            mycursor.execute(sql)
            sqlretval = mycursor.fetchone()
            brand_id = sqlretval[0]
            spider.log("brand_id = %s" %brand_id)
        except:
            spider.log("error while looking for brand_id for - %s" %item['brand'])

        if item['year_processed'] == 0: 
            spider.log("Error - slug = %s because year is 0" %slug_name)
            return


        ## Gear
 
        sql = "INSERT INTO gears_gear(title, \
                                    description, \
                                    year,\
                                    slug, \
                                    image, \
                                    created_at, \
                                    updated_at, \
                                    category_id, \
                                    contributor_id, \
                                    is_trending,\
                                    brand_id ) \
                                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        
        data = (item['name_processed'], \
            item['description'], \
            item['year_processed'],\
            slug_name, \
            item['image_paths'][0], \
            datetime.datetime.now().strftime('%Y-%m-%d'), \
            datetime.datetime.now().strftime('%Y-%m-%d'), \
            1, \
            1, \
            0,
            brand_id) 
        try:
            self.cursor.execute(sql, data)
            spider.log("Finished executing sql to db %s" %slug_name)
            # lastrowid = self.cursor.lastrowid
            # spider.log("Finished executing sql to db lastrowid %s" %lastrowid)
        except pymysql.Error as e:
            spider.log("Error executing sql slug = %s" %slug_name)
            spider.log(e)
        
        ## Get gear_id for later use - 
        gear_id = 0
        try:
            sql = "select id from gears_gear where slug = '%s'" % slug_name
            mycursor = self.cursor
            mycursor.execute(sql)
            sqlretval = mycursor.fetchone()
            gear_id = sqlretval[0]
            spider.log("gear_id = %s" %gear_id)
        except:
            logging.warning("error while looking for gear_id for - %s" %slug_name)

        if gear_id != 0:
            #################
            ## GearFeature ##
            #################
            
            sql = "INSERT INTO gears_gearfeature (gear_id, attribute, detail) \
                                        VALUES(%s,%s,%s) "
            for eachFeature in item["feature"]:
                data = (gear_id, \
                        eachFeature, \
                        item["feature"][eachFeature]
                        ) 
                try:
                    self.cursor.execute(sql, data)
                    spider.log("Finished executing sql to db gear_id=%s feature=%s" %(gear_id,eachFeature))
                except pymysql.Error as e:
                    spider.log("error while executing sql for gearfeature gear_id - %s" %gear_id)
                    spider.log(e)


            #################
            ## GearSpec ##
            #################

            
            sql = "INSERT INTO gears_gearspec (gear_id, attribute, detail) \
                                            VALUES(%s,%s,%s) "
            for eachSpec in item["spec"]:
                data = (gear_id, \
                        eachSpec, \
                        item["spec"][eachSpec]
                        ) 
                try:
                    self.cursor.execute(sql, data)
                    spider.log("Finished executing sql to db gear_id=%s spec=%s" %(gear_id,eachSpec))
                except pymysql.Error as e:
                    spider.log("error while executing sql for gearspec gear_id - %s" %gear_id)
                    spider.log(e)


            #################
            ## GearSize ##
            #################

            sql1 = "INSERT INTO gears_gearsize (gear_id, size, size_value) \
                                            VALUES(%s,%s,%s) "

            sql2 = "INSERT INTO gears_gearsizedetail (gear_size_id, attribute, detail)  \
                                            VALUES(%s,%s,%s) "

            for eachSize in item["size_table"]:
                size_value = item["size_table"][eachSize]["Size (cm)"] if "Size (cm)" in item["size_table"][eachSize] else 0
                data1 = (gear_id, eachSize, size_value) 
                try:
                    self.cursor.execute(sql1, data1)
                    spider.log("Finished executing sql to db gear_id=%s %s %s" %(gear_id, eachSize, size_value))

                except pymysql.Error as e:
                    spider.log("error while executing sql for gearsize gear_id - %s %s %s" %(gear_id, eachSize, size_value))
                    spider.log(e)


                ## Get gear_size_id for later use - 
                gear_size_id = 0
                try:
                    sql = "select id from gears_gearsize where gear_id = '%s' and size = '%s'  " % (gear_id, eachSize)
                    mycursor = self.cursor
                    mycursor.execute(sql)
                    sqlretval = mycursor.fetchone()
                    gear_size_id = sqlretval[0]
                    spider.log("gear_size_id = %s" %gear_size_id)
                except:
                    logging.warning("error while looking for gear_id %s size %s" %(gear_id,eachSize))




                for eachSizeDetail in item["size_table"][eachSize]:
                    data2 = (gear_size_id, \
                            eachSizeDetail, \
                            item["size_table"][eachSize][eachSizeDetail] \
                            ) 

                    try:
                        self.cursor.execute(sql2, data2)
                        spider.log("Finished executing sql to db gear_id=%s size=%s sizedetail=%s" %(gear_id,eachSize, eachSizeDetail))
                    except pymysql.Error as e:
                        spider.log("error while executing sql for gearsizedetail gear_id %s - size %s - sizedetail %s" %(gear_id, eachSize,eachSizeDetail))
                        spider.log(e)




        return
 
    def close_spider(self, spider):
        self.connect.commit()
        self.connect.close()





