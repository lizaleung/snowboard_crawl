# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class EvoSnowboardItem(scrapy.Item):
    unique_id = scrapy.Field()
    item_id = scrapy.Field()
    url = scrapy.Field()
    price = scrapy.Field()
    name = scrapy.Field()
    brand = scrapy.Field()
    description = scrapy.Field()
    size_table = scrapy.Field()
    feature = scrapy.Field()
    spec = scrapy.Field()
    name_processed = scrapy.Field()
    year_processed = scrapy.Field()
    image_urls = scrapy.Field()
    images = scrapy.Field()
    image_paths = scrapy.Field()


class EvoSnowboardImageItem(scrapy.Item):
    # url = scrapy.Field()
    image_urls = scrapy.Field()
    images = scrapy.Field()

