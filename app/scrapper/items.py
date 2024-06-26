# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Item, Field



class ScrapperItem(scrapy.Item):
  # define the fields for your item here like:
  # name = scrapy.Field()
  pass

class Job(scrapy.Item):
  url = scrapy.Field()
  status = scrapy.Field()
  progress = scrapy.Field()
  message = scrapy.Field()
