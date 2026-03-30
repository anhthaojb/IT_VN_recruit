# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Item, Field 

class JobscrapersItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
class JobItem(scrapy.Item):
    website = scrapy.Field()
    job_title = scrapy.Field()
    company_title = scrapy.Field()
    location = scrapy.Field()
    experience = scrapy.Field()
    compensation = scrapy.Field()
    job_type = scrapy.Field()
    work_mode = scrapy.Field()
    level = scrapy.Field()
    job_url = scrapy.Field()
    company_size = scrapy.Field()
    company_industry = scrapy.Field()
    job_category= scrapy.Field()
    number_recruit = scrapy.Field()
    education_level= scrapy.Field()
    job_description = scrapy.Field()
    job_requirement = scrapy.Field()
    job_posted_at = scrapy.Field()
    job_deadline = scrapy.Field()
    scraped_at = scrapy.Field()