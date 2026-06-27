
import scrapy

class JobItem(scrapy.Item):
    # ── Metadata ───────────────────────────────────────────
    website         = scrapy.Field()
    job_url         = scrapy.Field()
    scraped_at      = scrapy.Field()

    # ── Thông tin job ──────────────────────────────────────
    job_title       = scrapy.Field()
    job_category    = scrapy.Field()
    job_type        = scrapy.Field()   
    work_mode       = scrapy.Field()    
    level           = scrapy.Field()    
    experience      = scrapy.Field()    
    education_level = scrapy.Field()
    number_recruit  = scrapy.Field()
    compensation    = scrapy.Field()
    job_posted_at   = scrapy.Field()
    job_deadline    = scrapy.Field()
    job_description = scrapy.Field()
    job_requirement = scrapy.Field()
    raw_about_job     = scrapy.Field()    

    # ── Thông tin công ty ──────────────────────────────────
    company_title   = scrapy.Field()
    company_size    = scrapy.Field()
    company_industry= scrapy.Field()
    location        = scrapy.Field()

    # ── Data quality ───────────────────────────────────────
    is_valid        = scrapy.Field()  
    error_log       = scrapy.Field()   