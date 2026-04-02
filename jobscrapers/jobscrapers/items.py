# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class JobItem(scrapy.Item):
    # ── Metadata ───────────────────────────────────────────
    website         = scrapy.Field()
    job_url         = scrapy.Field()
    scraped_at      = scrapy.Field()

    # ── Thông tin job ──────────────────────────────────────
    job_title       = scrapy.Field()
    job_category    = scrapy.Field()
    job_type        = scrapy.Field()    # Full-time / Part-time / Contract
    work_mode       = scrapy.Field()    # On-site / Remote / Hybrid
    level           = scrapy.Field()    # Junior / Senior / Manager
    experience      = scrapy.Field()    # "2 năm" / "1-3 years"
    education_level = scrapy.Field()
    number_recruit  = scrapy.Field()
    compensation    = scrapy.Field()
    job_posted_at   = scrapy.Field()
    job_deadline    = scrapy.Field()
    job_description = scrapy.Field()
    job_requirement = scrapy.Field()

    # ── Thông tin công ty ──────────────────────────────────
    company_title   = scrapy.Field()
    company_size    = scrapy.Field()
    company_industry= scrapy.Field()
    location        = scrapy.Field()

    # ── Data quality ───────────────────────────────────────
    # CleaningPipeline tự điền 2 field này — spider không cần set
    is_valid        = scrapy.Field()   # True / False
    error_log       = scrapy.Field()   # None hoặc mô tả lỗi