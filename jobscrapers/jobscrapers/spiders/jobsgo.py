import scrapy
from scrapy_playwright.page import PageMethod
from datetime import datetime
from jobscrapers.items import JobItem


class JobsgoSpider(scrapy.Spider):
    name = "jobsgo"
    allowed_domains = ["jobsgo.vn"]

    async def start(self):
        yield scrapy.Request(
            url="https://jobsgo.vn/viec-lam-cong-nghe-thong-tin.html",
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "div.job-card", timeout=15000),
                ],
                "playwright_context_kwargs": {
                    "java_script_enabled": True,
                },
            },
            callback=self.parse,
        )

    def parse(self, response):
        jobs = response.css('div.job-card')
        self.logger.info(f"Found {len(jobs)} jobs on {response.url}")

        for job in jobs:
            job_url = job.css('a.text-decoration-none::attr(href)').get()
            if job_url:
                yield scrapy.Request(
                    url=job_url,
                    meta={
                        "playwright": True,
                        "playwright_page_methods": [
                            PageMethod("wait_for_selector", "h1", timeout=15000),
                        ],
                        "list_title": job.css('h3.job-title::text').get('').strip(),
                        "list_company": job.css('div.company-title::text').get('').strip(),
                        "list_salary": job.css('div.text-primary span:first-child::text').get('').strip(),
                        "list_location": job.css('div.text-primary span:last-child::text').get('').strip(),
                        "list_job_type": job.css('span[title="Loại hình"]::text').get('').strip(),
                        "list_experience": job.css('span[title="Yêu cầu kinh nghiệm"]::text').get('').strip(),
                    },
                    callback=self.parse_job_page,
                )

        next_page = response.css('ul.pagination li.next a::attr(href)').get()
        if next_page:
            yield scrapy.Request(
                url=response.urljoin(next_page),
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", "div.job-card", timeout=15000),
                    ],
                },
                callback=self.parse,
            )

    def parse_job_page(self, response):
        job_item = JobItem()
        job_item['website'] = 'jobsgo'
        job_item['job_url'] = response.url
        job_item['job_title'] = response.css('h1.job-title::text').get('').strip()
        job_item['company_title'] = response.css('h6.fw-semibold::text').get('').strip()
        job_item['compensation'] = response.xpath(
            "//span[contains(.,'Mức lương')]/strong/text()"
        ).get('').strip()
        job_item['location'] = response.xpath(
            "string(//span[contains(.,'Địa điểm')]/strong)"
        ).get('').strip()
        job_item['experience'] = response.xpath(
            "//span[contains(.,'Kinh nghiệm')]/strong/text()"
        ).get('').strip()
        job_item['education_level'] = response.xpath(
            "//span[contains(.,'Bằng cấp')]/strong/text()"
        ).get('').strip()
        job_item['job_deadline'] = response.xpath(
            "//span[contains(.,'Hạn nộp hồ sơ')]/following-sibling::strong/text()"
        ).get('').strip()
        job_item['job_description'] = response.xpath(
            "//h3[contains(text(),'Mô tả công việc')]/following-sibling::div[1]//text()"
        ).getall()
        job_item['job_requirement'] = response.xpath(
            "//h3[contains(text(),'Yêu cầu công việc')]/following-sibling::div[1]//text()"
        ).getall()
        job_item['job_type'] = response.xpath(
            "//span[contains(.,'Loại hình')]/following-sibling::strong/text()"
        ).get('').strip()
        job_item['level'] = response.xpath(
            "//span[contains(.,'Cấp bậc')]/following-sibling::strong/text()"
        ).get('').strip()
        job_item['job_posted_at'] = response.xpath(
            "//span[contains(.,'Ngày đăng tuyển')]/following-sibling::strong/text()"
        ).get('').strip()
        job_item['job_category']= response.xpath(
            "//div[contains(@class,'text-muted') and contains(text(),'Ngành nghề:')]"
            "/following-sibling::strong//a/text()"
        ).getall()
        job_item['work_mode'] = ''
        job_item['number_recruit'] = ''
        job_item['scraped_at'] = datetime.now()

        company_url = response.css('div.card-company a::attr(href)').get()
        if company_url:
            yield scrapy.Request(
                url=response.urljoin(company_url),
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", "div.company-info", timeout=15000),
                    ],
                    "job_item": job_item,
                },
                callback=self.parse_company_page,
            )
        else:
            yield job_item

    def parse_company_page(self, response):
        job_item = response.meta['job_item']
        job_item['company_size'] = response.css('li.d-flex i.pb-heroicons-users ~ span::text').get('').strip()
        job_item['company_industry'] = response.css('div.company-category span.company-category-list span::text').get('').strip()
        yield job_item