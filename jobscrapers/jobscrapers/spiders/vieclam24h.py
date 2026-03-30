import scrapy
from datetime import datetime
from jobscrapers.items import JobItem


class Vieclam24hSpider(scrapy.Spider):
    name = "vieclam24h"
    allowed_domains = ["vieclam24h.vn"]
    start_urls = ["https://vieclam24h.vn/viec-lam-it-phan-mem-o8.html"]

    def parse(self, response):
        # Dựa vào HTML: mỗi job card là <a data-job-id="...">
        jobs = response.css('a[data-job-id]')

        for job in jobs:
            job_url = job.attrib.get('href')
            if job_url:
                yield response.follow(job_url, callback=self.parse_job_page)

        # Pagination - cần inspect thêm trang vieclam24h để lấy đúng selector
        next_page = response.css('a[rel="next"]::attr(href)').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_job_page(self, response):
        job_item = JobItem()
        job_item['website'] = 'vieclam24h'
        job_item['job_url'] = response.url
        # --- Header info ---
        job_item['job_title'] = response.css('h1::text').get()
        # Icon-based fields (salary, location, experience)
        job_item['compensation'] = response.xpath("//div[div[text()='Mức lương']]/div[contains(@class,'text-14')]/text()").get()
        job_item['location'] = response.xpath("//div[div[text()='Khu vực tuyển']]//a/span/text()").get()
        job_item['experience'] = response.xpath("//div[div[text()='Kinh nghiệm']]/div[contains(@class,'text-14')]/text()").get()
        job_item['job_deadline'] = response.xpath("//div[contains(text(),'Hạn nộp hồ sơ')]/following-sibling::div[1]/text()").get()

        # --- "Thông tin chung" grid ---
        job_item['job_posted_at'] = response.xpath("//div[./div[text()='Ngày đăng']]/div[2]/text()").get()
        job_item['level'] = response.xpath("//div[./div[text()='Cấp bậc']]/div[2]/text()").get()
        job_item['number_recruit'] = response.xpath("//div[./div[text()='Số lượng tuyển']]/div[2]/text()").get()
        job_item['job_type'] = response.xpath("//div[./div[text()='Hình thức làm việc']]/div[2]/text()").get()
        job_item['company_industry'] = response.xpath("//div[./div[text()='Ngành nghề']]/div[2]//a/text()").getall()
        job_item['job_category']    =''
        # --- Nội dung ---
        job_item['job_description'] = response.xpath("//h2[contains(text(),'Mô tả công việc')]/following-sibling::div[1]//text()").getall()
        job_item['job_requirement'] = response.xpath("//h2[contains(text(),'Yêu cầu công việc')]/following-sibling::div[1]//text()").getall()
        # --- Để trống vì không có trong HTML ---
        job_item['work_mode'] = ''
        job_item['education_level'] = ''
        job_item['company_title'] = response.xpath("//i[contains(@class,'svicon-users')]/ancestor::div[contains(@class,'flex flex-col gap-3')]//a[@title]/div/text()").get()
        job_item['company_size'] = response.xpath("//i[contains(@class,'svicon-users')]/following-sibling::div/text()").get()
        job_item['scraped_at'] = datetime.now()
        yield job_item