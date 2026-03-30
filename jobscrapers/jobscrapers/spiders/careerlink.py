import scrapy
from datetime import datetime
from jobscrapers.items import JobItem

class CareerlinkSpider(scrapy.Spider):
    name = "careerlink"
    allowed_domains = ["careerlink.vn"]
    start_urls = ["https://www.careerlink.vn/viec-lam/cntt-phan-mem/19"]

    def parse(self, response):
        pass
        jobs = response.css('.list-group li.list-group-item')
        for job in jobs:
            href = job.css('.media-body a::attr(href)').get()
            # job_url=  'careerlink.vn'+href
            job_url= href
            yield response.follow(job_url, callback=self.parse_job_page)
        page =  response.css('.page-item a::attr(href)').get()
        pagr_url = 'careerlink.vn'+page
        yield scrapy.Request(url=pagr_url, callback=self.parse)

    
    def parse_job_page(self, response):
        job_item = JobItem()
        job_item['website'] = 'careerlink',
        job_item['job_url']= response.url,
        job_item['job_title'] = response.css('.job-title::text').get(),
        job_item['location'] = response.xpath('//div[@id="job-location"]//a').attrib['title'],
        job_item['experience']= response.xpath("//div[contains(text(),'Kinh nghiệm')]/following-sibling::div/text()").get(),
        job_item['compensation']=  response.xpath('//div[@id="job-salary"]/span[contains(@class, "text-primary")]/text()').get(), 
        job_item['job_type'] = response.xpath("//div[contains(text(),'Loại công việc')]/following-sibling::div/text()").get(),
        job_item['work_mode']='',
        job_item['level']= response.xpath("//div[contains(text(),'Cấp bậc')]/following-sibling::div/text()").get(),
        job_item['company_title'] = response.css('.company-info .company-name-title a span::text').get(),
        job_item['company_size']= response.xpath('//i[contains(@class,"cli-users")]/following-sibling::span/text()').get(),     
        job_item['company_industry']=response.xpath("//div[contains(text(),'Ngành nghề')]/following-sibling::div/text()").get(),
        job_item['job_category']     =''
        job_item['number_recruit']='',
        job_item['education_level']= response.xpath("//div[contains(text(),'Học vấn')]/following-sibling::div/text()").get(),
        job_item['job_description']=response.xpath('//div[@id="section-job-description"]//li/text()').getall(),
        job_item['job_requirement']=response.xpath('//div[@id="section-job-skills"]//li/text()').getall(),
        job_item['job_posted_at']=response.xpath('//div[contains(@class,"date-from")]//span[@class="d-flex"]/text()[normalize-space()]').get(),  
        job_item['job_deadline']= response.xpath("//div[@id='job-date']//div[contains(@class,'day-expired')]//b/text()").get(),
        job_item['scraped_at'] = datetime.now()    
        yield job_item