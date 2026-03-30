import scrapy
from datetime import datetime
from jobscrapers.items import JobItem
class JobokoSpider(scrapy.Spider):
    name = "joboko"
    allowed_domains = ["joboko.com"]
    start_urls = ["https://vn.joboko.com/viec-lam-nganh-it-phan-mem-cong-nghe-thong-tin-iot-dien-tu-vien-thong-xni124"]

    def parse(self, response):
        pass
        jobs = response.css('.nw-job-list__list div.item')
        for job in jobs:
            href = job.css('h2 a::attr(href)').get()
            job_url=  'https://vn.joboko.com'+href
            yield response.follow(job_url, callback=self.parse_job_page)
        page =  response.css('.nw-job-list__more a::attr(href)').get()
        pagr_url = 'https://vn.joboko.com'+page
        yield scrapy.Request(url=pagr_url, callback=self.parse)

    
    def parse_job_page(self, response):
        job_item = JobItem()
        job_item['website'] = 'joboko',
        job_item['job_url']= response.url,
        job_item['job_title'] = response.css('h1.nw-company-hero__title a::text').get(),
        job_item['location'] = response.css('.nw-company-hero__address a::text').get(),
        job_item['experience']=  response.xpath("//div[contains(., 'Kinh nghiệm')]/span/text()").get(),
        job_item['compensation']=  response.xpath("//div[contains(., 'Thu thập')]/span/text()").get(), 
        job_item['job_type'] = response.xpath("//div[contains(., 'Loại hình')]/span/text()").get(),
        job_item['work_mode']='',
        job_item['level']= response.xpath("//div[contains(., 'Chức vụ')]/span/text()").get(),
        job_item['company_title'] = response.css('.nw-company-hero__info h2 a::text').get(),
        job_item['company_size']= response.xpath("//span[contains(text(),'Quy mô công ty')]/ancestor::div[1]/following-sibling::div[1]//text()").getall(),     
        job_item['company_industry']='',
        job_item['job_category']     =''
        job_item['number_recruit']='',
        job_item['education_level']='',
        job_item['job_description']=response.xpath("//h3[contains(text(),'Mô tả công việc')]/following-sibling::div[1]//text()").getall(),
        job_item['job_requirement']=response.xpath("//h3[contains(text(),'Yêu cầu')]/following-sibling::div[1]//text()").getall(),
        job_item['job_posted_at']='',  
        job_item['job_deadline']= response.css('em.item-date::attr(data-value)').get(),
        job_item['scraped_at'] = datetime.now()    
        yield job_item