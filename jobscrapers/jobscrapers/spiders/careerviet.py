import scrapy
from datetime import datetime
from jobscrapers.items import JobItem
import re

class CareervietSpider(scrapy.Spider):
    name = "careerviet"
    allowed_domains = ["careerviet.vn"]
    start_urls = ["https://careerviet.vn/viec-lam/cntt-phan-mem-c1-vi.html"]

    def parse(self, response):
        pass
        jobs = response.css('.jobs-side-list .job-item')
        if not jobs:
            return
        for job in jobs:
            job_url= job.css('.title h2 a::attr(href)').get()
            yield response.follow(job_url, callback=self.parse_job_page)
        current_url = response.url

        match = re.search(r'trang-(\d+)', current_url)
        if match:
            page_num = int(match.group(1))
        else:
            page_num = 1
        next_page = page_num + 1
        next_url = f"https://careerviet.vn/viec-lam/cntt-phan-mem-c1-trang-{next_page}-vi.html"
        yield scrapy.Request(next_url, callback=self.parse)


    
    def parse_job_page(self, response):
        job_item = JobItem()
        job_item['website'] = 'careerviet'
        job_item['job_url']= response.url
        job_item['job_title'] =response.css('.job-desc h1::text').get(),
        job_item['location'] = response.css('.detail-box .map p a::text').get(),
        job_item['experience']= response.xpath('//li[.//strong[contains(.,"Kinh nghiệm")]]/p/text()').getall(),
        job_item['compensation']=  response.xpath('//li[.//strong[contains(.,"Lương")]]/p/text()').get(), 
        job_item['job_type'] = response.xpath('//li[.//strong[contains(.,"Hình thức")]]/p/text()').get(),
        job_item['work_mode']='',
        job_item['level']= response.xpath('//li[.//strong[contains(.,"Cấp bậc")]]/p/text()').get(),   
        job_item['company_industry']=response.xpath('//li[.//strong[contains(.,"Ngành nghề")]]/p//text()').get(),
        job_item['job_category']     =''
        job_item['number_recruit']='',
        job_item['education_level']= response.xpath('normalize-space(substring-after(//li[contains(text(),"Bằng cấp")], ":"))').get(),
        job_item['job_description']=response.xpath('//div[h2[contains(text(),"Mô tả Công việc")]]//div//text()').getall(),
        job_item['job_requirement']=response.xpath('//div[h2[contains(text(),"Yêu Cầu Công Việc")]]//div//text()').getall(),
        job_item['job_posted_at']=response.xpath('//li[.//strong[contains(.,"Ngày cập nhật")]]/p/text()').get(),  
        job_item['job_deadline']= response.xpath('//li[.//strong[contains(.,"Hết hạn nộp")]]/p/text()').get(),
        job_item['scraped_at'] = datetime.now()   
        # company_url ='careerviet.vn' + response.css('.job-desc a::attr(href)').get()
        company_url =response.css('.job-desc a::attr(href)').get()
        if company_url:
            yield response.follow(
                company_url,
                callback=self.parse_company_info,
                meta={'job_item': job_item}
            )
        else:
            yield job_item 
            
    def parse_company_info(self, response):
        
        job_item = response.meta['job_item']
        job_item['company_title'] = response.css('.company-info h1::text').get(),
        job_item['company_size'] =response.xpath('normalize-space(substring-after(//li[contains(.,"Quy mô công ty")], ":"))').get(),

        yield job_item

#còn size của công ty chưa lấy đc