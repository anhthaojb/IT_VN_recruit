import scrapy
from scrapy_playwright.page import PageMethod
from datetime import datetime
from jobscrapers.items import JobItem

class Timviec365Spider(scrapy.Spider):
    name = "timviec365"
    allowed_domains = ["timviec365.vn"]
    start_urls = ["https://timviec365.vn/viec-lam-it-phan-mem-c13v0"]

    def parse(self, response):
        jobs = response.css(".boxShowListNew div.item_vl")

        for job in jobs:
            job_url = job.css('div.box_left_vl a::attr(href)').get()
            if job_url:
                yield response.follow(job_url, callback=self.parse_job_page)
        next_page = response.css('.pagi_pre a[rel ="nofollow"]::attr(href)').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_job_page(self, response):
        job_item = JobItem()
        job_item['website'] = 'timviec365'
        job_item['job_url'] = response.url
        job_item['job_title'] = response.css('.boxTitleNameNtd h1::text').get()

        job_item['location'] = response.xpath("//p[text()='Địa điểm']/following-sibling::p/text()").get()
        job_item['experience']= response.xpath("//p[text()='Kinh nghiệm']/following-sibling::p/text()").get()
        job_item['compensation']=  response.css('.valContentSalary.txtSalaryNew::text').get()
        job_item['job_type'] = response.xpath("//p[text()='Hình thức làm việc']/following-sibling::p/text()").get()
        job_item['work_mode']='',
        job_item['level']= response.xpath("//p[text()='Chức vụ']/following-sibling::p/text()").get()   
        job_item['job_category']=response.xpath("//p[text()='Lĩnh vực: ']/following-sibling::div//a/text()").getall()
        job_item['number_recruit']=response.xpath("//p[text()='Số lượng cần tuyển']/following-sibling::p/text()").get(),
        job_item['education_level']= response.xpath("//p[text()='Bằng cấp']/following-sibling::p/text()").get()
        job_item['job_description']=response.xpath("//h2[text()='Mô tả công việc']/ancestor::div[@class='itemInfoSpecific']//div[@class='valInfoSpecific']//text()").getall()
        #chưa lấy đc
        job_item['job_requirement']=response.xpath("//p[text()='Lĩnh vực: ']/following-sibling::div//a/text()").getall()
        job_item['job_posted_at']=response.xpath("(//p[text()='Cập nhật']/following-sibling::p//text())[2]").get() #ngày cập nhật :)  
        job_item['job_deadline']= response.css('.valHanNop::text').get()
        job_item['scraped_at'] = datetime.now()   
        job_item['company_title'] = response.css('.boxTitleNameNtd a::text').get()
        job_item['company_size'] =''
        job_item['company_industry']=''
        yield job_item 
        