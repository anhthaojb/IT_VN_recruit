import scrapy
import re
from datetime import datetime
from jobscrapers.items import JobItem


class TopcvSpider(scrapy.Spider):
    name = "topcv"
    allowed_domains = ["topcv.vn"]

    BASE_URL = "https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257"
    PAGE_PARAMS = "?sort=newp&page={page}&category_family=r257"
    # USER_AGENTS = [
    #     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    #     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    #     "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    #     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/122.0.0.0",
    #     "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    #     "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.5; rv:124.0) Gecko/20100101 Firefox/124.0",
    #     "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
    #     "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    # ]
    # def _random_ua(self):
    #     return {"User-Agent": random.choice(self.USER_AGENTS)}
    async def start(self):
        yield scrapy.Request(
            url=self.BASE_URL + self.PAGE_PARAMS.format(page=1),
            callback=self.parse,
            cb_kwargs={"page": 1},
            # headers=self._random_ua(),
        )

    def parse(self, response, page=1):
        if page == 1:
            pagination_text = response.css("#job-listing-paginate-text::text").get("")
            match = re.search(r"/\s*(\d+)\s*trang", pagination_text)
            self.max_page = int(match.group(1)) if match else 1
            self.logger.info(f"Tổng số trang: {self.max_page}")

        for job in response.css("div.job-item-search-result"):
            job_url = job.css("h3.title a::attr(href)").get()
            job_posted_at = job.css("label.label-update::text").getall()

            if job_url:                          # ← phải nằm TRONG for loop
                yield response.follow(
                    job_url,
                    callback=self.parse_job_page,
                    cb_kwargs={"job_posted_at": job_posted_at[-1].strip() if job_posted_at else ""},
                    # headers=self._random_ua(),
                )

        next_page = page + 1                    
        if next_page <= getattr(self, "max_page", 1):
            yield scrapy.Request(
                url=self.BASE_URL + self.PAGE_PARAMS.format(page=next_page),
                callback=self.parse,
                cb_kwargs={"page": next_page},
                # headers=self._random_ua(),
            )

    def parse_job_page(self, response, job_posted_at=None):
        def css(selector):
            return response.css(selector).get("").strip()

        def xpath(query):
            return response.xpath(query).get("").strip()

        def xpath_all(query):
            return " ".join(response.xpath(query).getall()).strip()

        job_item = JobItem()
        job_item["website"]          = "topcv"
        job_item["job_url"]          = response.url
        job_item["job_title"]        = css("h1.job-detail__info--title::text")
        job_item["location"]         = xpath_all("//div[contains(text(),'Địa điểm') and contains(@class,'job-detail__info--section-content-title')]/following-sibling::div//text()")
        job_item["experience"]       = xpath("//div[div[contains(text(),'Kinh nghiệm')]]/div[contains(@class,'value')]//text()")
        job_item["compensation"]     = xpath("//div[div[contains(text(),'Mức lương')]]/div[contains(@class,'value')]//text()")
        job_item["job_type"]         = xpath("//div[div[contains(text(),'Hình thức làm việc')]]/div[contains(@class,'value')]//text()")
        job_item["work_mode"]        = xpath_all("//h3[contains(text(),'Thời gian làm việc')]/following-sibling::div//text()")
        job_item["level"]            = xpath("//div[div[contains(text(),'Cấp bậc')]]/div[contains(@class,'value')]//text()")
        job_item["company_title"]    = css("a.name::text")
        job_item["company_size"]     = xpath("//div[contains(@class,'company-scale')]//div[@class='company-value']//text()")
        job_item["company_industry"] = xpath("//div[@class='company-title' and contains(normalize-space(),'Lĩnh vực:')]/following-sibling::div[@class='company-value']//text()")
        job_item['job_category']     =''
        job_item["number_recruit"]   = xpath("//div[div[contains(text(),'Số lượng tuyển')]]/div[contains(@class,'value')]//text()")
        job_item["education_level"]  = xpath("//div[div[contains(text(),'Học vấn')]]/div[contains(@class,'value')]//text()")
        job_item["job_description"]  = xpath_all("//h3[contains(text(),'Mô tả công việc')]/following-sibling::div[@class='job-description__item--content']//*//text()")
        job_item["job_requirement"]  = xpath_all("//h3[contains(text(),'Yêu cầu ứng viên')]/following-sibling::div[@class='job-description__item--content']//*//text()")
        job_item["job_posted_at"] = job_posted_at
        job_item["job_deadline"]     = css("div.job-detail__info--deadline::text").replace("Hạn nộp hồ sơ: ", "")
        job_item["scraped_at"]       = datetime.now()

        yield job_item