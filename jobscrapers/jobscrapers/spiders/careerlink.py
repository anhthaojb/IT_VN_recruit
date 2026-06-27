import scrapy
import re
from datetime import datetime
from jobscrapers.items import JobItem


class CareerlinkSpider(scrapy.Spider):
    name = "careerlink"
    allowed_domains = ["careerlink.vn"]
    start_urls = ["https://www.careerlink.vn/viec-lam/cntt-phan-mem/19"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stopped    = False
        self.page_count = 0

    def _get_mode(self):
        return self.crawler.settings.get("CRAWL_MODE", "daily")

    @staticmethod
    def _is_old(posted_text: str) -> bool:
        """
        Careerlink detail format: "28-03-2026"
        """
        if not posted_text:
            return False
        m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", posted_text.strip())
        if m:
            try:
                posted_date = datetime(
                    int(m.group(3)), int(m.group(2)), int(m.group(1))
                ).date()
                return (datetime.now().date() - posted_date).days > 1
            except ValueError:
                pass
        return False

    def parse(self, response):
        if self.stopped:
            return

        self.page_count += 1

        jobs = response.css("li.job-item")
        if not jobs:
            self.logger.info("[careerlink] Không còn job — dừng")
            return

        for job in jobs:
            job_url = job.css(".media-body a::attr(href)").get()
            if job_url:
                yield response.follow(
                    job_url,
                    callback=self.parse_job_page,
                    errback=self.handle_error,  
                )

     
        if not self.stopped:
            next_href = response.css(".page-item.active + .page-item a::attr(href)").get()
            if next_href and (self._get_mode() == "full" or self.page_count < 3):
                next_url = (
                    next_href
                    if next_href.startswith("http")
                    else "https://www.careerlink.vn" + next_href
                )
                yield scrapy.Request(
    url=next_url,
    callback=self.parse,
    errback=self.handle_error,  
)
   
    def handle_error(self, failure):
        from scrapy.spidermiddlewares.httperror import HttpError
        from twisted.internet.error import TCPTimedOutError, ConnectionRefusedError

        if failure.check(HttpError):
            response = failure.value.response
            if response.status == 504:
                self.logger.warning(
                    f"[careerlink] 504 Gateway Timeout — dừng spider: {failure.request.url}"
                )
                self.stopped = True
                self.crawler.engine.close_spider(self, "504_timeout")
        else:
            self.logger.error(f"[careerlink] Lỗi không xác định: {failure}")



    def parse_job_page(self, response):
        def xpath(query):
            return response.xpath(query).get("").strip()

        def xpath_all(query):
            return " ".join(response.xpath(query).getall()).strip()


        posted_at_nodes = response.xpath(
            '//div[contains(@class,"date-from")]//span[@class="d-flex"]/text()[normalize-space()]'
        ).getall()
        job_posted_at = posted_at_nodes[-1].strip() if posted_at_nodes else ""

        # Daily check trên detail page
        if self._get_mode() == "daily" and self._is_old(job_posted_at):
            self.logger.info(
                f"[careerlink][daily] Job cũ ({job_posted_at!r}) — bỏ qua: {response.url}"
            )
            return

        item = JobItem()
        item["website"]          = "careerlink"
        item["job_url"]          = response.url
        item["job_title"]        = response.css(".job-title::text").get("").strip()
        item["location"]         = xpath('//div[@id="job-location"]//a/@title')
        item["experience"]       = xpath(
            "//div[contains(text(),'Kinh nghiệm')]/following-sibling::div/text()"
        )
        item["compensation"]     = xpath(
            '//div[@id="job-salary"]/span[contains(@class,"text-primary")]/text()'
        )
        item["job_type"]         = xpath(
            "//div[contains(text(),'Loại công việc')]/following-sibling::div/text()"
        )
        item["work_mode"]        = None
        item["level"]            = xpath(
            "//div[contains(text(),'Cấp bậc')]/following-sibling::div/text()"
        )
        item["company_title"]    = response.css(
            ".company-info .company-name-title a span::text"
        ).get("").strip()
        item["company_size"]     = xpath(
            '//i[contains(@class,"cli-users")]/following-sibling::span/text()'
        )
        item["company_industry"] = " / ".join(
            t.strip()
            for t in response.xpath(
                "//div[contains(text(),'Ngành nghề')]/following-sibling::div//span/text()"
            ).getall()
            if t.strip()
        )
        item["job_category"]     = None
        item["number_recruit"]   = None
        item["education_level"]  = xpath(
            "//div[contains(text(),'Học vấn')]/following-sibling::div/text()"
        )
        item["job_description"]  = xpath_all(
            '//div[@id="section-job-description"]//div[contains(@class,"rich-text-content")]//text()'
        )
        item["job_requirement"]  = xpath_all(
            '//div[@id="section-job-skills"]//div[contains(@class,"rich-text-content")]//text()'
        )
        item["job_posted_at"]    = job_posted_at or None
        item["job_deadline"]     = xpath(
            "//div[@id='job-date']//div[contains(@class,'day-expired')]//b/text()"
        )
        item["scraped_at"]       = datetime.now()

        yield item