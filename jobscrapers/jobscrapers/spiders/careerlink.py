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
        self.stopped = False

    def _get_mode(self):
        return self.crawler.settings.get("CRAWL_MODE", "daily")

    @staticmethod
    def _is_old(posted_text: str) -> bool:
        """
        Careerlink format:
          "Đăng 2 giờ trước"   → False
          "Đăng 3 ngày trước"  → True
          "Đăng 1 tuần trước"  → True
        """
        if not posted_text:
            return False
        return bool(re.search(
            r"\d+\s+(?:ngày|tuần|tháng|năm)\s+trước",
            posted_text,
            re.IGNORECASE,
        ))

    def parse(self, response):
        if self.stopped:
            return

        jobs = response.css(".list-group li.list-group-item")
        for job in jobs:
            job_url    = job.css(".media-body a::attr(href)").get()
            # Lấy posted_at từ card để check trước khi follow
            posted_raw = job.css(".text-muted::text").get("").strip()

            if self._get_mode() == "daily" and self._is_old(posted_raw):
                self.logger.info(
                    f"[careerlink][daily] Gặp job cũ ({posted_raw!r}) — dừng"
                )
                self.stopped = True
                return

            if job_url:
                yield response.follow(
                    job_url,
                    callback=self.parse_job_page,
                    cb_kwargs={"job_posted_at": posted_raw},
                )

        # Next page — fix bug: thêm đúng prefix https://www.
        if not self.stopped:
            next_href = response.css(".page-item.active + .page-item a::attr(href)").get()
            if next_href:
                next_url = (
                    next_href
                    if next_href.startswith("http")
                    else "https://www.careerlink.vn" + next_href
                )
                yield scrapy.Request(url=next_url, callback=self.parse)

    def parse_job_page(self, response, job_posted_at=""):
        def xpath(query):
            return response.xpath(query).get("").strip()

        def xpath_all(query):
            return " ".join(response.xpath(query).getall()).strip()

        item = JobItem()
        # Không có trailing comma — tránh tạo tuple
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
        item["work_mode"]        = ""
        item["level"]            = xpath(
            "//div[contains(text(),'Cấp bậc')]/following-sibling::div/text()"
        )
        item["company_title"]    = response.css(
            ".company-info .company-name-title a span::text"
        ).get("").strip()
        item["company_size"]     = xpath(
            '//i[contains(@class,"cli-users")]/following-sibling::span/text()'
        )
        item["company_industry"] = xpath(
            "//div[contains(text(),'Ngành nghề')]/following-sibling::div/text()"
        )
        item["job_category"]     = ""
        item["number_recruit"]   = ""
        item["education_level"]  = xpath(
            "//div[contains(text(),'Học vấn')]/following-sibling::div/text()"
        )
        item["job_description"]  = xpath_all(
            '//div[@id="section-job-description"]//li/text()'
        )
        item["job_requirement"]  = xpath_all(
            '//div[@id="section-job-skills"]//li/text()'
        )
        item["job_posted_at"]    = job_posted_at or xpath(
            '//div[contains(@class,"date-from")]//span[@class="d-flex"]/text()[normalize-space()]'
        )
        item["job_deadline"]     = xpath(
            "//div[@id='job-date']//div[contains(@class,'day-expired')]//b/text()"
        )
        item["scraped_at"]       = datetime.now()

        yield item