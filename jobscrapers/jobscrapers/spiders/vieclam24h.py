import scrapy
import re
from datetime import datetime
from jobscrapers.items import JobItem


class Vieclam24hSpider(scrapy.Spider):
    name = "vieclam24h"
    allowed_domains = ["vieclam24h.vn"]
    start_urls = ["https://vieclam24h.vn/viec-lam-it-phan-mem-o8.html"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stopped = False

    def _get_mode(self):
        return self.crawler.settings.get("CRAWL_MODE", "daily")

    @staticmethod
    def _is_old(posted_text: str) -> bool:
        """
        Vieclam24h format:
          "Hôm nay"        → False
          "Hôm qua"        → True
          "20/03/2026"     → so sánh với hôm nay
          "3 ngày trước"   → True
        """
        if not posted_text:
            return False
        text = posted_text.strip().lower()

        if "hôm nay" in text:
            return False
        if "hôm qua" in text:
            return True
        if re.search(r"\d+\s+ngày\s+trước", text):
            return True

        m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", posted_text)
        if m:
            try:
                posted_date = datetime(
                    int(m.group(3)), int(m.group(2)), int(m.group(1))
                ).date()
                return (datetime.now().date() - posted_date).days > 1
            except ValueError:
                pass
        return False

    # ------------------------------------------------------------------
    # parse — danh sách job
    # ------------------------------------------------------------------

    def parse(self, response):
        if self.stopped:
            return

        jobs = response.css("a[data-job-id]")
        if not jobs:
            self.logger.info("[vieclam24h] Không còn job — dừng")
            return

        for job in jobs:
            job_url    = job.attrib.get("href")
            posted_raw = job.css(".time-post::text, .posted-time::text").get("").strip()

            if self._get_mode() == "daily" and self._is_old(posted_raw):
                self.logger.info(
                    f"[vieclam24h][daily] Gặp job cũ ({posted_raw!r}) — dừng"
                )
                self.stopped = True
                return

            if job_url:
                yield response.follow(
                    job_url,
                    callback=self.parse_job_page,
                    cb_kwargs={"job_posted_at_card": posted_raw},
                )

        if not self.stopped:
            next_page = response.css('a[rel="next"]::attr(href)').get()
            if next_page:
                yield response.follow(next_page, callback=self.parse)

    # ------------------------------------------------------------------
    # parse_job_page — chi tiết job
    # ------------------------------------------------------------------

    def parse_job_page(self, response, job_posted_at_card=""):
        def xpath(query):
            return response.xpath(query).get("").strip()

        def xpath_all(query):
            return " ".join(response.xpath(query).getall()).strip()

        item = JobItem()
        item["website"]         = "vieclam24h"
        item["job_url"]         = response.url
        item["job_title"]       = response.css("h1::text").get("").strip()
        item["compensation"]    = xpath(
            "//div[div[text()='Mức lương']]/div[contains(@class,'text-14')]/text()"
        )
        item["location"]        = xpath(
            "//div[div[text()='Khu vực tuyển']]//a/span/text()"
        )
        item["experience"]      = xpath(
            "//div[div[text()='Kinh nghiệm']]/div[contains(@class,'text-14')]/text()"
        )
        item["job_deadline"]    = xpath(
            "//div[contains(text(),'Hạn nộp hồ sơ')]/following-sibling::div[1]/text()"
        )
        item["job_posted_at"]   = job_posted_at_card or xpath(
            "//div[./div[text()='Ngày đăng']]/div[2]/text()"
        )
        item["level"]           = xpath(
            "//div[./div[text()='Cấp bậc']]/div[2]/text()"
        )
        item["number_recruit"]  = xpath(
            "//div[./div[text()='Số lượng tuyển']]/div[2]/text()"
        )
        item["job_type"]        = xpath(
            "//div[./div[text()='Hình thức làm việc']]/div[2]/text()"
        )
        item["company_industry"]= xpath_all(
            "//div[./div[text()='Ngành nghề']]/div[2]//a/text()"
        )
        item["job_category"]    = ""
        item["job_description"] = xpath_all(
            "//h2[contains(text(),'Mô tả công việc')]"
            "/following-sibling::div[1]//text()"
        )
        item["job_requirement"] = xpath_all(
            "//h2[contains(text(),'Yêu cầu công việc')]"
            "/following-sibling::div[1]//text()"
        )
        item["work_mode"]       = ""
        item["education_level"] = ""
        item["company_title"]   = xpath(
            "//i[contains(@class,'svicon-users')]"
            "/ancestor::div[contains(@class,'flex flex-col gap-3')]"
            "//a[@title]/div/text()"
        )
        item["company_size"]    = xpath(
            "//i[contains(@class,'svicon-users')]/following-sibling::div/text()"
        )
        item["scraped_at"]      = datetime.now()

        yield item