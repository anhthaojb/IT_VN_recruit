import scrapy
import re
from datetime import datetime
from jobscrapers.items import JobItem


class Timviec365Spider(scrapy.Spider):
    name = "timviec365"
    allowed_domains = ["timviec365.vn"]
    start_urls = ["https://timviec365.vn/viec-lam-it-phan-mem-c13v0"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stopped    = False
        self.page_count = 0

    def _get_mode(self):
        return self.crawler.settings.get("CRAWL_MODE", "daily")

    @staticmethod
    def _is_old(posted_text: str) -> bool:
        """
        Timviec365 format:
          "20/03/2026"       → so sánh với hôm nay
          "Hôm nay"          → False
          "1 ngày trước"     → True
        """
        if not posted_text:
            return False
        text = posted_text.strip().lower()

        if "hôm nay" in text:
            return False
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

        self.page_count += 1

        jobs = response.css(".boxShowListNew div.item_vl")
        if not jobs:
            self.logger.info("[timviec365] Không còn job — dừng")
            return

        for job in jobs:
            job_url = job.css("div.box_left_vl a::attr(href)").get()
            if job_url:
                yield response.follow(
                    job_url,
                    callback=self.parse_job_page,
                )

        # Next page — daily chỉ crawl tối đa 3 trang
        if not self.stopped:
            next_anchors = response.css('.pagi_pre a')
            next_page = None
            for a in next_anchors:
                if a.css('::text').get('').strip() == '>':
                    next_page = a.attrib.get('href')
                    break

            if next_page and (self._get_mode() == "full" or self.page_count < 3):
                yield response.follow(next_page, callback=self.parse)

    # ------------------------------------------------------------------
    # parse_job_page — chi tiết job
    # ------------------------------------------------------------------

    def parse_job_page(self, response):
        def xpath(query):
            return response.xpath(query).get("").strip()

        def xpath_all(query):
            return " ".join(response.xpath(query).getall()).strip()

        # ── Check date TRƯỚC khi build item ──────────────────────────
        job_posted_at = response.css(
    "span.timeUpdate.dataNewReal::text"
).get("").strip()
        if self._get_mode() == "daily" and self._is_old(job_posted_at):
            self.logger.info(
                f"[timviec365][daily] Job cũ ({job_posted_at!r}) — bỏ qua: {response.url}"
            )
            return

        # ── Build item ────────────────────────────────────────────────
        item = JobItem()
        item["website"]          = "timviec365"
        item["job_url"]          = response.url
        item["job_title"]        = response.css(
            ".boxTitleNameNtd h1::text"
        ).get("").strip()
        item["job_posted_at"]    = job_posted_at or None
        item["location"]         = xpath(
            "//p[text()='Địa điểm']/following-sibling::p/text()"
        )
        item["experience"]       = xpath(
            "//p[text()='Kinh nghiệm']/following-sibling::p/text()"
        )
        item["compensation"]     = response.css(
            ".valContentSalary.txtSalaryNew::text"
        ).get("").strip()
        item["job_type"]         = xpath(
            "//p[text()='Hình thức làm việc']/following-sibling::p/text()"
        )
        item["work_mode"]        = None
        item["level"]            = xpath(
            "//p[text()='Chức vụ']/following-sibling::p/text()"
        )
        item["job_category"] = xpath_all(
    "//p[contains(text(),'Lĩnh vực')]/following-sibling::div//a/text()"
)
        item["number_recruit"]   = xpath(
            "//p[text()='Số lượng cần tuyển']/following-sibling::p/text()"
        )
        item["education_level"]  = xpath(
            "//p[text()='Bằng cấp']/following-sibling::p/text()"
        )
        item["job_description"]  = xpath_all(
            "//h2[text()='Mô tả công việc']"
            "/ancestor::div[@class='itemInfoSpecific']"
            "//div[@class='valInfoSpecific']//text()"
        )
        item["job_requirement"]  = " ".join(
            response.css(".boxYeuCauKhac .valInfoSpecific.w100 *::text").getall()
        ).strip()
        item["job_deadline"]     = response.css(".valHanNop::text").get("").strip()
        item["company_title"]    = response.css(
            ".boxTitleNameNtd a::text"
        ).get("").strip()
        item["company_size"]     = None
        item["company_industry"] = xpath_all(
    "//p[contains(text(),'Ngành nghề')]/following-sibling::div//a/text()"
)
        item["scraped_at"]       = datetime.now()

        yield item