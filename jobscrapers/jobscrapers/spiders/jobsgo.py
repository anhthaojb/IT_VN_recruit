import scrapy
import re
from scrapy_playwright.page import PageMethod
from datetime import datetime
from jobscrapers.items import JobItem


class JobsgoSpider(scrapy.Spider):
    name = "jobsgo"
    allowed_domains = ["jobsgo.vn"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stopped = False

    def _get_mode(self):
        return self.crawler.settings.get("CRAWL_MODE", "daily")

    @staticmethod
    def _is_old(posted_text: str) -> bool:
        """
        Jobsgo format: "20/03/2026" hoặc "Hôm nay", "Hôm qua"
        """
        if not posted_text:
            return False
        text = posted_text.strip().lower()

        if "hôm nay" in text or "today" in text:
            return False
        if "hôm qua" in text or "yesterday" in text:
            return True

        # dd/mm/yyyy
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
    # start
    # ------------------------------------------------------------------

    async def start(self):
        yield scrapy.Request(
            url="https://jobsgo.vn/viec-lam-cong-nghe-thong-tin.html",
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "div.job-card", timeout=15000),
                ],
                "playwright_context_kwargs": {
                    "java_script_enabled": True,
                },
            },
            callback=self.parse,
        )

    # ------------------------------------------------------------------
    # parse — danh sách job
    # ------------------------------------------------------------------

    def parse(self, response):
        if self.stopped:
            return

        jobs = response.css("div.job-card")
        self.logger.info(f"[jobsgo] {len(jobs)} jobs | {response.url}")

        if not jobs:
            self.logger.info("[jobsgo] Không còn job — dừng")
            return

        for job in jobs:
            job_url = job.css("a.text-decoration-none::attr(href)").get()
            if not job_url:
                continue

            yield scrapy.Request(
                url=job_url,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", "h1", timeout=15000),
                    ],
                    # Meta từ card — dùng làm fallback nếu detail chưa load kịp
                    "list_title"      : job.css("h3.job-title::text").get("").strip(),
                    "list_company"    : job.css("div.company-title::text").get("").strip(),
                    "list_salary"     : job.css("div.text-primary span:first-child::text").get("").strip(),
                    "list_location"   : job.css("div.text-primary span:last-child::text").get("").strip(),
                    "list_job_type"   : job.css('span[title="Loại hình"]::text').get("").strip(),
                    "list_experience" : job.css('span[title="Yêu cầu kinh nghiệm"]::text').get("").strip(),
                },
                callback=self.parse_job_page,
            )

        # Next page
        if not self.stopped:
            next_page = response.css("ul.pagination li.next a::attr(href)").get()
            if next_page:
                yield scrapy.Request(
                    url=response.urljoin(next_page),
                    meta={
                        "playwright": True,
                        "playwright_page_methods": [
                            PageMethod("wait_for_selector", "div.job-card", timeout=15000),
                        ],
                    },
                    callback=self.parse,
                )

    # ------------------------------------------------------------------
    # parse_job_page — chi tiết job
    # ------------------------------------------------------------------

    def parse_job_page(self, response):
        job_posted_at = response.xpath(
            "//span[contains(.,'Ngày đăng tuyển')]/following-sibling::strong/text()"
        ).get("").strip()

        # Daily mode: drop item nếu job cũ
        # (jobsgo không có posted_at trên card — phải check tại đây)
        if self._get_mode() == "daily" and self._is_old(job_posted_at):
            self.logger.info(
                f"[jobsgo][daily] Job cũ ({job_posted_at!r}) — bỏ qua: {response.url}"
            )
            # Không dừng toàn spider vì jobsgo sort không hoàn toàn theo ngày
            return

        item = JobItem()
        item["website"]        = "jobsgo"
        item["job_url"]        = response.url
        item["job_title"]      = response.css("h1.job-title::text").get("").strip()
        item["company_title"]  = response.css("h6.fw-semibold::text").get("").strip()
        item["compensation"]   = response.xpath(
            "//span[contains(.,'Mức lương')]/strong/text()"
        ).get("").strip()
        item["location"]       = response.xpath(
            "string(//span[contains(.,'Địa điểm')]/strong)"
        ).get("").strip()
        item["experience"]     = response.xpath(
            "//span[contains(.,'Kinh nghiệm')]/strong/text()"
        ).get("").strip()
        item["education_level"]= response.xpath(
            "//span[contains(.,'Bằng cấp')]/strong/text()"
        ).get("").strip()
        item["job_deadline"]   = response.xpath(
            "//span[contains(.,'Hạn nộp hồ sơ')]/following-sibling::strong/text()"
        ).get("").strip()
        item["job_description"]= " ".join(response.xpath(
            "//h3[contains(text(),'Mô tả công việc')]/following-sibling::div[1]//text()"
        ).getall()).strip()
        item["job_requirement"]= " ".join(response.xpath(
            "//h3[contains(text(),'Yêu cầu công việc')]/following-sibling::div[1]//text()"
        ).getall()).strip()
        item["job_type"]       = response.xpath(
            "//span[contains(.,'Loại hình')]/following-sibling::strong/text()"
        ).get("").strip()
        item["level"]          = response.xpath(
            "//span[contains(.,'Cấp bậc')]/following-sibling::strong/text()"
        ).get("").strip()
        item["job_posted_at"]  = job_posted_at
        item["job_category"]   = " ".join(response.xpath(
            "//div[contains(@class,'text-muted') and contains(text(),'Ngành nghề:')]"
            "/following-sibling::strong//a/text()"
        ).getall()).strip()
        item["work_mode"]      = ""
        item["number_recruit"] = ""
        item["company_size"]   = ""   # điền ở parse_company_page
        item["company_industry"]= ""  # điền ở parse_company_page
        item["scraped_at"]     = datetime.now()

        company_url = response.css("div.card-company a::attr(href)").get()
        if company_url:
            yield scrapy.Request(
                url=response.urljoin(company_url),
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod(
                            "wait_for_selector", "div.company-info", timeout=15000
                        ),
                    ],
                    "job_item": item,
                },
                callback=self.parse_company_page,
            )
        else:
            yield item

    # ------------------------------------------------------------------
    # parse_company_page — thông tin công ty
    # ------------------------------------------------------------------

    def parse_company_page(self, response):
        item = response.meta["job_item"]
        item["company_size"]     = response.css(
            "li.d-flex i.pb-heroicons-users ~ span::text"
        ).get("").strip()
        item["company_industry"] = response.css(
            "div.company-category span.company-category-list span::text"
        ).get("").strip()
        yield item