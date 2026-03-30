import os
import scrapy
from scrapy_playwright.page import PageMethod
from datetime import datetime
from jobscrapers.items import JobItem


BLOCKED_DOMAINS = {
    "googleads", "doubleclick", "google-analytics",
    "googletagmanager", "facebook", "connect.facebook",
    "pingdom", "linkedin", "px.ads", "px4.ads",
    "nr-data", "bam.nr-data", "cloudflareinsights",
    "ga.jspm.io", "analytics.google",
}

BLOCKED_RESOURCE_TYPES = {"image", "media", "font", "ping"}


def should_abort_request(request):
    if request.resource_type in BLOCKED_RESOURCE_TYPES:
        return True
    return any(domain in request.url for domain in BLOCKED_DOMAINS)


class ItviecSpider(scrapy.Spider):
    name = "itviec"
    allowed_domains = ["itviec.com"]

    itviec_email    = os.environ.get("ITVIEC_EMAIL",    "technicalwriting20241@gmail.com")
    itviec_password = os.environ.get("ITVIEC_PASSWORD", "Testchaycode@24")

    custom_settings = {
        "PLAYWRIGHT_ABORT_REQUEST": should_abort_request,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 30_000,
        "PLAYWRIGHT_CONTEXTS": {
            "itviec_session": {
                "ignore_https_errors": True,
            },
        },
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408],
        "CONCURRENT_REQUESTS": 4,
        "DOWNLOAD_DELAY": 1,
    }

    # ─────────────────────────────────────────
    # BƯỚC 1: Đăng nhập → vào trang danh sách
    # ─────────────────────────────────────────
    def start_requests(self):
        yield scrapy.Request(
            url="https://itviec.com/sign_in",
            meta={
                "playwright": True,
                "playwright_context": "itviec_session",
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "input#user_email"),
                    PageMethod("fill", "input#user_email", self.itviec_email),
                    PageMethod("fill", "input#user_password", self.itviec_password),
                    PageMethod("click", "button[data-disable-with='Signing in...']"),
                    PageMethod("wait_for_url", "https://itviec.com/**", timeout=15_000),
                    PageMethod("goto", "https://itviec.com/it-jobs"),
                    PageMethod("wait_for_load_state", "domcontentloaded"),
                ],
            },
            callback=self.parse,
            errback=self.handle_error,
        )

    # ─────────────────────────────────────────
    # BƯỚC 2: Thu thập link + phân trang
    # (giống Careerlink — chỉ lấy link, không scrape detail ở đây)
    # ─────────────────────────────────────────
    def parse(self, response):
        jobs = response.css("div.job-card")
        self.logger.info(f"[parse] Tìm thấy {len(jobs)} jobs tại {response.url}")

        for job in jobs:
            slug = job.attrib.get("data-search--job-selection-job-slug-value", "")
            if not slug:
                continue

            job_url = f"https://itviec.com/it-jobs/{slug}"

            # Dữ liệu nhanh từ card ngoài danh sách → truyền sang bước 3
            yield scrapy.Request(
                url=job_url,
                meta={
                    "playwright": True,
                    "playwright_context": "itviec_session",
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "domcontentloaded"),
                    ],
                    "list_salary":    job.css("div.salary span.fw-500::text").get("").strip(),
                    "list_work_mode": job.css("div.text-rich-grey.flex-shrink-0::text").get("").strip(),
                    "list_location":  job.css("div.text-rich-grey.text-truncate::text").get("").strip(),
                    "list_skills":    job.css('div[data-controller="responsive-tag-list"] a.itag::text').getall(),
                    "list_expertise": job.css("a.text-decoration-dot-underline::text").get("").strip(),
                },
                callback=self.parse_job_page,
                errback=self.handle_error,
            )

        # Phân trang
        next_page = response.css("div.page.next a::attr(href)").get()
        if next_page:
            self.logger.info(f"[parse] Sang trang kế: {next_page}")
            yield scrapy.Request(
                url=response.urljoin(next_page),
                meta={
                    "playwright": True,
                    "playwright_context": "itviec_session",
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "domcontentloaded"),
                    ],
                },
                callback=self.parse,
                errback=self.handle_error,
            )

    # ─────────────────────────────────────────
    # BƯỚC 3: Scrape chi tiết từng job
    # ─────────────────────────────────────────
    def parse_job_page(self, response):
        job_item = JobItem()

        job_item["website"]       = "itviec"
        job_item["job_url"]       = response.url
        job_item["job_title"]     = response.css("h2.text-it-black:not(.modal-title)::text").get("").strip()
        job_item["company_title"] = response.css("section.company-infos h2 a::text").get("").strip()

        # Từ card ngoài danh sách (truyền qua meta)
        job_item["compensation"] = response.meta.get("list_salary", "")
        job_item["location"]     = response.meta.get("list_location", "")
        job_item["work_mode"]    = response.meta.get("list_work_mode", "")

        job_item["level"] = response.css(
            "div.row small.normal-text:first-child::text"
        ).get("").strip()

        job_item["company_industry"] = response.css(
            "section.company-infos div.d-inline-flex::text"
        ).getall()

        job_item["company_size"] = response.css(
            "section.company-infos div.row small.normal-text:nth-child(3)::text"
        ).get("").strip()

        job_item["job_catergory"]       = response.css("div.itag.bg-light-grey.itag-sm::text").getall()
        job_item["job_description"] = response.css("section.job-description div.paragraph *::text").getall()
        job_item["job_requirement"] = response.css("section.job-experiences div.paragraph *::text").getall()

        job_item["job_posted_at"] = response.css(
            "div.preview-header-item span.small-text::text"
        ).get("").strip()

        job_item["job_deadline"]    = ""
        job_item["education_level"] = ""
        job_item["number_recruit"]  = ""
        job_item["job_type"]        = "Full-time"
        job_item["scraped_at"]      = datetime.now()

        self.logger.info(
            f"[parse_job_page] Scraped: {job_item['job_title']} @ {job_item['company_title']}"
        )
        yield job_item

    # ─────────────────────────────────────────
    # Xử lý lỗi
    # ─────────────────────────────────────────
    def handle_error(self, failure):
        self.logger.error(
            f"[handle_error] Request thất bại: {failure.request.url} "
            f"— {failure.getErrorMessage()}"
        )