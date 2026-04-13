import scrapy
import re
from datetime import datetime
from jobscrapers.items import JobItem


class CareervietSpider(scrapy.Spider):
    name = "careerviet"
    allowed_domains = ["careerviet.vn"]
    start_urls = ["https://careerviet.vn/viec-lam/cntt-phan-mem-c1-vi.html"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stopped = False

    def _get_mode(self):
        return self.crawler.settings.get("CRAWL_MODE", "daily")

    @staticmethod
    def _is_old(posted_text: str) -> bool:
        """
        Careerviet format: "05-04-2026" hoặc "05/04/2026"
        """
        if not posted_text:
            return False
        posted_text = posted_text.strip()
        # Match cả dd/mm/yyyy lẫn dd-mm-yyyy
        m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", posted_text)
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
    # parse — danh sách job theo trang
    # ------------------------------------------------------------------

    def parse(self, response):
        if self.stopped:
            return

        jobs = response.css(".jobs-side-list .job-item")

        if not jobs:
            self.logger.info("[careerviet] Không còn job — dừng")
            return

        for job in jobs:
            job_url    = job.css(".title h2 a::attr(href)").get()
            time_texts = job.css('.time li time::text').getall()
            posted_raw = (
                time_texts[1].strip() if len(time_texts) > 1
                else time_texts[0].strip() if time_texts
                else ""
            )

            if self._get_mode() == "daily" and self._is_old(posted_raw):
                self.logger.info(
                    f"[careerviet][daily] Gặp job cũ ({posted_raw!r}) — dừng"
                )
                self.stopped = True
                return

            if job_url:
                yield response.follow(
                    job_url,
                    callback=self.parse_job_page,
                    cb_kwargs={"job_posted_at": posted_raw},
                )

        # Next page — tăng số trang từ URL hiện tại
        if not self.stopped:
            current_url = response.url
            match = re.search(r"trang-(\d+)", current_url)
            page_num = int(match.group(1)) if match else 1
            next_url = (
                f"https://careerviet.vn/viec-lam/"
                f"cntt-phan-mem-c1-trang-{page_num + 1}-vi.html"
            )
            yield scrapy.Request(next_url, callback=self.parse)

    # ------------------------------------------------------------------
    # parse_job_page — chi tiết job
    # ------------------------------------------------------------------

    def parse_job_page(self, response, job_posted_at=""):
        def xpath(query):
            return response.xpath(query).get("").strip()

        def xpath_all(query):
            return " ".join(response.xpath(query).getall()).strip()

        edu_texts = response.xpath('//li[contains(.,"Bằng cấp")]/text()').getall()

        item = JobItem()
        item["website"]          = "careerviet"
        item["job_url"]          = response.url
        item["job_title"]        = response.css(".job-desc h1::text").get("").strip()
        item["location"]         = response.css(".detail-box .map p a::text").get("").strip()
        item["experience"]       = xpath(
            '//li[.//strong[contains(.,"Kinh nghiệm")]]/p/text()'
        )
        item["compensation"]     = xpath(
            '//li[.//strong[contains(.,"Lương")]]/p/text()'
        )
        item["job_type"]         = xpath(
            '//li[.//strong[contains(.,"Hình thức")]]/p/text()'
        )
        item["work_mode"]        = None
        item["level"]            = xpath(
            '//li[.//strong[contains(.,"Cấp bậc")]]/p/text()'
        )
        item["company_title"]    = None  # điền ở parse_company_info
        item["company_size"]     = None  # điền ở parse_company_info
        item["company_industry"] = xpath(
            '//li[.//strong[contains(.,"Ngành nghề")]]/p//text()'
        )
        item["job_category"]     = None
        item["number_recruit"]   = None
        item["education_level"]  = edu_texts[-1].strip() if edu_texts else None
        item["job_description"]  = xpath_all(
            '//div[h2[contains(text(),"Mô tả Công việc")]]//div//text()'
        )
        item["job_requirement"]  = xpath_all(
            '//div[h2[contains(text(),"Yêu Cầu Công Việc")]]//div//text()'
        )
        item["job_posted_at"]    = job_posted_at or xpath(
            '//li[.//strong[contains(.,"Ngày cập nhật")]]/p/text()'
        )
        item["job_deadline"]     = xpath(
            '//li[.//strong[contains(.,"Hết hạn nộp")]]/p/text()'
        )
        item["scraped_at"]       = datetime.now()

        company_url = response.css(".job-desc a::attr(href)").get()
        if company_url:
            yield response.follow(
                company_url,
                callback=self.parse_company_info,
                meta={"job_item": item},
            )
        else:
            yield item

    # ------------------------------------------------------------------
    # parse_company_info — thông tin công ty
    # ------------------------------------------------------------------

    def parse_company_info(self, response):
        item = response.meta["job_item"]

        item["company_title"] = response.css(
            ".company-info h1::text"
        ).get("").strip()

        size_texts = response.xpath(
            '//li[contains(.,"Quy mô công ty")]'
            '/descendant-or-self::text()[normalize-space()]'
        ).getall()
        item["company_size"] = " ".join(
            t.strip() for t in size_texts
            if t.strip() and "quy mô" not in t.lower()
        )

        yield item