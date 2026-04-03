import scrapy
import re
from datetime import datetime
from jobscrapers.items import JobItem


class JobokoSpider(scrapy.Spider):
    name = "joboko"
    allowed_domains = ["joboko.com"]
    start_urls = [
        "https://vn.joboko.com/viec-lam-nganh-it-phan-mem-cong-nghe-thong-tin-iot-dien-tu-vien-thong-xni124"
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stopped = False

    def _get_mode(self):
        return self.crawler.settings.get("CRAWL_MODE", "daily")

    @staticmethod
    def _is_old(posted_text: str) -> bool:
        """
        Joboko dùng em.item-date với data-value dạng "dd/mm/yyyy".
        So sánh với ngày hiện tại.
        """
        if not posted_text:
            return False
        m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", posted_text.strip())
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

        jobs = response.css(".nw-job-list__list div.item")

        # Dừng khi trang không còn job
        if not jobs:
            self.logger.info("[joboko] Không còn job — dừng")
            return

        for job in jobs:
            href = job.css("h2 a::attr(href)").get()
            if not href:
                continue

            # ── Lấy meta từ card ──────────────────────────────────────
            # HTML card có 4 div.col trong div.fz-15.row:
            #   col[0] = compensation  ("Cạnh tranh")
            #   col[1] = location      ("Thừa Thiên Huế")
            #   col[2] = posted_at     ("Ngày làm mới: 04/02/2026")
            #   col[3] = deadline      ("Ngày hết hạn: 04/04/2026")
            cols = job.css("div.fz-15.row div.col-6")

            compensation_raw = cols[0].css("::text").get("").strip() if len(cols) > 0 else ""
            location_raw     = cols[1].css("::text").get("").strip() if len(cols) > 1 else ""
            posted_raw       = cols[2].css("::text").get("").strip() if len(cols) > 2 else ""
            deadline_raw     = cols[3].css("::text").get("").strip() if len(cols) > 3 else ""

            # Trích date từ text "Ngày làm mới: 04/02/2026"
            m_posted   = re.search(r"\d{2}/\d{2}/\d{4}", posted_raw)
            m_deadline = re.search(r"\d{2}/\d{2}/\d{4}", deadline_raw)
            posted_at  = m_posted.group(0)   if m_posted   else ""
            deadline   = m_deadline.group(0) if m_deadline else ""

            # Daily mode: dùng posted_at để check thay vì deadline
            check_date = posted_at or deadline
            if self._get_mode() == "daily" and self._is_old(check_date):
                self.logger.info(
                    f"[joboko][daily] Gặp job cũ ({check_date!r}) — dừng"
                )
                self.stopped = True
                return

            job_url = (
                href if href.startswith("http")
                else "https://vn.joboko.com" + href
            )
            yield response.follow(
                job_url,
                callback=self.parse_job_page,
                cb_kwargs={
                    "card_compensation": compensation_raw,
                    "card_location"    : location_raw,
                    "card_posted_at"   : posted_at,
                    "card_deadline"    : deadline,
                },
            )

        # Next page — fix bug: kiểm tra None trước khi ghép URL
        if not self.stopped:
            next_href = response.css(".nw-job-list__more a::attr(href)").get()
            if next_href:
                next_url = (
                    next_href if next_href.startswith("http")
                    else "https://vn.joboko.com" + next_href
                )
                yield scrapy.Request(url=next_url, callback=self.parse)

    # ------------------------------------------------------------------
    # parse_job_page — chi tiết job
    # ------------------------------------------------------------------

    def parse_job_page(self, response,
                       card_compensation="", card_location="",
                       card_posted_at="", card_deadline=""):
        def xpath(query):
            return response.xpath(query).get("").strip()

        def xpath_all(query):
            return " ".join(response.xpath(query).getall()).strip()

        # Deadline từ detail page — trích date, bỏ "(Còn X ngày)"
        deadline_detail = response.css("em.item-date::attr(data-value)").get("").strip()
        m = re.search(r"\d{2}/\d{2}/\d{4}", deadline_detail)
        deadline_detail = m.group(0) if m else deadline_detail

        item = JobItem()
        item["website"]         = "joboko"
        item["job_url"]         = response.url
        item["job_title"]       = response.css(
            "h1.nw-company-hero__title a::text"
        ).get("").strip()
        item["location"]        = (
            response.css(".nw-company-hero__address a::text").get("").strip()
            or card_location
        )
        item["experience"]      = xpath(
            "//div[contains(., 'Kinh nghiệm')]/span/text()"
        )
        item["compensation"]    = (
            xpath("//div[contains(., 'Thu nhập')]/span/text()")
            or card_compensation
        )
        item["job_type"]        = xpath(
            "//div[contains(., 'Loại hình')]/span/text()"
        )
        item["work_mode"]       = ""
        item["level"]           = xpath(
            "//div[contains(., 'Chức vụ')]/span/text()"
        )
        item["company_title"]   = response.css(
            ".nw-company-hero__info h2 a::text"
        ).get("").strip()
        item["company_size"]    = xpath_all(
            "//span[contains(text(),'Quy mô công ty')]"
            "/ancestor::div[1]/following-sibling::div[1]//text()"
        )
        item["company_industry"]= ""
        item["job_category"]    = ""
        item["number_recruit"]  = ""
        item["education_level"] = ""
        item["job_description"] = xpath_all(
            "//h3[contains(text(),'Mô tả công việc')]/following-sibling::div[1]//text()"
        )
        item["job_requirement"] = xpath_all(
            "//h3[contains(text(),'Yêu cầu')]/following-sibling::div[1]//text()"
        )
        item["job_posted_at"]   = card_posted_at   # từ card — chính xác hơn
        item["job_deadline"]    = deadline_detail or card_deadline
        item["scraped_at"]      = datetime.now()

        yield item