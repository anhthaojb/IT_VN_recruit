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
        Joboko dùng "Ngày làm mới: dd/mm/yyyy" làm proxy cho ngày đăng.
        Ngưỡng > 2 ngày để buffer cho job đăng cuối ngày hôm qua.
        """
        if not posted_text:
            return False  # Không có date → không dừng, cứ crawl
        m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", posted_text.strip())
        if m:
            try:
                posted_date = datetime(
                    int(m.group(3)), int(m.group(2)), int(m.group(1))
                ).date()
                return (datetime.now().date() - posted_date).days > 2
            except ValueError:
                pass
        return False

    def parse(self, response):
        if self.stopped:
            return

        jobs = response.css(".nw-job-list__list div.item")

        if not jobs:
            self.logger.info("[joboko] Không còn job — dừng")
            return

        for job in jobs:
            href = job.css("h2 a::attr(href)").get()
            if not href:
                continue

            cols = job.css("div.fz-15.row div.col-6")

            compensation_raw = cols[0].css("::text").get("").strip() if len(cols) > 0 else ""
            location_raw     = cols[1].css("::text").get("").strip() if len(cols) > 1 else ""
            posted_raw       = cols[2].css("::text").get("").strip() if len(cols) > 2 else ""
            deadline_raw     = cols[3].css("::text").get("").strip() if len(cols) > 3 else ""

            m_posted   = re.search(r"\d{2}/\d{2}/\d{4}", posted_raw)
            m_deadline = re.search(r"\d{2}/\d{2}/\d{4}", deadline_raw)
            posted_at  = m_posted.group(0)   if m_posted   else ""
            deadline   = m_deadline.group(0) if m_deadline else ""

            # Chỉ dùng "Ngày làm mới" để check — không fallback sang deadline
            if self._get_mode() == "daily" and self._is_old(posted_at):
                self.logger.info(
                    f"[joboko][daily] Ngày làm mới cũ ({posted_at!r}) — dừng"
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

        if not self.stopped:
            next_href = response.css(".nw-job-list__more a::attr(href)").get()
            if next_href:
                next_url = (
                    next_href if next_href.startswith("http")
                    else "https://vn.joboko.com" + next_href
                )
                yield scrapy.Request(url=next_url, callback=self.parse)

    def parse_job_page(self, response,
                       card_compensation="", card_location="",
                       card_posted_at="", card_deadline=""):
        def xpath(query):
            return response.xpath(query).get("").strip()

        def xpath_all(query):
            return " ".join(response.xpath(query).getall()).strip()

        job_title = response.css(
            "h1.nw-company-hero__title a::text"
        ).get("").strip()

        locations = response.css(".nw-company-hero__address a::text").getall()
        location = ", ".join(l.strip() for l in locations if l.strip()) or card_location

        deadline_detail = response.css("em.item-date::attr(data-value)").get("").strip()

        compensation = response.xpath(
            "//div[contains(@class,'item-content')]"
            "[contains(.,'Thu nhập')]/span[@class='fw-bold']/text()"
        ).get("").strip() or card_compensation

        experience = response.xpath(
            "//div[contains(@class,'item-content')]"
            "[contains(.,'Kinh nghiệm')]/span[@class='fw-bold']/text()"
        ).get("").strip()

        job_type = response.xpath(
            "//div[contains(@class,'item-content')]"
            "[contains(.,'Loại hình')]/span[@class='fw-bold']/text()"
        ).get("").strip()

        level = response.xpath(
            "//div[contains(@class,'item-content')]"
            "[contains(.,'Chức vụ')]/span[@class='fw-bold']/text()"
        ).get("").strip()

        company_title = response.xpath(
            "//a[contains(@class,'nw-company-hero__text')]/text()"
        ).get("").strip()

        company_size = response.xpath(
            "//span[contains(.,'Quy mô công ty')]"
            "/ancestor::div[contains(@class,'nw-job-detail__heading')]"
            "/following-sibling::div[contains(@class,'nw-job-detail__text')][1]/text()"
        ).get("").strip()

        job_description = xpath_all("//div[@class='text-left job-desc']//text()")
        job_requirement = xpath_all("//div[@class='text-left job-requirement']//text()")

        item = JobItem()
        item["website"]          = "joboko"
        item["job_url"]          = response.url
        item["job_title"]        = job_title or None
        item["location"]         = location or None
        item["experience"]       = experience or None
        item["compensation"]     = compensation or None
        item["job_type"]         = job_type or None
        item["work_mode"]        = None
        item["level"]            = level or None
        item["company_title"]    = company_title or None
        item["company_size"]     = company_size or None
        item["company_industry"] = None
        item["job_category"]     = None
        item["number_recruit"]   = None
        item["education_level"]  = None
        item["job_description"]  = job_description or None
        item["job_requirement"]  = job_requirement or None
        item["job_posted_at"]    = card_posted_at or None
        item["job_deadline"]     = deadline_detail or card_deadline
        item["scraped_at"]       = datetime.now()

        yield item