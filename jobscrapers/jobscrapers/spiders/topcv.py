import scrapy
import re
from datetime import datetime
from jobscrapers.items import JobItem


class TopcvSpider(scrapy.Spider):
    name = "topcv"
    allowed_domains = ["topcv.vn"]

    BASE_URL    = "https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257"
    PAGE_PARAMS = "?sort=newp&page={page}&category_family=r257"

    # ------------------------------------------------------------------
    # Khởi tạo — đọc mode từ settings
    # ------------------------------------------------------------------

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stopped  = False   # flag báo hiệu đã gặp job cũ → dừng
        self.max_page = 1

    def _get_mode(self):
        """Đọc CRAWL_MODE từ settings. Mặc định: 'daily'."""
        return getattr(self, "crawler", None) and \
               self.crawler.settings.get("CRAWL_MODE", "daily") or "daily"

    # ------------------------------------------------------------------
    # Helpers kiểm tra job cũ
    # ------------------------------------------------------------------

    @staticmethod
    def _is_old(posted_text: str) -> bool:
        """
        Trả về True nếu job cũ hơn 1 ngày — dùng để dừng daily crawl.

        TopCV dùng format:
          "Cập nhật 3 giờ trước"   → còn mới   → False
          "Cập nhật 2 ngày trước"  → cũ        → True
          "Cập nhật 1 tuần trước"  → cũ        → True
        """
        if not posted_text:
            return False
        # Nếu có "ngày", "tuần", "tháng", "năm" trước → cũ
        return bool(re.search(
            r"\d+\s+(?:ngày|tuần|tháng|năm)\s+trước",
            posted_text,
            re.IGNORECASE,
        ))

    # ------------------------------------------------------------------
    # start — trang đầu tiên
    # ------------------------------------------------------------------

    async def start(self):
        yield scrapy.Request(
            url=self.BASE_URL + self.PAGE_PARAMS.format(page=1),
            callback=self.parse,
            cb_kwargs={"page": 1},
        )

    # ------------------------------------------------------------------
    # parse — danh sách job theo trang
    # ------------------------------------------------------------------

    def parse(self, response, page=1):
        # Dừng ngay nếu flag đã bật (gặp job cũ ở trang trước)
        if self.stopped:
            return

        # Lần đầu: đọc tổng số trang
        if page == 1:
            pagination_text = response.css(
                "#job-listing-paginate-text::text"
            ).get("")
            match = re.search(r"/\s*(\d+)\s*trang", pagination_text)
            self.max_page = int(match.group(1)) if match else 1
            self.logger.info(
                f"[topcv] Mode={self._get_mode()} | Tổng trang: {self.max_page}"
            )

        for job in response.css("div.job-item-search-result"):
            job_url    = job.css("h3.title a::attr(href)").get()
            posted_raw = job.css("label.label-update::text").getall()
            posted_text = posted_raw[-1].strip() if posted_raw else ""

            # ── Daily mode: dừng khi gặp job cũ hơn 1 ngày ────────────
            if self._get_mode() == "daily" and self._is_old(posted_text):
                self.logger.info(
                    f"[topcv][daily] Gặp job cũ ({posted_text!r}) "
                    f"— dừng crawl trang {page}"
                )
                self.stopped = True
                return   # không yield thêm request nào nữa

            if job_url:
                yield response.follow(
                    job_url,
                    callback=self.parse_job_page,
                    cb_kwargs={"job_posted_at": posted_text},
                )

        # ── Sang trang tiếp ────────────────────────────────────────────
        next_page = page + 1
        if next_page <= self.max_page and not self.stopped:
            yield scrapy.Request(
                url=self.BASE_URL + self.PAGE_PARAMS.format(page=next_page),
                callback=self.parse,
                cb_kwargs={"page": next_page},
            )

    # ------------------------------------------------------------------
    # parse_job_page — chi tiết từng job
    # ------------------------------------------------------------------

    def parse_job_page(self, response, job_posted_at=""):
        def css(selector):
            return response.css(selector).get("").strip()

        def xpath(query):
            return response.xpath(query).get("").strip()

        def xpath_all(query):
            return " ".join(response.xpath(query).getall()).strip()

        item = JobItem()
        item["website"]          = "topcv"
        item["job_url"]          = response.url
        item["job_title"]        = css("h1.job-detail__info--title::text")
        item["location"]         = xpath_all(
            "//div[contains(text(),'Địa điểm') and contains(@class,'job-detail__info--section-content-title')]"
            "/following-sibling::div//text()"
        )
        item["experience"]       = xpath(
            "//div[div[contains(text(),'Kinh nghiệm')]]/div[contains(@class,'value')]//text()"
        )
        item["compensation"]     = xpath(
            "//div[div[contains(text(),'Mức lương')]]/div[contains(@class,'value')]//text()"
        )
        item["job_type"]         = xpath(
            "//div[div[contains(text(),'Hình thức làm việc')]]/div[contains(@class,'value')]//text()"
        )
        item["work_mode"]        = xpath_all(
            "//h3[contains(text(),'Thời gian làm việc')]/following-sibling::div//text()"
        )
        item["level"]            = xpath(
            "//div[div[contains(text(),'Cấp bậc')]]/div[contains(@class,'value')]//text()"
        )
        item["company_title"]    = css("a.name::text")
        item["company_size"]     = xpath(
            "//div[contains(@class,'company-scale')]//div[@class='company-value']//text()"
        )
        item["company_industry"] = xpath(
            "//div[@class='company-title' and contains(normalize-space(),'Lĩnh vực:')]"
            "/following-sibling::div[@class='company-value']//text()"
        )
        item["job_category"]     = ""
        item["number_recruit"]   = xpath(
            "//div[div[contains(text(),'Số lượng tuyển')]]/div[contains(@class,'value')]//text()"
        )
        item["education_level"]  = xpath(
            "//div[div[contains(text(),'Học vấn')]]/div[contains(@class,'value')]//text()"
        )
        item["job_description"]  = xpath_all(
            "//h3[contains(text(),'Mô tả công việc')]"
            "/following-sibling::div[@class='job-description__item--content']//*//text()"
        )
        item["job_requirement"]  = xpath_all(
            "//h3[contains(text(),'Yêu cầu ứng viên')]"
            "/following-sibling::div[@class='job-description__item--content']//*//text()"
        )
        item["job_posted_at"]    = job_posted_at
        item["job_deadline"]     = css(
            "div.job-detail__info--deadline::text"
        ).replace("Hạn nộp hồ sơ: ", "")
        item["scraped_at"]       = datetime.now()

        yield item