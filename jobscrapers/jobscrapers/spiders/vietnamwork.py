import scrapy
import json
import re
import html as html_lib
from datetime import datetime
from jobscrapers.items import JobItem


class VietnamworkSpider(scrapy.Spider):
    name = "vietnamwork"
    allowed_domains = ["ms.vietnamworks.com"]

    API_URL = "https://ms.vietnamworks.com/job-search/v1.0/search"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stopped = False

    def _get_mode(self):
        return self.crawler.settings.get("CRAWL_MODE", "daily")

    @staticmethod
    def _is_old(posted_text: str) -> bool:
        """
        Vietnamworks API trả về ISO format: "2026-03-28T10:30:00"
        So sánh với hôm nay.
        """
        if not posted_text:
            return False
        # ISO datetime: "2026-03-28T10:30:00" hoặc "2026-03-28"
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", posted_text.strip())
        if m:
            try:
                posted_date = datetime(
                    int(m.group(1)), int(m.group(2)), int(m.group(3))
                ).date()
                return (datetime.now().date() - posted_date).days > 1
            except ValueError:
                pass
        return False

    # ------------------------------------------------------------------
    # start
    # ------------------------------------------------------------------

    async def start(self):
        yield self._build_request(page=0)

    def _build_request(self, page: int):
        payload = {
            "jobFunction": 5,
            "page"       : page,
            "hitsPerPage": 50,
        }
        return scrapy.Request(
            url=self.API_URL,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept"       : "application/json",
                "Referer"      : "https://www.vietnamworks.com/",
            },
            body=json.dumps(payload),
            callback=self.parse,
            cb_kwargs={"page": page},
        )

    # ------------------------------------------------------------------
    # parse — nhận JSON từ API
    # ------------------------------------------------------------------

    def parse(self, response, page=0):
        if self.stopped:
            return

        data     = response.json()
        meta     = data.get("meta", {})
        jobs     = data.get("data", [])
        nb_pages = meta.get("nbPages", 0)

        self.logger.info(
            f"[vietnamwork] Page {page}/{nb_pages} — {len(jobs)} jobs"
            f" | mode={self._get_mode()}"
        )

        for job in jobs:
            # Daily mode: kiểm tra ngày đăng từ JSON trước khi map
            approved_on = job.get("approvedOn", "")
            if self._get_mode() == "daily" and self._is_old(approved_on):
                self.logger.info(
                    f"[vietnamwork][daily] Gặp job cũ ({approved_on!r}) — dừng"
                )
                self.stopped = True
                return   # bỏ qua toàn bộ job còn lại trên trang này

            yield self._map_job(job)

        # Sang page tiếp nếu còn và chưa dừng
        if not self.stopped and page + 1 < nb_pages:
            yield self._build_request(page=page + 1)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def strip_html(text: str) -> str:
        if not text:
            return ""
        text = html_lib.unescape(text)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _map_job(self, job: dict) -> JobItem:
        # Lương
        salary_min = job.get("salaryMin", 0)
        salary_max = job.get("salaryMax", 0)
        currency   = job.get("salaryCurrency", "")
        if salary_min or salary_max:
            compensation = f"{salary_min:,} - {salary_max:,} {currency}".strip()
        else:
            compensation = job.get("prettySalary", "Thương lượng")

        # Địa điểm — lấy city đầu tiên
        locations = job.get("workingLocations") or []
        location  = locations[0].get("cityNameVI", "") if locations else ""

        # Ngành
        industries = job.get("industriesV3") or []
        industry   = industries[0].get("industryV3NameVI", "") if industries else ""

        item = JobItem()
        item["website"]         = "vietnamwork"
        item["job_url"]         = job.get("jobUrl", "")
        item["job_title"]       = job.get("jobTitle", "")
        item["location"]        = location
        item["experience"]      = str(job.get("yearsOfExperience", ""))
        item["compensation"]    = compensation
        item["job_type"]        = str(job.get("typeWorkingId", ""))
        item["work_mode"]       = ""
        item["level"]           = job.get("jobLevelVI", "")
        item["company_title"]   = job.get("companyName", "")
        item["company_size"]    = str(job.get("companySize", ""))
        item["company_industry"]= industry
        item["job_category"]    = ""
        item["number_recruit"]  = ""
        item["education_level"] = str(job.get("highestDegreeId", ""))
        item["job_description"] = self.strip_html(job.get("jobDescription", ""))
        item["job_requirement"] = self.strip_html(job.get("jobRequirement", ""))
        item["job_posted_at"]   = job.get("approvedOn", "")
        item["job_deadline"]    = job.get("expiredOn", "")
        item["scraped_at"]      = datetime.now()

        return item