import scrapy
import json
import re
from datetime import datetime
from jobscrapers.items import JobItem
import html as html_lib


class VietnamworkSpider(scrapy.Spider):
    name = "vietnamwork"
    allowed_domains = ["ms.vietnamworks.com"]

    API_URL = "https://ms.vietnamworks.com/job-search/v1.0/search"

    async def start(self):
        yield self._build_request(page=0)

    def _build_request(self, page: int):
        payload = {
            "jobFunction": 5,
            "page": page,
            "hitsPerPage": 50,   # max 50/page → ít request hơn
        }
        return scrapy.Request(
            url=self.API_URL,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Referer": "https://www.vietnamworks.com/",
            },
            body=json.dumps(payload),
            callback=self.parse,
            cb_kwargs={"page": page},
        )

    def parse(self, response, page=0):
      
        data = response.json()
        meta = data.get("meta", {})
        jobs = data.get("data", [])

        nb_pages = meta.get("nbPages", 0)
        self.logger.info(f"Page {page}/{nb_pages} — {len(jobs)} jobs")
        for job in jobs:
            yield self.map_job(job)



        # Sang page tiếp nếu còn
        if page + 1 < nb_pages:
            yield self._build_request(page=page + 1)

    @staticmethod
    def strip_html(text: str) -> str:
        if not text:
            return ""
        # 1. Decode HTML entities trước: &amp; → &, &#43; → +, v.v.
        text = html_lib.unescape(text)
        # 2. Xóa HTML tags
        text = re.sub(r"<[^>]+>", " ", text)
        # 3. Gộp nhiều space/newline thành 1
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def map_job(self, job: dict) -> JobItem:
        item = JobItem()

        # Lương
        salary_min = job.get("salaryMin", 0)
        salary_max = job.get("salaryMax", 0)
        currency   = job.get("salaryCurrency", "")
        if salary_min or salary_max:
            compensation = f"{salary_min:,} - {salary_max:,} {currency}"
        else:
            compensation = job.get("prettySalary", "Thương lượng")

        # Địa điểm
        locations = job.get("workingLocations") or []
        location  = locations[0].get("cityNameVI", "") if locations else ""

        # Ngành
        industries = job.get("industriesV3") or []
        industry   = industries[0].get("industryV3NameVI", "") if industries else ""

        item['website']          = 'vietnamwork'
        item['job_url']          = job.get("jobUrl", "")
        item['job_title']        = job.get("jobTitle", "")
        item['location']         = location
        item['experience']       = job.get("yearsOfExperience", "")
        item['compensation']     = compensation
        item['job_type']         = job.get("typeWorkingId", "")
        item['work_mode']        = ""
        item['level']            = job.get("jobLevelVI", "")
        item['company_title']    = job.get("companyName", "")
        item['company_size']     = job.get("companySize", "")
        item['company_industry'] = industry
        item['job_category']    =''
        item['number_recruit']   = ""   # isShowNumberOfRecruits luôn = 0
        item['education_level']  = job.get("highestDegreeId", "")   # highestDegreeId có nhưng không có label
        item['job_description']  = self.strip_html(job.get("jobDescription", ""))
        item['job_requirement']  = self.strip_html(job.get("jobRequirement", ""))
        item['job_posted_at']    = job.get("approvedOn", "")
        item['job_deadline']     = job.get("expiredOn", "")
        item['scraped_at']       = datetime.now()

        return item