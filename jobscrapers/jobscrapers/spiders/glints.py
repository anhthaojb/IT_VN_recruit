import scrapy
from scrapy_playwright.page import PageMethod
from jobscrapers.items import JobItem
from datetime import datetime


class GlintsSpider(scrapy.Spider):
    name = "glints"
    allowed_domains = ["glints.com"]
    START_URL = "https://glints.com/vn/opportunities/jobs/explore?country=VN&locationName=All+Cities%2FProvinces&industryId=6"

    async def start(self):
        yield scrapy.Request(
            url=self.START_URL,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "div.CompactOpportunityCardsc__CompactJobCard-sc-dkg8my-4"),
                ],
                "page": 1,
            },
            callback=self.parse,
        )

    async def parse(self, response):
        page = response.meta.get("page", 1)

        # ✅ Selector hẹp hơn, chỉ lấy job card trong listing
        for job in response.css("div.CompactOpportunityCardsc__CompactJobCard-sc-dkg8my-4"):
            job_url = job.css("h2 a::attr(href)").get()
            job_posted_at = job.css("p.CompactOpportunityCardsc__UpdatedAtMessage-sc-dkg8my-26::text").get("").strip()
            if job_url:
                full_url = "https://glints.com" + job_url if job_url.startswith("/") else job_url
                yield scrapy.Request(
                    url=full_url,
                    meta={
                        "playwright": True,
                        "playwright_page_methods": [
                            PageMethod("wait_for_selector", "h1[aria-label='Job Title']"),
                        ],
                        "job_posted_at": job_posted_at,
                    },
                    callback=self.parse_job_page,
                )

        next_page = page + 1
        yield scrapy.Request(
            url=self.START_URL + f"&page={next_page}",
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "div.CompactOpportunityCardsc__CompactJobCard-sc-dkg8my-4"),
                ],
                "page": next_page,
            },
            callback=self.parse,
        )

    def parse_job_page(self, response):
        def css(selector):
            return response.css(selector).get("").strip()

        def css_all(selector):
            return " ".join(response.css(selector).getall()).strip()

        job_item = JobItem()
        job_item["website"]          = "glints"
        job_item["job_url"]          = response.url
        job_item["job_title"]        = css("h1[aria-label='Job Title']::text")
        job_item["company_title"]    = css("div.TopFoldExperimentsc__JobOverViewCompanyName-sc-b8dbys-5 a::text")
        job_item["location"]         = css("div.AboutCompanySectionsc__AddressWrapper-sc-c7oevo-14 p::text")
        job_item["compensation"]     = css("span.TopFoldExperimentsc__BasicSalary-sc-b8dbys-13::text")
        job_item["job_type"]         = css("div.TopFoldExperimentsc__JobOverViewInfo-sc-b8dbys-9:nth-child(3) span::text")
        job_item["experience"]       = css("div.TopFoldExperimentsc__JobOverViewInfo-sc-b8dbys-9:last-of-type span::text")
        job_item["education_level"]  = css_all("div.JobRequirementssc__TagsWrapper-sc-15g5po6-2 div.TagStyle__TagContentWrapper-sc-r1wv7a-1::text")
        job_item["level"]            = ""
        job_item["work_mode"]        = ""
        job_item["number_recruit"]   = ""
        job_item["company_size"]     = css("div.AboutCompanySectionsc__CompanyIndustryAndSize-sc-c7oevo-7 span:last-child::text")
        job_item["company_industry"] = css("div.AboutCompanySectionsc__CompanyIndustryAndSize-sc-c7oevo-7 span:first-child::text")
        job_item["job_description"]  = " ".join(
            response.css("div[aria-label='Job Description'] p::text, div[aria-label='Job Description'] li::text").getall()
        ).strip()
        job_item["job_requirement"]  = css_all("div.JobRequirementssc__TagsWrapper-sc-15g5po6-2 div.TagStyle__TagContentWrapper-sc-r1wv7a-1::text")
        job_item["job_benefits"]     = css_all("div.Benefitssc__TagContainer-sc-lu2ip4-1 p::text")
        job_item["job_posted_at"]    = response.meta.get("job_posted_at", "")
        job_item["job_deadline"]     = ""
        job_item["scraped_at"]       = datetime.now()
        yield job_item