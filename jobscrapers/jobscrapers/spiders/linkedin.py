import scrapy
import time
import os
from datetime import datetime
from scrapy.http import HtmlResponse
from dotenv import load_dotenv

load_dotenv()

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from jobscrapers.items import JobItem


# ===== CONFIG =====
SEARCH_KEYWORD   = "data scientist"
MAX_PAGES        = 3

# ===== SELECTORS =====
SELECTORS_PASSWORD  = [(By.ID, "password"), (By.NAME, "session_password"), (By.XPATH, '//input[@type="password"]')]
SELECTORS_LOGIN_BTN = [(By.XPATH, '//button[@type="submit"]'), (By.XPATH, '//button[contains(.,"Sign in")]')]
SELECTORS_SEARCH_BOX= [(By.XPATH, '//input[@aria-label="Search"]'), (By.XPATH, '//input[contains(@class,"search-global-typeahead")]')]
SELECTORS_JOBS_BTN  = [(By.XPATH, '//a[contains(@href,"/jobs")]'), (By.XPATH, '//button[contains(@aria-label,"Jobs")]')]
SELECTORS_JOBS_SEARCH=[
    (By.CSS_SELECTOR, 'input[componentkey="jobSearchBox"]'),
    (By.CSS_SELECTOR, 'input[data-testid="typeahead-input"]'),
    (By.XPATH, '//input[contains(@placeholder,"Title, skill or Company")]'),
]
SELECTORS_NEXT_PAGE = [
    (By.CSS_SELECTOR, 'button.jobs-search-pagination__button--next'),
    (By.XPATH, '//button[contains(@aria-label,"next")]'),
]


# =========================================================
#  Helpers
# =========================================================

def find_element_fallback(driver, selectors, timeout=10):
    wait = WebDriverWait(driver, timeout)
    for by, selector in selectors:
        try:
            el = wait.until(EC.presence_of_element_located((by, selector)))
            if el.is_displayed():
                return el
        except Exception:
            continue
    raise Exception(f"Element not found among selectors: {selectors}")


def build_scrapy_response(driver, url=None):
    """Wrap current Selenium page_source into a Scrapy HtmlResponse."""
    return HtmlResponse(
        url=url or driver.current_url,
        body=driver.page_source,
        encoding="utf-8",
    )


# =========================================================
#  Spider
# =========================================================

class LinkedinSpider(scrapy.Spider):
    name = "linkedin"
    allowed_domains = ["linkedin.com"]

    # Selenium driver is shared across the spider lifetime
    driver = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start_requests(self):
        """Bootstrap: launch Selenium, log in, navigate to job results."""
        self.driver = self._init_driver()
        self._login()
        self._go_to_jobs()
        self._search_jobs(SEARCH_KEYWORD)

        # Hand off to Scrapy by yielding a dummy request whose callback
        # immediately reads from the already-loaded Selenium page.
        yield scrapy.Request(
            url=self.driver.current_url,
            callback=self.parse,
            dont_filter=True,
        )

    def closed(self, reason):
        if self.driver:
            self.driver.quit()
            self.logger.info("🛑 Selenium driver closed")

    # ------------------------------------------------------------------
    # parse  –  list page  (mirrors CareerlinkSpider.parse)
    # ------------------------------------------------------------------

    def parse(self, response):
        """Iterate job cards on the current search-results page."""
        for page_num in range(1, MAX_PAGES + 1):
            self.logger.info(f"📄 Page {page_num}")

            # Re-read live DOM into a fresh Scrapy response
            response = build_scrapy_response(self.driver)

            job_cards = self.driver.find_elements(
                By.CSS_SELECTOR, "li.jobs-search-results__list-item"
            )

            for card in job_cards:
                try:
                    card.click()
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, "div.jobs-description-content__text")
                        )
                    )
                    detail_response = build_scrapy_response(self.driver)
                    yield from self.parse_job_page(detail_response)

                except Exception as e:
                    self.logger.warning(f"⚠️  Skip job: {e}")

            # ── next page ──────────────────────────────────────────────
            if page_num < MAX_PAGES:
                if not self._go_next_page(job_cards):
                    self.logger.info("❌ No more pages")
                    break

    # ------------------------------------------------------------------
    # parse_job_page  –  detail page  (mirrors CareerlinkSpider.parse_job_page)
    # ------------------------------------------------------------------

    def parse_job_page(self, response):
        job_item = JobItem()

        job_item["website"]          = "linkedin"
        job_item["job_url"]          = response.url

        # ── Core fields ──────────────────────────────────────────────
        job_item["job_title"]        = response.css("h1::text").get("").strip()
        job_item["company_title"]    = (
            response.css("a[href*='company']::text").get("")
            or response.css(".job-details-jobs-unified-top-card__company-name a::text").get("")
        ).strip()
        job_item["location"]         = response.css(
            "span.tvm__text::text, "
            ".job-details-jobs-unified-top-card__bullet::text"
        ).get("").strip()

        # ── Metadata chips (level / job-type / work-mode) ─────────────
        chips = response.css(
            "li.job-details-jobs-unified-top-card__job-insight span::text, "
            "span.ui-label::text"
        ).getall()
        chips = [c.strip() for c in chips if c.strip()]

        job_item["level"]            = self._find_chip(chips, ["Internship", "Entry level",
                                                                "Associate", "Mid-Senior",
                                                                "Director", "Executive"])
        job_item["job_type"]         = self._find_chip(chips, ["Full-time", "Part-time",
                                                                "Contract", "Temporary",
                                                                "Volunteer", "Other"])
        job_item["work_mode"]        = self._find_chip(chips, ["On-site", "Hybrid", "Remote"])

        # ── Salary / compensation ─────────────────────────────────────
        job_item["compensation"]     = response.css(
            "div.salary--tiers span::text, "
            ".compensation__salary::text"
        ).get("").strip()

        # ── Company extras ────────────────────────────────────────────
        job_item["company_size"]     = response.xpath(
            '//span[contains(text(),"employees")]/text()'
        ).get("").strip()
        job_item["company_industry"] = response.css(
            ".job-details-jobs-unified-top-card__job-insight--highlight span::text"
        ).get("").strip()

        # ── Fields not surfaced on LinkedIn ──────────────────────────
        job_item["experience"]       = ""
        job_item["job_category"]     = ""
        job_item["number_recruit"]   = ""
        job_item["education_level"]  = ""
        job_item["job_deadline"]     = ""

        # ── Description / Requirements ────────────────────────────────
        full_desc = response.css(
            "div.jobs-description-content__text *::text"
        ).getall()
        job_item["job_description"]  = [t.strip() for t in full_desc if t.strip()]
        job_item["job_requirement"]  = []   # LinkedIn merges desc+requirements

        # ── Dates ─────────────────────────────────────────────────────
        job_item["job_posted_at"]    = response.css(
            "span.jobs-unified-top-card__posted-date::text, "
            "span.tvm__text--neutral::text"
        ).get("").strip()
        job_item["scraped_at"]       = datetime.now()

        yield job_item

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _init_driver(self):
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        driver = webdriver.Chrome(options=options)
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        self.logger.info("✅ Driver initialized")
        return driver

    def _login(self):
        username = os.getenv("LINKEDIN_USERNAME")
        password = os.getenv("LINKEDIN_PASSWORD")

        if not username or not password:
            raise ValueError("❌ LINKEDIN_USERNAME hoặc LINKEDIN_PASSWORD chưa được set trong .env")

        self.driver.get("https://www.linkedin.com/login")
        time.sleep(3)

        self.driver.find_element(By.ID, "username").send_keys(username)
        find_element_fallback(self.driver, SELECTORS_PASSWORD).send_keys(password)
        find_element_fallback(self.driver, SELECTORS_LOGIN_BTN).click()

        # Chờ URL thoát khỏi trang login (không phụ thuộc selector search box)
        try:
            WebDriverWait(self.driver, 20).until(
                lambda d: "/login" not in d.current_url and "/checkpoint" not in d.current_url
            )
        except Exception:
            current_url = self.driver.current_url
            if "/checkpoint" in current_url or "/challenge" in current_url:
                raise Exception("⚠️  LinkedIn yêu cầu xác minh bảo mật — hãy hoàn thành thủ công rồi chạy lại.")
            raise Exception(f"❌ Login timeout — URL hiện tại: {current_url}")

        self.logger.info(f"✅ Login successful — URL: {self.driver.current_url}")

    def _go_to_jobs(self):
        jobs_btn = find_element_fallback(self.driver, SELECTORS_JOBS_BTN)
        jobs_btn.click()
        WebDriverWait(self.driver, 10).until(lambda d: "jobs" in d.current_url)
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "ul.jobs-search-results__list"))
        )
        self.logger.info("✅ Jobs page loaded")

    def _search_jobs(self, keyword):
        search_box = find_element_fallback(self.driver, SELECTORS_JOBS_SEARCH)
        self.driver.execute_script("arguments[0].click();", search_box)
        time.sleep(1)
        search_box.send_keys(keyword)
        search_box.send_keys(Keys.ENTER)
        WebDriverWait(self.driver, 15).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "li.jobs-search-results__list-item")
            )
        )
        self.logger.info(f"✅ Job search loaded: '{keyword}'")

    def _go_next_page(self, current_jobs):
        try:
            next_btn = find_element_fallback(self.driver, SELECTORS_NEXT_PAGE, timeout=5)
            next_btn.click()
            WebDriverWait(self.driver, 10).until(EC.staleness_of(current_jobs[0]))
            time.sleep(2)
            return True
        except Exception:
            return False

    @staticmethod
    def _find_chip(chips: list, candidates: list) -> str:
        """Return the first chip value that matches one of the candidate strings."""
        for chip in chips:
            for c in candidates:
                if c.lower() in chip.lower():
                    return chip
        return ""