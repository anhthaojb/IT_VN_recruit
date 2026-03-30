import scrapy
import time
import os
import signal
from datetime import datetime
from urllib.parse import quote_plus
from scrapy.http import HtmlResponse
from scrapy.exceptions import CloseSpider
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

load_dotenv()


# ===== CONFIG =====
SEARCH_KEYWORD  = "data scientist"
MAX_JOBS        = 10          # tự dừng sau đúng 150 job
JOB_DETAIL_WAIT = 8            # giây chờ detail load


# ===== SELECTORS =====
SEL_PASSWORD  = [(By.ID, "password"), (By.XPATH, '//input[@type="password"]')]
SEL_LOGIN_BTN = [(By.XPATH, '//button[@type="submit"]'), (By.XPATH, '//button[contains(.,"Sign in")]')]
SEL_JOB_CARDS = [
    "div[data-job-id]",                    # selector ổn định nhất — luôn có khi card render
    "li.scaffold-layout__list-item div.job-card-container",
    "li.jobs-search-results__list-item",
]
SEL_DETAIL_LOADED = [
    "div#job-details",                     # id cố định từ HTML thực tế
    "div.jobs-description-content__text",
    "div.jobs-description__content",
    "article.jobs-description",
]
SEL_NEXT_PAGE = [
    (By.CSS_SELECTOR, 'button[aria-label="View next page"]'),   # từ HTML thực tế
    (By.CSS_SELECTOR, 'button.jobs-search-pagination__button--next'),
    (By.XPATH,        '//button[contains(@aria-label,"next")]'),
]


# =========================================================
#  Helpers
# =========================================================

def wait_any(driver, css_selectors, timeout=10):
    """Chờ đến khi bất kỳ selector nào xuất hiện."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for sel in css_selectors:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els:
                return sel, els
        time.sleep(0.5)
    return None, []


def find_first(driver, selectors_list, timeout=8):
    """selectors_list: list of (By, selector). Trả về element đầu tiên tìm thấy."""
    wait = WebDriverWait(driver, timeout)
    for by, sel in selectors_list:
        try:
            el = wait.until(EC.presence_of_element_located((by, sel)))
            if el.is_displayed():
                return el
        except Exception:
            continue
    return None


def scrapy_response(driver):
    return HtmlResponse(url=driver.current_url, body=driver.page_source, encoding="utf-8")


def clean_text(texts):
    """Gộp list string thành một đoạn text sạch."""
    return " ".join(t.strip() for t in texts if t.strip())


# =========================================================
#  Spider
# =========================================================

class LinkedinSpider(scrapy.Spider):
    name            = "linkedin"
    allowed_domains = ["linkedin.com"]
    driver          = None
    _job_count      = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start_requests(self):
        self.driver = self._init_driver()

        # Ctrl+C: kill chromedriver process trực tiếp, không dùng driver.quit()
        # vì quit() gửi HTTP request — nếu chromedriver đã chết thì retry mãi không thoát
        def _exit(sig, frame):
            print("\n⛔ Ctrl+C — force kill...")
            try:
                if self.driver and self.driver.service.process:
                    self.driver.service.process.kill()
            except Exception:
                pass
            os._exit(0)
        signal.signal(signal.SIGINT, _exit)

        self._login()
        self._go_to_search()

        yield scrapy.Request(
            url=self.driver.current_url,
            callback=self.parse,
            dont_filter=True,
        )

    def closed(self, reason):
        if self.driver:
            try:
                # Dùng timeout ngắn — tránh treo khi chromedriver đã chết
                self.driver.service.process.kill()
            except Exception:
                pass
            self.logger.info(f"🛑 Driver đóng — {reason} — đã scrape {self._job_count} job")

    # ------------------------------------------------------------------
    # parse — vòng lặp trang
    # ------------------------------------------------------------------

    def parse(self, response):
        page = 1
        while True:
            self.logger.info(f"📄 Trang {page} | {self._job_count}/{MAX_JOBS} job")

            matched_sel, cards = wait_any(self.driver, SEL_JOB_CARDS, timeout=12)
            if not cards:
                self.logger.warning("⚠️  Không tìm thấy job card — dừng")
                break

            for card in cards:
                # Kiểm tra đủ số lượng TRƯỚC khi click
                if self._job_count >= MAX_JOBS:
                    self.logger.info(f"✅ Đủ {MAX_JOBS} job — đóng spider")
                    raise CloseSpider(f"reached_{MAX_JOBS}_jobs")

                try:
                    # Scroll vào view — bắt buộc vì LinkedIn lazy-render (occlude) các card ngoài viewport
                    self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", card)
                    time.sleep(0.4)   # chờ LinkedIn render card vừa scroll vào
                    card.click()
                    time.sleep(1)

                    matched_detail, _ = wait_any(self.driver, SEL_DETAIL_LOADED, timeout=JOB_DETAIL_WAIT)
                    if not matched_detail:
                        self.logger.warning("⚠️  Detail không load — skip")
                        continue

                    item = self._parse_raw(scrapy_response(self.driver))
                    if item:
                        self._job_count += 1
                        yield item

                except CloseSpider:
                    raise
                except Exception as e:
                    self.logger.warning(f"⚠️  Skip: {e}")

            # ── next page ─────────────────────────────────────────────
            next_btn = find_first(self.driver, SEL_NEXT_PAGE, timeout=5)
            if not next_btn:
                self.logger.info("📭 Hết trang")
                break

            try:
                first_card = cards[0]
                next_btn.click()
                WebDriverWait(self.driver, 10).until(EC.staleness_of(first_card))
                time.sleep(2)
                page += 1
            except Exception as e:
                self.logger.warning(f"⚠️  Không qua được trang tiếp: {e}")
                break

    # ------------------------------------------------------------------
    # _parse_raw — chỉ thu raw text 3 phần chính
    # ------------------------------------------------------------------

    def _parse_raw(self, resp):
        """
        Thu đúng 3 phần raw — xử lý cấu trúc chi tiết để sau:
          job_title         : tên công việc
          raw_about_job     : toàn bộ phần About the job
          raw_about_company : toàn bộ phần About the company
        """

        # 1. Job title
        job_title = (
            resp.css("h1.job-details-jobs-unified-top-card__job-title::text").get()
            or resp.css("h1::text").get()
            or ""
        ).strip()

        if not job_title:
            return None  # trang chưa load đủ

        # 2. About the job — id="job-details" là container chính xác từ HTML
        about_job = clean_text(
            resp.css("div#job-details *::text").getall()
            or resp.css("div.jobs-description-content__text *::text").getall()
            or resp.css("article.jobs-description *::text").getall()
        )

        # 3. About the company — section.jobs-company > div.jobs-company__box
        about_company = clean_text(
            resp.css("section.jobs-company div.jobs-company__box *::text").getall()
            or resp.css("div.jobs-company__box *::text").getall()
            or resp.css("section[data-view-name='job-details-about-company-module'] *::text").getall()
        )

        return {
            "website"          : "linkedin",
            "job_url"          : resp.url,
            "job_title"        : job_title,
            "raw_about_job"    : about_job,
            "raw_about_company": about_company,
            "scraped_at"       : datetime.now().isoformat(),
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _init_driver(self):
        opts = Options()
        opts.add_argument("--start-maximized")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        driver = webdriver.Chrome(options=opts)
        driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        self.logger.info("✅ Driver initialized")
        return driver

    def _login(self):
        username = os.getenv("LINKEDIN_USERNAME")
        password = os.getenv("LINKEDIN_PASSWORD")
        if not username or not password:
            raise ValueError("❌ Thiếu LINKEDIN_USERNAME / LINKEDIN_PASSWORD trong .env")

        self.driver.get("https://www.linkedin.com/login")
        time.sleep(3)

        self.driver.find_element(By.ID, "username").send_keys(username)
        pwd = find_first(self.driver, SEL_PASSWORD)
        if not pwd:
            raise Exception("❌ Không tìm thấy ô password")
        pwd.send_keys(password)

        btn = find_first(self.driver, SEL_LOGIN_BTN)
        if not btn:
            raise Exception("❌ Không tìm thấy nút Sign in")
        btn.click()

        try:
            WebDriverWait(self.driver, 20).until(
                lambda d: "/login" not in d.current_url and "/checkpoint" not in d.current_url
            )
        except Exception:
            url = self.driver.current_url
            if any(x in url for x in ["/checkpoint", "/challenge"]):
                raise Exception("⚠️  LinkedIn yêu cầu xác minh — hoàn thành thủ công rồi chạy lại")
            raise Exception(f"❌ Login timeout — URL: {url}")

        self.logger.info(f"✅ Đăng nhập thành công — {self.driver.current_url}")

    def _go_to_search(self):
        url = f"https://www.linkedin.com/jobs/search/?keywords={quote_plus(SEARCH_KEYWORD)}&refresh=true"
        self.driver.get(url)
        self.logger.info(f"🔍 {url}")

        matched, _ = wait_any(self.driver, SEL_JOB_CARDS, timeout=15)
        if not matched:
            raise Exception(f"❌ Job list không load — title: {self.driver.title}")
        self.logger.info(f"✅ Sẵn sàng ({matched})")