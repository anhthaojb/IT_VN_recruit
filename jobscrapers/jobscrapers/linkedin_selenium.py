import time
import os
import json
import re
import signal
from datetime import datetime, date
from urllib.parse import quote_plus
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from jobscrapers.pipelines import clean_dict as clean_item, get_db_connection, save_to_db
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "jobscrapers"))

load_dotenv()

# ===== CONFIG =====
MAX_JOBS_PER_KEYWORD = 10
JOB_DETAIL_WAIT      = 8
OUTPUT_DIR  = os.path.join("data", "linkedin")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, f"{date.today().isoformat()}.jsonl")

KEYWORDS_BY_CATEGORY = {
    "software_dev": [
        "software engineer",
        "backend developer",
        "frontend developer",
        "full stack developer",
    ],
    "data": [
        "data analyst",
        "data scientist",
        "data engineer",
        "business intelligence",
    ],
    "devops_cloud": [
        "devops engineer",
        "cloud engineer",
        "site reliability engineer",
    ],
    "security": [
        "cybersecurity",
        "security engineer",
        "penetration tester",
    ],
    "ai_ml": [
        "machine learning engineer",
        "AI engineer",
        "NLP engineer",
    ],
}

# ===== SELECTORS =====
SEL_JOB_CARDS = [
    "div[data-job-id]",
    "li.scaffold-layout__list-item div.job-card-container",
    "li.jobs-search-results__list-item",
]
SEL_DETAIL_LOADED = [
    "div#job-details",
    "div.jobs-description-content__text",
    "div.jobs-description__content",
]
SEL_NEXT_PAGE = [
    (By.CSS_SELECTOR, 'button[aria-label="View next page"]'),
    (By.CSS_SELECTOR, "button.jobs-search-pagination__button--next"),
    (By.XPATH,        '//button[contains(@aria-label,"next")]'),
]
SEL_PASSWORD  = [(By.ID, "password"), (By.XPATH, '//input[@type="password"]')]
SEL_LOGIN_BTN = [
    (By.XPATH, '//button[@type="submit"]'),
    (By.XPATH, '//button[contains(.,"Sign in")]'),
]

# Regex dùng chung — compile 1 lần
TIME_PAT = re.compile(
    r"(?:reposted\s+)?(?:\d+\s+(?:second|minute|hour|day|week|month|year)s?\s+ago|just now)",
    re.IGNORECASE,
)
LOC_PAT = re.compile(r".+,.+")


# =========================================================
#  Helpers
# =========================================================

def safe_text(el):
    """Đọc .text an toàn — trả về '' nếu element đã stale."""
    try:
        return el.text.strip()
    except StaleElementReferenceException:
        return ""


def wait_any_css(driver, css_selectors, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        for sel in css_selectors:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els:
                return sel, els
        time.sleep(0.5)
    return None, []


def find_first(driver, selectors_list, timeout=8):
    wait = WebDriverWait(driver, timeout)
    for by, sel in selectors_list:
        try:
            el = wait.until(EC.presence_of_element_located((by, sel)))
            if el.is_displayed():
                return el
        except Exception:
            continue
    return None


def load_seen_urls(filepath):
    """Load các URL đã scrape từ file JSONL hiện có (tránh trùng khi chạy lại trong ngày)."""
    seen = set()
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    item = json.loads(line)
                    if item.get("job_url"):
                        seen.add(item["job_url"])
                except Exception:
                    pass
    return seen


# =========================================================
#  Parse job detail
# =========================================================

def parse_job(driver, keyword, category):
    """
    Parse job detail panel.
    Trả về dict thô hoặc None nếu trang chưa load đủ.
    Cleaning được xử lý bởi clean_item() từ selenium_pipelines.
    """

    # ── Job title ──────────────────────────────────────────
    job_title = ""
    for sel in [
        "h1.job-details-jobs-unified-top-card__job-title",
        "h1.t-24",
        "h1",
    ]:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els and els[0].text.strip():
            job_title = els[0].text.strip()
            break
    if not job_title:
        return None

    # ── About the job ──────────────────────────────────────
    about_job = ""
    for sel in [
        "div#job-details",
        "div.jobs-description-content__text",
        "article.jobs-description",
    ]:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els:
            about_job = els[0].text.strip()
            break

    # ── Location + job_posted_at ───────────────────────────
    location = ""
    job_posted_at = ""
    tertiary = driver.find_elements(
        By.CSS_SELECTOR,
        "div.job-details-jobs-unified-top-card__tertiary-description-container "
        "span.tvm__text--low-emphasis",
    )
    tokens = [t for t in (safe_text(s) for s in tertiary) if t and t != "·"]
    for token in tokens:
        if not job_posted_at and TIME_PAT.search(token):
            job_posted_at = token
        elif not location and LOC_PAT.match(token) and not TIME_PAT.search(token):
            location = token

    # ── work_mode + job_type ───────────────────────────────
    work_mode = ""
    job_type  = ""
    WORK_MODE_VALUES = {"on-site", "remote", "hybrid"}
    JOB_TYPE_VALUES  = {"full-time", "part-time", "contract", "internship", "temporary"}
    pref_btns = driver.find_elements(
        By.CSS_SELECTOR,
        "div.job-details-fit-level-preferences button span.tvm__text",
    )
    for btn in pref_btns:
        text  = safe_text(btn)
        lower = text.lower()
        if not work_mode and lower in WORK_MODE_VALUES:
            work_mode = text
        elif not job_type and lower in JOB_TYPE_VALUES:
            job_type = text

    # ── Company title ──────────────────────────────────────
    company_title = ""
    els = driver.find_elements(
        By.CSS_SELECTOR,
        'a[data-view-name="job-details-about-company-name-link"]',
    )
    if els:
        company_title = els[0].text.strip()

    # ── Company industry + size ────────────────────────────
    company_industry = ""
    company_size = ""
    info_divs = driver.find_elements(
        By.CSS_SELECTOR,
        "div.jobs-company__box div.t-14.mt5",
    )
    if info_divs:
        spans = info_divs[0].find_elements(
            By.CSS_SELECTOR, "span.jobs-company__inline-information"
        )
        if spans:
            company_size = spans[0].text.strip()
        raw = info_divs[0].text.strip()
        for sp in spans:
            raw = raw.replace(sp.text.strip(), "")
        company_industry = raw.strip()

    # ── Trả về dict thô — clean_item() sẽ xử lý tiếp ─────
    return {
        "website"          : "linkedin",
        "job_title"        : job_title,
        "company_title"    : company_title,
        "location"         : location,
        "job_posted_at"    : job_posted_at,
        "work_mode"        : work_mode,
        "job_type"         : job_type,
        "raw_about_job"    : about_job,   # clean_item() map → job_description
        "company_title"    : company_title,
        "company_industry" : company_industry,
        "company_size"     : company_size,
        "job_category"     : category,
        "compensation"     : "",
        "scraped_at"       : datetime.now().isoformat(),
        "_search_keyword"  : keyword,
    }


# =========================================================
#  Core scrape — một keyword
# =========================================================

def _get_cards(driver):
    """Luôn fetch cards MỚI từ DOM — không giữ reference cũ."""
    matched_sel, cards = wait_any_css(driver, SEL_JOB_CARDS, timeout=12)
    return matched_sel, cards


def _click_card_by_index(driver, index):
    """
    Re-fetch card tại đúng index từ DOM rồi mới click.
    Có retry 3 lần nếu gặp StaleElementReferenceException.
    """
    for attempt in range(3):
        try:
            _, cards = _get_cards(driver)
            if index >= len(cards):
                return False
            card = cards[index]
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", card)
            time.sleep(0.4)
            driver.execute_script("arguments[0].click();", card)
            return True
        except StaleElementReferenceException:
            time.sleep(0.5)
    return False


def _click_next_page(driver):
    """
    Click Next bằng JavaScript — tránh ElementClickIntercepted.
    Trả về True nếu click thành công.
    """
    next_btn = find_first(driver, SEL_NEXT_PAGE, timeout=5)
    if not next_btn:
        return False
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_btn)
        time.sleep(0.3)
        driver.execute_script("arguments[0].click();", next_btn)
        return True
    except Exception:
        return False


def scrape_keyword(driver, keyword, category, seen_urls, f):
    """
    Scrape tối đa MAX_JOBS_PER_KEYWORD job cho một keyword.
    seen_urls : set các URL đã scrape (tránh trùng trong ngày).
    f         : file handle để ghi JSONL ngay lập tức.
    Trả về số job mới scrape được.
    """
    url = (
        f"https://www.linkedin.com/jobs/search/"
        f"?keywords={quote_plus(keyword)}&refresh=true"
    )
    driver.get(url)
    print(f"\n[{category}] {keyword!r} → {url}")

    matched, _ = wait_any_css(driver, SEL_JOB_CARDS, timeout=15)
    if not matched:
        print("Job list không load — bỏ qua keyword này")
        return 0

    job_count = 0
    page = 1

    while job_count < MAX_JOBS_PER_KEYWORD:
        print(f"  📄 Trang {page} | {job_count}/{MAX_JOBS_PER_KEYWORD}")

        _, cards_now = _get_cards(driver)
        if not cards_now:
            print("Không tìm thấy card")
            break
        total_cards = len(cards_now)

        card_index = 0
        while card_index < total_cards:
            if job_count >= MAX_JOBS_PER_KEYWORD:
                break

            # ── Click card ────────────────────────────────────────
            clicked = _click_card_by_index(driver, card_index)
            if not clicked:
                _, cards_now = _get_cards(driver)
                total_cards = len(cards_now)
                card_index += 1
                continue

            time.sleep(1.2)

            # ── Chờ detail panel ──────────────────────────────────
            matched_detail, _ = wait_any_css(
                driver, SEL_DETAIL_LOADED, timeout=JOB_DETAIL_WAIT
            )
            if not matched_detail:
                print(f"Detail không load (card {card_index}) — skip")
                card_index += 1
                continue

            # ── Bỏ qua URL đã scrape ─────────────────────────────
            current_url = driver.current_url
            if current_url in seen_urls:
                print("Đã scrape — skip")
                card_index += 1
                continue

            # ── Parse + clean ─────────────────────────────────────
            raw = parse_job(driver, keyword, category)
            if not raw:
                print(f" Parse thất bại (card {card_index}) — skip")
                card_index += 1
                continue

            item = clean_item(raw)   # ← dùng selenium_pipelines

            seen_urls.add(current_url)
            job_count += 1
            print(f"[{job_count}] {item['job_title']}")
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            f.flush()

            card_index += 1

        if job_count >= MAX_JOBS_PER_KEYWORD:
            break

        # ── Next page ─────────────────────────────────────────────
        _, cards_before = _get_cards(driver)
        anchor = cards_before[0] if cards_before else None

        if not _click_next_page(driver):
            print(" Hết trang — không tìm thấy nút Next")
            break

        try:
            if anchor:
                WebDriverWait(driver, 12).until(EC.staleness_of(anchor))
            else:
                time.sleep(2)
            wait_any_css(driver, SEL_JOB_CARDS, timeout=10)
            time.sleep(1)
        except Exception:
            time.sleep(3)

        page += 1

    return job_count


# =========================================================
#  Driver + Login
# =========================================================

def init_driver():
    opts = Options()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=opts)
    driver.execute_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    )
    return driver


def login(driver):
    username = os.getenv("LINKEDIN_USERNAME")
    password = os.getenv("LINKEDIN_PASSWORD")
    if not username or not password:
        raise ValueError("Thiếu LINKEDIN_USERNAME / LINKEDIN_PASSWORD trong .env")

    driver.get("https://www.linkedin.com/login")
    time.sleep(3)
    try:
        user_field = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "username"))
        )
    except Exception:
        raise Exception(
            f"Trang login không load — title: {driver.title!r}, url: {driver.current_url}"
        )

    user_field.clear()
    user_field.send_keys(username)

    pwd = find_first(driver, SEL_PASSWORD)
    if not pwd:
        raise Exception("Không tìm thấy ô password")
    pwd.clear()
    pwd.send_keys(password)

    btn = find_first(driver, SEL_LOGIN_BTN)
    if not btn:
        raise Exception("Không tìm thấy nút Sign in")
    btn.click()

    try:
        WebDriverWait(driver, 30).until(lambda d: "/login" not in d.current_url)
    except Exception:
        raise Exception(f"Login timeout — URL: {driver.current_url}")

    current = driver.current_url
    if any(x in current for x in ["/checkpoint", "/challenge", "/uas/login"]):
        print(f"\nLinkedIn yêu cầu xác minh bảo mật — URL: {current}")
        input("   Hoàn thành xác minh trên browser rồi nhấn Enter: ")
        current = driver.current_url
        if "/login" in current or "/checkpoint" in current:
            raise Exception(f" Vẫn chưa login — URL: {current}")

    print(f"Đăng nhập thành công — {driver.current_url}")


# =========================================================
#  Main
# =========================================================

def main():
    driver = init_driver()

    def _exit(sig, frame):
        print("\nđóng driver...")
        try:
            driver.service.process.kill()
        except Exception:
            pass
        os._exit(0)

    signal.signal(signal.SIGINT, _exit)

    try:
        login(driver)

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        seen_urls = load_seen_urls(OUTPUT_FILE)
        print(f"File hôm nay: {OUTPUT_FILE}")
        print(f"Đã load {len(seen_urls)} URL cũ — sẽ bỏ qua khi scrape")

        total = 0
        summary = {}

        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            for category, keywords in KEYWORDS_BY_CATEGORY.items():
                summary[category] = {}
                for keyword in keywords:
                    count = scrape_keyword(driver, keyword, category, seen_urls, f)
                    summary[category][keyword] = count
                    total += count
                    time.sleep(3)

        print(f"\n{'='*55}")
        print(f"HOÀN THÀNH — {total} job mới → {OUTPUT_FILE}")
        print(f"{'='*55}")
        for cat, kw_counts in summary.items():
            cat_total = sum(kw_counts.values())
            print(f"  [{cat}] {cat_total} job")
            for kw, cnt in kw_counts.items():
                print(f"    • {kw!r}: {cnt}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()