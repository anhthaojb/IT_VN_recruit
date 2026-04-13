import time
import os
import re
import sys
import json
import random
import signal
import argparse
import pathlib
from datetime import datetime
from urllib.parse import quote_plus, urlparse, parse_qs, urlencode
from dotenv import load_dotenv

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
import undetected_chromedriver as uc

load_dotenv()

_project_root = str(pathlib.Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from jobscrapers.pipelines import clean_dict, save_to_db, get_db_connection

try:
    from ai_processor import process_linkedin_item
    AI_ENABLED = True
    print("✅ AI processor loaded")
except Exception:
    AI_ENABLED = False
    print("⚠️  ai_processor không tìm thấy — chạy không có AI")

# ===== CLI =====
parser = argparse.ArgumentParser()
parser.add_argument("--mode", default="daily", choices=["daily", "full"])
args, unknown = parser.parse_known_args()
if unknown and unknown[0] in ["daily", "full"]:
    args.mode = unknown[0]

# ===== CONFIG =====
MAX_JOBS_PER_KEYWORD = 5   # KPI: chỉ tính job MỚI
JOB_DETAIL_WAIT      = 20
MIN_ABOUT_JOB_CHARS  = 200
DAILY_MAX_AGE_DAYS   = 3

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

# ===== COOKIE =====
COOKIE_FILE = pathlib.Path("data") / "linkedin_cookies.json"

# ===== SELECTORS =====
SEL_JOB_CARDS = [
    "div[data-job-id]",
    "div.job-card-container",
    "li[data-occludable-job-id]",
]
SEL_DETAIL_LOADED = [
    "div#job-details",
    "div.jobs-description__content",
    "div.jobs-description-content__text",
]
SEL_NEXT_PAGE = [
    (By.CSS_SELECTOR, 'button[aria-label="View next page"]'),
    (By.CSS_SELECTOR, 'button[aria-label="Xem trang tiếp theo"]'),
    (By.CSS_SELECTOR, "button.jobs-search-pagination__button--next"),
    (By.XPATH,        '//button[contains(@aria-label,"next")]'),
    (By.XPATH,        '//button[contains(@aria-label,"tiếp theo")]'),
]
SEL_PASSWORD  = [(By.ID, "password"), (By.XPATH, '//input[@type="password"]')]
SEL_LOGIN_BTN = [
    (By.XPATH, '//button[@type="submit"]'),
    (By.XPATH, '//button[contains(.,"Sign in")]'),
]

TIME_PAT = re.compile(
    r"(?:reposted\s+)?(?:\d+\s+(?:second|minute|hour|day|week|month|year)s?\s+ago|just now)",
    re.IGNORECASE,
)
LOC_PAT = re.compile(r".+,.+")


# =========================================================
#  URL Normalization
#  LinkedIn URL thường có dạng:
#    /jobs/search/?currentJobId=123&keywords=...   (search page)
#    /jobs/view/123/                               (detail page)
#  Cần chuẩn hóa về 1 dạng để so sánh đúng với DB.
# =========================================================

def _normalize_li_url(url: str) -> str:
    """
    Trích jobId từ URL LinkedIn và trả về URL chuẩn dạng:
      https://www.linkedin.com/jobs/view/{jobId}/
    Nếu không tìm thấy jobId → trả về url gốc (stripped query).
    """
    if not url:
        return url
    # Thử lấy từ path: /jobs/view/4352402798/
    m = re.search(r"/jobs/view/(\d+)", url)
    if m:
        return f"https://www.linkedin.com/jobs/view/{m.group(1)}/"
    # Thử lấy từ query param: ?currentJobId=4352402798
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    job_id = qs.get("currentJobId", [None])[0]
    if job_id:
        return f"https://www.linkedin.com/jobs/view/{job_id}/"
    # Fallback: bỏ query string
    return url.split("?")[0]


# =========================================================
#  Helpers
# =========================================================

def safe_text(el):
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


def ensure_db_connection(cur, conn):
    try:
        conn.ping(reconnect=True, attempts=3, delay=2)
    except Exception as e:
        print(f"  ⚠️  DB reconnect failed: {e}")


def _is_old_linkedin(posted_text: str) -> bool:
    if not posted_text:
        return False
    m = re.search(
        r"(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago",
        posted_text, re.IGNORECASE,
    )
    if not m:
        return False
    n, unit = int(m.group(1)), m.group(2).lower()
    age_days = {
        "second": 0, "minute": 0, "hour": 0,
        "day": n, "week": n * 7,
        "month": n * 30, "year": n * 365,
    }.get(unit, 0)
    return age_days > DAILY_MAX_AGE_DAYS


def _enrich_with_ai(item: dict) -> dict:
    if not AI_ENABLED:
        item["job_description"] = item.get("raw_about_job", "")
        item["job_requirement"] = ""
        return item
    try:
        enriched = process_linkedin_item(item)
        if not enriched.get("job_description"):
            enriched["job_description"] = item.get("raw_about_job", "")
        return enriched
    except Exception as e:
        print(f"    ⚠️  AI error: {e} — fallback về raw_about_job")
        item["job_description"] = item.get("raw_about_job", "")
        item["job_requirement"] = ""
        return item


# =========================================================
#  Driver + Login
# =========================================================

def init_driver():
    opts = uc.ChromeOptions()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--lang=vi-VN,vi;q=0.9,en-US;q=0.8")

    driver = uc.Chrome(options=opts, version_main=None)
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(20)
    return driver


def _is_logged_in(driver):
    current = driver.current_url
    if any(x in current for x in ["/login", "/checkpoint", "/authwall", "/uas/", "/challenge"]):
        return False
    if any(x in current for x in ["/feed", "/jobs", "/mynetwork", "/messaging", "/in/"]):
        return True
    for sel in ["div.global-nav__me", "img.global-nav__me-photo", "a[href*='logout']"]:
        if driver.find_elements(By.CSS_SELECTOR, sel):
            return True
    return False


def save_cookies(driver):
    COOKIE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(COOKIE_FILE, "w", encoding="utf-8") as f:
        json.dump(driver.get_cookies(), f, ensure_ascii=False, indent=2)
    print(f"✓ Đã lưu cookies → {COOKIE_FILE}")


def load_cookies(driver):
    if not COOKIE_FILE.exists():
        return False
    driver.get("https://www.linkedin.com")
    time.sleep(3)
    with open(COOKIE_FILE, encoding="utf-8") as f:
        cookies = json.load(f)
    for cookie in cookies:
        cookie.pop("sameSite", None)
        cookie.pop("expiry", None)
        try:
            driver.add_cookie(cookie)
        except Exception:
            pass
    try:
        driver.refresh()
        time.sleep(3)
    except Exception:
        return False
    return True


def login(driver):
    if COOKIE_FILE.exists():
        print("Tìm thấy cookie — đang thử load...")
        load_cookies(driver)
        driver.get("https://www.linkedin.com/jobs")
        time.sleep(3)
        if _is_logged_in(driver):
            print("✓ Đã login từ cookie")
            return
        print("Cookie hết hạn — login lại")
        COOKIE_FILE.unlink(missing_ok=True)

    driver.get("https://www.linkedin.com/login")
    username = os.getenv("LINKEDIN_USERNAME")
    password = os.getenv("LINKEDIN_PASSWORD")

    if username and password:
        try:
            user_field = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "username"))
            )
            user_field.clear()
            user_field.send_keys(username)
            pwd = find_first(driver, SEL_PASSWORD)
            if pwd:
                pwd.clear()
                pwd.send_keys(password)
            btn = find_first(driver, SEL_LOGIN_BTN)
            if btn:
                btn.click()
            WebDriverWait(driver, 20).until(lambda d: "/login" not in d.current_url)
            time.sleep(2)
            print(f"✓ Tự điền form thành công — {driver.current_url}")
        except Exception as e:
            print(f"⚠️  Tự điền thất bại ({e}) — chờ login thủ công")

    current = driver.current_url
    if any(x in current for x in ["/checkpoint", "/challenge", "/uas/login", "/login"]):
        print("\n" + "=" * 55)
        print("  Vui lòng ĐĂNG NHẬP / XÁC MINH THỦ CÔNG trên Chrome.")
        print("  Sau khi vào được trang chủ LinkedIn, nhấn Enter.")
        print("=" * 55)
        input("  >>> Nhấn Enter khi đã login xong: ")
        time.sleep(2)

    driver.get("https://www.linkedin.com/jobs")
    time.sleep(3)

    if not _is_logged_in(driver):
        raise Exception(f"❌ Chưa login được — URL: {driver.current_url}")

    save_cookies(driver)
    print(f"✅ Đăng nhập thành công — {driver.current_url}")


# =========================================================
#  Parse job detail
# =========================================================

def parse_job(driver, keyword, category):
    job_title = ""
    for sel in ["h1.job-details-jobs-unified-top-card__job-title", "h1.t-24", "h1"]:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els and els[0].text.strip():
            job_title = els[0].text.strip()
            break
    if not job_title:
        return None

    raw_about_job = ""
    for sel in ["div#job-details", "div.jobs-description__content", "div.jobs-description-content__text", "article.jobs-description"]:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els:
            raw_about_job = els[0].text.strip()
            break

    if len(raw_about_job) < MIN_ABOUT_JOB_CHARS:
        print(f"    ⚠️  Detail quá ngắn ({len(raw_about_job)} chars) — skip")
        return None

    location = job_posted_at = ""
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

    work_mode = job_type = ""
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

    company_title = ""
    els = driver.find_elements(
        By.CSS_SELECTOR, 'a[data-view-name="job-details-about-company-name-link"]'
    )
    if els:
        company_title = els[0].text.strip()

    company_industry = company_size = ""
    info_divs = driver.find_elements(By.CSS_SELECTOR, "div.jobs-company__box div.t-14.mt5")
    if info_divs:
        spans = info_divs[0].find_elements(By.CSS_SELECTOR, "span.jobs-company__inline-information")
        if spans:
            company_size = spans[0].text.strip()
        raw = info_divs[0].text.strip()
        for sp in spans:
            raw = raw.replace(sp.text.strip(), "")
        company_industry = raw.strip()

    return {
        "website"         : "linkedin",
        "job_title"       : job_title,
        "company_title"   : company_title,
        "location"        : location,
        "job_url"         : driver.current_url,
        "job_posted_at"   : job_posted_at,
        "job_deadline"    : "",
        "work_mode"       : work_mode,
        "job_type"        : job_type,
        "company_size"    : company_size,
        "company_industry": company_industry,
        "job_category"    : keyword,
        "number_recruit"  : "",
        "raw_about_job"   : raw_about_job,
        "job_description" : "",
        "job_requirement" : "",
        "compensation"    : "",
        "level"           : "",
        "experience"      : "",
        "education_level" : "",
        "scraped_at"      : datetime.now().isoformat(),
        "_search_keyword" : keyword,
        "_category"       : category,
    }


# =========================================================
#  Core scrape
# =========================================================

def _get_cards(driver):
    return wait_any_css(driver, SEL_JOB_CARDS, timeout=12)


def _click_card_by_index(driver, index):
    for _ in range(3):
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
        except Exception:
            return False
    return False


def _click_next_page(driver):
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


def scrape_keyword(driver, keyword, category, seen_urls, cur, conn, mode):
    url = (
        f"https://www.linkedin.com/jobs/search/"
        f"?keywords={quote_plus(keyword)}&location=Vietnam"
    )
    driver.get(url)

    wait_time = random.uniform(6, 12)
    print(f"  ⏳ Chờ {wait_time:.1f}s...")
    time.sleep(wait_time)

    print(f"\n🔍 [{category}] {keyword!r} | mode={mode}")
    print(f"  📍 URL: {driver.current_url[:80]}")
    print(f"  📄 Title: {driver.title[:60]}")

    current = driver.current_url
    if any(x in current for x in ["/login", "/checkpoint", "/authwall"]):
        print(f"  ⛔ Bị redirect: {current} — thử login lại")
        login(driver)
        driver.get(url)
        time.sleep(random.uniform(6, 12))

    matched, _ = wait_any_css(driver, SEL_JOB_CARDS, timeout=20)
    if not matched:
        print("  ⚠️  Job list không load — bỏ qua")
        return 0, 0

    # FIX: tách riêng đếm job mới (KPI) và job updated
    count_new     = 0
    count_updated = 0
    page          = 1
    stop_keyword  = False

    while count_new < MAX_JOBS_PER_KEYWORD and not stop_keyword:
        print(f"  📄 Trang {page} | ✅ mới={count_new}/{MAX_JOBS_PER_KEYWORD} 🔄 updated={count_updated}")

        _, cards_now = _get_cards(driver)
        if not cards_now:
            print("  ⚠️  Không tìm thấy card")
            break
        total_cards = len(cards_now)
        card_index  = 0

        while card_index < total_cards:
            if count_new >= MAX_JOBS_PER_KEYWORD or stop_keyword:
                break

            if not _click_card_by_index(driver, card_index):
                _, cards_now = _get_cards(driver)
                total_cards  = len(cards_now)
                card_index  += 1
                continue

            matched_detail, _ = wait_any_css(driver, SEL_DETAIL_LOADED, timeout=JOB_DETAIL_WAIT)
            if not matched_detail:
                print(f"    ⚠️  Detail không load (card {card_index}) — skip")
                card_index += 1
                continue

            current_url      = driver.current_url
            normalized_url   = _normalize_li_url(current_url)

            # Skip nếu đã xử lý trong session này (tránh gọi AI 2 lần)
            if normalized_url in seen_urls:
                print("    ⏭️  Đã có trong DB — skip")
                card_index += 1
                continue

            raw = parse_job(driver, keyword, category)
            if not raw:
                card_index += 1
                continue

            if mode == "daily" and _is_old_linkedin(raw.get("job_posted_at", "")):
                print(f"  ⏹ Job cũ ({raw['job_posted_at']!r}) — dừng keyword")
                stop_keyword = True
                break

            # Lưu URL đã normalize vào seen_urls VÀ ghi đè vào raw
            seen_urls.add(normalized_url)
            raw["job_url"] = normalized_url

            enriched = _enrich_with_ai(raw)

            if enriched.get("job_description"):
                enriched["raw_about_job"] = ""

            ensure_db_connection(cur, conn)
            cleaned = clean_dict(enriched)

            # save_to_db trả về tuple (success: bool, status: str)
            ok, status = save_to_db(cur, conn, cleaned)

            if status == "new":
                count_new += 1
                print(f"    ✅ MỚI     [{count_new}] {cleaned.get('job_title')} @ {cleaned.get('company_title')}")
            elif status == "updated":
                count_updated += 1
                print(f"    🔄 UPDATED [{count_updated}] {cleaned.get('job_title')} @ {cleaned.get('company_title')}")
            elif status == "duplicate":
                print(f"    ⏭️  Không đổi — {cleaned.get('job_title')}")
            else:
                print(f"    ❌ {status.upper()} — {cleaned.get('job_title')}")

            card_index += 1

        if stop_keyword or count_new >= MAX_JOBS_PER_KEYWORD:
            break

        _, cards_before = _get_cards(driver)
        anchor = cards_before[0] if cards_before else None

        if not _click_next_page(driver):
            print("  📭 Hết trang")
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

    return count_new, count_updated


# =========================================================
#  Main
# =========================================================

def main():
    driver = init_driver()
    conn = cur = None

    def _exit(sig, frame):
        print("\n⛔ Ctrl+C — đóng kết nối...")
        try: driver.quit()
        except Exception: pass
        try:
            if cur: cur.close()
            if conn: conn.close()
        except Exception: pass
        sys.exit(0)

    signal.signal(signal.SIGINT, _exit)

    try:
        login(driver)
        conn, cur = get_db_connection()

        if args.mode == "daily":
            cur.execute(
                "SELECT job_url FROM jobs WHERE website='linkedin' "
                "AND scraped_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)"
            )
        else:
            cur.execute("SELECT job_url FROM jobs WHERE website='linkedin'")

        # FIX: normalize tất cả URL từ DB về dạng /jobs/view/{id}/
        # tránh mismatch khi LinkedIn trả về URL dạng /jobs/search/?currentJobId=...
        seen_urls = {_normalize_li_url(row[0]) for row in cur.fetchall()}
        print(f"✅ Loaded {len(seen_urls)} URL từ DB | mode={args.mode}")

        total_new     = 0
        total_updated = 0
        summary       = {}

        for category, keywords in KEYWORDS_BY_CATEGORY.items():
            summary[category] = {}
            for keyword in keywords:
                new, updated = scrape_keyword(
                    driver, keyword, category, seen_urls, cur, conn, args.mode
                )
                summary[category][keyword] = (new, updated)
                total_new     += new
                total_updated += updated
                delay = random.uniform(5, 10)
                print(f"  💤 Nghỉ {delay:.1f}s...")
                time.sleep(delay)

        print(f"\n{'='*55}")
        print(f"🎉 HOÀN THÀNH | mode={args.mode}")
        print(f"   ✅ Job mới    : {total_new}   ← KPI")
        print(f"   🔄 Job updated: {total_updated}")
        print(f"{'='*55}")
        for cat, kw_counts in summary.items():
            cat_new     = sum(v[0] for v in kw_counts.values())
            cat_updated = sum(v[1] for v in kw_counts.values())
            print(f"  [{cat}] mới={cat_new} updated={cat_updated}")
            for kw, (n, u) in kw_counts.items():
                print(f"    • {kw!r}: {n} mới, {u} updated")

    finally:
        try:
            if cur: cur.close()
            if conn: conn.close()
        except Exception: pass
        try: driver.quit()
        except Exception: pass


if __name__ == "__main__":
    main()