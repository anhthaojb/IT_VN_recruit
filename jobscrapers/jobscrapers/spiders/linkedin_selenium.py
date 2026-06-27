
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
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc

load_dotenv()

_project_root = str(pathlib.Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from jobscrapers.pipelines import RunTracker, clean_dict, save_to_db, get_db_connection
parser = argparse.ArgumentParser()
parser.add_argument("--mode", default="daily", choices=["daily", "full"])
args, unknown = parser.parse_known_args()
if unknown and unknown[0] in ["daily", "full"]:
    args.mode = unknown[0]

MAX_JOBS_PER_KEYWORD = 5
JOB_DETAIL_WAIT      = 12
MIN_ABOUT_JOB_CHARS  = 200
DAILY_MAX_AGE_DAYS   = 3
MAX_PAGES = 10
KEYWORDS_BY_CATEGORY = {
    "software_dev": ["backend developer", "frontend developer", "full stack developer"],
    "data":         ["data analyst", "data engineer","data scientist", "business intelligence"],
    "devops_cloud": ["devops engineer", "cloud engineer"],
    "security":     ["cybersecurity", "security engineer"],
    "ai_ml":        ["machine learning engineer", "AI engineer"],
}

COOKIE_FILE = pathlib.Path("data") / "linkedin_cookies.json"

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
LOC_PAT   = re.compile(r".+,.+")
NOISE_PAT = re.compile(
    r"people clicked apply|responses managed|actively recruiting",
    re.IGNORECASE,
)

def _normalize_li_url(url: str) -> str:
    if not url:
        return url
    m = re.search(r"/jobs/view/(\d+)", url)
    if m:
        return f"https://www.linkedin.com/jobs/view/{m.group(1)}/"
    parsed = urlparse(url)
    qs     = parse_qs(parsed.query)
    job_id = qs.get("currentJobId", [None])[0]
    if job_id:
        return f"https://www.linkedin.com/jobs/view/{job_id}/"
    return url.split("?")[0]


def safe_text(el):
    try:
        return el.text.strip()
    except StaleElementReferenceException:
        return None


def wait_any_css(driver, css_selectors, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        for sel in css_selectors:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els:
                return sel, els
        time.sleep(0.3)
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
    """
    [THAY ĐỔI 2] psycopg2 không có .ping() — dùng SELECT 1.
    Trả về (cur, conn) mới nếu connection chết.
    """
    try:
        cur.execute("SELECT 1")
        return cur, conn
    except Exception as e:
        print(f"  ⚠️  DB reconnect: {e}")
        try:
            conn.close()
        except Exception:
            pass
        conn_new, cur_new = get_db_connection()
        return cur_new, conn_new


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

def init_driver():
    opts = uc.ChromeOptions()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--lang=vi-VN,vi;q=0.9,en-US;q=0.8")
    
    driver = uc.Chrome(options=opts,version_main=149)
    driver.set_page_load_timeout(60)
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
            print(f"Tự điền thất bại ({e}) — chờ login thủ công")

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
        raise Exception(f" Chưa login được — URL: {driver.current_url}")
    save_cookies(driver)
    print(f" Đăng nhập thành công — {driver.current_url}")



def parse_job(driver, keyword, category):
    raw_about_job = ""
    job_title = ""
    for sel in [
        "h1.job-details-jobs-unified-top-card__job-title",
        "h1.t-24", "h1"
    ]:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els and els[0].text.strip():
            job_title = els[0].text.strip()
            break
    if not job_title:
        return None

    DESC_SELS = [
        "div#job-details",
        "div.jobs-description__content",
        "div.jobs-description-content__text",
        "article.jobs-description",
    ]
    try:
        WebDriverWait(driver, JOB_DETAIL_WAIT).until(
            lambda d: any(
                el.text.strip() and len(el.text.strip()) > 50
                for sel in DESC_SELS
                for el in d.find_elements(By.CSS_SELECTOR, sel)
            )
        )
    except Exception:
        pass

    for sel in DESC_SELS:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els:
            txt = els[0].text.strip()
            if len(txt) > 50:
                raw_about_job = txt
                break

    if not raw_about_job or len(raw_about_job) < MIN_ABOUT_JOB_CHARS:
        chars = len(raw_about_job) if raw_about_job else 0
        print(f"    ⚠️  Detail quá ngắn ({chars} chars) — skip")
        return None

    location = job_posted_at = None
    tertiary = driver.find_elements(
        By.CSS_SELECTOR,
        "div.job-details-jobs-unified-top-card__tertiary-description-container "
        "span.tvm__text",
    )
    tokens = [
        t for t in (safe_text(s) for s in tertiary)
        if t and t not in ("·", "") and not NOISE_PAT.search(t)
    ]
    for token in tokens:
        if not job_posted_at and TIME_PAT.search(token):
            job_posted_at = token
        elif not location and LOC_PAT.match(token) and not TIME_PAT.search(token):
            location = token

    work_mode = job_type = None
    WORK_MODE_VALUES = {"on-site", "remote", "hybrid"}
    JOB_TYPE_VALUES  = {"full-time", "part-time", "contract", "internship", "temporary"}
    pref_btns = driver.find_elements(
        By.CSS_SELECTOR,
        "div.job-details-fit-level-preferences button span.tvm__text",
    )
    for btn in pref_btns:
        text = safe_text(btn)
        if text:
            lower = text.lower()
            if not work_mode and any(m in lower for m in WORK_MODE_VALUES):
                work_mode = text
            elif not job_type and any(jt in lower for jt in JOB_TYPE_VALUES):
                job_type = text

    company_title = None
    els = driver.find_elements(
        By.CSS_SELECTOR, 'a[data-view-name="job-details-about-company-name-link"]'
    )
    if els:
        company_title = els[0].text.strip()

    company_industry = company_size = None
    info_divs = driver.find_elements(
        By.CSS_SELECTOR, "div.jobs-company__box div.t-14.mt5"
    )
    if info_divs:
        try:
            spans = info_divs[0].find_elements(
                By.CSS_SELECTOR, "span.jobs-company__inline-information"
            )
            if spans:
                company_size = spans[0].text.strip()
            try:
                raw_info = info_divs[0].text.strip()
            except StaleElementReferenceException:
                raw_info = ""
            for sp in spans:
                try:
                    raw_info = raw_info.replace(sp.text.strip(), "")
                except StaleElementReferenceException:
                    pass
            company_industry = raw_info.strip().strip("·").strip() or None
        except StaleElementReferenceException:
            pass  

    return {
        "website"         : "linkedin",
        "job_title"       : job_title,
        "company_title"   : company_title,
        "location"        : location,
        "job_url"         : driver.current_url,
        "job_posted_at"   : job_posted_at,
        "job_deadline"    : None,
        "work_mode"       : work_mode,
        "job_type"        : job_type,
        "company_size"    : company_size,
        "company_industry": company_industry,
        "job_category"    : keyword,
        "number_recruit"  : None,
        "job_description" : raw_about_job,
        "job_requirement" : None,
        "compensation"    : None,
        "level"           : None,
        "experience"      : None,
        "education_level" : None,
        "scraped_at"      : datetime.now().isoformat(),
        "_search_keyword" : keyword,
        "_category"       : category,
    }



def _get_cards(driver):
    return wait_any_css(driver, SEL_JOB_CARDS, timeout=12)


def _click_card_by_index(driver, index):
    for attempt in range(3):
        try:
            _, cards = _get_cards(driver)
            if index >= len(cards):
                return None
            card   = cards[index]
            job_id = (
                card.get_attribute("data-job-id")
                or card.get_attribute("data-occludable-job-id")
                or ""
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", card)
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", card)
            if job_id:
                try:
                    WebDriverWait(driver, 10).until(
                        lambda d: job_id in d.current_url
                        or job_id in d.page_source[:3000]
                    )
                except Exception:
                    pass
            return job_id or True
        except StaleElementReferenceException:
            time.sleep(0.5)
        except Exception:
            return None
    return None


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


def scrape_keyword(driver, keyword, category, seen_urls, cur, conn, mode, tracker):
    url = (
        f"https://www.linkedin.com/jobs/search/"
        f"?keywords={quote_plus(keyword)}&location=Vietnam"
    )
    try:
        driver.get(url)
    except TimeoutException:
        driver.execute_script("window.stop();")

    time.sleep(random.uniform(3, 6))
    print(f"\n [{category}] {keyword!r} | mode={mode}")

    current = driver.current_url
    if any(x in current for x in ["/login", "/checkpoint", "/authwall"]):
        print(f"   Bị redirect — thử login lại")
        login(driver)
        driver.get(url)
        time.sleep(random.uniform(3, 6))

    matched, _ = wait_any_css(driver, SEL_JOB_CARDS, timeout=15)
    if not matched:
        print("   Job list không load — bỏ qua")
        return 0, 0
    count_new = count_updated = 0
    page = 1
    stop_keyword = False

    while not stop_keyword:   
        if page > MAX_PAGES:
            print(f"  Đã đủ {MAX_PAGES} trang — dừng keyword")
            break
        print(f"  Trang {page} |  mới={count_new}  updated={count_updated}")

        _, cards_now = _get_cards(driver)
        if not cards_now:
            break
        total_cards = len(cards_now)
        card_index  = 0

        while card_index < total_cards:
            if stop_keyword:
                break

            clicked = _click_card_by_index(driver, card_index)
            if not clicked:
                _, cards_now = _get_cards(driver)
                total_cards  = len(cards_now)
                card_index  += 1
                continue

            matched_detail, _ = wait_any_css(
                driver, SEL_DETAIL_LOADED, timeout=JOB_DETAIL_WAIT
            )
            if not matched_detail:
                print(f"      Detail không load (card {card_index}) — skip")
                card_index += 1
                time.sleep(random.uniform(2, 5))
                continue

            normalized_url = _normalize_li_url(driver.current_url)
            if normalized_url in seen_urls:
                print("     Đã có trong DB — skip")
                card_index += 1
                continue

            raw = parse_job(driver, keyword, category)
            if not raw:
                card_index += 1
                continue
            if mode == "daily" and _is_old_linkedin(raw.get("job_posted_at", "")):
                print(f"    Job cũ ({raw['job_posted_at']!r}) — skip")
                card_index += 1
                continue
            seen_urls.add(normalized_url)
            raw["job_url"]         = normalized_url
            raw["job_requirement"] = None

            cur, conn = ensure_db_connection(cur, conn)
            cleaned = clean_dict(raw)
            ok, status = save_to_db(cur, conn, cleaned)
            tracker.record(status, cleaned)
            if status == "new":
                count_new += 1
                print(f"    MỚI [{count_new}] {cleaned.get('job_title')} @ {cleaned.get('company_title')}")
            elif status == "updated":
                count_updated += 1
                print(f"     UPDATED [{count_updated}] {cleaned.get('job_title')}")
            elif status == "duplicate":
                print("   Không đổi")
            else:
                print(f"  {status.upper()}")

            card_index += 1

        if stop_keyword:
            break

        _, cards_before = _get_cards(driver)
        anchor = cards_before[0] if cards_before else None
        if not _click_next_page(driver):
            print("  Hết trang")
            break

        try:
            if anchor:
                WebDriverWait(driver, 10).until(EC.staleness_of(anchor))
            wait_any_css(driver, SEL_JOB_CARDS, timeout=8)
            time.sleep(0.5)
        except Exception:
            time.sleep(2)

        page += 1

    return count_new, count_updated

def main():
    driver  = init_driver()
    conn = cur = None
    tracker  = None

    def _exit(sig, frame):
        print("\n Ctrl+C — đóng kết nối...")
        try:  driver.quit()
        except Exception: pass
        try:
            if cur:  cur.close()
            if conn: conn.close()
        except Exception: pass
        sys.exit(0)

    signal.signal(signal.SIGINT, _exit)

    try:
        login(driver)
        conn, cur = get_db_connection()
        tracker   = RunTracker(website="linkedin", cur=cur, conn=conn)

        if args.mode == "daily":
            cur.execute(
                "SELECT job_url FROM staging_jobs WHERE website='linkedin' "
                "AND scraped_at::timestamp >= NOW() - INTERVAL '30 days'"
            )
        else:
            cur.execute("SELECT job_url FROM staging_jobs WHERE website='linkedin'")

        seen_urls = {_normalize_li_url(row[0]) for row in cur.fetchall()}
        print(f" Loaded {len(seen_urls)} URL từ DB | mode={args.mode}")

        total_new = total_updated = 0
        summary   = {}

        for category, keywords in KEYWORDS_BY_CATEGORY.items():
            summary[category] = {}
            for keyword in keywords:
                try:
                    new, updated = scrape_keyword(
                        driver, keyword, category, seen_urls, cur, conn, args.mode, tracker
                    )
                except Exception as e:
                    if "invalid session" in str(e).lower() or "session deleted" in str(e).lower():
                        print(f"    Chrome crash — restart driver...")
                        try:
                            driver.service.process.kill()
                            driver.quit()
                        except Exception:
                            pass
                        driver = init_driver()
                        login(driver)
                        new, updated = 0, 0
                    else:
                        print(f"  keyword {keyword!r} lỗi: {e}")
                        new, updated = 0, 0
                
                summary[category][keyword] = (new, updated)
                total_new     += new
                total_updated += updated
                delay = random.uniform(10, 30)
                print(f"  💤 Nghỉ {delay:.1f}s...")
                time.sleep(delay)

        print(f"\n{'='*55}")
        print(f"🎉 HOÀN THÀNH | mode={args.mode}")
        print(f"   Job mới    : {total_new}")
        print(f"   Job updated: {total_updated}")
        print(f"{'='*55}")
        for cat, kw_counts in summary.items():
            cat_new     = sum(v[0] for v in kw_counts.values())
            cat_updated = sum(v[1] for v in kw_counts.values())
            print(f"  [{cat}] mới={cat_new} updated={cat_updated}")
            for kw, (n, u) in kw_counts.items():
                print(f"    • {kw!r}: {n} mới, {u} updated")

    finally:
        if tracker:
            try:  tracker.finish()
            except Exception as e:
                print(f"⚠ tracker.finish() lỗi: {e}")
        try:
            if cur:  cur.close()
            if conn: conn.close()
        except Exception: pass
        try:
            driver.service.process.kill()  
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    main()
