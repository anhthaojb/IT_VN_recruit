import time
import os
import re
import json
import signal
import argparse
from datetime import datetime, date
from urllib.parse import quote_plus
from pathlib import Path

import mysql.connector
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from jobscrapers.pipelines import clean_dict as clean_item, get_db_connection, save_to_db
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "jobscrapers"))

# =========================================================
#  CONFIG
# =========================================================

MAX_JOBS_PER_KEYWORD = 20

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

DB_CONFIG = dict(
    host     = "localhost",
    user     = "root",
    password = "123456",
    database = "itta",
    charset  = "utf8mb4",
)

COOKIE_FILE = Path("data") / "itviec_cookies.json"

# =========================================================
#  SELECTORS
# =========================================================

SEL_JOB_CARDS = [
    "div.job-card[data-search--job-selection-job-slug-value]",
    "div.job-card",
]
SEL_DETAIL_LOADED = [
    "section.job-description",
    "div.job-details--content",
    "h1.text-it-black",
]
SEL_NEXT_PAGE = [
    (By.CSS_SELECTOR, "div.page.next a"),
    (By.CSS_SELECTOR, "a[rel='next']"),
    (By.CSS_SELECTOR, "a.next_page"),
]

# =========================================================
#  KIỂM TRA NGÀY — dùng cho daily mode
# =========================================================

def _is_old(posted_text: str) -> bool:
    """
    Trả về True nếu job cũ hơn 1 ngày.

    ITviec dùng format tiếng Anh:
      "Posted 3 hours ago"   → còn mới → False
      "Posted 2 days ago"    → cũ      → True
      "Posted 1 week ago"    → cũ      → True
    """
    if not posted_text:
        return False
    return bool(re.search(
        r"\d+\s+(?:day|week|month|year)s?\s+ago",
        posted_text,
        re.IGNORECASE,
    ))

# =========================================================
#  MYSQL
# =========================================================

def get_db_connection():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id               INT          NOT NULL AUTO_INCREMENT,
            website          VARCHAR(50),
            job_title        TEXT,
            company_title    VARCHAR(255),
            location         VARCHAR(255),
            experience       VARCHAR(100),
            compensation     VARCHAR(255),
            job_type         VARCHAR(100),
            work_mode        VARCHAR(100),
            level            VARCHAR(100),
            job_url          VARCHAR(500)  UNIQUE,
            company_size     VARCHAR(100),
            company_industry VARCHAR(255),
            job_category     VARCHAR(255),
            number_recruit   VARCHAR(50),
            education_level  VARCHAR(100),
            job_description  LONGTEXT,
            job_requirement  LONGTEXT,
            job_posted_at    VARCHAR(100),
            job_deadline     VARCHAR(100),
            scraped_at       VARCHAR(50),
            is_valid         TINYINT(1)   DEFAULT 1,
            error_log        TEXT,
            PRIMARY KEY (id),
            INDEX idx_website    (website),
            INDEX idx_company    (company_title(100)),
            INDEX idx_location   (location(100)),
            INDEX idx_is_valid   (is_valid),
            INDEX idx_scraped_at (scraped_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    conn.commit()
    return conn, cur


def save_to_db(cur, conn, item: dict) -> bool:
    """
    INSERT IGNORE theo job_url.
    Trả về True nếu insert thành công, False nếu duplicate hoặc lỗi.
    """
    if not item.get("is_valid"):
        print(f"    ⚠ Invalid — bỏ qua: {item.get('error_log')}")
        return False

    try:
        cur.execute("""
            INSERT IGNORE INTO jobs (
                website, job_title, company_title, location,
                experience, compensation, job_type, work_mode,
                level, job_url, company_size, company_industry,
                job_category, number_recruit, education_level,
                job_description, job_requirement,
                job_posted_at, job_deadline, scraped_at,
                is_valid, error_log
            ) VALUES (
                %s,%s,%s,%s, %s,%s,%s,%s,
                %s,%s,%s,%s, %s,%s,%s,
                %s,%s, %s,%s,%s,
                %s,%s
            )
        """, (
            item.get("website"),
            item.get("job_title"),
            item.get("company_title"),
            item.get("location"),
            item.get("experience"),
            item.get("compensation"),
            item.get("job_type"),
            item.get("work_mode"),
            item.get("level"),
            item.get("job_url"),
            item.get("company_size"),
            item.get("company_industry"),
            item.get("job_category"),
            item.get("number_recruit"),
            item.get("education_level"),
            item.get("job_description"),
            item.get("job_requirement"),
            item.get("job_posted_at"),
            item.get("job_deadline"),
            item.get("scraped_at"),
            int(item.get("is_valid", True)),
            item.get("error_log"),
        ))
        conn.commit()
        return cur.rowcount > 0

    except mysql.connector.Error as e:
        print(f"    ✗ MySQL error: {e}")
        conn.rollback()
        return False

# =========================================================
#  SELENIUM HELPERS
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


def get_text_first(driver, selectors):
    for sel in selectors:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els:
            t = safe_text(els[0])
            if t:
                return t
    return ""


def get_text_all(driver, selectors):
    for sel in selectors:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els:
            return [t for t in (safe_text(e) for e in els) if t]
    return []


def get_employer_row(driver, label):
    rows = driver.find_elements(
        By.CSS_SELECTOR, "section.job-show-employer-info div.row"
    )
    for row in rows:
        cols = row.find_elements(By.CSS_SELECTOR, "div.col")
        if len(cols) >= 2 and label.lower() in safe_text(cols[0]).lower():
            return safe_text(cols[1])
    return ""


def get_paragraph_by_heading(driver, heading_text):
    try:
        el = driver.find_element(
            By.XPATH,
            f"//section[contains(@class,'job-content')]"
            f"//div[contains(@class,'paragraph')]"
            f"[.//h2[contains(text(),'{heading_text}')]]"
        )
        return safe_text(el)
    except Exception:
        return ""

# =========================================================
#  PARSE JOB DETAIL
# =========================================================

def parse_job(driver, keyword, category, list_meta):
    job_title = get_text_first(driver, ["h1.text-it-black", "h1"])
    if not job_title:
        return None

    company_title = get_text_first(driver, [
        "div.employer-name",
        "div.job-header-info div.employer-name",
    ])

    compensation = list_meta.get("salary") or get_text_first(driver, [
        "div.salary span.fw-500",
        "div.salary",
    ])

    location = list_meta.get("location") or get_text_first(driver, [
        "div.d-flex.flex-column.gap-2 > div:first-child span.normal-text",
        "div.job-show-info span.normal-text.text-rich-grey",
    ])

    work_mode = list_meta.get("work_mode", "")
    if not work_mode:
        preview_items = driver.find_elements(
            By.CSS_SELECTOR, "div.preview-header-item"
        )
        if preview_items:
            spans = preview_items[0].find_elements(
                By.CSS_SELECTOR, "span.normal-text"
            )
            work_mode = safe_text(spans[0]) if spans else ""

    job_posted_at = list_meta.get("job_posted_at", "")
    if not job_posted_at:
        preview_items = driver.find_elements(
            By.CSS_SELECTOR, "div.preview-header-item"
        )
        if len(preview_items) >= 2:
            spans = preview_items[1].find_elements(
                By.CSS_SELECTOR, "span.normal-text"
            )
            job_posted_at = safe_text(spans[0]) if spans else ""

    skills   = get_text_all(driver, ["a.itag.itag-light.itag-sm"])
    domain   = get_text_all(driver, ["div.itag.bg-light-grey.itag-sm"])

    expertise_from_card = list_meta.get("job_expertise", "")
    job_category = (
        [expertise_from_card] if expertise_from_card
        else skills or list_meta.get("skills", [])
    )

    level = ""
    for kw in ["fresher", "junior", "mid", "senior", "lead", "manager",
               "director", "principal", "intern", "staff", "associate"]:
        if kw in job_title.lower():
            level = kw.capitalize()
            break

    company_size     = get_employer_row(driver, "Company size")
    company_industry = (
        ", ".join(domain) if domain
        else get_employer_row(driver, "Company industry")
    )

    job_description = get_paragraph_by_heading(driver, "Job description")
    if not job_description:
        job_description = get_text_first(driver, ["section.job-content"])

    job_requirement = get_paragraph_by_heading(driver, "Your skills and experience")
    if not job_requirement:
        job_requirement = get_paragraph_by_heading(driver, "skills and experience")

    experience = ""
    if job_requirement:
        m = re.search(
            r"(\d+)\+?\s*(?:năm|year)s?\s*(?:of\s*)?(?:kinh nghiệm|experience)",
            job_requirement, re.IGNORECASE,
        )
        if m:
            experience = m.group(0)

    return {
        "website"         : "itviec",
        "job_title"       : job_title,
        "company_title"   : company_title,
        "location"        : location,
        "experience"      : experience,
        "compensation"    : compensation,
        "job_type"        : "",
        "work_mode"       : work_mode,
        "level"           : level,
        "job_url"         : driver.current_url,
        "company_size"    : company_size,
        "company_industry": company_industry,
        "job_category"    : job_category,
        "number_recruit"  : "",
        "education_level" : "",
        "job_description" : job_description,
        "job_requirement" : job_requirement,
        "job_posted_at"   : job_posted_at,
        "job_deadline"    : "",
        "scraped_at"      : datetime.now().isoformat(),
        "skills"          : skills,
        "_search_keyword" : keyword,
    }

# =========================================================
#  PARSE CARD META
# =========================================================

def extract_card_meta(card):
    def css(sel):
        try:
            return safe_text(card.find_element(By.CSS_SELECTOR, sel))
        except Exception:
            return ""

    def attr(sel, attribute):
        try:
            el = card.find_element(By.CSS_SELECTOR, sel)
            return (el.get_attribute(attribute) or "").strip()
        except Exception:
            return ""

    def css_all(sel):
        try:
            els = card.find_elements(By.CSS_SELECTOR, sel)
            return [t for t in (safe_text(e) for e in els) if t]
        except Exception:
            return []

    return {
        "slug"         : card.get_attribute(
            "data-search--job-selection-job-slug-value"
        ) or "",
        "job_posted_at": css("span.small-text.text-dark-grey"),
        "salary"       : css("div.salary span.fw-500") or css("div.salary"),
        "job_expertise": (
            attr("a.text-decoration-dot-underline.small-text", "title")
            or css("a.text-decoration-dot-underline.small-text")
        ),
        "work_mode"    : css("div.text-rich-grey.flex-shrink-0"),
        "location"     : (
            attr("div.text-rich-grey.text-truncate.text-nowrap", "title")
            or css("div.text-rich-grey.text-truncate.text-nowrap")
        ),
        "skills"       : css_all('a[data-responsive-tag-list-target="tag"]'),
    }

# =========================================================
#  CORE SCRAPE — một keyword
# =========================================================

def _click_next_page(driver):
    next_el = find_first(driver, SEL_NEXT_PAGE, timeout=5)
    if not next_el:
        return False
    href = next_el.get_attribute("href")
    if href:
        driver.get(href)
        return True
    try:
        driver.execute_script("arguments[0].click();", next_el)
        return True
    except Exception:
        return False


def scrape_keyword(driver, keyword, category, seen_urls, cur, conn, mode):
    search_url = f"https://itviec.com/it-jobs?query={quote_plus(keyword)}"
    driver.get(search_url)
    print(f"\n[{category}] {keyword!r}")

    matched, _ = wait_any_css(driver, SEL_JOB_CARDS, timeout=15)
    if not matched:
        print("  ⚠ Job list không load — bỏ qua")
        return 0

    job_count        = 0
    page             = 1
    current_list_url = driver.current_url
    stop_keyword     = False

    while job_count < MAX_JOBS_PER_KEYWORD and not stop_keyword:
        print(f"  📄 Trang {page} | {job_count}/{MAX_JOBS_PER_KEYWORD}")

        _, cards = wait_any_css(driver, SEL_JOB_CARDS, timeout=12)
        if not cards:
            print("  ⚠ Không tìm thấy card")
            break

        # ── Thu thập meta từ tất cả card trên trang ───────────────────
        page_metas = []
        for card in cards:
            try:
                meta = extract_card_meta(card)
                if not meta["slug"]:
                    continue

                if mode == "daily" and _is_old(meta["job_posted_at"]):
                    print(
                        f"  ⏹ [{mode}] Gặp job cũ "
                        f"({meta['job_posted_at']!r}) — dừng keyword này"
                    )
                    stop_keyword = True
                    break

                page_metas.append(meta)
            except StaleElementReferenceException:
                continue

        # ── Navigate vào từng detail page ─────────────────────────────
        for meta in page_metas:
            if job_count >= MAX_JOBS_PER_KEYWORD or stop_keyword:
                break

            detail_url = f"https://itviec.com/it-jobs/{meta['slug']}"
            if detail_url in seen_urls:
                print(f"  ↩ Đã scrape — bỏ qua: {meta['slug']}")
                continue

            driver.get(detail_url)
            time.sleep(1.2)

            matched_detail, _ = wait_any_css(
                driver, SEL_DETAIL_LOADED, timeout=10
            )
            if not matched_detail:
                print(f"  ✗ Detail không load: {meta['slug']}")
                continue

            real_url = driver.current_url
            if real_url in seen_urls:
                print(f"  ↩ URL thực đã scrape: {real_url}")
                continue

            raw  = parse_job(driver, keyword, category, meta)
            if not raw:
                print(f"  ✗ Parse thất bại: {meta['slug']}")
                continue

            item  = clean_item(raw)   # ← dùng selenium_pipelines
            saved = save_to_db(cur, conn, item)

            seen_urls.add(real_url)
            job_count += 1

            status = "✅" if saved else "↩ dup"
            print(f"  {status} [{job_count}] {item['job_title']} @ {item['company_title']}")

            time.sleep(0.8)

        if stop_keyword or job_count >= MAX_JOBS_PER_KEYWORD:
            break

        # ── Sang trang kế ─────────────────────────────────────────────
        driver.get(current_list_url)
        wait_any_css(driver, SEL_JOB_CARDS, timeout=10)
        time.sleep(0.5)

        if not _click_next_page(driver):
            print("  📭 Hết trang")
            break

        wait_any_css(driver, SEL_JOB_CARDS, timeout=12)
        time.sleep(1)
        current_list_url = driver.current_url
        page += 1

    return job_count

# =========================================================
#  DRIVER & LOGIN
# =========================================================

def init_driver():
    opts = uc.ChromeOptions()
    opts.add_argument("--start-maximized")
    opts.add_experimental_option(
        "prefs",
        {"profile.managed_default_content_settings.images": 2}
    )
    return uc.Chrome(options=opts, version_main=146)
    


def _is_logged_in(driver):
    return bool(driver.find_elements(
        By.CSS_SELECTOR,
        "a[href*='sign_out'], div.header-user, nav .avatar-wrapper"
    ))


def save_cookies(driver):
    COOKIE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(COOKIE_FILE, "w", encoding="utf-8") as f:
        json.dump(driver.get_cookies(), f, ensure_ascii=False, indent=2)
    print(f"✓ Đã lưu cookies → {COOKIE_FILE}")


def load_cookies(driver):
    if not COOKIE_FILE.exists():
        return False
    driver.get("https://itviec.com")
    time.sleep(1)
    with open(COOKIE_FILE, encoding="utf-8") as f:
        cookies = json.load(f)
    for cookie in cookies:
        cookie.pop("sameSite", None)
        cookie.pop("expiry", None)
        try:
            driver.add_cookie(cookie)
        except Exception:
            pass
    driver.refresh()
    time.sleep(2)
    return True


def login(driver):
    if COOKIE_FILE.exists():
        print("Tìm thấy cookie — đang thử load...")
        load_cookies(driver)
        driver.get("https://itviec.com/it-jobs")
        time.sleep(2)
        if _is_logged_in(driver):
            print("✓ Đã login từ cookie")
            return
        print("Cookie hết hạn — yêu cầu login lại")
        COOKIE_FILE.unlink(missing_ok=True)

    driver.get("https://itviec.com/sign_in")
    print("\n" + "="*55)
    print("  Vui lòng ĐĂNG NHẬP THỦ CÔNG trên Chrome vừa mở.")
    print("  Sau khi login xong, quay lại đây nhấn Enter.")
    print("="*55)
    input("  >>> Nhấn Enter khi đã login xong: ")

    driver.get("https://itviec.com/it-jobs")
    time.sleep(2)
    if not _is_logged_in(driver):
        raise Exception("Chưa detect được trạng thái login.")

    save_cookies(driver)
    print("✓ Đăng nhập thành công")

# =========================================================
#  MAIN
# =========================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        default="daily",
        choices=["full", "daily"],
        help="full = cào hết, daily = chỉ job mới trong 24h (mặc định)",
    )
    args = parser.parse_args()
    mode = args.mode

    print(f"\n{'='*55}")
    print(f"🚀 ITviec scraper | mode={mode}")
    print(f"{'='*55}")

    driver = init_driver()

    def _exit(sig, frame):
        print("\n⛔ Ctrl+C — đóng driver...")
        try:
            driver.service.process.kill()
        except Exception:
            pass
        os._exit(0)

    signal.signal(signal.SIGINT, _exit)

    conn, cur = get_db_connection()

    try:
        login(driver)

        cur.execute("SELECT job_url FROM jobs WHERE website='itviec'")
        seen_urls = {row[0] for row in cur.fetchall()}
        print(f"URL đã có trong DB: {len(seen_urls)}")

        total   = 0
        summary = {}

        for category, keywords in KEYWORDS_BY_CATEGORY.items():
            summary[category] = {}
            for keyword in keywords:
                count = scrape_keyword(
                    driver, keyword, category, seen_urls, cur, conn, mode
                )
                summary[category][keyword] = count
                total += count
                time.sleep(3)

        print(f"\n{'='*55}")
        print(f"🎉 HOÀN THÀNH | mode={mode} | {total} job mới → MySQL")
        print(f"{'='*55}")
        for cat, kw_counts in summary.items():
            cat_total = sum(kw_counts.values())
            print(f"  [{cat}] {cat_total} job")
            for kw, cnt in kw_counts.items():
                print(f"    • {kw!r}: {cnt}")

    finally:
        cur.close()
        conn.close()
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()