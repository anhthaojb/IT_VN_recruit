import sys
import os
import re
import time
import json
import random
import signal
import argparse
import pathlib
from datetime import datetime
from urllib.parse import quote_plus
from pathlib import Path
from dotenv import load_dotenv
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
load_dotenv()
_project_root = str(pathlib.Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from jobscrapers.pipelines import RunTracker, clean_dict, save_to_db, get_db_connection
# =========================================================
#  CONFIG
# =========================================================

MAX_JOBS_PER_KEYWORD = 5

KEYWORDS_BY_CATEGORY = {
    "software_dev": [
        "Backend Developer",
        "Frontend Developer",
        "Full Stack Developer",
        "Java Developer",
        "Python Developer",
        "PHP Developer",
        "NodeJS Developer",
        ".NET Developer",
        "C++ Developer",
        "Embedded Engineer",
    ],
    "mobile": [
        "Mobile Developer",
        "Android Developer",
        "iOS Developer",
    ],
    "architecture": [
        "Solution Architect",
        "System Administrator",
    ],
    "management": [
        "Business Analyst",
        "Product Owner",
        "Project Manager",
    ],
    "design_qa": [
        "UI UX Designer",
        "QA Tester",
    ],
    "data": [
        "Data Analyst",
        "Data Engineer",
        "Data Scientist",
        "Business Intelligence",
        "Database Administrator",
    ],
    "ai_ml": [
        "AI Engineer",
        "Machine Learning Engineer",
        "MLOps Engineer",
        "LLM Engineer",
    ],
}
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

def _is_old(posted_text: str, max_days: int = 3) -> bool:
    m = re.search(r"(\d+)\s+(day|week|month|year)s?\s+ago", posted_text, re.IGNORECASE)
    if not m:
        return False
    n, unit = int(m.group(1)), m.group(2).lower()
    age = {"day": n, "week": n*7, "month": n*30, "year": n*365}.get(unit, 0)
    return age > max_days

# =========================================================
#  DEBUG HELPER
# =========================================================

def debug_job_structure(driver):
    """In ra tất cả h2/h3 và section tìm thấy trên trang detail."""
    soup = BeautifulSoup(driver.page_source, "html.parser")

    print("\n===== DEBUG HEADINGS =====")
    for tag in soup.find_all(["h2", "h3"]):
        parent = tag.find_parent("div") or tag.find_parent("section")
        print(f"[{tag.name}] '{tag.get_text(strip=True)}'")
        print(f"  parent classes: {parent.get('class') if parent else 'N/A'}")
        print()

    print("===== ALL SECTIONS =====")
    for sec in soup.find_all("section"):
        print(f"section class={sec.get('class')}, first 80 chars: {sec.get_text()[:80]!r}")
    print("=========================\n")

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
    """
    Parse page_source bằng BeautifulSoup, tìm h2/h3 khớp heading_text,
    lấy toàn bộ nội dung của block cha chứa nó.
    """
    soup = BeautifulSoup(driver.page_source, "html.parser")

    for tag in soup.find_all(["h2", "h3"]):
        if heading_text.lower() not in tag.get_text(strip=True).lower():
            continue

        # Đi lên tìm div/section cha gần nhất có nội dung thực
        parent = tag.find_parent(["div", "section"])
        while parent:
            text = parent.get_text(separator="\n", strip=True)
            # Đủ dài và không phải toàn bộ trang
            if 50 < len(text) < 8000:
                return text
            parent = parent.find_parent(["div", "section"])

    return ""

# =========================================================
#  PARSE JOB DETAIL
# =========================================================

def parse_job(driver, keyword, category, list_meta):
    # ── Bật debug để xem HTML thực tế (tắt sau khi đã xác nhận selector) ──
    # debug_job_structure(driver)

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

    work_mode = list_meta.get("work_mode")
    if not work_mode:
        preview_items = driver.find_elements(
            By.CSS_SELECTOR, "div.preview-header-item"
        )
        if preview_items:
            spans = preview_items[0].find_elements(
                By.CSS_SELECTOR, "span.normal-text"
            )
            work_mode = safe_text(spans[0]) if spans else ""

    job_posted_at = list_meta.get("job_posted_at")
    if not job_posted_at:
        preview_items = driver.find_elements(
            By.CSS_SELECTOR, "div.preview-header-item"
        )
        if len(preview_items) >= 2:
            spans = preview_items[1].find_elements(
                By.CSS_SELECTOR, "span.normal-text"
            )
            job_posted_at = safe_text(spans[0]) if spans else ""

    skills = get_text_all(driver, ["a.itag.itag-light.itag-sm"])
    domain = get_text_all(driver, ["div.itag.bg-light-grey.itag-sm"])

    expertise_from_card = list_meta.get("job_expertise")
    job_category = (
        [expertise_from_card] if expertise_from_card
        else skills or list_meta.get("skills")
    )

    level = None
    for kw in ["fresher", "junior", "mid", "senior", "lead", "manager",
               "director", "principal", "intern", "staff", "associate"]:
        if kw in job_title.lower():
            level = kw.capitalize()
            break

    company_size = get_employer_row(driver, "Company size")
    company_industry = (
        ", ".join(domain) if domain
        else get_employer_row(driver, "Company industry")
    )

    # ── Job description ────────────────────────────────────────────────────
    job_description = get_paragraph_by_heading(driver, "Job description")
    if not job_description:
        job_description = get_paragraph_by_heading(driver, "Mô tả công việc")
    if not job_description:
        job_description = get_text_first(driver, [
            "section.job-content",
            "div.job-content",
        ])

    # ── Job requirement ────────────────────────────────────────────────────
    job_requirement = get_paragraph_by_heading(driver, "Your skills and experience")
    if not job_requirement:
        job_requirement = get_paragraph_by_heading(driver, "Skills and experience")
    if not job_requirement:
        job_requirement = get_paragraph_by_heading(driver, "Yêu cầu công việc")
    if not job_requirement:
        job_requirement = get_paragraph_by_heading(driver, "skills and experience")

    # ── In debug để kiểm tra kết quả ──────────────────────────────────────
    # print(f"\n  [DEBUG] job_description ({len(job_description)} chars): {job_description[:120]!r}")
    # print(f"  [DEBUG] job_requirement ({len(job_requirement)} chars): {job_requirement[:120]!r}")

    experience = None
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
        "job_type"        : "Full-time",
        "work_mode"       : work_mode,
        "level"           : level,
        "job_url"         : driver.current_url,
        "company_size"    : company_size,
        "company_industry": company_industry,
        "job_category"    : job_category,
        "number_recruit"  : None,
        "education_level" : None,
        "job_description" : job_description,
        "job_requirement" : job_requirement,
        "job_posted_at"   : job_posted_at,
        "job_deadline"    : None,
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


def scrape_keyword(driver, keyword, category, seen_urls, cur, conn, mode, tracker):
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

        for meta in page_metas:
            if job_count >= MAX_JOBS_PER_KEYWORD or stop_keyword:
                break

            detail_url = f"https://itviec.com/it-jobs/{meta['slug']}"
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
                print(f"  ↩ Đã cào trong session: {real_url}")
                continue

            raw = parse_job(driver, keyword, category, meta)
            if not raw:
                print(f"  ✗ Parse thất bại: {meta['slug']}")
                continue

            item  = clean_dict(raw)
            ok, status = save_to_db(cur, conn, item)
            tracker.record(status, item)

            seen_urls.add(real_url)
            if status == "new":
                job_count += 1

            status = "✅ mới" if status== "new" else "🔄 updated"
            print(f"  {status} [{job_count}] {item['job_title']} @ {item['company_title']}")

            time.sleep(0.8)

        if stop_keyword or job_count >= MAX_JOBS_PER_KEYWORD:
            break

        # driver.get(current_list_url)
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


def init_driver():
    opts = uc.ChromeOptions()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option(
        "prefs",
        {"profile.managed_default_content_settings.images": 2}
    )
    return uc.Chrome(options=opts)



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
            print(f"✓ Đã login từ cookie")
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
    print(f"✓ Đăng nhập thành công")

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
    tracker = RunTracker(website="itviec", cur=cur, conn=conn)

    try:
        login(driver)

        seen_urls = set()
        print("Session mới — sẽ INSERT mới hoặc UPDATE tất cả job tìm thấy")

        total   = 0
        summary = {}

        for category, keywords in KEYWORDS_BY_CATEGORY.items():
            summary[category] = {}
            for keyword in keywords:
                count = scrape_keyword(
                    driver, keyword, category, seen_urls, cur, conn, mode, tracker
                )
                summary[category][keyword] = count
                total += count
                time.sleep(3)

        print(f"\n{'='*55}")
        print(f"🎉 HOÀN THÀNH | mode={mode} | {total} job → PostgreSQL")
        print(f"{'='*55}")
        for cat, kw_counts in summary.items():
            cat_total = sum(kw_counts.values())
            print(f"  [{cat}] {cat_total} job")
            for kw, cnt in kw_counts.items():
                print(f"    • {kw!r}: {cnt}")

    finally:
        try:
            tracker.finish()
            cur.close()
            conn.close()
        except Exception:
            pass
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()