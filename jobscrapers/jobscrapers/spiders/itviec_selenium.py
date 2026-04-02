import time
import os
import json
import re
import signal
from datetime import datetime, date
from urllib.parse import quote_plus
from pathlib import Path

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException

# ===== CONFIG =====
MAX_JOBS_PER_KEYWORD = 20
OUTPUT_DIR  = os.path.join("data", "itviec")
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

# Trang danh sách — mỗi card có data attribute chứa slug
SEL_JOB_CARDS = [
    "div.job-card[data-search--job-selection-job-slug-value]",
    "div.job-card",
]

# Xác nhận trang detail đã load
SEL_DETAIL_LOADED = [
    "section.job-description",
    "div.job-details--content",
    "h1.text-it-black",
]

# Pagination — ITviec dùng thẻ <a> trong div.page.next
SEL_NEXT_PAGE = [
    (By.CSS_SELECTOR, "div.page.next a"),
    (By.CSS_SELECTOR, "a[rel='next']"),
    (By.CSS_SELECTOR, "a.next_page"),
]

# Login — Devise gem (Rails)
SEL_EMAIL     = [(By.ID, "user_email"), (By.XPATH, '//input[@type="email"]')]
SEL_PASSWORD  = [(By.ID, "user_password"), (By.XPATH, '//input[@type="password"]')]
SEL_LOGIN_BTN = [
    (By.CSS_SELECTOR, "button[type='submit']"),
    (By.CSS_SELECTOR, "input[type='submit']"),
    (By.XPATH,        '//button[contains(.,"Sign in")]'),
]

# Regex hỗ trợ parse
_NUM_RE   = re.compile(r"\d[\d,\.]*")
_MONEY_RE = re.compile(
    r"(?:[\$\€]?\s*[\d,\.]+\s*[-–]\s*[\d,\.]+\s*(?:USD|VND|triệu|million|K)?)"
    r"|(?:Negotiate|Thỏa thuận|Up to .+)",
    re.IGNORECASE,
)
_WORK_MODE_VALUES = {"on-site", "remote", "hybrid", "tại văn phòng", "làm từ xa"}
_JOB_TYPE_VALUES  = {
    "full-time", "part-time", "contract", "internship", "temporary",
    "toàn thời gian", "bán thời gian", "thực tập", "hợp đồng",
}


# =========================================================
#  Helpers (giữ nguyên pattern của LinkedIn scraper)
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


def load_seen_urls(filepath):
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


def get_text_first(driver, selectors):
    """Thử lần lượt các CSS selector, trả về text của element đầu tiên tìm thấy."""
    for sel in selectors:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els:
            t = safe_text(els[0])
            if t:
                return t
    return ""


def get_text_all(driver, selectors):
    """Trả về list text của tất cả element match selector đầu tiên có kết quả."""
    for sel in selectors:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els:
            return [t for t in (safe_text(e) for e in els) if t]
    return []


def get_employer_row(driver, label):
    """
    Lấy value từ bảng employer info bên phải.
    Cấu trúc HTML thực tế:
      <div class="row ipy-2 gx-0 border-bottom-dashed">
        <div class="col text-dark-grey">Company size</div>
        <div class="col text-end text-it-black">51-150 employees</div>
      </div>
    """
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
    Lấy text của div.paragraph chứa <h2> khớp heading_text.
    Cấu trúc HTML thực tế trong section.job-content:
      <div class="imy-5 paragraph">
        <h2>Job description</h2>
        <p>...</p>
      </div>
    """
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
#  Parse từng job detail page
#  — Selector căn chỉnh theo HTML thực tế của ITviec
# =========================================================

def parse_job(driver, keyword, category, list_meta):
    """
    Parse trang detail của một job.
    list_meta: dict các trường đã lấy được từ card ngoài danh sách
               (salary, work_mode, location, skills).
    Trả về dict đúng cấu trúc JobItem, hoặc None nếu không parse được.

    HTML structure (verified 2026-04):
      Left col  → class="col-xl-8"  : header + job-content
      Right col → class="col-xl-4"  : section.job-show-employer-info
    """

    # ── job_title ─────────────────────────────────────────
    # <h1 class="ipt-xl-6 text-it-black">Mid/Sr Frontend Developer...</h1>
    job_title = get_text_first(driver, [
        "h1.text-it-black",
        "h1",
    ])
    if not job_title:
        return None

    # ── company_title ──────────────────────────────────────
    # <div class="employer-name">GrapeCity</div>
    company_title = get_text_first(driver, [
        "div.employer-name",
        "div.job-header-info div.employer-name",
    ])

    # ── compensation ──────────────────────────────────────
    # <div class="d-flex align-items-center salary text-success-color">
    #   <span class="ips-2 fw-500">30,000,000 - 50,000,000đ</span>
    # Ưu tiên lấy từ meta card trước (vì card luôn có lương nếu public)
    compensation = list_meta.get("salary") or get_text_first(driver, [
        "div.salary span.fw-500",
        "div.salary",
    ])

    # ── location ──────────────────────────────────────────
    # <span class="normal-text text-rich-grey">Tầng 12B tòa nhà Machinco...</span>
    # (đứng sau icon map-pin, trong div.d-flex.flex-column.gap-2)
    location = list_meta.get("location") or get_text_first(driver, [
        "div.d-flex.flex-column.gap-2 > div:first-child span.normal-text",
        "div.job-show-info span.normal-text.text-rich-grey",
    ])

    # ── work_mode ─────────────────────────────────────────
    # <span class="normal-text text-rich-grey ms-1">At office</span>
    # Nằm trong div.preview-header-item đầu tiên (icon người)
    # div.preview-header-item thứ hai là posted-at (icon clock)
    work_mode = list_meta.get("work_mode", "")
    if not work_mode:
        preview_items = driver.find_elements(
            By.CSS_SELECTOR, "div.preview-header-item"
        )
        if preview_items:
            # item đầu tiên = work mode (At office / Remote / Hybrid)
            work_mode = safe_text(
                preview_items[0].find_element(By.CSS_SELECTOR, "span.normal-text")
            ) if preview_items[0].find_elements(By.CSS_SELECTOR, "span.normal-text") else ""

    # ── job_posted_at ─────────────────────────────────────
    # Ưu tiên lấy từ card meta (đã parse khi ở trang list)
    # Fallback: tìm trên trang detail
    job_posted_at = list_meta.get("job_posted_at", "")
    if not job_posted_at:
        preview_items = driver.find_elements(By.CSS_SELECTOR, "div.preview-header-item")
        if len(preview_items) >= 2:
            spans = preview_items[1].find_elements(By.CSS_SELECTOR, "span.normal-text")
            if spans:
                job_posted_at = safe_text(spans[0])

    # ── job_type ──────────────────────────────────────────
    # ITviec không hiển thị job_type riêng — mặc định Full-time
    job_type = "Full-time"

    # ── skills & job_category ─────────────────────────────
    # Job Expertise → job_category  (Frontend Developer, Backend Developer...)
    # Job Domain    → company_industry (Education and Training...)
    # Skills        → skills bonus field
    skills  = get_text_all(driver, ["a.itag.itag-light.itag-sm"])
    domain  = get_text_all(driver, ["div.itag.bg-light-grey.itag-sm"])

    # job_category = expertise từ card (ưu tiên) hoặc skills tags
    expertise_from_card = list_meta.get("job_expertise", "")
    job_category = (
        [expertise_from_card] if expertise_from_card
        else skills
        or list_meta.get("skills", [])
    )

    # company_industry = domain tag trên detail page (ưu tiên)
    # hoặc row "Company industry" từ bảng employer info bên phải

    # ── level ─────────────────────────────────────────────
    # Không có trường riêng trong HTML này — thử tìm trong title
    level = ""
    title_lower = job_title.lower()
    for kw in ["fresher", "junior", "mid", "senior", "lead", "manager",
               "director", "principal", "intern", "staff", "associate"]:
        if kw in title_lower:
            level = kw.capitalize()
            break

    # ── experience ────────────────────────────────────────
    # Không có field riêng — tìm text "X years" trong job_requirement
    experience = ""

    # ── education_level ───────────────────────────────────
    education_level = ""

    # ── number_recruit ────────────────────────────────────
    number_recruit = ""

    # ── job_deadline ──────────────────────────────────────
    job_deadline = ""

    # ── company_size ──────────────────────────────────────
    # <div class="col text-end text-it-black">51-150 employees</div>
    # (row có label "Company size" trong section.job-show-employer-info)
    company_size = get_employer_row(driver, "Company size")

    # ── company_industry ──────────────────────────────────
    # Ưu tiên: Job Domain tag trên detail page
    # <div class="itag bg-light-grey itag-sm cursor-default">Education and Training</div>
    # Fallback: row "Company industry" từ bảng employer info bên phải
    company_industry = (
        ", ".join(domain) if domain
        else get_employer_row(driver, "Company industry")
    )

    # ── job_description ───────────────────────────────────
    # section.job-content chứa nhiều div.paragraph, mỗi cái có <h2>
    # Lấy đúng paragraph "Job description"
    job_description = get_paragraph_by_heading(driver, "Job description")
    if not job_description:
        # Fallback: lấy toàn bộ section.job-content
        job_description = get_text_first(driver, ["section.job-content"])

    # ── job_requirement ───────────────────────────────────
    # Paragraph "Your skills and experience"
    job_requirement = get_paragraph_by_heading(driver, "Your skills and experience")
    if not job_requirement:
        job_requirement = get_paragraph_by_heading(driver, "skills and experience")

    # ── Trích experience từ job_requirement nếu chưa có ──
    if not experience and job_requirement:
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
        "job_type"        : job_type,
        "work_mode"       : work_mode,
        "level"           : level,
        "job_url"         : driver.current_url,
        "company_size"    : company_size,
        "company_industry": company_industry,
        "job_category"    : job_category,
        "number_recruit"  : number_recruit,
        "education_level" : education_level,
        "job_description" : job_description,
        "job_requirement" : job_requirement,
        "job_posted_at"   : job_posted_at,
        "job_deadline"    : job_deadline,
        "scraped_at"      : datetime.now().isoformat(),
        # bonus
        "search_keyword"  : keyword,
        "category"        : category,
        "skills"          : skills,
    }


# =========================================================
#  Lấy meta từ card ngoài danh sách
#  (nhanh hơn parse từ detail vì không cần navigate)
# =========================================================

def extract_card_meta(card):
    """
    Parse nhanh các trường có sẵn trên card listing.

    HTML card thực tế (verified 2026-04):

      <span class="small-text text-dark-grey">Posted 22 hours ago</span>
      <h3><a href="/it-jobs/{slug}...">Title</a></h3>
      <div class="salary ..."><span class="ips-2 fw-500">30,000,000đ</span></div>
      <a class="text-decoration-dot-underline small-text" title="Frontend Developer">...</a>
      <div class="text-rich-grey flex-shrink-0">Hybrid</div>          ← work_mode
      <div class="text-rich-grey text-truncate text-nowrap"
           title="Ho Chi Minh - Ha Noi">...</div>                     ← location (title attr)
      <a data-responsive-tag-list-target="tag">ReactJS</a>            ← skills
    """
    def css(sel):
        try:
            el = card.find_element(By.CSS_SELECTOR, sel)
            return safe_text(el)
        except Exception:
            return ""

    def attr(sel, attribute):
        """Lấy attribute thay vì text — dùng cho title không bị truncate."""
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

    # ── slug ─────────────────────────────────────────────────
    slug = card.get_attribute("data-search--job-selection-job-slug-value") or ""

    # ── job_posted_at ────────────────────────────────────────
    # <span class="small-text text-dark-grey">Posted 22 hours ago</span>
    job_posted_at = css("span.small-text.text-dark-grey")

    # ── salary ────────────────────────────────────────────────
    # <div class="salary ..."><span class="ips-2 fw-500">30,000,000 - 50,000,000đ</span>
    salary = css("div.salary span.fw-500") or css("div.salary")

    # ── job_expertise (job_category) ─────────────────────────
    # <a class="text-decoration-dot-underline small-text" title="Frontend Developer">
    # Dùng title attribute — text có thể bị truncate
    job_expertise = (
        attr("a.text-decoration-dot-underline.small-text", "title")
        or css("a.text-decoration-dot-underline.small-text")
    )

    # ── work_mode ─────────────────────────────────────────────
    # <div class="text-rich-grey flex-shrink-0">Hybrid</div>
    work_mode = css("div.text-rich-grey.flex-shrink-0")

    # ── location ──────────────────────────────────────────────
    # <div class="text-rich-grey text-truncate text-nowrap" title="Ho Chi Minh - Ha Noi">
    # Đọc title attribute để tránh text bị cắt bởi CSS text-truncate
    location = (
        attr("div.text-rich-grey.text-truncate.text-nowrap", "title")
        or css("div.text-rich-grey.text-truncate.text-nowrap")
        or css("div.text-rich-grey.text-truncate")
    )

    # ── skills ────────────────────────────────────────────────
    # <a data-responsive-tag-list-target="tag" href="/it-jobs/reactjs...">ReactJS</a>
    skills = css_all('a[data-responsive-tag-list-target="tag"]')

    return {
        "slug"         : slug,
        "job_posted_at": job_posted_at,
        "salary"       : salary,
        "job_expertise": job_expertise,
        "work_mode"    : work_mode,
        "location"     : location,
        "skills"       : skills,
    }


# =========================================================
#  Core scrape — một keyword
# =========================================================

def _get_cards(driver):
    matched_sel, cards = wait_any_css(driver, SEL_JOB_CARDS, timeout=12)
    return matched_sel, cards


def _click_next_page(driver):
    """
    ITviec pagination dùng thẻ <a href="..."> — lấy href rồi navigate trực tiếp.
    Tránh ElementClickIntercepted và vấn đề scroll.
    """
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


def scrape_keyword(driver, keyword, category, seen_urls, f):
    """
    Scrape tối đa MAX_JOBS_PER_KEYWORD job cho một keyword.
    ITviec dùng full-page navigation: click card → sang trang detail riêng.
    Chiến lược:
      1. Vào trang list, collect hết slug + meta của tất cả card trên trang.
      2. Navigate từng slug → parse detail → quay lại URL list.
    Không dùng driver.back() để tránh reload không ổn định; lưu URL list thay thế.
    """
    search_url = (
        f"https://itviec.com/it-jobs"
        f"?query={quote_plus(keyword)}"
    )
    driver.get(search_url)
    print(f"\n[{category}] {keyword!r} → {search_url}")

    matched, _ = wait_any_css(driver, SEL_JOB_CARDS, timeout=15)
    if not matched:
        print("  Job list không load — bỏ qua keyword này")
        return 0

    job_count = 0
    page      = 1
    current_list_url = driver.current_url  # giữ URL sau redirect/filter

    while job_count < MAX_JOBS_PER_KEYWORD:
        print(f"  📄 Trang {page} | đã scrape {job_count}/{MAX_JOBS_PER_KEYWORD}")

        # ── Bước A: Thu thập slug + meta từ tất cả card trên trang hiện tại ──
        _, cards = _get_cards(driver)
        if not cards:
            print("  Không tìm thấy card")
            break

        page_metas = []
        for card in cards:
            try:
                meta = extract_card_meta(card)
                if meta["slug"]:
                    page_metas.append(meta)
            except StaleElementReferenceException:
                continue

        if not page_metas:
            print("  Không lấy được slug từ card nào")
            break

        # ── Bước B: Navigate vào từng detail page ───────────────────────────
        for meta in page_metas:
            if job_count >= MAX_JOBS_PER_KEYWORD:
                break

            detail_url = f"https://itviec.com/it-jobs/{meta['slug']}"

            if detail_url in seen_urls:
                print(f"  ↩ Đã scrape — bỏ qua: {meta['slug']}")
                continue

            driver.get(detail_url)
            time.sleep(1.2)

            # Chờ trang detail load
            matched_detail, _ = wait_any_css(
                driver, SEL_DETAIL_LOADED, timeout=10
            )
            if not matched_detail:
                print(f"  ✗ Detail không load: {meta['slug']}")
                continue

            # Kiểm tra URL thực sau redirect (có thể khác slug)
            real_url = driver.current_url
            if real_url in seen_urls:
                print(f"  ↩ URL thực đã scrape: {real_url}")
                continue

            item = parse_job(driver, keyword, category, meta)
            if not item:
                print(f"  ✗ Parse thất bại: {meta['slug']}")
                continue

            seen_urls.add(real_url)
            job_count += 1
            print(f"  [{job_count}] {item['job_title']} @ {item['company_title']}")
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            f.flush()

            time.sleep(0.8)  # lịch sự với server

        if job_count >= MAX_JOBS_PER_KEYWORD:
            break

        # ── Bước C: Sang trang kế ────────────────────────────────────────────
        # Phải quay về trang list trước khi tìm nút Next
        driver.get(current_list_url)
        wait_any_css(driver, SEL_JOB_CARDS, timeout=10)
        time.sleep(0.5)

        if not _click_next_page(driver):
            print("  Hết trang — không tìm thấy nút Next")
            break

        wait_any_css(driver, SEL_JOB_CARDS, timeout=12)
        time.sleep(1)
        current_list_url = driver.current_url
        page += 1

    return job_count


# =========================================================
#  Driver  —  dùng undetected-chromedriver bypass bot check
# =========================================================

COOKIE_FILE = Path("data") / "itviec_cookies.json"


def init_driver():
    """
    undetected_chromedriver tự patch ChromeDriver binary để qua Cloudflare.
    Không cần add_argument disable-blink-features hay useAutomationExtension —
    uc đã xử lý hết bên trong.
    """
    opts = uc.ChromeOptions()
    opts.add_argument("--start-maximized")
    # Tắt load ảnh để tăng tốc (vẫn hoạt động với uc)
    prefs = {"profile.managed_default_content_settings.images": 2}
    opts.add_experimental_option("prefs", prefs)

    driver = uc.Chrome(options=opts, version_main=146)
    return driver


def _is_logged_in(driver):
    """Kiểm tra đã login chưa bằng cách tìm link sign_out trên navbar."""
    return bool(driver.find_elements(By.CSS_SELECTOR,
        "a[href*='sign_out'], div.header-user, nav .avatar-wrapper"))


def save_cookies(driver):
    """Lưu cookies ra file JSON sau khi login thành công."""
    COOKIE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(COOKIE_FILE, "w", encoding="utf-8") as f:
        json.dump(driver.get_cookies(), f, ensure_ascii=False, indent=2)
    print(f"✓ Đã lưu cookies → {COOKIE_FILE}")


def load_cookies(driver):
    """
    Load cookies từ file vào browser.
    Phải navigate đến domain trước khi add cookie (same-origin restriction).
    """
    if not COOKIE_FILE.exists():
        return False
    driver.get("https://itviec.com")
    time.sleep(1)
    with open(COOKIE_FILE, encoding="utf-8") as f:
        cookies = json.load(f)
    for cookie in cookies:
        # Bỏ các key Selenium không chấp nhận
        cookie.pop("sameSite", None)
        cookie.pop("expiry", None)
        try:
            driver.add_cookie(cookie)
        except Exception:
            pass
    driver.refresh()
    time.sleep(2)
    return True


# =========================================================
#  Login — cookie-based, tự động fallback sang login thủ công
# =========================================================

def login(driver):
    """
    Luồng login 2 bước:

    Lần đầu (chưa có cookie):
      → Mở itviec, in hướng dẫn, đợi user tự login trên browser
      → Sau khi user nhấn Enter → kiểm tra → lưu cookie

    Các lần sau (đã có cookie):
      → Load cookie → refresh → kiểm tra còn hợp lệ không
      → Nếu hết hạn → xóa file → chạy lại flow thủ công
    """
    # ── Thử load cookie trước ─────────────────────────────
    if COOKIE_FILE.exists():
        print("Tìm thấy cookie file — đang thử load...")
        load_cookies(driver)
        driver.get("https://itviec.com/it-jobs")
        time.sleep(2)
        if _is_logged_in(driver):
            print(f"✓ Đã login từ cookie — {driver.current_url}")
            return
        else:
            print("Cookie hết hạn — xóa và yêu cầu login lại")
            COOKIE_FILE.unlink(missing_ok=True)

    # ── Login thủ công (lần đầu hoặc cookie hết hạn) ──────
    driver.get("https://itviec.com/users/sign_in")
    print("\n" + "="*55)
    print("  Vui lòng ĐĂNG NHẬP THỦ CÔNG trên cửa sổ Chrome vừa mở.")
    print("  ITviec dùng Google OAuth / bot check — phải login bằng tay.")
    print("  Sau khi login xong và thấy trang chủ, quay lại đây nhấn Enter.")
    print("="*55)
    input("  >>> Nhấn Enter khi đã login xong: ")

    # Xác nhận
    driver.get("https://itviec.com/it-jobs")
    time.sleep(2)
    if not _is_logged_in(driver):
        raise Exception(
            "Vẫn chưa detect được trạng thái login. "
            "Hãy đảm bảo đã đăng nhập thành công rồi chạy lại."
        )

    save_cookies(driver)
    print(f"✓ Đăng nhập thành công — {driver.current_url}")


# =========================================================
#  Main
# =========================================================

def main():
    driver = init_driver()

    def _exit(sig, frame):
        print("\nĐóng driver...")
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
        print(f"\nFile hôm nay : {OUTPUT_FILE}")
        print(f"URL đã có    : {len(seen_urls)} — sẽ bỏ qua khi scrape")

        total   = 0
        summary = {}

        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            for category, keywords in KEYWORDS_BY_CATEGORY.items():
                summary[category] = {}
                for keyword in keywords:
                    count = scrape_keyword(driver, keyword, category, seen_urls, f)
                    summary[category][keyword] = count
                    total += count
                    time.sleep(3)  # nghỉ giữa keyword, tránh rate-limit

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