import time
import os
import json
import signal
from datetime import datetime
from urllib.parse import quote_plus
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

load_dotenv()

# ===== CONFIG =====
SEARCH_KEYWORD  = "data scientist"
MAX_JOBS        = 10
JOB_DETAIL_WAIT = 8
OUTPUT_FILE     = "linkedin_jobs.jsonl"

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
    (By.CSS_SELECTOR, 'button.jobs-search-pagination__button--next'),
    (By.XPATH,        '//button[contains(@aria-label,"next")]'),
]
SEL_PASSWORD  = [(By.ID, "password"), (By.XPATH, '//input[@type="password"]')]
SEL_LOGIN_BTN = [
    (By.XPATH, '//button[@type="submit"]'),
    (By.XPATH, '//button[contains(.,"Sign in")]'),
]


# =========================================================
#  Helpers
# =========================================================

def wait_any_css(driver, css_selectors, timeout=10):
    """Chờ đến khi bất kỳ CSS selector nào có element."""
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


def clean_text(driver, css_selector):
    """Lấy toàn bộ text từ một selector."""
    els = driver.find_elements(By.CSS_SELECTOR, css_selector + " *")
    texts = [e.text.strip() for e in els if e.text.strip()]
    return " ".join(texts)


def parse_job(driver):
    """Đọc dữ liệu job từ panel bên phải sau khi click card."""

    # Job title
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

    # About the job
    about_job = ""
    for sel in ["div#job-details", "div.jobs-description-content__text", "article.jobs-description"]:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els:
            about_job = els[0].text.strip()
            break

    # About the company
    about_company = ""
    for sel in ["section.jobs-company div.jobs-company__box", "div.jobs-company__box"]:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els:
            about_company = els[0].text.strip()
            break

    return {
        "website"          : "linkedin",
        "job_url"          : driver.current_url,
        "job_title"        : job_title,
        "raw_about_job"    : about_job,
        "raw_about_company": about_company,
        "scraped_at"       : datetime.now().isoformat(),
    }


# =========================================================
#  Main
# =========================================================

def init_driver():
    opts = Options()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=opts)
    driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    return driver


def login(driver):
    username = os.getenv("LINKEDIN_USERNAME")
    password = os.getenv("LINKEDIN_PASSWORD")
    if not username or not password:
        raise ValueError("❌ Thiếu LINKEDIN_USERNAME / LINKEDIN_PASSWORD trong .env")

    driver.get("https://www.linkedin.com/login")

    # Chờ trang login load xong — field username phải xuất hiện
    try:
        user_field = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "username"))
        )
    except Exception:
        raise Exception(f"❌ Trang login không load — title: {driver.title!r}, url: {driver.current_url}")

    user_field.clear()
    user_field.send_keys(username)

    pwd = find_first(driver, SEL_PASSWORD)
    if not pwd:
        raise Exception("❌ Không tìm thấy ô password")
    pwd.clear()
    pwd.send_keys(password)

    btn = find_first(driver, SEL_LOGIN_BTN)
    if not btn:
        raise Exception("❌ Không tìm thấy nút Sign in")
    btn.click()

    # Chờ thoát khỏi trang /login — tối đa 30 giây
    try:
        WebDriverWait(driver, 30).until(
            lambda d: "/login" not in d.current_url
        )
    except Exception:
        raise Exception(f"❌ Login timeout — URL hiện tại: {driver.current_url}")

    current = driver.current_url

    # LinkedIn yêu cầu xác minh thủ công
    if any(x in current for x in ["/checkpoint", "/challenge", "/uas/login"]):
        print(f"\n⚠️  LinkedIn yêu cầu xác minh bảo mật!")
        print(f"   URL: {current}")
        print(f"   → Hãy hoàn thành xác minh trên browser đang mở, rồi nhấn Enter ở đây...")
        input("   Nhấn Enter sau khi xác minh xong: ")

        # Kiểm tra lại sau khi user xác minh
        current = driver.current_url
        if "/login" in current or "/checkpoint" in current:
            raise Exception(f"❌ Vẫn chưa login được — URL: {current}")

    print(f"✅ Đăng nhập thành công — {driver.current_url}")


def scrape(driver):
    url = f"https://www.linkedin.com/jobs/search/?keywords={quote_plus(SEARCH_KEYWORD)}&refresh=true"
    driver.get(url)
    print(f"🔍 {url}")

    matched, _ = wait_any_css(driver, SEL_JOB_CARDS, timeout=15)
    if not matched:
        raise Exception("❌ Job list không load")

    job_count = 0
    page = 1
    results = []

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        while job_count < MAX_JOBS:
            print(f"\n📄 Trang {page} | {job_count}/{MAX_JOBS} job")

            matched_sel, cards = wait_any_css(driver, SEL_JOB_CARDS, timeout=12)
            if not cards:
                print("⚠️  Không tìm thấy card — dừng")
                break

            for card in cards:
                if job_count >= MAX_JOBS:
                    break

                try:
                    # Scroll để LinkedIn render card
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", card)
                    time.sleep(0.5)
                    card.click()
                    time.sleep(1.5)  # chờ panel phải load

                    # Chờ detail panel hiện ra
                    matched_detail, _ = wait_any_css(driver, SEL_DETAIL_LOADED, timeout=JOB_DETAIL_WAIT)
                    if not matched_detail:
                        print("⚠️  Detail không load — skip")
                        continue

                    item = parse_job(driver)
                    if not item:
                        print("⚠️  Không parse được job — skip")
                        continue

                    job_count += 1
                    print(f"  ✅ [{job_count}] {item['job_title']}")

                    # Ghi ngay ra file (không mất data nếu crash)
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")
                    f.flush()
                    results.append(item)

                except Exception as e:
                    print(f"  ⚠️  Skip card: {e}")

            if job_count >= MAX_JOBS:
                break

            # Next page
            next_btn = find_first(driver, SEL_NEXT_PAGE, timeout=5)
            if not next_btn:
                print("📭 Hết trang")
                break

            try:
                first_card = cards[0]
                next_btn.click()
                WebDriverWait(driver, 10).until(EC.staleness_of(first_card))
                time.sleep(2)
                page += 1
            except Exception as e:
                print(f"⚠️  Không qua được trang tiếp: {e}")
                break

    print(f"\n🎉 Hoàn thành: {job_count} job → {OUTPUT_FILE}")
    return results


def main():
    driver = init_driver()

    def _exit(sig, frame):
        print("\n⛔ Ctrl+C — đóng driver...")
        try:
            driver.service.process.kill()
        except Exception:
            pass
        os._exit(0)

    signal.signal(signal.SIGINT, _exit)

    try:
        login(driver)
        scrape(driver)
    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()