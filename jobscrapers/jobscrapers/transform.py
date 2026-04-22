# etl.py  (transform.py)
# ==============================================================================
# RECRUITMENT ETL — Daily batch processor
#
# Input : bảng jobs (24 cột thô từ Scrapy)
# Output: 3 bảng
#   ├── fact_jobs_etl   — toàn bộ cột cũ + cột mới đã xử lý
#   ├── fact_etl_log    — log kết quả mỗi lần chạy ETL
#   └── fact_etl_error  — chi tiết lỗi từng cột/row để debug
#
# Chạy:
#   python etl.py                    → xử lý jobs scraped hôm nay
#   python etl.py --all              → xử lý toàn bộ bảng jobs
#   python etl.py --date 2026-04-18  → ngày cụ thể
# ==============================================================================

import re
import argparse
import numpy as np
import pandas as pd
import sqlalchemy
from datetime import datetime, timedelta, date

from lookups import (
    # GEO
    PROVINCE_CANONICAL, GEO_KEYS_SORTED, REGION_MAP,
    FOREIGN_KW,
    # SALARY
    CURRENCY_RULES, NEGOTIABLE_KW,
    # EXPERIENCE
    NO_EXP_KW,
    # JOB CLASSIFICATION
    LEVEL_MAP, EXP_TO_LEVEL,
    EDUCATION_MAP,
    INDUSTRY_TREE,
    COMPANY_TYPE_PATTERNS, COMPANY_TYPE_STRIP,
    JOB_CATEGORY_MAP, IT_TITLES,
    JOB_TITLE_MAP,
    MAJOR_MAP,
    CERT_KW, LANG_CERT_KW, LANG_CERT_TO_LANG,
    SKILL_MAP,
    WORK_TYPE_MAP, WORK_MODE_MAP,
    ROLE_WORDS, TECH_DOMAIN, ROLE_DOMAIN_TO_TITLE,
)

# ==============================================================================
# 0. CONFIG
# ==============================================================================
try:
    from pipelines import DB_CONFIG as _DB
    DATABASE_URL = (
        f"mysql+mysqlconnector://{_DB['user']}:{_DB['password']}"
        f"@{_DB['host']}/{_DB['database']}?charset=utf8mb4"
    )
except Exception:
    DATABASE_URL = "mysql+mysqlconnector://root:123456@localhost/itta?charset=utf8mb4"

SRC_TABLE   = "jobs"
FACT_TABLE  = "fact_jobs_etl"
LOG_TABLE   = "fact_etl_log"
ERROR_TABLE = "fact_etl_error"

# ==============================================================================
# 1. DDL
# ==============================================================================

_DDL_FACT = f"""
CREATE TABLE IF NOT EXISTS {FACT_TABLE} (
    etl_id                  BIGINT        NOT NULL AUTO_INCREMENT,
    src_id                  INT,
    job_url                 VARCHAR(500) ,
    scraped_at              VARCHAR(30),
    etl_run_id              INT,
    etl_processed_at        DATETIME,
    website                 VARCHAR(50),
    website_clean           VARCHAR(50),
    job_posted_at           VARCHAR(50),
    job_deadline            VARCHAR(50),
    job_posted_at_clean     DATE,
    job_deadline_clean      DATE,
    job_title               TEXT,
    job_title_clean         VARCHAR(150),
    job_category_clean      VARCHAR(100),
    is_it                   TINYINT(1),
    company_title           VARCHAR(255),
    company_title_clean     VARCHAR(255),
    company_type            VARCHAR(50),
    location                VARCHAR(255),
    location_province       VARCHAR(100),
    location_region         VARCHAR(30),
    is_vn                   TINYINT(1),
    job_type                VARCHAR(100),
    work_mode               VARCHAR(100),
    job_type_clean          VARCHAR(30),
    work_mode_clean         VARCHAR(30),
    compensation            VARCHAR(255),
    salary_min              BIGINT,
    salary_max              BIGINT,
    salary_currency         VARCHAR(10),
    conversion_rate         DOUBLE,
    is_negotiable           TINYINT(1)  DEFAULT 1,
    experience              VARCHAR(100),
    exp_min_yr              FLOAT,
    exp_max_yr              FLOAT,
    is_exp_required         TINYINT(1),
    level                   VARCHAR(100),
    level_clean             VARCHAR(30),
    job_description         LONGTEXT,
    job_requirement         LONGTEXT,
    raw_about_job           LONGTEXT,
    hard_skills             TEXT,
    soft_skills             TEXT,
    major                   VARCHAR(255),
    certifications          TEXT,
    languages               TEXT,
    education_level         VARCHAR(100),
    education_clean         VARCHAR(30),
    company_size            VARCHAR(100),
    company_size_min        INT,
    company_size_max        INT,
    company_industry        VARCHAR(255),
    industry_level1         VARCHAR(100),
    industry_level2         VARCHAR(10),
    industry_level3         VARCHAR(100),
    job_category            VARCHAR(255),
    number_recruit          VARCHAR(50),
    number_recruit_clean    SMALLINT UNSIGNED,
    is_valid                TINYINT(1)  DEFAULT 1,
    error_log               TEXT,
    PRIMARY KEY (etl_id),
    UNIQUE KEY uq_url_province (job_url(200), location_province(100)),
    INDEX idx_run       (etl_run_id),
    INDEX idx_website   (website_clean),
    INDEX idx_province  (location_province),
    INDEX idx_region    (location_region),
    INDEX idx_posted    (job_posted_at_clean),
    INDEX idx_scraped   (scraped_at(20))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

_DDL_LOG = f"""
CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
    run_id          INT       NOT NULL AUTO_INCREMENT,
    run_date        DATE,
    mode            VARCHAR(20),
    target_date     DATE,
    started_at      DATETIME,
    finished_at     DATETIME,
    duration_sec    INT,
    total_input     INT  DEFAULT 0,
    total_output    INT  DEFAULT 0,
    new_rows        INT  DEFAULT 0,
    updated_rows    INT  DEFAULT 0,
    error_rows      INT  DEFAULT 0,
    status          VARCHAR(20),
    note            TEXT,
    PRIMARY KEY (run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

_DDL_ERROR = f"""
CREATE TABLE IF NOT EXISTS {ERROR_TABLE} (
    error_id        BIGINT    NOT NULL AUTO_INCREMENT,
    run_id          INT,
    src_id          INT,
    job_url         VARCHAR(500),
    field_name      VARCHAR(100),
    raw_value       TEXT,
    error_type      VARCHAR(50),
    error_detail    TEXT,
    created_at      DATETIME  DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (error_id),
    INDEX idx_run   (run_id),
    INDEX idx_src   (src_id),
    INDEX idx_field (field_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

# ==============================================================================
# 2. HELPERS
# ==============================================================================

def _s(val) -> str:
    """Giá trị → string sạch, bỏ nan/None."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return ""
    return str(val).strip()


# ── 2.1 Website ───────────────────────────────────────────────────────────────

def parse_website(raw: str) -> str:
    return _s(raw).lower()


# ── 2.2 Dates ─────────────────────────────────────────────────────────────────

def _to_date(text: str, ref_iso: str) -> date | None:
    s = _s(text).lower()
    s = re.sub(r"\(.*?\)", "", s).strip()
    if not s:
        return None

    # DD/MM/YYYY hoặc DD-MM-YYYY
    m = re.match(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", s)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass

    # YYYY-MM-DD hoặc YYYY/MM/DD
    m = re.match(r"(\d{4})[/\-](\d{2})[/\-](\d{2})", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # "còn N ngày"
    m = re.search(r"còn\s+(\d+)\s+ngày", s)
    if m:
        try:
            ref = datetime.fromisoformat(ref_iso.replace(" ", "T"))
            return (ref + timedelta(days=int(m.group(1)))).date()
        except Exception:
            pass

    # "N unit ago"
    m = re.search(r"(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago", s)
    if m:
        try:
            ref = datetime.fromisoformat(ref_iso.replace(" ", "T"))
            n, unit = int(m.group(1)), m.group(2)
            delta_map = {
                "second": timedelta(seconds=n), "minute": timedelta(minutes=n),
                "hour":   timedelta(hours=n),   "day":    timedelta(days=n),
                "week":   timedelta(weeks=n),   "month":  timedelta(days=n * 30),
                "year":   timedelta(days=n * 365),
            }
            return (ref - delta_map[unit]).date()
        except Exception:
            pass

    return None


def parse_dates(posted_raw: str, deadline_raw: str, scraped_at: str,
                job_desc: str = "") -> dict:
    ref      = _s(scraped_at)
    posted   = _to_date(posted_raw,   ref)
    deadline = _to_date(deadline_raw, ref)

    if deadline is None and job_desc:
        desc_lower = _s(job_desc).lower()
        for pat in [
            r"hạn\s+nộp[:\s]+(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})",
            r"deadline[:\s]+(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})",
            r"ngày\s+hết\s+hạn[:\s]+(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})",
            r"apply\s+before[:\s]+(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})",
        ]:
            m = re.search(pat, desc_lower)
            if m:
                deadline = _to_date(m.group(1), ref)
                if deadline:
                    break

    if posted is None:
        posted = deadline
    if posted is None and ref:
        try:
            posted = datetime.fromisoformat(ref.replace(" ", "T")).date()
        except Exception:
            pass

    return {"job_posted_at_clean": posted, "job_deadline_clean": deadline}


# ── 2.3 Job title ─────────────────────────────────────────────────────────────

# FIX: Lấy danh sách tất cả hard-skill keywords từ SKILL_MAP để strip khỏi title
def _build_skill_noise_pattern() -> re.Pattern:
    """
    Ghép tất cả keyword từ SKILL_MAP["hard"] thành một regex để strip
    các cụm skill/tech ra khỏi job title.
    """
    all_kws = []
    for kws in SKILL_MAP["hard"].values():
        for kw in kws:
            # Chỉ lấy keyword dài >= 2 ký tự, không phải regex phức tạp
            if len(kw) >= 2 and not any(c in kw for c in r"\^$[]{}|?"):
                all_kws.append(re.escape(kw))
    # Sắp xếp dài → ngắn để match greedy
    all_kws.sort(key=len, reverse=True)
    pattern = r"(?i)\b(?:" + "|".join(all_kws) + r")\b"
    return re.compile(pattern, re.IGNORECASE)

# Noise patterns cần strip khỏi job title
# Keywords NON-IT category để map từ job_category raw
_NON_IT_CATEGORY_MAP: dict[str, list[str]] = {
    "Finance":          ["kế toán", "tài chính", "accountant", "finance", "kiểm toán",
                         "audit", "thuế", "tax", "ngân hàng", "banking", "chứng khoán"],
    "HR":               ["nhân sự", "human resource", "hr", "tuyển dụng", "recruiter",
                         "recruitment", "c&b", "hrbp", "training"],
    "Admin":            ["hành chính", "administrative", "admin", "văn phòng", "office",
                         "executive assistant", "thư ký", "secretary"],
    "Marketing":        ["marketing", "truyền thông", "brand", "digital marketing",
                         "seo", "content", "pr ", "public relation"],
    "Sales":            ["kinh doanh", "sales", "bán hàng", "business development",
                         "account manager", "account executive"],
    "Customer Service": ["chăm sóc khách hàng", "customer service", "customer support",
                         "helpdesk", "after sales", "dịch vụ khách hàng"],
    "Logistics":        ["logistics", "chuỗi cung ứng", "supply chain", "kho vận",
                         "xuất nhập khẩu", "import export", "vận tải", "warehouse"],
    "Manufacturing":    ["sản xuất", "manufacturing", "kỹ thuật", "chất lượng",
                         "quality", "qc", "qa ", "an toàn", "safety", "bảo trì",
                         "maintenance", "vận hành", "operator"],
    "Legal":            ["pháp lý", "luật", "legal", "compliance", "hợp đồng", "contract"],
    "Education":        ["giáo dục", "giảng dạy", "giáo viên", "teacher", "training",
                         "đào tạo", "education"],
    "Healthcare":       ["y tế", "dược", "bác sĩ", "y tá", "healthcare", "pharma",
                         "nurse", "doctor", "medical"],
    "Construction":     ["xây dựng", "construction", "kiến trúc", "architecture",
                         "bất động sản", "real estate", "cơ điện", "mep"],
    "Design":           ["thiết kế", "design", "đồ họa", "graphic", "interior",
                         "nội thất"],
    "Other":            [],
}
 
# IT keywords ngắn dùng để detect is_it khi không map được title
_IT_EXTRA_KW: list[str] = [
    "phần mềm", "lập trình", "kỹ thuật phần mềm", "công nghệ thông tin",
    "hệ thống", "mạng máy tính", "an ninh mạng", "bảo mật",
    "it ", " it,", "etl", "elt", "data", "cloud", "devops", "sre",
    "ci/cd", "api", "microservice", "blockchain", "web3", "embedded",
    "firmware", "iot", "plc", "scada", "sap", "erp", "odoo",
    "salesforce", "figma", "ui/ux", "power bi", "tableau",
    "looker", "bigquery",
]
 
# Hard-coded IT title keywords (fallback khi JOB_TITLE_MAP không load được)
_IT_TITLE_KW_FALLBACK: list[str] = [
    "software", "developer", "engineer", "programmer", "coder",
    "data", "ai ", "machine learning", "deep learning", "ml ",
    "devops", "sre", "cloud", "backend", "front end", "frontend",
    "fullstack", "full stack", "mobile", "android", "ios",
    "database", "dba", "network", "system admin", "sysadmin",
    "security", "cyber", "infosec", "architect", "it ",
    "qa ", "qc ", "tester", "ux", "ui/ux", "scrum", "agile",
    "product manager", "product owner", "technical",
    "blockchain", "iot", "embedded", "firmware",
]
_TITLE_NOISE_PATTERNS = [
    r"\btuyển\s*(gấp|dụng)?\b",
    r"\bgấp\b",
    r"\bremote\b",
    r"\bfull[\s\-]?time\b",
    r"\bpart[\s\-]?time\b",
    r"\bhybrid\b",
    r"\bonsite\b",
    r"\bwork\s+from\s+home\b",
    r"\bwfh\b",
    r"\blương\b[^,;()\[\]]*",
    r"\bsalary\b[^,;()\[\]]*",
    r"\btại\s+[\w\s]{2,30}(?=[,;()\[\]]|$)",
    r"\blàm\s+việc\s+tại\b[^,;()\[\]]*",
    r"\bnhiều\s+vị\s+trí\b",
    r"\b\d+\s+vị\s+trí\b",
    r"\b\d+\s+slots?\b",
    r"\bslot\b",
    r"\bvới\s+mức\s+lương\b[^,;()\[\]]*",
    r"\bnhiều\s+ưu\s+đãi\b",
    r"\bưu\s+đãi\s+hấp\s+dẫn\b",
    r"\[.*?\]",      # nội dung trong ngoặc vuông (địa điểm, skill...)
    r"\(.*?\)",      # nội dung trong ngoặc tròn
]

# FIX: build skill noise pattern một lần lúc import
_SKILL_NOISE_RE: re.Pattern = _build_skill_noise_pattern()

# Compile tất cả noise patterns
_TITLE_NOISE_RES = [re.compile(p, re.IGNORECASE | re.UNICODE)
                    for p in _TITLE_NOISE_PATTERNS]

# FIX: danh sách hard skill keywords dùng để detect is_it
_IT_HARD_SKILL_KW: frozenset[str] = frozenset(
    kw.lower()
    for kws in SKILL_MAP["hard"].values()
    for kw in kws
    if len(kw) >= 2 and not any(c in kw for c in r"\^$[]{}|?+*")
)
_TITLE_NOISE_RES = [re.compile(p, re.IGNORECASE | re.UNICODE)
                    for p in _TITLE_NOISE_PATTERNS]
 
 
def _strip_title_noise(raw_title: str) -> str:
    """Strip noise ra khỏi job title, GIỮ NGUYÊN role keywords."""
    s = raw_title.strip()
    for noise_re in _TITLE_NOISE_RES:
        s = noise_re.sub(" ", s)
    s = re.sub(r"[-–—,;/|_]+$", "", s)
    s = re.sub(r"^[-–—,;/|_]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# def _strip_title_noise(raw_title: str) -> str:
#     """
#     Strip noise ra khỏi job title:
#     1. Bỏ nội dung trong ngoặc/ngoặc vuông (thường là skill list)
#     2. Bỏ skill keywords từ SKILL_MAP
#     3. Bỏ các noise patterns (tuyển, địa điểm, lương, cấp bậc...)
#     """
#     s = raw_title.strip()

#     # Bước 1: Bỏ nội dung trong ngoặc (skill list, địa điểm...)
#     s = re.sub(r"\(.*?\)", " ", s)
#     s = re.sub(r"\[.*?\]", " ", s)

#     # Bước 2: Bỏ skill keywords từ SKILL_MAP
#     s = _SKILL_NOISE_RE.sub(" ", s)

#     # Bước 3: Bỏ các noise patterns
#     for noise_re in _TITLE_NOISE_RES:
#         s = noise_re.sub(" ", s)

#     # Làm sạch
#     s = re.sub(r"[-–—,;/|]+$", "", s)      # bỏ ký tự thừa cuối
#     s = re.sub(r"^[-–—,;/|]+", "", s)      # bỏ ký tự thừa đầu
#     s = re.sub(r"\s+", " ", s).strip()
#     return s

def _map_category_from_raw(job_category_raw: str) -> str | None:
    """
    FIX C: Map job_category_clean từ job_category raw (scraped).
    Trả về category string nếu match, None nếu không match.
    """
    if not job_category_raw:
        return None
    text = _s(job_category_raw).lower()
    if not text:
        return None
 
    # Thử IT category qua JOB_CATEGORY_MAP trước
    for std_title, category in JOB_CATEGORY_MAP.items():
        if std_title.lower() in text or text in std_title.lower():
            return category
 
    # Thử Non-IT category
    for category, kws in _NON_IT_CATEGORY_MAP.items():
        if kws and any(kw in text for kw in kws):
            return category
 
    return None
 
def parse_job_title(raw: str, job_category_raw: str = "") -> dict:
    """
    FIX C + feedback:
    1. job_category_clean: ưu tiên job_category_raw → fallback job_title
    2. is_it: dùng JOB_TITLE_MAP keywords + TECH_DOMAIN (không chỉ IT_TITLES)
    3. job_title_clean: không bỏ role keyword (manager, engineer...)
    4. Non-IT category đúng thay vì default "software engineer"
    """
    result_base = {"job_title_clean": None, "job_category_clean": None, "is_it": 0}
 
    if not raw:
        # Vẫn thử map category từ raw dù không có title
        cat = _map_category_from_raw(job_category_raw)
        result_base["job_category_clean"] = cat or "Other"
        return result_base
 
    raw_s = _s(raw)
    text  = raw_s.lower()
    stripped = _strip_title_noise(raw_s).lower()
 
    title_clean  = None
    matched_dict = False
 
    # ── Pass 1: exact keyword match trong JOB_TITLE_MAP (stripped title) ─────
    for std, kws in JOB_TITLE_MAP.items():
        if any(k in stripped for k in kws):
            title_clean  = std
            matched_dict = True
            break
 
    # ── Pass 2: thử lại với raw text ─────────────────────────────────────────
    if title_clean is None:
        for std, kws in JOB_TITLE_MAP.items():
            if any(k in text for k in kws):
                title_clean  = std
                matched_dict = True
                break
 
    # ── Pass 3: infer từ role word + tech domain ──────────────────────────────
    if title_clean is None:
        role   = next((v for k, v in ROLE_WORDS.items() if k in stripped), None)
        domain = next((v for k, v in TECH_DOMAIN.items() if k in stripped), None)
        if role:
            title_clean = ROLE_DOMAIN_TO_TITLE.get(
                (role, domain),
                ROLE_DOMAIN_TO_TITLE.get((role, None))
            )
            if title_clean:
                matched_dict = True
 
    # Nếu vẫn không map được → dùng stripped title (giữ role keyword)
    if title_clean is None:
        clean_raw = re.sub(r"^\W+|\W+$", "", stripped).strip()
        clean_raw = re.sub(r"\s+", " ", clean_raw)
        title_clean = clean_raw if clean_raw else raw_s
 
    # ── FIX: is_it dùng JOB_TITLE_MAP keywords (không chỉ IT_TITLES) ─────────
    is_it = 0
    if matched_dict:
        # Nếu map được → check IT_TITLES
        if title_clean in IT_TITLES:
            is_it = 1
        else:
            # Cũng check bằng keyword scan (bắt case như "AI Scientist", "Solutions Architect")
            for std, kws in JOB_TITLE_MAP.items():
                if std in IT_TITLES and any(k in text for k in kws):
                    is_it = 1
                    break
    if not is_it:
        # Scan TECH_DOMAIN
        if any(k in text for k in TECH_DOMAIN):
            is_it = 1
    if not is_it:
        # Fallback: scan hard-coded IT title keywords
        for kw in _IT_TITLE_KW_FALLBACK:
            if kw in text:
                is_it = 1
                break
    if not is_it:
        # Scan extra IT keywords
        for kw in _IT_EXTRA_KW:
            if kw in text:
                is_it = 1
                break
 
    # ── FIX C: job_category_clean – ưu tiên job_category raw ─────────────────
    # Bước 1: thử map từ job_category raw (scraped)
    category = _map_category_from_raw(job_category_raw)
 
    # Bước 2: nếu không map được từ raw → dùng title
    if category is None:
        if matched_dict and title_clean in JOB_CATEGORY_MAP:
            category = JOB_CATEGORY_MAP[title_clean]
        elif is_it:
            category = "Other"  # IT nhưng chưa map được category cụ thể
        else:
            # Thử Non-IT category từ title
            for cat_name, kws in _NON_IT_CATEGORY_MAP.items():
                if kws and any(kw in text for kw in kws):
                    category = cat_name
                    break
 
    # Bước 3: fallback cuối
    if category is None:
        category = "Other" if is_it else "Non-IT"
 
    return {
        "job_title_clean":     title_clean,
        "job_category_clean":  category,
        "is_it":               is_it,
    }
 


# ── 2.4 Company title ─────────────────────────────────────────────────────────

_COMPANY_NOISE = re.compile(
    r"\b(company|công ty|co\.?,?\s*ltd\.?|co\s+ltd|corp\.?|corporation"
    r"|trách nhiệm hữu hạn|tnhh|cổ phần|\bcp\b|hợp danh|\bhd\b"
    r"|doanh nghiệp tư nhân|dntn|tập đoàn"
    r"|\bllc\b|\binc\.?\b|\bincorporated\b|\bjsc\b|\bltd\.?\b|\blimited\b)\b",
    re.IGNORECASE | re.UNICODE,
)


def parse_company_title(raw: str) -> dict:
    if not raw:
        return {"company_title_clean": None, "company_type": None}
    s  = _s(raw)
    sl = s.lower()

    company_type = None
    for pattern, ctype in COMPANY_TYPE_PATTERNS:
        if re.search(pattern, sl):
            company_type = ctype
            break

    cleaned = _COMPANY_NOISE.sub("", s)
    cleaned = re.sub(r"[,.\-–—]+$", "", cleaned).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip() or None
    return {"company_title_clean": cleaned, "company_type": company_type or "Khác"}


# ── 2.5 Location ──────────────────────────────────────────────────────────────

def _resolve_province(raw: str) -> tuple[str | None, str]:
    loc      = raw.lower().strip()
    province = PROVINCE_CANONICAL.get(loc)
    if province is None:
        for key in GEO_KEYS_SORTED:
            if key in loc:
                province = PROVINCE_CANONICAL[key]
                break
    region = REGION_MAP.get(province, "Khác") if province else "Khác"
    return province, region


def parse_location(raw: str) -> list[dict]:
    if not raw:
        return [{"location_province": "Khác", "location_region": "Khác", "is_vn": 0}]

    s = _s(raw)
    s = re.sub(r",?\s*viet\s*nam\s*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(
        r"(?:phường|xã|thị trấn|quận|huyện|district|ward)\s+[\w\s]+?(?=[,;|]|$)",
        "", s, flags=re.IGNORECASE | re.UNICODE
    ).strip(" ,;")

    parts = [p.strip() for p in re.split(r"[;,|&]|\s+[-–]\s+", s) if p.strip()]

    if len(parts) > 9:
        return []

    result         = []
    seen_provinces = set()

    for part in parts:
        pl         = part.lower().strip()
        is_foreign = any(fw in pl for fw in FOREIGN_KW)
        province, region = _resolve_province(part)

        if province and province in seen_provinces:
            continue
        if province:
            seen_provinces.add(province)

        result.append({
            "location_province": province or ("Nước ngoài" if is_foreign else "Khác"),
            "location_region":   "Nước ngoài" if is_foreign else region,
            "is_vn":             0 if is_foreign else (1 if province else 0),
        })

    return result or [{"location_province": "Khác", "location_region": "Khác", "is_vn": 0}]


# ── 2.6 Work style ────────────────────────────────────────────────────────────

def parse_work_style(job_type_raw: str, work_mode_raw: str, job_desc: str) -> dict:
    full = (_s(job_type_raw) + " " + _s(work_mode_raw) + " " +
            _s(job_desc)[:500]).lower()

    jt = "Full-time"
    for jtype, kws in WORK_TYPE_MAP.items():
        if any(k in full for k in kws):
            jt = jtype
            break

    wm = "Onsite"
    for mode, kws in WORK_MODE_MAP.items():
        if any(k in full for k in kws):
            wm = mode
            break

    return {"job_type_clean": jt, "work_mode_clean": wm}


# ── 2.7 Compensation ──────────────────────────────────────────────────────────

# ==============================================================================
# PATCH: parse_compensation – rewrite hoàn toàn theo feedback
# Thay thế toàn bộ phần "── 2.7 Compensation" trong etl.py
# ==============================================================================

# ==============================================================================
# 2.7  Compensation  (REWRITE)
# ==============================================================================

# ── Đơn vị output chuẩn ──────────────────────────────────────────────────────
# • salary_min / salary_max : đơn vị NGUYÊN GỐC theo currency
#       USD  → USD
#       VND  → VND  (đồng, KHÔNG chia nghìn)
#       JPY  → JPY
# • conversion_rate : 1 đơn vị currency = conversion_rate VND
#       USD: 25_500 (cập nhật theo tỷ giá thực tế)
#       VND: 1
#       JPY: 168
# • period : "month" | "year"  (sau khi normalize, salary đã ÷12 nếu year)
# • is_negotiable : 1 nếu không tìm được số hợp lệ

# ── Currency table (code, vnd_rate) ──────────────────────────────────────────
_CURRENCY_TABLE: dict[str, float] = {
    "USD": 25_500.0,
    "VND": 1.0,
    "JPY": 168.0,
    "EUR": 27_500.0,
    "SGD": 19_000.0,
    "GBP": 32_000.0,
    "AUD": 16_500.0,
    "CAD": 18_500.0,
    "KRW": 18.5,
    "CNY": 3_500.0,
    "THB": 700.0,
}

# ── Patterns detect currency ──────────────────────────────────────────────────
_CURRENCY_DETECT: list[tuple[str, str]] = [
    # USD
    (r"\$|usd|\bđô\b|dollar", "USD"),
    # VND
    (r"vnđ|vnd|đ(?!ồng)|đồng|triệu|triêu|nghìn\s*đ|ngàn\s*đ", "VND"),
    # JPY
    (r"\bjpy\b|¥|yen", "JPY"),
    # EUR
    (r"\beur\b|€|euro", "EUR"),
    # SGD
    (r"\bsgd\b|s\$", "SGD"),
    # GBP
    (r"\bgbp\b|£|pound", "GBP"),
    # AUD
    (r"\baud\b|a\$", "AUD"),
    # CAD
    (r"\bcad\b|ca\$", "CAD"),
    # KRW
    (r"\bkrw\b|₩|won", "KRW"),
    # CNY
    (r"\bcny\b|rmb|yuan", "CNY"),
    # THB
    (r"\bthb\b|฿|baht", "THB"),
]

# ── Negotiable keywords (chỉ trigger khi KHÔNG có số thực sự) ────────────────
_NEG_KW: list[str] = [
    "thỏa thuận", "thương lượng", "competitive", "negotiable",
    "attractive", "market rate", "commensurate", "tbd", "t.b.d",
    "to be discussed", "to be confirmed", "sẽ thảo luận",
]

# ── Multiplier suffix ────────────────────────────────────────────────────────
# Xử lý sau khi extract từng token số
#   "3.5k"      → 3500
#   "3.5m/3.5M" → 3_500_000  (dùng khi currency là USD; triệu đồng khi VND)
#   "30M"       → 30_000_000
#   "triệu"     → ×1_000_000
#   "tr"        → ×1_000_000
#   "k"         → ×1000
_SUFFIX_RE = re.compile(
    r"(?P<num>\d+(?:[.,]\d+)?)\s*"
    r"(?P<sfx>triệu|triêu\b|tr\b|[kKmM](?!\w))",
    re.UNICODE,
)

# Pattern để detect "X - Y triệu" (suffix ở cuối áp dụng cho CẢ HAI số)
_RANGE_SUFFIX_RE = re.compile(
    r"(?P<n1>\d+(?:[.,]\d+)?)\s*[-–—~]\s*(?P<n2>\d+(?:[.,]\d+)?)\s*"
    r"(?P<sfx>triệu|triêu\b|tr\b|[kKmMđ](?!\w))",
    re.UNICODE,
)

# ── Period detection ─────────────────────────────────────────────────────────
_YEAR_RE  = re.compile(r"/\s*year|per\s+year|/\s*yr|/\s*năm|/\s*annum", re.I)
_MONTH_RE = re.compile(r"/\s*month|per\s+month|/\s*tháng|/\s*mo\b", re.I)


def _detect_currency(text: str) -> str:
    """Trả về mã tiền tệ (vd: 'USD', 'VND'). Mặc định 'VND'."""
    for pattern, code in _CURRENCY_DETECT:
        if re.search(pattern, text, re.I):
            return code
    # Nếu có suffix k/K/m/M mà không có dấu hiệu VND → giả định USD
    if re.search(r"\d\s*[kK]\b", text) and not re.search(r"triệu|triêu|đồng|vnđ|vnd|\bđ\b", text, re.I):
        return "USD"
    return "VND"


def _normalize_separators(text: str) -> str:
    """
    Chuẩn hoá dấu phân cách:
      • "1,200,000"  (US thousand)  → "1200000"
      • "1.200.000"  (VN/EU thousand) → "1200000"
      • "1.5"        (decimal)       → "1.5"   ← GIỮ NGUYÊN
    Quy tắc: áp dụng nhiều lần để bắt chuỗi x.xxx.xxx và x,xxx,xxx.
    """
    # Bỏ thousand-sep dạng "." (áp dụng nhiều lần cho x.xxx.xxx.xxx)
    while re.search(r"\d\.\d{3}(?!\d)", text):
        text = re.sub(r"(\d)\.(\d{3})(?!\d)", r"\1\2", text)
    # Bỏ thousand-sep dạng ","
    while re.search(r"\d,\d{3}(?!\d)", text):
        text = re.sub(r"(\d),(\d{3})(?!\d)", r"\1\2", text)
    # Thay dấu "," còn lại → "." (decimal)
    text = text.replace(",", ".")
    return text


def _apply_suffix(num_str: str, sfx: str, currency: str) -> float:
    """Áp dụng hệ số suffix vào giá trị số."""
    val = float(num_str.replace(",", "."))
    sfx_lower = sfx.lower()
    if sfx_lower in ("triệu", "triêu", "tr", "m"):
        # "m" trong ngữ cảnh USD (1m = 1_000_000); cũng là triệu VND
        return val * 1_000_000
    if sfx_lower == "k":
        return val * 1_000
    # uppercase M đã được xử lý ở trên
    return val


def _extract_salary_numbers(text: str, currency: str) -> list[float]:
    """
    Trích xuất danh sách giá trị tiền lương từ chuỗi văn bản.
    Ưu tiên pattern có suffix trước, fallback sang số thường.

    Trả về list đã sorted (không áp dụng period).
    """
    # Chuẩn hoá dấu ~, –, — thành -
    t = text.replace("~", "-").replace("–", "-").replace("—", "-")
    t = _normalize_separators(t)

    results: list[float] = []
    consumed_spans: list[tuple[int, int]] = []

    # Pass 0: "X - Y triệu/k/M" → áp suffix cho cả 2 số
    for m in _RANGE_SUFFIX_RE.finditer(t):
        sfx = m.group("sfx")
        for key in ("n1", "n2"):
            val = _apply_suffix(m.group(key), sfx, currency)
            results.append(val)
        consumed_spans.append(m.span())

    # Pass 1: suffix patterns đơn lẻ (greedy, dài trước ngắn)
    for m in _SUFFIX_RE.finditer(t):
        s_pos, e_pos = m.span()
        if any(cs <= s_pos < ce for cs, ce in consumed_spans):
            continue
        val = _apply_suffix(m.group("num"), m.group("sfx"), currency)
        results.append(val)
        consumed_spans.append((s_pos, e_pos))

    # Pass 2: số thường (plain) – bỏ những vị trí đã consumed
    for m in re.finditer(r"\d+(?:\.\d+)?", t):
        # Kiểm tra không overlap với suffix match
        s, e = m.span()
        if any(cs <= s < ce for cs, ce in consumed_spans):
            continue
        val = float(m.group())
        # Lọc số hợp lệ theo currency
        if currency == "VND":
            # Số nhỏ hơn 1000 trong ngữ cảnh VND (không có suffix) thường là rác
            # Ngoại lệ: 100–999 có thể là lương tháng theo đơn vị nghìn đồng → giữ
            if val < 100:
                continue
        results.append(val)

    return sorted(results)


def _has_real_number(text: str) -> bool:
    """Kiểm tra text có chứa ít nhất một số tiền hợp lệ (> 0)."""
    return bool(re.search(r"\d", text))


def _split_main_bonus(text: str) -> str:
    """
    Tách phần lương chính khỏi bonus/benefit text.
    Ví dụ: "up to 30M. OKR/KPI bonus..." → "up to 30M"
    """
    # Cắt tại dấu chấm câu hoặc newline đầu tiên SAU khi đã có số
    m = re.search(
        r"(.*?\d.*?)(?:\.\s+[A-ZÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐ]|\n|;|,\s+(?:including|plus|with|and\s+(?:bonus|benefits?)))",
        text, re.I | re.DOTALL,
    )
    return m.group(1) if m else text


# ── Public API ────────────────────────────────────────────────────────────────

def parse_compensation(raw: str) -> dict:
    """
    Parse chuỗi lương → dict với các key:
        salary_min       : int | None  (đơn vị gốc theo currency)
        salary_max       : int | None
        salary_currency  : str | None
        conversion_rate  : float | None  (1 đơn vị = N VND)
        is_negotiable    : int  (0 hoặc 1)

    Cải tiến so với phiên bản cũ:
        1. Fix min/max (không bị overwrite thành max)
        2. Fix VND scale (giữ nguyên đơn vị gốc; conversion_rate = 1)
        3. Fix currency detect ($, k, suffix M)
        4. Handle /year → chia 12
        5. Negotiable chỉ khi KHÔNG có số
        6. Tách bonus text trước khi parse
        7. Fix JPY scale
    """
    _base = {
        "salary_min":      None,
        "salary_max":      None,
        "salary_currency": None,
        "conversion_rate": None,
        "is_negotiable":   1,
    }

    s = (raw or "").strip()
    if not s:
        return _base

    text = s.lower()

    # ── FIX 9: Tách main salary khỏi bonus text trước mọi thứ ───────────────
    s_main = _split_main_bonus(s)
    text_main = s_main.lower()

    # ── FIX 5: Negotiable CHỈ khi không có số ────────────────────────────────
    if any(kw in text for kw in _NEG_KW) and not _has_real_number(text_main):
        return _base
    if any(kw in text for kw in NEGOTIABLE_KW) and not _has_real_number(text_main):
        return _base

    # ── FIX 3: Detect currency từ toàn bộ text (bao gồm suffix M, $...) ─────
    currency = _detect_currency(text)
    rate     = _CURRENCY_TABLE.get(currency, 1.0)

    # ── FIX 4: Detect period (/year → chia 12) ───────────────────────────────
    is_yearly = bool(_YEAR_RE.search(text))

    # ── FIX 1 & 2 & 7: Extract numbers với suffix + separator đúng ──────────
    nums = _extract_salary_numbers(text_main, currency)

    if not nums:
        # Thử lại với toàn bộ text (không bỏ bonus text)
        nums = _extract_salary_numbers(text, currency)

    if not nums:
        return _base

    # ── Phán đoán min / max ──────────────────────────────────────────────────
    lo_kw = ["từ", "from", "trên", "hơn", "minimum", "tối thiểu", "at least",
             "ít nhất", "starting", "bắt đầu từ"]
    hi_kw = ["đến", "tới", "up to", "upto", "up-to", "dưới", "maximum", "tối đa",
             "không quá", "tối đa"]

    is_hi_only = any(kw in text_main for kw in hi_kw) or bool(re.search(r"\bupto\b|\bup[\s\-]+to\b", text_main))
    is_lo_only = any(kw in text_main for kw in lo_kw)

    if len(nums) == 1:
        val = nums[0]
        if is_lo_only and not is_hi_only:
            sal_min, sal_max = val, None
        elif is_hi_only:
            sal_min, sal_max = None, val
        else:
            sal_min = sal_max = val
    else:
        # FIX 1: lấy đúng min/max từ sorted list
        sal_min, sal_max = nums[0], nums[-1]

    # ── FIX 4: Normalize /year → /month ─────────────────────────────────────
    if is_yearly:
        if sal_min is not None:
            sal_min = round(sal_min / 12, 2)
        if sal_max is not None:
            sal_max = round(sal_max / 12, 2)

    # ── Sanity check: bỏ cặp min/max không hợp lý ───────────────────────────
    if sal_min is not None and sal_max is not None and sal_min > sal_max:
        sal_min, sal_max = sal_max, sal_min

    return {
        "salary_min":      int(sal_min) if sal_min is not None else None,
        "salary_max":      int(sal_max) if sal_max is not None else None,
        "salary_currency": currency,
        "conversion_rate": rate,
        "is_negotiable":   0,
    }


# # ==============================================================================
# # Quick smoke-test (python etl_patched.py)
# # ==============================================================================

# if __name__ == "__main__":
#     cases = [
#         # (input, expected_min, expected_max, expected_currency)
#         ("15 - 25triệu",                         15_000_000,  25_000_000, "VND"),
#         ("12 - 20 triệu",                         12_000_000,  20_000_000, "VND"),
#         ("20,000,000 - 40,000,000đ",              20_000_000,  40_000_000, "VND"),
#         ("15.000.000 - 25.000.000 VND",           15_000_000,  25_000_000, "VND"),
#         ("80.000.000/tháng",                      80_000_000,  80_000_000, "VND"),
#         ("upto 3.5k",                                   None,       3_500, "USD"),   # k USD
#         ("Up-to 34,000 usd/year",                       None,       2_833, "USD"),   # /year ÷12
#         ("JPY 4,000,000 – 9,000,000",              4_000_000,   9_000_000, "JPY"),
#         ("up to 30M. OKR/KPI bonus...",                 None,  30_000_000, "VND"),
#         ("15,000,000 VND – 35,000,000 VND (based on experience and performance)",
#                                                   15_000_000,  35_000_000, "VND"),
#         ("thỏa thuận",                                  None,        None, None),
#         ("1000 - 2000 USD",                            1_000,       2_000, "USD"),
#         ("từ 20 triệu",                           20_000_000,        None, "VND"),
#         ("$3,000 - $5,000",                            3_000,       5_000, "USD"),
#     ]

#     print(f"{'Input':<55} {'min':>12} {'max':>12} {'cur':>5} {'neg':>4}")
#     print("-" * 95)
#     for raw, exp_min, exp_max, exp_cur in cases:
#         r = parse_compensation(raw)
#         ok_min = "✓" if r["salary_min"] == exp_min else f"✗(got {r['salary_min']})"
#         ok_max = "✓" if r["salary_max"] == exp_max else f"✗(got {r['salary_max']})"
#         ok_cur = "✓" if r["salary_currency"] == exp_cur else f"✗(got {r['salary_currency']})"
#         print(f"{raw:<55} {str(r['salary_min']):>12} {str(r['salary_max']):>12} "
#               f"{str(r['salary_currency']):>5} {r['is_negotiable']:>4}  "
#               f"min:{ok_min} max:{ok_max} cur:{ok_cur}")

# ── 2.8 Experience ────────────────────────────────────────────────────────────

# FIX: Từ khóa bắt buộc phải có khi scan JD/requirement
# Chỉ lấy exp từ JD/req khi đi kèm với các từ khóa yêu cầu rõ ràng
_EXP_REQUIREMENT_KW: list[str] = [
    "tối thiểu", "yêu cầu", "cần có", "có ít nhất", "ít nhất",
    "kinh nghiệm làm việc", "kinh nghiệm tối thiểu",
    "năm kinh nghiệm", "năm kn", "năm kinh nghiêm",
    "tháng kinh nghiệm",
    "years? of experience", "years? experience",
    "year experience", "yrs? of experience",
    "minimum.*experience", "at least.*experience",
    "required.*experience", "experience required",
    r"\d+\+\s*years?",
    r"\d+\s+to\s+\d+\s+years?",
    r"\d+[-–]\d+\s+years?",
]
 
_EXP_REQ_RE = re.compile("|".join(_EXP_REQUIREMENT_KW), re.IGNORECASE | re.UNICODE)


def parse_experience(raw: str, job_desc: str = "", job_req: str = "") -> dict:
    """
    FIX B: dùng _extract_exp_nums (hỗ trợ thập phân) thay vì findall số nguyên.
    "0,5 năm" → exp_min=0.5, exp_max=0.5 (không còn bị tách thành 0 và 5).
    """
    base = {"exp_min_yr": None, "exp_max_yr": None, "is_exp_required": None}
 
    raw_s = _s(raw)
    if raw_s:
        combined = _normalize_exp_text(raw_s.lower())
        if any(kw in combined for kw in NO_EXP_KW):
            return {"exp_min_yr": 0.0, "exp_max_yr": 0.0, "is_exp_required": 0}
 
        nums = _extract_exp_nums(combined)
        if nums:
            if "tháng" in combined and "năm" not in combined:
                nums = [round(n / 12, 2) for n in nums]
            return _parse_exp_nums(combined, nums)
 
    for source_text in [_s(job_req), _s(job_desc)]:
        if not source_text:
            continue
        source_lower = _normalize_exp_text(source_text.lower())
        sentences = re.split(r"[.\n\r]", source_lower)
        for sent in sentences:
            sent = sent.strip()
            if not sent or not _EXP_REQ_RE.search(sent):
                continue
            if any(kw in sent for kw in NO_EXP_KW):
                return {"exp_min_yr": 0.0, "exp_max_yr": 0.0, "is_exp_required": 0}
 
            nums = _extract_exp_nums(sent)
            nums = [n for n in nums if 0 <= n <= 50]
            if not nums:
                continue
            if "tháng" in sent and "năm" not in sent:
                nums = [round(n / 12, 2) for n in nums]
 
            result = _parse_exp_nums(sent, nums)
            if result.get("exp_min_yr") is not None or result.get("exp_max_yr") is not None:
                return result
 
    return base
 
def _normalize_exp_text(text: str) -> str:
    """
    Chuẩn hoá văn bản kinh nghiệm trước khi extract số:
    - "0,5 năm" → "0.5 năm"   (dấu phẩy thập phân VN → dấu chấm)
    - "1,5 năm" → "1.5 năm"
    Chỉ replace dấu ',' là decimal (không phải thousand-sep).
    Quy tắc: N,D năm với D là 1 chữ số → decimal.
    """
    # Dấu phẩy thập phân kiểu Việt: số,chữ_số_đơn (vd: 0,5 / 1,5 / 2,5)
    text = re.sub(r"(\d),(\d)(?!\d)", r"\1.\2", text)
    return text
def _extract_exp_nums(text: str) -> list[float]:
    """
    Extract số thập phân từ text kinh nghiệm.
    Hỗ trợ: "0.5", "1.5", "2+", "3-5", v.v.
    """
    text = _normalize_exp_text(text)
    # Tìm số thập phân (bao gồm cả 0.5, 1.5...)
    return [float(n) for n in re.findall(r"\d+(?:\.\d+)?", text)]
 

def _parse_exp_nums(text: str, nums: list[float]) -> dict:
    """Helper: từ danh sách số và text → dict exp."""
    exp_min = exp_max = None
 
    if "dưới 1" in text:
        exp_min, exp_max = 0.0, 1.0
    elif any(kw in text for kw in ["trên", "hơn", "over", "minimum", "tối thiểu",
                                    "at least", "ít nhất"]) or re.search(r"\d+\+", text):
        exp_min = nums[0]
    elif any(kw in text for kw in ["dưới", "less than", "maximum", "tối đa", "up to"]):
        exp_min, exp_max = 0.0, nums[0]
    elif len(nums) >= 2:
        exp_min, exp_max = min(nums[0], nums[1]), max(nums[0], nums[1])
    else:
        exp_min = exp_max = nums[0]
 
    if exp_min is None and exp_max is None:
        return {"exp_min_yr": None, "exp_max_yr": None, "is_exp_required": None}
 
    return {"exp_min_yr": exp_min, "exp_max_yr": exp_max, "is_exp_required": 1}

# ── 2.9 Level ─────────────────────────────────────────────────────────────────

def parse_level(level_raw: str, job_title_raw: str,
                exp_min: float | None, exp_max: float | None,
                job_desc: str = "", job_req: str = "") -> str | None:
    for source in [_s(level_raw), _s(job_title_raw),
                   _s(job_desc)[:800], _s(job_req)[:800]]:
        if not source:
            continue
        t = source.lower()
        for kws, label in LEVEL_MAP:
            if any(re.search(r"(?:^|\W)" + re.escape(k) + r"(?:$|\W)", t) for k in kws):
                return label

    if exp_min is not None or exp_max is not None:
        avg = ((exp_min or 0.0) + (exp_max or exp_min or 0.0)) / 2
        for lo, hi, label in EXP_TO_LEVEL:
            if lo <= avg < hi:
                return label

    if exp_min is not None:
        return "Fresher" if exp_min == 0 else "Mid-level"

    return "Mid-level"


# ── 2.10 JD fields ────────────────────────────────────────────────────────────

def parse_jd_fields(job_desc: str, job_req: str,
                    job_category_raw: str, website: str) -> dict:
    full = (_s(job_desc) + " " + _s(job_req)).lower()
    if _s(website).lower() not in ("linkedin", "itviec"):
        full += " " + _s(job_category_raw).lower()

    hard = []
    for skill, kws in SKILL_MAP["hard"].items():
        for kw in kws:
            if re.search(r"(?:^|\W)" + re.escape(kw) + r"(?:$|\W)", full):
                hard.append(skill)
                break

    soft = []
    for skill, kws in SKILL_MAP["soft"].items():
        for kw in kws:
            if re.search(r"(?:^|\W)" + re.escape(kw) + r"(?:$|\W)", full):
                soft.append(skill)
                break

    major = None
    for m_name, m_kws in MAJOR_MAP:
        if any(k in full for k in m_kws):
            major = m_name
            break

    found_certs = sorted({c for c in CERT_KW if c in full})

    languages = sorted({
        LANG_CERT_TO_LANG[c]
        for c in found_certs
        if c in LANG_CERT_TO_LANG
    })

    return {
        "hard_skills":    ", ".join(sorted(hard)) or None,
        "soft_skills":    ", ".join(sorted(soft)) or None,
        "major":          major,
        "certifications": ", ".join(found_certs) or None,
        "languages":      ", ".join(languages)   or None,
    }


# ── 2.11 Education ────────────────────────────────────────────────────────────

def parse_education(edu_raw: str, job_desc: str, job_req: str) -> str:
    """
    Trả về label education để ghi vào education_clean.
    KHÔNG ghi đè edu_raw (education_level).
    Fallback → "Không yêu cầu".
    """
    sources = [_s(edu_raw), _s(job_req), _s(job_desc)]
    for source in sources:
        if not source:
            continue
        t = source.lower()
        for label, kws in EDUCATION_MAP.items():
            if label == "Không yêu cầu":
                continue
            if any(k in t for k in kws):
                return label
    return "Không yêu cầu"
 

# ── 2.12 Company size ─────────────────────────────────────────────────────────

def parse_company_size(raw: str) -> dict:
    base = {"company_size_min": None, "company_size_max": None}
    s = _s(raw)
    if not s or s.upper() in ("NULL", "N/A"):
        return base
    text = s.lower().replace(".", "").replace(",", "")
    nums = [int(n) for n in re.findall(r"\d+", text)]
    if not nums:
        return base
    if any(kw in text for kw in ["dưới", "ít hơn", "less than"]):
        return {"company_size_min": 0, "company_size_max": nums[0]}
    if any(kw in text for kw in ["trên", "hơn", "over", "+"]):
        return {"company_size_min": nums[0], "company_size_max": None}
    if len(nums) >= 2:
        nums.sort()
        return {"company_size_min": nums[0], "company_size_max": nums[-1]}
    return {"company_size_min": nums[0], "company_size_max": nums[0]}


# ── 2.13 Industry ─────────────────────────────────────────────────────────────

_UNKNOWN_INDUSTRY = {
    "industry_level1": "Không xác định",
    "industry_level2": "NA",
    "industry_level3": "Không xác định",
}


def parse_industry(raw: str, major: str | None = None) -> dict:
    text = (_s(raw) + " " + _s(major)).lower()
    if not text.strip():
        return _UNKNOWN_INDUSTRY.copy()
    for entry in INDUSTRY_TREE:
        if any(kw in text for kw in entry["kw"]):
            return {"industry_level1": entry["l1"],
                    "industry_level2": entry["l2"],
                    "industry_level3": entry["l3"]}
    return _UNKNOWN_INDUSTRY.copy()


# ── 2.14 Number recruit ───────────────────────────────────────────────────────

def parse_number_recruit(num_raw: str, job_title_raw: str) -> int:
    """
    Trả về int để ghi vào number_recruit_clean.
    KHÔNG ghi đè number_recruit (raw column).
    Fallback → 1.
    """
    s = _s(num_raw)
    if s.upper() in ("NULL", "N/A", "NONE", ""):
        s = ""
 
    if s:
        sl = s.lower()
        if any(k in sl for k in ["nhiều", "số lượng lớn", "vô hạn", "không giới hạn"]):
            return 999
        col_nums = [int(x) for x in re.findall(r"\d+", sl) if 0 < int(x) < 1000]
        if col_nums:
            return max(col_nums)
 
    title = _s(job_title_raw).lower()
    found = []
    for pat in [r"tuyển\s+(\d+)", r"(\d+)\s+vị trí", r"(\d+)\s+nhân sự",
                r"(\d+)\s+nhân viên", r"(\d+)\s+người", r"(\d+)\s+slot",
                r"(\d+)\s+kỹ sư", r"(\d+)\s+chuyên viên",
                r"số lượng\s*[:\-]?\s*(\d+)"]:
        m = re.search(pat, title)
        if m:
            val = int(m.group(1))
            if 1 <= val < 200:
                found.append(val)
 
    return max(found) if found else 1
 
 
# ==============================================================================
# 3. ERROR COLLECTOR
# ==============================================================================

class ErrorCollector:
    def __init__(self):
        self._rows: list[dict] = []

    def add(self, run_id: int, src_id, job_url: str,
            field: str, raw_val, err_type: str, detail: str = ""):
        self._rows.append({
            "run_id":       run_id,
            "src_id":       src_id,
            "job_url":      job_url,
            "field_name":   field,
            "raw_value":    str(raw_val)[:500] if raw_val is not None else None,
            "error_type":   err_type,
            "error_detail": detail,
        })

    def __len__(self):
        return len(self._rows)

    def to_df(self) -> pd.DataFrame:
        return pd.DataFrame(self._rows) if self._rows else pd.DataFrame()


def _nan_to_none(rows: list[dict]) -> list[dict]:
    """Convert tất cả nan/NaT/NaN → None để MySQL nhận là NULL."""
    cleaned = []
    for row in rows:
        new_row = {}
        for k, v in row.items():
            if v is None:
                new_row[k] = None
            elif isinstance(v, float) and np.isnan(v):
                new_row[k] = None
            elif isinstance(v, str) and v.lower() == "nan":
                new_row[k] = None
            elif hasattr(v, 'isnull') and v.isnull():
                new_row[k] = None
            else:
                new_row[k] = v
        cleaned.append(new_row)
    return cleaned


# ==============================================================================
# 4. ETL CLASS
# ==============================================================================

class RecruitmentETL:

    def __init__(self, connection_string: str):
        self.engine = sqlalchemy.create_engine(connection_string)
        self._ensure_tables()
        print("✅ Kết nối DB & 3 bảng output sẵn sàng.")

    def _ensure_tables(self):
        with self.engine.begin() as conn:
            for ddl in [_DDL_FACT, _DDL_LOG, _DDL_ERROR]:
                conn.execute(sqlalchemy.text(ddl))

    def _start_log(self, mode: str, target_date) -> int:
        with self.engine.begin() as conn:
            td_value = None if str(target_date) == "all" else (str(target_date) if target_date else None)
            r = conn.execute(sqlalchemy.text(f"""
                INSERT INTO {LOG_TABLE} (run_date, mode, target_date, started_at, status)
                VALUES (CURDATE(), :mode, :td, NOW(), 'RUNNING')
            """), {"mode": mode, "td": td_value})
            return r.lastrowid

    def _finish_log(self, run_id: int, counts: dict, status: str, note: str = ""):
        note_safe = note[:2000] if note else ""
        with self.engine.begin() as conn:
            conn.execute(sqlalchemy.text(f"""
                UPDATE {LOG_TABLE} SET
                    finished_at  = NOW(),
                    duration_sec = TIMESTAMPDIFF(SECOND, started_at, NOW()),
                    total_input  = :ti, total_output = :to_,
                    new_rows     = :nr, updated_rows  = :ur,
                    error_rows   = :er, status = :st, note = :note
                WHERE run_id = :rid
            """), {"ti": counts.get("input", 0), "to_": counts.get("output", 0),
                   "nr": counts.get("new", 0),   "ur":  counts.get("updated", 0),
                   "er": counts.get("errors", 0), "st": status,
                   "note": note_safe, "rid": run_id})

    def _load(self, mode: str, date_str: str | None) -> tuple[pd.DataFrame, str]:
        if mode == "all":
            q, td = f"SELECT * FROM {SRC_TABLE}", "all"
        elif mode == "date" and date_str:
            q, td = (f"SELECT * FROM {SRC_TABLE} WHERE DATE(scraped_at) = '{date_str}'",
                     date_str)
        else:
            q, td = (f"SELECT * FROM {SRC_TABLE} WHERE DATE(scraped_at) = CURDATE()",
                     str(date.today()))
        df = pd.read_sql(q, self.engine)
        print(f"   Đọc {len(df):,} rows.")
        return df, td

    def _transform(self, df: pd.DataFrame,
                   run_id: int) -> tuple[pd.DataFrame, ErrorCollector]:
        ec  = ErrorCollector()
        now = datetime.now()
        rows_out: list[dict] = []

        for _, r in df.iterrows():
            src_id  = r.get("id")
            job_url = _s(r.get("job_url"))

            row: dict = {
                "src_id":           src_id,
                "job_url":          job_url,
                "scraped_at":       _s(r.get("scraped_at")),
                "etl_run_id":       run_id,
                "etl_processed_at": now,
                "website":          _s(r.get("website")),
                "job_title":        _s(r.get("job_title")),
                "company_title":    _s(r.get("company_title")),
                "location":         _s(r.get("location")),
                "job_type":         _s(r.get("job_type")),
                "work_mode":        _s(r.get("work_mode")),
                "compensation":     _s(r.get("compensation")),
                "experience":       _s(r.get("experience")),
                "level":            _s(r.get("level")),
                "company_size":     _s(r.get("company_size")),
                "company_industry": _s(r.get("company_industry")),
                "job_category":     _s(r.get("job_category")),
                "number_recruit":   _s(r.get("number_recruit")),
                "education_level":  _s(r.get("education_level")),
                "job_description":  _s(r.get("job_description")),
                "job_requirement":  _s(r.get("job_requirement")),
                "raw_about_job":    _s(r.get("raw_about_job")),
                "job_posted_at":    _s(r.get("job_posted_at")),
                "job_deadline":     _s(r.get("job_deadline")),
                "is_valid":         r.get("is_valid"),
                "error_log":        _s(r.get("error_log")),
            }

            def _try(field, fn, *args, fallback=None):
                try:
                    result = fn(*args)
                    if isinstance(result, dict):
                        row.update(result)
                    else:
                        row[field] = result
                except Exception as e:
                    ec.add(run_id, src_id, job_url, field,
                           args[0] if args else None, "PARSE_FAIL", str(e))
                    if isinstance(fallback, dict):
                        row.update(fallback)
                    elif fallback is not None:
                        row[field] = fallback

            row["website_clean"] = parse_website(row["website"])

            _try("job_posted_at", parse_dates,
                 row["job_posted_at"], row["job_deadline"], row["scraped_at"],
                 row["job_description"],
                 fallback={"job_posted_at_clean": None, "job_deadline_clean": None})

            _try("job_title", parse_job_title,
     row["job_title"], row["job_category"],   # ← thêm job_category raw
     fallback={"job_title_clean": None, "job_category_clean": "Other", "is_it": 0})
            _try("company_title", parse_company_title, row["company_title"],
                 fallback={"company_title_clean": None, "company_type": None})

            _try("job_type", parse_work_style,
                 row["job_type"], row["work_mode"], row["job_description"],
                 fallback={"job_type_clean": "Full-time", "work_mode_clean": "Onsite"})

            _try("compensation", parse_compensation, row["compensation"],
                 fallback={"salary_min": None, "salary_max": None,
                           "salary_currency": None, "conversion_rate": None,
                           "is_negotiable": 1})

            _try("experience", parse_experience,
                 row["experience"], row["job_description"], row["job_requirement"],
                 fallback={"exp_min_yr": None, "exp_max_yr": None, "is_exp_required": None})

            _try("level", parse_level,
                 row["level"], row["job_title"],
                 row.get("exp_min_yr"), row.get("exp_max_yr"),
                 row["job_description"], row["job_requirement"],
                 fallback="Mid-level")

            if "level" in row and row.get("level_clean") is None:
                row["level_clean"] = row.get("level")

            _try("job_description", parse_jd_fields,
                 row["job_description"], row["job_requirement"],
                 row["job_category"], row["website"],
                 fallback={"hard_skills": None, "soft_skills": None, "major": None,
                           "certifications": None, "languages": None})

            try:
                row["education_clean"] = parse_education(
                    row["education_level"], row["job_description"], row["job_requirement"]
                )
            except Exception as e:
                ec.add(run_id, src_id, job_url, "education_clean",
                    row["education_level"], "PARSE_FAIL", str(e))
                row["education_clean"] = "Không yêu cầu"

            _try("company_size", parse_company_size, row["company_size"],
                 fallback={"company_size_min": None, "company_size_max": None})

            _try("company_industry", parse_industry,
                 row["company_industry"], row.get("major"),
                 fallback=_UNKNOWN_INDUSTRY.copy())

            try:
                row["number_recruit_clean"] = parse_number_recruit(
                    row["number_recruit"], row["job_title"]
                )
            except Exception as e:
                ec.add(run_id, src_id, job_url, "number_recruit_clean",
                    row["number_recruit"], "PARSE_FAIL", str(e))
                row["number_recruit_clean"] = 1
            # Đảm bảo level_clean luôn có giá trị
            if "level_clean" not in row or row.get("level_clean") is None:
                try:
                    lv = parse_level(
                        row.get("level", ""), row.get("job_title", ""),
                        row.get("exp_min_yr"), row.get("exp_max_yr"),
                        row.get("job_description", ""), row.get("job_requirement", "")
                    )
                    row["level_clean"] = lv
                except Exception:
                    row["level_clean"] = "Mid-level"

            # Location → explode mỗi tỉnh thành 1 row
            try:
                locs = parse_location(row["location"])
            except Exception as e:
                ec.add(run_id, src_id, job_url, "location",
                       row["location"], "PARSE_FAIL", str(e))
                locs = [{"location_province": "Khác",
                         "location_region": "Khác", "is_vn": 0}]

            if not locs:
                ec.add(run_id, src_id, job_url, "location", row["location"],
                       "TOO_MANY_LOCATIONS", "Hơn 9 địa điểm, bỏ qua")
                continue

            for loc in locs:
                rows_out.append({**row, **loc})

        df_out = pd.DataFrame(rows_out)
        print(f"   {len(df)} input → {len(df_out)} output rows | {len(ec)} lỗi cột.")
        return df_out, ec

    def _save_fact(self, df: pd.DataFrame) -> dict:
        if df.empty:
            return {"new": 0, "updated": 0}

        with self.engine.connect() as conn:
            existing = set(
                pd.read_sql(
                    f"SELECT CONCAT(job_url, '|', IFNULL(location_province,'Khác')) AS uk FROM {FACT_TABLE}",
                    conn
                )["uk"].tolist()
            )

        df["_uk"] = df["job_url"] + "|" + df["location_province"].fillna("Khác")
        new_df = df[~df["_uk"].isin(existing)].drop(columns=["_uk"])
        upd_df = df[ df["_uk"].isin(existing)].drop(columns=["_uk"])

        def _insert_chunk(target_df):
            with self.engine.begin() as conn:
                for i in range(0, len(target_df), 50):
                    chunk        = target_df.iloc[i:i+50]
                    cols         = ", ".join(chunk.columns)
                    placeholders = ", ".join([f":{c}" for c in chunk.columns])
                    rows         = _nan_to_none(chunk.to_dict("records"))
                    conn.execute(
                        sqlalchemy.text(
                            f"INSERT IGNORE INTO {FACT_TABLE} ({cols}) VALUES ({placeholders})"
                        ),
                        rows,
                    )

        if not new_df.empty:
            _insert_chunk(new_df)
            print(f"   ✅ INSERT {len(new_df):,} rows mới.")

        if not upd_df.empty:
            with self.engine.begin() as conn:
                urls = upd_df["job_url"].tolist()
                for i in range(0, len(urls), 500):
                    batch = urls[i:i+500]
                    ph    = ",".join([f":u{j}" for j in range(len(batch))])
                    conn.execute(
                        sqlalchemy.text(f"DELETE FROM {FACT_TABLE} WHERE job_url IN ({ph})"),
                        {f"u{j}": u for j, u in enumerate(batch)},
                    )
            _insert_chunk(upd_df)
            print(f"   🔄 UPDATE {len(upd_df):,} rows cũ.")

        return {"new": len(new_df), "updated": len(upd_df)}

    def _save_errors(self, ec: ErrorCollector, run_id: int):
        df_err = ec.to_df()
        if df_err.empty:
            return
        df_err.to_sql(ERROR_TABLE, self.engine, if_exists="append",
                      index=False, chunksize=500)
        print(f"   ⚠ Ghi {len(df_err)} error records.")

    def run(self, mode: str = "today", date_str: str | None = None):
        print(f"\n{'=' * 62}")
        print(f"  ETL START [{datetime.now():%Y-%m-%d %H:%M:%S}]  mode={mode}")
        print(f"{'=' * 62}")

        print("\n⏳ [1/4] Load...")
        df_raw, target_date = self._load(mode, date_str)
        if df_raw.empty:
            print("   Không có dữ liệu.")
            return None

        run_id = self._start_log(mode, target_date)
        print(f"   run_id={run_id}")

        counts = {"input": len(df_raw), "output": 0,
                  "new": 0, "updated": 0, "errors": 0}
        status = "SUCCESS"

        try:
            print("\n⏳ [2/4] Transform...")
            df_clean, ec = self._transform(df_raw, run_id)
            counts["output"] = len(df_clean)
            counts["errors"] = len(ec)

            print("\n⏳ [3/4] Save fact...")
            saved = self._save_fact(df_clean)
            counts["new"]     = saved["new"]
            counts["updated"] = saved["updated"]

            print("\n⏳ [4/4] Save errors...")
            self._save_errors(ec, run_id)

            if counts["input"] > 0 and counts["errors"] / counts["input"] > 0.2:
                status = "WARN"

        except Exception as e:
            status = "FAILED"
            self._finish_log(run_id, counts, status, str(e))
            print(f"\n✗ FAILED: {e}")
            raise

        self._finish_log(run_id, counts, status)
        print(f"\n{'=' * 62}")
        print(f"  DONE [{datetime.now():%Y-%m-%d %H:%M:%S}] {status}")
        print(f"  input={counts['input']} out={counts['output']} "
              f"new={counts['new']} upd={counts['updated']} err={counts['errors']}")
        print(f"{'=' * 62}\n")
        return df_clean


# ==============================================================================
# 5. CLI
# ==============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Recruitment ETL")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--all",  action="store_true")
    grp.add_argument("--date", type=str, metavar="YYYY-MM-DD")
    args = parser.parse_args()

    etl = RecruitmentETL(DATABASE_URL)
    if args.all:
        etl.run("all")
    elif args.date:
        etl.run("date", args.date)
    else:
        etl.run("today")