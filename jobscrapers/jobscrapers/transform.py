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
#
# Company match (gộp từ company.py):
#   - Bidirectional fuzzy score (ratio + partial_ratio cả 2 chiều + token_sort)
#   - Typesense 2 pass: prefix=true → infix fallback
#   - type_penalty = 30 (không phải 100) để tránh bắn hết sang no_match
#   - ok (score ≥ 85)     → UPDATE company_title_clean = tên chuẩn Typesense
#   - review (65–84)      → giữ tên normalize, export Excel để xem lại
#   - no_match (< 65)     → coi là tên mới, giữ nguyên
# ==============================================================================

import re
import time
import argparse
import unicodedata
import numpy as np
import pandas as pd
import sqlalchemy
import typesense
from rapidfuzz import fuzz as _fuzz
from tqdm import tqdm
from datetime import datetime, timedelta, date
from lookups import (
    PROVINCE_CANONICAL, GEO_KEYS_SORTED, REGION_MAP,
    NEGOTIABLE_KW,
    NO_EXP_KW,
    LEVEL_MAP, EXP_TO_LEVEL,
    EDUCATION_MAP,
    INDUSTRY_TREE,
    COMPANY_TYPE_PATTERNS, COMPANY_TYPE_STRIP,
    JOB_CATEGORY_MAP, IT_TITLES,
    JOB_TITLE_MAP,
    NON_IT_TITLE_MAP,
    MAJOR_MAP,
    CERT_KW, LANG_CERT_TO_LANG,
    SKILL_MAP,
    WORK_TYPE_MAP, WORK_MODE_MAP,
    ROLE_WORDS, TECH_DOMAIN, ROLE_DOMAIN_TO_TITLE,
)

# ==============================================================================
# 0. CONFIG
# ==============================================================================

import os

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "mysql+mysqlconnector://root:123456@localhost/itta?charset=utf8mb4"
)
SRC_TABLE   = "jobs"
FACT_TABLE  = "fact_jobs_etl"
LOG_TABLE   = "fact_etl_log"
ERROR_TABLE = "fact_etl_error"

TS_CONFIG = {
    "host":    "localhost",
    "port":    "8108",
    "api_key": "changeme123",
    "timeout": 3,
}

# ==============================================================================
# 0.5 COMPANY NORMALIZE — dùng chung cho parse_company_title & match
# ==============================================================================

_COMPANY_TYPE_NORMALIZE = [
    (r'\bTNHH\s+MTV\b',                            'Công ty TNHH MTV'),
    (r'\bCông\s+ty\s+TNHH\s+Một\s+thành\s+viên\b', 'Công ty TNHH MTV'),
    (r'\bCty\s+TNHH\s+MTV\b',                      'Công ty TNHH MTV'),
    (r'\bCty\s+TNHH\b',                            'Công ty TNHH'),
    (r'\bCông\s+ty\s+Trách\s+nhiệm\s+hữu\s+hạn\b', 'Công ty TNHH'),
    (r'\bCTCP\b',                                  'Công ty Cổ phần'),
    (r'\bCty\s+CP\b',                              'Công ty Cổ phần'),
    (r'\bCông\s+ty\s+CP\b',                        'Công ty Cổ phần'),
    (r'\bCty\s+Cổ\s+phần\b',                       'Công ty Cổ phần'),
    (r'\bJSC\b',                                   'Công ty Cổ phần'),
    (r'\bDNTN\b',                                  'Doanh nghiệp Tư nhân'),
    (r'\bDoanh\s+nghiệp\s+TN\b',                   'Doanh nghiệp Tư nhân'),
    (r'\bTĐ\b',                                    'Tập đoàn'),
]

# Rules detect loại hình — dùng cho cả parse và match scoring
_COMPANY_TYPE_RULES_MATCH = [
    (r'\bCông\s+ty\s+Cổ\s+phần\b',      'Cổ phần'),
    (r'\bTrách\s+nhiệm\s+hữu\s+hạn\b',  'TNHH'),
    (r'\bMột\s+thành\s+viên\b',          'TNHH MTV'),
    (r'\bDoanh\s+nghiệp\s+Tư\s+nhân\b', 'Tư nhân'),
    (r'\bTập\s+đoàn\b',                  'Tập đoàn'),
    (r'\bCTCP\b|\bCty\s+CP\b|\bJSC\b',  'Cổ phần'),
    (r'\bTNHH\b|\bLtd\b|\bLLC\b',        'TNHH'),
    (r'\bMTV\b',                         'TNHH MTV'),
]

# Noise words cần strip trước khi gửi lên Typesense
_COMPANY_SEARCH_NOISE = [
    r'\bCông\s+ty\b', r'\bCty\b', r'\bViệt\s+Nam\b', r'\bVN\b', r'\bVietnam\b',
    r'\bTNHH\b', r'\bCổ\s+phần\b', r'\bCP\b', r'\bMTV\b',
]


def _normalize_company_name(name: str) -> str:
    """Chuẩn hoá prefix pháp lý, giữ nguyên tên thương hiệu.
    'CTCP Vinamilk' → 'Công ty Cổ phần Vinamilk'
    """
    result = (name or "").strip()
    for pattern, replacement in _COMPANY_TYPE_NORMALIZE:
        result, n = re.subn(pattern, replacement, result, flags=re.IGNORECASE)
        if n:
            break
    return result


def _match_clean_for_search(name: str) -> str:
    """Strip noise words, lowercase, collapse spaces — dùng làm query Typesense."""
    res = name.lower()
    for pat in _COMPANY_SEARCH_NOISE:
        res = re.sub(pat, ' ', res, flags=re.IGNORECASE)
    res = re.sub(r'[^\w\s]', ' ', res)
    return re.sub(r'\s+', ' ', res).strip()


def _match_get_type(name: str) -> str | None:
    """Trả về loại hình công ty từ tên, hoặc None nếu không xác định."""
    for pattern, ctype in _COMPANY_TYPE_RULES_MATCH:
        if re.search(pattern, name, flags=re.IGNORECASE):
            return ctype
    return None


def _match_search_typesense(ts, name_clean: str,
                             retries: int = 1, delay: float = 0.5) -> list:
    """
    2-pass Typesense search:
      Pass 1: prefix=true  → ưu tiên khớp từ trái sang phải
      Pass 2: prefix=false → infix fallback nếu pass 1 trống
    """
    params_prefix = {
        'q': name_clean, 'query_by': 'name_official',
        'per_page': 10, 'prefix': 'true', 'sort_by': '_text_match:desc',
    }
    params_infix = {
        'q': name_clean, 'query_by': 'name_official',
        'per_page': 10, 'prefix': 'false', 'sort_by': '_text_match:desc',
    }
    last_err = None
    for attempt in range(retries):
        try:
            res  = ts.collections['companies'].documents.search(params_prefix)
            hits = res.get('hits', [])
            if hits:
                return hits
            res2 = ts.collections['companies'].documents.search(params_infix)
            return res2.get('hits', [])
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    raise last_err


def _bidirectional_score(name_a: str, name_b: str) -> tuple[int, int, str]:
    """
    Score tương đồng hai chiều:
      max(ratio, partial_ratio a→b, partial_ratio b→a, token_sort_ratio) + prefix_bonus

    prefix_bonus:
      +30 nếu cả 2 chiều đều là prefix của nhau (bằng nhau)
      +15 nếu 1 chiều
      -20 nếu không chiều nào

    Trả về: (raw_ratio, prefix_bonus, note)
    """
    a = name_a.lower().strip()
    b = name_b.lower().strip()

    r_full    = _fuzz.ratio(a, b)
    r_ab      = _fuzz.partial_ratio(a, b)
    r_ba      = _fuzz.partial_ratio(b, a)
    r_sort    = _fuzz.token_sort_ratio(a, b)
    raw_ratio = max(r_full, r_ab, r_ba, r_sort)

    a_prefix_b = b.startswith(a)
    b_prefix_a = a.startswith(b)

    if a_prefix_b and b_prefix_a:
        prefix_bonus, note = 30, 'Khớp prefix 2 chiều'
    elif a_prefix_b:
        prefix_bonus, note = 15, 'A là prefix của B'
    elif b_prefix_a:
        prefix_bonus, note = 15, 'B là prefix của A'
    else:
        prefix_bonus, note = -20, 'Không khớp prefix'

    return raw_ratio, prefix_bonus, note


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
    job_title_detect        VARCHAR(150),
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
    company_canonical_key   VARCHAR(200),
    industry_level1         VARCHAR(100),
    industry_level2         VARCHAR(150),
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


# ==============================================================================
# 2.1 Website
# ==============================================================================

def parse_website(raw: str) -> str:
    return _s(raw).lower()


# ==============================================================================
# 2.2 Dates
# ==============================================================================

def _to_date(text: str, ref_iso: str) -> date | None:
    s = _s(text).lower()
    s = re.sub(r"\(.*?\)", "", s).strip()
    if not s:
        return None

    m = re.match(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", s)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass

    m = re.match(r"(\d{4})[/\-](\d{2})[/\-](\d{2})", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    m = re.search(r"còn\s+(\d+)\s+ngày", s)
    if m:
        try:
            ref = datetime.fromisoformat(ref_iso.replace(" ", "T"))
            return (ref + timedelta(days=int(m.group(1)))).date()
        except Exception:
            pass

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


# ==============================================================================
# 2.3 Job title
# ==============================================================================

_IT_EXTRA_KW: list[str] = [
    "phần mềm", "lập trình", "kỹ thuật phần mềm", "công nghệ thông tin",
    "hệ thống", "mạng máy tính", "an ninh mạng", "bảo mật",
    "it ", " it,", "etl", "elt", "data", "cloud", "devops", "sre",
    "ci/cd", "api", "microservice", "blockchain", "web3", "embedded",
    "firmware", "iot", "plc", "scada", "sap", "erp", "odoo",
    "salesforce", "figma", "ui/ux", "power bi", "tableau",
    "looker", "bigquery",
]

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
    r"\[.*?\]",
    r"\(.*?\)",
]

_TITLE_NOISE_RES = [re.compile(p, re.IGNORECASE | re.UNICODE)
                    for p in _TITLE_NOISE_PATTERNS]


def _clean_job_title(raw: str) -> str:
    s = _s(raw).strip()
    if not s:
        return ""
    for noise_re in _TITLE_NOISE_RES:
        s = noise_re.sub(" ", s)
    s = re.sub(r"[-–—,;/|_]+$", "", s)
    s = re.sub(r"^[-–—,;/|_]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _detect_job_title(clean_title: str) -> str | None:
    if not clean_title:
        return None
    text = clean_title.lower()

    for std_title, kws in JOB_TITLE_MAP.items():
        if any(k in text for k in kws):
            return std_title

    role   = next((v for k, v in ROLE_WORDS.items()  if k in text), None)
    domain = next((v for k, v in TECH_DOMAIN.items() if k in text), None)
    if role:
        inferred = ROLE_DOMAIN_TO_TITLE.get(
            (role, domain),
            ROLE_DOMAIN_TO_TITLE.get((role, None)),
        )
        if inferred:
            return inferred

    for kws, en_title in NON_IT_TITLE_MAP:
        if any(k in text for k in kws):
            return en_title

    return None


def _has_it_signal(text: str) -> bool:
    return (
        any(k in text for k in TECH_DOMAIN)
        or any(k in text for k in _IT_TITLE_KW_FALLBACK)
        or any(k in text for k in _IT_EXTRA_KW)
    )


def parse_job_title(raw: str, job_category_raw: str = "") -> dict:
    raw_s   = _s(raw)
    cleaned = _clean_job_title(raw_s)
    detected = _detect_job_title(cleaned) or _detect_job_title(raw_s)

    if detected is not None:
        if detected in IT_TITLES:
            return {
                "job_title_clean":    cleaned or raw_s,
                "job_title_detect":   detected,
                "job_category_clean": JOB_CATEGORY_MAP.get(detected, "IT - Khác"),
                "is_it":              1,
            }
        elif detected in JOB_CATEGORY_MAP:
            return {
                "job_title_clean":    cleaned or raw_s,
                "job_title_detect":   detected,
                "job_category_clean": JOB_CATEGORY_MAP[detected],
                "is_it":              0,
            }
        else:
            return {
                "job_title_clean":    cleaned or raw_s,
                "job_title_detect":   detected,
                "job_category_clean": "Non-IT",
                "is_it":              0,
            }

    text = (cleaned or raw_s).lower()
    if _has_it_signal(text):
        return {
            "job_title_clean":    cleaned or raw_s,
            "job_title_detect":   None,
            "job_category_clean": "IT - Khác",
            "is_it":              1,
        }

    return {
        "job_title_clean":    cleaned or raw_s,
        "job_title_detect":   None,
        "job_category_clean": "Non-IT",
        "is_it":              0,
    }


# ==============================================================================
# 2.4 Company title
# ==============================================================================

_LEGAL_STRIP_RE = re.compile(
    r'\b('
    r'tổng\s+công\s+ty'
    r'|trách\s+nhiệm\s+hữu\s+hạn\s+một\s+thành\s+viên'
    r'|trách\s+nhiệm\s+hữu\s+hạn\s+mtv'
    r'|trách\s+nhiệm\s+hữu\s+hạn'
    r'|một\s+thành\s+viên'
    r'|ngân\s+hàng\s+thương\s+mại\s+cổ\s+phần'
    r'|ngân\s+hàng\s+tmcp'
    r'|ngân\s+hàng'
    r'|hợp\s+tác\s+xã'
    r'|cổ\s+phần|tập\s+đoàn|chi\s+nhánh'
    r'|văn\s+phòng\s+đại\s+diện'
    r'|\bctcp\b'
    r'|\btnhh\s+mtv\b|\btnhh\s+1tv\b|\btnhh\b'
    r'|\bjoint[\s\-]stock\s+company\b|\bjoint[\s\-]stock\b'
    r'|\bjsc\b|\bllc\b|\bltd\.?\b|\binc\.?\b'
    r'|\bcorp\.?\b|\bcorporation\b|\bco\.?\s*,?\s*ltd\.?\b'
    r'|\bpte\.?\s*ltd\.?\b|\bplc\b|\bgmbh\b|\bag\b'
    r'|\bgroup\b|\bholdings?\b|\bventures?\b'
    r'|\bservices?\b|\btrading\b|\bsolutions?\b'
    r'|\bsystems?\b|\btechnolog(?:y|ies)\b'
    r'|\binternational\b|\bglobal\b'
    r'|\bviệt\s*nam\b|\bvietnam\b|\bviet\s*nam\b'
    r'|\(việt\s*nam\)|\(vietnam\)'
    r'|\bcn\b'
    r')\b',
    re.IGNORECASE | re.UNICODE,
)

_DASH_NOISE_RE  = re.compile(r'\s*(?:[-–—|])\s*(?=\S{20,}|\w[\w\s]{15,})', re.UNICODE)
_PAREN_NOISE_RE = re.compile(r'\([^)]{0,40}\)', re.UNICODE)

_COMPANY_NOISE = re.compile(
    r'\b(công\s+ty|tổng\s+công\s+ty|company|co\.?,?\s*ltd\.?|co\s+ltd'
    r'|corp\.?|corporation'
    r'|trách\s+nhiệm\s+hữu\s+hạn|tnhh|cổ\s+phần|\bcp\b|hợp\s+tác\s+xã'
    r'|hợp\s+danh|\bhd\b'
    r'|doanh\s+nghiệp\s+tư\s+nhân|dntn|tập\s+đoàn|\bctcp\b'
    r'|\bllc\b|\binc\.?\b|\bincorporated\b|\bjsc\b|\bltd\.?\b|\blimited\b)\b',
    re.IGNORECASE | re.UNICODE,
)
_VN_SUFFIX_RE = re.compile(
    r'[-–\s]*\b(?:việt\s*nam|vietnam|viet\s*nam)\b[-–\s]*',
    re.IGNORECASE | re.UNICODE,
)
_CONFIDENTIAL_RE = re.compile(
    r"(?:careerlink|vietnamworks|topcv|itviec|linkedin|jobstreet|timviecnhanh)"
    r"['\s]*(?:client|'s\s+client)|confidential\s+(?:company|employer)"
    r"|employer\s+brand|ẩn\s+danh",
    re.IGNORECASE | re.UNICODE,
)

_SYNONYM_MAP = [
    (re.compile(r'\bphan\s+mem\b',       re.I), 'software'),
    (re.compile(r'\bcong\s+nghe\b',      re.I), 'technology'),
    (re.compile(r'\bgiai\s+phap\b',      re.I), 'solutions'),
    (re.compile(r'\bhe\s+thong\b',       re.I), 'systems'),
    (re.compile(r'\bphat\s+trien\b',     re.I), 'development'),
    (re.compile(r'\bung\s+dung\b',       re.I), 'application'),
    (re.compile(r'\bthuong\s+mai\b',     re.I), 'trading'),
    (re.compile(r'\bdich\s+vu\b',        re.I), 'services'),
    (re.compile(r'\bsan\s+xuat\b',       re.I), 'manufacturing'),
    (re.compile(r'\bxay\s+dung\b',       re.I), 'construction'),
    (re.compile(r'\bdau\s+tu\b',         re.I), 'investment'),
    (re.compile(r'\bquang\s+cao\b',      re.I), 'advertising'),
    (re.compile(r'\bvan\s+chuyen\b',     re.I), 'logistics'),
    (re.compile(r'\bvan\s+tai\b',        re.I), 'logistics'),
    (re.compile(r'\bngan\s+hang\b',      re.I), 'bank'),
    (re.compile(r'\bbao\s+hiem\b',       re.I), 'insurance'),
    (re.compile(r'\bchung\s+khoan\b',    re.I), 'securities'),
    (re.compile(r'\bdia\s+oc\b',         re.I), 'realestate'),
    (re.compile(r'\bbat\s+dong\s+san\b', re.I), 'realestate'),
    (re.compile(r'\btuyen\s+dung\b',     re.I), 'recruitment'),
    (re.compile(r'\bgiao\s+duc\b',       re.I), 'education'),
    (re.compile(r'\by\s+te\b',           re.I), 'healthcare'),
    (re.compile(r'\bduoc\b',             re.I), 'pharma'),
    (re.compile(r'\bthuc\s+pham\b',      re.I), 'food'),
    (re.compile(r'\bnha\s+hang\b',       re.I), 'restaurant'),
    (re.compile(r'\bkhach\s+san\b',      re.I), 'hotel'),
    (re.compile(r'\bdu\s+lich\b',        re.I), 'travel'),
    (re.compile(r'\bmoi\s+truong\b',     re.I), 'environment'),
    (re.compile(r'\bnang\s+luong\b',     re.I), 'energy'),
    (re.compile(r'\bdai\s+hoc\b',        re.I), 'university'),
    (re.compile(r'\bhoc\s+vien\b',       re.I), 'academy'),
    (re.compile(r'\btrung\s+tam\b',      re.I), 'center'),
    (re.compile(r'\bbenh\s+vien\b',      re.I), 'hospital'),
    (re.compile(r'\bso\s+giao\s+duc\b',  re.I), 'education_dept'),
]

_ABBREV_WHITELIST = {
    'ghn': 'giao hang nhanh', 'ghtk': 'giao hang tiet kiem',
    'jt': 'j&t express', 'ems': 'chuyen phat nhanh buu dien',
    'vcb': 'vietcombank', 'tcb': 'techcombank', 'acb': 'a chau bank',
    'bidv': 'dau tu va phat trien viet nam bank', 'vib': 'quoc te bank',
    'msb': 'hang hai bank', 'vpbank': 'viet nam thinh vuong bank',
    'mb': 'quan doi bank', 'mbb': 'quan doi bank',
    'stb': 'sai gon thuong tin bank', 'sacombank': 'sai gon thuong tin bank',
    'tpbank': 'tien phong bank', 'lpb': 'loc phat viet nam bank',
    'lpbank': 'loc phat viet nam bank', 'pvcombank': 'dai chung viet nam bank',
    'vbsp': 'chinh sach xa hoi bank',
    'agribank': 'nong nghiep va phat trien nong thon bank',
    'hdb': 'phat trien thanh pho ho chi minh bank',
    'hdbank': 'phat trien thanh pho ho chi minh bank',
    'shb': 'sai gon ha noi bank', 'ocb': 'phuong dong bank',
    'vbb': 'viet nam thuong tin bank', 'vietbank': 'viet nam thuong tin bank',
    'abb': 'an binh bank', 'abbank': 'an binh bank', 'bab': 'bac a bank',
    'vpb': 'viet nam thinh vuong bank',
    'vnpt': 'buu chinh vien thong viet nam',
    'viettel': 'vien thong quan doi viettel',
    'momo': 'truyen thong truc tuyen momo', 'vng': 'cong nghe vng',
    'fpt': 'fpt technology', 'cmc': 'cong nghe cmc', 'hcl': 'hcl technologies',
    'tma': 'tma solutions', 'vti': 'vti cloud technology', 'kms': 'kms technology',
    'vnm': 'vinamilk', 'mwg': 'the gioi di dong',
    'pnj': 'vàng bac da quy phu nhuan', 'vic': 'vingroup', 'vhm': 'vinhomes',
    'vre': 'vincom retail', 'msn': 'masan group', 'hpg': 'hoa phat group',
    'fss': 'financial software solutions', 'ssi': 'chung khoan ssi',
    'vps': 'chung khoan vps', 'pvi': 'bao hiem dau khi', 'bv': 'bao viet',
    'bhv': 'bao viet', 'prudential': 'bao hiem prudential',
    'manulife': 'bao hiem manulife',
}


def _remove_vn_accents(text: str) -> str:
    text = text.replace('Đ', 'D').replace('đ', 'd')
    return ''.join(
        c for c in unicodedata.normalize('NFKD', text)
        if not unicodedata.combining(c)
    )


def _apply_synonyms(text: str) -> str:
    for pattern, replacement in _SYNONYM_MAP:
        text = pattern.sub(replacement, text)
    return text


def _build_canonical_key(raw_clean: str) -> str:
    s = (raw_clean or '').strip()
    if not s:
        return ''
    s = _DASH_NOISE_RE.split(s)[0]
    s = _PAREN_NOISE_RE.sub(' ', s)
    for _ in range(3):
        prev = s
        s = _LEGAL_STRIP_RE.sub(' ', s)
        if s == prev:
            break
    s = _remove_vn_accents(s)
    s = re.sub(r'[^a-z0-9\s]', ' ', s.lower())
    s = re.sub(r'\s+', ' ', s).strip()
    s = _ABBREV_WHITELIST.get(s, s)
    s = _apply_synonyms(s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def parse_company_title(raw: str) -> dict:
    if not raw:
        return {
            'company_title_clean':   None,
            'company_type':          None,
            'company_canonical_key': None,
        }

    s  = str(raw).strip()
    sl = s.lower()

    if _CONFIDENTIAL_RE.search(sl):
        return {
            'company_title_clean':   'Confidential',
            'company_type':          'Confidential',
            'company_canonical_key': 'confidential',
        }

    company_type = None
    for pattern, ctype in COMPANY_TYPE_PATTERNS:
        if re.search(pattern, sl):
            company_type = ctype
            break

    normalized = _normalize_company_name(s)
    canonical  = _build_canonical_key(normalized)

    return {
        'company_title_clean':   normalized or s,
        'company_type':          company_type or 'Khác',
        'company_canonical_key': canonical or None,
    }


# ==============================================================================
# 2.5 Location
# ==============================================================================

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
        province, region = _resolve_province(part)

        if province and province in seen_provinces:
            continue
        if province:
            seen_provinces.add(province)

        if province:
            result.append({
                "location_province": province,
                "location_region":   region,
                "is_vn":             1,
            })
        else:
            result.append({
                "location_province": "Khác",
                "location_region":   "Khác",
                "is_vn":             0,
            })

    return result or [{"location_province": "Khác", "location_region": "Khác", "is_vn": 0}]


# ==============================================================================
# 2.6 Work style
# ==============================================================================

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


# ==============================================================================
# 2.7 Compensation
# ==============================================================================

_CURRENCY_TABLE: dict[str, float] = {
    "USD": 25_500.0, "VND": 1.0,     "JPY": 168.0,
    "EUR": 27_500.0, "SGD": 19_000.0, "GBP": 32_000.0,
    "AUD": 16_500.0, "CAD": 18_500.0, "KRW": 18.5,
    "CNY": 3_500.0,  "THB": 700.0,
}

_CURRENCY_DETECT: list[tuple[str, str]] = [
    (r"\$|usd|\bđô\b|dollar",                "USD"),
    (r"vnđ|vnd|đ(?!ồng)|đồng|triệu|triêu|nghìn\s*đ|ngàn\s*đ", "VND"),
    (r"\bjpy\b|¥|yen",                        "JPY"),
    (r"\beur\b|€|euro",                       "EUR"),
    (r"\bsgd\b|s\$",                          "SGD"),
    (r"\bgbp\b|£|pound",                      "GBP"),
    (r"\baud\b|a\$",                          "AUD"),
    (r"\bcad\b|ca\$",                         "CAD"),
    (r"\bkrw\b|₩|won",                        "KRW"),
    (r"\bcny\b|rmb|yuan",                     "CNY"),
    (r"\bthb\b|฿|baht",                       "THB"),
]

_NEG_KW: list[str] = [
    "thỏa thuận", "thương lượng", "competitive", "negotiable",
    "attractive", "market rate", "commensurate", "tbd", "t.b.d",
    "to be discussed", "to be confirmed", "sẽ thảo luận",
]

_YEAR_RE = re.compile(
    r"/\s*year|per\s+year|/\s*yr|/\s*năm|/\s*annum"
    r"|annually|hàng\s*năm|mỗi\s*năm|\bpa\b|\bp\.a\b", re.I,
)
_MONTH_RE = re.compile(
    r"/\s*month|per\s+month|/\s*tháng|/\s*mo\b|hàng\s*tháng|mỗi\s*tháng", re.I,
)
_HOUR_RE = re.compile(
    r"/\s*hour|per\s+hour|/\s*hr\b|/\s*giờ|\bphp\b", re.I,
)

_MONTHLY_SANITY: dict[str, tuple[float, float]] = {
    "USD": (50,      50_000),
    "VND": (500_000, 500_000_000),
    "JPY": (50_000,  5_000_000),
    "EUR": (500,     30_000),
    "SGD": (500,     30_000),
    "GBP": (500,     30_000),
    "AUD": (500,     30_000),
    "CAD": (500,     30_000),
    "KRW": (500_000, 50_000_000),
    "CNY": (1_000,   200_000),
    "THB": (5_000,   500_000),
}

_DATE_PAT = re.compile(
    r"\b\d{1,2}[/\-]\d{1,2}[/\-]\d{4}\b|\b\d{4}[/\-]\d{2}[/\-]\d{2}\b",
)

_SUFFIX_RE = re.compile(
    r"(?P<num>\d+(?:[.,]\d+)?)\s*(?P<sfx>triệu|triêu\b|tr\b|[kKmM](?!\w))",
    re.UNICODE,
)
_RANGE_SUFFIX_RE = re.compile(
    r"(?P<n1>\d+(?:[.,]\d+)?)\s*[-–—~]\s*(?P<n2>\d+(?:[.,]\d+)?)\s*"
    r"(?P<sfx>triệu|triêu\b|tr\b|[kKmMđ](?!\w))",
    re.UNICODE,
)


def _detect_currency(text: str) -> str:
    for pattern, code in _CURRENCY_DETECT:
        if re.search(pattern, text, re.I):
            return code
    if re.search(r"\d\s*[kK]\b", text) and not re.search(
            r"triệu|triêu|đồng|vnđ|vnd|\bđ\b", text, re.I):
        return "USD"
    return "VND"


def _normalize_separators(text: str) -> str:
    while re.search(r"\d\.\d{3}(?!\d)", text):
        text = re.sub(r"(\d)\.(\d{3})(?!\d)", r"\1\2", text)
    while re.search(r"\d,\d{3}(?!\d)", text):
        text = re.sub(r"(\d),(\d{3})(?!\d)", r"\1\2", text)
    text = text.replace(",", ".")
    return text


def _apply_suffix(num_str: str, sfx: str, currency: str) -> float:
    val = float(num_str.replace(",", "."))
    sfx_lower = sfx.lower()
    if sfx_lower in ("triệu", "triêu", "tr", "m"):
        return val * 1_000_000
    if sfx_lower == "k":
        return val * 1_000
    return val


def _extract_salary_numbers(text: str, currency: str) -> list[float]:
    text = _DATE_PAT.sub(" ", text)
    t = text.replace("~", "-").replace("–", "-").replace("—", "-")
    t = _normalize_separators(t)

    results: list[float] = []
    consumed_spans: list[tuple[int, int]] = []

    for m in _RANGE_SUFFIX_RE.finditer(t):
        sfx = m.group("sfx")
        for key in ("n1", "n2"):
            val = _apply_suffix(m.group(key), sfx, currency)
            results.append(val)
        consumed_spans.append(m.span())

    for m in _SUFFIX_RE.finditer(t):
        s_pos, e_pos = m.span()
        if any(cs <= s_pos < ce for cs, ce in consumed_spans):
            continue
        val = _apply_suffix(m.group("num"), m.group("sfx"), currency)
        results.append(val)
        consumed_spans.append((s_pos, e_pos))

    for m in re.finditer(r"\d+(?:\.\d+)?", t):
        s, e = m.span()
        if any(cs <= s < ce for cs, ce in consumed_spans):
            continue
        val = float(m.group())
        if currency == "VND" and val < 100:
            continue
        results.append(val)

    return sorted(results)


def _has_real_number(text: str) -> bool:
    return bool(re.search(r"\d", text))


def _split_main_bonus(text: str) -> str:
    m = re.search(
        r"(.*?\d.*?)(?:\.\s+[A-ZÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐ]|\n|;|,\s+(?:including|plus|with|and\s+(?:bonus|benefits?)))",
        text, re.I | re.DOTALL,
    )
    return m.group(1) if m else text


def _sanity_check(val: float | None, currency: str) -> float | None:
    if val is None:
        return val
    lo, hi = _MONTHLY_SANITY.get(currency, (0, float("inf")))
    if val < lo or val > hi:
        return None
    return val


def parse_compensation(raw: str) -> dict:
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

    text      = s.lower()
    s_main    = _split_main_bonus(s)
    text_main = s_main.lower()

    if any(kw in text for kw in _NEG_KW) and not _has_real_number(text_main):
        return _base
    if any(kw in text for kw in NEGOTIABLE_KW) and not _has_real_number(text_main):
        return _base

    currency = _detect_currency(text)
    rate     = _CURRENCY_TABLE.get(currency, 1.0)

    explicit_yearly  = bool(_YEAR_RE.search(text))
    explicit_monthly = bool(_MONTH_RE.search(text))
    explicit_hourly  = bool(_HOUR_RE.search(text))

    if explicit_yearly and not explicit_monthly and not explicit_hourly:
        period = "year"
    elif explicit_hourly:
        period = "hour"
    else:
        period = "month"

    nums = _extract_salary_numbers(text_main, currency)
    if not nums:
        nums = _extract_salary_numbers(text, currency)
    if not nums:
        return _base

    nums = [n for n in nums if n > 0]
    if not nums:
        return _base

    lo_kw = ["từ", "from", "trên", "hơn", "minimum", "tối thiểu", "at least",
             "ít nhất", "starting", "bắt đầu từ"]
    hi_kw = ["đến", "tới", "up to", "upto", "up-to", "dưới", "maximum", "tối đa",
             "không quá", "lên đến", "tới"]

    is_hi_only = any(kw in text_main for kw in hi_kw) or bool(
        re.search(r"\bupto\b|\bup[\s\-]+to\b", text_main))
    is_lo_only = any(kw in text_main for kw in lo_kw) and not is_hi_only

    if len(nums) == 1:
        val = nums[0]
        if is_lo_only:
            sal_min, sal_max = val, None
        elif is_hi_only:
            sal_min, sal_max = None, val
        else:
            sal_min = sal_max = val
    else:
        sal_min, sal_max = nums[0], nums[-1]

    if period == "year":
        if sal_min is not None:
            sal_min = round(sal_min / 12, 2)
        if sal_max is not None:
            sal_max = round(sal_max / 12, 2)
    elif period == "hour":
        if sal_min is not None:
            sal_min = round(sal_min * 160, 2)
        if sal_max is not None:
            sal_max = round(sal_max * 160, 2)

    lo_bound, hi_bound = _MONTHLY_SANITY.get(currency, (0, float("inf")))

    if period == "month":
        if sal_min is not None and sal_min > hi_bound:
            sal_min = round(sal_min / 12, 2)
            if sal_max is not None:
                sal_max = round(sal_max / 12, 2)
        elif sal_max is not None and sal_max < lo_bound and sal_max > 0:
            if currency == "VND" and sal_max < 10_000:
                sal_min = round(sal_min * 1_000, 2) if sal_min else None
                sal_max = round(sal_max * 1_000, 2)

    sal_min = _sanity_check(sal_min, currency)
    sal_max = _sanity_check(sal_max, currency)

    if sal_min is not None and sal_max is not None and sal_min > sal_max:
        sal_min, sal_max = sal_max, sal_min

    if sal_min is None and sal_max is None:
        return _base

    return {
        "salary_min":      int(sal_min) if sal_min is not None else None,
        "salary_max":      int(sal_max) if sal_max is not None else None,
        "salary_currency": currency,
        "conversion_rate": rate,
        "is_negotiable":   0,
    }


# ==============================================================================
# 2.8 Experience
# ==============================================================================

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


def _normalize_exp_text(text: str) -> str:
    return re.sub(r"(\d),(\d)(?!\d)", r"\1.\2", text)


def _extract_exp_nums(text: str) -> list[float]:
    text = _normalize_exp_text(text)
    return [float(n) for n in re.findall(r"\d+(?:\.\d+)?", text)]


def _parse_exp_nums(text: str, nums: list[float]) -> dict:
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


def parse_experience(raw: str, job_desc: str = "", job_req: str = "") -> dict:
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


# ==============================================================================
# 2.9 Level
# ==============================================================================

def _match_level_in_text(text: str) -> str | None:
    t = text.lower()
    for kws, label in reversed(LEVEL_MAP):
        for kw in kws:
            if any(c in kw for c in r"\^$[]{}|?+*()"):
                pattern = kw
            else:
                pattern = r"(?:^|\W)" + re.escape(kw) + r"(?:$|\W)"
            if re.search(pattern, t):
                return label
    return None


def parse_level(level_raw: str, job_title_raw: str,
                exp_min: float | None, exp_max: float | None,
                job_desc: str = "", job_req: str = "") -> str | None:
    for source in [
        _s(level_raw), _s(job_title_raw),
        _s(job_desc)[:500], _s(job_req)[:500],
    ]:
        if not source:
            continue
        label = _match_level_in_text(source)
        if label:
            return label

    if exp_min is not None or exp_max is not None:
        avg = ((exp_min or 0.0) + (exp_max or exp_min or 0.0)) / 2
        for lo, hi, label in EXP_TO_LEVEL:
            if lo <= avg < hi:
                return label

    return "Unknown"


# ==============================================================================
# 2.10 JD fields
# ==============================================================================

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
    languages   = sorted({LANG_CERT_TO_LANG[c] for c in found_certs if c in LANG_CERT_TO_LANG})

    return {
        "hard_skills":    ", ".join(sorted(hard)) or None,
        "soft_skills":    ", ".join(sorted(soft)) or None,
        "major":          major,
        "certifications": ", ".join(found_certs) or None,
        "languages":      ", ".join(languages)   or None,
    }


# ==============================================================================
# 2.11 Education
# ==============================================================================

def parse_education(edu_raw: str, job_desc: str, job_req: str) -> str:
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


# ==============================================================================
# 2.12 Company size
# ==============================================================================

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


# ==============================================================================
# 2.13 Industry
# ==============================================================================

_UNKNOWN_INDUSTRY = {"industry_level1": "Không xác định", "industry_level2": "Không xác định"}


def parse_industry(raw: str, major: str | None = None) -> dict:
    text = (_s(raw) + " " + _s(major)).lower()
    if not text.strip():
        return _UNKNOWN_INDUSTRY.copy()
    for entry in INDUSTRY_TREE:
        if any(kw in text for kw in entry["kw"]):
            return {"industry_level1": entry["l1"], "industry_level2": entry["l2"]}
    return _UNKNOWN_INDUSTRY.copy()


# ==============================================================================
# 2.14 Number recruit
# ==============================================================================

def parse_number_recruit(num_raw: str, job_title_raw: str) -> int:
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
# 3.5 COMPANY DEDUPLICATOR
# ==============================================================================

class CompanyDeduplicator:
    THRESHOLD = 88

    def __init__(self):
        self._fuzz = _fuzz  # đã import ở đầu file

    @property
    def available(self) -> bool:
        return self._fuzz is not None

    @staticmethod
    def _make_uf(items):
        parent = {x: x for x in items}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x, y):
            parent[find(y)] = find(x)

        return find, union, parent

    def build_mapping(self, name_key_pairs: list[tuple[str, str]]) -> dict[str, str]:
        if not self.available:
            return {}

        valid = [(n, k) for n, k in name_key_pairs if n and k]
        if not valid:
            return {}

        unique_map: dict[str, str] = {}
        for n, k in valid:
            if n not in unique_map:
                unique_map[n] = k
        names = list(unique_map.keys())

        blocks: dict[str, list[str]] = {}
        for name in names:
            prefix = unique_map[name][:2]
            blocks.setdefault(prefix, []).append(name)

        find, union, _ = self._make_uf(names)

        for block in blocks.values():
            if len(block) < 2:
                continue
            for i in range(len(block)):
                ki = unique_map[block[i]]
                for j in range(i + 1, len(block)):
                    kj = unique_map[block[j]]
                    if self._fuzz.token_sort_ratio(ki, kj) >= self.THRESHOLD:
                        union(block[i], block[j])

        groups: dict[str, list[str]] = {}
        for name in names:
            groups.setdefault(find(name), []).append(name)

        mapping: dict[str, str] = {}
        for members in groups.values():
            rep = max(members, key=len)
            for m in members:
                mapping[m] = rep

        return mapping

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.available or "company_title_clean" not in df.columns:
            return df

        pairs = list(zip(
            df["company_title_clean"].fillna(""),
            df.get("company_canonical_key", pd.Series([""] * len(df))).fillna(""),
        ))
        mapping = self.build_mapping(pairs)
        if not mapping:
            return df

        total_before = df["company_title_clean"].nunique()
        df["company_title_clean"] = df["company_title_clean"].map(
            lambda x: mapping.get(x, x) if pd.notna(x) else x
        )
        total_after = df["company_title_clean"].nunique()
        n_merged    = total_before - total_after

        print(f"   🔗 Dedup công ty: {total_before:,} tên → {total_after:,} tên đại diện "
              f"(gộp {n_merged:,} tên trùng).")
        return df


# ==============================================================================
# 4. ETL CLASS
# ==============================================================================

class RecruitmentETL:

    def __init__(self, connection_string: str):
        self.engine = sqlalchemy.create_engine(connection_string)
        self._ensure_tables()
        self._migrate_tables()
        print("✅ Kết nối DB & 3 bảng output sẵn sàng.")

    def _ensure_tables(self):
        with self.engine.begin() as conn:
            for ddl in [_DDL_FACT, _DDL_LOG, _DDL_ERROR]:
                conn.execute(sqlalchemy.text(ddl))

    def _migrate_tables(self):
        migrations = [
            f"ALTER TABLE {FACT_TABLE} ADD COLUMN company_canonical_key VARCHAR(200) NULL AFTER company_type",
            f"ALTER TABLE {FACT_TABLE} ADD COLUMN job_title_detect VARCHAR(150) NULL AFTER job_title",
        ]
        with self.engine.begin() as conn:
            for sql in migrations:
                try:
                    conn.execute(sqlalchemy.text(sql))
                    print(f"   ✅ Migration: {sql[:60]}...")
                except Exception as e:
                    if "Duplicate column name" in str(e) or "1060" in str(e):
                        pass
                    else:
                        raise

    def _start_log(self, mode: str, target_date) -> int:
        with self.engine.begin() as conn:
            td_value = None if str(target_date) == "all" else (
                str(target_date) if target_date else None)
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
                 row["job_title"], row["job_category"],
                 fallback={
                     "job_title_clean":    None,
                     "job_title_detect":   None,
                     "job_category_clean": "Non-IT",
                     "is_it":              0,
                 })

            _try("company_title", parse_company_title, row["company_title"],
                 fallback={"company_title_clean": None, "company_type": None,
                           "company_canonical_key": None})

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

            _try("level_clean", parse_level,
                row["level"], row["job_title"],
                row.get("exp_min_yr"), row.get("exp_max_yr"),
                row["job_description"], row["job_requirement"],
                fallback="Unknown")

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

            # if "level_clean" not in row or row.get("level_clean") is None:
            #     try:
            #         lv = parse_level(
            #             row.get("level", ""), row.get("job_title", ""),
            #             row.get("exp_min_yr"), row.get("exp_max_yr"),
            #             row.get("job_description", ""), row.get("job_requirement", "")
            #         )
            #         row["level_clean"] = lv
            #     except Exception:
            #         row["level_clean"] = "Unknown"

            try:
                locs = parse_location(row["location"])
            except Exception as e:
                ec.add(run_id, src_id, job_url, "location",
                       row["location"], "PARSE_FAIL", str(e))
                locs = [{"location_province": "Khác", "location_region": "Khác", "is_vn": 0}]

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
                pairs = list(zip(
                    upd_df["job_url"].tolist(),
                    upd_df["location_province"].fillna("Khác").tolist(),
                ))
                for i in range(0, len(pairs), 500):
                    batch = pairs[i:i+500]
                    conditions = " OR ".join(
                        [f"(job_url = :u{j} AND location_province = :p{j})"
                         for j in range(len(batch))]
                    )
                    params = {}
                    for j, (url, prov) in enumerate(batch):
                        params[f"u{j}"] = url
                        params[f"p{j}"] = prov
                    conn.execute(
                        sqlalchemy.text(f"DELETE FROM {FACT_TABLE} WHERE {conditions}"),
                        params,
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

    # --------------------------------------------------------------------------
    # COMPANY MATCH — gộp từ company.py, logic bidirectional + 2-pass search
    # --------------------------------------------------------------------------

    def _ts_client(self):
        return typesense.Client({
            'nodes': [{'host': TS_CONFIG["host"], 'port': TS_CONFIG["port"],
                       'protocol': 'http'}],
            'api_key':                    TS_CONFIG["api_key"],
            'connection_timeout_seconds': TS_CONFIG["timeout"],
        })

    def _match_one_company(self, ts, name: str) -> dict:
        """
        Match 1 tên công ty với Typesense.
        Dùng bidirectional_score + 2-pass search + type_penalty=30.
        """
        normalized = _normalize_company_name(name)
        name_clean = _match_clean_for_search(normalized)
        raw_type   = _match_get_type(normalized)

        _base = {'raw_name': name, 'matched_name': None, 'final_score': 0}

        if not name_clean:
            return {**_base, 'status': 'empty', 'note': ''}

        try:
            hits = _match_search_typesense(ts, name_clean)
        except Exception as e:
            return {**_base, 'status': 'error', 'note': str(e)}

        if not hits:
            return {**_base, 'status': 'no_match', 'note': ''}

        scored = []
        for h in hits:
            official = h['document']['name_official']
            off_type = _match_get_type(official)

            raw_ratio, prefix_bonus, prefix_note = _bidirectional_score(normalized, official)

            # type_penalty = 30 (không phải 100) — tránh bắn hết sang no_match
            if raw_type and off_type:
                type_penalty = 30 if raw_type != off_type else 0
                type_note    = (f'Loại hình khác ({raw_type}≠{off_type})'
                                if type_penalty else '')
            elif raw_type or off_type:
                type_penalty, type_note = 5, 'Một bên không rõ loại hình'
            else:
                type_penalty, type_note = 0, ''

            final_score = raw_ratio + prefix_bonus - type_penalty
            scored.append({
                'official_name': official,
                'raw_ratio':     raw_ratio,
                'prefix_bonus':  prefix_bonus,
                'type_penalty':  type_penalty,
                'final_score':   final_score,
                'note':          ' | '.join(filter(None, [prefix_note, type_note])),
            })

        scored.sort(key=lambda x: x['final_score'], reverse=True)
        best = scored[0]

        if best['final_score'] >= 85:
            status = 'ok'
        elif best['final_score'] >= 65:
            status = 'review'
        else:
            status = 'no_match'

        return {
            'raw_name':     name,
            'matched_name': best['official_name'] if status == 'ok' else None,
            'final_score':  best['final_score'],
            'raw_ratio':    best['raw_ratio'],
            'prefix_bonus': best['prefix_bonus'],
            'type_penalty': best['type_penalty'],
            'status':       status,
            'note':         best['note'],
        }

    def _match_and_update_companies(self, run_id: int):
        """
        Sau khi ETL lưu xong fact_jobs_etl:
          1. Lấy distinct company_title_clean của run hiện tại
          2. Match với Typesense (bidirectional score)
          3. ok       → UPDATE company_title_clean = tên chuẩn
             review   → giữ nguyên (tên normalize từ ETL)
             no_match → giữ nguyên (coi là tên mới)
          4. Export Excel chi tiết + ghi note vào fact_etl_log
        """
        print("\n⏳ [3.5/4] Match tên công ty với Typesense...")

        with self.engine.connect() as conn:
            df_co = pd.read_sql(
                f"""SELECT DISTINCT company_title_clean
                    FROM {FACT_TABLE}
                    WHERE etl_run_id = {run_id}
                      AND company_title_clean IS NOT NULL
                      AND company_title_clean != ''
                      AND company_title_clean != 'Confidential'""",
                conn,
            )

        if df_co.empty:
            print("   Không có tên công ty cần match.")
            return

        names = df_co["company_title_clean"].tolist()
        print(f"   {len(names):,} tên công ty unique cần xử lý...")

        ts      = self._ts_client()
        results = []

        for name in tqdm(names, desc="  Matching", unit="cty"):
            r = self._match_one_company(ts, name)
            results.append(r)
            time.sleep(0.05)

        df_res = pd.DataFrame(results)

        n_ok       = int((df_res['status'] == 'ok'      ).sum())
        n_review   = int((df_res['status'] == 'review'  ).sum())
        n_no_match = int((df_res['status'] == 'no_match').sum())
        n_error    = int((df_res['status'] == 'error'   ).sum())

        # UPDATE những cái ok
        ok_rows = df_res[df_res['status'] == 'ok']
        if not ok_rows.empty:
            with self.engine.begin() as conn:
                for _, row in ok_rows.iterrows():
                    conn.execute(sqlalchemy.text(f"""
                        UPDATE {FACT_TABLE}
                        SET    company_title_clean = :matched
                        WHERE  etl_run_id          = :run_id
                          AND  company_title_clean  = :raw
                    """), {
                        'matched': row['matched_name'],
                        'run_id':  run_id,
                        'raw':     row['raw_name'],
                    })
            print(f"   ✅ Cập nhật {n_ok:,} tên chuẩn (ok).")

        if n_review:
            print(f"   🔶 {n_review:,} cần review  → giữ tên normalize.")
        if n_no_match:
            print(f"   ⬜ {n_no_match:,} no_match   → coi là tên mới, giữ nguyên.")
        if n_error:
            print(f"   ❌ {n_error:,} lỗi timeout → giữ nguyên.")

        # Ghi note vào log
        note = (f"company_match ok={n_ok} review={n_review} "
                f"no_match={n_no_match} error={n_error}")
        with self.engine.begin() as conn:
            conn.execute(sqlalchemy.text(f"""
                UPDATE {LOG_TABLE}
                SET note = CONCAT(IFNULL(note,''), ' | ', :note)
                WHERE run_id = :rid
            """), {"note": note, "rid": run_id})

        # Export Excel chi tiết
        ts_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        df_res.to_excel(f'company_match_{ts_str}.xlsx', index=False)
        print(f"   📋 Chi tiết → company_match_{ts_str}.xlsx  "
              f"(ok={n_ok} review={n_review} no_match={n_no_match} err={n_error})")

    # --------------------------------------------------------------------------
    # RUN
    # --------------------------------------------------------------------------

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

            print("\n⏳ [2.5/4] Dedup công ty...")
            deduper  = CompanyDeduplicator()
            df_clean = deduper.apply(df_clean)

            print("\n⏳ [3/4] Save fact...")
            saved             = self._save_fact(df_clean)
            counts["new"]     = saved["new"]
            counts["updated"] = saved["updated"]

            # Match tên công ty sau khi đã lưu vào DB
            self._match_and_update_companies(run_id)

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