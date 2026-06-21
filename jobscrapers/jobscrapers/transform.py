
# etl_supabase.py
# ==============================================================================
# RECRUITMENT ETL — Phiên bản Local PostgreSQL
#
# THAY ĐỔI SO VỚI BẢN GỐC (Supabase remote):
#   [LOCAL-1] SRC_TABLE  = "staging_jobs"  (thay vì "jobs")
#   [LOCAL-2] DATABASE_URL default → local PostgreSQL
#             (vẫn đọc từ env var DATABASE_URL nếu có)
#

import pathlib
import re
import time
import argparse
import unicodedata
import numpy as np
import pandas as pd
import sqlalchemy
import os
import sys
from dotenv import load_dotenv
load_dotenv()
_project_root = str(pathlib.Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

TYPESENSE_ENABLED: bool = os.environ.get("TYPESENSE_ENABLED", "true").lower() == "true"

if TYPESENSE_ENABLED:
    try:
        import typesense
    except ImportError:
        TYPESENSE_ENABLED = False
        print("⚠️  typesense không được cài — TYPESENSE_ENABLED tự động tắt")

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
    ROLE_WORDS, TECH_DOMAIN, ROLE_DOMAIN_TO_TITLE,COMPILED_JOB_TITLE_MAP,   # thêm
    COMPILED_NON_IT_TITLE_MAP
)

# ==============================================================================
# 0. CONFIG
# ==============================================================================

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:123456@localhost:5432/recruitment_dw"
)

# [LOCAL-1] staging_jobs thay vì jobs
SRC_TABLE   = "staging_jobs"
FACT_TABLE  = "fact_jobs_etl"
LOG_TABLE   = "fact_etl_log"
ERROR_TABLE = "fact_etl_error"

TS_CONFIG = {
    "host":    os.environ.get("TYPESENSE_HOST",    "localhost"),
    "port":    os.environ.get("TYPESENSE_PORT",    "8108"),
    "api_key": os.environ.get("TYPESENSE_API_KEY", "changeme123"),
    "timeout": 3,
}

# ==============================================================================
# 0.5 MODULE-LEVEL CONSTANTS
# ==============================================================================

_BOOL_COLS: frozenset[str] = frozenset({
    "is_it", "is_vn", "is_valid", "is_duplicate",
    "is_negotiable", "is_exp_required",
})

# ==============================================================================
# 0.6 COMPANY NORMALIZE
# ==============================================================================

_COMPANY_TYPE_NORMALIZE = [
    (r'\bTNHH\s+MTV\b',                             'Công ty TNHH MTV'),
    (r'\bCông\s+ty\s+TNHH\s+Một\s+thành\s+viên\b',  'Công ty TNHH MTV'),
    (r'\bCty\s+TNHH\s+MTV\b',                       'Công ty TNHH MTV'),
    (r'\bCty\s+TNHH\b',                             'Công ty TNHH'),
    (r'\bCông\s+ty\s+Trách\s+nhiệm\s+hữu\s+hạn\b',  'Công ty TNHH'),
    (r'\bCTCP\b',                                   'Công ty Cổ phần'),
    (r'\bCty\s+CP\b',                               'Công ty Cổ phần'),
    (r'\bCông\s+ty\s+CP\b',                         'Công ty Cổ phần'),
    (r'\bCty\s+Cổ\s+phần\b',                        'Công ty Cổ phần'),
    (r'\bJSC\b',                                    'Công ty Cổ phần'),
    (r'\bDNTN\b',                                   'Doanh nghiệp Tư nhân'),
    (r'\bDoanh\s+nghiệp\s+TN\b',                    'Doanh nghiệp Tư nhân'),
    (r'\bTĐ\b',                                     'Tập đoàn'),
]
_SUFFIX_STRIP_RE = re.compile(
    r"\s*\b(?:capital|city|metropolitan|metropolis|metro|province"
    r"|municipality|region|urban area|greater|district|thủ đô"
    r"|thành phố|tỉnh|tp\.?)\b\s*",
    re.IGNORECASE | re.UNICODE,
)
_CONFIDENTIAL_RE = re.compile(
    r"(?:careerlink|vietnamworks|topcv|itviec|linkedin|jobstreet|timviecnhanh)"
    r"['\s]*(?:client|'s\s+client)|confidential\s+(?:company|employer)"
    r"|employer\s+brand|ẩn\s+danh",
    re.IGNORECASE | re.UNICODE,
)


def _normalize_company_name(name: str) -> str:
    result = (name or "").strip()
    for pattern, replacement in _COMPANY_TYPE_NORMALIZE:
        result, n = re.subn(pattern, replacement, result, flags=re.IGNORECASE)
        if n:
            break
    return result


def _clean_strict(text: str) -> str:
    if not text:
        return ""
    s = text.lower().strip()
    s = s.replace('đ', 'd')
    s = ''.join(c for c in unicodedata.normalize('NFKD', s)
                if not unicodedata.combining(c))
    noise_patterns = (
        r'\b(cong ty|cty|tnhh|mtv|co phan|ctcp|jsc|ltd|llc|inc'
        r'|group|tap doan|viet nam|vietnam|vn)\b'
    )
    s = re.sub(noise_patterns, ' ', s)
    s = re.sub(r'[^\w\s]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


def _match_search_typesense(ts, name_clean: str,
                             retries: int = 1, delay: float = 0.5) -> list:
    params_infix = {
        'q': name_clean, 'query_by': 'name_official',
        'per_page': 5, 'prefix': 'false', 'sort_by': '_text_match:desc',
    }
    params_prefix = {
        'q': name_clean, 'query_by': 'name_official',
        'per_page': 5, 'prefix': 'true', 'sort_by': '_text_match:desc',
    }
    last_err = None
    for attempt in range(retries):
        try:
            res  = ts.collections['companies'].documents.search(params_infix)
            hits = res.get('hits', [])
            if hits:
                return hits
            res2 = ts.collections['companies'].documents.search(params_prefix)
            return res2.get('hits', [])
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    raise last_err


# ==============================================================================
# 2. HELPERS
# ==============================================================================

def _s(val) -> str:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return ""
    return str(val).strip()


def parse_website(raw: str) -> str:
    return _s(raw).lower()


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
    _VN_UNITS = {
        "giây":  timedelta(seconds=1), "phút":  timedelta(minutes=1),
        "giờ":   timedelta(hours=1),   "ngày":  timedelta(days=1),
        "tuần":  timedelta(weeks=1),   "tháng": timedelta(days=30),
        "năm":   timedelta(days=365),
    }
    m = re.search(r"(\d+)\s+(giây|phút|giờ|ngày|tuần|tháng|năm)(?:\s+trước)?",
                  s, re.UNICODE)
    if m:
        try:
            ref = datetime.fromisoformat(ref_iso.replace(" ", "T"))
            n, unit = int(m.group(1)), m.group(2)
            return (ref - _VN_UNITS[unit] * n).date()
        except Exception:
            pass
    return None


def parse_dates(posted_raw: str, deadline_raw: str, scraped_at: str,
                job_desc: str = "") -> dict:
    ref      = _s(scraped_at)
    posted   = _to_date(posted_raw, ref)
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
    if posted is None and ref:
        try:
            posted = datetime.fromisoformat(ref.replace(" ", "T")).date()
        except Exception:
            pass

    return {"job_posted_at_clean": posted, "job_deadline_clean": deadline}

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
    r"^(?:\[[^\]]*\]|\([^)]*\))\s*",    # [bất kỳ] hoặr (bất kỳ) ở ĐẦU
    r"\s*(?:\[[^\]]*\]|\([^)]*\))\s*$", # [bất kỳ] hoặc (bất kỳ) ở CUỐI — có dấu phẩy
    r"\btuyển\s*(gấp|dụng)?\b",
    r"\bgấp\b",
    r"\bremote\b",
    r"\bfull[\s\-]?time\b",
    r"\bpart[\s\-]?time\b",
    r"\bhybrid\b",
    r"\bonsite\b",
    r"\bwork\s+from\s+home\b",
    r"\bwfh\b",
    r"\bnhiều\s+vị\s+trí\b",
    r"\b\d+\s+vị\s+trí\b",
    r"\b\d+\s+slots?\b",
    r"\bslot\b",
    r"\bnhiều\s+ưu\s+đãi\b",
    r"\bưu\s+đãi\s+hấp\s+dẫn\b",
    r"\blương\s+\d[\d\s\-–triệumk\.]+",
    r"\bsalary\s+upto\b[^,;]*",
    r"\bsalary\s+up\s+to\b[^,;]*",
    r"\s*[-–]\s*(?:làm việc\s+)?tại\s+[\w\s]{2,30}$",
    # dòng [Hưng Yên] ở cuối đã được cover bởi pattern thứ 2 → xóa đi
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

    # 1. JOB_TITLE_MAP — đã compile sẵn, sort dài trước
    for std_title, patterns in COMPILED_JOB_TITLE_MAP:
        for pat in patterns:
            if pat.search(text):
                return std_title

    # 2. ROLE_WORDS + TECH_DOMAIN inference — giữ nguyên
    role   = next((v for k, v in ROLE_WORDS.items()  if k in text), None)
    domain = next((v for k, v in TECH_DOMAIN.items() if k in text), None)
    if role:
        inferred = ROLE_DOMAIN_TO_TITLE.get(
            (role, domain),
            ROLE_DOMAIN_TO_TITLE.get((role, None)),
        )
        if inferred:
            return inferred

    # 3. NON_IT_TITLE_MAP — đã compile sẵn
    for en_title, patterns in COMPILED_NON_IT_TITLE_MAP:
        for pat in patterns:
            if pat.search(text):
                return en_title

    return None
def _has_it_signal(text: str) -> bool:
    return (
        any(k in text for k in TECH_DOMAIN)
        or any(k in text for k in _IT_TITLE_KW_FALLBACK)
        or any(k in text for k in _IT_EXTRA_KW)
    )


def parse_job_title(raw: str, job_category_raw: str = "") -> dict:
    raw_s    = _s(raw)
    cleaned  = _clean_job_title(raw_s)
    detected = _detect_job_title(cleaned) or _detect_job_title(raw_s)
    if detected is not None:
        if detected in IT_TITLES:
            return {
                "job_title_clean":    cleaned or raw_s,
                "job_title_detect":   detected,
                "job_category_clean": JOB_CATEGORY_MAP.get(detected, "IT - Khác"),
                "is_it":              True,
            }
        elif detected in JOB_CATEGORY_MAP:
            return {
                "job_title_clean":    cleaned or raw_s,
                "job_title_detect":   detected,
                "job_category_clean": JOB_CATEGORY_MAP[detected],
                "is_it":              False,
            }
        else:
            return {
                "job_title_clean":    cleaned or raw_s,
                "job_title_detect":   detected,
                "job_category_clean": "Non-IT",
                "is_it":              False,
            }
    text = (cleaned or raw_s).lower()
    if _has_it_signal(text):
        return {
            "job_title_clean":    cleaned or raw_s,
            "job_title_detect":   None,
            "job_category_clean": "IT - Khác",
            "is_it":              True,
        }
    return {
        "job_title_clean":    cleaned or raw_s,
        "job_title_detect":   None,
        "job_category_clean": "Non-IT",
        "is_it":              False,
    }


_LEGAL_STRIP_RE = re.compile(
    r'\b('
    r'tổng\s+công\s+ty|trách\s+nhiệm\s+hữu\s+hạn\s+một\s+thành\s+viên'
    r'|trách\s+nhiệm\s+hữu\s+hạn\s+mtv|trách\s+nhiệm\s+hữu\s+hạn'
    r'|một\s+thành\s+viên|ngân\s+hàng\s+thương\s+mại\s+cổ\s+phần'
    r'|ngân\s+hàng\s+tmcp|ngân\s+hàng|hợp\s+tác\s+xã'
    r'|cổ\s+phần|tập\s+đoàn|chi\s+nhánh|văn\s+phòng\s+đại\s+diện'
    r'|\bctcp\b|\btnhh\s+mtv\b|\btnhh\s+1tv\b|\btnhh\b'
    r'|\bjoint[\s\-]stock\s+company\b|\bjoint[\s\-]stock\b'
    r'|\bjsc\b|\bllc\b|\bltd\.?\b|\binc\.?\b'
    r'|\bcorp\.?\b|\bcorporation\b|\bco\.?\s*,?\s*ltd\.?\b'
    r'|\bpte\.?\s*ltd\.?\b|\bplc\b|\bgmbh\b|\bag\b'
    r'|\bgroup\b|\bholdings?\b|\bventures?\b'
    r'|\bservices?\b|\btrading\b|\bsolutions?\b'
    r'|\bsystems?\b|\btechnolog(?:y|ies)\b'
    r'|\binternational\b|\bglobal\b'
    r'|\bviệt\s*nam\b|\bvietnam\b|\bviet\s*nam\b'
    r'|\(việt\s*nam\)|\(vietnam\)|\bcn\b'
    r')\b',
    re.IGNORECASE | re.UNICODE,
)
_DASH_NOISE_RE  = re.compile(r'\s*(?:[-–—|])\s*(?=\S{20,}|\w[\w\s]{15,})', re.UNICODE)
_PAREN_NOISE_RE = re.compile(r'\([^)]{0,40}\)', re.UNICODE)


def _remove_vn_accents(text: str) -> str:
    text = text.replace('Đ', 'D').replace('đ', 'd')
    return ''.join(c for c in unicodedata.normalize('NFKD', text)
                   if not unicodedata.combining(c))


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
    return s

def parse_company_title(raw: str) -> dict:
    s = (raw or "").strip()
    
    if not s or s.lower() in ("null", "n/a", "none", "nan"):
        return {
            'company_title_clean':   'Unknown',
            'company_type':          'Unknown',
            'company_canonical_key': None,
        }
    
    sl = s.lower()
    if _CONFIDENTIAL_RE.search(sl):
        return {
            'company_title_clean':   'Confidential',
            'company_type':          'Confidential',
            'company_canonical_key': None,
        }
    
    company_type = None
    for pattern, ctype in COMPANY_TYPE_PATTERNS:
        if re.search(pattern, sl):
            company_type = ctype
            break
    
    normalized = _normalize_company_name(s)
    canonical  = _build_canonical_key(normalized)
    clean_name = normalized or s
    
    if not clean_name.strip():
        return {
            'company_title_clean':   'Unknown',
            'company_type':          'Unknown',
            'company_canonical_key': None,
        }
    
    return {
        'company_title_clean':   clean_name,
        'company_type':          company_type or 'Khác',
        'company_canonical_key': canonical or None,
    }

def _resolve_province(raw: str) -> tuple[str | None, str]:
    loc = raw.lower().strip()
    province = PROVINCE_CANONICAL.get(loc)
    if province is None:
        for key in GEO_KEYS_SORTED:
            if key in loc:
                province = PROVINCE_CANONICAL[key]
                break
    if province is None:
        stripped = _SUFFIX_STRIP_RE.sub(" ", loc).strip()
        if stripped and stripped != loc:
            province = PROVINCE_CANONICAL.get(stripped)
            if province is None:
                for key in GEO_KEYS_SORTED:
                    if key in stripped:
                        province = PROVINCE_CANONICAL[key]
                        break

    region = REGION_MAP.get(province, "Khác") if province else "Khác"
    return province, region
def parse_location(raw: str) -> list[dict]:
    if not raw:
        return [{"location_province": "Khác", "location_region": "Khác", "is_vn": False}]
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
            result.append({"location_province": province, "location_region": region, "is_vn": True})
        else:
            result.append({"location_province": "Khác", "location_region": "Khác", "is_vn": False})
    return result or [{"location_province": "Khác", "location_region": "Khác", "is_vn": False}]


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


_CURRENCY_TABLE: dict[str, float] = {
    "USD": 25_500.0, "VND": 1.0,      "JPY": 168.0,
    "EUR": 27_500.0, "SGD": 19_000.0, "GBP": 32_000.0,
    "AUD": 16_500.0, "CAD": 18_500.0, "KRW": 18.5,
    "CNY": 3_500.0,  "THB": 700.0,
}
_CURRENCY_DETECT: list[tuple[str, str]] = [
    (r"\$|usd|\bđô\b|dollar",                 "USD"),
    (r"vnđ|vnd|đ(?!ồng)|đồng|triệu|triêu|nghìn\s*đ|ngàn\s*đ", "VND"),
    (r"\bjpy\b|¥|yen",                         "JPY"),
    (r"\beur\b|€|euro",                        "EUR"),
    (r"\bsgd\b|s\$",                           "SGD"),
    (r"\bgbp\b|£|pound",                       "GBP"),
    (r"\baud\b|a\$",                           "AUD"),
    (r"\bcad\b|ca\$",                          "CAD"),
    (r"\bkrw\b|₩|won",                         "KRW"),
    (r"\bcny\b|rmb|yuan",                      "CNY"),
    (r"\bthb\b|฿|baht",                        "THB"),
]
_NEG_KW = [
    "thỏa thuận", "thương lượng", "competitive", "negotiable",
    "attractive", "market rate", "commensurate", "tbd", "t.b.d",
    "to be discussed", "to be confirmed", "sẽ thảo luận",
]
_YEAR_RE  = re.compile(r"/\s*year|per\s+year|/\s*yr|/\s*năm|/\s*annum|annually|hàng\s*năm|mỗi\s*năm|\bpa\b|\bp\.a\b", re.I)
_MONTH_RE = re.compile(r"/\s*month|per\s+month|/\s*tháng|/\s*mo\b|hàng\s*tháng|mỗi\s*tháng", re.I)
_HOUR_RE  = re.compile(r"/\s*hour|per\s+hour|/\s*hr\b|/\s*giờ|\bphp\b", re.I)
_MONTHLY_SANITY: dict[str, tuple[float, float]] = {
    "USD": (50, 50_000), "VND": (500_000, 500_000_000), "JPY": (50_000, 5_000_000),
    "EUR": (500, 30_000), "SGD": (500, 30_000), "GBP": (500, 30_000),
    "AUD": (500, 30_000), "CAD": (500, 30_000), "KRW": (500_000, 50_000_000),
    "CNY": (1_000, 200_000), "THB": (5_000, 500_000),
}
_DATE_PAT    = re.compile(r"\b\d{1,2}[/\-]\d{1,2}[/\-]\d{4}\b|\b\d{4}[/\-]\d{2}[/\-]\d{2}\b")
_SUFFIX_RE   = re.compile(r"(?P<num>\d+(?:[.,]\d+)?)\s*(?P<sfx>triệu|triêu\b|tr\b|[kKmM](?!\w))", re.UNICODE)
_RANGE_SUFFIX_RE = re.compile(
    r"(?P<n1>\d+(?:[.,]\d+)?)\s*[-–—~]\s*(?P<n2>\d+(?:[.,]\d+)?)\s*"
    r"(?P<sfx>triệu|triêu\b|tr\b|[kKmMđ](?!\w))", re.UNICODE)


def _detect_currency(text):
    for pattern, code in _CURRENCY_DETECT:
        if re.search(pattern, text, re.I):
            return code
    if re.search(r"\d\s*[kK]\b", text) and not re.search(r"triệu|triêu|đồng|vnđ|vnd|\bđ\b", text, re.I):
        return "USD"
    return "VND"


def _normalize_separators(text):
    while re.search(r"\d\.\d{3}(?!\d)", text):
        text = re.sub(r"(\d)\.(\d{3})(?!\d)", r"\1\2", text)
    while re.search(r"\d,\d{3}(?!\d)", text):
        text = re.sub(r"(\d),(\d{3})(?!\d)", r"\1\2", text)
    return text.replace(",", ".")


def _apply_suffix(num_str, sfx, currency):
    val = float(num_str.replace(",", "."))
    sfx_lower = sfx.lower()
    if sfx_lower in ("triệu", "triêu", "tr", "m"):
        return val * 1_000_000
    if sfx_lower == "k":
        return val * 1_000
    return val


def _extract_salary_numbers(text, currency):
    text = _DATE_PAT.sub(" ", text)
    t = text.replace("~", "-").replace("–", "-").replace("—", "-")
    t = _normalize_separators(t)
    results: list[float] = []
    consumed_spans: list[tuple[int, int]] = []
    for m in _RANGE_SUFFIX_RE.finditer(t):
        sfx = m.group("sfx")
        for key in ("n1", "n2"):
            results.append(_apply_suffix(m.group(key), sfx, currency))
        consumed_spans.append(m.span())
    for m in _SUFFIX_RE.finditer(t):
        s_pos, e_pos = m.span()
        if any(cs <= s_pos < ce for cs, ce in consumed_spans):
            continue
        results.append(_apply_suffix(m.group("num"), m.group("sfx"), currency))
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


def _has_real_number(text):
    return bool(re.search(r"\d", text))


def _split_main_bonus(text):
    m = re.search(
        r"(.*?\d.*?)(?:\.\s+[A-ZÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐ]|\n|;|,\s+(?:including|plus|with|and\s+(?:bonus|benefits?)))",
        text, re.I | re.DOTALL)
    return m.group(1) if m else text


def _sanity_check(val, currency):
    if val is None:
        return val
    lo, hi = _MONTHLY_SANITY.get(currency, (0, float("inf")))
    if val < lo or val > hi:
        return None
    return val


def parse_compensation(raw: str) -> dict:
    _base = {"salary_min": None, "salary_max": None, "salary_currency": None,
             "conversion_rate": None, "is_negotiable": True, "salary_type": "negotiable"}
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
    nums = _extract_salary_numbers(text_main, currency) or _extract_salary_numbers(text, currency)
    if not nums:
        return _base
    nums = [n for n in nums if n > 0]
    if not nums:
        return _base
    lo_kw = ["từ", "from", "trên", "hơn", "minimum", "tối thiểu", "at least", "ít nhất", "starting", "bắt đầu từ"]
    hi_kw = ["đến", "tới", "up to", "upto", "up-to", "dưới", "maximum", "tối đa", "không quá", "lên đến", "tới"]
    is_hi_only = any(kw in text_main for kw in hi_kw) or bool(re.search(r"\bupto\b|\bup[\s\-]+to\b", text_main))
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
        sal_min = round(sal_min / 12, 2) if sal_min else None
        sal_max = round(sal_max / 12, 2) if sal_max else None
    elif period == "hour":
        sal_min = round(sal_min * 160, 2) if sal_min else None
        sal_max = round(sal_max * 160, 2) if sal_max else None
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
    if explicit_yearly:
        salary_type = "yearly"
    elif explicit_hourly:
        salary_type = "hourly"
    elif re.search(r"per\s*task|/\s*task|by\s*task", s.lower()):
        salary_type = "per_task"
    else:
        salary_type = "monthly"
    return {
        "salary_min":      int(sal_min) if sal_min is not None else None,
        "salary_max":      int(sal_max) if sal_max is not None else None,
        "salary_currency": currency,
        "conversion_rate": rate,
        "is_negotiable":   False,
        "salary_type":     salary_type,
    }


_EXP_REQUIREMENT_KW = [
    "tối thiểu", "yêu cầu", "cần có", "có ít nhất", "ít nhất",
    "kinh nghiệm làm việc", "kinh nghiệm tối thiểu",
    "năm kinh nghiệm", "năm kn", "năm kinh nghiêm", "tháng kinh nghiệm",
    "years? of experience", "years? experience", "year experience",
    "yrs? of experience", "minimum.*experience", "at least.*experience",
    "required.*experience", "experience required",
    r"\d+\+\s*years?", r"\d+\s+to\s+\d+\s+years?", r"\d+[-–]\d+\s+years?",
]
_EXP_REQ_RE = re.compile("|".join(_EXP_REQUIREMENT_KW), re.IGNORECASE | re.UNICODE)


def _normalize_exp_text(text):
    return re.sub(r"(\d),(\d)(?!\d)", r"\1.\2", text)

def _extract_exp_nums(text):
    return [float(n) for n in re.findall(r"\d+(?:\.\d+)?", _normalize_exp_text(text))]

def _parse_exp_nums(text, nums):
    exp_min = exp_max = None
    if "dưới 1" in text:
        exp_min, exp_max = 0.0, 1.0
    elif any(kw in text for kw in ["trên", "hơn", "over", "minimum", "tối thiểu", "at least", "ít nhất"]) or re.search(r"\d+\+", text):
        exp_min = nums[0]
    elif any(kw in text for kw in ["dưới", "less than", "maximum", "tối đa", "up to"]):
        exp_min, exp_max = 0.0, nums[0]
    elif len(nums) >= 2:
        exp_min, exp_max = min(nums[0], nums[1]), max(nums[0], nums[1])
    else:
        exp_min = exp_max = nums[0]
    if exp_min is None and exp_max is None:
        return {"exp_min_yr": None, "exp_max_yr": None, "is_exp_required": None}
    return {"exp_min_yr": exp_min, "exp_max_yr": exp_max, "is_exp_required": True}


def parse_experience(raw: str, job_desc: str = "", job_req: str = "") -> dict:
    base = {"exp_min_yr": None, "exp_max_yr": None, "is_exp_required": None}
    raw_s = _s(raw)
    if raw_s:
        combined = _normalize_exp_text(raw_s.lower())
        if any(kw in combined for kw in NO_EXP_KW):
            return {"exp_min_yr": 0.0, "exp_max_yr": 0.0, "is_exp_required": False}
        nums = _extract_exp_nums(combined)
        if nums:
            if "tháng" in combined and "năm" not in combined:
                nums = [round(n / 12, 2) for n in nums]
            return _parse_exp_nums(combined, nums)
    for source_text in [_s(job_req), _s(job_desc)]:
        if not source_text:
            continue
        source_lower = _normalize_exp_text(source_text.lower())
        for sent in re.split(r"[.\n\r]", source_lower):
            sent = sent.strip()
            if not sent or not _EXP_REQ_RE.search(sent):
                continue
            if any(kw in sent for kw in NO_EXP_KW):
                return {"exp_min_yr": 0.0, "exp_max_yr": 0.0, "is_exp_required": False}
            nums = [n for n in _extract_exp_nums(sent) if 0 <= n <= 50]
            if not nums:
                continue
            if "tháng" in sent and "năm" not in sent:
                nums = [round(n / 12, 2) for n in nums]
            result = _parse_exp_nums(sent, nums)
            if result.get("exp_min_yr") is not None or result.get("exp_max_yr") is not None:
                return result
    return base


def _match_level_in_text(text):
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


def parse_level(level_raw, job_title_raw, exp_min, exp_max, job_desc="", job_req=""):
    for source in [_s(level_raw), _s(job_title_raw), _s(job_desc)[:500], _s(job_req)[:500]]:
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


def parse_jd_fields(job_desc, job_req, job_category_raw, website):
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


def parse_education(edu_raw, job_desc, job_req):
    for source in [_s(edu_raw), _s(job_req), _s(job_desc)]:
        if not source:
            continue
        t = source.lower()
        for label, kws in EDUCATION_MAP.items():
            if label == "Không yêu cầu":
                continue
            if any(k in t for k in kws):
                return label
    return "Không yêu cầu"


def parse_company_size(raw):
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


_UNKNOWN_INDUSTRY = {"industry_level1": "Không xác định", "industry_level2": "Không xác định"}


def parse_industry(raw, major=None):
    text = (_s(raw) + " " + _s(major)).lower()
    if not text.strip():
        return _UNKNOWN_INDUSTRY.copy()
    for entry in INDUSTRY_TREE:
        if any(kw in text for kw in entry["kw"]):
            return {"industry_level1": entry["l1"], "industry_level2": entry["l2"]}
    return _UNKNOWN_INDUSTRY.copy()


def parse_number_recruit(num_raw, job_title_raw):
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
                r"(\d+)\s+kỹ sư", r"(\d+)\s+chuyên viên", r"số lượng\s*[:\-]?\s*(\d+)"]:
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

    def add(self, run_id, src_id, job_url, field, raw_val, err_type, detail=""):
        self._rows.append({
            "run_id": run_id, "src_id": src_id, "job_url": job_url,
            "field_name": field,
            "raw_value":  str(raw_val)[:500] if raw_val is not None else None,
            "error_type": err_type, "error_detail": detail,
        })

    def __len__(self):
        return len(self._rows)

    def to_df(self):
        return pd.DataFrame(self._rows) if self._rows else pd.DataFrame()


def _nan_to_none(rows):
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
# 3.5 DEDUP HELPERS
# ==============================================================================

_TECH_IN_TITLE: list[str] = [
    "java", "python", "golang", " go ", "nodejs", "node.js",
    "php", "react", "angular", "vue", "flutter",
    "android", "ios", "swift", "kotlin",
    ".net", "c#", "ruby", "scala", "rust",
]


def _title_dedup_key(title_detect, title_clean):
    td = "" if (title_detect is None or isinstance(title_detect, float)) else str(title_detect)
    tc = "" if (title_clean  is None or isinstance(title_clean,  float)) else str(title_clean)
    base = td.strip().lower()
    if not base:
        return tc.strip().lower()
    tech = next((t.strip() for t in _TECH_IN_TITLE if t in tc.lower()), "")
    return f"{base}::{tech}" if tech else base


# ==============================================================================
# 4. ETL CLASS
# ==============================================================================

class RecruitmentETL:
    def __init__(self, db_url: str):
        self.engine = sqlalchemy.create_engine(db_url)
        self._ts = None
        if TYPESENSE_ENABLED:
            try:
                self._ts = typesense.Client({
                    "nodes": [{"host": TS_CONFIG["host"],
                               "port": TS_CONFIG["port"],
                               "protocol": "http"}],
                    "api_key": TS_CONFIG["api_key"],
                    "connection_timeout_seconds": TS_CONFIG["timeout"],
                })
                self._ts.collections['companies'].documents.search(
                    {'q': 'test', 'query_by': 'name_official', 'per_page': 1}
                )
                print("✅ Typesense kết nối OK")
            except Exception as e:
                print(f"⚠️  Typesense không kết nối được: {e} — company match bị tắt")
                self._ts = None

    def _start_log(self, mode: str, target_date) -> int:
        with self.engine.begin() as conn:
            result = conn.execute(sqlalchemy.text(f"""
                INSERT INTO {LOG_TABLE}
                    (run_date, mode, target_date, started_at, status)
                VALUES
                    (CURRENT_DATE, :mode, :dt, NOW(), 'RUNNING')
                RETURNING run_id
            """), {"mode": mode, "dt": str(target_date) if target_date else None})
            return result.scalar()

    def _finish_log(self, run_id: int, counts: dict, status: str, note: str = None):
        with self.engine.begin() as conn:
            conn.execute(sqlalchemy.text(f"""
                UPDATE {LOG_TABLE} SET
                    finished_at  = NOW(),
                    duration_sec = EXTRACT(EPOCH FROM (NOW() - started_at))::int,
                    total_input  = :inp,
                    total_output = :out,
                    new_rows     = :new,
                    updated_rows = :upd,
                    error_rows   = :err,
                    status       = :status,
                    note         = :note
                WHERE run_id = :rid
            """), {
                "inp":    counts.get("input",   0),
                "out":    counts.get("output",  0),
                "new":    counts.get("new",     0),
                "upd":    counts.get("updated", 0),
                "err":    counts.get("errors",  0),
                "status": status,
                "note":   note,
                "rid":    run_id,
            })

    def _load(self, mode: str, date_str: str | None):
        """
        [LOCAL] Đọc từ staging_jobs thay vì jobs.
        staging_jobs không có cột is_valid → bỏ filter is_valid cho mode=all.
        """
        with self.engine.connect() as conn:
            if mode == "all":
                df = pd.read_sql(f"SELECT * FROM {SRC_TABLE} where ai_processed = True", conn)
                target_date = None
            elif mode == "date" and date_str:
                df = pd.read_sql(
                    f"SELECT * FROM {SRC_TABLE} "
                    f"WHERE scraped_at::date = '{date_str}' and ai_processed::date = True", conn
                )
                target_date = date_str
            else:  # today
                df = pd.read_sql(
                    f"SELECT * FROM {SRC_TABLE} "
                    f"WHERE scraped_at::date = CURRENT_DATE and ai_processed = True", conn
                )
                target_date = date.today()
        print(f"   Đọc {len(df):,} rows từ {SRC_TABLE}.")
        return df, target_date

    def _transform(self, df: pd.DataFrame,
                   run_id: int) -> tuple[pd.DataFrame, ErrorCollector]:
        ec  = ErrorCollector()
        now = datetime.now()
        rows_out: list[dict] = []

        for _, r in df.iterrows():
            src_id  = r.get("id")
            job_url = _s(r.get("job_url"))
            if not _s(r.get("job_title")).strip():
                ec.add(run_id, src_id, job_url, "job_title", None, "SKIP", "Không có job_title")
                continue
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
                "job_posted_at":    _s(r.get("job_posted_at")),
                "job_deadline":     _s(r.get("job_deadline")),
                "is_valid":         bool(r.get("is_valid")) if r.get("is_valid") is not None else True,
                "error_log":        _s(r.get("error_log")),
                "is_duplicate":     False,
                "duplicate_of_id":  None,
                "dedup_method":     None,
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
            _try("job_title", parse_job_title, row["job_title"], row["job_category"],
                 fallback={"job_title_clean": None, "job_title_detect": None,
                           "job_category_clean": "Non-IT", "is_it": False})
            _try("company_title", parse_company_title, row["company_title"],
                 fallback={"company_title_clean": "Unknown", "company_type": "Unknown",
                           "company_canonical_key": "unknown"})
            _try("job_type", parse_work_style,
                 row["job_type"], row["work_mode"], row["job_description"],
                 fallback={"job_type_clean": "Full-time", "work_mode_clean": "Onsite"})
            _try("compensation", parse_compensation, row["compensation"],
                 fallback={"salary_min": None, "salary_max": None,
                           "salary_currency": None, "conversion_rate": None,
                           "is_negotiable": True})
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
                    row["education_level"], row["job_description"], row["job_requirement"])
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
                    row["number_recruit"], row["job_title"])
            except Exception as e:
                ec.add(run_id, src_id, job_url, "number_recruit_clean",
                       row["number_recruit"], "PARSE_FAIL", str(e))
                row["number_recruit_clean"] = 1
            try:
                locs = parse_location(row["location"])
            except Exception as e:
                ec.add(run_id, src_id, job_url, "location",
                       row["location"], "PARSE_FAIL", str(e))
                locs = [{"location_province": "Khác", "location_region": "Khác", "is_vn": False}]
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

            _VARCHAR_LIMITS = {
                "job_title_clean":    500,
                "job_title_detect":   255,
                "job_category_clean": 100,
                "level_clean":         30,
                "job_type_clean":      30,
                "work_mode_clean":     30,
                "salary_currency":     10,
                "education_clean":     30,
            }

            def _truncate_varchars(d: dict) -> dict:
                for col, max_len in _VARCHAR_LIMITS.items():
                    if col in d and isinstance(d[col], str):
                        d[col] = d[col][:max_len]
                return d

            def _clean_row(d: dict) -> dict:
                result = {}
                for k, v in d.items():
                    if v is None:
                        result[k] = None
                    elif isinstance(v, float) and np.isnan(v):
                        result[k] = None
                    elif isinstance(v, str) and v.lower() == "nan":
                        result[k] = None
                    elif k in _BOOL_COLS:
                        if isinstance(v, bool):
                            result[k] = v
                        elif v is not None:
                            result[k] = bool(int(v))
                        else:
                            result[k] = None
                    else:
                        result[k] = v
                return _truncate_varchars(result)

            update_cols = [
                "job_title_clean", "job_title_detect", "job_category_clean", "is_it",
                "job_type_clean", "work_mode_clean",
                "salary_min", "salary_max", "salary_currency",
                "conversion_rate", "is_negotiable", "salary_type",
                "exp_min_yr", "exp_max_yr", "is_exp_required", "level_clean",
                "hard_skills", "soft_skills", "major", "certifications", "languages",
                "education_clean", "company_size_min", "company_size_max",
                "industry_level1", "industry_level2", "number_recruit_clean",
                "job_posted_at_clean", "job_deadline_clean",
                "etl_run_id", "etl_processed_at",
            ]

            # --- BẮT ĐẦU ĐOẠN SỬA ---
            counts = {"new": 0, "updated": 0}
            generated_ids = []

            with self.engine.begin() as conn:
                for i in range(0, len(df), 20):
                    chunk = df.iloc[i:i+20]
                    
                    # Làm sạch dữ liệu của chunk trước
                    cleaned_rows = [_clean_row(r) for r in chunk.to_dict("records")]
                    
                    for row_dict in cleaned_rows:
                        # Bắt buộc loại bỏ cột ID khỏi tập dữ liệu đầu vào để kích hoạt cột tự tăng SERIAL ở Postgres
                        row_dict.pop("etl_id", None)
                        row_dict.pop("id", None)
                        
                        # Lấy danh sách các cột thực tế gửi đi (không bao gồm id)
                        cols = list(row_dict.keys())
                        placeholders = ", ".join([f":{c}" for c in cols])
                        col_list     = ", ".join(cols)
                        
                        on_conflict = ", ".join(
                            f"{c} = EXCLUDED.{c}"
                            for c in update_cols if c in cols
                        )
                        
                        # Sử dụng RETURNING xmax, etl_id để kiểm tra trạng thái và lấy ID tự sinh từ Postgres
                        sql = f"""
                            INSERT INTO {FACT_TABLE} ({col_list})
                            VALUES ({placeholders})
                            ON CONFLICT (job_url, location_province) DO UPDATE SET {on_conflict}
                            RETURNING xmax, etl_id
                        """
                        
                        result = conn.execute(sqlalchemy.text(sql), row_dict).fetchone()
                        if result:
                            xmax, new_id = result[0], result[1]
                            generated_ids.append(new_id)
                            
                            # Trong PostgreSQL: xmax = 0 tức là bản ghi mới (INSERT), xmax != 0 là bản ghi cũ bị đè (UPDATE)
                            if xmax == 0:
                                counts["new"] += 1
                            else:
                                counts["updated"] += 1
                        else:
                            generated_ids.append(None)
                            
                    time.sleep(0.2)
            df["etl_id"] = generated_ids

            print(f"   ✅ UPSERT hoàn thành: {counts['new']:,} dòng mới, {counts['updated']:,} dòng cập nhật.")
            return counts

    def _save_errors(self, ec: ErrorCollector, run_id: int):
        df_err = ec.to_df()
        if df_err.empty:
            return
        df_err.to_sql(ERROR_TABLE, self.engine, if_exists="append",
                      index=False, chunksize=500)
        print(f"   ⚠ Ghi {len(df_err)} error records.")

    def _ts_client(self):
        return typesense.Client({
            'nodes': [{'host': TS_CONFIG["host"], 'port': TS_CONFIG["port"],
                       'protocol': 'http'}],
            'api_key':                    TS_CONFIG["api_key"],
            'connection_timeout_seconds': TS_CONFIG["timeout"],
        })

    def _match_one_company(self, ts, name_raw: str, fallback: str) -> dict:
        s_name = (name_raw or "").strip()
        _base  = {"raw_name": name_raw, "company_name_clean": fallback, "status": "no_match"}
        if not s_name:
            return {**_base, "company_name_clean": "Unknown", "status": "empty"}
        if any(kw in s_name.lower() for kw in ["client", "confidential", "ẩn danh"]):
            return {**_base, "company_name_clean": "Confidential", "status": "confidential"}
        cleaned_source = _clean_strict(s_name)
        if not cleaned_source:
            return _base
        try:
            hits = _match_search_typesense(ts, cleaned_source)
        except Exception:
            return _base
        if not hits:
            return _base
        for h in hits:
            official_name  = h["document"]["name_official"]
            cleaned_target = _clean_strict(official_name)
            if (_fuzz.ratio(cleaned_source, cleaned_target) >= 100
                    and _fuzz.token_sort_ratio(cleaned_source, cleaned_target) >= 100):
                return {"raw_name": name_raw, "company_name_clean": official_name, "status": "ok"}
        return _base

    def _match_and_update_companies(self, run_id: int):
        if not TYPESENSE_ENABLED or self._ts is None:
            print("   ⏭️  Company match bị tắt (TYPESENSE_ENABLED=false)")
            with self.engine.begin() as conn:
                conn.execute(sqlalchemy.text(f"""
                    UPDATE {FACT_TABLE}
                    SET    company_name_clean = company_title_clean
                    WHERE  etl_run_id = :rid
                      AND  company_name_clean IS NULL
                """), {"rid": run_id})
            return

        print("\n⏳ [3.5/5] Đối sánh tên công ty với Typesense...")
        with self.engine.connect() as conn:
            df_co = pd.read_sql(f"""
                SELECT DISTINCT company_title, company_title_clean
                FROM {FACT_TABLE}
                WHERE etl_run_id = {run_id}
                  AND company_title IS NOT NULL
            """, conn)

        if df_co.empty:
            print("   Không có công ty nào cần xử lý.")
            return

        ts      = self._ts_client()
        results = []
        for _, r in tqdm(df_co.iterrows(), total=len(df_co), desc="   Matching", unit="cty"):
            results.append(self._match_one_company(
                ts, name_raw=r["company_title"], fallback=r["company_title_clean"]))
            time.sleep(0.01)

        df_res = pd.DataFrame(results)
        with self.engine.begin() as conn:
            for _, row in df_res.iterrows():
                conn.execute(sqlalchemy.text(f"""
                    UPDATE {FACT_TABLE}
                    SET    company_name_clean = :cname
                    WHERE  etl_run_id = :run_id AND company_title = :raw
                """), {"cname": row["company_name_clean"],
                       "run_id": run_id, "raw": row["raw_name"]})

        n_ok = int((df_res["status"] == "ok").sum())
        print(f"   ✅ {len(df_res):,} công ty ({n_ok} Typesense | {len(df_res)-n_ok} fallback).")

    def run(self, mode: str = "today", date_str: str | None = None):
        print(f"\n{'=' * 62}")
        print(f"  ETL START [{datetime.now():%Y-%m-%d %H:%M:%S}]  mode={mode}")
        print(f"  DB: {self.engine.url.database} @ {self.engine.url.host}")
        print(f"{'=' * 62}")

        print("\n⏳ [1/4] Load từ staging_jobs...")
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
            df_clean, ec     = self._transform(df_raw, run_id)
            counts["output"] = len(df_clean)
            counts["errors"] = len(ec)

            print("\n⏳ [3/4] Save fact_jobs_etl...")
            saved             = self._save_fact(df_clean)
            counts["new"]     = saved["new"]
            counts["updated"] = saved["updated"]

            print("\n⏳ [3.5/4] Match company (Typesense)...")
            self._match_and_update_companies(run_id)

            print("\n⏳ [4/4] Save errors...")
            self._save_errors(ec, run_id)

            # NOTE: Dedup + Load DW chạy riêng bởi dedup.py (bước tiếp theo trong pipeline)

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
    parser = argparse.ArgumentParser(description="Recruitment ETL — Local PostgreSQL")
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


