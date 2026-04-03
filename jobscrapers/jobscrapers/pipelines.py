from itemadapter import ItemAdapter
import mysql.connector
import re
from datetime import datetime, timedelta


# =========================================================
#  Lookup tables — Vietnamworks API trả về ID thay vì text
# =========================================================

VW_JOB_TYPE = {
    "1": "Toàn thời gian",
    "2": "Bán thời gian",
    "3": "Hợp đồng",
    "4": "Thực tập",
    "5": "Tạm thời",
}

VW_EDUCATION = {
    "0": "",
    "1": "Trung học",
    "2": "Trung cấp",
    "3": "Cao đẳng",
    "4": "Đại học",
    "5": "Thạc sĩ",
    "6": "Tiến sĩ",
}

# =========================================================
#  Helpers dùng chung
# =========================================================

def _clean_date(text: str) -> str:
    """
    Chuẩn hoá ngày về dạng dd/mm/yyyy.
      "08/04/2026 (Còn 5 ngày)"   → "08/04/2026"
      "2026-04-30T10:16:00"       → "30/04/2026"
      "2026-04-03T10:40:53+07:00" → "03/04/2026"
      "20/03/2026"                → "20/03/2026"
    """
    if not text:
        return ""
    text = text.strip()
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
    m = re.match(r"(\d{2}/\d{2}/\d{4})", text)
    if m:
        return m.group(1)
    return text


def _clean_nbsp(text: str) -> str:
    """Thay \xa0 và \r\n thừa bằng ký tự thường."""
    if not text:
        return ""
    text = text.replace("\xa0", " ").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r" {2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _relative_to_date(relative_text: str, scraped_at_iso: str) -> str:
    """
    Chuyển thời gian tương đối sang dd/mm/yyyy tính từ scraped_at.
      "1 week ago"      → "27/03/2026"
      "Posted 6 days ago" → "27/03/2026"
      "Reposted 2 weeks ago" → ...
    Trả về chuỗi gốc nếu không parse được.
    """
    if not relative_text:
        return ""

    text = relative_text.lower().strip()
    text = re.sub(r"^reposted\s+", "", text)
    text = re.sub(r"^posted\s+",   "", text)

    try:
        base = datetime.fromisoformat(scraped_at_iso)
    except Exception:
        base = datetime.now()

    m = re.match(
        r"(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago",
        text, re.IGNORECASE,
    )
    if m:
        n    = int(m.group(1))
        unit = m.group(2).lower()
        delta_map = {
            "second": timedelta(seconds=n),
            "minute": timedelta(minutes=n),
            "hour"  : timedelta(hours=n),
            "day"   : timedelta(days=n),
            "week"  : timedelta(weeks=n),
            "month" : timedelta(days=n * 30),
            "year"  : timedelta(days=n * 365),
        }
        return (base - delta_map[unit]).strftime("%d/%m/%Y")

    if "just now" in text:
        return base.strftime("%d/%m/%Y")

    return relative_text


_NEGOTIABLE = [
    "thỏa thuận", "thoả thuận", "thương lượng",
    "cạnh tranh", "mới cập nhật", "negotiate",
    "you'll love it",
]

_ITVIEC_WORK_MODE_MAP = {
    "at office": "On-site",
    "hybrid"   : "Hybrid",
    "remote"   : "Remote",
}

_ITVIEC_WORK_MODE_VALUES = set(_ITVIEC_WORK_MODE_MAP.keys())

_LI_TIME_PAT = re.compile(
    r"(?:reposted\s+)?(?:\d+\s+(?:second|minute|hour|day|week|month|year)s?\s+ago|just\s+now)",
    re.IGNORECASE,
)

_IT_POSTED_PAT = re.compile(r"^posted\s+(.+)$", re.IGNORECASE)


# =========================================================
#  Core: clean_dict()
#  Hàm thuần Python — không phụ thuộc Scrapy.
#  Cả CleaningPipeline (Scrapy) lẫn Selenium spider đều gọi hàm này.
# =========================================================

def clean_dict(raw: dict) -> dict:
    """
    Nhận dict thô từ bất kỳ spider nào, trả về dict đã chuẩn hoá.

    Xử lý đặc thù theo raw['website']:
      - careerlink   : job_posted_at = "|" → ""
      - careerviet   : company_size bắt đầu bằng ":" → strip
      - vietnamwork  : job_type/education_level là ID số → map sang text
                       compensation "0 - X" → bỏ "0 -"
      - linkedin     : raw_about_job → job_description
                       job_posted_at dạng relative → dd/mm/yyyy
      - itviec       : job_posted_at "Posted X ago" → dd/mm/yyyy
                       work_mode "At office" → "On-site"
                       job_category list → string
                       skills list → nối vào job_description
                       tự sửa bug lệch cột work_mode / job_posted_at
    """
    item    = {}
    website = (raw.get("website") or "").lower().strip()

    # ── 1. Các field string chuẩn — strip whitespace ──────
    STR_FIELDS = [
        "website", "job_title", "company_title", "location",
        "experience", "job_type", "work_mode", "level", "job_url",
        "company_size", "company_industry", "number_recruit",
        "education_level", "job_posted_at", "job_deadline",
    ]
    for field in STR_FIELDS:
        val = raw.get(field)
        if isinstance(val, str):
            item[field] = re.sub(r"\s+", " ", val).strip()
        else:
            item[field] = ""

    # ── 2. List/text fields → string + clean nbsp ─────────
    for field in ("job_description", "job_requirement"):
        val = raw.get(field)
        if isinstance(val, list):
            item[field] = "\n".join(
                _clean_nbsp(v) for v in val if isinstance(v, str) and v.strip()
            )
        elif isinstance(val, str):
            item[field] = _clean_nbsp(val)
        else:
            item[field] = ""

    # job_category: list → ", " join  |  string → giữ nguyên
    cat = raw.get("job_category")
    if isinstance(cat, list):
        item["job_category"] = ", ".join(v.strip() for v in cat if v.strip())
    elif isinstance(cat, str):
        item["job_category"] = re.sub(r"\s+", " ", cat).strip()
    else:
        item["job_category"] = ""

    # ── 3. scraped_at → ISO ───────────────────────────────
    scraped = raw.get("scraped_at")
    if isinstance(scraped, datetime):
        scraped_iso = scraped.isoformat()
    elif isinstance(scraped, str) and scraped:
        scraped_iso = scraped.replace(" ", "T", 1)
    else:
        scraped_iso = datetime.now().isoformat()
    item["scraped_at"] = scraped_iso

    # ── 4. Xử lý đặc thù từng website ────────────────────
    if website == "careerlink":
        if item["job_posted_at"] in ("|", "-"):
            item["job_posted_at"] = ""
        item["job_posted_at"] = _clean_date(item["job_posted_at"])
        item["job_deadline"]  = _clean_date(item["job_deadline"])

    elif website == "careerviet":
        size = item["company_size"]
        if size.startswith(":"):
            item["company_size"] = size.split("|")[0].replace(":", "").strip()
        item["job_posted_at"] = _clean_date(item["job_posted_at"])
        item["job_deadline"]  = _clean_date(item["job_deadline"])

    elif website == "vietnamwork":
        item["job_type"]        = VW_JOB_TYPE.get(item["job_type"], item["job_type"])
        item["education_level"] = VW_EDUCATION.get(item["education_level"], item["education_level"])
        item["job_posted_at"]   = _clean_date(item["job_posted_at"])
        item["job_deadline"]    = _clean_date(item["job_deadline"])

    elif website == "linkedin":
        # raw_about_job → job_description
        raw_desc = raw.get("raw_about_job") or raw.get("job_description") or ""
        item["job_description"] = _clean_nbsp(raw_desc)
        item["job_requirement"] = ""

        # job_posted_at: relative → dd/mm/yyyy
        posted = item["job_posted_at"]
        if _LI_TIME_PAT.search(posted):
            item["job_posted_at"] = _relative_to_date(posted, scraped_iso)
        # job_deadline không có trên LinkedIn
        item["job_deadline"] = ""

    elif website == "itviec":
        # job_posted_at: "Posted 6 days ago" → dd/mm/yyyy
        posted = item["job_posted_at"]
        m = _IT_POSTED_PAT.match(posted)
        relative_part = m.group(1).strip() if m else posted
        item["job_posted_at"] = _relative_to_date(relative_part, scraped_iso)
        item["job_deadline"]  = ""

        # work_mode: "At office" → "On-site"
        item["work_mode"] = _ITVIEC_WORK_MODE_MAP.get(
            item["work_mode"].lower(), item["work_mode"]
        )

        # Fix bug lệch cột: posted_at chứa work_mode value
        if item["job_posted_at"].lower() in _ITVIEC_WORK_MODE_VALUES:
            item["work_mode"]     = _ITVIEC_WORK_MODE_MAP.get(
                item["job_posted_at"].lower(), item["job_posted_at"]
            )
            item["job_posted_at"] = ""
        # work_mode chứa địa chỉ lạc (không phải giá trị hợp lệ)
        elif item["work_mode"] and item["work_mode"].lower() not in _ITVIEC_WORK_MODE_VALUES:
            item["work_mode"] = ""

        # skills → nối vào cuối job_description
        skills_raw = raw.get("skills") or []
        if isinstance(skills_raw, list) and skills_raw:
            skills_str = "Skills: " + ", ".join(s.strip() for s in skills_raw if s.strip())
            item["job_description"] = (
                (item["job_description"] + "\n\n" + skills_str).strip()
                if item["job_description"] else skills_str
            )

    else:
        # Các website Scrapy còn lại (joboko, jobsgo, timviec365, v.v.)
        item["job_posted_at"] = _clean_date(item["job_posted_at"])
        item["job_deadline"]  = _clean_date(item["job_deadline"])

    # ── 5. Compensation ───────────────────────────────────
    raw_comp = (raw.get("compensation") or "").strip()
    if not raw_comp or any(p in raw_comp.lower() for p in _NEGOTIABLE):
        item["compensation"] = "Thỏa thuận"
    elif website == "vietnamwork":
        m = re.match(r"^0\s*-\s*(.+)$", raw_comp)
        item["compensation"] = m.group(1).strip() if m else re.sub(r"\s+", " ", raw_comp)
    else:
        item["compensation"] = re.sub(r"\s+", " ", raw_comp)

    # ── 6. Validate ───────────────────────────────────────
    missing = [f for f in ("job_title", "job_url") if not item.get(f)]
    item["is_valid"]  = len(missing) == 0
    item["error_log"] = f"Thiếu field bắt buộc: {', '.join(missing)}" if missing else None

    return item


# =========================================================
#  Pipeline 1: Làm sạch & chuẩn hoá dữ liệu  (Scrapy)
# =========================================================

class CleaningPipeline:
    """
    Scrapy pipeline — bọc clean_dict() cho ItemAdapter.
    Selenium spider dùng clean_dict() trực tiếp, không qua class này.
    """

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        cleaned = clean_dict(dict(adapter))

        for field, value in cleaned.items():
            adapter[field] = value

        if not adapter.get("is_valid"):
            spider.logger.warning(
                f"[CleaningPipeline] Invalid — {adapter.get('error_log')}"
            )

        return item


# =========================================================
#  Pipeline 2: Lưu vào MySQL  (Scrapy + Selenium dùng chung)
# =========================================================

DB_CONFIG = dict(
    host     = "localhost",
    user     = "root",
    password = "123456",
    database = "itta",
    charset  = "utf8mb4",
)

_CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS jobs (
        id               INT           NOT NULL AUTO_INCREMENT,
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
        job_posted_at    VARCHAR(20),
        job_deadline     VARCHAR(20),
        scraped_at       VARCHAR(30),
        is_valid         TINYINT(1)    DEFAULT 1,
        error_log        TEXT,
        PRIMARY KEY (id),
        INDEX idx_website    (website),
        INDEX idx_company    (company_title(100)),
        INDEX idx_location   (location(100)),
        INDEX idx_is_valid   (is_valid),
        INDEX idx_scraped_at (scraped_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

_INSERT_SQL = """
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
        %s,%s, %s,%s,%s, %s,%s
    )
"""


def _insert_params(item: dict) -> tuple:
    return (
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
    )


class SaveToMySQLPipeline:
    """
    Scrapy pipeline — dùng cho Scrapy spider.
    Selenium spider dùng save_to_db() trực tiếp.
    """

    def __init__(self):
        self.conn = mysql.connector.connect(**DB_CONFIG)
        self.cur  = self.conn.cursor()
        self.cur.execute(_CREATE_TABLE_SQL)
        self.conn.commit()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter.get("is_valid") is False:
            spider.logger.warning(
                f"[MySQL] DROP invalid: {adapter.get('error_log')} "
                f"| url={adapter.get('job_url')}"
            )
            return item

        try:
            self.cur.execute(_INSERT_SQL, _insert_params(dict(adapter)))
            self.conn.commit()

            if self.cur.rowcount == 0:
                spider.logger.debug(f"[MySQL] DUPLICATE: {adapter.get('job_url')}")
            else:
                spider.logger.info(
                    f"[MySQL] Saved: {adapter.get('job_title')!r} "
                    f"@ {adapter.get('company_title')}"
                )

        except mysql.connector.Error as e:
            spider.logger.error(f"[MySQL] Error: {e} | url={adapter.get('job_url')}")
            self.conn.rollback()

        return item

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()
        spider.logger.info("[MySQL] Connection closed.")


# =========================================================
#  save_to_db() — dùng cho Selenium spider (không có Scrapy)
# =========================================================

def get_db_connection():
    """Tạo connection + bảng nếu chưa có. Dùng cho Selenium spider."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cur  = conn.cursor()
    cur.execute(_CREATE_TABLE_SQL)
    conn.commit()
    return conn, cur


def save_to_db(cur, conn, item: dict) -> bool:
    """
    INSERT IGNORE theo job_url. Dùng cho Selenium spider.
    Trả về True nếu insert thành công, False nếu duplicate hoặc lỗi.
    """
    if not item.get("is_valid"):
        print(f"    ⚠ Invalid — bỏ qua: {item.get('error_log')}")
        return False

    try:
        cur.execute(_INSERT_SQL, _insert_params(item))
        conn.commit()
        return cur.rowcount > 0
    except mysql.connector.Error as e:
        print(f"    ✗ MySQL error: {e}")
        conn.rollback()
        return False