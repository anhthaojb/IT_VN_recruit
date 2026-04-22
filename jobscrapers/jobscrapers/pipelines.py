from itemadapter import ItemAdapter
import mysql.connector
import re
from datetime import datetime, timedelta
#from lookups import VW_JOB_TYPE, VW_EDUCATION,VW_JOB_LEVEL,VW_COMPANY_SIZE, _ITVIEC_WORK_MODE_MAP, _ITVIEC_VALID_OUTPUTS, _ITVIEC_WORK_MODE_INPUTS
from lookups import VW_JOB_TYPE, VW_EDUCATION, VW_JOB_LEVEL, VW_COMPANY_SIZE, ITVIEC_WORK_MODE_MAP, ITVIEC_VALID_OUTPUTS, ITVIEC_WORK_MODE_INPUTS
# =========================================================
#  Helpers dùng chung
# =========================================================

def _clean_date(text: str) -> str:
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
    if not text:
        return ""
    text = text.replace("\xa0", " ").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r" {2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _relative_to_date(relative_text: str, scraped_at_iso: str) -> str:
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




_LI_TIME_PAT = re.compile(
    r"(?:reposted\s+)?(?:\d+\s+(?:second|minute|hour|day|week|month|year)s?\s+ago|just\s+now)",
    re.IGNORECASE,
)

_IT_POSTED_PAT = re.compile(r"^posted\s+(.+)$", re.IGNORECASE)


# =========================================================
#  Core: clean_dict()
# =========================================================

def clean_dict(raw: dict) -> dict:
    item    = {}
    website = (raw.get("website") or "").lower().strip()

    STR_FIELDS = [
        "website", "job_title", "company_title", "location",
        "experience", "job_type", "work_mode", "level", "job_url",
        "company_size", "company_industry", "number_recruit",
        "education_level", "job_posted_at", "job_deadline",
        "raw_about_job",
    ]
    for field in STR_FIELDS:
        val = raw.get(field)
        if isinstance(val, str):
            item[field] = re.sub(r"\s+", " ", val).strip()
        else:
            item[field] = ""

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

    cat = raw.get("job_category")
    if isinstance(cat, list):
        item["job_category"] = ", ".join(v.strip() for v in cat if v.strip())
    elif isinstance(cat, str):
        item["job_category"] = re.sub(r"\s+", " ", cat).strip()
    else:
        item["job_category"] = ""

    scraped = raw.get("scraped_at")
    if isinstance(scraped, datetime):
        scraped_iso = scraped.isoformat()
    elif isinstance(scraped, str) and scraped:
        scraped_iso = scraped.replace(" ", "T", 1)
    else:
        scraped_iso = datetime.now().isoformat()
    item["scraped_at"] = scraped_iso

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
        item["level"]           = VW_JOB_LEVEL.get(item["level"], item["level"])
        item["company_size"]    = VW_COMPANY_SIZE.get(item["company_size"], item["company_size"])
        item["job_posted_at"]   = _clean_date(item["job_posted_at"])
        item["job_deadline"]    = _clean_date(item["job_deadline"])

    elif website == "linkedin":
        if not item["job_description"]:
            raw_desc = (raw.get("raw_about_job") or "").strip()
            item["job_description"] = _clean_nbsp(raw_desc)

        posted = item["job_posted_at"]
        if _LI_TIME_PAT.search(posted):
            item["job_posted_at"] = _relative_to_date(posted, scraped_iso)

        item["job_deadline"] = ""

    elif website == "itviec":
        posted = item["job_posted_at"]
        m = _IT_POSTED_PAT.match(posted)
        relative_part = m.group(1).strip() if m else posted
        item["job_posted_at"] = _relative_to_date(relative_part, scraped_iso)
        item["job_deadline"]  = ""

        item["work_mode"] = ITVIEC_WORK_MODE_MAP.get(
            item["work_mode"].lower(), item["work_mode"]
        )
        if item["job_posted_at"].lower() in ITVIEC_WORK_MODE_INPUTS:
            item["work_mode"]     = ITVIEC_WORK_MODE_MAP.get(
                item["job_posted_at"].lower(), item["job_posted_at"]
            )
            item["job_posted_at"] = ""
        elif item["work_mode"] and item["work_mode"] not in ITVIEC_VALID_OUTPUTS:
            item["work_mode"] = ""   # ✅ "On-site" CÓ trong {"On-site","Hybrid","Remote"} → giữ lại!

        skills_raw = raw.get("skills") or []
        if isinstance(skills_raw, list) and skills_raw:
            skills_str = "Skills: " + ", ".join(s.strip() for s in skills_raw if s.strip())
            item["job_description"] = (
                (item["job_description"] + "\n\n" + skills_str).strip()
                if item["job_description"] else skills_str
            )

    else:
        item["job_posted_at"] = _clean_date(item["job_posted_at"])
        item["job_deadline"]  = _clean_date(item["job_deadline"])

    raw_comp = (raw.get("compensation") or "").strip()
    if not raw_comp or any(p in raw_comp.lower() for p in _NEGOTIABLE):
        item["compensation"] = "Thỏa thuận"
    elif website == "vietnamwork":
        m = re.match(r"^0\s*-\s*(.+)$", raw_comp)
        item["compensation"] = m.group(1).strip() if m else re.sub(r"\s+", " ", raw_comp)
    else:
        item["compensation"] = re.sub(r"\s+", " ", raw_comp)

    missing = [f for f in ("job_title", "job_url") if not item.get(f)]
    item["is_valid"]  = len(missing) == 0
    item["error_log"] = f"Thiếu field bắt buộc: {', '.join(missing)}" if missing else None

    return item


# =========================================================
#  Pipeline 1: Làm sạch & chuẩn hoá dữ liệu  (Scrapy)
# =========================================================

class CleaningPipeline:
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
        raw_about_job    LONGTEXT,
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
_CREATE_FACT_TABLES_SQL = """
    CREATE TABLE IF NOT EXISTS fact_pipeline_snapshot (
        run_id          INT AUTO_INCREMENT PRIMARY KEY,
        session_id      VARCHAR(20),
        website         VARCHAR(50),
        triggered_by    VARCHAR(20)  DEFAULT 'manual',
        started_at      DATETIME,
        finished_at     DATETIME,
        duration_sec    INT,
        total_scraped   INT DEFAULT 0,
        new_jobs        INT DEFAULT 0,
        updated_jobs    INT DEFAULT 0,
        duplicate_jobs  INT DEFAULT 0,
        invalid_jobs    INT DEFAULT 0,
        error_jobs      INT DEFAULT 0,
        status          VARCHAR(20),
        INDEX idx_session (session_id),
        INDEX idx_website (website),
        INDEX idx_started (started_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

    CREATE TABLE IF NOT EXISTS fact_error_detail (
        error_id        INT AUTO_INCREMENT PRIMARY KEY,
        run_id          INT,
        column_name     VARCHAR(100),
        bad_value       TEXT,
        error_type      VARCHAR(50),
        created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (run_id) REFERENCES fact_pipeline_snapshot(run_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
_INSERT_SQL = """
    INSERT INTO jobs (
        website, job_title, company_title, location,
        experience, compensation, job_type, work_mode,
        level, job_url, company_size, company_industry,
        job_category, number_recruit, education_level,
        job_description, job_requirement, raw_about_job,
        job_posted_at, job_deadline, scraped_at,
        is_valid, error_log
    ) VALUES (
        %s,%s,%s,%s, %s,%s,%s,%s,
        %s,%s,%s,%s, %s,%s,%s,%s,
        %s,%s, %s,%s,%s, %s,%s
    )
    ON DUPLICATE KEY UPDATE
        job_title        = VALUES(job_title),
        company_title    = VALUES(company_title),
        company_industry = VALUES(company_industry),
        compensation     = VALUES(compensation),
        job_description  = VALUES(job_description),
        education_level  = VALUES(education_level),
        job_category     = VALUES(job_category),
        job_requirement  = VALUES(job_requirement),
        job_posted_at    = VALUES(job_posted_at),
        scraped_at       = VALUES(scraped_at),
        is_valid         = VALUES(is_valid);
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
        item.get("raw_about_job"),
        item.get("job_posted_at"),
        item.get("job_deadline"),
        item.get("scraped_at"),
        int(item.get("is_valid", True)),
        item.get("error_log"),
    )


class SaveToMySQLPipeline:
    def open_spider(self, spider):
        self.conn, self.cur = get_db_connection()
        self.tracker = RunTracker(
            website=spider.name,
            cur=self.cur,
            conn=self.conn
        )

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        success, status = save_to_db(self.cur, self.conn, dict(adapter))
        self.tracker.record(status, dict(adapter))
        return item

    def close_spider(self, spider):
        self.tracker.finish()
        self.cur.close()
        self.conn.close()


def save_to_db(cur, conn, item: dict) -> tuple[bool, str]:
    """
    Lưu item vào DB.
    Trả về tuple (success: bool, status: str).
      "new"       → job mới hoàn toàn
      "updated"   → job cũ, đã update các trường
      "duplicate" → job cũ, không có gì thay đổi
      "invalid"   → item thiếu field bắt buộc
      "error"     → MySQL error
    """
    if item.get("is_valid") is False:
        print(f"    ⚠ Invalid — bỏ qua: {item.get('error_log')}")
        return False, "invalid"

    try:
        cur.execute(_INSERT_SQL, _insert_params(item))
        conn.commit()

        rc = cur.rowcount
        if rc == 1:
            return True, "new"       # INSERT thành công
        elif rc == 2:
            return True, "updated"   # ON DUPLICATE → UPDATE có thay đổi
        else:
            return False, "duplicate"  # ON DUPLICATE → không đổi gì

    except mysql.connector.Error as e:
        print(f"    ✗ MySQL error: {e}")
        conn.rollback()
        return False, "error"


class RunTracker:
    def __init__(self, website: str, cur, conn, session_id: str = None, triggered_by: str = "manual"):
        self.website     = website
        self.cur         = cur
        self.conn        = conn
        self.session_id  = session_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.triggered_by = triggered_by
        self.started_at  = datetime.now()
        self.counts      = {
            "total": 0, "new": 0, "updated": 0,
            "duplicate": 0, "invalid": 0, "error": 0
        }

        cur.execute("""
            INSERT INTO fact_pipeline_snapshot
                (website, session_id, triggered_by, started_at, status)
            VALUES (%s, %s, %s, %s, 'RUNNING')
        """, (website, self.session_id, triggered_by, self.started_at))
        self.run_id = cur.lastrowid
        conn.commit()
        if not self.run_id:
            raise RuntimeError(f"[RunTracker] Không tạo được run_id cho {website}")

    def record(self, status: str, item: dict):
        """Gọi sau mỗi save_to_db()"""
        self.counts["total"] += 1
        self.counts[status]  += 1

        if status == "invalid":
            self._log_error(item)

    def _log_error(self, item: dict):
        error_log = item.get("error_log") or ""
        if "Thiếu field bắt buộc:" in error_log:
            missing_fields = error_log.split(":")[1].strip().split(", ")
            for field in missing_fields:
                self.cur.execute("""
                    INSERT INTO fact_error_detail
                        (run_id, column_name, bad_value, error_type)
                    VALUES (%s, %s, %s, %s)
                """, (
                    self.run_id,
                    field.strip(),
                    str(item.get(field.strip())),
                    "NULL_REQUIRED"
                ))
            self.conn.commit()

    def finish(self):
        finished_at  = datetime.now()
        duration_sec = int((finished_at - self.started_at).total_seconds())

        # WARN nếu invalid > 20% tổng
        status = (
            "FAILED"  if self.counts["error"] > 0 else
            "WARN"    if self.counts["total"] > 0 and
                         self.counts["invalid"] / self.counts["total"] > 0.2 else
            "SUCCESS"
        )

        self.cur.execute("""
            UPDATE fact_pipeline_snapshot SET
                finished_at    = %s,
                duration_sec   = %s,
                total_scraped  = %s,
                new_jobs       = %s,
                updated_jobs   = %s,
                duplicate_jobs = %s,
                invalid_jobs   = %s,
                error_jobs     = %s,
                status         = %s
            WHERE run_id = %s
        """, (
            finished_at,
            duration_sec,
            self.counts["total"],
            self.counts["new"],
            self.counts["updated"],
            self.counts["duplicate"],
            self.counts["invalid"],
            self.counts["error"],
            status,
            self.run_id,
        ))
        self.conn.commit()

        print(f"\n📊 Run #{self.run_id} [{self.website}] {status} "
              f"| session={self.session_id} "
              f"| new={self.counts['new']} updated={self.counts['updated']} "
              f"dup={self.counts['duplicate']} invalid={self.counts['invalid']} "
              f"({duration_sec}s)")
def get_db_connection():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur  = conn.cursor()
    cur.execute(_CREATE_TABLE_SQL)          # tạo bảng jobs
    # Tách từng statement vì execute() không chạy được multi-statement
    for sql in _CREATE_FACT_TABLES_SQL.strip().split(";"):
        sql = sql.strip()
        if sql:
            cur.execute(sql)
    conn.commit()
    return conn, cur