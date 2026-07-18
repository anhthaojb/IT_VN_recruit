
from itemadapter import ItemAdapter
import psycopg2
import re
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
from jobscrapers.lookups import (
    VW_JOB_TYPE, VW_EDUCATION, VW_JOB_LEVEL, VW_COMPANY_SIZE,
    ITVIEC_WORK_MODE_MAP, ITVIEC_VALID_OUTPUTS, ITVIEC_WORK_MODE_INPUTS,
)

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

_LI_TIME_PAT  = re.compile(
    r"(?:reposted\s+)?(?:\d+\s+(?:second|minute|hour|day|week|month|year)s?\s+ago|just\s+now)",
    re.IGNORECASE,
)
_IT_POSTED_PAT = re.compile(r"^posted\s+(.+)$", re.IGNORECASE)


def clean_dict(raw: dict) -> dict:
    item    = {}
    website = (raw.get("website") or "").lower().strip()

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
            item["work_mode"] = ""
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
        mm = re.match(r"^0\s*-\s*(.+)$", raw_comp)
        item["compensation"] = mm.group(1).strip() if mm else re.sub(r"\s+", " ", raw_comp)
    else:
        item["compensation"] = re.sub(r"\s+", " ", raw_comp)

    missing = [f for f in ("job_title", "job_url") if not item.get(f)]
    item["is_valid"]  = len(missing) == 0          # [THAY ĐỔI 9] True/False thay vì 1/0
    item["error_log"] = f"Thiếu field bắt buộc: {', '.join(missing)}" if missing else None

    return item

def get_db_connection():
    """
    Kết nối PostgreSQL Local qua psycopg2.
    Schema đã tạo sẵn — không tạo lại ở đây.
    """
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur  = conn.cursor()
    return conn, cur

_INSERT_SQL = """
    INSERT INTO staging_jobs (
        website, job_title, company_title, location,
        experience, compensation, job_type, work_mode,
        level, job_url, company_size, company_industry,
        job_category, number_recruit, education_level,
        job_description, job_requirement,
        job_posted_at, job_deadline, scraped_at,
        is_valid, error_log, ai_processed
    ) VALUES (
        %s,%s,%s,%s, %s,%s,%s,%s,
        %s,%s,%s,%s, %s,%s,%s,
        %s,%s, %s,%s,%s, %s,%s,%s
    )
    ON CONFLICT (job_url) DO UPDATE SET
        job_title        = EXCLUDED.job_title,
        company_title    = EXCLUDED.company_title,
        company_industry = EXCLUDED.company_industry,
        compensation     = EXCLUDED.compensation,
        job_description  = EXCLUDED.job_description,
        education_level  = EXCLUDED.education_level,
        job_category     = EXCLUDED.job_category,
        job_requirement  = EXCLUDED.job_requirement,
        scraped_at       = EXCLUDED.scraped_at,
        is_valid         = EXCLUDED.is_valid
        -- ai_processed KHÔNG update để tránh reset về False khi scrape lại
"""

def _insert_params(item: dict) -> tuple:
    website = (item.get("website") or "").lower().strip()
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
        bool(item.get("is_valid", True)),
        item.get("error_log"),
        False if website == "linkedin" else True,  # linkedin → AI xử lý sau
    )

def save_to_db(cur, conn, item: dict) -> tuple[bool, str]:
    """
    Lưu item vào Supabase.
    Trả về (success: bool, status: str):
      "new"       → INSERT mới hoàn toàn
      "updated"   → ON CONFLICT → UPDATE (có thay đổi)
      "duplicate" → ON CONFLICT → không thay đổi gì
      "invalid"   → thiếu field bắt buộc
      "error"     → DB error
    """
    if item.get("is_valid") is False:
        print(f"    ⚠ Invalid — bỏ qua: {item.get('error_log')}")
        return False, "invalid"

    try:
        sql_returning = _INSERT_SQL + " RETURNING (xmax = 0) AS is_new_row, id"
        cur.execute(sql_returning, _insert_params(item))
        row = cur.fetchone()
        conn.commit()

        if row is not None:
            is_new_row = row[0]
            generated_id = row[1]
            item["id"] = generated_id 
            return (True, "new") if is_new_row else (True, "updated")

    except psycopg2.Error as e:
        print(f"    ✗ PostgreSQL error: {e}")
        conn.rollback()
        return False, "error"
class RunTracker:
    def __init__(self, website: str, cur, conn,
                 session_id: str = None, triggered_by: str = "manual"):
        self.website      = website
        self.cur          = cur
        self.conn         = conn
        self.session_id   = session_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.triggered_by = triggered_by
        self.started_at   = datetime.now()
        self.counts       = {
            "total": 0, "new": 0, "updated": 0,
            "duplicate": 0, "invalid": 0, "error": 0,
        }
        cur.execute("""
            INSERT INTO fact_pipeline_snapshot
                (website, session_id, triggered_by, started_at, status)
            VALUES (%s, %s, %s, %s, 'RUNNING')
            RETURNING run_id
        """, (website, self.session_id, triggered_by, self.started_at))

        row = cur.fetchone()  
        conn.commit()        
        self.run_id = row[0] if row else None
        if not self.run_id:
            raise RuntimeError(f"[RunTracker] Không tạo được run_id cho {website}")

    def record(self, status: str, item: dict):
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
                    "NULL_REQUIRED",
                ))
            self.conn.commit()

    def finish(self):
        finished_at  = datetime.now()
        duration_sec = int((finished_at - self.started_at).total_seconds())
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
            finished_at, duration_sec,
            self.counts["total"],   self.counts["new"],
            self.counts["updated"], self.counts["duplicate"],
            self.counts["invalid"], self.counts["error"],
            status, self.run_id,
        ))
        self.conn.commit()
        print(f"\nRun #{self.run_id} [{self.website}] {status} "
              f"| session={self.session_id} "
              f"| new={self.counts['new']} updated={self.counts['updated']} "
              f"dup={self.counts['duplicate']} invalid={self.counts['invalid']} "
              f"({duration_sec}s)")

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


class SaveToPostgresPipeline:
    """Tên mới — SaveToMySQLPipeline vẫn là alias để không phải sửa settings.py."""

    def open_spider(self, spider):
        self.conn, self.cur = get_db_connection()
        self.tracker = RunTracker(
            website=spider.name,
            cur=self.cur,
            conn=self.conn,
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

SaveToMySQLPipeline = SaveToPostgresPipeline

def ensure_db_connection(cur, conn):
    """Kiểm tra connection còn sống không — psycopg2 dùng SELECT 1."""
    try:
        cur.execute("SELECT 1")
        return cur
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        conn_new, cur_new = get_db_connection()
        return cur_new
