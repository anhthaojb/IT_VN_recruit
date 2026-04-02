from itemadapter import ItemAdapter
import mysql.connector
import re
from datetime import datetime


# =========================================================
#  Pipeline 1: Làm sạch & chuẩn hoá dữ liệu
# =========================================================

class CleaningPipeline:
    """
    Chuẩn hoá JobItem trước khi lưu DB:
      1. Strip whitespace tất cả string field
      2. List field → join thành string
      3. compensation → xử lý riêng theo website
      4. scraped_at → ISO string
      5. Validate field bắt buộc → đánh dấu is_valid / error_log
    """

    # Field chỉ cần strip
    STR_FIELDS = {
        "website", "job_title", "company_title", "location",
        "experience", "job_type", "work_mode", "level", "job_url",
        "company_size", "company_industry", "number_recruit",
        "education_level", "job_posted_at", "job_deadline",
    }

    # Field có thể là list → join thành string
    LIST_FIELDS = {"job_category", "job_description", "job_requirement"}

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # ── 1. Strip whitespace ────────────────────────────
        for field in self.STR_FIELDS:
            val = adapter.get(field)
            if isinstance(val, str):
                adapter[field] = re.sub(r"\s+", " ", val).strip()
            elif val is None:
                adapter[field] = ""

        # ── 2. List field → string ─────────────────────────
        for field in self.LIST_FIELDS:
            val = adapter.get(field)
            if isinstance(val, list):
                cleaned = [v.strip() for v in val if isinstance(v, str) and v.strip()]
                adapter[field] = "\n".join(cleaned)
            elif val is None:
                adapter[field] = ""
            elif isinstance(val, str):
                adapter[field] = re.sub(r"\s+", " ", val).strip()

        # ── 3. Compensation — xử lý riêng theo nguồn ──────
        website = adapter.get("website", "").lower()
        raw_comp = adapter.get("compensation") or ""
        adapter["compensation"] = self._clean_compensation(raw_comp, website)

        # ── 4. job_posted_at — chuẩn hoá whitespace ───────
        posted = adapter.get("job_posted_at") or ""
        adapter["job_posted_at"] = re.sub(r"\s+", " ", posted).strip()

        # ── 5. scraped_at → ISO string ─────────────────────
        scraped = adapter.get("scraped_at")
        if isinstance(scraped, datetime):
            adapter["scraped_at"] = scraped.isoformat()
        elif not scraped:
            adapter["scraped_at"] = datetime.now().isoformat()

        # ── 6. Validate bắt buộc ───────────────────────────
        missing = []
        if not adapter.get("job_title"):
            missing.append("job_title")
        if not adapter.get("job_url"):
            missing.append("job_url")

        if missing:
            adapter["is_valid"]  = False
            adapter["error_log"] = f"Thiếu field bắt buộc: {', '.join(missing)}"
            spider.logger.warning(
                f"[CleaningPipeline] Invalid item — {adapter['error_log']}"
            )
        else:
            adapter["is_valid"]  = True
            adapter["error_log"] = None

        return item

    # ------------------------------------------------------------------
    # Compensation helpers
    # ------------------------------------------------------------------

    def _clean_compensation(self, raw: str, website: str) -> str:
        """Dispatch sang helper riêng theo website."""
        if not raw or not raw.strip():
            return "Thỏa thuận"

        if website == "topcv":
            return self._comp_topcv(raw)
        if website == "itviec":
            return self._comp_itviec(raw)
        if website == "linkedin":
            # LinkedIn hiếm khi hiển thị lương — giữ nguyên nếu có, không thì mặc định
            return raw.strip() if raw.strip() else "Thỏa thuận"
        # Các nguồn khác: strip whitespace thừa
        return re.sub(r"\s+", " ", raw).strip()

    @staticmethod
    def _comp_topcv(raw: str) -> str:
        """
        TopCV format:
          "10 - 15 triệu"  →  giữ nguyên
          "Mới cập nhật"   →  "Thỏa thuận"
          "Thương lượng"   →  "Thỏa thuận"
        """
        cleaned = re.sub(r"\s+", " ", raw).strip()
        NEGOTIABLE_PATTERNS = ["mới cập nhật", "thương lượng", "thoả thuận", "thỏa thuận"]
        if any(p in cleaned.lower() for p in NEGOTIABLE_PATTERNS):
            return "Thỏa thuận"
        return cleaned

    @staticmethod
    def _comp_itviec(raw: str) -> str:
        """
        ITviec format:
          "Up to $2,000"        →  giữ nguyên
          "1,500 - 3,000 USD"   →  giữ nguyên
          "Cạnh tranh"          →  "Thỏa thuận"
          "You'll love it"      →  "Thỏa thuận"
        """
        cleaned = re.sub(r"\s+", " ", raw).strip()
        NEGOTIABLE_PATTERNS = ["cạnh tranh", "you'll love it", "thỏa thuận", "thoả thuận"]
        if any(p in cleaned.lower() for p in NEGOTIABLE_PATTERNS):
            return "Thỏa thuận"
        return cleaned


# =========================================================
#  Pipeline 2: Lưu vào MySQL
# =========================================================

class SaveToMySQLPipeline:
    """
    Lưu JobItem vào bảng `jobs`.
    - Bảng tự tạo nếu chưa có
    - INSERT IGNORE theo job_url (dedup tự động)
    - Drop item nếu is_valid = False
    """

    DB_CONFIG = dict(
        host     = "localhost",
        user     = "root",
        password = "123456",
        database = "jobscrapers",
        charset  = "utf8mb4",
    )

    def __init__(self):
        self.conn = mysql.connector.connect(**self.DB_CONFIG)
        self.cur  = self.conn.cursor()
        self._create_table()

    # ------------------------------------------------------------------

    def _create_table(self):
        self.cur.execute("""
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
                job_posted_at    VARCHAR(100),
                job_deadline     VARCHAR(100),
                scraped_at       VARCHAR(50),
                is_valid         TINYINT(1)    DEFAULT 1,
                error_log        TEXT,
                PRIMARY KEY (id),
                INDEX idx_website    (website),
                INDEX idx_company    (company_title(100)),
                INDEX idx_location   (location(100)),
                INDEX idx_is_valid   (is_valid),
                INDEX idx_scraped_at (scraped_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        self.conn.commit()

    # ------------------------------------------------------------------

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Drop item không hợp lệ — không lưu vào DB
        if adapter.get("is_valid") is False:
            spider.logger.warning(
                f"[MySQL] DROP invalid item: {adapter.get('error_log')} "
                f"| url={adapter.get('job_url')}"
            )
            return item   # trả về để pipeline sau vẫn nhận (nếu cần log)

        try:
            self.cur.execute("""
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
                adapter.get("website"),
                adapter.get("job_title"),
                adapter.get("company_title"),
                adapter.get("location"),
                adapter.get("experience"),
                adapter.get("compensation"),
                adapter.get("job_type"),
                adapter.get("work_mode"),
                adapter.get("level"),
                adapter.get("job_url"),
                adapter.get("company_size"),
                adapter.get("company_industry"),
                adapter.get("job_category"),
                adapter.get("number_recruit"),
                adapter.get("education_level"),
                adapter.get("job_description"),
                adapter.get("job_requirement"),
                adapter.get("job_posted_at"),
                adapter.get("job_deadline"),
                adapter.get("scraped_at"),
                int(adapter.get("is_valid", True)),
                adapter.get("error_log"),
            ))
            self.conn.commit()

            if self.cur.rowcount == 0:
                spider.logger.debug(
                    f"[MySQL] DUPLICATE skipped: {adapter.get('job_url')}"
                )
            else:
                spider.logger.info(
                    f"[MySQL] Saved: {adapter.get('job_title')!r} "
                    f"@ {adapter.get('company_title')}"
                )

        except mysql.connector.Error as e:
            spider.logger.error(
                f"[MySQL] Error: {e} | url={adapter.get('job_url')}"
            )
            self.conn.rollback()

        return item

    # ------------------------------------------------------------------

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()
        spider.logger.info("[MySQL] Connection closed.")