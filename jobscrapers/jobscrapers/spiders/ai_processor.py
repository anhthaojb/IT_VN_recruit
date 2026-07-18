import os
import pathlib
import sys
import re
import json
import time
import logging
from groq import Groq
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()
_project_root = str(pathlib.Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
from jobscrapers.pipelines import get_db_connection, _clean_nbsp
logger = logging.getLogger(__name__)
MAX_TITLE_CHARS = 300
MAX_DESCRIPTION_CHARS = 5000
MAX_REQUIREMENT_CHARS = 3000

# MODEL = "llama-3.3-70b-versatile"
MODEL = "openai/gpt-oss-120b"   
# MODEL = "qwen/qwen3.6-27b"  
MAX_RETRIES     = 3
RETRY_DELAY     = 2
MAX_RAW_CHARS   = 9000
MAX_REQ_PER_MIN = 28

SYSTEM_PROMPT = """
You are a data extraction assistant specialized in technology job postings.

The input contains up to three explicitly marked sections:

<JOB_TITLE>
The original, possibly noisy job title (may contain company name, location,
salary, emoji, tags like "HOT", "Gấp", "Urgent", brackets, extra punctuation).
</JOB_TITLE>

<JOB_DESCRIPTION_RAW>
Raw job description content. It may contain responsibilities, requirements,
benefits, salary, work mode, experience, and education mixed together.
</JOB_DESCRIPTION_RAW>

<JOB_REQUIREMENT_RAW>
Raw candidate requirement content. It may overlap with the description
or may be empty.
</JOB_REQUIREMENT_RAW>

Your task is to combine evidence from ALL available sections and return
one structured JSON object.

Return ONLY valid JSON using this exact schema:

{"job_title": "",
  "job_description": "",
  "job_requirement": "",
  "compensation": "",
  "salary_type": "",
  "level": "",
  "job_type": "",
  "work_mode": "",
  "location": "",
  "education_level": "",
  "experience": ""}

Extraction rules:

0. job_title:
   Remove noise from JOB_TITLE: company name, location, salary/compensation
   text, emoji, decorative symbols, recruitment tags (e.g. "HOT", "Gấp",
   "Urgent", "Tuyển gấp"), surrounding brackets/quotes, and duplicate
   whitespace. Keep only the core job position name, in its original
   language, without translating or rewording it.
   If after removing noise no clear job title remains, or the title is
   too ambiguous to isolate confidently, return "" for job_title.
   Do not guess or reconstruct a title that is not clearly present.

1. job_description:
   Extract only responsibilities, duties, tasks, scope of work,
   and what the employee will do.

2. job_requirement:
   Extract only candidate requirements, including skills, technologies,
   experience, education, language, certifications, and personal qualifications.

3. Use information from JOB_TITLE, JOB_DESCRIPTION_RAW, and
   JOB_REQUIREMENT_RAW together.

4. If the same information appears in multiple sections, merge it and remove
   duplicate sentences.

5. Do not copy requirements into job_description.

6. Do not copy responsibilities into job_requirement.

7. Do not invent or infer information that is not supported by the input.

8. If a field is not explicitly supported by the input, return an empty
   string "". This applies to every field, including job_title and
   compensation — do not fabricate or estimate a value.

9. level must be one of:
   Intern, Fresher, Junior, Mid, Senior, Manager, Director.

10. job_type must be one of:
    Full-time, Part-time, Contract, Freelance.

11. work_mode must be one of:
    On-site, Remote, Hybrid.

12. salary_type must be one of:
    hourly, monthly, yearly, per_task, negotiable.
12.1 location: Phải là tên một thành phố cụ thể ở Việt Nam (ví dụ: Thành phố Hồ Chí Minh, Hà Nội, Đà Nẵng,...) hoặc tên thành phố/quốc gia nước ngoài nếu làm việc tại nước ngoài (oversea). Nếu không có thông tin hoặc không xác định được, bắt buộc trả về chuỗi rỗng "". Không điền tên quận/huyện hoặc địa chỉ chi tiết vào đây.

13. Keep Vietnamese text in Vietnamese and English text in English.
    Do not translate.

14. Remove bullet symbols. Return plain text with separate sentences
    divided by newline characters.

15. For experience:
    preserve explicit ranges such as "2+ years" or "3-5 years".
    Do not convert experience into level unless the level is explicitly stated
    in the title or source text.

16. Do not use markdown fences, explanations, or introductory text.
    Return raw JSON only.
""".strip()

_client     = None
_req_count  = 0
_req_window = 0.0
def _get_client():
    global _client
    if _client is None:
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise EnvironmentError("GROQ_API_KEY chưa được set trong .env")
        _client = Groq(api_key=api_key)
    return _client

def _rate_limit_wait():
    global _req_count, _req_window
    now     = time.time()
    elapsed = now - _req_window
    if elapsed >= 60:
        _req_count  = 0
        _req_window = now
        return
    if _req_count >= MAX_REQ_PER_MIN:
        wait = 61 - elapsed
        print(f"    Rate limit Groq — chờ {wait:.1f}s...")
        time.sleep(wait)
        _req_count  = 0
        _req_window = time.time()


def _call_groq(raw_text: str) -> dict:
    global _req_count
    if len(raw_text) > MAX_RAW_CHARS:
        raw_text = raw_text[:MAX_RAW_CHARS] + "\n[truncated]"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            _rate_limit_wait()
            response = _get_client().chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": raw_text},
                ],
                temperature=0.1,
                max_tokens=4096,
            )
            _req_count += 1
            text = response.choices[0].message.content.strip()
            text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
            text = re.sub(r"\s*```$", "", text)
            m = re.search(r"\{[\s\S]*\}", text)
            if m:
                text = m.group(0)
            result = json.loads(text)
            for k, v in result.items():
                if v is None:
                    result[k] = ""
            return result

        except json.JSONDecodeError:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        except Exception as e:
            err = str(e)
            if "429" in err or "rate_limit" in err.lower():
                print(f"    Groq 429 — chờ 62s...")
                time.sleep(62)
                _req_count  = 0
                _req_window = time.time()
            elif attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)

    return {}


def main():
    conn, cur = get_db_connection()
    cur.execute("""
        SELECT
            id,
            job_title,
            company_title,
            job_url,
            job_description,
            job_requirement,
            work_mode,
            job_type,
            compensation,
            level,
            experience,
            education_level,
            location
        FROM staging_jobs
        WHERE website = 'linkedin'
        AND ai_processed = FALSE
        AND (
                NULLIF(TRIM(job_description), '') IS NOT NULL
            OR NULLIF(TRIM(job_requirement), '') IS NOT NULL
        )
        AND (
            job_requirement IS NULL OR TRIM(job_requirement) = ''
            OR level IS NULL OR TRIM(level) = ''
            OR education_level IS NULL OR TRIM(education_level) = ''
            OR compensation IS NULL OR TRIM(compensation) = ''
            OR experience IS NULL OR TRIM(experience) = ''
            OR work_mode IS NULL OR TRIM(work_mode) = ''
            OR job_type IS NULL OR TRIM(job_type) = ''
            OR location IS NULL OR TRIM(location) = ''
        )
        ORDER BY scraped_at ASC, id ASC
        LIMIT 50;
    """)


    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    items = [dict(zip(cols, row)) for row in rows]

    print(f"Cần xử lý AI: {len(items)} jobs")

    for i, item in enumerate(items, 1):
        print(f"  [{i}/{len(items)}] {item['job_title']}")
        try:
            job_title_raw = (item.get("job_title") or "").strip()[:MAX_TITLE_CHARS]
            job_description_raw = (item.get("job_description") or "").strip()[:MAX_DESCRIPTION_CHARS]
            job_requirement_raw = (item.get("job_requirement") or "").strip()[:MAX_REQUIREMENT_CHARS]

            full_raw_text = f"""
            <JOB_TITLE>
            {job_title_raw}
            </JOB_TITLE>

            <JOB_DESCRIPTION_RAW>
            {job_description_raw}
            </JOB_DESCRIPTION_RAW>

            <JOB_REQUIREMENT_RAW>
            {job_requirement_raw}
            </JOB_REQUIREMENT_RAW>
            """.strip()

            ai = _call_groq(full_raw_text)

            def _clean_field(field):
                ai_val = (ai.get(field) or "").strip()
                if ai_val:
                    return ai_val
                return (item.get(field) or "").strip()
            job_title_clean = (ai.get("job_title") or "").strip()

            job_desc = (ai.get("job_description") or "").strip()
            job_req = (ai.get("job_requirement") or "").strip()
            if not job_req:
                job_req = "Thông tin chi tiết xem tại mô tả công việc hoặc liên hệ nhà tuyển dụng."
            if not job_desc:
                job_desc = (item.get("job_description") or "").strip()

            compensation_val = _clean_field("compensation")
            if not compensation_val or compensation_val.lower() in ["", "none", "null"]:
                compensation_val = "Thỏa thuận"

            cur.execute("""
                UPDATE staging_jobs SET
                    job_title       = COALESCE(NULLIF(%s, ''), job_title),
                    job_description = %s,
                    job_requirement = %s,
                    compensation    = %s,
                    level           = %s,
                    work_mode       = %s,
                    job_type        = %s,
                    experience      = %s,
                    education_level = %s,
                    location        = %s,
                    ai_processed    = TRUE
                WHERE id = %s
            """, (
                _clean_nbsp(job_title_clean),
                _clean_nbsp(job_desc),
                _clean_nbsp(job_req),
                compensation_val,
                _clean_field("level"),
                _clean_field("work_mode"),
                _clean_field("job_type"),
                _clean_field("experience"),
                _clean_field("education_level"),
                _clean_field("location"),
                item["id"],
            ))
            conn.commit()
        except Exception as e:
            print(f"    Lỗi tại Job ID {item['id']}: {e}")
            conn.rollback()

    cur.close()
    conn.close()
    print("AI processing xong")
if __name__ == "__main__":
    main()