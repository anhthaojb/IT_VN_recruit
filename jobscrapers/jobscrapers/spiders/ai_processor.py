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

# MODEL = "llama-3.3-70b-versatile"
# MODEL="openai/gpt-oss-120b"   
MODEL="qwen/qwen3.6-27b" 
MAX_RETRIES     = 3
RETRY_DELAY     = 2
MAX_RAW_CHARS   = 6000
MAX_REQ_PER_MIN = 28

SYSTEM_PROMPT = """
You are an expert data extraction assistant specialized in tech job postings.
Your sole task is to parse the provided raw job text and organize it into a structured JSON object.

Input text contains both what the candidate will do and what qualifications they need. Separate them cleanly.

Return ONLY a valid JSON with this exact schema:
{
  "job_description": "Responsibilities, core duties, and daily tasks. Plain text, remove all bullet points, separate sentences with newlines.",
  "job_requirement": "Requirements, skills, tech stack, experience, and education needed. Plain text, remove all bullet points, separate sentences with newlines.",
  "compensation": "Salary info if found (e.g., 'Thỏa thuận', '20-30 triệu/tháng', '1000 USD/month').",
  "salary_type": "hourly, monthly, yearly, per_task, or negotiable.",
  "level": "Intern, Fresher, Junior, Mid, Senior, Manager, or Director.",
  "job_type": "Full-time, Part-time, Contract, or Freelance.",
  "work_mode": "On-site, Remote, or Hybrid.",
  "education_level": "Minimum degree required if mentioned.",
  "experience": "Years of experience needed (e.g., '2+ years', '0' for intern)."
}

CRITICAL: Do not write any markdown fences (like ```json), no explanations, no chat preamble. Just raw JSON. If any information is missing, use empty string "".
5. Do not invent information not present in the source.
6. Keep Vietnamese text in Vietnamese. Keep English text in English. Do NOT translate.
7. Remove ALL bullet symbols (-, •, *, ▪, ·, –) — plain sentences separated by newlines only.
8. For experience with multiple levels (intern/junior/senior), extract the most junior requirement.
9. salary_type: /hour or per hour or /giờ → hourly; /month or /tháng → monthly; /year or /năm → yearly; per task or theo dự án → per_task; thỏa thuận or negotiable or competitive → negotiable.
10. job_type: Never use 'Internship' as job_type — use level field for intern detection instead.
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
                max_tokens=1024,
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
            SELECT id, job_title, company_title, job_url,
           job_description, work_mode, job_type,
           compensation, level
    FROM staging_jobs
    WHERE website = 'linkedin'
      AND ai_processed = FALSE
      AND job_description IS NOT NULL
      AND job_description != ''
      AND (
          job_requirement IS NULL OR job_requirement = ''
          OR level IS NULL OR level = ''
          OR education_level IS NULL OR education_level = ''
          OR compensation IS NULL OR compensation = ''
          OR experience IS NULL OR experience = ''
      )
    ORDER BY scraped_at DESC
    LIMIT 40;
    """)


    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    items = [dict(zip(cols, row)) for row in rows]

    print(f"Cần xử lý AI: {len(items)} jobs")

    for i, item in enumerate(items, 1):
        print(f"  [{i}/{len(items)}] {item['job_title']}")
        try:
            full_raw_text = item["job_description"]
            ai = _call_groq(full_raw_text)

            def _clean_field(field):
                ai_val = (ai.get(field) or "").strip()
                if ai_val:
                    return ai_val
                return (item.get(field) or "").strip()


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
                    job_description = %s,
                    job_requirement = %s,
                    compensation    = %s,
                    level           = %s,
                    work_mode       = %s,
                    job_type        = %s,
                    experience      = %s,
                    education_level = %s,
                    ai_processed    = TRUE
                WHERE id = %s
            """, (
                _clean_nbsp(job_desc),
                _clean_nbsp(job_req),
                compensation_val,
                _clean_field("level"),
                _clean_field("work_mode"),
                _clean_field("job_type"),
                (ai.get("experience") or "").strip(),
                (ai.get("education_level") or "").strip(),
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