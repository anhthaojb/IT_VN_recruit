"""
ai_processor.py
===============
Dùng Groq (Llama) để xử lý raw_about_job từ LinkedIn.

Setup:
    pip install groq
    # Thêm vào .env:
    GROQ_API_KEY=gsk_...

Lấy API key miễn phí (chỉ cần email) tại:
    https://console.groq.com
"""

import os
import json
import re
import time
import logging
from groq import Groq
from dotenv import load_dotenv
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from jobscrapers.pipelines import _clean_nbsp
load_dotenv()
logger = logging.getLogger(__name__)

# ===== CONFIG =====
MODEL         = "llama-3.1-8b-instant"  # Free, nhanh, 14400 req/ngày
MAX_RETRIES   = 3
RETRY_DELAY   = 2
MAX_RAW_CHARS = 6000
RATE_LIMIT_DELAY = 2.5  # giây nghỉ giữa các lần gọi AI — tránh 30 req/phút

# FIX: Prompt rõ ràng hơn, ví dụ cụ thể để AI tách đúng 2 section
SYSTEM_PROMPT = """
You are a job data extraction assistant. Given raw text from a LinkedIn job posting,
extract and return ONLY a valid JSON object with these exact fields.
Use empty string "" if information is not present.

{
  "job_description" : "ONLY the responsibilities/duties section. What the candidate will DO. Plain text, no bullet symbols.",
  "job_requirement" : "ONLY the requirements/qualifications section. What the candidate MUST HAVE (skills, experience, education). Plain text, no bullet symbols.",
  "compensation"    : "Salary/pay range if explicitly mentioned, INCLUDING the time period (e.g. '/year', '/month', '/năm', '/tháng'). Examples: '100000-120000 USD/year', '2000-3000 USD/month', '20-30 triệu/tháng'. ALWAYS include /year or /month. Use empty string if not found.",
  "level"           : "Seniority level: Junior / Mid / Senior / Manager / Director / Intern. Empty string if unclear.",
  "job_type"        : "Employment type: Full-time / Part-time / Contract / Freelance / Internship. Empty string if not mentioned.",
  "work_mode"       : "Work arrangement: On-site / Remote / Hybrid. Empty string if not mentioned.",
  "education_level" : "Minimum required education degree e.g. 'Bachelor', 'Master'. Empty string if not mentioned.",
  "experience"      : "Required years of experience e.g. '2+ years', '3-5 years'. Empty string if not mentioned."
}

CRITICAL RULES:
1. Return ONLY raw JSON. No markdown fences (```), no explanation, no preamble.
2. "job_description" = responsibilities / what you will do / duties. Do NOT include requirements here.
3. "job_requirement" = requirements / qualifications / must-have skills. Do NOT include duties here.
4. If the text mixes both sections without clear headers, use context to separate them.
5. Do not invent any information not present in the source text.
6. Keep Vietnamese text in Vietnamese. Keep English text in English.
7. Remove bullet point symbols (-, •, *, ▪) from all values — plain sentences only.
8. For compensation, ALWAYS append /year or /month based on context. If unclear, assume /year for USD amounts > 10000, /month for smaller amounts.

EXAMPLE OUTPUT:
{
  "job_description": "Phát triển các tính năng mới cho hệ thống. Phối hợp với các phòng ban đề xuất giải pháp.",
  "job_requirement": "Tốt nghiệp Đại học chuyên ngành CNTT. Có kiến thức về Java hoặc Python.",
  "compensation": "100000-120000 USD/year",
  "level": "Junior",
  "job_type": "Full-time",
  "work_mode": "On-site",
  "education_level": "Bachelor",
  "experience": "1+ years"
}
""".strip()

# ===== KHỞI TẠO GROQ (1 lần duy nhất) =====
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

# Đếm số request trong phút hiện tại — tránh rate limit
_req_count    = 0
_req_window   = time.time()
MAX_REQ_PER_MIN = 28  # buffer thấp hơn giới hạn 30


def _rate_limit_wait():
    """Đảm bảo không vượt quá MAX_REQ_PER_MIN request/phút."""
    global _req_count, _req_window

    now = time.time()
    elapsed = now - _req_window

    if elapsed >= 60:
        _req_count  = 0
        _req_window = now
        return

    if _req_count >= MAX_REQ_PER_MIN:
        wait = 60 - elapsed + 1
        logger.info(f"[AI] Rate limit — chờ {wait:.1f}s")
        print(f"    ⏳ Rate limit Groq — chờ {wait:.1f}s...")
        time.sleep(wait)
        _req_count  = 0
        _req_window = time.time()


# ===== GỌI API =====

def _extract_with_groq(raw_text: str) -> dict:
    """
    Gọi Groq API để extract structured data từ raw job text.
    Trả về dict với các field đã extract, hoặc {} nếu thất bại.
    """
    global _req_count

    if len(raw_text) > MAX_RAW_CHARS:
        raw_text = raw_text[:MAX_RAW_CHARS] + "\n[truncated]"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            _rate_limit_wait()

            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": raw_text},
                ],
                temperature=0.1,  # FIX: giảm temperature → output ổn định hơn
            )
            _req_count += 1

            text = response.choices[0].message.content.strip()

            # Strip markdown code block nếu có
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$",           "", text)

            # FIX: tìm JSON block trong response phòng trường hợp AI vẫn thêm text xung quanh
            json_match = re.search(r"\{[\s\S]*\}", text)
            if json_match:
                text = json_match.group(0)

            result = json.loads(text)

            # FIX: validate các field bắt buộc có mặt
            required_fields = [
                "job_description", "job_requirement", "compensation",
                "level", "job_type", "work_mode", "education_level", "experience"
            ]
            for f in required_fields:
                if f not in result:
                    result[f] = ""

            logger.debug(f"[AI] Extract OK — {list(result.keys())}")
            return result

        except json.JSONDecodeError as e:
            logger.warning(f"[AI] JSON parse error attempt {attempt}: {e}")
            logger.debug(f"[AI] Raw response: {text!r}")

        except Exception as e:
            err = str(e)
            logger.warning(f"[AI] API error attempt {attempt}: {err}")

            if "429" in err or "rate" in err.lower():
                wait = 60
                logger.info(f"[AI] 429 — chờ {wait}s")
                print(f"    ⏳ Groq 429 — chờ {wait}s...")
                time.sleep(wait)
                _req_count  = 0
                _req_window = time.time()
            elif attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)

    return {}


# ===== XỬ LÝ TỪNG ITEM =====

def process_linkedin_item(item: dict) -> dict:
    """
    Nhận dict raw từ LinkedIn spider.
    Gọi Groq để extract các field có cấu trúc.
    Trả về dict đã enrich — sẵn sàng để clean_dict() + save_to_db().

    Flow:
      raw_about_job → Groq AI → job_description + job_requirement + ...
      Fallback: nếu AI lỗi → job_description = raw_about_job
    """
    raw_text = (item.get("raw_about_job") or item.get("job_description") or "").strip()

    if not raw_text:
        logger.warning(f"[AI] raw_about_job trống — {item.get('job_title')!r}")
        ai = {}
    else:
        logger.info(f"[AI] Calling Groq: {item.get('job_title')!r}")
        ai = _extract_with_groq(raw_text)

        if ai:
            logger.info(f"[AI] OK — {item.get('job_title')!r}")
        else:
            logger.warning(f"[AI] FAIL — fallback raw_about_job: {item.get('job_title')!r}")

    def pick(f):
        """
        Ưu tiên AI output, fallback về item gốc.
        Ngược lại với version cũ — vì mục tiêu là AI điền vào các field trống.
        """
        return (ai.get(f) or "").strip() or (item.get(f) or "").strip()

    enriched = dict(item)

    # ── AI output → enriched ──────────────────────────────────────────────────
    # FIX: job_description và job_requirement luôn lấy từ AI trước
    # Fallback về raw_about_job chỉ khi AI hoàn toàn thất bại
    enriched["job_description"] = _clean_nbsp(
        (ai.get("job_description") or "").strip()
        or raw_text  # fallback
    )
    enriched["job_requirement"] = _clean_nbsp(
        (ai.get("job_requirement") or "").strip()
    )

    # ── Các field AI bổ sung ──────────────────────────────────────────────────
    # FIX: dùng pick() — AI trước, item gốc sau (không phải ngược lại)
    enriched["compensation"]    = pick("compensation") or "Thoa thuan"
    enriched["level"]           = pick("level")
    enriched["job_type"]        = pick("job_type")
    enriched["work_mode"]       = pick("work_mode")
    enriched["education_level"] = pick("education_level")
    enriched["experience"]      = pick("experience")

    # ── Giữ raw_about_job để caller quyết định xóa sau khi lưu DB ────────────
    enriched["raw_about_job"] = raw_text

    # ── Điền field còn thiếu ─────────────────────────────────────────────────
    for f in ("number_recruit", "job_category", "company_size",
              "company_industry", "job_deadline"):
        if not enriched.get(f):
            enriched[f] = ""

    # ── Xóa field không thuộc DB schema ──────────────────────────────────────
    for f in ("search_keyword", "_search_keyword", "category", "_category"):
        enriched.pop(f, None)

    # ── Delay giữa các lần gọi — tránh rate limit ────────────────────────────
    time.sleep(RATE_LIMIT_DELAY)

    return enriched


# ===== XỬ LÝ BATCH =====

def process_linkedin_batch(items: list) -> list:
    """
    Xử lý danh sách items tuần tự.
    Delay đã được handle trong process_linkedin_item().
    """
    results = []
    total   = len(items)
    for i, item in enumerate(items, 1):
        logger.info(f"[AI] Batch {i}/{total}: {item.get('job_title')!r}")
        print(f"  🤖 AI [{i}/{total}] {item.get('job_title')!r}")
        results.append(process_linkedin_item(item))
    return results