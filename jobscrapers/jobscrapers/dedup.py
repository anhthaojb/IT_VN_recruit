import os
import re
import sys
import time
import argparse
import sqlalchemy
import pandas as pd
from rapidfuzz import fuzz as _rfuzz

# ==============================================================================
# CẤU HÌNH KẾT NỐI
# ==============================================================================
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:123456@localhost:5432/recruitment_dw"
)
FACT_TABLE = "fact_jobs_etl"

SOURCE_PRIORITY = {"itviec": 0, "linkedin": 1, "topcv": 2, "vietnamworks": 3}
FUZZY_THRESHOLD = 90

_TECH_IN_TITLE = [
    "java", "python", "golang", "go", "nodejs", "node.js", "php",
    "react", "angular", "vue", "flutter", "android", "ios", "swift",
    "kotlin", ".net", "c#", "ruby", "scala", "rust",
]
_TECH_PATTERNS = [
    (t, re.compile(r"(?<![a-z0-9])" + re.escape(t) + r"(?![a-z0-9])", re.IGNORECASE))
    for t in _TECH_IN_TITLE
]


def _title_dedup_key(title_detect, title_clean):
    td = "" if (title_detect is None or isinstance(title_detect, float)) else str(title_detect)
    tc = "" if (title_clean  is None or isinstance(title_clean,  float)) else str(title_clean)
    base = td.strip().lower()
    if not base:
        return tc.strip().lower()
    tech = next((label for label, pat in _TECH_PATTERNS if pat.search(tc)), "")
    return f"{base}::{tech}" if tech else base


# ==============================================================================
# LOAD DỮ LIỆU
# ==============================================================================

def _get_run_id_col(engine) -> str:
    candidates = [
        "etl_run_id", "run_id", "batch_id", "etl_batch",
        "scraped_at", "crawled_date", "created_at", "etl_date",
    ]
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = :tbl
            ORDER BY ordinal_position
        """), {"tbl": FACT_TABLE})
        cols = {row[0] for row in result}
    for c in candidates:
        if c in cols:
            print(f"   ℹ️  Dùng cột '{c}' làm batch key cho daily mode.")
            return c
    raise RuntimeError(
        f"Không tìm thấy cột batch trong {FACT_TABLE}. "
        f"Các cột hiện có: {cols}. "
        f"Hãy truyền --run-id-col."
    )


def _load_corpus(engine, run_id_col: str | None = None, days_lookback: int | None = None):
    """
    Tải kho (is_valid=TRUE) làm nền để so sánh.
    - daily mode: chỉ load days_lookback ngày gần nhất
    - full mode:  days_lookback=None → load toàn kho
    """
    extra_col = f", {run_id_col}" if run_id_col else ""

    if days_lookback is not None and run_id_col is not None:
        time_filter = (
    f"AND {run_id_col}::text ~ '^[0-9]{{8}}$' "
    f"AND to_date({run_id_col}::text, 'YYYYMMDD') >= (CURRENT_DATE - INTERVAL '{days_lookback} days')"
)
    else:
        time_filter = ""

    with engine.connect() as conn:
        df = pd.read_sql(f"""
            SELECT etl_id{extra_col}, website_clean, company_name_clean,
                   job_title_detect, job_title_clean, location_province,
                   salary_min, salary_max
            FROM {FACT_TABLE}
            WHERE is_valid = TRUE
            {time_filter}
        """, conn)

    # FIX: drop NaN etl_id ngay khi load — tránh lỗi int(NaN) về sau
    df = df.dropna(subset=["etl_id"])
    df["etl_id"] = df["etl_id"].astype(int)

    return df


def _enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Thêm các cột tính toán phục vụ dedup."""
    df = df.copy()
    df["_co"]       = df["company_name_clean"].fillna("unknown").str.lower().str.strip()
    df["_prov"]     = df["location_province"].fillna("Khác")
    df["_src_rank"] = df["website_clean"].map(lambda x: SOURCE_PRIORITY.get(str(x).lower(), 99))
    df["_info"]     = (
        df["salary_min"].notna().astype(int) + df["salary_max"].notna().astype(int)
    )
    df["_tdk"] = df.apply(
        lambda r: _title_dedup_key(r["job_title_detect"], r["job_title_clean"] or ""), axis=1
    )
    return df


# ==============================================================================
# THUẬT TOÁN DEDUP
# ==============================================================================

def _find_duplicates(df_new: pd.DataFrame, df_history: pd.DataFrame) -> list[dict]:
    """
    So sánh df_new với df_history để tìm bản trùng.

    Chiến lược:
      - Có title_detect   → EXACT match theo key (company + title_detect + province)
      - Không title_detect → FUZZY match dùng job_title_clean,
                             so với TOÀN BỘ history cùng company + province

    Trả về list[{dup_id, canon_id, method}]
    """
    dup_records = []

    # ---- EXACT MATCH ----
    hist_det = df_history[df_history["job_title_detect"].notna()].copy()
    hist_det["_key"] = hist_det["_co"] + "||" + hist_det["_tdk"] + "||" + hist_det["_prov"]

    hist_canon: dict[str, int] = {}
    for key, grp in hist_det.groupby("_key", sort=False):
        grp_sorted = grp.sort_values(
            ["etl_id", "_src_rank", "_info"], ascending=[True, True, False]
        )
        hist_canon[key] = int(grp_sorted.iloc[0]["etl_id"])

    new_det = df_new[df_new["job_title_detect"].notna()].copy()
    new_det["_key"] = new_det["_co"] + "||" + new_det["_tdk"] + "||" + new_det["_prov"]

    for _, row in new_det.iterrows():
        canon_id = hist_canon.get(row["_key"])
        if canon_id is not None and canon_id != int(row["etl_id"]):
            dup_records.append({
                "dup_id":   int(row["etl_id"]),
                "canon_id": canon_id,
                "method":   "exact",
            })

    # ---- FUZZY MATCH ----
    # Áp dụng cho job KHÔNG CÓ title_detect, dùng job_title_clean để so sánh
    # So với TOÀN BỘ history cùng company + province (kể cả job có detect)
    already_flagged = {r["dup_id"] for r in dup_records}
    new_nod = df_new[df_new["job_title_detect"].isna()].copy()

    for (co, prov), grp_new in new_nod.groupby(["_co", "_prov"], sort=False):
        grp_hist = df_history[
            (df_history["_co"] == co) & (df_history["_prov"] == prov)
        ].copy()

        combined = pd.concat([grp_hist, grp_new]).drop_duplicates("etl_id")
        combined = combined.sort_values(
            ["etl_id", "_src_rank", "_info"], ascending=[True, True, False]
        )

        titles = combined["job_title_clean"].fillna("").str.lower().tolist()
        ids    = combined["etl_id"].tolist()
        is_dup = [False] * len(combined)

        for i in range(len(titles)):
            if is_dup[i]:
                continue
            canon_id = int(ids[i])
            for j in range(i + 1, len(titles)):
                if is_dup[j]:
                    continue
                row_id = int(ids[j])
                if row_id in df_new["etl_id"].values and row_id not in already_flagged:
                    if _rfuzz.token_sort_ratio(titles[i], titles[j]) >= FUZZY_THRESHOLD:
                        is_dup[j] = True
                        already_flagged.add(row_id)
                        dup_records.append({
                            "dup_id":   row_id,
                            "canon_id": canon_id,
                            "method":   "fuzzy",
                        })

    return dup_records


# ==============================================================================
# DAILY DEDUP
# Chỉ xét job mới (run_id hôm nay), so sánh với kho N ngày gần nhất
# KHÔNG reset cờ cũ
# ==============================================================================

def run_daily_deduplication(engine, run_id: str, run_id_col: str | None = None,
                            days_lookback: int = 30):
    if run_id_col is None:
        run_id_col = _get_run_id_col(engine)

    print(f"\n📥 [DAILY] Tải kho {days_lookback} ngày gần nhất + batch {run_id_col}={run_id}...")
    df_all = _load_corpus(engine, run_id_col=run_id_col, days_lookback=days_lookback)

    if df_all.empty:
        print("🛑 Không có dữ liệu trong kho.")
        return 0

    df_new = df_all[df_all[run_id_col].astype(str) == str(run_id)].copy()
    if df_new.empty:
        print(f"⚠️  Không có job nào với {run_id_col}={run_id}.")
        print(f"   Giá trị mẫu trong cột: {df_all[run_id_col].dropna().unique()[:5].tolist()}")
        return 0

    print(f"📊 Corpus ({days_lookback}d): {len(df_all):,} dòng | Mới hôm nay: {len(df_new):,} dòng")

    df_all = _enrich(df_all)
    df_new = df_all[df_all[run_id_col].astype(str) == str(run_id)].copy()

    dup_records = _find_duplicates(df_new=df_new, df_history=df_all)

    n_exact = sum(1 for r in dup_records if r["method"] == "exact")
    n_fuzzy = sum(1 for r in dup_records if r["method"] == "fuzzy")
    print(f"⚡ Phát hiện {len(dup_records):,} bản trùng "
          f"({n_exact} exact | {n_fuzzy} fuzzy).")

    if dup_records:
        print("💾 Ghi cờ trùng lặp (chỉ dòng mới)...")
        with engine.begin() as conn:
            for i in range(0, len(dup_records), 20):
                batch = dup_records[i:i + 20]
                conn.execute(sqlalchemy.text(f"""
                    UPDATE {FACT_TABLE}
                    SET    is_duplicate     = TRUE,
                           duplicate_of_id = :canon_id,
                           dedup_method    = :method
                    WHERE  etl_id = :dup_id
                """), batch)

    return len(dup_records)


# ==============================================================================
# FULL DEDUP
# Reset toàn bộ kho + quét lại 100%
# ==============================================================================

def run_full_deduplication(engine):
    print("\n[FULL] Tải toàn bộ kho...")
    df_all = _load_corpus(engine)

    if df_all.empty:
        print("   Không có dữ liệu trong kho.")
        return 0

    print(f"   Đã tải {len(df_all):,} dòng. Tiến hành chuẩn hóa...")
    df_all = _enrich(df_all)
    dup_records = _find_duplicates(df_new=df_all, df_history=df_all)

    n_exact = sum(1 for r in dup_records if r["method"] == "exact")
    n_fuzzy = sum(1 for r in dup_records if r["method"] == "fuzzy")
    print(f"   Phát hiện {len(dup_records):,} bản trùng "
          f"({n_exact} exact | {n_fuzzy} fuzzy).")

    print("   Reset trạng thái cũ trên Database...")
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(
            f"UPDATE {FACT_TABLE} "
            f"SET is_duplicate = FALSE, duplicate_of_id = NULL, dedup_method = NULL"
        ))

    if dup_records:
        print("   Ghi cờ trùng lặp (toàn kho)...")
        with engine.begin() as conn:
            for i in range(0, len(dup_records), 20):
                batch = dup_records[i:i + 20]
                conn.execute(sqlalchemy.text(f"""
                    UPDATE {FACT_TABLE}
                    SET    is_duplicate     = TRUE,
                           duplicate_of_id = :canon_id,
                           dedup_method    = :method
                    WHERE  etl_id = :dup_id
                """), batch)

    return len(dup_records)


# ==============================================================================
# LOAD DATA WAREHOUSE
# ==============================================================================

def run_load_dw(engine, run_id: str | None = None):
    p_mode = "today" if run_id else "all"
    scope  = f"run_id={run_id}" if run_id else "all"
    print(f"\n   Kích hoạt Stored Procedure đồng bộ DW (scope={scope}, p_mode={p_mode})...")
    with engine.begin() as conn:
        result = conn.execute(
            sqlalchemy.text("SELECT sp_etl_load_dw(:p_mode, :run_id)"),
            {"p_mode": p_mode, "run_id": run_id},
        )
        msg = result.scalar()
        print(f"   [SP Message]: {msg}")
    print("   ✅ Data Warehouse đã được đồng bộ.")


# ==============================================================================
# CLI
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Dedup + Load DW")
    parser.add_argument(
        "--mode",
        choices=["daily", "full"],
        default="full",
        help="daily: chỉ dedup batch mới (cần --run-id) | full: reset + quét lại 100%%",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="run_id của batch hôm nay (bắt buộc với --mode daily)",
    )
    parser.add_argument(
        "--run-id-col",
        type=str,
        default=None,
        help="Tên cột batch trong DB (mặc định: tự phát hiện). VD: crawled_date, created_at",
    )
    parser.add_argument(
        "--days-lookback",
        type=int,
        default=30,
        help="Daily mode: số ngày corpus để so sánh (mặc định: 30). Dùng 0 để load toàn kho.",
    )
    parser.add_argument(
        "--skip-dw",
        action="store_true",
        help="Bỏ qua bước Load Data Warehouse",
    )
    args = parser.parse_args()

    if args.mode == "daily" and not args.run_id:
        parser.error("--mode daily yêu cầu --run-id <run_id>")

    engine = sqlalchemy.create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_size=3,
        max_overflow=2,
    )

    print(f"\n{'=' * 62}")
    print(f"  DEDUP START [{time.strftime('%Y-%m-%d %H:%M:%S')}]  mode={args.mode}")
    print(f"{'=' * 62}")
    start_time = time.time()

    if args.mode == "daily":
        lookback = None if args.days_lookback == 0 else args.days_lookback
        total_dups = run_daily_deduplication(
            engine,
            run_id=args.run_id,
            run_id_col=args.run_id_col,
            days_lookback=lookback,
        )
    else:
        total_dups = run_full_deduplication(engine)

    if args.skip_dw:
        print("\n⏭️  Bỏ qua bước Load DW (--skip-dw được bật).")
    else:
        run_load_dw(engine, run_id=args.run_id if args.mode == "daily" else None)

    elapsed = time.time() - start_time
    print(f"\n{'=' * 62}")
    print(f"  DEDUP DONE — {total_dups:,} dups flagged | {elapsed:.2f}s")
    print(f"{'=' * 62}\n")


if __name__ == "__main__":
    main()