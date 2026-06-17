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
FACT_TABLE      = "fact_jobs_etl"
FUZZY_THRESHOLD = 92

SOURCE_PRIORITY = {"itviec": 0, "linkedin": 1, "topcv": 2, "vietnamworks": 3}

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


def _load_corpus(engine, run_id_col: str | None = None,
                 days_lookback: int | None = None):
    extra_col = f", {run_id_col}" if run_id_col else ""

    if days_lookback is not None and run_id_col is not None:
        time_filter = (
            f"AND {run_id_col}::text ~ '^[0-9]{{8}}$' "
            f"AND to_date({run_id_col}::text, 'YYYYMMDD') "
            f">= (CURRENT_DATE - INTERVAL '{days_lookback} days')"
        )
    else:
        time_filter = ""

    with engine.connect() as conn:
        df = pd.read_sql(f"""
            SELECT etl_id{extra_col},
                   website_clean,
                   company_canonical_key,
                   company_name_clean,
                   job_title_detect,
                   job_title_clean,
                   location_province,
                   salary_min,
                   salary_max
            FROM {FACT_TABLE}
            WHERE is_valid = TRUE
            {time_filter}
        """, conn)

    df = df.dropna(subset=["etl_id"])
    df["etl_id"] = df["etl_id"].astype(int)
    return df


def _enrich(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["_co"]   = df["company_canonical_key"].fillna("").str.lower().str.strip()
    df["_prov"] = df["location_province"].fillna("Khác")

    # Job không có company thực sự → không tham gia dedup
    df["_skip_dedup"] = df["company_canonical_key"].isna()

    df["_src_rank"] = df["website_clean"].map(
        lambda x: SOURCE_PRIORITY.get(str(x).lower(), 99)
    )
    df["_info"] = (
        df["salary_min"].notna().astype(int) +
        df["salary_max"].notna().astype(int)
    )
    df["_tdk"] = df.apply(
        lambda r: _title_dedup_key(r["job_title_detect"],
                                   r["job_title_clean"] or ""), axis=1
    )
    return df


# ==============================================================================
# THUẬT TOÁN DEDUP
# ==============================================================================

def _find_duplicates(df_new: pd.DataFrame,
                     df_history: pd.DataFrame) -> list[dict]:
    dup_records     = []
    df_new_valid    = df_new[~df_new["_skip_dedup"]].copy()
    df_hist_valid   = df_history[~df_history["_skip_dedup"]].copy()
    already_flagged = set()

    # ── TẦNG 1: Exact match ──────────────────────────────────────────────────
    # company_canonical_key + location_province + title_detect (nếu có)
    hist_det = df_hist_valid[df_hist_valid["job_title_detect"].notna()].copy()
    hist_det["_key"] = (
        hist_det["_co"] + "||"
        + hist_det["_prov"] + "||"
        + hist_det["_tdk"]
    )

    hist_canon: dict[str, int] = {}
    for key, grp in hist_det.groupby("_key", sort=False):
        grp_sorted = grp.sort_values(
            ["etl_id", "_src_rank", "_info"],
            ascending=[True, True, False]
        )
        hist_canon[key] = int(grp_sorted.iloc[0]["etl_id"])

    new_det = df_new_valid[df_new_valid["job_title_detect"].notna()].copy()
    new_det["_key"] = (
        new_det["_co"] + "||"
        + new_det["_prov"] + "||"
        + new_det["_tdk"]
    )

    for _, row in new_det.iterrows():
        canon_id = hist_canon.get(row["_key"])
        if canon_id is not None and canon_id != int(row["etl_id"]):
            already_flagged.add(int(row["etl_id"]))
            dup_records.append({
                "dup_id":   int(row["etl_id"]),
                "canon_id": canon_id,
                "method":   "exact",
            })

    # ── TẦNG 2: Fuzzy match ──────────────────────────────────────────────────
    # Chỉ với job KHÔNG có title_detect
    # company_canonical_key EXACT + location_province EXACT + title_clean FUZZY
    new_nod  = df_new_valid[df_new_valid["job_title_detect"].isna()].copy()
    hist_nod = df_hist_valid[df_hist_valid["job_title_detect"].isna()].copy()

    for (co, prov), grp_new in new_nod.groupby(
            ["_co", "_prov"], sort=False):

        grp_hist = hist_nod[
            (hist_nod["_co"] == co) &
            (hist_nod["_prov"] == prov)
        ].copy()

        if grp_hist.empty:
            continue

        combined = (
            pd.concat([grp_hist, grp_new])
            .drop_duplicates("etl_id")
            .sort_values(
                ["etl_id", "_src_rank", "_info"],
                ascending=[True, True, False]
            )
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
                if (row_id not in df_new_valid["etl_id"].values
                        or row_id in already_flagged):
                    continue

                # Fuzzy hai chiều title_clean
                score = max(
                    _rfuzz.token_sort_ratio(titles[i], titles[j]),
                    _rfuzz.partial_ratio(titles[i],    titles[j]),
                    _rfuzz.partial_ratio(titles[j],    titles[i]),
                )
                if score < FUZZY_THRESHOLD:
                    continue

                is_dup[j] = True
                already_flagged.add(row_id)
                dup_records.append({
                    "dup_id":   row_id,
                    "canon_id": canon_id,
                    "method":   "fuzzy_title",
                })

    return dup_records


# ==============================================================================
# DAILY DEDUP
# ==============================================================================

def run_daily_deduplication(engine, run_id: str,
                             run_id_col: str | None = None,
                             days_lookback: int = 30):
    if run_id_col is None:
        run_id_col = _get_run_id_col(engine)

    print(f"\n📥 [DAILY] Tải kho {days_lookback} ngày + batch {run_id_col}={run_id}...")
    df_all = _load_corpus(engine, run_id_col=run_id_col,
                          days_lookback=days_lookback)

    if df_all.empty:
        print("🛑 Không có dữ liệu trong kho.")
        return 0

    df_new = df_all[df_all[run_id_col].astype(str) == str(run_id)].copy()
    if df_new.empty:
        print(f"⚠️  Không có job nào với {run_id_col}={run_id}.")
        print(f"   Giá trị mẫu: {df_all[run_id_col].dropna().unique()[:5].tolist()}")
        return 0

    print(f"📊 Corpus ({days_lookback}d): {len(df_all):,} | Mới: {len(df_new):,}")

    df_all = _enrich(df_all)
    df_new = df_all[df_all[run_id_col].astype(str) == str(run_id)].copy()

    dup_records = _find_duplicates(df_new=df_new, df_history=df_all)

    n_exact = sum(1 for r in dup_records if r["method"] == "exact")
    n_fuzzy = sum(1 for r in dup_records if r["method"] == "fuzzy_title")
    print(f"⚡ Phát hiện {len(dup_records):,} bản trùng "
          f"({n_exact} exact | {n_fuzzy} fuzzy).")

    if dup_records:
        print("💾 Ghi cờ trùng lặp...")
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
# ==============================================================================

def run_full_deduplication(engine):
    print("\n[FULL] Tải toàn bộ kho...")
    df_all = _load_corpus(engine)

    if df_all.empty:
        print("   Không có dữ liệu.")
        return 0

    print(f"   Đã tải {len(df_all):,} dòng. Chuẩn hóa...")
    df_all      = _enrich(df_all)
    dup_records = _find_duplicates(df_new=df_all, df_history=df_all)

    n_exact = sum(1 for r in dup_records if r["method"] == "exact")
    n_fuzzy = sum(1 for r in dup_records if r["method"] == "fuzzy_title")
    print(f"   Phát hiện {len(dup_records):,} bản trùng "
          f"({n_exact} exact | {n_fuzzy} fuzzy).")

    print("   Reset trạng thái cũ...")
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(
            f"UPDATE {FACT_TABLE} "
            f"SET is_duplicate = FALSE, "
            f"    duplicate_of_id = NULL, "
            f"    dedup_method = NULL"
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
    print(f"\n   Kích hoạt SP đồng bộ DW (scope={scope}, p_mode={p_mode})...")
    with engine.begin() as conn:
        result = conn.execute(
            sqlalchemy.text("SELECT sp_etl_load_dw(:p_mode, :p_run_id)"),
            {"p_mode": p_mode, "p_run_id": run_id},          # ✅ đổi key
        )
        print(f"   [SP]: {result.scalar()}")
    print("   ✅ DW đồng bộ xong.")

# ==============================================================================
# CLI
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Dedup + Load DW")
    parser.add_argument("--mode", choices=["daily", "full"], default="full")
    parser.add_argument("--run-id",      type=str, default=None)
    parser.add_argument("--run-id-col",  type=str, default=None)
    parser.add_argument("--days-lookback", type=int, default=30)
    parser.add_argument("--skip-dw",     action="store_true")
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
        lookback   = None if args.days_lookback == 0 else args.days_lookback
        total_dups = run_daily_deduplication(
            engine,
            run_id=args.run_id,
            run_id_col=args.run_id_col,
            days_lookback=lookback,
        )
    else:
        total_dups = run_full_deduplication(engine)

    if args.skip_dw:
        print("\n⏭️  Bỏ qua Load DW (--skip-dw).")
    else:
        run_load_dw(engine, run_id=args.run_id if args.mode == "daily" else None)

    elapsed = time.time() - start_time
    print(f"\n{'=' * 62}")
    print(f"  DONE — {total_dups:,} dups | {elapsed:.2f}s")
    print(f"{'=' * 62}\n")


if __name__ == "__main__":
    main()