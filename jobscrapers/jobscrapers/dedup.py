import os
import re
import time
import argparse
import sqlalchemy
import pandas as pd
from rapidfuzz import fuzz as _rfuzz

try:
    from lookups import SKILL_MAP
except ImportError:
    from jobscrapers.lookups import SKILL_MAP

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:123456@localhost:5432/recruitment_dw"
)
FACT_TABLE      = "fact_jobs_etl"
FUZZY_THRESHOLD = 92

SOURCE_PRIORITY = {"itviec": 0, "linkedin": 1, "topcv": 2, "vietnamworks": 3}

def _build_tech_patterns() -> list[tuple[str, re.Pattern]]:
    patterns = []
    for skill_name, kws in SKILL_MAP.get("hard", {}).items():
        for kw in sorted(kws, key=len, reverse=True):
            if re.fullmatch(r"[a-z0-9]+", kw, re.IGNORECASE):
                pat = re.compile(
                    r"(?<![a-z0-9])" + re.escape(kw) + r"(?![a-z0-9])",
                    re.IGNORECASE,
                )
            else:
                pat = re.compile(kw, re.IGNORECASE)
            patterns.append((skill_name, pat))
    return patterns

_TECH_PATTERNS = _build_tech_patterns()

def _title_dedup_key(title_clean):
    """Khóa dedup dùng trực tiếp job_title_clean, chỉ trim và lowercase."""
    if title_clean is None or isinstance(title_clean, float):
        return ""
    return str(title_clean).strip().lower()

def _get_run_id_col(engine) -> str:
    candidates = ["etl_run_id", "run_id", "batch_id", "etl_batch"]

    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = :tbl
            ORDER BY ordinal_position
        """), {"tbl": FACT_TABLE})
        cols = {row[0] for row in result}

    for c in candidates:
        if c in cols:
            print(f" Dùng cột '{c}' làm batch key cho daily mode.")
            return c

    raise RuntimeError(
        f"KHÔNG tìm thấy cột batch id hợp lệ trong {FACT_TABLE} "
        f"(đã thử: {candidates}). "
        f"Các cột hiện có: {sorted(cols)}. "
        f"Dedup theo --mode daily cần một cột định danh batch (không phải timestamp) "
        f"để tránh so sánh sai. Hãy tạo cột này hoặc truyền --run-id-col tường minh."
    )

def _load_corpus(engine, run_id_col: str | None = None,
                 days_lookback: int | None = None):
    extra_col = f", {run_id_col}" if run_id_col else ""

    if days_lookback is not None:
        time_filter = (
            f"AND job_posted_at_clean >= NOW() - INTERVAL '{days_lookback} days'"
        )
    else:
        time_filter = ""

    with engine.connect() as conn:
        df = pd.read_sql(f"""
            SELECT etl_id{extra_col},
                   website_clean,
                   company_canonical_key,
                   company_title_clean,
                   job_title_detect,
                   job_title_clean,
                   level_clean,
                   location_province,
                   salary_min,
                   salary_max,
                   job_posted_at_clean
            FROM {FACT_TABLE}
            WHERE is_valid = TRUE
            AND job_posted_at_clean IS NOT NULL
            {time_filter}
        """, conn)

    df = df.dropna(subset=["etl_id"])
    df["etl_id"] = df["etl_id"].astype(int)
    df["job_posted_at_clean"] = pd.to_datetime(df["job_posted_at_clean"])

    n_before = len(df)
    df = df.dropna(subset=["job_posted_at_clean"])
    n_dropped = n_before - len(df)
    if n_dropped:
        print(f"    Loại {n_dropped} dòng có job_posted_at_clean không hợp lệ (NaT).")

    return df
def _enrich(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    company_clean = (
        df["company_title_clean"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    co_key = (
        df["company_canonical_key"]
        .fillna("")
        .astype(str)
        .str.lower()
        .str.strip()
    )

    prov_key = (
        df["location_province"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    df["_skip_dedup"] = (
        company_clean.eq("unknown")
        | co_key.eq("")
        | prov_key.eq("")
    )

    df["_co"]   = co_key
    df["_prov"] = prov_key

    df["_src_rank"] = df["website_clean"].map(
        lambda x: SOURCE_PRIORITY.get(str(x).lower(), 99)
    )

    df["_info"] = (
        df["salary_min"].notna().astype(int)
        + df["salary_max"].notna().astype(int)
    )

    df["_tdk"] = df["job_title_clean"].map(_title_dedup_key)
    df["_level"] = (
        df["level_clean"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    return df
def _find_duplicates(df_new: pd.DataFrame,
                     df_history: pd.DataFrame,
                     days_lookback: int = 90) -> list[dict]:
    from pandas import Timedelta
    max_delta = Timedelta(days=days_lookback)

    dup_records     = []
    already_flagged = set()

    df_new_valid  = df_new[~df_new["_skip_dedup"]].copy()
    df_hist_valid = df_history[~df_history["_skip_dedup"]].copy()
    new_ids = set(df_new_valid["etl_id"].astype(int).tolist())

    combined = (
        pd.concat([df_hist_valid, df_new_valid])
        .drop_duplicates("etl_id")
        .copy()
    )
    combined["etl_id"] = combined["etl_id"].astype(int)

    exact_pool = combined[combined["_tdk"].ne("")].copy()
    exact_pool["_key"] = (
        exact_pool["_co"]
        + "||" + exact_pool["_prov"]
        + "||" + exact_pool["_tdk"]
        + "||" + exact_pool["_level"]
    )

    for _key, grp in exact_pool.groupby("_key", sort=False):
        grp_sorted = grp.sort_values(
            ["job_posted_at_clean", "_src_rank", "_info"],
            ascending=[True, True, False],
            kind="stable",
        )

        root_id     = None
        last_posted = None

        for _, row in grp_sorted.iterrows():
            eid    = int(row["etl_id"])
            posted = row["job_posted_at_clean"]

            if root_id is None:
                root_id, last_posted = eid, posted
                continue

            delta = posted - last_posted
            if delta > max_delta:
                root_id, last_posted = eid, posted
                continue

            last_posted = posted
            if eid == root_id:
                continue
            if eid in new_ids and eid not in already_flagged:
                already_flagged.add(eid)
                dup_records.append({"dup_id": eid, "canon_id": root_id, "method": "exact"})

    fuzzy_pool = combined[
        combined["_tdk"].ne("")
        & ~combined["etl_id"].isin(already_flagged)
    ].copy()

    for (co, prov), grp in fuzzy_pool.groupby(["_co", "_prov"], sort=False):
        grp_sorted = grp.sort_values(
            ["job_posted_at_clean", "_src_rank", "_info"],
            ascending=[True, True, False],
            kind="stable",
        )

        chains = []

        for _, row in grp_sorted.iterrows():
            eid    = int(row["etl_id"])
            posted = row["job_posted_at_clean"]
            title  = row["_tdk"]
            level  = row["_level"]

            chains = [c for c in chains if (posted - c["last_posted"]) <= max_delta]

            best_chain, best_score = None, -1
            for c in chains:
                if c["level"] and level and c["level"] != level:
                    continue
                score = max(
                    _rfuzz.token_sort_ratio(c["last_title"], title),
                    _rfuzz.token_set_ratio(c["last_title"], title),
                )
                if score >= FUZZY_THRESHOLD and score > best_score:
                    best_score, best_chain = score, c

            if best_chain is not None:
                best_chain["last_posted"] = posted
                best_chain["last_title"]  = title
                if not best_chain["level"] and level:
                    best_chain["level"] = level
                if eid != best_chain["root_id"] and eid in new_ids and eid not in already_flagged:
                    already_flagged.add(eid)
                    dup_records.append({
                        "dup_id":   eid,
                        "canon_id": best_chain["root_id"],
                        "method":   "fuzzy_title",
                    })
            else:
                chains.append({
                    "root_id": eid, "last_posted": posted,
                    "last_title": title, "level": level,
                })

    return dup_records


def run_daily_deduplication(engine, run_id: str,
                             run_id_col: str | None = None,
                             load_window_days: int = 90,
                             match_window_days: int = 45):
    if run_id_col is None:
        run_id_col = _get_run_id_col(engine)

    print(f"\n [DAILY] Tải kho {load_window_days} ngày "
          f"(match window {match_window_days}d) + batch {run_id_col}={run_id}...")
    df_all = _load_corpus(engine, run_id_col=run_id_col,
                          days_lookback=load_window_days)

    if df_all.empty:
        print("Không có dữ liệu trong kho.")
        return 0

    df_new = df_all[df_all[run_id_col].astype(str) == str(run_id)].copy()
    if df_new.empty:
        print(f"Không có job nào với {run_id_col}={run_id}.")
        print(f"   Giá trị mẫu: {df_all[run_id_col].dropna().unique()[:5].tolist()}")
        return 0

    print(f"Corpus ({load_window_days}d): {len(df_all):,} | Mới: {len(df_new):,}")

    df_all = _enrich(df_all)
    df_new = df_all[df_all[run_id_col].astype(str) == str(run_id)].copy()

    dup_records = _find_duplicates(df_new=df_new, df_history=df_all,
                                   days_lookback=match_window_days)

    n_exact = sum(1 for r in dup_records if r["method"] == "exact")
    n_fuzzy = sum(1 for r in dup_records if r["method"] == "fuzzy_title")
    print(f"Phát hiện {len(dup_records):,} bản trùng "
          f"({n_exact} exact | {n_fuzzy} fuzzy).")

    if dup_records:
        print("Ghi cờ trùng lặp...")
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


def run_full_deduplication(engine, match_window_days: int = 45,
                            run_id_col: str | None = None):
    print(f"\n[FULL] Tải toàn bộ kho (so sánh trong vòng {match_window_days} ngày)...")
    if run_id_col is None:
        run_id_col = _get_run_id_col(engine)
    df_all = _load_corpus(engine, run_id_col=run_id_col,
                          days_lookback=None)
    if df_all.empty:
        print("   Không có dữ liệu.")
        return 0

    print(f"   Đã tải {len(df_all):,} dòng. Chuẩn hóa...")
    df_all      = _enrich(df_all)
    dup_records = _find_duplicates(df_new=df_all, df_history=df_all,
                                   days_lookback=match_window_days)
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

def run_load_dw(engine, run_id: str | None = None):
    p_mode = "today" if run_id else "all"
    scope  = f"run_id={run_id}" if run_id else "all"
    print(f"\n   Kích hoạt SP đồng bộ DW (scope={scope}, p_mode={p_mode})...")
    with engine.begin() as conn:
        result = conn.execute(
            sqlalchemy.text("SELECT sp_etl_load_dw(:p_mode, :p_run_id)"),
            {"p_mode": p_mode, "p_run_id": run_id},
        )
        print(f"   [SP]: {result.scalar()}")
    print("   DW đồng bộ xong.")

def main():
    parser = argparse.ArgumentParser(description="Dedup + Load DW")
    parser.add_argument("--mode", choices=["daily", "full"], default="full")
    parser.add_argument("--run-id",      type=str, default=None)
    parser.add_argument("--run-id-col",  type=str, default=None)
    parser.add_argument("--days-lookback", type=int, default=None)
    parser.add_argument("--load-window-days", type=int, default=None)
    parser.add_argument("--match-window-days", type=int, default=None)

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
        match_window = args.match_window_days if args.match_window_days is not None else (
            args.days_lookback if args.days_lookback is not None else 45
        )
        load_window = args.load_window_days if args.load_window_days is not None else 90
        if load_window < match_window:
            print(f"    load_window_days ({load_window}) < match_window_days ({match_window}) "
                  f"-> nâng load_window_days lên {match_window} để tránh bỏ sót.")
            load_window = match_window
        total_dups = run_daily_deduplication(
            engine, run_id=args.run_id, run_id_col=args.run_id_col,
            load_window_days=load_window, match_window_days=match_window,
        )
    else:
        match_window = args.match_window_days if args.match_window_days is not None else (
            args.days_lookback if args.days_lookback is not None else 45
        )
        total_dups = run_full_deduplication(
            engine, match_window_days=match_window, run_id_col=args.run_id_col,
        )
    if args.skip_dw:
        print("\n Bỏ qua Load DW (--skip-dw).")
    else:
        run_load_dw(engine, run_id=args.run_id if args.mode == "daily" else None)

    elapsed = time.time() - start_time
    print(f"\n{'=' * 62}")
    print(f"  DONE — {total_dups:,} dups | {elapsed:.2f}s")
    print(f"{'=' * 62}\n")

if __name__ == "__main__":
    main()