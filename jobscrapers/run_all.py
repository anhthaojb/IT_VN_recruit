#!/usr/bin/env python3
"""
run_all.py — Điều phối toàn bộ pipeline cào dữ liệu
=====================================================
Cách dùng:
  python run_all.py          # daily mode (mặc định) — chỉ job mới 24h
  python run_all.py full     # full mode — cào hết, không giới hạn

Chiến lược:
  - Scrapy spiders  : chạy trong thread phụ (subprocess per spider)
  - Selenium scripts: chạy trong main thread (Chrome cần main thread)
  - Hai nhóm chạy SONG SONG với nhau
"""

import subprocess
import sys
import time
import threading
import os
from datetime import datetime

# =============================================================================
#  CẤU HÌNH — chỉnh sửa tại đây
# =============================================================================

# Scrapy spiders — tên spider (giá trị trong `name = "..."`)
SCRAPY_SPIDERS = [
    "topcv",
    # "careerlink",
    "careerviet",
    "joboko",
    "jobsgo",
    "timviec365",
    "vieclam24h",
    "vietnamwork",
    # "glints",   # chưa code
]

# Selenium scripts — đường dẫn tới file .py chạy độc lập
SELENIUM_SCRIPTS = [
    "linkedin_selenium.py",
    "itviec_selenium.py",
]

# Python executable — dùng cùng venv với run_all.py
PYTHON = sys.executable


# =============================================================================
#  Scrapy spiders
# =============================================================================

def run_scrapy_spiders(spider_names: list, mode: str) -> dict:
    """
    Chạy từng Scrapy spider tuần tự trong thread phụ.
    Truyền CRAWL_MODE vào settings qua -s flag.
    Trả về {spider_name: returncode}.
    """
    results = {}
    for name in spider_names:
        print(f"  [Scrapy] Bắt đầu: {name}  (mode={mode})")
        start = time.time()
        proc = subprocess.run(
            [
                PYTHON, "-m", "scrapy", "crawl", name,
                "-s", f"CRAWL_MODE={mode}",
            ],
            capture_output=False,
        )
        elapsed = time.time() - start
        results[name] = proc.returncode
        status = "OK" if proc.returncode == 0 else "FAIL"
        print(f"  [Scrapy] {status} {name} — {elapsed:.0f}s (exit={proc.returncode})")
    return results


# =============================================================================
#  Selenium scripts
# =============================================================================

def run_selenium_scripts(scripts: list, mode: str) -> dict:
    """
    Chạy từng Selenium script tuần tự trong main thread.
    Tuần tự để tránh nhiều Chrome cùng lúc ngốn RAM.
    Truyền --mode vào argparse của mỗi script.
    Trả về {script_path: returncode}.
    """
    results = {}
    for script in scripts:
        print(f"  [Selenium] Bắt đầu: {script}  (mode={mode})")
        start = time.time()
        proc = subprocess.run(
            [PYTHON, script, f"--mode={mode}"],
            capture_output=False,
        )
        elapsed = time.time() - start
        results[script] = proc.returncode
        status = "OK" if proc.returncode == 0 else "FAIL"
        print(f"  [Selenium] {status} {script} — {elapsed:.0f}s (exit={proc.returncode})")
    return results


# =============================================================================
#  Main
# =============================================================================

def main():
    # Đọc mode từ argument
    # python run_all.py        -> daily (mac dinh)
    # python run_all.py full   -> full
    mode = "daily"
    if len(sys.argv) > 1:
        arg = sys.argv[1].strip().lower()
        if arg in ("full", "daily"):
            mode = arg
        else:
            print(f"Argument khong hop le: {sys.argv[1]!r}")
            print("Dung: python run_all.py [full|daily]")
            sys.exit(1)

    run_start = datetime.now()
    print(f"\n{'='*60}")
    print(f"Pipeline bat dau  {run_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Mode    : {mode.upper()}")
    print(f"  Scrapy  : {len(SCRAPY_SPIDERS)} spider")
    print(f"  Selenium: {len(SELENIUM_SCRIPTS)} script")
    print(f"{'='*60}\n")

    scrapy_results   = {}
    selenium_results = {}

    scrapy_thread = threading.Thread(
        target=lambda: scrapy_results.update(
            run_scrapy_spiders(SCRAPY_SPIDERS, mode)
        ),
        name="scrapy-thread",
        daemon=True,
    )

    if SCRAPY_SPIDERS:
        scrapy_thread.start()
        print("[Main] Scrapy thread da khoi dong\n")

    if SELENIUM_SCRIPTS:
        print("[Main] Selenium scripts bat dau (main thread)\n")
        selenium_results = run_selenium_scripts(SELENIUM_SCRIPTS, mode)

    if SCRAPY_SPIDERS:
        print("\n[Main] Cho Scrapy thread hoan thanh...")
        scrapy_thread.join()

    elapsed = (datetime.now() - run_start).seconds
    print(f"\n{'='*60}")
    print(f"KET QUA | mode={mode} | {elapsed // 60}m{elapsed % 60}s")
    print(f"{'='*60}")

    all_ok = True

    if scrapy_results:
        print("\n[Scrapy]")
        for name, code in scrapy_results.items():
            icon = "OK" if code == 0 else "FAIL"
            print(f"  {icon} {name}  (exit={code})")
            if code != 0:
                all_ok = False

    if selenium_results:
        print("\n[Selenium]")
        for script, code in selenium_results.items():
            icon = "OK" if code == 0 else "FAIL"
            print(f"  {icon} {os.path.basename(script)}  (exit={code})")
            if code != 0:
                all_ok = False

    print(f"\n{'='*60}")
    print("Tat ca hoan thanh!" if all_ok else "Mot so spider gap loi — xem log ben tren")
    print(f"{'='*60}\n")

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())