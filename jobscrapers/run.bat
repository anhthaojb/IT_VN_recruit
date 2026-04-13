
@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul

:: =============================================================
::  run_all.bat — Điều phối pipeline cào dữ liệu
::  Scrapy spiders chạy SONG SONG → Selenium scripts chạy tuần tự
:: =============================================================

set MODE=daily
@REM set MODE=full
set PYTHON=D:\ITTA\venv\Scripts\python.exe
set SCRAPY_DIR=%~dp0
set SPIDER_DIR=%SCRAPY_DIR%jobscrapers\spiders\

:: PYTHONPATH để các Selenium script tìm thấy pipelines.py và ai_processor.py
set PYTHONPATH=%SCRAPY_DIR%jobscrapers;%SCRAPY_DIR%;%PYTHONPATH%

:: Đếm lỗi
set ERRORS=0

echo.
echo ============================================================
echo  Pipeline bat dau  %DATE% %TIME%
echo  Mode    : %MODE%
echo  Scrapy  : 8 spiders (song song)
echo  Selenium: 2 scripts (tuan tu)
echo ============================================================
echo.


:: ==========================
::  PHAN 1 — SCRAPY (song song)
:: ==========================

echo [Scrapy] Bat dau chay tat ca spider song song...
echo.

%PYTHON% "%SCRAPY_DIR%run_spiders.py" %MODE%

if %ERRORLEVEL%==0 (
    echo [Scrapy] OK — Tat ca spider hoan thanh
) else (
    echo [Scrapy] FAIL (exit=%ERRORLEVEL%)
    set /a ERRORS+=1
)
echo.


:: ============================
::  PHAN 2 — SELENIUM SCRIPTS
:: ============================

echo [Selenium] Bat dau chay cac scripts...
echo.

call :run_selenium linkedin_selenium.py
call :run_selenium itviec_selenium.py

echo.
echo [Selenium] Hoan thanh tat ca scripts.
echo.


:: ==========================
::  KET QUA TONG KET
:: ==========================

echo ============================================================
if %ERRORS%==0 (
    echo  KET QUA: Tat ca hoan thanh thanh cong!
) else (
    echo  KET QUA: Co %ERRORS% spider/script gap loi — xem log ben tren
)
echo  Ket thuc: %DATE% %TIME%
echo ============================================================
echo.

exit /b %ERRORS%


:: =============================================================
::  SUBROUTINES
:: =============================================================

:run_selenium
set SCRIPT=%~1
echo   [Selenium] Bat dau: %SCRIPT%
%PYTHON% "%SPIDER_DIR%%SCRIPT%" --mode=%MODE%
if %ERRORLEVEL%==0 (
    echo   [Selenium] OK    %SCRIPT%
) else (
    echo   [Selenium] FAIL  %SCRIPT%  (exit=%ERRORLEVEL%)
    set /a ERRORS+=1
)
echo.
exit /b 0