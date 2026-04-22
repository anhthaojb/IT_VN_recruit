@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul

set MODE=daily
@REM set MODE=full
set PYTHON=D:\ITTA\venv\Scripts\python.exe
set SCRAPY_DIR=%~dp0
set SPIDER_DIR=%SCRAPY_DIR%jobscrapers\spiders\
set PYTHONPATH=%SCRAPY_DIR%jobscrapers;%SCRAPY_DIR%;%PYTHONPATH%

:: Dùng file tạm để đếm lỗi (setlocal không chia sẻ biến qua subroutine)
set ERROR_FILE=%TEMP%\pipeline_errors.tmp
echo 0 > "%ERROR_FILE%"

echo.
echo ============================================================
echo  Pipeline bat dau  %DATE% %TIME%
echo  Mode    : %MODE%
echo ============================================================
echo.

:: ============================
::  PHAN 1 — SELENIUM SCRIPTS
:: ============================

echo [Selenium] Bat dau chay cac scripts...
echo.

call :run_selenium linkedin_selenium.py
call :run_selenium itviec_selenium.py

echo.
echo [Selenium] Hoan thanh tat ca scripts.
echo.

:: ==========================
::  PHAN 2 — SCRAPY (song song)
:: ==========================

echo [Scrapy] Bat dau chay tat ca spider song song...
echo.

"%PYTHON%" "%SCRAPY_DIR%run_spiders.py" %MODE%

if %ERRORLEVEL%==0 (
    echo [Scrapy] OK — Tat ca spider hoan thanh
) else (
    echo [Scrapy] FAIL (exit=%ERRORLEVEL%)
    echo 1 > "%ERROR_FILE%"
)
echo.
:: ==========================
::  PHAN 3 — ETL
:: ==========================

echo [ETL] Bat dau xu ly du lieu...
echo.

:: Chỉ chạy ETL nếu Scrapy không lỗi nghiêm trọng
:: Bỏ điều kiện này nếu muốn ETL luôn chạy dù scraper lỗi
"%PYTHON%" "%SCRAPY_DIR%transform.py"

if %ERRORLEVEL%==0 (
    echo [ETL] OK — ETL hoan thanh
) else (
    echo [ETL] FAIL (exit=%ERRORLEVEL%)
    echo 1 > "%ERROR_FILE%"
)
echo.

:: ==========================
::  KET QUA TONG KET
:: ==========================

set /p ERRORS=<"%ERROR_FILE%"
del "%ERROR_FILE%" 2>nul

echo ============================================================
if "%ERRORS%"=="0" (
    echo  KET QUA: Tat ca hoan thanh thanh cong!
) else (
    echo  KET QUA: Co loi — xem log ben tren
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
"%PYTHON%" "%SPIDER_DIR%%SCRIPT%" --mode=%MODE%
if %ERRORLEVEL%==0 (
    echo   [Selenium] OK    %SCRIPT%
) else (
    echo   [Selenium] FAIL  %SCRIPT%  (exit=%ERRORLEVEL%)
    echo 1 > "%ERROR_FILE%"
)
echo.
exit /b 0