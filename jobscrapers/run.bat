@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul

set MODE=daily
@REM set MODE=full
set PYTHON=D:\ITTA\venv\Scripts\python.exe
set SCRAPY_DIR=%~dp0
set SPIDER_DIR=%SCRAPY_DIR%jobscrapers\spiders\
set PYTHONPATH=%SCRAPY_DIR%jobscrapers\spiders;%SCRAPY_DIR%jobscrapers;%SCRAPY_DIR%;%PYTHONPATH%

:: FIX unicode
set PYTHONIOENCODING=utf-8

:: FIX mysql path
set PATH=%PATH%;C:\Program Files\MySQL\MySQL Server 9.4\bin

:: FIX LOG_FILE — DATE format "Mon 04/27/2026"
set LOG_FILE=%SCRAPY_DIR%pipeline_%DATE:~10,4%%DATE:~4,2%%DATE:~7,2%.log

set ERROR_FILE=%TEMP%\pipeline_errors.tmp
echo 0 > "%ERROR_FILE%"

echo. >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"
echo  Pipeline bat dau  %DATE% %TIME% >> "%LOG_FILE%"
echo  Mode    : %MODE% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

type "%LOG_FILE%"

:: ============================
::  PHAN 1 — SELENIUM SCRIPTS
:: ============================

call :run_selenium linkedin_selenium.py
call :run_selenium itviec_selenium.py

:: ==========================
::  PHAN 2 — SCRAPY
:: ==========================

echo [Scrapy] Bat dau chay tat ca spider song song... >> "%LOG_FILE%"

"%PYTHON%" "%SCRAPY_DIR%run_spiders.py" %MODE% >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL%==0 (
    echo [Scrapy] OK — Tat ca spider hoan thanh >> "%LOG_FILE%"
) else (
    echo [Scrapy] FAIL (exit=%ERRORLEVEL%) >> "%LOG_FILE%"
    echo 1 > "%ERROR_FILE%"
)

:: ==========================
::  PHAN 3 — ETL
:: ==========================

set /p CURRENT_ERR=<"%ERROR_FILE%"
if "%CURRENT_ERR%"=="1" (
    echo [ETL] SKIP — Scrapy co loi, bo qua ETL >> "%LOG_FILE%"
    goto :skip_etl
)

echo [ETL] Bat dau xu ly du lieu... >> "%LOG_FILE%"

if /I "%MODE%"=="full" (
    "%PYTHON%" "%SCRAPY_DIR%jobscrapers\transform.py" --all >> "%LOG_FILE%" 2>&1
) else (
    "%PYTHON%" "%SCRAPY_DIR%jobscrapers\transform.py" >> "%LOG_FILE%" 2>&1
)

if %ERRORLEVEL%==0 (
    echo [ETL] OK — ETL hoan thanh >> "%LOG_FILE%"
) else (
    echo [ETL] FAIL (exit=%ERRORLEVEL%) >> "%LOG_FILE%"
    echo 1 > "%ERROR_FILE%"
)

:skip_etl

:: ==========================
::  PHAN 4 — LOAD DW
:: ==========================

echo [DW] Bat dau nap Data Warehouse... >> "%LOG_FILE%"

set MYSQL_HOST=localhost
set MYSQL_USER=root
set MYSQL_PASS=123456
set MYSQL_DB=itta

if /I "%MODE%"=="daily" (set SP_MODE=today) else (set SP_MODE=all)

mysql -h %MYSQL_HOST% -u %MYSQL_USER% -p%MYSQL_PASS% %MYSQL_DB% ^
    -e "CALL sp_ETL_Load_DW('%SP_MODE%', NULL);" >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL%==0 (
    echo [DW] OK — Load DW hoan thanh >> "%LOG_FILE%"
) else (
    echo [DW] FAIL (exit=%ERRORLEVEL%) >> "%LOG_FILE%"
    echo 1 > "%ERROR_FILE%"
)

:: ==========================
::  KET QUA TONG KET
:: ==========================

set /p ERRORS=<"%ERROR_FILE%"
del "%ERROR_FILE%" 2>nul

echo ============================================================ >> "%LOG_FILE%"
if "%ERRORS%"=="0" (
    echo  KET QUA: Tat ca hoan thanh thanh cong! >> "%LOG_FILE%"
) else (
    echo  KET QUA: Co loi — xem: %LOG_FILE% >> "%LOG_FILE%"
)
echo  Ket thuc: %DATE% %TIME% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

type "%LOG_FILE%"
exit /b %ERRORS%

:: =============================================================
::  SUBROUTINES
:: =============================================================

:run_selenium
set SCRIPT=%~1
echo   [Selenium] Bat dau: %SCRIPT% >> "%LOG_FILE%"
"%PYTHON%" "%SPIDER_DIR%%SCRIPT%" --mode=%MODE% >> "%LOG_FILE%" 2>&1
if %ERRORLEVEL%==0 (
    echo   [Selenium] OK    %SCRIPT% >> "%LOG_FILE%"
) else (
    echo   [Selenium] FAIL  %SCRIPT% (exit=%ERRORLEVEL%) >> "%LOG_FILE%"
    echo 1 > "%ERROR_FILE%"
)
exit /b 0