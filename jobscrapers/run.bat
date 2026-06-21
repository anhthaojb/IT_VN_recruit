@echo off
setlocal EnableDelayedExpansion
chcp 65001 >nul

:: ============================================================
:: ROOT
:: ============================================================

set ROOT_DIR=D:\ITTA\jobscrapers
cd /d "%ROOT_DIR%"

:: Python
set PYTHON=D:\ITTA\venv\Scripts\python.exe


:: package root
set PYTHONPATH=%ROOT_DIR%

set PYTHONIOENCODING=utf-8

:: Database
set DATABASE_URL=postgresql://postgres:123456@localhost:5432/recruitment_dw

:: PostgreSQL CLI
set PATH=%PATH%;C:\Program Files\PostgreSQL\18\bin
:: API Keys
if not exist "%ROOT_DIR%\..\env" (
    call :log "WARN - Khong tim thay file env"
) else (
    for /f "usebackq tokens=1,* delims==" %%a in ("%ROOT_DIR%\..\env") do (
        if "%%a"=="GROQ_API_KEY" set GROQ_API_KEY=%%b
    )
)
:: Datef
for /f "delims=" %%d in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"') do (
set _DATE=%%d
)

:: Log
for /f "delims=" %%t in ('powershell -NoProfile -Command "Get-Date -Format HHmmss"') do set _TIME=%%t
set LOG_FILE=%ROOT_DIR%\logs\pipeline_%_DATE%_%_TIME%.log
if not exist "%ROOT_DIR%\logs" mkdir "%ROOT_DIR%\logs"
:: Mode
set MODE=daily
@REM set MODE=full

:: Error flag
set ERRORS=0

goto :main

:: ============================================================
:: LOGGER
:: ============================================================

:log
echo %~1
echo %~1 >> "%LOG_FILE%"
exit /b 0

:: ============================================================
:: MAIN
:: ============================================================

:main

echo. >> "%LOG_FILE%"

call :log "============================================================"
call :log " Pipeline bat dau  %DATE% %TIME%"
call :log " Mode    : %MODE%"
call :log " DB      : LOCAL PostgreSQL"
call :log "============================================================"

:: ============================================================
:: STEP 1 - LINKEDIN SELENIUM
:: ============================================================

:: ============================================================
:: [1/5] Chay Selenium Crawlers
:: ============================================================

:: 1. Chạy LinkedIn Selenium
call :log "[1/5] Chay Selenium LinkedIn..."
"%PYTHON%" "%ROOT_DIR%\jobscrapers\spiders\linkedin_selenium.py" --mode=%MODE% >> "%LOG_FILE%" 2>&1

if !ERRORLEVEL! equ 0 (
    call :log "[1/5] OK - LinkedIn Selenium xong"
) else (
    call :log "[1/5] WARN - LinkedIn Selenium loi"
)

:: 2. Chạy ITViec Selenium (Thêm mới)
call :log "[1/5] Chay Selenium ITViec..."
"%PYTHON%" "%ROOT_DIR%\jobscrapers\spiders\itviec_selenium.py" --mode=%MODE% >> "%LOG_FILE%" 2>&1

if !ERRORLEVEL! equ 0 (
    call :log "[1/5] OK - ITViec Selenium xong"
) else (
    call :log "[1/5] WARN - ITViec Selenium loi"
)
:: ============================================================
:: STEP 2 - SCRAPY
:: ============================================================

call :log "[2/5] Chay Scrapy spiders..."

"%PYTHON%" "%ROOT_DIR%\run_spiders.py" %MODE% >> "%LOG_FILE%" 2>&1

if !ERRORLEVEL! equ 0 (
call :log "[2/5] OK - Scrapy spiders"
) else (
call :log "[2/5] WARN - Mot so spiders loi"
)

:: ============================================================
:: STEP 3 - AI PROCESSOR
:: ============================================================

call :log "[3/5] Chay AI processor..."

"%PYTHON%" "%ROOT_DIR%\jobscrapers\spiders\ai_processor.py" >> "%LOG_FILE%" 2>&1

if !ERRORLEVEL! equ 0 (
call :log "[3/5] OK - AI processor"
) else (
call :log "[3/5] WARN - AI processor loi"
)

:: ============================================================
:: STEP 4 - ETL TRANSFORM
:: ============================================================

call :log "[4/5] ETL transform..."

if /I "%MODE%"=="full" (


"%PYTHON%" "%ROOT_DIR%\jobscrapers\transform.py" ^
    --all >> "%LOG_FILE%" 2>&1


) else (


"%PYTHON%" "%ROOT_DIR%\jobscrapers\transform.py" ^
    >> "%LOG_FILE%" 2>&1

)

if !ERRORLEVEL! equ 0 (


call :log "[4/5] OK - ETL transform"


) else (


call :log "[4/5] FAIL - ETL loi"

set ERRORS=1


)

:: ============================================================
:: STEP 5 - DEDUP
:: ============================================================

if "!ERRORS!"=="1" (


call :log "[5/5] SKIP - ETL loi"

goto :summary


)

call :log "[5/5] Global Dedup..."

if /I "%MODE%"=="daily" (


"%PYTHON%" "%ROOT_DIR%\jobscrapers\dedup.py" ^
    --mode daily ^
    --run-id %_DATE% ^
    --days-lookback 30 ^
    >> "%LOG_FILE%" 2>&1


) else (


"%PYTHON%" "%ROOT_DIR%\jobscrapers\dedup.py" ^
    --mode full ^
    >> "%LOG_FILE%" 2>&1


)

if !ERRORLEVEL! equ 0 (

call :log "[5/5] OK - Dedup"


) else (


call :log "[5/5] FAIL - Dedup loi"

set ERRORS=1


)

:: ============================================================
:: SUMMARY
:: ============================================================

:summary

call :log "============================================================"

if "!ERRORS!"=="0" (


call :log " KET QUA: Pipeline thanh cong!"


) else (


call :log " KET QUA: Pipeline co loi!"
call :log " Xem log: %LOG_FILE%"


)

call :log " Ket thuc: %DATE% %TIME%"
call :log "============================================================"

exit /b %ERRORS%
