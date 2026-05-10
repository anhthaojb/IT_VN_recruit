@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul

:: ============================
::  KHOI DONG DOCKER + TYPESENSE
:: ============================

echo [Docker] Kiem tra Docker Desktop...
docker info >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [Docker] Dang khoi dong Docker Desktop...
    start "" "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    echo [Docker] Cho Docker khoi dong 30 giay...
    timeout /t 30 /nobreak >nul
)

echo [Typesense] Khoi dong container...
docker start typesense >nul 2>&1
timeout /t 5 /nobreak >nul

curl -s http://localhost:8108/health | findstr "ok" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [Typesense] WARN - Typesense chua san sang, cho them...
    timeout /t 10 /nobreak >nul
)
echo [Typesense] San sang!

:: ============================
::  CONFIG
:: ============================

set MODE=daily
@REM set MODE=full

set PYTHON=D:\ITTA\venv\Scripts\python.exe
set SCRAPY_DIR=%~dp0
set SPIDER_DIR=%SCRAPY_DIR%jobscrapers\spiders\
set PYTHONPATH=%SCRAPY_DIR%jobscrapers\spiders;%SCRAPY_DIR%jobscrapers;%SCRAPY_DIR%;%PYTHONPATH%
set PYTHONIOENCODING=utf-8
set PATH=%PATH%;C:\Program Files\MySQL\MySQL Server 9.4\bin

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
::  PHAN 0 - TYPESENSE REINDEX
::  Chi chay khi MODE=full
::  Daily: bo qua, ETL tu match
:: ============================

if /I "%MODE%"=="full" (
    echo [Typesense] MODE=full - Bat dau reindex toan bo cong ty... >> "%LOG_FILE%"
    "%PYTHON%" "%SCRAPY_DIR%jobscrapers\typesence.py" >> "%LOG_FILE%" 2>&1
    if %ERRORLEVEL%==0 (
        echo [Typesense] OK - Reindex hoan thanh >> "%LOG_FILE%"
    ) else (
        echo [Typesense] FAIL - Reindex loi, tiep tuc pipeline >> "%LOG_FILE%"
    )
) else (
    echo [Typesense] MODE=daily - Bo qua reindex, ETL se tu match cong ty moi >> "%LOG_FILE%"
)

:: ============================
::  PHAN 1 - SELENIUM SCRIPTS
::  FAIL khong block ETL
:: ============================

call :run_selenium linkedin_selenium.py
call :run_selenium itviec_selenium.py

:: ============================
::  PHAN 2 - SCRAPY
:: ============================

echo [Scrapy] Bat dau chay tat ca spider song song... >> "%LOG_FILE%"

"%PYTHON%" "%SCRAPY_DIR%run_spiders.py" %MODE% >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL%==0 (
    echo [Scrapy] OK - Tat ca spider hoan thanh >> "%LOG_FILE%"
) else (
    echo [Scrapy] FAIL (exit=%ERRORLEVEL%) - Tiep tuc ETL >> "%LOG_FILE%"
)

:: ============================
::  PHAN 3 - ETL
::  Luon chay du Scrapy co loi
:: ============================

echo [ETL] Bat dau xu ly du lieu... >> "%LOG_FILE%"

if /I "%MODE%"=="full" (
    "%PYTHON%" "%SCRAPY_DIR%jobscrapers\transform.py" --all >> "%LOG_FILE%" 2>&1
) else (
    "%PYTHON%" "%SCRAPY_DIR%jobscrapers\transform.py" >> "%LOG_FILE%" 2>&1
)

if %ERRORLEVEL%==0 (
    echo [ETL] OK - ETL hoan thanh >> "%LOG_FILE%"
) else (
    echo [ETL] FAIL (exit=%ERRORLEVEL%) >> "%LOG_FILE%"
    echo 1 > "%ERROR_FILE%"
)

:: ============================
::  PHAN 4 - LOAD DW
:: ============================

set /p CURRENT_ERR=<"%ERROR_FILE%"
if "%CURRENT_ERR%"=="1" (
    echo [DW] SKIP - ETL co loi >> "%LOG_FILE%"
    goto :summary
)

echo [DW] Bat dau nap Data Warehouse... >> "%LOG_FILE%"

set MYSQL_HOST=localhost
set MYSQL_USER=root
set MYSQL_PASS=123456
set MYSQL_DB=itta

if /I "%MODE%"=="daily" (set SP_MODE=today) else (set SP_MODE=all)

mysql -h %MYSQL_HOST% -u %MYSQL_USER% -p%MYSQL_PASS% %MYSQL_DB% ^
    -e "SET GLOBAL innodb_buffer_pool_size=536870912;" >> "%LOG_FILE%" 2>&1

mysql -h %MYSQL_HOST% -u %MYSQL_USER% -p%MYSQL_PASS% %MYSQL_DB% ^
    -e "CALL sp_ETL_Load_DW('%SP_MODE%', NULL);" >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL%==0 (
    echo [DW] OK - Load DW hoan thanh >> "%LOG_FILE%"
) else (
    echo [DW] FAIL (exit=%ERRORLEVEL%) >> "%LOG_FILE%"
    echo 1 > "%ERROR_FILE%"
)

:: ============================
::  KET QUA TONG KET
:: ============================

:summary
set /p ERRORS=<"%ERROR_FILE%"
del "%ERROR_FILE%" 2>nul

echo ============================================================ >> "%LOG_FILE%"
if "%ERRORS%"=="0" (
    echo  KET QUA: Tat ca hoan thanh thanh cong! >> "%LOG_FILE%"
) else (
    echo  KET QUA: Co loi - xem: %LOG_FILE% >> "%LOG_FILE%"
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
    echo   [Selenium] FAIL  %SCRIPT% (exit=%ERRORLEVEL%) - Bo qua, tiep tuc >> "%LOG_FILE%"
)
exit /b 0