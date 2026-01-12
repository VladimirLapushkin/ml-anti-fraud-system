@echo off
setlocal EnableDelayedExpansion

REM path to  .env
set ENV_FILE=.env

REM read str from  .env
for /f "usebackq tokens=1,2 delims==" %%A in ("%ENV_FILE%") do (
    if "%%A"=="S3_ENDPOINT_URL" set S3_ENDPOINT_URL=%%B
    if "%%A"=="S3_BUCKET_NAME"   set S3_BUCKET_NAME=%%B
    if "%%A"=="S3_ACCESS_KEY"    set S3_ACCESS_KEY=%%B
    if "%%A"=="S3_SECRET_KEY"    set S3_SECRET_KEY=%%B
)

REM Deleted spaces
for %%V in (S3_ENDPOINT_URL S3_BUCKET_NAME S3_ACCESS_KEY S3_SECRET_KEY) do (
    for /f "tokens=* delims= " %%X in ("!%%V!") do set %%V=%%X
)

REM Create s3cmd.ini
(
    echo [default]
    echo access_key = !S3_ACCESS_KEY!
    echo secret_key = !S3_SECRET_KEY!
    echo.
    echo bucket_location = ru-central1
    echo host_base = storage.yandexcloud.net
    echo host_bucket = %%(bucket^)s.storage.yandexcloud.net
    echo.
    echo use_https = True
    echo signature_v2 = False
) > s3cmd.ini

xcopy s3cmd.ini "%USERPROFILE%\appdata\roaming\" /Y


echo s3cmd.ini created.
endlocal