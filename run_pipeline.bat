@echo off
REM Batch wrapper for run_pipeline.ps1 (bypasses execution policy restrictions)
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_pipeline.ps1" %*
