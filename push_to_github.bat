@echo off
title Push FinSight to GitHub
cls
echo ======================================================================
echo          FinSight - Upload Project to GitHub (Yaseenshaik17)
echo ======================================================================
echo.
echo Before running this, please create an empty repository on GitHub:
echo 1. Open: https://github.com/new
echo 2. Repository name: FinSight-BigData-Analytics
echo 3. Keep it Public and leave "Initialize with README" UNCHECKED.
echo.
set /p REPO_NAME="Enter GitHub repo name (Press ENTER for 'FinSight-BigData-Analytics'): "
if "%REPO_NAME%"=="" set REPO_NAME=FinSight-BigData-Analytics

git remote remove origin 2>nul
git remote add origin https://github.com/Yaseenshaik17/%REPO_NAME%.git
git branch -M main

echo.
echo ----------------------------------------------------------------------
echo Pushing project to https://github.com/Yaseenshaik17/%REPO_NAME%.git ...
echo ----------------------------------------------------------------------
git push -u origin main

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ==================================================================
    echo [SUCCESS] Project uploaded successfully to GitHub!
    echo URL: https://github.com/Yaseenshaik17/%REPO_NAME%
    echo ==================================================================
) else (
    echo.
    echo ==================================================================
    echo [NOTE] If prompted for password, use your GitHub Personal Access Token (PAT).
    echo Generate a token at: https://github.com/settings/tokens
    echo ==================================================================
)

echo.
pause
