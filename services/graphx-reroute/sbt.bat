@echo off
rem Urbanix GraphX sbt launcher wrapper for Windows
setlocal

rem Check if sbt is already on PATH
where sbt >nul 2>nul
if %ERRORLEVEL% equ 0 (
    sbt %*
    exit /b %ERRORLEVEL%
)

rem Fall back to user-installed sbt in LocalAppData
if exist "%LOCALAPPDATA%\Programs\sbt\bin\sbt.bat" (
    "%LOCALAPPDATA%\Programs\sbt\bin\sbt.bat" %*
    exit /b %ERRORLEVEL%
)

echo [ERROR] sbt was not found on PATH or in %LOCALAPPDATA%\Programs\sbt\bin\
exit /b 1
