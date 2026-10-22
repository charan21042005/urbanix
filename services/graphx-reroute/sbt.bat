@echo off
rem Urbanix GraphX sbt launcher wrapper for Windows
setlocal

rem Check if sbt exists in LocalAppData
if exist "%LOCALAPPDATA%\Programs\sbt\bin\sbt.bat" (
    "%LOCALAPPDATA%\Programs\sbt\bin\sbt.bat" %*
    exit /b %ERRORLEVEL%
)

rem Fall back to sbt on PATH if not in current directory
sbt.exe %* 2>nul
if %ERRORLEVEL% equ 0 exit /b 0

echo [ERROR] sbt was not found in %LOCALAPPDATA%\Programs\sbt\bin\ or on PATH
exit /b 1
