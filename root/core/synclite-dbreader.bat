@echo off

set "JVM_ARGS="
if exist "%~dp0\synclite-dbreader-variables.bat" (
  call "%~dp0\synclite-dbreader-variables.bat"
)

if defined JAVA_HOME (
  if exist "%JAVA_HOME%\bin\java.exe" (
     set "JAVA_CMD=%JAVA_HOME%\bin\java
  ) else (
     set "JAVA_CMD=java"
  )
) else (
  set "JAVA_CMD=java"
)

"%JAVA_CMD%" %JVM_ARGS% -classpath "%~dp0\synclite-dbreader.jar;%~dp0\*" com.synclite.dbreader.Main %1 %2 %3 %4 %5 %6 %7
