@echo off
chcp 65001 > nul
setlocal EnableExtensions

set "SCRIPT_DIR=%~dp0"
set "ROOT_DIR=%SCRIPT_DIR%..\.."
set "PARENT_DIR=%ROOT_DIR%\.."
set "DOCS_DIR=%PARENT_DIR%\docs"
set "EXEC_DIR=%ROOT_DIR%\exec"
set "OUTPUT_DIR=%EXEC_DIR%\outputs"
set "PROMPT_FILE=%EXEC_DIR%\prompts\review.md"
set "REVIEW_INPUT=%OUTPUT_DIR%\review-input.md"
set "REVIEW_RESULT=%OUTPUT_DIR%\review-result.md"
set "DIFF_FILE=%OUTPUT_DIR%\work.diff"
set "TEST_RESULT_FILE=%OUTPUT_DIR%\test-result.txt"

set "TASK_FILE=%~1"
set "PLAN_FILE=%~2"

if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

if not exist "%PROMPT_FILE%" (
  echo review prompt not found: "%PROMPT_FILE%"
  exit /b 1
)

where git > nul 2>&1
if errorlevel 1 (
  echo git command not found.
  exit /b 1
)

if "%AI_REVIEW_CMD%"=="" (
  where claude > nul 2>&1
  if errorlevel 1 (
    echo claude command not found.
    echo Install Claude CLI or set AI_REVIEW_CMD to another review command.
    echo Example: set AI_REVIEW_CMD=codex exec
    exit /b 1
  )
  set "AI_REVIEW_CMD=claude -p"
)

if exist "%REVIEW_INPUT%" del "%REVIEW_INPUT%"
if exist "%REVIEW_RESULT%" del "%REVIEW_RESULT%"
if exist "%DIFF_FILE%" del "%DIFF_FILE%"
if exist "%TEST_RESULT_FILE%" del "%TEST_RESULT_FILE%"

git -C "%ROOT_DIR%" diff --cached --quiet
if errorlevel 1 (
  git -C "%ROOT_DIR%" diff --cached > "%DIFF_FILE%"
) else (
  git -C "%ROOT_DIR%" diff > "%DIFF_FILE%"
)

powershell -NoProfile -ExecutionPolicy Bypass -Command "$root=(Resolve-Path -LiteralPath '%ROOT_DIR%').Path; $out='%DIFF_FILE%'; git -C $root ls-files --others --exclude-standard | Where-Object { $_ -notlike 'exec/outputs/*' } | ForEach-Object { $path=$_; $full=Join-Path $root $path; if (Test-Path -LiteralPath $full -PathType Leaf) { Add-Content -LiteralPath $out -Encoding utf8 -Value ('diff --git a/' + $path + ' b/' + $path); Add-Content -LiteralPath $out -Encoding utf8 -Value 'new file mode 100644'; Add-Content -LiteralPath $out -Encoding utf8 -Value '--- /dev/null'; Add-Content -LiteralPath $out -Encoding utf8 -Value ('+++ b/' + $path); Get-Content -LiteralPath $full -Encoding utf8 | ForEach-Object { Add-Content -LiteralPath $out -Encoding utf8 -Value ('+' + $_) }; Add-Content -LiteralPath $out -Encoding utf8 -Value '' } }"

for %%A in ("%DIFF_FILE%") do if %%~zA==0 (
  echo No staged or working-tree diff found.
  exit /b 1
)

pushd "%ROOT_DIR%" > nul
if exist "gradlew.bat" (
  call gradlew.bat test > "%TEST_RESULT_FILE%" 2>&1
) else (
  echo gradlew.bat not found. Test execution skipped. > "%TEST_RESULT_FILE%"
)
popd > nul

call :append "# AI Code Review Request"
call :append ""
call :append "## Review Prompt"
type "%PROMPT_FILE%" >> "%REVIEW_INPUT%"
call :append ""
call :append "---"

call :append "## Project Architecture"
if exist "%ROOT_DIR%\docs\architecture.md" (
  type "%ROOT_DIR%\docs\architecture.md" >> "%REVIEW_INPUT%"
) else (
  call :append "docs/architecture.md not found."
)
call :append ""
call :append "---"

call :append "## Task"
call :appendDocsFile "%TASK_FILE%" "Task file was not provided or was not found."
call :append ""
call :append "---"

call :append "## Plan"
call :appendDocsFile "%PLAN_FILE%" "Plan file was not provided or was not found."
call :append ""
call :append "---"

call :append "## Changed Code Diff"
call :append "```diff"
type "%DIFF_FILE%" >> "%REVIEW_INPUT%"
call :append "```"
call :append ""
call :append "---"

call :append "## Test Result"
call :append "```"
type "%TEST_RESULT_FILE%" >> "%REVIEW_INPUT%"
call :append "```"
call :append ""
call :append "---"

call :append "## Required Output"
call :append "End the response with exactly one of these lines:"
call :append "REVIEW_STATUS: PASS"
call :append "REVIEW_STATUS: FAIL"

type "%REVIEW_INPUT%" | %AI_REVIEW_CMD% > "%REVIEW_RESULT%"
if errorlevel 1 (
  echo AI review command failed.
  exit /b 1
)

echo Review complete: "%REVIEW_RESULT%"
exit /b 0

:append
>> "%REVIEW_INPUT%" echo(%~1
exit /b 0

:appendDocsFile
if "%~1"=="" (
  call :append "%~2"
  exit /b 0
)
if exist "%DOCS_DIR%\%~1" (
  type "%DOCS_DIR%\%~1" >> "%REVIEW_INPUT%"
  exit /b 0
)
if exist "%PARENT_DIR%\%~1" (
  type "%PARENT_DIR%\%~1" >> "%REVIEW_INPUT%"
  exit /b 0
)
if exist "%~1" (
  type "%~1" >> "%REVIEW_INPUT%"
  exit /b 0
)
call :append "%~2"
exit /b 0
