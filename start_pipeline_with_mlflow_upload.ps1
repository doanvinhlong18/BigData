param(
    [string]$GitBash = "C:\Program Files\Git\bin\bash.exe"
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$bashScript = Join-Path $scriptDir "start_pipeline.sh"  # thay vì _with_mlflow_upload.sh

if (-not (Test-Path -LiteralPath $bashScript)) {
    throw "Cannot find $bashScript"
}

if (-not (Test-Path -LiteralPath $GitBash)) {
    $candidates = @(
        "C:\Program Files\Git\bin\bash.exe",
        "C:\Program Files\Git\usr\bin\bash.exe",
        "C:\msys64\usr\bin\bash.exe"
    )
    $GitBash = $candidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
}

if (-not $GitBash) {
    throw "Git Bash was not found. Install Git for Windows or pass -GitBash 'path\to\bash.exe'."
}

Write-Host "[INFO] Using Git Bash: $GitBash"
& $GitBash $bashScript
exit $LASTEXITCODE
