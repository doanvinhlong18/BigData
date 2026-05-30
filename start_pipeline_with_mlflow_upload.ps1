param(
    [string]$GitBash = "C:\Program Files\Git\bin\bash.exe",
    [switch]$UploadOnly
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$bashScript = Join-Path $scriptDir "start_pipeline.sh"
$uploadScript = Join-Path $scriptDir "upload_model_to_mlflow.py"
$envFile = Join-Path $scriptDir ".env"

function Read-DotEnv {
    param([string]$Path)
    $result = @{}
    if (-not (Test-Path -LiteralPath $Path)) {
        return $result
    }
    foreach ($raw in Get-Content -LiteralPath $Path) {
        $line = $raw.Trim()
        if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) {
            continue
        }
        $key, $value = $line.Split("=", 2)
        $result[$key.Trim()] = $value.Trim().Trim('"').Trim("'")
    }
    return $result
}

function Get-ProjectPython {
    $venvPython = Join-Path $scriptDir ".venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $venvPython) {
        return $venvPython
    }
    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($python) {
        return $python.Source
    }
    throw "Python was not found. Create .venv or install Python."
}

function Invoke-ModelUpload {
    if (-not (Test-Path -LiteralPath $uploadScript)) {
        throw "Cannot find $uploadScript"
    }
    $python = Get-ProjectPython
    Write-Host "[INFO] Uploading model with: $python"
    Push-Location $scriptDir
    try {
        & $python $uploadScript
        if ($LASTEXITCODE -ne 0) {
            throw "upload_model_to_mlflow.py failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
}

$config = Read-DotEnv $envFile
$masterIp = $config["MASTER_IP"]
$localIps = @(Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue | Select-Object -ExpandProperty IPAddress)
$isMasterMachine = $masterIp -and ($localIps -contains $masterIp)

if ($UploadOnly -or -not $isMasterMachine) {
    if (-not $isMasterMachine) {
        Write-Host "[INFO] This machine is not MASTER_IP=$masterIp, running upload only."
    }
    Invoke-ModelUpload
    exit 0
}

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

Write-Host "[INFO] Running on master. Using Git Bash: $GitBash"
Push-Location $scriptDir
try {
    & $GitBash $bashScript
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
