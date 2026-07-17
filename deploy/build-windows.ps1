param(
    [Parameter(Mandatory = $false)]
    [string]$PythonPath,
    [Parameter(Mandatory = $false)]
    [string]$DistRoot
)

$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path

if ([string]::IsNullOrWhiteSpace($PythonPath)) {
    $PythonPath = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
}
if ([string]::IsNullOrWhiteSpace($DistRoot)) {
    $DistRoot = Join-Path $ProjectRoot "dist"
}

$PythonPath = [System.IO.Path]::GetFullPath($PythonPath)
$DistRoot = [System.IO.Path]::GetFullPath($DistRoot)
$ServerPath = Join-Path $ProjectRoot "server.py"
$ConfigPath = Join-Path $ProjectRoot "config.yaml"
$WorkRoot = Join-Path $ProjectRoot "build\pyinstaller"
$PackageRoot = Join-Path $DistRoot "QStockDataServer"

foreach ($Path in @($PythonPath, $ServerPath, $ConfigPath)) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Required file does not exist: $Path"
    }
}

& $PythonPath -m PyInstaller `
    --noconfirm `
    --clean `
    --onedir `
    --console `
    --noupx `
    --name QStockDataServer `
    --distpath $DistRoot `
    --workpath $WorkRoot `
    --specpath (Join-Path $ProjectRoot "build") `
    --hidden-import apscheduler.schedulers.background `
    --hidden-import apscheduler.triggers.cron `
    --hidden-import apscheduler.executors.pool `
    --hidden-import apscheduler.jobstores.memory `
    --copy-metadata APScheduler `
    $ServerPath

if ($LASTEXITCODE -ne 0) {
    throw "PyInstaller failed with exit code $LASTEXITCODE."
}

Copy-Item -LiteralPath $ConfigPath -Destination (Join-Path $PackageRoot "config.yaml") -Force
Copy-Item `
    -LiteralPath (Join-Path $PSScriptRoot "windows-autostart.ps1") `
    -Destination (Join-Path $PackageRoot "install-autostart.ps1") `
    -Force

foreach ($Directory in @("data", "logs", "runtime")) {
    New-Item -ItemType Directory -Path (Join-Path $PackageRoot $Directory) -Force | Out-Null
}

$ExecutablePath = Join-Path $PackageRoot "QStockDataServer.exe"
& $ExecutablePath --help | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Packaged executable smoke test failed with exit code $LASTEXITCODE."
}

Write-Host "Windows package created: $PackageRoot"
