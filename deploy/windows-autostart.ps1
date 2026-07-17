param(
    [Parameter(Mandatory = $false)]
    [string]$InstallRoot,
    [string]$TaskName = "QStockDataServer",
    [switch]$ValidateOnly
)

$ErrorActionPreference = "Stop"
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($InstallRoot)) {
    $InstallRoot = $ScriptRoot
}
$InstallRoot = (Resolve-Path -LiteralPath $InstallRoot).Path
$Executable = Join-Path $InstallRoot "QStockDataServer.exe"
$Config = Join-Path $InstallRoot "config.yaml"

foreach ($Path in @($Executable, $Config)) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Required file does not exist: $Path"
    }
}

if ($ValidateOnly) {
    Write-Host "Validation succeeded for install root '$InstallRoot'."
    return
}

$Action = New-ScheduledTaskAction `
    -Execute $Executable `
    -Argument "serve" `
    -WorkingDirectory $InstallRoot

$Trigger = New-ScheduledTaskTrigger -AtStartup

$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -MultipleInstances IgnoreNew

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -User "SYSTEM" `
    -RunLevel Highest `
    -Force | Out-Null

Start-ScheduledTask -TaskName $TaskName
Write-Host "Installed and started scheduled task '$TaskName'."
