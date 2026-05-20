# 启动 sidecar（在 sidecar 目录执行，或任意路径调用本脚本）
$ErrorActionPreference = "Stop"
$SidecarDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $env:LOW_GI_PROJECT_ROOT) {
    $env:LOW_GI_PROJECT_ROOT = (Resolve-Path (Join-Path $SidecarDir "..\..\..")).Path
}
Set-Location $SidecarDir
Write-Host "[jd_semiauto] LOW_GI_PROJECT_ROOT=$($env:LOW_GI_PROJECT_ROOT)"
python __main__.py @args
