$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$backendScript = Join-Path $root "backend\run_backend.ps1"
$frontendScript = Join-Path $root "frontend\run_frontend.ps1"

if (!(Test-Path $backendScript)) {
    throw "Missing backend launcher: $backendScript"
}

if (!(Test-Path $frontendScript)) {
    throw "Missing frontend launcher: $frontendScript"
}

Write-Host "Starting backend in a new terminal..."
Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", "Set-Location '$root\backend'; .\run_backend.ps1"

Start-Sleep -Seconds 2

Write-Host "Starting frontend in a new terminal..."
Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", "Set-Location '$root\frontend'; .\run_frontend.ps1"

Write-Host ""
Write-Host "Services starting:"
Write-Host "Backend:  http://127.0.0.1:8000"
Write-Host "Frontend: http://127.0.0.1:3000"
