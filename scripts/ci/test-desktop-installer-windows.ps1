param(
    [Parameter(Mandatory)][string]$Installer,
    [Parameter(Mandatory)][string]$SandboxRoot,
    [switch]$NoStart
)
$ErrorActionPreference = 'Stop'
$env:SINOMO_DATA_DIR = $null
$env:SINOMO_INSTALL_DIR = $null
$SandboxRoot = [IO.Path]::GetFullPath($SandboxRoot)
$application = Join-Path $SandboxRoot 'application with spaces'
$data = Join-Path $SandboxRoot ('data ' + [char]0x00e9 + ' with spaces')
$key = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\Sinomo'
if (Test-Path $key) { throw 'Use a clean CI account: an existing Sinomo registration must not be changed by this test.' }
if (Test-Path -LiteralPath $SandboxRoot) { throw 'Use a fresh smoke-test directory.' }
$originalPath = [Environment]::GetEnvironmentVariable('Path', 'User')

function Get-FixtureHash([string]$Path) {
    $algorithm = [Security.Cryptography.SHA256]::Create()
    try {
        $stream = [IO.File]::OpenRead($Path)
        try { return [BitConverter]::ToString($algorithm.ComputeHash($stream)) }
        finally { $stream.Dispose() }
    } finally { $algorithm.Dispose() }
}

function Install-Fixture {
    Write-Output 'Installing graphical fixture.'
    $startOption = if ($NoStart) { '/NO_START' } else { '' }
    $process = Start-Process -FilePath $Installer -ArgumentList "/S /NO_MENU /ADD_PATH $startOption /DATA_DIR=`"$data`" /D=$application" -WindowStyle Hidden -PassThru
    $null = $process.Handle
    if (-not $process.WaitForExit(120000)) { throw 'Installer timed out; retained state for diagnosis.' }
    if ($process.ExitCode -ne 0) { throw "Installer failed: $($process.ExitCode)" }
    foreach ($name in @('sinomo.exe', 'sinomo-app.exe', 'sinomo-ui-extension-host.exe')) {
        if (-not (Test-Path -LiteralPath (Join-Path (Join-Path $data 'bin') $name))) { throw "Missing installed file: $name" }
    }
    if (Test-Path (Join-Path $application 'sinomo.exe')) { throw 'Graphical setup left a duplicate CLI.' }
    if (-not (Test-Path $key)) { throw 'Missing uninstall registration.' }
    $receipt = Get-Content -LiteralPath (Join-Path $application 'sinomo-install.json') -Encoding UTF8 -Raw | ConvertFrom-Json
    if ((Get-Content -LiteralPath (Join-Path $application 'sinomo-data-dir') -Encoding UTF8 -Raw) -ne $receipt.data_dir) {
        throw 'The CLI/app data directory binding does not match the installation receipt.'
    }
    $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    if (-not ($userPath -split ';' | Where-Object { $_ -ieq (Join-Path $data 'bin') })) { throw 'Optional PATH integration is missing.' }
    $cli = Start-Process -FilePath (Join-Path $data 'bin/sinomo.exe') -ArgumentList '--help' -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $SandboxRoot 'cli-help.txt') -RedirectStandardError (Join-Path $SandboxRoot 'cli-error.txt')
    # Cache the handle: Windows PowerShell can otherwise lose a fast child's exit code.
    $null = $cli.Handle
    if (-not $cli.WaitForExit(30000)) { throw 'The installed CLI timed out.' }
    $cli.Refresh()
    if ($cli.ExitCode -ne 0) { throw "The installed CLI did not start successfully: $($cli.ExitCode)" }
    if (-not $receipt.services) { throw 'Graphical installation did not register daemon services.' }
    $script:serviceEntries = @($receipt.services.main_entry, $receipt.services.shadow_entry)
    foreach ($entry in $script:serviceEntries) { if (-not (Test-Path -LiteralPath $entry)) { throw 'Missing graphical-install service entry.' } }
    if (-not $NoStart) {
        foreach ($role in @('daemon', 'daemon shadow')) {
            $log = Join-Path $SandboxRoot ($role.Replace(' ', '-') + '-status.txt')
            $status = Start-Process -FilePath (Join-Path $data 'bin/sinomo.exe') -ArgumentList "$role status" -WindowStyle Hidden -PassThru -RedirectStandardOutput $log
            $null = $status.Handle
            if (-not $status.WaitForExit(30000)) { throw 'Service status timed out.' }
            if ((Get-Content -LiteralPath $log -Raw) -notmatch 'is running') { throw "$role did not start." }
        }
    }
}

function Uninstall-Fixture([switch]$DeleteData) {
    Write-Output "Uninstalling graphical fixture (delete data: $DeleteData)."
    $arguments = if ($DeleteData) { '/S /DELETE_DATA' } else { '/S' }
    $script=Join-Path $application 'uninstall.ps1'
    $flags=if($DeleteData){'-Quiet -DeleteData'}else{'-Quiet'}
    $process = Start-Process -FilePath powershell.exe -ArgumentList "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$script`" $flags" -WindowStyle Hidden -PassThru
    if (-not $process.WaitForExit(120000)) { throw 'Uninstaller timed out; retained state for diagnosis.' }
    # NSIS may run a temporary copy after the original process exits.
    $deadline = (Get-Date).AddSeconds(30)
    while ((Test-Path $key) -and (Get-Date) -lt $deadline) { Start-Sleep -Milliseconds 200 }
    if (Test-Path $key) { throw 'Uninstaller registration remains; inspect the retained test installation.' }
    if (Test-Path -LiteralPath (Join-Path $application 'sinomo-app.exe')) { throw 'Uninstall left an application copy in the metadata folder.' }
    if (-not $DeleteData -and (-not (Test-Path (Join-Path $data 'bin/sinomo-app.exe')) -or -not (Test-Path (Join-Path $data 'config/uninstalled')))) { throw 'Uninstall did not retain and fence the deployment.' }
    foreach ($entry in $script:serviceEntries) { if (Test-Path -LiteralPath $entry) { throw 'Graphical uninstall left daemon startup integration.' } }
    if ([Environment]::GetEnvironmentVariable('Path', 'User') -cne $originalPath) { throw 'Uninstall did not restore the exact previous user PATH.' }
}

Install-Fixture
# Repeating the other frontend verifies the installation and preserves runtime versions.
$cli=Join-Path $data 'bin/sinomo.exe'
$before=(Get-FixtureHash $cli)
$process=Start-Process -FilePath $cli -ArgumentList 'install' -WindowStyle Hidden -PassThru
$null=$process.Handle
if (-not $process.WaitForExit(120000)) {throw 'Cross-frontend verification timed out.'}
$process.Refresh()
if ($process.ExitCode -ne 0 -or (Get-FixtureHash $cli) -ne $before) {throw 'CLI verification of GUI install changed the runtime or failed.'}
foreach ($role in @('daemon shadow','daemon')) {
    $process=Start-Process -FilePath $cli -ArgumentList "$role stop" -WindowStyle Hidden -PassThru
    $null=$process.Handle
    if (-not $process.WaitForExit(30000) -or $process.ExitCode -ne 0) {throw 'Cannot stop fixture before damaging its binaries.'}
}
$sentinel=Join-Path $data 'repair-retained-data'
[IO.File]::WriteAllText($sentinel,'application data survives repair')
[IO.File]::WriteAllBytes($cli,[Text.Encoding]::UTF8.GetBytes('damaged CLI'))
[IO.File]::WriteAllBytes((Join-Path $data 'bin/sinomo-shadow.exe'),[Text.Encoding]::UTF8.GetBytes('damaged shadow'))
$process=Start-Process -FilePath $Installer -ArgumentList "/S /REPAIR /NO_START /DATA_DIR=`"$data`" /D=$application" -WindowStyle Hidden -PassThru
$null=$process.Handle
if (-not $process.WaitForExit(120000) -or $process.ExitCode -ne 0) {throw 'Packaged graphical repair failed.'}
if ((Get-FixtureHash $cli) -ne $before -or [IO.File]::ReadAllText($sentinel) -cne 'application data survives repair') {throw 'Graphical repair changed data or failed to restore the runtime.'}
Install-Fixture
Uninstall-Fixture
if (-not (Test-Path -LiteralPath $data)) { throw 'Default uninstall deleted data.' }
# Reinstall using the retained cluster release; then exercise explicit permanent removal.
Install-Fixture
Uninstall-Fixture -DeleteData
if (Test-Path -LiteralPath $data) { throw 'Explicit data removal did not remove the selected data directory.' }
# The CLI must also remove a deployment made by the graphical installer.
Install-Fixture
$process = Start-Process -FilePath (Join-Path $data 'bin/sinomo.exe') -ArgumentList 'uninstall --delete-data' -WindowStyle Hidden -PassThru
$null = $process.Handle
if (-not $process.WaitForExit(120000)) { throw 'CLI cross-uninstall timed out.' }
$process.Refresh()
if ($process.ExitCode -ne 0 -or (Test-Path $key) -or (Test-Path -LiteralPath $data)) { throw 'CLI could not remove a graphical deployment.' }
foreach ($entry in $script:serviceEntries) { if (Test-Path -LiteralPath $entry) { throw 'CLI cross-uninstall left startup integration.' } }
if ([Environment]::GetEnvironmentVariable('Path', 'User') -cne $originalPath) { throw 'CLI cross-uninstall changed original PATH.' }
Write-Output 'PASS: graphical/CLI deployment parity, registered/running services, both uninstall entrypoints, custom paths, PATH, retained/deleted data and reinstall.'
