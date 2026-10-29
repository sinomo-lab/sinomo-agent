param(
    [Parameter(Mandatory)][string]$Cli,
    [Parameter(Mandatory)][string]$SandboxRoot
)
$ErrorActionPreference = 'Stop'
$SandboxRoot = [IO.Path]::GetFullPath($SandboxRoot)
$application = Join-Path $SandboxRoot 'application with spaces'
$data = Join-Path $SandboxRoot 'data with spaces'
$key = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\Sinomo'
if (Test-Path $key) { throw 'Use a clean CI account; an existing installation must not be changed by this test.' }
if (Test-Path -LiteralPath $SandboxRoot) { throw 'Use a fresh CLI smoke-test directory.' }
New-Item -ItemType Directory -Path $SandboxRoot | Out-Null
$originalPath = [Environment]::GetEnvironmentVariable('Path', 'User')

function Invoke-Cli([string]$Executable, [string]$Arguments, [string]$Log) {
    $process = Start-Process -FilePath $Executable -ArgumentList $Arguments -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $SandboxRoot ($Log + '.out')) -RedirectStandardError (Join-Path $SandboxRoot ($Log + '.err'))
    $null = $process.Handle
    if (-not $process.WaitForExit(120000)) { throw "CLI timed out: $Arguments" }
    $process.Refresh()
    return $process.ExitCode
}

function Get-CliDigest([string]$File) {
    # Use .NET directly so the smoke test does not depend on module autoloading.
    $stream = [IO.File]::OpenRead($File)
    $sha = [Security.Cryptography.SHA256]::Create()
    try { return [BitConverter]::ToString($sha.ComputeHash($stream)) }
    finally { $stream.Dispose(); $sha.Dispose() }
}

function Test-ApplicationRemovalPending {
    try { return (Test-Path -LiteralPath $application -ErrorAction Stop) }
    catch {
        # Windows may expose a delete-pending image/directory as access denied briefly.
        if ($_.CategoryInfo.Category -eq 'PermissionDenied') { return $true }
        throw
    }
}

$installArguments = "install --headless --install-dir `"$application`" --data-dir `"$data`" --add-to-path"
if ((Invoke-Cli $Cli $installArguments 'install') -ne 0) { throw 'Initial CLI install failed.' }
if (-not (Test-Path $key)) { throw 'CLI install did not register an uninstaller.' }
$installedCli = Join-Path $application 'sinomo.exe'
$receipt = Get-Content -LiteralPath (Join-Path $application 'sinomo-install.json') -Raw | ConvertFrom-Json
$receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $SandboxRoot 'deployment.json') -Encoding utf8
if (-not $receipt.services) { throw 'CLI install did not register daemon services.' }
$serviceEntries = @($receipt.services.main_entry, $receipt.services.shadow_entry)
foreach ($entry in $serviceEntries) { if (-not (Test-Path -LiteralPath $entry)) { throw "Missing sign-in service entry: $entry" } }
if (Test-Path -LiteralPath (Join-Path $data 'source/sinomo')) { throw 'Initial setup unexpectedly imported source.' }
foreach ($role in @('daemon', 'daemon shadow')) {
    $log = $role.Replace(' ', '-') + '-status'
    if ((Invoke-Cli $installedCli "$role status" $log) -ne 0) { throw "$role status failed" }
    if ((Get-Content -LiteralPath (Join-Path $SandboxRoot ($log + '.out')) -Raw) -notmatch 'is running') { throw "$role is not answering RPC" }
}
$before = Get-CliDigest $installedCli
if ((Invoke-Cli $Cli $installArguments 'refuse-overwrite') -eq 0) { throw 'Initial install overwrote an existing installation.' }
if ((Get-CliDigest $installedCli) -ne $before) { throw 'An existing CLI was modified.' }
if ((Invoke-Cli $installedCli '--help' 'help') -ne 0) { throw 'Installed CLI did not launch.' }
if ((Invoke-Cli $installedCli 'uninstall' 'self-uninstall') -ne 0) { throw 'Installed CLI could not uninstall itself.' }
$deadline = (Get-Date).AddSeconds(30)
while ((Test-ApplicationRemovalPending) -and (Get-Date) -lt $deadline) { Start-Sleep -Milliseconds 200 }
if ((Test-Path $key) -or (Test-ApplicationRemovalPending)) { throw 'Self-uninstall left its registration or executable.' }
foreach ($entry in $serviceEntries) { if (Test-Path -LiteralPath $entry) { throw 'Self-uninstall left a daemon sign-in entry.' } }
if (-not (Test-Path -LiteralPath $data)) { throw 'Default uninstall removed data.' }
if ([Environment]::GetEnvironmentVariable('Path', 'User') -cne $originalPath) { throw 'User PATH was not restored exactly.' }
if ((Invoke-Cli $Cli $installArguments 'reinstall') -ne 0) { throw 'Reinstall with retained data failed.' }
if ((Invoke-Cli $Cli "uninstall --install-dir `"$application`" --delete-data" 'delete-data') -ne 0) { throw 'Explicit data deletion failed.' }
if ((Test-Path $key) -or (Test-Path -LiteralPath $data)) { throw 'Explicit uninstall left its registration or data.' }
if ([Environment]::GetEnvironmentVariable('Path', 'User') -cne $originalPath) { throw 'Final uninstall changed the original PATH.' }
# A CLI-created deployment is also removable by its registered graphical uninstaller.
if ((Invoke-Cli $Cli $installArguments 'cross-install') -ne 0) { throw 'Cross-install failed.' }
$uninstaller = Join-Path $application 'uninstall.ps1'
$process = Start-Process -FilePath powershell.exe -ArgumentList "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$uninstaller`" -Quiet -DeleteData" -WindowStyle Hidden -PassThru
$null = $process.Handle
if (-not $process.WaitForExit(120000)) { throw 'Registered uninstaller timed out.' }
$process.Refresh()
if ($process.ExitCode -ne 0 -or (Test-Path $key) -or (Test-Path -LiteralPath $data)) { throw 'Registered uninstaller could not remove a CLI deployment.' }
foreach ($entry in $serviceEntries) { if (Test-Path -LiteralPath $entry) { throw 'Registered uninstaller left a sign-in service.' } }
if ([Environment]::GetEnvironmentVariable('Path', 'User') -cne $originalPath) { throw 'Registered uninstaller changed PATH.' }
# Explicit staging still registers services, while --no-start suppresses launch.
if ((Invoke-Cli $Cli ($installArguments + ' --no-start') 'stage-only') -ne 0) { throw '--no-start staging failed.' }
if (Test-Path -LiteralPath (Join-Path $data 'daemon/status.sobj')) { throw '--no-start launched a daemon.' }
if ((Invoke-Cli $Cli "uninstall --install-dir `"$application`" --delete-data" 'remove-stage') -ne 0) { throw 'Staged deployment removal failed.' }
Write-Output 'PASS: headless CLI install, registered/running main and shadow services, custom paths, overwrite refusal, self/cross-uninstall, data retention/deletion and --no-start.'
