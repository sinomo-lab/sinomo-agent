param([switch]$KeepData, [switch]$Quiet, [switch]$DeleteData)
$ErrorActionPreference = 'Stop'
$install = Split-Path -Parent $PSCommandPath
$receipt = Get-Content -LiteralPath (Join-Path $install 'sinomo-install.json') -Raw | ConvertFrom-Json
if ($KeepData -and $DeleteData) { throw 'Choose KeepData or DeleteData.' }
$removeData = [bool]$DeleteData
if (-not $Quiet) {
    Add-Type -AssemblyName System.Windows.Forms
    $choice = [System.Windows.Forms.MessageBox]::Show("Uninstall Sinomo from $install?", 'Uninstall Sinomo', 'YesNo', 'Question', 'Button2')
    if ($choice -ne 'Yes') { exit 0 }
    if (-not $KeepData -and -not $DeleteData -and $receipt.owns_data) {
        $choice = [System.Windows.Forms.MessageBox]::Show("Permanently delete all Sinomo data in $($receipt.data_dir)? Choose No to keep it.", 'Sinomo data', 'YesNo', 'Warning', 'Button2')
        $removeData = $choice -eq 'Yes'
    }
}
# Execute an owned temporary copy so Windows can remove the installed CLI.
$temporary = Join-Path ([IO.Path]::GetTempPath()) ('sinomo-uninstall-' + [guid]::NewGuid() + '.exe')
try {
    Copy-Item -LiteralPath (Join-Path $install 'sinomo.exe') -Destination $temporary
    $arguments = @('uninstall', '--install-dir', $install)
    if ($removeData) { $arguments += '--delete-data' }
    & $temporary @arguments
    if ($LASTEXITCODE -ne 0) { throw "Uninstall failed ($LASTEXITCODE)." }
} catch {
    if ($Quiet) { Write-Error $_.Exception.Message }
    else { [void][System.Windows.Forms.MessageBox]::Show($_.Exception.Message, 'Sinomo uninstall', 'OK', 'Error') }
    exit 1
} finally {
    Remove-Item -LiteralPath $temporary -ErrorAction SilentlyContinue
}
