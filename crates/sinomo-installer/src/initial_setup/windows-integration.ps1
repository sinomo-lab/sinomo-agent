param(
    [Parameter(Mandatory)][ValidateSet('install', 'uninstall')][string]$Action,
    [Parameter(Mandatory)][string]$InstallDir,
    [switch]$AddPath, [switch]$Menu, [switch]$Desktop, [switch]$Register
)
$ErrorActionPreference = 'Stop'
$receipt = Get-Content -LiteralPath (Join-Path $InstallDir 'sinomo-install.json') -Encoding UTF8 -Raw | ConvertFrom-Json
$BinDir = [IO.Path]::Combine([string]$receipt.data_dir, 'bin')
if ($BinDir.StartsWith('\\?\UNC\')) { $BinDir = '\\' + $BinDir.Substring(8) }
elseif ($BinDir.StartsWith('\\?\')) { $BinDir = $BinDir.Substring(4) }
$ledger = Join-Path $InstallDir 'windows-integration.json'
function Save-Ledger($State) {
    $temporary = $ledger + '.next'
    [IO.File]::WriteAllText($temporary, ($State | ConvertTo-Json -Depth 8), [Text.UTF8Encoding]::new($true))
    if ([IO.File]::Exists($ledger)) { [IO.File]::Replace($temporary, $ledger, [NullString]::Value) }
    else { [IO.File]::Move($temporary, $ledger) }
}
$shell = New-Object -ComObject WScript.Shell
if ($Action -eq 'install') {
    if ($Register) {
        $key = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\Sinomo'
        if (Test-Path $key) { throw 'Sinomo is already registered. Uninstall that copy before installing another.' }
        New-Item -Path $key -Force | Out-Null
        New-ItemProperty $key -Name DisplayName -Value 'Sinomo' -Force | Out-Null
        New-ItemProperty $key -Name DisplayVersion -Value $env:SINOMO_PRODUCT_VERSION -Force | Out-Null
        New-ItemProperty $key -Name Publisher -Value 'Sinomo' -Force | Out-Null
        New-ItemProperty $key -Name InstallLocation -Value $InstallDir -Force | Out-Null
        New-ItemProperty $key -Name UninstallString -Value ('powershell.exe -WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -File "' + (Join-Path $InstallDir 'uninstall.ps1') + '"') -Force | Out-Null
        New-ItemProperty $key -Name QuietUninstallString -Value ('powershell.exe -WindowStyle Hidden -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + (Join-Path $InstallDir 'uninstall.ps1') + '" -Quiet') -Force | Out-Null
        New-ItemProperty $key -Name NoModify -Value 1 -PropertyType DWord -Force | Out-Null
        New-ItemProperty $key -Name NoRepair -Value 1 -PropertyType DWord -Force | Out-Null
    }
    if ($AddPath -and $BinDir.Contains(';')) { throw 'PATH cannot contain a directory name with a semicolon. Disable PATH integration or choose another location.' }
    $state = @{ pathAdded = $false; shortcuts = @(); originalPath = $null; installedPath = $null }
    if (Test-Path -LiteralPath $ledger) { throw 'Shell integration already exists.' }
    $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    $entries = @($userPath -split ';')
    if ($AddPath -and -not ($entries | Where-Object { $_.TrimEnd('\') -ieq $BinDir.TrimEnd('\') })) {
        $state.pathAdded = $true
        $state.originalPath = $userPath
        $state.installedPath = if ([string]::IsNullOrEmpty($userPath)) { $BinDir } else { $userPath + ';' + $BinDir }
        Save-Ledger $state
        [Environment]::SetEnvironmentVariable('Path', $state.installedPath, 'User')
    }
    $links = @()
    if ($Menu) { $links += Join-Path ([Environment]::GetFolderPath('Programs')) 'Sinomo.lnk' }
    if ($Desktop) { $links += Join-Path ([Environment]::GetFolderPath('DesktopDirectory')) 'Sinomo.lnk' }
    foreach ($link in $links) {
        if (Test-Path -LiteralPath $link) { throw "Shortcut already exists: $link" }
        $state.shortcuts += $link
        Save-Ledger $state
        $shortcut = $shell.CreateShortcut($link)
        $shortcut.TargetPath = Join-Path $BinDir 'sinomo-app.exe'
        $shortcut.WorkingDirectory = $BinDir
        $shortcut.Save()
    }
    Save-Ledger $state
} elseif (Test-Path -LiteralPath $ledger) {
    $state = Get-Content -LiteralPath $ledger -Encoding UTF8 -Raw | ConvertFrom-Json
    if ($state.pathAdded) {
        $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
        if ($userPath -ceq $state.installedPath) {
            [Environment]::SetEnvironmentVariable('Path', $state.originalPath, 'User')
        } else {
            $entries = [System.Collections.Generic.List[string]]::new()
            $entries.AddRange([string[]]($userPath -split ';'))
            for ($index = 0; $index -lt $entries.Count; $index++) {
                if ($entries[$index].TrimEnd('\') -ieq $BinDir.TrimEnd('\')) {
                    $entries.RemoveAt($index)
                    break
                }
            }
            [Environment]::SetEnvironmentVariable('Path', ($entries -join ';'), 'User')
        }
    }
    foreach ($link in $state.shortcuts) {
        if (Test-Path -LiteralPath $link) {
            $shortcut = $shell.CreateShortcut($link)
            if ($shortcut.TargetPath -ieq (Join-Path $BinDir 'sinomo-app.exe')) {
                Remove-Item -LiteralPath $link
            }
        }
    }
    Remove-Item -LiteralPath $ledger
}
# Notify Explorer so newly launched terminals receive the changed user PATH.
Add-Type @'
using System;
using System.Runtime.InteropServices;
public static class SinomoEnvironment {
    [DllImport("user32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    public static extern IntPtr SendMessageTimeout(IntPtr hwnd, uint msg, UIntPtr wparam,
        string lparam, uint flags, uint timeout, out UIntPtr result);
}
'@
$broadcastResult = [UIntPtr]::Zero
[void][SinomoEnvironment]::SendMessageTimeout([IntPtr]0xffff, 0x1a, [UIntPtr]::Zero,
    'Environment', 2, 3000, [ref]$broadcastResult)
