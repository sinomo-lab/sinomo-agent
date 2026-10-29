param(
    [Parameter(Mandatory)][ValidateSet('install', 'uninstall')][string]$Action,
    [Parameter(Mandatory)][string]$InstallDir,
    [switch]$AddPath, [switch]$Menu, [switch]$Desktop, [switch]$Register
)
$ErrorActionPreference = 'Stop'
$ledger = Join-Path $InstallDir 'windows-integration.json'
$shell = New-Object -ComObject WScript.Shell
if ($Action -eq 'install') {
    if ($Register) {
        $key = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\Sinomo'
        if (Test-Path $key) { throw 'Sinomo is already registered. Uninstall that copy before installing another.' }
        New-Item -Path $key -Force | Out-Null
        New-ItemProperty $key -Name DisplayName -Value 'Sinomo' -Force | Out-Null
        New-ItemProperty $key -Name InstallLocation -Value $InstallDir -Force | Out-Null
        New-ItemProperty $key -Name UninstallString -Value ('powershell.exe -WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -File "' + (Join-Path $InstallDir 'uninstall.ps1') + '"') -Force | Out-Null
        New-ItemProperty $key -Name QuietUninstallString -Value ('powershell.exe -WindowStyle Hidden -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + (Join-Path $InstallDir 'uninstall.ps1') + '" -Quiet') -Force | Out-Null
        New-ItemProperty $key -Name NoModify -Value 1 -PropertyType DWord -Force | Out-Null
        New-ItemProperty $key -Name NoRepair -Value 1 -PropertyType DWord -Force | Out-Null
    }
    if ($AddPath -and $InstallDir.Contains(';')) { throw 'PATH cannot contain a directory name with a semicolon. Disable PATH integration or choose another location.' }
    $state = @{ pathAdded = $false; shortcuts = @(); originalPath = $null; installedPath = $null }
    if (Test-Path -LiteralPath $ledger) { throw 'Shell integration already exists.' }
    $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    $entries = @($userPath -split ';')
    if ($AddPath -and -not ($entries | Where-Object { $_.TrimEnd('\') -ieq $InstallDir.TrimEnd('\') })) {
        $state.pathAdded = $true
        $state.originalPath = $userPath
        $state.installedPath = if ([string]::IsNullOrEmpty($userPath)) { $InstallDir } else { $userPath + ';' + $InstallDir }
        $state | ConvertTo-Json | Set-Content -LiteralPath $ledger -Encoding utf8
        [Environment]::SetEnvironmentVariable('Path', $state.installedPath, 'User')
    }
    $links = @()
    if ($Menu) { $links += Join-Path ([Environment]::GetFolderPath('Programs')) 'Sinomo.lnk' }
    if ($Desktop) { $links += Join-Path ([Environment]::GetFolderPath('DesktopDirectory')) 'Sinomo.lnk' }
    foreach ($link in $links) {
        if (Test-Path -LiteralPath $link) { throw "Shortcut already exists: $link" }
        $state.shortcuts += $link
        $state | ConvertTo-Json | Set-Content -LiteralPath $ledger -Encoding utf8
        $shortcut = $shell.CreateShortcut($link)
        $shortcut.TargetPath = Join-Path $InstallDir 'sinomo-app.exe'
        $shortcut.WorkingDirectory = $InstallDir
        $shortcut.Save()
    }
    $state | ConvertTo-Json | Set-Content -LiteralPath $ledger -Encoding utf8
} elseif (Test-Path -LiteralPath $ledger) {
    $state = Get-Content -LiteralPath $ledger -Raw | ConvertFrom-Json
    if ($state.pathAdded) {
        $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
        if ($userPath -ceq $state.installedPath) {
            [Environment]::SetEnvironmentVariable('Path', $state.originalPath, 'User')
        } else {
            $entries = [System.Collections.Generic.List[string]]::new()
            $entries.AddRange([string[]]($userPath -split ';'))
            for ($index = 0; $index -lt $entries.Count; $index++) {
                if ($entries[$index].TrimEnd('\') -ieq $InstallDir.TrimEnd('\')) {
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
            if ($shortcut.TargetPath -ieq (Join-Path $InstallDir 'sinomo-app.exe')) {
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
