param(
    [Parameter(Mandatory)][ValidateSet('install','uninstall','commit','rollback')][string]$Action,
    [Parameter(Mandatory)][string]$InstallDir,
    [switch]$AddPath,[switch]$Menu,[switch]$Desktop,[switch]$Register
)
$ErrorActionPreference='Stop'
function Normal-Path([string]$Path) {
    if($Path.StartsWith('\\?\UNC\')){$Path='\\'+$Path.Substring(8)}
    elseif($Path.StartsWith('\\?\')){$Path=$Path.Substring(4)}
    [Environment]::ExpandEnvironmentVariables($Path).TrimEnd('\')
}
$InstallDir=Normal-Path $InstallDir
$receipt=Get-Content -LiteralPath (Join-Path $InstallDir 'sinomo-install.json') -Raw -Encoding UTF8|ConvertFrom-Json
$BinDir=Normal-Path ([IO.Path]::Combine([string]$receipt.data_dir,'bin'))
$ledger=Join-Path $InstallDir 'windows-integration.json'
$key='HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\Sinomo'
$menuLink=Join-Path ([Environment]::GetFolderPath('Programs')) 'Sinomo.lnk'
$desktopLink=Join-Path ([Environment]::GetFolderPath('DesktopDirectory')) 'Sinomo.lnk'
$shell=New-Object -ComObject WScript.Shell
$changed=$false
function Save-Ledger($State) {
    $text=$State|ConvertTo-Json -Depth 12
    if([IO.File]::Exists($ledger) -and [IO.File]::ReadAllText($ledger) -ceq $text){return}
    $temporary=$ledger+'.next'
    [IO.File]::WriteAllText($temporary,$text,[Text.UTF8Encoding]::new($true))
    if([IO.File]::Exists($ledger)){[IO.File]::Replace($temporary,$ledger,[NullString]::Value)}else{[IO.File]::Move($temporary,$ledger)}
}
function Own-Shortcut([string]$Path) {
    (Test-Path -LiteralPath $Path) -and (Normal-Path $shell.CreateShortcut($Path).TargetPath) -ieq (Join-Path $BinDir 'sinomo-app.exe')
}
function Set-UserPath([string]$Value) {
    if([Environment]::GetEnvironmentVariable('Path','User') -cne $Value){[Environment]::SetEnvironmentVariable('Path',$Value,'User');$script:changed=$true}
}
function Read-Registration {
    if( -not (Test-Path $key)){return $null}
    $entry=Get-ItemProperty $key
    if((Normal-Path ([string]$entry.InstallLocation)) -ine $InstallDir){throw 'Sinomo is registered to another installation directory.'}
    $values=@{}
    foreach($p in $entry.PSObject.Properties){if($p.Name -notlike 'PS*'){$values[$p.Name]=$p.Value}}
    return $values
}
function Restore-Before($Before) {
    Set-UserPath $Before.path
    foreach($item in $Before.shortcuts){
        if(Test-Path -LiteralPath $item.path){if( -not (Own-Shortcut $item.path)){throw "Shortcut changed during setup: $($item.path)"};Remove-Item -LiteralPath $item.path}
        if($null -ne $item.bytes){[IO.File]::WriteAllBytes($item.path,[Convert]::FromBase64String($item.bytes))}
    }
    if($null -eq $Before.registration){if(Test-Path $key){$null=Read-Registration;Remove-Item $key -Recurse}}
    else{New-Item $key -Force|Out-Null;foreach($p in $Before.registration.PSObject.Properties){New-ItemProperty $key -Name $p.Name -Value $p.Value -Force|Out-Null}}
    if($null -eq $Before.ledger){Remove-Item -LiteralPath $ledger -ErrorAction SilentlyContinue}
    else{[IO.File]::WriteAllText($ledger,$Before.ledger,[Text.UTF8Encoding]::new($true))}
}
$previousText=if([IO.File]::Exists($ledger)){[IO.File]::ReadAllText($ledger)}else{$null}
$state=if($previousText){$previousText|ConvertFrom-Json}else{$null}
if($Action -eq 'commit'){
    if($state -and $state.pendingBefore){$state.PSObject.Properties.Remove('pendingBefore');Save-Ledger $state}
    exit 0
}
if($Action -eq 'rollback'){
    if($state -and $state.pendingBefore){Restore-Before $state.pendingBefore}
    exit 0
}
if($state -and $state.pendingBefore){
    if($Action -eq 'uninstall'){
        Restore-Before $state.pendingBefore
        $state=if([IO.File]::Exists($ledger)){[IO.File]::ReadAllText($ledger)|ConvertFrom-Json}else{$null}
    }else{throw 'Interrupted shell configuration requires installer recovery.'}
}
if($Action -eq 'install'){
    # Preflight ownership and all optional integration collisions before effects.
    $registration=Read-Registration
    if($AddPath -and $BinDir.Contains(';')){throw 'PATH cannot contain a directory name with a semicolon.'}
    foreach($link in @($menuLink,$desktopLink)){
        if((($link -eq $menuLink -and $Menu) -or ($link -eq $desktopLink -and $Desktop)) -and (Test-Path -LiteralPath $link) -and  -not (Own-Shortcut $link)){throw "Shortcut belongs to another application: $link"}
    }
    $userPath=[Environment]::GetEnvironmentVariable('Path','User')
    $entries=@($userPath -split ';'|Where-Object {(Normal-Path $_) -ine $BinDir})
    if($AddPath){
        if(@($userPath -split ';'|Where-Object {(Normal-Path $_) -ieq $BinDir}).Count){$nextPath=$userPath}
        else{$nextPath=if([string]::IsNullOrEmpty($userPath)){$BinDir}else{$userPath+';'+$BinDir}}
    }else{$nextPath=$entries-join ';'}
    $shortcuts=@();if($Menu){$shortcuts+=$menuLink};if($Desktop){$shortcuts+=$desktopLink}
    $desired=@{pathAdded=[bool]$AddPath;originalPath=if($state){$state.originalPath}else{$userPath};installedPath=$nextPath;shortcuts=$shortcuts}
    $uninstall='powershell.exe -WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -File "'+(Join-Path $InstallDir 'uninstall.ps1')+'"'
    $modify=if([IO.File]::Exists((Join-Path $InstallDir 'sinomo-setup.exe'))){'"'+(Join-Path $InstallDir 'sinomo-setup.exe')+'"'}else{'"'+(Join-Path $BinDir 'sinomo.exe')+'" install'}
    $values=@{DisplayName='Sinomo';DisplayVersion=$env:SINOMO_PRODUCT_VERSION;Publisher='Sinomo';InstallLocation=$InstallDir;UninstallString=$uninstall;QuietUninstallString=$uninstall+' -Quiet';ModifyPath=$modify;NoModify=0;NoRepair=0}
    $matches=$state -and $state.pathAdded -eq [bool]$AddPath -and $userPath -ceq $nextPath
    foreach($link in @($menuLink,$desktopLink)){if((Own-Shortcut $link) -ne ($shortcuts -contains $link)){$matches=$false}}
    if($Register){if($null -eq $registration){$matches=$false}else{foreach($name in $values.Keys){if($registration[$name] -cne $values[$name]){$matches=$false}}}}
    if($matches){exit 0}
    $before=@{path=$userPath;ledger=$previousText;registration=$registration;shortcuts=@(foreach($link in @($menuLink,$desktopLink)){@{path=$link;bytes=if([IO.File]::Exists($link)){[Convert]::ToBase64String([IO.File]::ReadAllBytes($link))}else{$null}}})}
    $pending=$desired.Clone();$pending.pendingBefore=$before;Save-Ledger $pending
    try{
        if($Register){
            if( -not (Test-Path $key)){New-Item $key -Force|Out-Null}
            foreach($name in $values.Keys){$old=(Get-ItemProperty $key -Name $name -ErrorAction SilentlyContinue).$name;if($old -cne $values[$name]){New-ItemProperty $key -Name $name -Value $values[$name] -Force|Out-Null}}
        }
        Set-UserPath $nextPath
        foreach($link in @($menuLink,$desktopLink)){
            if($shortcuts -contains $link){
                if( -not (Test-Path -LiteralPath $link)){$shortcut=$shell.CreateShortcut($link);$shortcut.TargetPath=Join-Path $BinDir 'sinomo-app.exe';$shortcut.WorkingDirectory=$BinDir;$shortcut.Save()}
            }elseif(Own-Shortcut $link){Remove-Item -LiteralPath $link}
        }
        # The Rust transaction commits this ledger after runtime verification.
    }catch{Restore-Before ($before|ConvertTo-Json -Depth 12|ConvertFrom-Json);throw}
}else{
    $null=Read-Registration
    $userPath=[Environment]::GetEnvironmentVariable('Path','User')
    $entries=@($userPath -split ';'|Where-Object {(Normal-Path $_) -ine $BinDir})
    $nextPath=$entries -join ';'
    if($state -and $userPath -ceq $state.installedPath -and -not $state.originalPath){$nextPath=$state.originalPath}
    Set-UserPath $nextPath
    foreach($link in @($menuLink,$desktopLink)){if(Own-Shortcut $link){Remove-Item -LiteralPath $link}}
    if(Test-Path -LiteralPath $ledger){Remove-Item -LiteralPath $ledger}
}
if($changed){
    Add-Type @'
using System;
using System.Runtime.InteropServices;
public static class SinomoEnvironment {
 [DllImport("user32.dll",CharSet=CharSet.Unicode,SetLastError=true)]
 public static extern IntPtr SendMessageTimeout(IntPtr hwnd,uint msg,UIntPtr wparam,string lparam,uint flags,uint timeout,out UIntPtr result);
}
'@
    $result=[UIntPtr]::Zero
    [void][SinomoEnvironment]::SendMessageTimeout([IntPtr]0xffff,0x1a,[UIntPtr]::Zero,'Environment',2,3000,[ref]$result)
}
