param(
    [string]$Model = 'gpt-6.1-sol',
    [ValidateSet('responses','realtime')][string]$AgentRuntime = 'responses',
    [string]$Case,
    [string]$Binary,
    [string]$AppUseModule,
    [string]$Credentials,
    [string]$RunDirectory,
    [ValidateRange(128,4096)][int]$CaptureMaxDimension = 1440,
    [switch]$CoordinateGrid,
    [switch]$BackgroundInterruption,
    [int]$TimeoutSeconds = 900,
    [switch]$FixtureOnly,
    [switch]$UseInteractiveDesktop,
    [switch]$Watchdog
)
$ErrorActionPreference = 'Stop'
Add-Type -TypeDefinition @'
using System;
using System.Text;
using System.Runtime.InteropServices;
using System.ComponentModel;
public static class DesktopHarness {
  [StructLayout(LayoutKind.Sequential)] public struct POINT { public int X,Y; }
  [StructLayout(LayoutKind.Sequential)] struct SECURITY_ATTRIBUTES { public int Length; public IntPtr Descriptor; public int Inherit; }
  [StructLayout(LayoutKind.Sequential,CharSet=CharSet.Unicode)] struct STARTUPINFO {
    public int cb; public string reserved,desktop,title; public uint x,y,width,height,xchars,ychars,fill,flags;
    public ushort show,reserved2; public IntPtr data,stdin,stdout,stderr;
  }
  [StructLayout(LayoutKind.Sequential)] struct PROCESS_INFORMATION { public IntPtr Process,Thread; public uint Id,ThreadId; }
  [DllImport("user32.dll",CharSet=CharSet.Unicode,SetLastError=true)] public static extern IntPtr CreateDesktopW(string name,IntPtr device,IntPtr mode,uint flags,uint access,IntPtr security);
  [DllImport("user32.dll",CharSet=CharSet.Unicode,SetLastError=true)] public static extern IntPtr OpenDesktopW(string name,uint flags,bool inherit,uint access);
  [DllImport("user32.dll",SetLastError=true)] public static extern IntPtr OpenInputDesktop(uint flags,bool inherit,uint access);
  [DllImport("user32.dll",SetLastError=true)] public static extern bool SwitchDesktop(IntPtr desktop);
  [DllImport("user32.dll")] public static extern bool CloseDesktop(IntPtr desktop);
  [DllImport("user32.dll",CharSet=CharSet.Unicode,SetLastError=true)] static extern bool GetUserObjectInformationW(IntPtr handle,int index,StringBuilder value,uint size,out uint needed);
  [DllImport("user32.dll")] public static extern bool GetCursorPos(out POINT point);
  [DllImport("user32.dll")] public static extern bool SetCursorPos(int x,int y);
  [DllImport("user32.dll")] public static extern IntPtr GetForegroundWindow();
  [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr window);
  [DllImport("user32.dll")] public static extern bool IsWindow(IntPtr window);
  [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)] static extern IntPtr CreateFileW(string path,uint access,uint share,ref SECURITY_ATTRIBUTES security,uint creation,uint flags,IntPtr template);
  [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)] static extern bool CreateProcessW(string app,StringBuilder args,IntPtr processSecurity,IntPtr threadSecurity,bool inherit,uint flags,IntPtr environment,string directory,ref STARTUPINFO startup,out PROCESS_INFORMATION info);
  [DllImport("kernel32.dll")] public static extern bool CloseHandle(IntPtr handle);
  [DllImport("kernel32.dll")] public static extern uint WaitForSingleObject(IntPtr handle,uint timeout);
  [DllImport("kernel32.dll")] public static extern bool GetExitCodeProcess(IntPtr handle,out uint code);
  [DllImport("kernel32.dll")] public static extern bool TerminateProcess(IntPtr handle,uint code);
  public static string Name(IntPtr desktop) {
    var value=new StringBuilder(256); uint needed;
    if(!GetUserObjectInformationW(desktop,2,value,512,out needed)) throw new Win32Exception();
    return value.ToString();
  }
  public static string Quote(string value) {
    var result=new StringBuilder("\""); int slashes=0;
    foreach(char c in value) {
      if(c=='\\') { slashes++; continue; }
      if(c=='"') { result.Append('\\',slashes*2+1); result.Append(c); slashes=0; continue; }
      result.Append('\\',slashes); slashes=0; result.Append(c);
    }
    result.Append('\\',slashes*2); result.Append('"'); return result.ToString();
  }
  public static IntPtr Start(string binary,string[] args,string desktop,string cwd,string output,string error,out uint pid) {
    var sa=new SECURITY_ATTRIBUTES {Length=Marshal.SizeOf<SECURITY_ATTRIBUTES>(),Inherit=1};
    IntPtr stdout=CreateFileW(output,0x40000000,3,ref sa,2,0,IntPtr.Zero);
    IntPtr stderr=CreateFileW(error,0x40000000,3,ref sa,2,0,IntPtr.Zero);
    IntPtr stdin=CreateFileW("NUL",0x80000000,3,ref sa,3,0,IntPtr.Zero);
    if(stdout==new IntPtr(-1)||stderr==new IntPtr(-1)||stdin==new IntPtr(-1)) throw new Win32Exception();
    try {
      var cmd=new StringBuilder(Quote(binary)); foreach(var arg in args) cmd.Append(" ").Append(Quote(arg));
      var si=new STARTUPINFO {cb=Marshal.SizeOf<STARTUPINFO>(),desktop="WinSta0\\"+desktop,flags=0x100,stdin=stdin,stdout=stdout,stderr=stderr};
      PROCESS_INFORMATION pi;
      if(!CreateProcessW(binary,cmd,IntPtr.Zero,IntPtr.Zero,true,0x08000000|0x400,IntPtr.Zero,cwd,ref si,out pi)) throw new Win32Exception();
      CloseHandle(pi.Thread); pid=pi.Id; return pi.Process;
    } finally {CloseHandle(stdin);CloseHandle(stdout);CloseHandle(stderr);}
  }
}
'@

function Stop-OwnedChild($owner) {
    $child = Get-Process -Id $owner.child_pid -ErrorAction SilentlyContinue
    if ($child -and $child.StartTime.ToUniversalTime().Ticks -eq $owner.child_start_ticks -and
        [string]::Equals($child.Path, $owner.binary, [StringComparison]::OrdinalIgnoreCase)) {
        $child.Kill($true)
        $child.WaitForExit(10000) | Out-Null
    }
}

if ($Watchdog) {
    $owner = Get-Content -LiteralPath (Join-Path $RunDirectory 'owner.json') -Raw | ConvertFrom-Json
    while (!(Test-Path -LiteralPath (Join-Path $RunDirectory 'watchdog.stop'))) {
        $parent = Get-Process -Id $owner.parent_pid -ErrorAction SilentlyContinue
        $parentMatches = $parent -and $parent.StartTime.ToUniversalTime().Ticks -eq $owner.parent_start_ticks
        if (!$parentMatches -or [DateTimeOffset]::UtcNow -gt [DateTimeOffset]::Parse($owner.deadline_utc)) {
            Stop-OwnedChild $owner
            $restore = [DesktopHarness]::OpenDesktopW($owner.original_desktop,0,$false,0x101)
            if ($restore -ne [IntPtr]::Zero) {
                if ($owner.switch_desktop) { [DesktopHarness]::SwitchDesktop($restore) | Out-Null }
                if ($owner.restore_input) {
                    [DesktopHarness]::SetCursorPos($owner.pointer_x,$owner.pointer_y) | Out-Null
                    if ([DesktopHarness]::IsWindow([IntPtr]$owner.foreground)) { [DesktopHarness]::SetForegroundWindow([IntPtr]$owner.foreground) | Out-Null }
                }
                [DesktopHarness]::CloseDesktop($restore) | Out-Null
            }
            [IO.File]::WriteAllText((Join-Path $RunDirectory 'watchdog-recovery.txt'),'The launcher ended or exceeded its deadline. Its exact child was stopped and the original desktop restored.')
            exit 1
        }
        Start-Sleep -Milliseconds 250
    }
    exit 0
}

if ($BackgroundInterruption -and $Case -ne 'agent.desktop.app-multiwindow') { throw 'BackgroundInterruption requires agent.desktop.app-multiwindow.' }
# Shared-desktop background tests must not undo another application's newer
# foreground/pointer state. Raw tests and switched desktops still own restoration.
$restoreInput = !$UseInteractiveDesktop -or !$Case -or !$Case.StartsWith('agent.desktop.app-')
$repo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if (!$Binary) { $Binary = Join-Path $repo 'target/debug/sinomo.exe' }
$Binary = (Resolve-Path -LiteralPath $Binary).Path
if (!$AppUseModule) { $AppUseModule = Join-Path (Split-Path -Parent $Binary) 'sinomo_app_use_module.dll' }
$AppUseModule = (Resolve-Path -LiteralPath $AppUseModule).Path
if (!$Credentials) { $Credentials = Join-Path $env:USERPROFILE '.sinomo/configs/lm/credentials/codex.json' }
$Credentials = (Resolve-Path -LiteralPath $Credentials).Path
$ffmpeg = (Get-Command ffmpeg -ErrorAction Stop).Source
$ffprobe = (Get-Command ffprobe -ErrorAction Stop).Source
if (!$RunDirectory) { $RunDirectory = Join-Path $repo ('benchmark-results/desktop-agent-' + [DateTime]::UtcNow.ToString('yyyyMMdd-HHmmss') + '-' + [Guid]::NewGuid().ToString('N').Substring(0,8)) }
$RunDirectory = [IO.Path]::GetFullPath($RunDirectory)
if (Test-Path -LiteralPath $RunDirectory) { throw 'Use a fresh run directory; previous evidence is never overwritten.' }
New-Item -ItemType Directory -Path $RunDirectory | Out-Null
$desktopName = 'SinomoE2E-' + [Guid]::NewGuid().ToString('N')
$original = [DesktopHarness]::OpenInputDesktop(0,$false,0x101)
if ($original -eq [IntPtr]::Zero) { throw 'Cannot retain the current input desktop.' }
$foreground = [DesktopHarness]::GetForegroundWindow()
if ($UseInteractiveDesktop) { $desktopName = [DesktopHarness]::Name($original) }
$pointer = [DesktopHarness+POINT]::new()
[DesktopHarness]::GetCursorPos([ref]$pointer) | Out-Null
$testDesktop = [IntPtr]::Zero
$childHandle = [IntPtr]::Zero
$oldData = $env:SINOMO_DATA_DIR
$oldCredentials = $env:SINOMO_CODEX_CREDENTIALS_FILE
$exitCode = 1
try {
    $testDesktop = if ($UseInteractiveDesktop) { [DesktopHarness]::OpenDesktopW($desktopName,0,$false,0x101) } else { [DesktopHarness]::CreateDesktopW($desktopName,[IntPtr]::Zero,[IntPtr]::Zero,0,0xF01FF,[IntPtr]::Zero) }
    if ($testDesktop -eq [IntPtr]::Zero) { throw 'Cannot create the isolated desktop.' }
    $env:SINOMO_DATA_DIR = Join-Path $RunDirectory 'bootstrap'
    $env:SINOMO_CODEX_CREDENTIALS_FILE = $Credentials
    New-Item -ItemType Directory -Path $env:SINOMO_DATA_DIR | Out-Null
    $arguments = @('test','run')
    if ($Case) { $arguments += $Case } else { $arguments += @('--tag','agent-desktop') }
    $arguments += @('--provider','codex','--model',$Model,'--live','--json',
        '--param',('agent-runtime='+$AgentRuntime),
        '--param',('app-use-module='+$AppUseModule),
        '--param',('capture-max-dimension='+$CaptureMaxDimension),
        '--param',('artifacts-dir='+$RunDirectory),'--param',('desktop-name='+$desktopName),
        '--param',('ffmpeg='+$ffmpeg),'--param',('ffprobe='+$ffprobe))
    if ($FixtureOnly) { $arguments += @('--param','fixture-only=true') }
    if ($CoordinateGrid) { $arguments += @('--param','coordinate-grid=true') }
    if ($BackgroundInterruption) { $arguments += @('--param','background-interruption=true') }
    if ($UseInteractiveDesktop) { $arguments += @('--param','interactive-fixture=true') }
    $sourceFiles = @(& git -C $repo diff --name-only) + @(& git -C $repo ls-files --others --exclude-standard)
    $deletedSources = @(& git -C $repo diff --name-only --diff-filter=D)
    $hashes = @($sourceFiles | Sort-Object -Unique | ForEach-Object {
        $candidate = Join-Path $repo $_
        if (Test-Path -LiteralPath $candidate -PathType Leaf) { @{path=$_;sha256=(Get-FileHash -LiteralPath $candidate -Algorithm SHA256).Hash} }
    })
    @{head=(& git -C $repo rev-parse HEAD);modified_sources=$hashes;deleted_sources=$deletedSources;model=$Model;agent_runtime=$AgentRuntime;binary=$Binary;binary_sha256=(Get-FileHash -LiteralPath $Binary -Algorithm SHA256).Hash;app_use_module=$AppUseModule;app_use_module_sha256=(Get-FileHash -LiteralPath $AppUseModule -Algorithm SHA256).Hash} |
        ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $RunDirectory 'source.json')
    [uint32]$childId=0
    $childHandle = [DesktopHarness]::Start($Binary,$arguments,$desktopName,$repo,(Join-Path $RunDirectory 'report.json'),(Join-Path $RunDirectory 'runtime.log'),[ref]$childId)
    $child = Get-Process -Id $childId
    $parent = Get-Process -Id $PID
    $owner = @{parent_pid=$PID;parent_start_ticks=$parent.StartTime.ToUniversalTime().Ticks;child_pid=$childId;child_start_ticks=$child.StartTime.ToUniversalTime().Ticks;
        binary=$Binary;switch_desktop=(!$UseInteractiveDesktop);restore_input=$restoreInput;original_desktop=[DesktopHarness]::Name($original);foreground=$foreground.ToInt64();pointer_x=$pointer.X;pointer_y=$pointer.Y;
        deadline_utc=[DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds).ToString('o')}
    $owner | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $RunDirectory 'owner.json')
    $watchdogArgs = @('-NoProfile','-File',$PSCommandPath,'-Watchdog','-RunDirectory',$RunDirectory) | ForEach-Object { [DesktopHarness]::Quote($_) }
    Start-Process -FilePath (Get-Process -Id $PID).Path -ArgumentList $watchdogArgs -WindowStyle Hidden |
        Out-Null
    Write-Output ("Desktop agent verification: " + $RunDirectory)
    $started = [Diagnostics.Stopwatch]::StartNew()
    $switched = $false
    while ([DesktopHarness]::WaitForSingleObject($childHandle,250) -eq 258) {
        if (!$UseInteractiveDesktop -and !$switched -and (Test-Path -LiteralPath (Join-Path $RunDirectory 'desktop-ready'))) {
            if (![DesktopHarness]::SwitchDesktop($testDesktop)) { throw 'Cannot activate isolated desktop.' }
            $switched=$true
        }
        if ($started.Elapsed.TotalSeconds -gt $TimeoutSeconds) { Stop-OwnedChild $owner; throw 'Desktop test deadline exceeded; evidence retained.' }
    }
    [uint32]$nativeExit=1
    [DesktopHarness]::GetExitCodeProcess($childHandle,[ref]$nativeExit) | Out-Null
    $exitCode=if ($nativeExit -gt [int]::MaxValue) { 1 } else { [int]$nativeExit }
} finally {
    if ($childHandle -ne [IntPtr]::Zero) {
        if ([DesktopHarness]::WaitForSingleObject($childHandle,0) -eq 258) { Stop-OwnedChild $owner }
        [DesktopHarness]::CloseHandle($childHandle) | Out-Null
    }
    if (!$UseInteractiveDesktop) { [DesktopHarness]::SwitchDesktop($original) | Out-Null }
    if ($restoreInput) {
        [DesktopHarness]::SetCursorPos($pointer.X,$pointer.Y) | Out-Null
        if ([DesktopHarness]::IsWindow($foreground)) { [DesktopHarness]::SetForegroundWindow($foreground) | Out-Null }
    }
    [DesktopHarness]::CloseDesktop($original) | Out-Null
    if ($testDesktop -ne [IntPtr]::Zero) { [DesktopHarness]::CloseDesktop($testDesktop) | Out-Null }
    [IO.File]::WriteAllText((Join-Path $RunDirectory 'watchdog.stop'),'restored')
    $env:SINOMO_DATA_DIR=$oldData
    $env:SINOMO_CODEX_CREDENTIALS_FILE=$oldCredentials
}
Write-Output ("Report: " + (Join-Path $RunDirectory 'report.json'))
exit $exitCode
