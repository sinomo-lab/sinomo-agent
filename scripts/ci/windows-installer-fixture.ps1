function Get-CliDigest([string]$File) {
    $stream = [IO.File]::OpenRead($File)
    $sha = [Security.Cryptography.SHA256]::Create()
    try { return [BitConverter]::ToString($sha.ComputeHash($stream)) }
    finally { $stream.Dispose(); $sha.Dispose() }
}

function Write-DamagedFixtureBinary([string]$File, [string]$Text) {
    # A daemon can acknowledge stop before Windows releases its executable.
    $deadline = [datetime]::UtcNow.AddSeconds(30)
    $bytes = [Text.Encoding]::UTF8.GetBytes($Text)
    while ($true) {
        try {
            [IO.File]::WriteAllBytes($File, $bytes)
            return
        } catch [IO.IOException] {
            $code = $_.Exception.GetBaseException().HResult -band 0xffff
            if ($code -notin 32, 33 -or [datetime]::UtcNow -ge $deadline) { throw }
            Start-Sleep -Milliseconds 200
        }
    }
}
