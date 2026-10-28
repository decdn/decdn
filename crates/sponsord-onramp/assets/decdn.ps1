# decdn installer for Windows - served by sponsord-onramp at GET /decdn.ps1,
# with the placeholders below substituted server-side (see
# crates/sponsord-onramp/src/installer.rs). The POSIX twin is assets/decdn.sh;
# the two keep the same state.
#
# Install decdn:
#   irm <onramp>/decdn.ps1 | iex
# Install, then download a bundle, one file of a bundle, or a blob, paid for
# by this onramp's sponsor:
#   & ([scriptblock]::Create((irm <onramp>/decdn.ps1))) get decdn:<hash>/[<path>][?ns=<id>] [-o <dir>] [<decdn get flags>]
#   & ([scriptblock]::Create((irm <onramp>/decdn.ps1))) get decdn:<hash>[?ns=<id>] -o <file> [<decdn get flags>]
#
# The `decdn:` URI comes first. It and every argument after it go to
# `decdn get` as given. A blob URI needs `-o <file>`. Without `-o`, a bundle
# or entry URI lets `decdn get` pick the destination, refuse a hidden
# (`.`-prefixed) path, and refuse to replace a file no earlier pull of the
# same entry left there.
#
# Each download gets a throwaway key under ~\.decdn\sponsored\downloads\<hash>\
# and a capability for it, issued once the person passes the gate in the
# browser. A failed download keeps both, so re-running the same command
# resumes without the gate; a finished one deletes them.
#
# Everything runs inside one script block, so `iex` leaves no variables or
# preference changes behind in the caller's session, and nothing here calls
# `exit` (which would close the caller's window). The process-wide settings
# it touches, the TLS protocol list and DECDN_KEYSTORE_PASSWORD, are restored
# on the way out.
& {
  $ErrorActionPreference = 'Stop'
  # Invoke-WebRequest's progress bar slows downloads sharply in Windows
  # PowerShell 5.1.
  $ProgressPreference = 'SilentlyContinue'
  $PriorProtocol = [Net.ServicePointManager]::SecurityProtocol
  $PriorPassword = $env:DECDN_KEYSTORE_PASSWORD
  [Net.ServicePointManager]::SecurityProtocol =
    $PriorProtocol -bor [Net.SecurityProtocolType]::Tls12
  $Locks = New-Object System.Collections.Generic.List[IO.FileStream]
  try {

    $OnrampUrl = '{{ONRAMP_URL}}'

    # decdn comes from a pinned GitHub Release, pinned by tag and by the
    # SHA-256 of its SHA256SUMS file.
    $Releases = '{{RELEASES_BASE}}'
    $DecdnRelease = '{{DECDN_RELEASE}}'
    $DecdnVersion = '{{DECDN_VERSION}}'
    $DecdnSumsSha256 = '{{DECDN_SUMS_SHA256}}'

    # The chain and contracts a download runs against. $SlashJudge is empty
    # when the onramp names none.
    $ChainId = '{{CHAIN_ID}}'
    $RpcUrl = '{{RPC_URL}}'
    $PaymentPool = '{{PAYMENT_POOL}}'
    $CapacityBond = '{{CAPACITY_BOND}}'
    $SlashJudge = '{{SLASH_JUDGE}}'

    # How often, and for how long, to ask the onramp for the capability
    # while the person passes the gate.
    $PollEverySecs = 2
    $PollTimeoutSecs = 600

    # A saved capability this close to expiry is replaced, together with its
    # key (a key's terms are frozen on-chain at its first redemption): a
    # default node refuses vouchers 420 s before expiry, and the download
    # needs an hour on top.
    $ExpiryMarginSecs = 4020

    $Usage = 'usage: decdn.ps1 [get decdn:<hash>/[<path>][?ns=<id>] [-o <dir>] [<decdn get flags>] | get decdn:<hash>[?ns=<id>] -o <file> [<decdn get flags>]]'

    # Parse the arguments before downloading anything.
    $Hash = $null
    $Uri = $null
    $PassArgs = New-Object System.Collections.Generic.List[string]
    if ($args.Count -gt 0) {
      if ($args[0] -ne 'get' -or $args.Count -lt 2) { throw $Usage }
      # A bundle URI (`decdn:<hash>/...`) or a blob URI (`decdn:<hash>` or
      # `decdn:<hash>?...`). Its hash, as 64 lowercase hex characters, keys
      # this download's state; decdn get checks the rest.
      $Uri = [string]$args[1]
      if (-not ($Uri -cmatch '^decdn:([0-9a-fA-F]{64})(/|\?|$)')) {
        throw "decdn: not a decdn: URI (expected decdn:<64 hex characters>, then / for a bundle): $Uri"
      }
      $Hash = $Matches[1].ToLowerInvariant()
      $IsBundle = $Matches[2] -eq '/'
      # The rest go to decdn get, which checks them.
      foreach ($Arg in ($args | Select-Object -Skip 2)) { $PassArgs.Add([string]$Arg) }
      # A bundle URI goes without `-o`: decdn get picks the destination and
      # keeps its overwrite and hidden-path guards, which an explicit `-o`
      # would turn off.
      if ((-not $IsBundle) -and -not ($PassArgs | Where-Object { $_ -cmatch '^(-o|--output(=|$))' })) {
        throw "decdn: a blob URI needs -o <file> to name the file it is written to: $Uri"
      }
    }

    $BinDir = Join-Path $env:LOCALAPPDATA 'decdn\bin'
    $DecdnDir = Join-Path $HOME '.decdn'
    $Markers = Join-Path $DecdnDir 'installer'
    $Sponsored = Join-Path $DecdnDir 'sponsored'
    $Decdn = Join-Path $BinDir 'decdn.exe'
    New-Item -ItemType Directory -Force -Path $BinDir, $DecdnDir, $Markers | Out-Null

    # The OS architecture, not the process's: x64 PowerShell under emulation on
    # an ARM64 machine reports AMD64 in PROCESSOR_ARCHITECTURE.
    $OsArch = try {
      [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
    } catch {
      $env:PROCESSOR_ARCHITECTURE
    }
    $Arch = switch ($OsArch) {
      { $_ -in 'X64', 'AMD64' } { 'x86_64' }
      { $_ -in 'Arm64', 'ARM64' } { 'aarch64' }
      default { throw "decdn: unsupported Windows architecture: $OsArch" }
    }

    $Target = "$Arch-pc-windows-msvc"
    $Utf8 = New-Object System.Text.UTF8Encoding $false

    # Downloads SHA256SUMS and checks it against the pinned digest, then
    # downloads the archive for this platform and checks it against
    # SHA256SUMS. Nothing lands in BinDir unless both checks pass.
    #
    # A marker in $Markers records the pinned digest and target of the last
    # install, so a binary already installed from the same release is kept
    # and nothing is downloaded. Deleting the marker forces a reinstall.
    $FetchBin = {
      param($Repo, $Bin, $Tag, $Version, $SumsSha256)
      $Base = "$Releases/$Repo/releases/download/$Tag"
      $Archive = "$Bin-$Version-$Target.zip"
      $Dir = Join-Path $Work $Bin
      $Marker = Join-Path $Markers $Bin
      $Stamp = "$SumsSha256 $Target"

      if ((Test-Path -PathType Leaf (Join-Path $BinDir "$Bin.exe")) -and
          (Test-Path -PathType Leaf $Marker) -and
          ([IO.File]::ReadAllText($Marker).Trim() -eq $Stamp)) {
        Write-Host "$Bin $Tag is already installed."
        return
      }
      New-Item -ItemType Directory -Force -Path $Dir | Out-Null

      Write-Host "Installing $Bin $Tag ($Target)..."
      $Sums = Join-Path $Dir 'SHA256SUMS'
      Invoke-WebRequest -UseBasicParsing -Uri "$Base/SHA256SUMS" -OutFile $Sums
      if ((Get-FileHash -Algorithm SHA256 $Sums).Hash.ToLower() -ne $SumsSha256) {
        throw "decdn: SHA256SUMS for $Repo $Tag does not match the pinned digest"
      }

      $Expected = Get-Content $Sums | ForEach-Object {
        $Sum, $Name = $_ -split '\s+', 2
        if ($Name -and $Name.TrimStart('*') -eq $Archive) { $Sum.ToLower() }
      } | Select-Object -First 1
      if (-not $Expected) {
        throw "decdn: $Repo $Tag has no $Archive (unsupported platform?)"
      }
      $Zip = Join-Path $Dir $Archive
      Invoke-WebRequest -UseBasicParsing -Uri "$Base/$Archive" -OutFile $Zip
      if ((Get-FileHash -Algorithm SHA256 $Zip).Hash.ToLower() -ne $Expected) {
        throw "decdn: $Archive does not match SHA256SUMS"
      }

      Expand-Archive -Path $Zip -DestinationPath $Dir -Force
      Move-Item -Force -Path (Join-Path $Dir "$Bin.exe") -Destination (Join-Path $BinDir "$Bin.exe")
      [IO.File]::WriteAllText($Marker, $Stamp)
    }

    # Takes the lock file at $Path, held open with no sharing until the run
    # ends; $false when another process holds it. Windows releases it when
    # the holder exits, however it exits.
    $Lock = {
      param($Path)
      try {
        $Locks.Add([IO.File]::Open($Path, 'OpenOrCreate', 'ReadWrite', 'None'))
        $true
      } catch [IO.IOException] {
        $false
      }
    }

    # The capability issued to $Address: an object with `token` and `expiry`,
    # or $null while there is none yet.
    $GetCapability = {
      param($Address)
      $Uri = "$OnrampUrl/v1/capability?client=$Address"
      try {
        $Resp = Invoke-WebRequest -UseBasicParsing -Uri $Uri
      } catch {
        throw "decdn: $Uri failed: $($_.Exception.Message)"
      }
      if ($Resp.StatusCode -eq 204) { return $null }
      $Body = $Resp.Content | ConvertFrom-Json
      if (-not ($Body.token -match '^dcap1:[A-Za-z0-9_-]+$') -or -not $Body.expiry) {
        throw "decdn: unexpected capability response: $($Resp.Content)"
      }
      $Body
    }

    # 1. Install decdn.
    $Work = Join-Path ([IO.Path]::GetTempPath()) ("decdn-" + [Guid]::NewGuid())
    New-Item -ItemType Directory -Force -Path $Work | Out-Null
    try {
      & $FetchBin 'decdn' 'decdn' $DecdnRelease $DecdnVersion $DecdnSumsSha256
    } finally {
      Remove-Item -Recurse -Force -Path $Work -ErrorAction SilentlyContinue
    }

    # 2. Put decdn on PATH: permanently for the user, and right away for this
    #    session so the next command on the same line finds it.
    $UserPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    if (-not (($UserPath -split ';') -contains $BinDir)) {
      $NewPath = if ($UserPath) { "$BinDir;$UserPath" } else { $BinDir }
      [Environment]::SetEnvironmentVariable('Path', $NewPath, 'User')
    }
    if (-not (($env:Path -split ';') -contains $BinDir)) {
      $env:Path = "$BinDir;$env:Path"
    }

    if (-not $Hash) {
      Write-Host ''
      Write-Host 'decdn is ready. Download a bundle or a blob with:'
      Write-Host "  & ([scriptblock]::Create((irm $OnrampUrl/decdn.ps1))) get decdn:<hash>/[<path>][?ns=<id>] [-o <dir>] [<decdn get flags>]"
      Write-Host "  & ([scriptblock]::Create((irm $OnrampUrl/decdn.ps1))) get decdn:<hash>[?ns=<id>] -o <file> [<decdn get flags>]"
      return
    }

    # 3. Download. One run per download at a time, so two runs never make
    #    two keys.
    $Downloads = Join-Path $Sponsored 'downloads'
    New-Item -ItemType Directory -Force -Path $Downloads | Out-Null
    if (-not (& $Lock (Join-Path $Downloads "$Hash.lock"))) {
      throw 'decdn: this download is already running in another window'
    }
    $State = Join-Path $Downloads $Hash
    New-Item -ItemType Directory -Force -Path $State | Out-Null
    $KeystoreFile = Join-Path $State 'keystore.json'
    $PasswordFile = Join-Path $State 'password'
    $AddressFile = Join-Path $State 'address'
    $CapabilityFile = Join-Path $State 'capability'
    $ExpiryFile = Join-Path $State 'expiry'

    $Now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $Expiry = [int64]0
    if (Test-Path -PathType Leaf $ExpiryFile) {
      [void][int64]::TryParse([IO.File]::ReadAllText($ExpiryFile).Trim(), [ref]$Expiry)
    }
    if ((Test-Path -PathType Leaf $CapabilityFile) -and $Expiry -le ($Now + $ExpiryMarginSecs)) {
      Remove-Item -Recurse -Force -Path $State
      New-Item -ItemType Directory -Force -Path $State | Out-Null
    }

    # decdn reads the password from this before the password file.
    Remove-Item Env:DECDN_KEYSTORE_PASSWORD -ErrorAction SilentlyContinue

    # This download's key, generated with a random password on first use. A
    # key missing its password or address is replaced, together with the
    # capability bound to it.
    # A string from the start: an `if` that yields nothing would assign
    # AutomationNull, against which -notmatch filters to an empty, false list.
    $Address = ''
    if ((Test-Path -PathType Leaf $KeystoreFile) -and
        (Test-Path -PathType Leaf $PasswordFile) -and
        (Test-Path -PathType Leaf $AddressFile)) {
      $Address = [IO.File]::ReadAllText($AddressFile).Trim()
    }
    if ($Address -notmatch '^0x[0-9a-fA-F]{40}$') {
      foreach ($Stale in $KeystoreFile, (Join-Path $State 'node.secret'), $AddressFile, $CapabilityFile, $ExpiryFile) {
        Remove-Item -Force -Path $Stale -ErrorAction SilentlyContinue
      }
      $Bytes = New-Object byte[] 32
      [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($Bytes)
      [IO.File]::WriteAllText($PasswordFile, (-join ($Bytes | ForEach-Object { $_.ToString('x2') })), $Utf8)
      $KeyGen = & $Decdn key-gen --output-dir $State --keystore-password-file $PasswordFile
      if ($LASTEXITCODE -ne 0) { throw 'decdn: decdn key-gen failed' }
      $Address = $KeyGen | ForEach-Object {
        if ($_ -match '^eth address:\s*(0x[0-9a-fA-F]{40})\s*$') { $Matches[1] }
      } | Select-Object -First 1
      if (-not $Address) { throw 'decdn: decdn key-gen printed no address' }
      [IO.File]::WriteAllText($AddressFile, $Address, $Utf8)
    }

    # Its capability: a saved one is reused; otherwise the onramp's, after
    # the person passes the gate if it has none.
    if (Test-Path -PathType Leaf $CapabilityFile) {
      $Token = [IO.File]::ReadAllText($CapabilityFile).Trim()
    } else {
      $Capability = & $GetCapability $Address
      if (-not $Capability) {
        $Url = "$OnrampUrl/fund?client=$Address"
        Write-Host 'Open this link to start the download:'
        Write-Host "  $Url"
        if ([Environment]::UserInteractive -and -not [Console]::IsOutputRedirected) {
          try { Start-Process $Url } catch { }
        }
        $Deadline = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds() + $PollTimeoutSecs
        while (-not $Capability) {
          if ([DateTimeOffset]::UtcNow.ToUnixTimeSeconds() -ge $Deadline) {
            throw "decdn: timed out after $PollTimeoutSecs s waiting for the check in the browser"
          }
          Start-Sleep -Seconds $PollEverySecs
          $Capability = & $GetCapability $Address
        }
      }
      $Token = [string]$Capability.token
      [IO.File]::WriteAllText($ExpiryFile, [string]$Capability.expiry, $Utf8)
      [IO.File]::WriteAllText($CapabilityFile, $Token, $Utf8)
    }

    # decdn's data dir holds its peer store and buyer-channel store, shared
    # by downloads so peers one finds serve the next. decdn locks it for a
    # whole pull, so a download started while another holds it uses its own.
    $DataDir = Join-Path $Sponsored 'decdn'
    if (& $Lock (Join-Path $Sponsored 'decdn.lock')) {
      New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
    } else {
      Write-Host 'decdn: another download is running; this one starts without the shared peer cache'
      $DataDir = $State
    }

    # The caller's arguments go in unchecked, before the ones this run owns.
    $PullArgs = @('get', $Uri) + @($PassArgs) + @(
      '--capability', $Token,
      '--keystore', $KeystoreFile,
      '--keystore-password-file', $PasswordFile,
      '--data-dir', $DataDir,
      '--rpc-url', $RpcUrl,
      '--payment-pool-address', $PaymentPool,
      '--capacity-bond-address', $CapacityBond,
      '--chain-id', $ChainId
    )
    if ($SlashJudge) { $PullArgs += '--slash-judge-address', $SlashJudge }

    & $Decdn @PullArgs
    if ($LASTEXITCODE -ne 0) {
      throw ('decdn: download did not finish. Re-run the same command to resume: ' +
        'downloaded bytes are kept, and the allowance is reused while it is valid.')
    }
    Remove-Item -Recurse -Force -Path $State
  } finally {
    foreach ($Held in $Locks) { $Held.Dispose() }
    [Net.ServicePointManager]::SecurityProtocol = $PriorProtocol
    $env:DECDN_KEYSTORE_PASSWORD = $PriorPassword
  }
} @args
