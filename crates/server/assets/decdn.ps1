# decdn-sponsored installer for Windows - served by sponsord at GET /decdn.ps1,
# with the placeholders below substituted server-side (see
# crates/server/src/http/installer.rs) from ServerConfig. The POSIX twin is
# assets/decdn.sh; the two write the same profile.
#
# Install, then download:
#   irm <gateway>/decdn.ps1 | iex; decdn-sponsored pull b3:<hash>
# Or pass the arguments through in one call:
#   & ([scriptblock]::Create((irm <gateway>/decdn.ps1))) pull b3:<hash>
#
# Everything runs inside one script block, so `iex` leaves no variables or
# preference changes behind in the caller's session, and nothing here calls
# `exit` (which would close the caller's window). The one process-wide
# setting it touches, the TLS protocol list, is restored on the way out.
& {
  $ErrorActionPreference = 'Stop'
  # Invoke-WebRequest's progress bar slows downloads sharply in Windows
  # PowerShell 5.1.
  $ProgressPreference = 'SilentlyContinue'
  $PriorProtocol = [Net.ServicePointManager]::SecurityProtocol
  [Net.ServicePointManager]::SecurityProtocol =
    $PriorProtocol -bor [Net.SecurityProtocolType]::Tls12
  try {

    $Gateway = '{{GATEWAY_BASE}}'
    $RpcUrl = '{{RPC_URL}}'
    $PaymentPool = '{{PAYMENT_POOL}}'
    $CapacityBond = '{{CAPACITY_BOND}}'
    $ChainId = '{{CHAIN_ID}}'

    # The binaries come from pinned GitHub Releases. Each release is pinned by
    # tag and by the SHA-256 of its SHA256SUMS file.
    $Releases = 'https://github.com/decdn'
    $DecdnRelease = '{{DECDN_RELEASE}}'
    $DecdnSumsSha256 = '{{DECDN_SUMS_SHA256}}'
    $WrapperRelease = '{{WRAPPER_RELEASE}}'
    $WrapperSumsSha256 = '{{WRAPPER_SUMS_SHA256}}'

    $BinDir = Join-Path $env:LOCALAPPDATA 'decdn\bin'
    $DecdnDir = Join-Path $HOME '.decdn'
    New-Item -ItemType Directory -Force -Path $BinDir, $DecdnDir | Out-Null

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

    # Downloads SHA256SUMS and checks it against the pinned digest, then
    # downloads the archive for this platform and checks it against
    # SHA256SUMS. Nothing lands in BinDir unless both checks pass.
    $FetchBin = {
      param($Repo, $Bin, $Tag, $SumsSha256)
      $Base = "$Releases/$Repo/releases/download/$Tag"
      $Archive = "$Bin-$($Tag.Substring(1))-$Target.zip"
      $Dir = Join-Path $Work $Bin
      New-Item -ItemType Directory -Force -Path $Dir | Out-Null

      Write-Host "Installing $Bin $Tag ($Target)..."
      $Sums = Join-Path $Dir 'SHA256SUMS'
      Invoke-WebRequest -UseBasicParsing -Uri "$Base/SHA256SUMS" -OutFile $Sums
      if ((Get-FileHash -Algorithm SHA256 $Sums).Hash.ToLower() -ne $SumsSha256) {
        throw "decdn: SHA256SUMS for $Repo $Tag does not match the pinned digest"
      }

      $Expected = Get-Content $Sums | ForEach-Object {
        $Hash, $Name = $_ -split '\s+', 2
        if ($Name -and $Name.TrimStart('*') -eq $Archive) { $Hash.ToLower() }
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
    }

    # 1. Install the decdn and decdn-sponsored binaries.
    $Work = Join-Path ([IO.Path]::GetTempPath()) ("decdn-" + [Guid]::NewGuid())
    New-Item -ItemType Directory -Force -Path $Work | Out-Null
    try {
      & $FetchBin 'decdn' 'decdn' $DecdnRelease $DecdnSumsSha256
      & $FetchBin 'sponsord' 'decdn-sponsored' $WrapperRelease $WrapperSumsSha256
    } finally {
      Remove-Item -Recurse -Force -Path $Work -ErrorAction SilentlyContinue
    }

    # 2. Put the binaries on PATH: permanently for the user, and right away for
    #    this session so the next command on the same line finds them.
    $UserPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    if (-not (($UserPath -split ';') -contains $BinDir)) {
      $NewPath = if ($UserPath) { "$BinDir;$UserPath" } else { $BinDir }
      [Environment]::SetEnvironmentVariable('Path', $NewPath, 'User')
    }
    if (-not (($env:Path -split ';') -contains $BinDir)) {
      $env:Path = "$BinDir;$env:Path"
    }

    # 3. Write the wrapper's profile. Field names and shape MUST match
    #    crates/wrapper/src/config.rs's `Profile` struct exactly. Paths use
    #    forward slashes, which Windows accepts and TOML strings need no
    #    escaping for. Each download gets its own throwaway key under data_dir.
    $Fwd = { param($p) $p.Replace('\', '/') }
    $DecdnBin = & $Fwd (Join-Path $BinDir 'decdn.exe')
    $DataDir = & $Fwd (Join-Path $DecdnDir 'sponsored')
    $ProfileToml = @"
gateway_base = "$Gateway"
decdn_bin = "$DecdnBin"
data_dir = "$DataDir"
rpc_url = "$RpcUrl"
payment_pool = "$PaymentPool"
capacity_bond = "$CapacityBond"
chain_id = $ChainId
"@
    # UTF-8 without a byte-order mark: Windows PowerShell 5.1's `-Encoding UTF8`
    # writes one, and a BOM is not valid TOML.
    [IO.File]::WriteAllText((Join-Path $DecdnDir 'sponsor.toml'), $ProfileToml,
      (New-Object System.Text.UTF8Encoding $false))

    if ($args.Count -gt 0) {
      & (Join-Path $BinDir 'decdn-sponsored.exe') @args
      return
    }

    Write-Host ''
    Write-Host 'decdn-sponsored is ready. Download with:'
    Write-Host '  decdn-sponsored pull b3:<hash> [-o <dir>]'
  } finally {
    [Net.ServicePointManager]::SecurityProtocol = $PriorProtocol
  }
} @args
