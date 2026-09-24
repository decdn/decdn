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

    # 1. Install the decdn and decdn-sponsored binaries.
    #
    # NOTE: release hosting (GET /dl/<bin>-<os>-<arch>) is not wired up on the
    # gateway yet; until it is, this step fails with a 404.
    foreach ($bin in 'decdn', 'decdn-sponsored') {
      Write-Host "Installing $bin..."
      Invoke-WebRequest -UseBasicParsing -Uri "$Gateway/dl/$bin-windows-$Arch.exe" `
        -OutFile (Join-Path $BinDir "$bin.exe")
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
