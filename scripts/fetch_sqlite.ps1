# Fetch and build the local SQLite this repo tests against (Windows).
#
# The Windows counterpart to fetch_sqlite.sh - same pinned version/hash,
# same output contract (third_party/sqlite/{sqlite3.exe,sqlite3.lib,
# sqlite3.h,sqlite3ext.h}), but built with cl.exe/lib.exe instead of
# cc/ar, since there is no portable cc/ar on a bare Windows box (Git for
# Windows bundles bash/curl/unzip but not a C toolchain). Requires a
# Visual Studio C++ toolset to already be installed (vswhere.exe locates
# it) - this script does not install one.

$ErrorActionPreference = "Stop"

$Root = Resolve-Path "$PSScriptRoot\.."
$Out = Join-Path $Root "third_party\sqlite"

$SqliteVersion = "3530400"     # 3.53.4
$SqliteZipSha3_256 = "628a44cfe82c66aed1ccbbe85a562d2e33ebe64b3288981ed76285612227934e"
$SqliteUrl = "https://www.sqlite.org/2026/sqlite-amalgamation-$SqliteVersion.zip"

New-Item -ItemType Directory -Force -Path $Out | Out-Null
Push-Location $Out
try {
    if (-not (Test-Path "sqlite3.c")) {
        Write-Host "downloading $SqliteUrl"
        Invoke-WebRequest -Uri $SqliteUrl -OutFile "amalgamation.zip"

        # SHA3-256 verification: PowerShell's own Get-FileHash has no SHA3
        # support (only MD5/SHA1/SHA256/SHA384/SHA512) - shell out to
        # openssl if present (Git for Windows bundles one under usr\bin),
        # matching fetch_sqlite.sh's own "verify if a tool exists, warn
        # and skip otherwise" degrade path rather than a hard requirement.
        $openssl = $null
        $onPath = Get-Command openssl.exe -ErrorAction SilentlyContinue
        if ($onPath) {
            $openssl = $onPath.Source
        } elseif (Test-Path "C:\Program Files\Git\usr\bin\openssl.exe") {
            $openssl = "C:\Program Files\Git\usr\bin\openssl.exe"
        }

        if ($openssl) {
            $digestLine = & $openssl dgst -sha3-256 "amalgamation.zip"
            $computed = ($digestLine -split "=")[-1].Trim()
            if ($computed -ne $SqliteZipSha3_256) {
                Write-Error "SHA3-256 mismatch: expected $SqliteZipSha3_256, got $computed"
                Remove-Item "amalgamation.zip" -Force
                exit 1
            }
            Write-Host "verified SHA3-256"
        } else {
            Write-Warning "no SHA3-256 tool found (openssl); skipping verification"
        }

        Expand-Archive -Path "amalgamation.zip" -DestinationPath "." -Force
        # The zip's own top-level entries already land directly in $Out
        # (sqlite-amalgamation-<version> is the zip's internal prefix, but
        # SQLite's amalgamation zips are NOT nested a second level deep) -
        # verified against fetch_sqlite.sh's own `unzip -j` (junk paths)
        # behavior; Expand-Archive has no direct equivalent, so flatten
        # manually if the archive did nest one level.
        $nested = Get-ChildItem -Directory -Filter "sqlite-amalgamation-*"
        if ($nested) {
            Get-ChildItem $nested.FullName | Move-Item -Destination "." -Force
            Remove-Item $nested.FullName -Recurse -Force
        }
        Remove-Item "amalgamation.zip" -Force
    } else {
        Write-Host "third_party\sqlite\sqlite3.c already present; skipping download (delete it to re-fetch)"
    }

    # Locate a Visual Studio C++ toolset via vswhere (ships with the VS
    # Installer since 2017) rather than assuming this script runs inside
    # an already-activated "Developer PowerShell" - most CI/automation
    # contexts won't have one active.
    $vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
    if (-not (Test-Path $vswhere)) {
        throw "vswhere.exe not found - is Visual Studio (with the C++ workload) installed?"
    }
    $vsInstall = & $vswhere -latest -products '*' -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 `
        -property installationPath
    if (-not $vsInstall) {
        throw "No Visual Studio installation with the C++ (VC.Tools.x86.x64) component found."
    }
    $vcvarsall = Join-Path $vsInstall "VC\Auxiliary\Build\vcvarsall.bat"

    # -DSQLITE_ENABLE_LOAD_EXTENSION=1 is largely a formality (the built-in
    # default outside SQLITE_OMIT_LOAD_EXTENSION builds) but stated
    # explicitly since a loadable vgi.dll is this whole repo's point.
    # Same flag set as fetch_sqlite.sh, MSVC spelling (/D instead of -D,
    # /O2 instead of -O2 - cl.exe also accepts -D directly, but /O2 is
    # required, cl has no plain -O2).
    $cflags = "/DSQLITE_ENABLE_LOAD_EXTENSION=1 /DSQLITE_ENABLE_COLUMN_METADATA=1 /O2"

    Write-Host "building sqlite3.lib"
    $buildLibCmd = "call `"$vcvarsall`" x64 && cl /c $cflags sqlite3.c /Fosqlite3.obj && lib /OUT:sqlite3.lib sqlite3.obj"
    cmd /c $buildLibCmd
    if ($LASTEXITCODE -ne 0) { throw "building sqlite3.lib failed" }
    Remove-Item "sqlite3.obj" -Force -ErrorAction SilentlyContinue

    Write-Host "building sqlite3.exe CLI"
    $buildCliCmd = "call `"$vcvarsall`" x64 && cl $cflags /DHAVE_READLINE=0 shell.c sqlite3.c /Fesqlite3.exe"
    cmd /c $buildCliCmd
    if ($LASTEXITCODE -ne 0) { throw "building sqlite3.exe failed" }
    Remove-Item "shell.obj", "sqlite3.obj" -Force -ErrorAction SilentlyContinue

    Write-Host "done: $Out\{sqlite3.exe,sqlite3.lib,sqlite3.h,sqlite3ext.h}"
    & "$Out\sqlite3.exe" --version
} finally {
    Pop-Location
}
