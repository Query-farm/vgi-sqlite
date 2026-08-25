#!/usr/bin/env bash
# Fetch and build the local SQLite this repo tests against.
#
# Pinned rather than "whatever's on $PATH": the driver needs a recent
# SQLite (built-in JSON, xBestIndex LIMIT/OFFSET constraint support,
# generated columns - all landed well after the ~3.8-vintage sqlite3 macOS
# ships with system Python/the system CLI) with extension loading enabled,
# which the system build usually is not.
#
# Produces third_party/sqlite/{sqlite3,libsqlite3.a,sqlite3.h,sqlite3ext.h}.
# The CLI is what you .load the built extension into for a manual session;
# the static lib + headers are what the test harness links against.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/third_party/sqlite"

SQLITE_VERSION="3530400"     # 3.53.4
SQLITE_ZIP_SHA3_256="628a44cfe82c66aed1ccbbe85a562d2e33ebe64b3288981ed76285612227934e"
SQLITE_URL="https://www.sqlite.org/2026/sqlite-amalgamation-${SQLITE_VERSION}.zip"

mkdir -p "$OUT"
cd "$OUT"

if [[ ! -f sqlite3.c ]]; then
    echo "downloading $SQLITE_URL"
    curl -sSL -o amalgamation.zip "$SQLITE_URL"

    # SHA3-256 verification: shasum -a3 is what macOS/most Linux distros
    # ship (part of Perl's Digest::SHA3 via the `shasum` wrapper, present
    # wherever `shasum` itself is); fall back to openssl's sha3-256 if not.
    computed=""
    if command -v shasum >/dev/null 2>&1 && shasum -a3-256 /dev/null >/dev/null 2>&1; then
        computed="$(shasum -a3-256 amalgamation.zip | cut -d' ' -f1)"
    elif command -v openssl >/dev/null 2>&1 && openssl dgst -sha3-256 /dev/null >/dev/null 2>&1; then
        computed="$(openssl dgst -sha3-256 amalgamation.zip | awk '{print $NF}')"
    fi
    if [[ -n "$computed" ]]; then
        if [[ "$computed" != "$SQLITE_ZIP_SHA3_256" ]]; then
            echo "SHA3-256 mismatch: expected $SQLITE_ZIP_SHA3_256, got $computed" >&2
            rm -f amalgamation.zip
            exit 1
        fi
        echo "verified SHA3-256"
    else
        echo "warning: no SHA3-256 tool found (shasum -a3-256 / openssl dgst -sha3-256); skipping verification" >&2
    fi

    unzip -q -j amalgamation.zip -d .
    rm -f amalgamation.zip
else
    echo "third_party/sqlite/sqlite3.c already present; skipping download (delete it to re-fetch)"
fi

# -DSQLITE_ENABLE_LOAD_EXTENSION=1 is largely a formality (it's the
# built-in default outside SQLITE_OMIT_LOAD_EXTENSION builds) but stated
# explicitly since a loadable vgi.{dylib,so} is this whole repo's point.
# -DSQLITE_ENABLE_RTREE / -DSQLITE_ENABLE_FTS5: not needed by this driver,
# left off. JSON functions and generated columns are compiled in by
# default in every 3.3x+ amalgamation (no flag needed).
CFLAGS=(
    -DSQLITE_ENABLE_LOAD_EXTENSION=1
    -DSQLITE_ENABLE_COLUMN_METADATA=1
    -O2
)

echo "building libsqlite3.a"
cc -c "${CFLAGS[@]}" sqlite3.c -o sqlite3.o
ar rcs libsqlite3.a sqlite3.o
rm -f sqlite3.o

echo "building sqlite3 CLI"
cc "${CFLAGS[@]}" -DHAVE_READLINE=0 shell.c sqlite3.c -o sqlite3 -lpthread -ldl -lm

echo "done: $OUT/{sqlite3,libsqlite3.a,sqlite3.h,sqlite3ext.h}"
"$OUT/sqlite3" --version
