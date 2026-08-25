#!/usr/bin/env bash
# Regenerate src/generated/vgi_request_builders.hpp from vgi-python.
#
# Unlike vgi-c++ (the worker SDK), this repo does NOT generate its own copy
# of the protocol schemas/constants/version - it links vgi::vgi and consumes
# vgi-c++'s public include/vgi/generated/ copy directly, so there is exactly
# one generated copy of those to ever drift. (Regenerate that one with
# ../vgi-c++/scripts/regenerate_protocol.sh when the protocol changes; this
# script assumes it's already current.)
#
# What IS generated here is this repo's own copy of the request builders -
# client-only code the worker SDK has no use for - targeting this repo's
# engine-neutral wire/wire_builders.h instead of vgi_rpc_types.hpp (which
# pulls in DuckDB), and throwing std::runtime_error instead of
# duckdb::IOException. Same generator vgi and vgi-c++ use
# (vgi.codegen.cpp_request_builders), different flags - see that module's
# --schemas-include/--helpers-include/--exception-include/--exception-type.
#
# Regenerating pulls in whatever protocol version vgi-python is currently
# at. If that is ahead of the worker you test against, the version gate
# will refuse every request - check VGI_PROTOCOL_VERSION in
# ../vgi-c++/include/vgi/generated/vgi_protocol_version.hpp after
# regenerating that one too.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VGI_PYTHON="${VGI_PYTHON:-$HOME/Development/vgi-python}"
NS="${VGI_SQLITE_NAMESPACE:-vgi::generated}"

[[ -d "$VGI_PYTHON" ]] || { echo "vgi-python not found at $VGI_PYTHON (set VGI_PYTHON)" >&2; exit 2; }

(cd "$VGI_PYTHON" && uv run --project . python -m vgi.codegen.cpp_request_builders \
    --namespace "$NS" \
    --schemas-include "vgi/generated/vgi_protocol_schemas.hpp" \
    --helpers-include "wire/wire_builders.h" \
    --exception-include "<stdexcept>" \
    --exception-type "std::runtime_error") \
    > "$ROOT/src/generated/vgi_request_builders.hpp"

# Rewrite the provenance banner: the generator's default "To regenerate"
# command names vgi's (the DuckDB extension's) destination and omits every
# flag this repo's generation actually depends on. Same fix vgi-c++'s
# regenerate_protocol.sh applies to its own generated headers, for the same
# reason.
python3 - "$ROOT/src/generated/vgi_request_builders.hpp" <<'PY'
import re, sys
p = sys.argv[1]
src = open(p).read()
src = re.sub(
    r"// To regenerate:\n(//   [^\n]*\n)+",
    "// To regenerate:\n//   scripts/regenerate_protocol.sh\n",
    src, count=1,
)
open(p, "w").write(src)
PY

echo "regenerated into namespace $NS"
