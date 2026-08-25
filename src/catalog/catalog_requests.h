// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Hand-coded *inner* request builders: some of the codegen'd outer
// `Build<Method>Params(request_bytes)` wrappers in
// generated/vgi_request_builders.hpp expect the caller to have already
// IPC-serialized an inner dataclass batch into `request_bytes`
// (CatalogAttachParamsSchema is just `{request: binary}`) - these produce
// that inner batch. Mirrors vgi's (the DuckDB extension's) hand-coded
// "Hand-coded inner request builders" section of vgi_rpc_types.cpp, ported
// as reference for field layout (see that file's BuildCatalogAttachRequest)
// with no DuckDB dependency.
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/record_batch.h>

namespace vgi_sqlite {

// Build the inner CatalogAttachRequest batch. Caller serializes (see
// wire::encode_ipc) and wraps with generated::BuildCatalogAttachParams to
// produce the wire params. Fields match vgi-python's CatalogAttachRequest
// (vgi/protocol.py): name (required), options (an IPC-serialized RecordBatch
// of ATTACH-option key/values, itself passed pre-encoded), data_version_spec
// and implementation_version (semver constraints, unconstrained when
// nullopt). client_capabilities is left null - a later phase's concern.
std::shared_ptr<arrow::RecordBatch> BuildCatalogAttachRequest(
    const std::string& name, const std::optional<std::vector<uint8_t>>& options_ipc_bytes,
    const std::optional<std::string>& data_version_spec,
    const std::optional<std::string>& implementation_version);

}  // namespace vgi_sqlite
