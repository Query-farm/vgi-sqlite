// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Wire helpers specific to VGI's write path (insert/update/delete),
// layered on top of the same bind/init lifecycle scan_requests.h drives -
// a table-in-out write function is bound and initted exactly like a table
// scan function, just with InitRequest.phase="INPUT" and a different input
// schema (see TableWriter's file comment). See write_requests.cpp for the
// concrete wire shapes, ported from vgi's (the DuckDB extension's)
// src/storage/vgi_physical_write.cpp client-side driver - the authoritative
// reference for how a client is actually meant to call this, since VGI's
// protocol.py has no dedicated write-options dataclass at all (it's a
// convention layered on top of the ordinary bind/init/exchange machinery,
// not a first-class wire type).
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace vgi_sqlite {

// IPC-encodes VGI's write_options convention: a 1-row
// {return_chunks: bool, on_conflict: utf8, on_conflict_columns: list<utf8>}
// batch. This driver only ever requests return_chunks=false (plain
// row-count responses - see TableWriter's file comment on why RETURNING
// support is out of scope for now) and on_conflict="throw" (SQLite's own
// UNIQUE-constraint semantics are enforced by the underlying table anyway;
// ON CONFLICT clause translation isn't implemented).
std::vector<uint8_t> BuildWriteOptionsBytes(bool return_chunks, const std::string& on_conflict,
                                             const std::vector<std::string>& on_conflict_columns = {});

// Repackages a write function's flat positional-argument batch
// (WriteFunctionResult.positional_arguments, wire-identical to
// ScanFunctionResult's - see catalog_client.h's file comment) into
// BindRequest.arguments' required struct-of-positional_N/named_X shape,
// with one extra field appended: `named_write_options`, a binary column
// carrying `write_options_bytes` verbatim (VGI's "write options travel as
// a named argument, IPC-serialized" convention, mirrored from
// vgi_physical_write.cpp's SetupWriteConnection). Always exactly 1 row,
// same reasoning as table_scanner.cpp's WrapAsArgsStruct - never trusts
// the flat source batch's own row count.
std::vector<uint8_t> BuildWriteArgsStruct(const std::vector<uint8_t>& flat_positional_args_ipc_bytes,
                                           const std::vector<uint8_t>& write_options_bytes);

}  // namespace vgi_sqlite
