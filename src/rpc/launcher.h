// © Copyright 2026 Query Farm LLC - https://query.farm
//
// AF_UNIX worker launcher: brings up - or reuses - a long-running worker
// process that VgiConnection can connect to via unix://, for a
// `launch:<argv...>` location. Concurrency contract: at most one worker
// exists per (worker_argv, cwd, VGI_RPC_*-env) tuple, system-wide and
// across every process (this driver, vgi's own DuckDB extension,
// `vgi-rpc launch` on the CLI, any other client) - coordination is via a
// per-hash flock in a per-user state directory, not anything specific to
// this driver.
//
// POSIX only (this driver has no Windows build target at all - see
// CLAUDE.md - so unlike vgi's own cross-platform port, this one doesn't
// carry a named-pipe/Windows half).
//
// Ported from vgi (the DuckDB C++ extension)'s own launcher
// (src/vgi_launcher{,_internal}.{hpp,cpp}), the reference C++
// implementation of docs/launcher-protocol.md - not reinvented, since the
// wire contract (state-dir layout, hash input, file naming, discovery-line
// format) must stay byte-for-byte compatible with vgi-rpc-python's
// launcher.py AND with vgi's own client, so that whichever one gets there
// first, the other reuses its already-running worker instead of spawning
// a second one. Every function below must keep matching that contract
// exactly, not just "work" - see launcher.cpp's file comment for the
// specific adaptations made (exception type, SHA-256 source) and why
// none of them touch the wire-visible behavior.
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace vgi_sqlite {
namespace launcher {

// ---------------------------------------------------------------------------
// Hash + canonical JSON
// ---------------------------------------------------------------------------

// JSON-encode `s` per RFC 8259 with the same escaping rules Python's
// json.dumps() uses by default (backslash/quote/control-char escapes;
// printable ASCII and UTF-8 bytes pass through unchanged). Returns the
// encoded string INCLUDING the surrounding double quotes.
std::string EncodeJsonString(const std::string& s);

// The canonical JSON payload that gets SHA-256-hashed:
//   {"cmd":["arg0",...],"cwd":"...","env":{"VGI_RPC_FOO":"x",...}}
// Object key order is exactly cmd/cwd/env (matches Python's
// json.dumps(sort_keys=True) ASCII-lex order); env's own keys are
// likewise ASCII-lex sorted; no whitespace; strings via EncodeJsonString.
std::string BuildCanonicalJson(const std::vector<std::string>& argv, const std::string& cwd,
                                const std::map<std::string, std::string>& env_subset);

// The 16-hex-char (lowercase) launcher hash: the first 8 bytes of
// sha256(BuildCanonicalJson(argv, cwd, env)), hex-encoded.
std::string ComputeLauncherHash(const std::vector<std::string>& argv, const std::string& cwd,
                                 const std::map<std::string, std::string>& env_subset);

// Filters a flat env map down to entries whose key starts with
// "VGI_RPC_" - the only env vars the hash (and hence worker identity)
// depends on. Output is sorted so it can feed BuildCanonicalJson/
// ComputeLauncherHash directly.
std::map<std::string, std::string> FilterVgiRpcEnv(
    const std::vector<std::pair<std::string, std::string>>& env);

// ---------------------------------------------------------------------------
// State directory resolution
// ---------------------------------------------------------------------------

// Per-user state directory for launcher coordination (lockfiles, sockets):
//   $XDG_RUNTIME_DIR set and non-empty -> $XDG_RUNTIME_DIR/vgi-rpc
//   else $TMPDIR set and non-empty     -> $TMPDIR/vgi-rpc-$EUID
//   else                                -> /tmp/vgi-rpc-$EUID
// Arguments passed explicitly (not read from the environment here) so
// this stays a pure, testable function - callers (Launch()) do the real
// getenv/geteuid calls. Returns the resolved path with no trailing slash.
std::string ResolveStateDir(const std::string& xdg_runtime_dir, const std::string& tmpdir, uint32_t euid);

// AF_UNIX `sun_path` length cap, counting the trailing NUL: 104 on macOS,
// 108 on Linux.
constexpr std::size_t MaxUnixPathLen() {
#if defined(__APPLE__)
    return 104;
#else
    return 108;
#endif
}

// Throws std::runtime_error if `path` is too long for MaxUnixPathLen();
// returns silently otherwise.
void ValidateRendezvousPathLength(const std::string& path);

// ---------------------------------------------------------------------------
// `launch:` location string parsing (POSIX shell-quote semantics)
// ---------------------------------------------------------------------------

// Parses the argv portion of a `launch:` location (the part after the
// "launch:" prefix): simple whitespace-separated words, double-quoted
// strings with backslash escapes for \" \\ \$ \`, single-quoted strings
// as raw literals (no escapes), bare backslash escapes outside quotes.
// Throws std::runtime_error on an unterminated quote or an empty argv.
std::vector<std::string> ParseLaunchArgv(const std::string& payload);

// ---------------------------------------------------------------------------
// Discovery-line parsing
// ---------------------------------------------------------------------------

// The worker prints exactly one "UNIX:<path>\n" line on stdout once
// bound, then must never write to stdout again (a launcher closing its
// read end of that pipe after seeing this line is what enforces that -
// see launcher.cpp).
inline const char* kDiscoveryLinePrefix = "UNIX:";

enum class DiscoveryParseResult {
    kNeedMore,  // No complete line yet - keep reading.
    kFound,     // The expected "<prefix><path>" line was consumed.
    kMismatch,  // A "<prefix>..." line was seen but its path doesn't match.
};

// Stateful line-by-line scanner: the caller appends newly-read bytes to
// `buffer`; this consumes complete '\n'-terminated lines from its front,
// skipping any that don't start with `prefix` (a misbehaving worker's
// startup noise, tolerated up to the caller's own byte cap), until it
// finds one that does. Returns kFound the first time
// "<prefix><expected_path>" is fully consumed, kMismatch if a
// "<prefix>..." line names a different path (a worker bug - fatal),
// kNeedMore otherwise.
DiscoveryParseResult ParseDiscoveryLine(std::string& buffer, const std::string& expected_path,
                                        const std::string& prefix = kDiscoveryLinePrefix);

// ---------------------------------------------------------------------------
// Location scheme detection
// ---------------------------------------------------------------------------

bool IsLaunchLocation(const std::string& location);

}  // namespace launcher

// Configuration for one Launch() call - see docs/launcher-protocol.md and
// this header's file comment for the protocol this implements.
struct LaunchConfig {
    // The worker command and its arguments. Must be non-empty. `--unix
    // <path>` and `--idle-timeout <secs>` are appended automatically -
    // do not include them.
    std::vector<std::string> worker_argv;
    // Skips the per-hash machinery entirely when set (mainly useful for
    // tests) - the worker is spawned bound to this exact path.
    std::optional<std::string> socket_path_override;
    std::chrono::milliseconds idle_timeout{300000};
    std::chrono::milliseconds connect_timeout{30000};
    std::chrono::milliseconds worker_startup_timeout{60000};
    // Default: discard (dup'd onto /dev/null in the child).
    std::optional<std::string> worker_stderr_path;
    // Default: resolved via ResolveStateDir from $XDG_RUNTIME_DIR/$TMPDIR/euid.
    std::optional<std::string> state_dir_override;
};

// Brings up (or reuses) a worker per `cfg`; returns the absolute AF_UNIX
// socket path the caller should connect to (via VgiConnection::Connect's
// existing unix:// path). Throws std::runtime_error on any failure to
// bring up a worker (flock contention timeout, worker exits before
// readiness, worker startup timeout, path-too-long, empty argv) - always
// either returns a path or throws, never silently fails or returns an
// empty string.
//
// Thread-safe: concurrent calls with the same `cfg` serialize on the
// kernel flock, with at most one of them actually performing the spawn -
// the same real-file coordination that makes it safe across PROCESSES
// too (this driver, vgi's own DuckDB extension, `vgi-rpc launch`, all
// agreeing on one worker for the same tuple).
std::string Launch(const LaunchConfig& cfg);

}  // namespace vgi_sqlite
