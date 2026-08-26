// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Ported from vgi (the DuckDB extension)'s src/vgi_launcher{,_internal}.cpp
// (POSIX half only - see launcher.h). Adaptations from that reference,
// none of which touch wire-visible behavior:
//   - IOException/InvalidInputException -> std::runtime_error, matching
//     this driver's uniform exception convention (see CLAUDE.md).
//   - SHA-256 via vgi_rpc::crypto::Sha256 (already linked - vgi-c++'s own
//     split_token.cpp uses it) instead of a direct mbedtls dependency
//     vgi-sqlite doesn't otherwise need.
//   - The liveness probe and the worker's stdout-pipe plumbing are raw
//     POSIX socket/fork calls here instead of going through a UnixSocket/
//     Pipe wrapper class - this driver has no other user for either
//     abstraction, so introducing them just for this one call site would
//     be speculative infrastructure, not a real simplification.
#include "rpc/launcher.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <fcntl.h>
#include <mutex>
#include <pthread.h>
#include <set>
#include <stdexcept>
#include <sys/file.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

#include <vgi_rpc/crypto.h>

extern char** environ;

namespace vgi_sqlite {
namespace launcher {

// ---------------------------------------------------------------------------
// Hash + canonical JSON
// ---------------------------------------------------------------------------

std::string EncodeJsonString(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    out.push_back('"');
    for (unsigned char c : s) {
        switch (c) {
            case '"':
                out.append("\\\"");
                break;
            case '\\':
                out.append("\\\\");
                break;
            case '\b':
                out.append("\\b");
                break;
            case '\f':
                out.append("\\f");
                break;
            case '\n':
                out.append("\\n");
                break;
            case '\r':
                out.append("\\r");
                break;
            case '\t':
                out.append("\\t");
                break;
            default:
                if (c < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out.append(buf);
                } else {
                    // Printable ASCII or a raw UTF-8 byte - passed through
                    // unchanged. Matches the reference's own documented
                    // caveat: Python's json.dumps(ensure_ascii=True)
                    // default would \uXXXX-escape non-ASCII bytes instead,
                    // so a non-ASCII byte in cwd/env diverges from the
                    // Python launcher's hash - an accepted, documented gap
                    // in the protocol itself, not something to silently
                    // "fix" here and diverge from the C++ reference too.
                    out.push_back(static_cast<char>(c));
                }
                break;
        }
    }
    out.push_back('"');
    return out;
}

std::string BuildCanonicalJson(const std::vector<std::string>& argv, const std::string& cwd,
                                const std::map<std::string, std::string>& env_subset) {
    std::string out;
    out.reserve(64 + cwd.size());
    out.append("{\"cmd\":[");
    for (size_t i = 0; i < argv.size(); ++i) {
        if (i > 0) out.push_back(',');
        out.append(EncodeJsonString(argv[i]));
    }
    out.append("],\"cwd\":");
    out.append(EncodeJsonString(cwd));
    out.append(",\"env\":{");
    bool first = true;
    for (const auto& kv : env_subset) {
        // std::map iterates in key order, which for std::less<std::string>
        // is byte-lex ASCII order - matches Python's sort_keys=True.
        if (!first) out.push_back(',');
        first = false;
        out.append(EncodeJsonString(kv.first));
        out.push_back(':');
        out.append(EncodeJsonString(kv.second));
    }
    out.append("}}");
    return out;
}

std::string ComputeLauncherHash(const std::vector<std::string>& argv, const std::string& cwd,
                                 const std::map<std::string, std::string>& env_subset) {
    std::string payload = BuildCanonicalJson(argv, cwd, env_subset);
    vgi_rpc::crypto::Sha256 h;
    h.update(payload);
    auto digest = h.digest();
    // First 8 bytes as 16 lowercase hex chars - matches the reference
    // (and vgi-rpc-python's launcher.py) exactly.
    char hex[17];
    for (size_t i = 0; i < 8; ++i) {
        std::snprintf(hex + i * 2, 3, "%02x", digest[i]);
    }
    return std::string(hex, 16);
}

std::map<std::string, std::string> FilterVgiRpcEnv(
    const std::vector<std::pair<std::string, std::string>>& env) {
    std::map<std::string, std::string> out;
    const std::string prefix = "VGI_RPC_";
    for (const auto& kv : env) {
        if (kv.first.size() >= prefix.size() && kv.first.compare(0, prefix.size(), prefix) == 0) {
            out.emplace(kv.first, kv.second);
        }
    }
    return out;
}

// ---------------------------------------------------------------------------
// State directory resolution
// ---------------------------------------------------------------------------

std::string ResolveStateDir(const std::string& xdg_runtime_dir, const std::string& tmpdir, uint32_t euid) {
    if (!xdg_runtime_dir.empty()) {
        return xdg_runtime_dir + "/vgi-rpc";
    }
    std::string base = tmpdir.empty() ? "/tmp" : tmpdir;
    while (base.size() > 1 && base.back() == '/') base.pop_back();
    return base + "/vgi-rpc-" + std::to_string(euid);
}

// ---------------------------------------------------------------------------
// Rendezvous path-length validation
// ---------------------------------------------------------------------------

void ValidateRendezvousPathLength(const std::string& path) {
    // `+1` for the trailing NUL the kernel requires.
    if (path.size() + 1 > MaxUnixPathLen()) {
        throw std::runtime_error("vgi launcher: AF_UNIX socket path too long (" +
                                  std::to_string(path.size()) + " bytes, max " +
                                  std::to_string(MaxUnixPathLen() - 1) + "): " + path);
    }
}

// ---------------------------------------------------------------------------
// `launch:` location string parsing
// ---------------------------------------------------------------------------

namespace {

enum class ShlexState { kDefault, kInDouble, kInSingle };

void FlushToken(std::string& token, std::vector<std::string>& out, bool& has_token) {
    if (has_token) {
        out.push_back(std::move(token));
        token.clear();
        has_token = false;
    }
}

}  // namespace

std::vector<std::string> ParseLaunchArgv(const std::string& payload) {
    std::vector<std::string> out;
    std::string token;
    bool has_token = false;
    ShlexState state = ShlexState::kDefault;

    for (size_t i = 0; i < payload.size(); ++i) {
        char c = payload[i];
        switch (state) {
            case ShlexState::kDefault:
                if (c == ' ' || c == '\t' || c == '\n') {
                    FlushToken(token, out, has_token);
                } else if (c == '"') {
                    state = ShlexState::kInDouble;
                    has_token = true;  // empty "" is still a token
                } else if (c == '\'') {
                    state = ShlexState::kInSingle;
                    has_token = true;
                } else if (c == '\\') {
                    if (i + 1 >= payload.size()) {
                        throw std::runtime_error("vgi launcher: trailing backslash in launch: argv");
                    }
                    token.push_back(payload[++i]);
                    has_token = true;
                } else {
                    token.push_back(c);
                    has_token = true;
                }
                break;

            case ShlexState::kInDouble:
                if (c == '"') {
                    state = ShlexState::kDefault;
                } else if (c == '\\' && i + 1 < payload.size()) {
                    char next = payload[i + 1];
                    // Only a few sequences are special inside double-quotes
                    // per POSIX; everything else preserves the backslash.
                    if (next == '"' || next == '\\' || next == '$' || next == '`' || next == '\n') {
                        token.push_back(next);
                        ++i;
                    } else {
                        token.push_back('\\');
                    }
                } else {
                    token.push_back(c);
                }
                break;

            case ShlexState::kInSingle:
                // Single quotes are raw - no escapes.
                if (c == '\'') {
                    state = ShlexState::kDefault;
                } else {
                    token.push_back(c);
                }
                break;
        }
    }

    if (state != ShlexState::kDefault) {
        throw std::runtime_error("vgi launcher: unterminated quote in launch: argv");
    }
    FlushToken(token, out, has_token);

    if (out.empty()) {
        throw std::runtime_error("vgi launcher: launch: location has empty argv");
    }
    return out;
}

// ---------------------------------------------------------------------------
// Discovery-line parsing
// ---------------------------------------------------------------------------

DiscoveryParseResult ParseDiscoveryLine(std::string& buffer, const std::string& expected_path,
                                        const std::string& prefix) {
    while (true) {
        size_t newline = buffer.find('\n');
        if (newline == std::string::npos) return DiscoveryParseResult::kNeedMore;
        std::string line = buffer.substr(0, newline);
        buffer.erase(0, newline + 1);
        if (!line.empty() && line.back() == '\r') line.pop_back();  // tolerate \r\n
        if (line.compare(0, prefix.size(), prefix) == 0) {
            std::string path = line.substr(prefix.size());
            return path == expected_path ? DiscoveryParseResult::kFound : DiscoveryParseResult::kMismatch;
        }
        // Non-matching line: third-party noise: skip it, keep scanning.
    }
}

// ---------------------------------------------------------------------------
// Location scheme detection
// ---------------------------------------------------------------------------

bool IsLaunchLocation(const std::string& location) {
    const std::string prefix = "launch:";
    return location.size() >= prefix.size() && location.compare(0, prefix.size(), prefix) == 0;
}

}  // namespace launcher

// =============================================================================
// Orchestration: spawn-or-connect
// =============================================================================

namespace {

// ---------------------------------------------------------------------------
// Fork-safety: see vgi's own vgi_subprocess.hpp for the full rationale this
// is ported from - the short version: a signal delivered in the fork()..
// exec() window still runs the PARENT's handler on the CHILD's copy of the
// address space (its own handlers aren't installed until it execs or
// explicitly resets them), so a worker killed moments after being spawned
// can end up running arbitrary parent-process signal-handling code instead
// of just dying. ScopedForkSignalBlock closes that window by blocking every
// signal for the fork() call itself; ResetChildSignalDispositions is the
// child's first statement, disarming inherited handlers before anything
// else touches fds or execs.
// ---------------------------------------------------------------------------

void ResetChildSignalDispositions() {
    for (int sig = 1; sig < NSIG; ++sig) {
        if (sig == SIGPIPE) continue;  // preserve the process-wide SIGPIPE ignore
        struct sigaction sa;
        std::memset(&sa, 0, sizeof(sa));
        sa.sa_handler = SIG_DFL;
        sigemptyset(&sa.sa_mask);
        (void)::sigaction(sig, &sa, nullptr);  // SIGKILL/SIGSTOP fail harmlessly
    }
    sigset_t empty;
    sigemptyset(&empty);
    (void)::sigprocmask(SIG_SETMASK, &empty, nullptr);
}

class ScopedForkSignalBlock {
public:
    ScopedForkSignalBlock() {
        sigset_t all;
        sigfillset(&all);
        if (::pthread_sigmask(SIG_SETMASK, &all, &saved_) == 0) active_ = true;
    }
    ~ScopedForkSignalBlock() { Restore(); }
    ScopedForkSignalBlock(const ScopedForkSignalBlock&) = delete;
    ScopedForkSignalBlock& operator=(const ScopedForkSignalBlock&) = delete;

    void Restore() noexcept {
        if (active_) {
            (void)::pthread_sigmask(SIG_SETMASK, &saved_, nullptr);
            active_ = false;
        }
    }

private:
    sigset_t saved_;
    bool active_ = false;
};

// ---------------------------------------------------------------------------
// Centralized worker-pid reaper - one shared thread polling every spawned
// pid with WNOHANG, rather than a detached waitpid thread per spawn (which
// would collide with a host that installs its own SIGCHLD handler, and
// accumulate thread/stack resources for a worker's full idle-timeout
// lifetime, potentially hours, once per spawn this process ever does).
// ---------------------------------------------------------------------------

class PidReaper {
public:
    static PidReaper& Instance() {
        static PidReaper inst;
        return inst;
    }

    void Register(pid_t pid) {
        if (pid <= 0) return;
        std::lock_guard<std::mutex> lk(mu_);
        pids_.insert(pid);
        if (!started_) {
            started_ = true;
            std::thread([this]() { Run(); }).detach();
        }
    }

private:
    PidReaper() = default;

    void Run() {
        using namespace std::chrono_literals;
        while (true) {
            std::this_thread::sleep_for(500ms);
            std::vector<pid_t> snapshot;
            {
                std::lock_guard<std::mutex> lk(mu_);
                snapshot.assign(pids_.begin(), pids_.end());
            }
            for (pid_t pid : snapshot) {
                int status = 0;
                pid_t rc = ::waitpid(pid, &status, WNOHANG);
                // rc > 0: reaped. rc < 0 (+ECHILD): kernel auto-reaped
                // (e.g. host installed SIG_IGN) or already gone - drop
                // either way. rc == 0: still running, keep polling.
                if (rc != 0) {
                    std::lock_guard<std::mutex> lk(mu_);
                    pids_.erase(pid);
                }
            }
        }
    }

    std::mutex mu_;
    std::set<pid_t> pids_;
    bool started_ = false;
};

// Hard cap on bytes buffered waiting for the worker's discovery line - a
// misbehaving (or hostile) worker that prints unbounded prefix noise would
// otherwise OOM this process.
constexpr std::size_t kDiscoveryBufferLimit = 1u << 20;  // 1 MiB

std::vector<std::pair<std::string, std::string>> SnapshotEnvironment() {
    std::vector<std::pair<std::string, std::string>> out;
    for (char** e = environ; e && *e; ++e) {
        std::string entry(*e);
        auto eq = entry.find('=');
        if (eq == std::string::npos) continue;
        out.emplace_back(entry.substr(0, eq), entry.substr(eq + 1));
    }
    return out;
}

std::string EnvOr(const char* name, const char* fallback) {
    const char* v = std::getenv(name);
    return v ? std::string(v) : (fallback ? std::string(fallback) : std::string());
}

void MkdirRecursive(const std::string& path) {
    if (path.empty() || path == "/") return;
    for (size_t i = 1; i <= path.size(); ++i) {
        if (i < path.size() && path[i] != '/') continue;
        std::string sub = path.substr(0, i);
        if (sub.empty()) continue;
        if (::mkdir(sub.c_str(), 0700) == 0) continue;
        if (errno == EEXIST) {
            struct stat st;
            if (::stat(sub.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) continue;
            throw std::runtime_error("vgi launcher: " + sub + " exists but is not a directory");
        }
        throw std::runtime_error("vgi launcher: mkdir(" + sub + ") failed: " + std::strerror(errno));
    }
}

// Owns an open lockfile fd; releases the flock (closing the fd) on scope
// exit. flock(2), not fcntl - matches the cross-SDK contract exactly (see
// docs/launcher-protocol.md).
class FlockGuard {
public:
    static FlockGuard Acquire(const std::string& path, std::chrono::milliseconds timeout) {
        int fd = ::open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
        if (fd < 0) {
            throw std::runtime_error("vgi launcher: open(" + path + ") for flock failed: " +
                                      std::strerror(errno));
        }
        auto deadline = std::chrono::steady_clock::now() + timeout;
        // flock(2) has no native timeout - spin on LOCK_NB with a small
        // sleep. ~50ms granularity is fine: the rare-contention case is
        // typically zero-wait or minutes-of-spawn, never fine-grained.
        while (true) {
            if (::flock(fd, LOCK_EX | LOCK_NB) == 0) return FlockGuard(fd);
            if (errno != EWOULDBLOCK && errno != EINTR) {
                int saved = errno;
                ::close(fd);
                throw std::runtime_error("vgi launcher: flock(" + path + ") failed: " + std::strerror(saved));
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                ::close(fd);
                throw std::runtime_error("vgi launcher: timed out acquiring lock " + path + " after " +
                                          std::to_string(timeout.count()) + "ms");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    FlockGuard(const FlockGuard&) = delete;
    FlockGuard& operator=(const FlockGuard&) = delete;
    FlockGuard(FlockGuard&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
    ~FlockGuard() {
        if (fd_ >= 0) ::close(fd_);  // closing the fd implicitly releases the flock
    }

private:
    explicit FlockGuard(int fd) : fd_(fd) {}
    int fd_ = -1;
};

std::string ResolveAndEnsureStateDir(const std::optional<std::string>& override_dir) {
    std::string dir = override_dir.value_or(launcher::ResolveStateDir(
        EnvOr("XDG_RUNTIME_DIR", ""), EnvOr("TMPDIR", ""), static_cast<uint32_t>(::geteuid())));
    MkdirRecursive(dir);
    // Tighten mode + ownership-check - catches a state dir a different
    // user created (e.g. a stale /tmp/vgi-rpc-<uid> from before a uid
    // reuse) before trusting anything under it.
    ::chmod(dir.c_str(), 0700);
    struct stat st;
    if (::stat(dir.c_str(), &st) != 0) {
        throw std::runtime_error("vgi launcher: stat(" + dir + ") failed: " + std::strerror(errno));
    }
    if (st.st_uid != ::geteuid()) {
        throw std::runtime_error("vgi launcher: state directory " + dir +
                                  " is not owned by the current user");
    }
    return dir;
}

// Best-effort connect probe - true iff a worker is currently accepting on
// `path`. A bare connect+close, not a real RPC handshake - liveness only.
bool ProbeAlive(const std::string& path) {
    if (path.size() + 1 > launcher::MaxUnixPathLen()) return false;
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return false;
    struct sockaddr_un addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    bool ok = ::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0;
    ::close(fd);
    return ok;
}

struct SpawnResult {
    pid_t pid;
    int stdout_fd;
};

SpawnResult SpawnWorker(const std::vector<std::string>& final_argv,
                         const std::optional<std::string>& worker_stderr_path) {
    int pipefd[2];
    if (::pipe(pipefd) != 0) {
        throw std::runtime_error(std::string("vgi launcher: pipe() failed: ") + std::strerror(errno));
    }
    // CLOEXEC the parent-retained ends so they don't leak into any later
    // fork+exec in this process (macOS lacks pipe2(), so set both flags
    // explicitly rather than relying on an atomic pipe2() call).
    if (::fcntl(pipefd[0], F_SETFD, FD_CLOEXEC) != 0 || ::fcntl(pipefd[1], F_SETFD, FD_CLOEXEC) != 0) {
        int saved = errno;
        ::close(pipefd[0]);
        ::close(pipefd[1]);
        throw std::runtime_error(std::string("vgi launcher: fcntl(FD_CLOEXEC) failed: ") +
                                  std::strerror(saved));
    }

    ScopedForkSignalBlock fork_signal_block;  // see the class comment above
    pid_t pid = ::fork();
    if (pid < 0) {
        int saved = errno;
        ::close(pipefd[0]);
        ::close(pipefd[1]);
        throw std::runtime_error(std::string("vgi launcher: fork() failed: ") + std::strerror(saved));
    }
    if (pid == 0) {
        // --- Child ---
        ResetChildSignalDispositions();

        ::close(pipefd[0]);
        if (::dup2(pipefd[1], STDOUT_FILENO) < 0) ::_exit(126);
        ::close(pipefd[1]);

        int err_fd = worker_stderr_path
                         ? ::open(worker_stderr_path->c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600)
                         : ::open("/dev/null", O_WRONLY | O_CLOEXEC);
        if (err_fd < 0) ::_exit(126);
        if (::dup2(err_fd, STDERR_FILENO) < 0) ::_exit(126);
        ::close(err_fd);

        int in_fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
        if (in_fd < 0) ::_exit(126);
        if (::dup2(in_fd, STDIN_FILENO) < 0) ::_exit(126);
        ::close(in_fd);

        // Deliberately no setsid() - matches the reference: terminal
        // SIGINT/SIGHUP propagate from this process to the worker.
        std::vector<char*> raw;
        raw.reserve(final_argv.size() + 1);
        for (const auto& a : final_argv) raw.push_back(const_cast<char*>(a.c_str()));
        raw.push_back(nullptr);
        ::execvp(raw[0], raw.data());
        ::_exit(127);  // exec failed
    }

    // --- Parent ---
    fork_signal_block.Restore();
    ::close(pipefd[1]);
    return {pid, pipefd[0]};
}

// Reads from `stdout_fd` until either ParseDiscoveryLine finds the
// expected line, the worker exits, or `deadline` passes. On success,
// closes the pipe - the cross-SDK contract says the worker must not write
// to stdout again after its discovery line, so a contract-violating
// worker gets SIGPIPE here, which is the right outcome (the bug is in the
// worker; a noisy crash there beats a silent thread/fd leak here) - and
// hands the pid to PidReaper. Throws std::runtime_error on any failure.
void WaitForReadinessAndDetach(pid_t worker_pid, int stdout_fd, const std::string& expected_path,
                                std::chrono::steady_clock::time_point deadline) {
    std::string buffer;
    char chunk[4096];
    auto fail = [&](const std::string& msg) -> void {
        ::kill(worker_pid, SIGTERM);
        ::close(stdout_fd);
        PidReaper::Instance().Register(worker_pid);
        throw std::runtime_error(msg);
    };
    while (true) {
        auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            fail("vgi launcher: worker did not emit UNIX:<path> within startup timeout");
        }
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);

        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(stdout_fd, &rfds);
        struct timeval tv;
        tv.tv_sec = static_cast<time_t>(remaining.count() / 1000);
        tv.tv_usec = static_cast<suseconds_t>((remaining.count() % 1000) * 1000);
        int sel = ::select(stdout_fd + 1, &rfds, nullptr, nullptr, &tv);
        if (sel < 0) {
            if (errno == EINTR) continue;
            fail(std::string("vgi launcher: select() failed: ") + std::strerror(errno));
        }
        if (sel == 0) continue;  // loop will hit the deadline check

        ssize_t n = ::read(stdout_fd, chunk, sizeof(chunk));
        if (n == 0) {
            ::close(stdout_fd);
            int status = 0;
            ::waitpid(worker_pid, &status, 0);
            throw std::runtime_error("vgi launcher: worker exited before emitting UNIX:<path> (status=" +
                                      std::to_string(status) + ")");
        }
        if (n < 0) {
            if (errno == EINTR) continue;
            fail(std::string("vgi launcher: read() from worker stdout failed: ") + std::strerror(errno));
        }
        if (buffer.size() + static_cast<size_t>(n) > kDiscoveryBufferLimit) {
            fail("vgi launcher: worker emitted >1 MiB of pre-discovery output without the "
                 "UNIX:<path> line; aborting");
        }
        buffer.append(chunk, static_cast<size_t>(n));
        auto parse = launcher::ParseDiscoveryLine(buffer, expected_path);
        if (parse == launcher::DiscoveryParseResult::kMismatch) {
            fail("vgi launcher: worker bound to a different path than requested");
        }
        if (parse == launcher::DiscoveryParseResult::kFound) {
            ::close(stdout_fd);
            PidReaper::Instance().Register(worker_pid);
            return;
        }
        // kNeedMore - keep reading.
    }
}

}  // namespace

std::string Launch(const LaunchConfig& cfg) {
    if (cfg.worker_argv.empty()) {
        throw std::runtime_error("vgi launcher: worker_argv must be non-empty");
    }

    std::string state_dir = ResolveAndEnsureStateDir(cfg.state_dir_override);

    std::string sock_path;
    std::string lock_path;
    if (cfg.socket_path_override) {
        sock_path = *cfg.socket_path_override;
        lock_path = sock_path + ".lock";
    } else {
        auto env_subset = launcher::FilterVgiRpcEnv(SnapshotEnvironment());
        std::string cwd;
        std::size_t buf_size = 4096;
        while (true) {
            std::vector<char> buf(buf_size);
            if (::getcwd(buf.data(), buf.size()) != nullptr) {
                cwd.assign(buf.data());
                break;
            }
            if (errno != ERANGE) break;  // cwd unreadable - empty string still hashes fine
            if (buf_size > (1u << 20)) break;
            buf_size *= 2;
        }
        auto hash = launcher::ComputeLauncherHash(cfg.worker_argv, cwd, env_subset);
        sock_path = state_dir + "/" + hash + ".sock";
        lock_path = state_dir + "/" + hash + ".lock";
    }

    launcher::ValidateRendezvousPathLength(sock_path);

    auto guard = FlockGuard::Acquire(lock_path, cfg.connect_timeout);

    // Inside the lock: probe for an existing healthy worker, else spawn.
    if (ProbeAlive(sock_path)) {
        return sock_path;
    }
    ::unlink(sock_path.c_str());  // stale socket file (worker crashed / never bound) - best-effort

    std::vector<std::string> argv = cfg.worker_argv;
    argv.emplace_back("--unix");
    argv.push_back(sock_path);
    argv.emplace_back("--idle-timeout");
    {
        // Per docs/launcher-protocol.md: decimal seconds in plain
        // notation, no scientific - "%g" switches to "%e" past 1e6,
        // which would silently mangle a large idle-timeout. "%.3f" keeps
        // millisecond precision; trailing zeros are stripped to match
        // the other client SDKs' own formatting.
        double sec = static_cast<double>(cfg.idle_timeout.count()) / 1000.0;
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%.3f", sec);
        std::string s(buf);
        auto dot = s.find('.');
        if (dot != std::string::npos) {
            while (s.size() > dot + 1 && s.back() == '0') s.pop_back();
            if (s.back() == '.') s.pop_back();
        }
        argv.push_back(std::move(s));
    }

    auto spawn = SpawnWorker(argv, cfg.worker_stderr_path);
    auto deadline = std::chrono::steady_clock::now() + cfg.worker_startup_timeout;
    WaitForReadinessAndDetach(spawn.pid, spawn.stdout_fd, sock_path, deadline);

    // FlockGuard releases as `guard` goes out of scope on return.
    return sock_path;
}

}  // namespace vgi_sqlite
