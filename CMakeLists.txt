# © Copyright 2026 Query Farm LLC - https://query.farm
cmake_minimum_required(VERSION 3.20)
project(vgi-sqlite VERSION 0.1.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT MSVC)
  add_compile_options(-Wall -Wextra)
endif()
set(CMAKE_CXX_EXTENSIONS OFF)

# Position-independent code, globally, before pulling in vgi-c++ (and,
# transitively through it, vgi-rpc-c++) below: every static library this
# repo builds or depends on - vgi_sqlite_driver, vgi-c++'s own vgi target,
# vgi-rpc-c++'s vgi_rpc target - ends up linked into vgi_extension, a
# MODULE (shared) library. Linux's linker refuses non-PIC object code in a
# shared object outright ("recompile with -fPIC"); macOS's doesn't enforce
# this the same way for a static library linked into a dylib, so every
# build of this repo so far (all on macOS) never surfaced it. Set here,
# globally, rather than per-target: it has to apply to vgi-c++'s and
# vgi-rpc-c++'s own targets too, and neither of those repos' CMakeLists.txt
# needs editing for that - a target created after this is set inherits it
# unless it explicitly overrides POSITION_INDEPENDENT_CODE itself.
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ── vgi-c++ (worker SDK, also home of the public generated protocol
#    schemas this client links against) and, transitively through it,
#    vgi-rpc-c++ (the Arrow-IPC RPC transport: RpcClient et al.) ──────────
#
# Sibling-checkout build by default, same rationale as vgi-c++ takes for
# vgi-rpc-c++: this driver and the SDK it links move together, so an
# installed copy that can go stale silently is the opt-in, not the default.
set(VGI_CPP_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../vgi-c++"
    CACHE PATH "Sibling vgi-c++ checkout to build against")

if(NOT EXISTS "${VGI_CPP_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR
        "vgi-c++ not found at ${VGI_CPP_SOURCE_DIR}.\n"
        "Clone it beside this repo, or set -DVGI_CPP_SOURCE_DIR=<path>.")
endif()
# We only want the vgi library target (and, transitively, vgi_rpc) - not
# vgi-c++'s own example worker or unit tests.
set(VGI_BUILD_TESTS OFF CACHE BOOL "" FORCE)
add_subdirectory("${VGI_CPP_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/vgi-cpp" EXCLUDE_FROM_ALL)

find_package(Arrow CONFIG REQUIRED)
find_package(ArrowCompute CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)

# ── SQLite (the local, pinned build scripts/fetch_sqlite.sh produces) ─────
#
# Not vcpkg/find_package(SQLite3): the driver needs a recent SQLite (JSON,
# xBestIndex LIMIT/OFFSET, generated columns) built with extension loading
# enabled, and the system/package-manager SQLite is neither reliably recent
# nor reliably built that way. See scripts/fetch_sqlite.sh's own comment.
#
# Two consumers, two different relationships to it:
#   * vgi_extension (the loadable .so/.dylib) needs only sqlite3ext.h's
#     *headers* - every sqlite3_* call it makes resolves dynamically
#     through the host process's own function table (SQLITE_EXTENSION_INIT,
#     see extension.cpp's file comment). It must NOT link libsqlite3.a: two
#     independently-initialized copies of SQLite's process-global state in
#     one process crash on load, even same-version.
#   * Anything that IS its own process (tools/, tests/) links the real
#     static library directly - there's no host to dispatch through.
set(VGI_SQLITE_THIRD_PARTY_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/sqlite")
if(NOT EXISTS "${VGI_SQLITE_THIRD_PARTY_DIR}/sqlite3.h")
    message(FATAL_ERROR
        "Local SQLite not found at ${VGI_SQLITE_THIRD_PARTY_DIR}.\n"
        "Run scripts/fetch_sqlite.sh first.")
endif()
add_library(sqlite3 STATIC IMPORTED)
set_target_properties(sqlite3 PROPERTIES
    IMPORTED_LOCATION "${VGI_SQLITE_THIRD_PARTY_DIR}/libsqlite3.a"
    INTERFACE_INCLUDE_DIRECTORIES "${VGI_SQLITE_THIRD_PARTY_DIR}"
)
add_library(SQLite::SQLite3 ALIAS sqlite3)

# ── The driver: pure VGI client logic, no SQLite dependency ───────────────
add_library(vgi_sqlite_driver
    src/rpc/vgi_connection.cpp
    src/wire/wire_builders.cpp
    src/wire/wire_readers.cpp
    src/catalog/catalog_requests.cpp
    src/catalog/catalog_client.cpp
    src/catalog/scan_requests.cpp
    src/catalog/catalog_table_plan.cpp
    src/catalog/table_scanner.cpp
    src/catalog/scalar_function_caller.cpp
    src/catalog/write_requests.cpp
    src/catalog/table_writer.cpp
    src/catalog/aggregate_requests.cpp
    src/catalog/aggregate_caller.cpp
    src/vtab/connection_pool.cpp
)
add_library(vgi_sqlite::driver ALIAS vgi_sqlite_driver)

target_include_directories(vgi_sqlite_driver PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
)

# vgi::vgi carries vgi-rpc-c++'s RpcClient (transport) and vgi-c++'s public
# include/vgi/generated/ protocol schemas as PUBLIC transitive dependencies.
target_link_libraries(vgi_sqlite_driver PUBLIC vgi::vgi nlohmann_json::nlohmann_json)

# ── The loadable extension itself (.load ./vgi in a sqlite3 session) ──────
#
# type_mapping.cpp, vgi_vtab.cpp, and filter_pushdown.cpp are built here,
# not into vgi_sqlite_driver above: they're the files that call sqlite3_*
# directly, and they need this target's SQLITE_EXTENSION_INIT wiring
# (headers only, no linked library - see the SQLite section's comment).
# connection_pool.cpp moved *into* vgi_sqlite_driver above: despite living
# under src/vtab/, it makes no sqlite3_* call at all - and
# ScalarFunctionCaller (src/catalog/, part of the driver) now depends on
# it directly (every scalar-function call Acquire()s its own connection -
# see scalar_function_caller.h's file comment), so it has to be reachable
# from tools/ and the pytest harness too, not just the extension.
add_library(vgi_extension MODULE
    src/extension.cpp
    src/types/type_mapping.cpp
    src/vtab/vgi_vtab.cpp
    src/vtab/filter_pushdown.cpp
)
target_link_libraries(vgi_extension PRIVATE vgi_sqlite::driver)
target_include_directories(vgi_extension PRIVATE "${VGI_SQLITE_THIRD_PARTY_DIR}")
set_target_properties(vgi_extension PROPERTIES
    OUTPUT_NAME vgi
    PREFIX ""              # SQLite's .load expects "vgi", not "libvgi"
)

# ── Dev tools (proof-of-connection binaries, not shipped) ─────────────────
add_subdirectory(tools)

# ── Tests (opt-in) ─────────────────────────────────────────────────────────
option(VGI_SQLITE_BUILD_TESTS "Build vgi-sqlite unit tests" OFF)
if(VGI_SQLITE_BUILD_TESTS)
    enable_testing()
endif()
