# © Copyright 2026 Query Farm LLC - https://query.farm
cmake_minimum_required(VERSION 3.20)
project(vgi-sqlite VERSION 0.1.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT MSVC)
  add_compile_options(-Wall -Wextra)
endif()
set(CMAKE_CXX_EXTENSIONS OFF)

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

# ── The driver ──────────────────────────────────────────────────────────
add_library(vgi_sqlite_driver
    src/rpc/vgi_connection.cpp
    src/wire/wire_builders.cpp
    src/wire/wire_readers.cpp
    src/catalog/catalog_requests.cpp
    src/catalog/catalog_client.cpp
    src/catalog/scan_requests.cpp
    src/catalog/table_scanner.cpp
)
add_library(vgi_sqlite::driver ALIAS vgi_sqlite_driver)

target_include_directories(vgi_sqlite_driver PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
)

# vgi::vgi carries vgi-rpc-c++'s RpcClient (transport) and vgi-c++'s public
# include/vgi/generated/ protocol schemas as PUBLIC transitive dependencies.
target_link_libraries(vgi_sqlite_driver PUBLIC vgi::vgi nlohmann_json::nlohmann_json)

# ── Dev tools (proof-of-connection binaries, not shipped) ─────────────────
add_subdirectory(tools)

# ── Tests (opt-in) ─────────────────────────────────────────────────────────
option(VGI_SQLITE_BUILD_TESTS "Build vgi-sqlite unit tests" OFF)
if(VGI_SQLITE_BUILD_TESTS)
    enable_testing()
endif()
