# © Copyright 2026 Query Farm LLC - https://query.farm
# describe_worker: spawns a worker and prints its RPC method inventory.
#
# Milestone-1 proof of connection - it exercises RpcClient::spawn, the
# protocol-version stamp, and one real unary RPC round trip
# (vgi_rpc's built-in __describe__) against a live worker process, without
# needing any VGI-protocol-level request/response marshalling yet. That
# marshalling (catalog_attach and friends) is the next slice of work.
add_executable(vgi-describe-worker describe_worker.cpp)
target_link_libraries(vgi-describe-worker PRIVATE vgi_sqlite::driver)

# catalog_probe: attaches a catalog and walks its schemas/tables - the
# Milestone-2 proof of the catalog-discovery RPCs end to end.
add_executable(vgi-catalog-probe catalog_probe.cpp)
target_link_libraries(vgi-catalog-probe PRIVATE vgi_sqlite::driver)

# scan_probe: binds/inits/scans one table and prints its rows - the
# Milestone-2 proof of the bind->init->producer-tick data path.
add_executable(vgi-scan-probe scan_probe.cpp)
target_link_libraries(vgi-scan-probe PRIVATE vgi_sqlite::driver)

# scalar_probe: binds and calls one scalar function via exchange mode -
# the Milestone-3 proof of the scalar-function bind/exchange data path.
add_executable(vgi-scalar-probe scalar_probe.cpp)
target_link_libraries(vgi-scalar-probe PRIVATE vgi_sqlite::driver)
