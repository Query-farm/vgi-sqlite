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
