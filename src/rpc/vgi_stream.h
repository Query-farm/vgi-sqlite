// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <memory>
#include <optional>
#include <string>

#include "wire/nanoarrow_types.h"

namespace vgi_sqlite {

struct AnnotatedBatch {
    nano::BatchPtr batch;
    std::string metadata;
};

class VgiStream {
public:
    virtual ~VgiStream() = default;
    virtual std::optional<AnnotatedBatch> Tick() = 0;
    virtual std::optional<AnnotatedBatch> Exchange(const nano::BatchPtr& input) = 0;
    virtual void Close() = 0;
};

}  // namespace vgi_sqlite
