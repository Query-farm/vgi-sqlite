// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Two small SQL-escaping helpers, shared by every place in this driver
// that builds a SQL string containing a value it doesn't control -
// vgi_attach()'s generated CREATE VIRTUAL TABLE/DROP TABLE statements
// (extension.cpp) and xConnect/xCreate's generated CREATE TABLE DDL
// passed to sqlite3_declare_vtab (vgi_vtab.cpp). Both embed
// worker-supplied catalog metadata (schema/table/column names) - not
// driver-controlled constants - directly into SQL text; every such value
// must go through one of these first. Found missing on the identifier
// side (SqlQuoteIdentifier) during this driver's Milestone 5 security
// review: a worker whose column or table name contains an embedded `"`
// could otherwise break out of the surrounding identifier and inject
// arbitrary SQL/DDL structure into the attaching connection.
#pragma once

#include <string>

namespace vgi_sqlite {

// Single-quotes a value for embedding as a SQL string literal (a
// CREATE VIRTUAL TABLE module argument), per SQL string-literal escaping
// (double any embedded single quote).
inline std::string SqlQuote(const std::string& s) {
    std::string out = "'";
    for (char c : s) {
        if (c == '\'') out += "''";
        else out += c;
    }
    out += "'";
    return out;
}

// Double-quotes a value for embedding as a SQL identifier (a table or
// column name), per SQL identifier escaping (double any embedded double
// quote) - the identifier analog of SqlQuote above.
inline std::string SqlQuoteIdentifier(const std::string& s) {
    std::string out = "\"";
    for (char c : s) {
        if (c == '"') out += "\"\"";
        else out += c;
    }
    out += "\"";
    return out;
}

}  // namespace vgi_sqlite
