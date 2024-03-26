#ifndef DUCKDBCONVERT_LIBRARY_H
#define DUCKDBCONVERT_LIBRARY_H

#include <stdio.h>
#include "duckdb.hpp"
#include <iostream>

using std::string;
using std::cout;

extern void send(const char* text);
extern void send(const string text);
extern void send(int64_t intValue);

extern void sendDuckTypeResult(const duckdb::QueryResult &result);
extern const char* typeName(duckdb_type typeId);
extern void sendDuckDataResult(duckdb::QueryResult &result);


/*
 * Used for debug output
 */
#define debug printf

#endif
