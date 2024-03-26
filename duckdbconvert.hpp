#ifndef DUCKDBCONVERT_LIBRARY_H
#define DUCKDBCONVERT_LIBRARY_H

#include <stdio.h>
#include "duckdb.hpp"
#include <iostream>

using std::string;
using std::cout;

extern void send(const char* text);
extern void send(const string text);

extern void sendDuckType(const duckdb::QueryResult &result);
extern const char* typeName(duckdb_type typeId);
extern void sendDuckData(duckdb::QueryResult &result);


/*
 * Used for debug output
 */
#define debug printf
static int level = 0;
static void indent()
{
	for (int i=0; i<level; i++)
		debug("   ");
}

#endif
