#ifndef DUCKDBCONVERT_LIBRARY_H
#define DUCKDBCONVERT_LIBRARY_H

#include <stdio.h>
#include "duckdb.hpp"

extern void sendDuckType(duckdb::LogicalType type);
extern const char* typeName(duckdb_type typeId);
extern void sendDuckData(duckdb::DataChunk *chunk);


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
