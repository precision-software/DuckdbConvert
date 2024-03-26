#include "duckdbconvert.hpp"
#include "duckdb.h"
#include <iostream>
#include <format>
#include <string>

using namespace duckdb;

struct sendDuckType
{
	static void Val(const LogicalType &type);
	static void Map(const LogicalType &type);
	static void Union(const LogicalType &type);
	static void Struct(const LogicalType &type);
	static void List(const LogicalType &type);
	static void Array(const LogicalType &type);
	static void Enum(const LogicalType &type);
	static void Primitive(const LogicalType &type);
	static void Decimal(const LogicalType &type);
};

/*
 * Convert a DuckDb type to a Postgres type.
 * Creates and reuses anonymous Postgres types as needed.
 *
 * TODO: how to deal with aliases (already named dependent types).
 * TODO:   maybe ... return a list of types including alias types, bottom up order.
 * TODO:   maybe ... then just use the alias name when referenced in higher types.
 */
void sendDuckType(const LogicalType &type)
{
	sendDuckType::Val(type);
}


void sendDuckType::Val(const LogicalType &type)
{
	switch (type.id())
	{
		case LogicalTypeId::MAP: return sendDuckType::Map(type);
		case LogicalTypeId::STRUCT: return sendDuckType::Struct(type);
		case LogicalTypeId::UNION: return sendDuckType::Union(type);
		case LogicalTypeId::LIST: return sendDuckType::List(type);
		case LogicalTypeId::ARRAY: return sendDuckType::Array(type);
		case LogicalTypeId::ENUM: return sendDuckType::Enum(type);
		default: return sendDuckType::Primitive(type);
	}
}



void sendDuckType::Map(const LogicalType &type)
{
	// Start the MAP by sending prefix
	send("MAP(");

	// Send the key type
	const auto keyType = MapType::KeyType(type);
	sendDuckType::Val(keyType);

	// Send separator
	send(", ");

	// Send the value type
	auto valueType = MapType::ValueType(type);
	sendDuckType::Val(valueType);

	/* Finish the map by sending suffix */
	send(")");
}


void sendDuckType::Struct(const LogicalType &type)
{
	// Start the struct by sending a prefix
	send("STRUCT(");
	const char *separator = "";

	// Do for each field of the struct
	idx_t nrChildren = StructType::GetChildCount(type);
	for (idx_t i = 0; i < nrChildren; i++)
	{
		// Send separator, except first time
		send(separator);

		// Send the name of the field
		const string &name = StructType::GetChildName(type, i);
		send(name);

		// Send the separator between field and type
		send(":");

		/* send the type of the field */
		LogicalType childType = StructType::GetChildType(type, i);
		sendDuckType::Val(childType);

		separator = ", ";
	}

	/* Finish by sending a suffix */
	send(")");
}

void sendDuckType::List(const LogicalType &type)
{
	send("LIST(");
	const LogicalType &childType = ListType::GetChildType(type);
	sendDuckType::Val(childType);
	send(")");
}

void sendDuckType::Array(const LogicalType &type)
{
	send("ARRAY(");
	const LogicalType &childType = ArrayType::GetChildType(type);
	sendDuckType::Val(childType);
	send(")");
}

void sendDuckType::Enum(const LogicalType &type)
{
	send("ENUM(");
	const char *separator = "";

	idx_t nrChildren = EnumType::GetSize(type);
	for (idx_t i = 0; i < nrChildren; i++)
	{
		send(separator);

		const string_t &tag = EnumType::GetString(type, i);
		send(tag.GetString());

		separator = ", ";
	}

	send(")");
}


void sendDuckType::Union(const LogicalType &type)
{
	send("UNION(");

	const char *separator = "";

	// Do for each member
	idx_t nrChildren = duckdb::UnionType::GetMemberCount(type);
	for (idx_t i = 0; i < nrChildren; i++)
	{

		send(separator);

		// Send name of the member
		const string &name = duckdb::UnionType::GetMemberName(type, i);
		send(name);
		send(":");

		// Send the type of the member
		const LogicalType &childType = duckdb::UnionType::GetMemberType(type, i);
		sendDuckType::Val(childType);

		// Complete the UNION string
		send(")");

		separator = ", ";
	}

	send(")");
}

void sendDuckType::Decimal(const LogicalType &type)
{
	uint8_t width = DecimalType::GetWidth(type);
	uint8_t scale = DecimalType::GetScale(type);

	char buffer[120]; // More than big enough
	snprintf(buffer, sizeof(buffer), "DECIMAL(%d,%d)", width, scale);
	send(buffer);
}

void sendDuckType::Primitive(const LogicalType &type)
{
	send(LogicalTypeIdToString(type.id()));
}


void sendDuckType(const QueryResult &result)
{
	send("TYPES(\n");
	for (const auto &type: result.types)
	{
		sendDuckType::Val(type);
		send(", \n");
	}
	send(")\n");
}
