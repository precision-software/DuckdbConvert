#include "duckdbconvert.hpp"
#include "duckdb.h"
#include <iostream>
#include <format>
#include <string>

using namespace duckdb;

struct sendDuckType
{
	static void Any(const LogicalType &type);
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
 *
 * Would be nice if we could add decorative method "send()" to LogicalType
 * like can be done in Scala.
 */

void sendDuckType::Any(const LogicalType &type)
{
	switch (type.id())
	{
		case LogicalTypeId::MAP:
			return sendDuckType::Map(type);
		case LogicalTypeId::STRUCT:
			return sendDuckType::Struct(type);
		case LogicalTypeId::UNION:
			return sendDuckType::Union(type);
		case LogicalTypeId::LIST:
			return sendDuckType::List(type);
		case LogicalTypeId::ARRAY:
			return sendDuckType::Array(type);
		case LogicalTypeId::ENUM:
			return sendDuckType::Enum(type);
		default:
			return sendDuckType::Primitive(type);
	}
}


/*
 * Send the types which exist in the query result.
 */
void sendDuckTypeResult(const QueryResult &result)
{
	send("ROW<");
	string separator = "";
	idx_t nrCols = result.types.size();
	for (int col = 0; col < nrCols; col++)
	{
		send(separator);
		separator = ",";

		sendDuckType::Any(result.types[col]);
	}
	send(">\n");
}


void sendDuckType::Map(const LogicalType &type)
{
	// Start the MAP by sending prefix
	send("MAP<");

	// Send the key type
	const auto keyType = MapType::KeyType(type);
	sendDuckType::Any(keyType);

	// Send separator between key and value types
	send(",");

	// Send the value type
	auto valueType = MapType::ValueType(type);
	sendDuckType::Any(valueType);

	/* Finish the map by sending suffix */
	send(">");
}


void sendDuckType::Struct(const LogicalType &type)
{
	// Start the struct by sending a prefix
	send("STRUCT<");
	string separator = "";

	// Do for each field of the struct
	idx_t nrChildren = StructType::GetChildCount(type);
	for (idx_t i = 0; i < nrChildren; i++)
	{
		send(separator);
		separator = ",";

		// Send the name of the field
		const string &name = StructType::GetChildName(type, i);
		send(name);

		// Send the separator between field and type
		if (name != "")
		    send(":");

		/* send the type of the field */
		LogicalType childType = StructType::GetChildType(type, i);
		sendDuckType::Any(childType);
	}

	/* Finish by sending a suffix */
	send(">");
}

void sendDuckType::List(const LogicalType &type)
{
	send("LIST<");
	const LogicalType &childType = ListType::GetChildType(type);
	sendDuckType::Any(childType);
	send(">");
}

void sendDuckType::Array(const LogicalType &type)
{
	send("ARRAY<");
	const LogicalType &childType = ArrayType::GetChildType(type);
	sendDuckType::Any(childType);
	send(">");
}

void sendDuckType::Enum(const LogicalType &type)
{
	send("ENUM<");
	string separator = "";

	/* For each enum value */
	idx_t nrChildren = EnumType::  GetSize(type);
	for (idx_t i = 0; i < nrChildren; i++)
	{
		send(separator);
		separator = ",";

		const string_t &tag = EnumType::GetString(type, i);
		send(tag.GetString());
	}

	send(">");
}


void sendDuckType::Union(const LogicalType &type)
{
	send("UNION<");
	string separator = "";

	// Do for each member
	idx_t nrChildren = duckdb::UnionType ::GetMemberCount(type);
	for (idx_t i = 0; i < nrChildren; i++)
	{
		send(separator);
		separator = ",";

		// Send name of the member
		const string &name = duckdb::UnionType::GetMemberName(type, i);
		send(name);

		// Send separator between name and type
		if (name != "")
		    send(":");

		// Send the type of the member
		const LogicalType &childType = duckdb::UnionType::GetMemberType(type, i);
		sendDuckType::Any(childType);
	}

	send(">");
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
	if (type.id() == LogicalTypeId::DECIMAL)
		sendDuckType::Decimal(type);
	else
	    send(LogicalTypeIdToString(type.id()));
}
