/* ---------------------------------------------------------------------------------
 * Routines for sending the data types from a Duckdb query back to postgres.
 *
 * Data types are sent as text because the duckdb server doesn't have a connection
 * into the postgres database. It is unable to lookup postgres oids.
 *
 * Once the textual types are sent to the postgres-duckdb client, the
 * client is able to look up and create corresponding postgres oids,
 * which will be used later to load data into postgres.
 *
 * Duckdb supports compound types which can be nested.
 *    STRUCT<a:type1, b:type2, ..., z:typeN>  - combine fields into a common structure.
 *    MAP<keyType,valueType>                  - map values of one type onto another type
 *    LIST<type>                              - a variable length list
 *    UNION<tag1:type1, tag2:type2, ..., tagN:typeN> - exactly one of the values is not NULL.
 *    ARRAY<type>                             - like a LIST, but closer to SQL fixed size arrays.
 *
 * along with the basic SQL types
 *    TEXT, INTEGER, DECIMAL(7,2), TIME ...
 *
 * Note Duckdb compound types can be nested arbitrarily.
 *   eg.   STRUCT<a:STRUCT<b:integer>,c:text, d:MAP<text,LIST<integer>>>>
 *
 * Postgres doesn't directly support all of the compound types, but they can be emulated
 * using other postgres types.
 *
 * ---------------------------------------------------------------------------------*/
#include "duckdbconvert.hpp"
#include "duckdb.h"
#include <iostream>
#include <format>
#include <string>

using namespace duckdb;

// Functions for formatting and sending textual type strings to the postgres-duckdb client.
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
 * Send the types which are returned by a query.
 * Represented as a pseudo "ROW<>" type which just holds them together.
 *   eg.   ROW<time, integer, STRUCT<a:double>>
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


/*
 * Send the text representation of a recursive Duckdb type.
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
		case LogicalTypeId::DECIMAL:
			return sendDuckType::Decimal(type);
		default:
			return sendDuckType::Primitive(type);
	}
}


/*
 * Format and send a MAP data type.
 *    eg.   MAP<text,integer>
 */
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

/*
 * Format and send a STRUCT data type.
 *     eg.  STRUCT<name1:type1, name2:type2>
 */
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

/*
 * Format and send a LIST data type.
 * Lists can be any size.
 *    eg.  LIST<text>  would contain multiple strings.
 */
void sendDuckType::List(const LogicalType &type)
{
	send("LIST<");
	const LogicalType &childType = ListType::GetChildType(type);
	sendDuckType::Any(childType);
	send(">");
}

/*
 * Format and send an ARRAY type.
 *    eg.  ARRAY<text>
 */
void sendDuckType::Array(const LogicalType &type)
{
	send("ARRAY<");
	const LogicalType &childType = ArrayType::GetChildType(type);
	sendDuckType::Any(childType);
	send(">");
}


/*
 * Format and send an ENUM type.
 *   eg.  ENUM<a,b,c>  declares an enum with three tags.
 */
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


/*
 * Format and send a UNION type.
 * A union contains multiple fields, exactly one of which is not NULL.
 */
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


/*
 * Format and send a DECIMAL type, include width and precision.
 *    eg.  DECIMAL(7,2)
 */
void sendDuckType::Decimal(const LogicalType &type)
{
	uint8_t width = DecimalType::GetWidth(type);
	uint8_t scale = DecimalType::GetScale(type);

	char buffer[120]; // More than big enough
	snprintf(buffer, sizeof(buffer), "DECIMAL(%d,%d)", width, scale);
	send(buffer);
}


/*
 * Format and send other primitive types.
 * They just consist of the type name.
 *    eg.   VARCHAR or BLOB
 */
void sendDuckType::Primitive(const LogicalType &type)
{
	if (type.id() == LogicalTypeId::DECIMAL)
		sendDuckType::Decimal(type);
	else
	    send(LogicalTypeIdToString(type.id()));
}
