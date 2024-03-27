/* ------------------------------------------------------------------------------------
 * Routines to format and send Duckdb values back to postgres.
 *
 * The values are delimited by paranthesis and contain no type information.
 * They must be interpreted in the context of their corresponding types.
 *'
 * eg.    ('a', 'b', 'c', 'Hello', 'World')     LIST<VARCHAR>
 * eg.    ( (1, 2.3)  (4, 5.6), (7, 8.9) )      LIST<STRUCT<a:INTEGER, b:FLOAT>>
 *
 * This current version formats text, but data could also be sent by the binary
 * wire protocal. To be done later.
 */
#include "duckdbconvert.hpp"
#include <stdio.h>
#include <stdlib.h>

using namespace duckdb;

// Forward refs
void sendDuckDataChunk(const DataChunk &chunk);
void sendDuckDataRow(const DataChunk &chunk, idx_t row);

// Routines to format and send data values.
struct sendDuckData {
	static void Any(const Value &value);
	static void Map(const Value &value);
	static void Array(const Value &value);
	static void Struct(const Value &value);
	static void List(const Value &value);
	static void Enum(const Value &value);
	static void Union(const Value &value);
	static void Primitive(const Value &value);
};


/*
 * Send all the data rows from a query.
 */
void sendDuckDataResult(QueryResult &result)
{
	// Send each chunk in the result
	while (const auto chunk = result.Fetch())
	{
		sendDuckDataChunk(*chunk);
	}
}

/*
 * Send all the result rows from the current "chunk:".
 * Duckdb splits the query data into a series of "chunks",
 * where each chunk contains a number of rows.
 *
 */
void sendDuckDataChunk(const DataChunk &chunk)
{
	// Send each row of data from the chunk
	idx_t nrRows = chunk.size();
	for (idx_t row = 0; row < nrRows; row++)
	{
		sendDuckDataRow(chunk, row);
	}
}

/*
 * Send a row of data from the query result.
 */
void sendDuckDataRow(const DataChunk &chunk, idx_t row)
{
	send("(");
	string separator = "";

	// Send each column of data from the row
	idx_t nrColumns = chunk.ColumnCount();
	for (idx_t col = 0; col < nrColumns; col++)
	{
		auto value = chunk.GetValue(col, row);
		sendDuckData::Any(value);

		send(separator);
		separator = ",";
	}

	send(")\n");
}


/*
 * Send an arbitrary Duckdb data value.
 */
void sendDuckData::Any(const Value &value)
{
	LogicalTypeId typeId = value.type().id();
	switch (typeId)
	{
		case LogicalTypeId::STRUCT:
			return sendDuckData::Struct(value);
		case LogicalTypeId::MAP:
			return sendDuckData::Map(value);
		case LogicalTypeId::ARRAY:
		    return sendDuckData::Array(value);
		case LogicalTypeId::ENUM:
			return sendDuckData::Enum(value);
		case LogicalTypeId::LIST:
			return sendDuckData::List(value);
		case LogicalTypeId::UNION:
			return sendDuckData::Union(value);
		default:
			return sendDuckData::Primitive(value);
	}
}

/*
 * Send a duckdb struct value. It will be used to populate a compound type in postgres.
 *     eg.  ('a', 5, 13.7) :: STRUCT<field1:text, field2:integer, field3:float>
 */
void sendDuckData::Struct(const Value &value)
{
	send("(");
	string separator = "";

	const auto &children = StructValue::GetChildren(value);
	idx_t nrChildren = children.size();
	for (const auto &child : StructValue::GetChildren(value))
	{
		send(separator);
		separator = ",";

		sendDuckData::Any(child);
	}
	send(")");
}


/*
 * Send a MAP data value.
 * Postgres doesn't directly implemeht MAP types, but they can be emulated
 * using a STRUCT with an array of keys and an array of values.
 * Duckdb stores the map as an array of pairs, so we "unzip" them
 * before sending to postgres.
 *    eg.    (  ('a', 'b', 'c'), ( [1, 2], [3], [] ) )  :: MAP<text,ARRAY<integer>>
 */
void sendDuckData::Map(const Value &value)
{
	send("(");

	// Get the children - one for each key,value pair
    const auto &children = ListValue::GetChildren(value);

	// Send array of keys
	send("(");
	string separator = "";
	for (const auto &pair: ListValue::GetChildren(value))
	{
		send(separator);
		separator = ",";

		const auto &key = StructValue::GetChildren(pair)[0];
		sendDuckData::Any(key);
	}

	// Punctution between the keys and values
	send(").(");

	// Send array of values
	separator = "";
	for (const auto &pair : ListValue::GetChildren(value))
	{
		send(separator);
		separator = ",";

		const auto &value = StructValue::GetChildren(pair)[1];
		sendDuckData::Any(value);
	}
	send("))");
}

/*
 * Send an array back to postgres.
 *   eg. ('abc', 'def', 'ghi')::ARRAY<text>
 */
void sendDuckData::Array(const Value &value)
{
	send("(");

	string separator = "";
	for (const auto &child : ArrayValue::GetChildren(value))
	{
		send(separator);
		separator = ",";

		sendDuckData::Any(child);
	}
	send(")");
}


/*
 * Send an ENUM value back to postgres.
 * For compactness, it consists of a small integer tag
 * and the corresponding value.
 *    eg.    3   :: ENUM(a,b,c)
 */
void sendDuckData::Enum(const Value &value)
{
	const LogicalType type = value.type();
	const auto &name = EnumType::GetValue(value);
	const auto &idx = EnumType::GetPos(type, name);
	send(idx);
}

/*
 * Send a list of values.
 *     eg.     (1, 2, 3, 5)  :: LIST<integer>
 */
void sendDuckData::List(const Value &value)
{
	send("(");
	string separator = "";

	for (const auto &child : ListValue::GetChildren(value))
	{
		send(separator);
		separator = ",";
		sendDuckData::Any(child);
	}
	send(")");

}


/*
 * Send one of the possible values, along with a tag.
 *     eg.   (2, 'hello')    ::   UNION<a:integer, b:float, c:tesxt>
 */
void sendDuckData::Union(const Value &value)
{
	send("(");
	const auto tag = UnionValue::GetTag(value);
	const auto child = UnionValue::GetValue(value);
	send(tag);
	send(":");
	sendDuckData::Any(child);
	send(")");
}


/*
 * Send a primitive value. Strings will be properly qouted and escaped.
 *   eg     'howdy'   :: varchar
 */
void sendDuckData::Primitive(const Value &value)
{
	send(value.ToSQLString());
}
