//
#include "duckdbconvert.hpp"
#include <stdio.h>
#include <stdlib.h>

using namespace duckdb;

// Forward refs
void sendDuckDataChunk(const DataChunk &chunk);
void sendDuckDataRow(const DataChunk &chunk, idx_t row);

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

void sendDuckDataResult(QueryResult &result)
{
	// Send each chunk in the result
	while (const auto chunk = result.Fetch())
	{
		sendDuckDataChunk(*chunk);
	}
}

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

	// Between the keys and values
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


void sendDuckData::Enum(const Value &value)
{
	const LogicalType type = value.type();
	const auto &name = EnumType::GetValue(value);
	const auto &idx = EnumType::GetPos(type, name);
	send(idx);
}


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


void sendDuckData::Primitive(const Value &value)
{
	send(value.ToSQLString());
}
