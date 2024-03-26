//
#include "duckdbconvert.hpp"
#include <stdio.h>
#include <stdlib.h>

using namespace duckdb;

// Forward refs
void sendDuckData(const DataChunk &chunk);
void sendDuckData(const DataChunk &chunk, idx_t row);

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

void sendDuckData(QueryResult &result)
{
	send("RESULT_DATA(");

	// Send each chunk in the result
	while (const auto chunk = result.Fetch())
	{
		sendDuckData(*chunk);
		send(", ");
	}

	send(")");
}

void sendDuckData(const DataChunk &chunk)
{
	// Send the prefix
	send("CHUNK_DATA(");

	// Send each row of data from the chunk
	idx_t nrRows = chunk.size();
	for (idx_t row = 0; row < nrRows; row++)
	{
		sendDuckData(chunk, row);
		send(", ");
	}

	// Send the suffix
	send(")");
}


void sendDuckData(const DataChunk &chunk, idx_t row)
{
	// Send each column of data from the row
	idx_t nrColumns = chunk.ColumnCount();
	send("ROW_DATA(");
	for (idx_t col = 0; col < nrColumns; col++)
	{
		auto value = chunk.GetValue(col, row);
		sendDuckData::Any(value);
		send(", ");
	}
	send(")");
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
	send("STRUCT_DATA(");
	for (const auto child : StructValue::GetChildren(value))
	{
		sendDuckData::Any(child);
		send(", ");
	}
	send(")");
}


void sendDuckData::Map(const Value &value)
{
	send("MAP_DATA(");

	// Send array of keys
	send("ARRAY_DATA(");
	for (const auto &pair: ListValue::GetChildren(value))
	{
		const auto &key = StructValue::GetChildren(pair)[0];
		sendDuckData::Any(key);
		send(", ");
	}
	send(")");
	send(", ");

	// Send array of values
	send("ARRAY_DATA(");
	for (const auto &pair : ListValue::GetChildren(value))
	{
		const auto &value = StructValue::GetChildren(pair)[1];
		sendDuckData::Any(value);
		send(", ");
	}
	send(")");

	// Send the Map suffix
	send(")");
}

void sendDuckData::Array(const Value &value)
{
	send("ARRAY_DATA(");
	const char *separator = "";
	for (const auto &child : ArrayValue::GetChildren(value))
	{
		send(separator);
		sendDuckData::Any(child);
		separator = ", ";
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
	send("LIST_DATA(");
	const char *separator = "";
	for (const auto &child : ListValue::GetChildren(value))
	{
		send(separator);
		sendDuckData::Any(child);
		separator = ", ";
	}
	send(")");

}
void sendDuckData::Union(const Value &value)
{
	send("UNION_DATA(");
	const auto tag = UnionValue::GetTag(value);
	const auto child = UnionValue::GetValue(value);
	send(tag);
	send(":");
	sendDuckData::Any(child);
	send(")");
}


void sendDuckData::Primitive(const Value &value)
{
	send(value.ToString());
}
