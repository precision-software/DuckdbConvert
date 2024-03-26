//
#include "duckdbconvert.hpp"
#include <stdio.h>
#include <stdlib.h>

using namespace duckdb;


struct sendDuckData {
	static void Result(QueryResult &result);
	static void Chunk(const DataChunk &chunk);
	static void Row(const DataChunk &chunk, idx_t row);

	static void Val(const Value &value);
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
	sendDuckData::Result(result);
}


void sendDuckData::Result(QueryResult &result)
{
	// Send each chunk in the result
	while (const auto chunk = result.Fetch())
	{
		send("RESULT_DATA(");
		sendDuckData::Chunk(*chunk);
		send(")");
	}
}

void sendDuckData::Chunk(const DataChunk &chunk)
{
	// Send each row of data from the chunk
	idx_t nrRows = chunk.size();
	for (idx_t row = 0; row < nrRows; row++)
	{
		send("CHUNK_DATA(");
		if (row != 0)
			send(", ");
		sendDuckData::Row(chunk, row);
		send(")");
	}
}


void sendDuckData::Row(const DataChunk &chunk, idx_t row)
{
	// Send each column of data from the row
	idx_t nrColumns = chunk.ColumnCount();
	send("ROW_DATA(");
	for (idx_t col = 0; col < nrColumns; col++)
	{
		if (col != 0)
			send(", ");
		auto value = chunk.GetValue(col, row);
		sendDuckData::Val(value);
	}
	send(")");
}


void sendDuckData::Val(const Value &value)
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
	const char *separator = "";
	for (const auto child : StructValue::GetChildren(value))
	{
		send(separator);
		sendDuckData::Val(child);
		separator = ", ";
	}
	send(")");
}


void sendDuckData::Map(const Value &value)
{
	send("MAP_DATA(");

	const char *separator = "";
	for (const auto &pair: ListValue::GetChildren(value))
	{
		send(separator);
		sendDuckData::Val(pair);

        separator = ", ";
	}

	send(")");



}

void sendDuckData::Array(const Value &value)
{
	send("ARRAY_DATA(");
	const char *separator = "";
	for (const auto &child : ArrayValue::GetChildren(value))
	{
		send(separator);
		sendDuckData::Val(child);
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
		sendDuckData::Val(child);
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
	sendDuckData::Val(child);
	send(")");
}


void sendDuckData::Primitive(const Value &value)
{
	send(value.ToString());
}
