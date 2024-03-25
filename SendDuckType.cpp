#include "duckdbconvert.hpp"
#include "duckdb.h"
#include <iostream>
#include <format>
#include <string>

using namespace duckdb;

using std::string;
using std::cout;

void send(const char* text) {cout << text;}
void send(const string text) {cout << text;}
/*
 * Convert a DuckDb type to a Postgres type.
 * Creates and reuses anonymous Postgres types as needed.
 *
 * TODO: how to deal with aliases (already named dependent types).
 * TODO:   maybe ... return a list of types including alias types, bottom up order.
 * TODO:   maybe ... then just use the alias name when referenced in higher types.
 */
void sendDuckType(const LogicalType type) {

    LogicalTypeId typeId = type.id();
    switch (typeId) {

		case LogicalTypeId::MAP: {
			/* Start the MAP by sending prefix */
			send("MAP(");

            /* Send the key type */
            auto keyType = MapType::KeyType(type);
        	sendDuckType(keyType);

			/* Send separator */
			send(", ");

            /* Send the value type */
            auto valueType = MapType::ValueType(type);
            sendDuckType(valueType);

            /* Finish the map by sending suffix */
            send(")");
        } break;

		case LogicalTypeId::STRUCT: {

			// Start the struct by sending a prefix
			send("STRUCT(");

            // Do for each field of the struct
			idx_t nrChildren = StructType::GetChildCount(type);
			for (idx_t i = 0; i < nrChildren; i++) {

				// Send separator, except first time
				if (i != 0)
					send(", ");

				// Send the name of the field
				string name = StructType::GetChildName(type, i);
				send(name);

				// Send the separator between field and type
				send(":");

				/* send the type of the field */
				LogicalType childType = StructType::GetChildType(type, i);
				sendDuckType(childType);
			   }

        	/* Finish by sending a suffix */
        	send(")");

        } break;

		case LogicalTypeId::LIST: {

		   send("LIST(");
		   LogicalType childType = ListType::GetChildType(type);
		   sendDuckType(childType);
		   send(")");
       } break;

	   case LogicalTypeId::UNION: {

		   send("UNION(");

		   // Do for each member
		   idx_t nrChildren = duckdb::UnionType::GetMemberCount(type);
		   for (idx_t i = 0; i < nrChildren; i++) {

				if (i > 0)
					send(", ");

                // Send name of the member
				string name = duckdb::UnionType::GetMemberName(type, i);
				send(name);
				send(":");

                // Send the type of the membe
       			LogicalType childType = duckdb::UnionType::GetMemberType(type, i);
       			sendDuckType(childType);
	        }

            // Complete the UNION string
            send(")");

       } break;

	   case LogicalTypeId::DECIMAL: {
           uint8_t width = DecimalType::GetWidth(type);
           uint8_t scale = DecimalType::GetScale(type);
		   char text[120];
		   snprintf(text, sizeof(text), "DECIMAL(%d,%d)", width, scale);
           send(text);
       } break;

      default:
		  send(LogicalTypeIdToString(typeId));
		  break;
    }
}
