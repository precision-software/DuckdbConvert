/*
 *
 */

#include "duckdb.hpp"
#include "duckdbconvert.hpp"

using namespace duckdb;

/*
 * A quick test program to explore converting DuckDb types to Postgres types.
 * This version scans the DuckDb logical type tree rather than parsing the type strings.
 */
int main()
{
	DuckDB db(nullptr);
	Connection con(db);

	/* An SQL statement with xxxx types */
	const char *sql =
		"SELECT {'birds':"
		"			{'yes': 'duck', 'maybe': 'goose', 'huh': NULL, 'no': 'heron'},"
		"		'aliens':"
		"			NULL,"
		"		'amphibians':"
		"			{'yes':'frog', 'maybe': 'salamander', 'huh': 'dragon', 'no':'toad'},"
		"        'row': (1, 107.66, 3.5, 17),"
		"        'map': MAP(['abc', 'def', 'efg'], [1.23, 4.56, 7.99]::float[])"
		"}";

	auto result = con.SendQuery(sql);

	//sendDuckType(*result);
	sendDuckData(*result);



	return 0;
}
