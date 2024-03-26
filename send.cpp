/*
 * Some quick and dirty "send" routines. Would normally send to the socket, but for now, just do stdout.
 */

#include <string>
#include <iostream>
typedef uint64_t idx_t;


void send(const char* text)
{
	std::cout << text;
}

void send(const std::string text)
{
	send(text.c_str());
}

void send(int64_t intValue)
{
	send(std::to_string(intValue));
}


/*
 * Quote the string. (do escapes later)
 */
std::string quoted(std::string orig)
{
	return "'" + orig + "'";
}

/*
 * Send binary data (later)
 */
std::string binary(uint8_t *data, idx_t len)
{
	abort();
}
