/*
 * Some quick and dirty "send" routines. Would normally send to the socket, but for now, just do stdout.
 */

#include <string>
#include <iostream>



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
