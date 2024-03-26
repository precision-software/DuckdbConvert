//

#include <string>
#include <iostream>
typedef uint64_t idx_t;


void send(const char* text)
{
	static idx_t level = 0;

	idx_t len = strlen(text);
	if (len > 0 && text[len-1] == '(')
	{
		// Indent new line.
		std::cout << "\n";
		for (idx_t i = 0; i < level; i++)
			std::cout << "  ";
		level++;

		std::cout << text;
	}
	else if (len > 0 && text[len-1] == ')')
	{
		// Unindent old line
		std::cout << text;
		if (level > 0)
			level--;
	}

	else
		std::cout << text;

	std::cout.flush();
}

void send(const std::string text)
{
	send(text.c_str());
}

void send(int64_t intValue)
{
	send(std::to_string(intValue));
}
