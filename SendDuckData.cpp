#include "duckdbconvert.hpp"
#include <stdio.h>
#include <stdlib.h>



void sendDuckData(duckdb::DataChunk *chunk)
{

	debug("sendDuckChunk\n");
	idx_t nrColumns = chunk->ColumnCount();
	idx_t nrRows = chunk->size();

	for (idx_t  row = 0; row < nrRows; row++)
	    for (idx_t col = 0; col < nrColumns; col++)
			sendDuckData(chunk);
}
