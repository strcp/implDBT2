#ifndef _STRUCTS_
#define _STRUCTS_

#include <stdio.h>

enum command
{
	CMD_WRITE = 0,
	CMD_READ,
	CMD_LOCK_S,
	CMD_LOCK_X,
	CMD_UNLOCK,
	CMD_UNKNOWN
};

struct operation
{
	int transaction;
	enum command cmd;
	char *var;
};

GSList *op_list;
GSList *block_list;

#endif
