#ifndef _PARSER_
#define _PARSER_

#include "structs.h"

enum command strcmd_to_cmd(char *cmd);
char *cmd_to_strcmd(enum command cmd);
GSList *parse_operations(char *filename);

#endif
