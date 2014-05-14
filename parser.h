#ifndef _PARSER_
#define _PARSER_

#include "structs.h"

enum command strcmd_to_cmd(char *cmd);
char *cmd_to_strcmd(enum command cmd);
void parse_command(char *command);
void parse_operations(char *filename);

#endif
