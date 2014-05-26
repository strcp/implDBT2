#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>

#include "structs.h"

GSList *op_list;

enum command strcmd_to_cmd(char *cmd)
{
	if (cmd == NULL)
		return CMD_UNKNOWN;

	if (strcmp(cmd, "WRITE") == 0)
		return CMD_WRITE;
	else if (strcmp(cmd, "READ") == 0)
		return CMD_READ;
	else if (strcmp(cmd, "LOCK-S") == 0)
		return CMD_LOCK_S;
	else if (strcmp(cmd, "LOCK-X") == 0)
		return CMD_LOCK_X;
	else if (strcmp(cmd, "UNLOCK") == 0)
		return CMD_UNLOCK;
	else
		return CMD_UNKNOWN;
}

char *cmd_to_strcmd(enum command cmd)
{
	switch (cmd) {
		case CMD_WRITE:
			return "WRITE";
		case CMD_READ:
			return "READ";
		case CMD_LOCK_S:
			return "LOCK-S";
		case CMD_LOCK_X:
			return "LOCK-X";
		case CMD_UNLOCK:
			return "UNLOCK";
		case CMD_UNKNOWN:
		default:
			return "UNKNOWN";
	}

	return "UNKNOWN";
}

static void parse_command(char *command) {
	char *trs, *cmd, *val;
	long size;
	struct operation *op;

	if (command == NULL)
		return;

	size = strlen(command);
	trs = malloc(size);
	cmd = malloc(size);
	val = malloc(size);
	bzero(trs, size);
	bzero(cmd, size);
	bzero(val, size);

	sscanf(command, "%[^':']:%[^':']:%s", trs, cmd, val);
	op = malloc(sizeof(struct operation));
	op->transaction = atoi(trs);
	op->cmd = strcmd_to_cmd(cmd);
	op->var = strdup(val);

	if (op->cmd != CMD_UNKNOWN)
		op_list = g_slist_append(op_list, op);

	if (trs)
		g_free(trs);
	if (cmd)
		g_free(cmd);
	if (val)
		g_free(val);
}

GSList *parse_operations(char *filename) {
	char *tok, *file_buffer;
	GSList *cmd_list = NULL;

	if (filename == NULL)
		return NULL;

	if (g_file_get_contents(filename, &file_buffer, NULL, NULL) == FALSE) {
		printf("Error reading file.\n");
		return NULL;
	}

	tok = strtok(file_buffer, "\n");
	while (tok != NULL) {
		cmd_list = g_slist_append(cmd_list, g_strdup(tok));
		tok = strtok(NULL, "\n");
	}

	for (int i = 0; i < g_slist_length(cmd_list); i++) {
		char *tmp = g_slist_nth_data(cmd_list, i);
		parse_command(tmp);
	}

	if (file_buffer)
		g_free(file_buffer);


	g_slist_free(cmd_list);

	return op_list;
}
