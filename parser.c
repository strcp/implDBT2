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
	FILE *fp;
	char *tok, *file_buffer;
	long size;
	char **cmd_list;
	int n = 0;

	if (filename == NULL)
		return NULL;

	fp = fopen(filename,"r");

	if (!fp) {
		perror("fopen");
		return NULL;
	}

	fseek(fp, 0L, SEEK_END);
	size = ftell(fp);
	fseek(fp, 0L, SEEK_SET);

	file_buffer = malloc(size * sizeof(char));
	fread(file_buffer, size, 1, fp);
	fclose(fp);

	cmd_list = NULL;
	tok = strtok(file_buffer, "\n");
	while (tok != NULL) {
		n++;
		if (cmd_list == NULL)
			cmd_list = malloc(n * sizeof(char *));
		else
			cmd_list = realloc(cmd_list, n * sizeof(char *));

		cmd_list[n - 1] = strdup(tok);
		tok = strtok(NULL, "\n");
	}

	for (int i = 0; i < n; i++) {
		parse_command(cmd_list[i]);
	}

	if (file_buffer)
		free(file_buffer);


	for (int i = 0; i < n; i++) {
		if (cmd_list[i] != NULL)
			free(cmd_list[i]);
	}

	if (cmd_list != NULL)
		free(cmd_list);

	return op_list;
}
