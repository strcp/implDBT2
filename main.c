#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>

#include "structs.h"
#include "parser.h"


void dump_operation(struct operation *op) {
	if (op == NULL)
		return;

	printf("[%d] [%s] [%s]\n", op->transaction, cmd_to_strcmd(op->cmd), op->var);
}

int main(int argc, char **argv) {
	op_list = NULL;
	block_list = NULL;

	if (argc < 2) {
		printf("ERROR!\n");
		return 0;
	}

	printf("Executing \"%s\"\n", argv[1]);
	parse_operations(argv[1]);

	printf("op length = %d\n", g_slist_length(op_list));
	g_slist_foreach(op_list, (GFunc)dump_operation, NULL);
}
