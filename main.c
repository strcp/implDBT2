#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>

#include "structs.h"
#include "parser.h"
#include "exec.h"

void operation_clean(struct operation *op) {
	if (op == NULL)
		return;

	if (op->var != NULL)
		g_free(op->var);

	g_free(op);
}

void operations_cleanup(GSList *op_list) {
	if (op_list == NULL)
		return;

	g_slist_free_full(op_list, (GDestroyNotify)operation_clean);
	op_list = NULL;
}

int main(int argc, char **argv) {
	GSList *op_list;

	op_list = NULL;

	if (argc < 2) {
		printf("ERROR!\n");
		return 0;
	}

	printf("Parsing \"%s\"\n", argv[1]);
	op_list = parse_operations(argv[1]);
	if (op_list == NULL) {
		printf("Error parsing file.\n");
		return 0;
	}

	printf("%d operations found\n", g_slist_length(op_list));
	g_slist_foreach(op_list, (GFunc)dump_operation, NULL);

	printf("Executing \"%s\"\n", argv[1]);
	exec_operations(op_list);

	printf("Cleaning \"%s\"\n", argv[1]);
	operations_cleanup(op_list);
}
