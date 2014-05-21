#ifndef _EXEC_
#define _EXEC_

#include <glib.h>

void exec_operations(GSList *op_list);
void dump_operation(struct operation *op);

#endif
