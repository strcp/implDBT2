#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>

#include "structs.h"
#include "parser.h"

enum var_lock_status
{
	VAR_X_LOCK = 0,
	VAR_S_LOCK,
	VAR_UNKNOWN
};

enum op_stats
{
	OP_ERROR = 0,
	OP_WAIT,
	OP_OK,
	OP_UNKNOWN
};


GSList *unlocked_transaction_list;
GHashTable *wait_table;
GHashTable *lock_s_table;
GHashTable *lock_x_table;

void dump_operation(struct operation *op) {
	if (op == NULL)
		return;

	printf("[%d] [%s] [%s]\n", op->transaction, cmd_to_strcmd(op->cmd), op->var);
}

static void dump_table_list(gpointer key, gpointer value, gpointer userdata)
{
	if (key == NULL)
		return;

	GSList *transactions = (GSList *)value;

	printf("\t%s:\n", key);
	for (int i = 0; i < g_slist_length(transactions); i++) {
		int *t = (int *)g_slist_nth_data(transactions, i);
		printf("\t\t%d\n", *t);
	}
}

static void dump_lock_s_table() {
	if (lock_s_table == NULL)
		return;

	printf("S LOCK TABLE:\n");
	g_hash_table_foreach(lock_s_table, dump_table_list, NULL);
	printf("\n");
}

static void dump_lock_x_table() {
	if (lock_x_table == NULL)
		return;

	printf("X LOCK TABLE:\n");
	g_hash_table_foreach(lock_x_table, dump_table_list, NULL);
	printf("\n");
}

static void dump_wait_table() {
	printf("WAIT TABLE:\n");
	g_hash_table_foreach(wait_table, dump_table_list, NULL);
	printf("\n");
}

static int is_transaction_waiting(struct operation *op) {
	if (op == NULL)
		return -1;

	char *strtrans = g_strdup_printf("%d", op->transaction);
	if (strtrans == NULL)
		return -1;

	GSList *op_list = g_hash_table_lookup(wait_table, strtrans);
	if (op_list)
		return 1;

	return 0;
}

static void add_transaction_to_wait(struct operation *op) {
	if (op == NULL)
		return;

	char *strtrans = g_strdup_printf("%d", op->transaction);
	if (strtrans == NULL)
		return;

	GSList *op_list = g_hash_table_lookup(wait_table, strtrans);
	op_list = g_slist_append(op_list, op);
	g_hash_table_insert(wait_table, strtrans, op_list);
}

static enum op_stats can_s_lock(char *var, int trans) {
	GSList *t = NULL;
	char *strtrans;

	// Checando se a variável já foi bloqueada exclusivamente por alguma
	// transição.
	t = g_hash_table_lookup(lock_x_table, var);
	if (t != NULL)
		return OP_WAIT;

	strtrans = g_strdup_printf("%d", trans);
	if (strtrans == NULL)
		return OP_UNKNOWN;

	// Checando se a transição já fez unlocks
	t = g_slist_find(unlocked_transaction_list, strtrans);
	g_free(strtrans);
	if (t != NULL)
		return OP_ERROR;

	return OP_OK;
}

static enum op_stats can_x_lock(char *var, int trans) {
	GSList *t = NULL;
	char *strtrans;
	struct operation *op;

	// Checando se a variável já foi bloqueada exclusivamente por alguma
	// outra transição.
	t = g_hash_table_lookup(lock_x_table, var);
	if (t != NULL) {
		for (int i = 0; i < g_slist_length(t); i++) {
			op = g_slist_nth_data(t, i);
			if (op->transaction == trans)
				continue;
			else
				// FIXME
				return OP_WAIT;
		}
	}

	// Checando se a variável já foi bloqueada de maneira compartilhada por
	// alguma outra transição.
	t = g_hash_table_lookup(lock_s_table, var);
	if (t != NULL) {
		for (int i = 0; i < g_slist_length(t); i++) {
			op = g_slist_nth_data(t, i);
			if (op->transaction == trans)
				continue;
			else
				return OP_ERROR;
		}
	}

	strtrans = g_strdup_printf("%d", trans);
	if (strtrans == NULL)
		return OP_UNKNOWN;

	// Checando se a transição já fez unlocks
	t = g_slist_find(unlocked_transaction_list, strtrans);
	g_free(strtrans);
	if (t != NULL)
		return OP_ERROR;

	return OP_OK;
}

static enum op_stats can_write(char *var, int trans) {
	// Checando se a variável já foi bloqueada exclusivamente pela transição.
	t = g_hash_table_lookup(lock_x_table, var);
	if (t != NULL) {
		for (int i = 0; i < g_slist_length(t); i++) {
			op = g_slist_nth_data(t, i);
			if (op->transaction == trans)
				return OP_OK;
		}
	}

	return OP_WAIT;
}

static enum op_stats can_read(char *var, int trans) {
	if (can_write(var, trans) == OP_OK)
		return OP_OK;

	// Checando se a variável já foi bloqueada de maneira compartilhada
	// pela transição.
	t = g_hash_table_lookup(lock_x_table, var);
	if (t != NULL) {
		for (int i = 0; i < g_slist_length(t); i++) {
			op = g_slist_nth_data(t, i);
			if (op->transaction == trans)
				continue;
			else
				// FIXME
				return OP_WAIT;
		}
	}

	return OP_OK;
}

static void unlock_variable(struct operation *op) {
	GSList *t = NULL;
	char *strtrans;
	struct operation *tmp_op;

	t = g_hash_table_lookup(lock_x_table, op->var);
	if (t != NULL) {
		for (int i = 0; i < g_slist_length(t); i++) {
			tmp_op = g_slist_nth_data(t, i);
			if (op->transaction == tmp_op->transaction) {
				t = g_slist_remove(t, tmp_op);
				g_hash_table_insert(lock_x_table, op->var, t);
				if (g_slist_length(t) == 0) {
					g_hash_table_remove(lock_x_table, op->var);
					g_slist_free(t);
					t = NULL;
				}
				break;
			}
		}
	}

	if (g_hash_table_size(lock_x_table) == 0) {
		g_hash_table_destroy(lock_x_table);
		lock_x_table = g_hash_table_new(g_str_hash, g_str_equal);
	}

	t = g_hash_table_lookup(lock_s_table, op->var);
	if (t != NULL) {
		for (int i = 0; i < g_slist_length(t); i++) {
			tmp_op = g_slist_nth_data(t, i);
			if (op->transaction == tmp_op->transaction) {
				t = g_slist_remove(t, tmp_op);
				g_hash_table_insert(lock_s_table, op->var, t);
				if (g_slist_length(t) == 0) {
					g_hash_table_remove(lock_s_table, op->var);
					g_slist_free(t);
					t = NULL;
				}
				break;
			}
		}
	}
	if (g_hash_table_size(lock_s_table) == 0) {
		g_hash_table_destroy(lock_s_table);
		lock_s_table = g_hash_table_new(g_str_hash, g_str_equal);
	}
}

static void x_lock(struct operation *op) {
	GSList *t = NULL;
	struct operation *tmp_op;

	t = g_hash_table_lookup(lock_s_table, op->var);
	if (t != NULL) {
		for (int i = 0; i < g_slist_length(t); i++) {
			tmp_op = g_slist_nth_data(t, i);
			if (op->transaction == tmp_op->transaction) {
				t = g_slist_remove(t, tmp_op);
				if (g_slist_length(t) == 0) {
					g_hash_table_remove(lock_s_table, op->var);
					g_slist_free(t);
					t = NULL;
				}
				break;
			}
		}
	}

	t = g_hash_table_lookup(lock_x_table, op->var);
	t = g_slist_append(t, &(op->transaction));
	g_hash_table_insert(lock_x_table, op->var, t);
}

static enum op_stats operation_status(struct operation *op) {
	GSList *t = NULL;
	enum op_stats stats;

	if (op == NULL)
		return OP_UNKNOWN;

	switch (op->cmd) {
		case CMD_WRITE:
			break;
		case CMD_READ:
			break;
		case CMD_LOCK_S:
			stats = can_s_lock(op->var, op->transaction);
			if (stats == OP_OK) {
				t = g_hash_table_lookup(lock_s_table, op->var);
				t = g_slist_append(t, &(op->transaction));
				g_hash_table_insert(lock_s_table, op->var, t);
			}
			else
				return stats;
			break;
		case CMD_LOCK_X:
			stats = can_x_lock(op->var, op->transaction);
			if (stats == OP_OK)
				x_lock(op);
			else {
				printf("returning: %d\n", stats);
				return stats;
			}
			break;
		case CMD_UNLOCK:
			unlock_variable(op);
			break;
		case CMD_UNKNOWN:
		default:
			break;
	}


	return OP_OK;
}

static check_waiting_operation() {
	for (int i = 0; i < g_slist_length(op_list); i++) {
		op = g_slist_nth_data(op_list, i);

	}

	void exec_operations(GSList *op_list) {
		struct operation *op;
		enum op_stats stats;

		if (op_list == NULL)
			return;

		lock_s_table = g_hash_table_new(g_str_hash, g_str_equal);
		lock_x_table = g_hash_table_new(g_str_hash, g_str_equal);
		wait_table = g_hash_table_new(g_str_hash, g_str_equal);

		for (int i = 0; i < g_slist_length(op_list); i++) {
			op = g_slist_nth_data(op_list, i);
			stats = operation_status(op);
			if ((stats == OP_WAIT) || (is_transaction_waiting(op)))
				add_transaction_to_wait(op);
			else if (stats != OP_OK) {
				printf("ERROR:\n");
				dump_operation(op);
			}
		}

		dump_lock_s_table();
		dump_lock_x_table();
		dump_wait_table();

		g_hash_table_destroy(lock_s_table);
		g_hash_table_destroy(lock_x_table);
		g_hash_table_destroy(wait_table);
	}
