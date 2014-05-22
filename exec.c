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
	struct operation *op;
	char *table_type;

	if (key == NULL)
		return;

	GSList *transactions = (GSList *)value;
	if (transactions == NULL)
		return;

	table_type = (char *)userdata;

	printf("\t%s:\n", key);
	for (int i = 0; i < g_slist_length(transactions); i++) {
		if (g_strcmp0(table_type, "wait_table") == 0) {
			op = (struct operation *)g_slist_nth_data(transactions, i);
			if (op != NULL) {
				printf("\t");
				dump_operation(op);
			}
		}
		else {
			int *t = (int *)g_slist_nth_data(transactions, i);
			printf("\t\t%d\n", *t);
		}
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
	if (wait_table == NULL)
		return;

	printf("WAIT TABLE:\n");
	g_hash_table_foreach(wait_table, dump_table_list, "wait_table");
	printf("\n");
}

static int is_transaction_waiting(struct operation *op) {
	char *strtrans = g_strdup_printf("%d", op->transaction);
	struct operation *tmp_op;

	if (op == NULL)
		return 0;

	GSList *op_list = g_hash_table_lookup(wait_table, strtrans);

	if (op_list != NULL) {
		printf("OK\n");
		for (int i = 0; i < g_slist_length(op_list); i++) {
			tmp_op = g_slist_nth_data(op_list, i);
			printf("DBG: ");
			dump_operation(tmp_op);
			if (((tmp_op->cmd == CMD_LOCK_S) || (tmp_op->cmd == CMD_LOCK_X)) && (g_strcmp0(op->var, tmp_op->var) == 0))
				return 1;
		}
	}
	else {
		printf("NOK\n");
		dump_wait_table();
	}

	return 0;
}

static void add_transaction_to_wait(struct operation *op) {
	if (op == NULL)
		return;

	char *strtrans = g_strdup_printf("%d", op->transaction);
	if (strtrans == NULL)
		return;

	dump_operation(op);
	GSList *op_list = g_hash_table_lookup(wait_table, strtrans);
	op_list = g_slist_append(op_list, op);
	g_hash_table_insert(wait_table, strtrans, op_list);
}

static void remove_transaction_from_wait(struct operation *op) {
	if (op == NULL)
		return;

	char *strtrans = g_strdup_printf("%d", op->transaction);
	if (strtrans == NULL)
		return;

	GSList *op_list = g_hash_table_lookup(wait_table, strtrans);
	op_list = g_slist_remove(op_list, op);
//	if (g_slist_length(op_list) == 0)
//		g_hash_table_remove(wait_table, strtrans);
//	else
		g_hash_table_insert(wait_table, strtrans, op_list);

/*	if (g_hash_table_size(wait_table) == 0) {
		g_hash_table_destroy(wait_table);
		wait_table = NULL;
	}*/
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
				return OP_WAIT;
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

static enum op_stats can_write(struct operation *op) {
	GSList *t;
	struct operation *tmpop;
	char *var;
	int trans;

	if (op == NULL) {
		return OP_ERROR;
	}

	if (is_transaction_waiting(op))
		return OP_WAIT;

	var = op->var;
	trans = op->transaction;

	// Checando se a variável já foi bloqueada exclusivamente pela transição.
	t = g_hash_table_lookup(lock_x_table, var);
	if (t != NULL) {
		for (int i = 0; i < g_slist_length(t); i++) {
			tmpop = g_slist_nth_data(t, i);
			if (tmpop->transaction == trans)
				return OP_OK;
		}
	}

	return OP_ERROR;
}

static enum op_stats can_read(struct operation *op) {
	GSList *t;
	struct operation *tmpop;
	enum op_stats stats;
	char *var;
	int trans;

	if (op == NULL)
		return OP_ERROR;

	stats = can_write(op);

	if (stats == OP_OK)
		return OP_OK;
	else if (stats == OP_WAIT)
		return OP_WAIT;

	var = op->var;
	trans = op->transaction;

	// Checando se a variável já foi bloqueada de maneira compartilhada
	// pela transição.
	t = g_hash_table_lookup(lock_s_table, var);
	if (t != NULL) {
		for (int i = 0; i < g_slist_length(t); i++) {
			tmpop = g_slist_nth_data(t, i);
			if (tmpop->transaction == trans)
				return OP_OK;
		}
	}

	return OP_ERROR;
}

static enum op_stats unlock_variable(struct operation *op) {
	GSList *t = NULL;
	struct operation *tmp_op;
	enum op_stats stats;

	if (op == NULL)
		return OP_ERROR;

	if (is_transaction_waiting(op)) {
		return OP_WAIT;
	}

	stats = OP_ERROR;

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
				stats = OP_OK;
				break;
			}
		}
	}

	if (g_hash_table_size(lock_x_table) == 0) {
		g_hash_table_destroy(lock_x_table);
		lock_x_table = g_hash_table_new(g_str_hash, g_str_equal);
	}

	if (stats == OP_OK)
		return stats;

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
				stats = OP_OK;
				break;
			}
		}
	}
	if (g_hash_table_size(lock_s_table) == 0) {
		g_hash_table_destroy(lock_s_table);
		lock_s_table = g_hash_table_new(g_str_hash, g_str_equal);
	}

	return stats;
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
			return can_write(op);
		case CMD_READ:
			return can_read(op);
		case CMD_LOCK_S:
			stats = can_s_lock(op->var, op->transaction);
			if (stats == OP_OK) {
				t = g_hash_table_lookup(lock_s_table, op->var);
				t = g_slist_append(t, &(op->transaction));
				g_hash_table_insert(lock_s_table, op->var, t);
			}
			return stats;
		case CMD_LOCK_X:
			stats = can_x_lock(op->var, op->transaction);
			if (stats == OP_OK)
				x_lock(op);
			return stats;
		case CMD_UNLOCK:
			return unlock_variable(op);
		case CMD_UNKNOWN:
		default:
			break;
	}


	return OP_ERROR;
}

static void cleanup_list_waiting_list(gpointer key, gpointer value, gpointer userdata) {
	if (key == NULL)
		return;

	GSList *transactions = (GSList *)value;
	GSList **clean_list = (GSList **)userdata;
	if (g_slist_length(transactions) == 0)
		*clean_list = g_slist_append(*clean_list, key);
}

static void cleanup_waiting_operations() {
	GSList *clean_list;
	char *key;

	if (wait_table == NULL)
		return;

	g_hash_table_foreach(wait_table, cleanup_list_waiting_list, &clean_list);


	for (int i = 0; i < g_slist_length(clean_list); i++) {
		key = g_slist_nth_data(clean_list, i);
		g_hash_table_remove(wait_table, key);
	}

	g_slist_free(clean_list);
}

static void exec_waiting_list(gpointer key, gpointer value, gpointer userdata) {
	struct operation *op;
	enum op_stats stats;

	if (key == NULL)
		return;

	GSList *transactions = (GSList *)value;

	for (int i = 0; i < g_slist_length(transactions); i++) {
		op = (struct operation *)g_slist_nth_data(transactions, i);
		stats = operation_status(op);
		if (stats == OP_OK) {
			printf("EXWO: ");
			dump_operation(op);
			remove_transaction_from_wait(op);
		}
		else {
			printf("EXWN: ");
			dump_operation(op);
		}
	}
}

void exec_waiting_operations() {
	if (wait_table == NULL)
		return;

	g_hash_table_foreach(wait_table, exec_waiting_list, NULL);
	cleanup_waiting_operations();
}

void exec_operations(GSList *op_list) {
	struct operation *op;
	enum op_stats stats;

	if (op_list == NULL)
		return;

	lock_s_table = g_hash_table_new(g_str_hash, g_str_equal);
	lock_x_table = g_hash_table_new(g_str_hash, g_str_equal);
	wait_table = g_hash_table_new(g_str_hash, g_str_equal);

	int i = 0;
	while ((i < g_slist_length(op_list)) || (g_hash_table_size(wait_table) > 0)) {
		exec_waiting_operations();
		if (i < g_slist_length(op_list)) {
			op = g_slist_nth_data(op_list, i);
			printf("EXEC: ");
			dump_operation(op);
			stats = operation_status(op);
			if (stats == OP_WAIT)
				add_transaction_to_wait(op);
			else if (stats != OP_OK) {
				printf("ERROR: ");
				dump_operation(op);
				goto END;
			}
			i++;
		}
	}

	dump_lock_s_table();
	dump_lock_x_table();
	dump_wait_table();

	if ((g_hash_table_size(lock_x_table) > 0) ||
			(g_hash_table_size(lock_s_table) > 0) ||
			(g_hash_table_size(wait_table) > 0))
		printf("ERROR!\n");

END:
	g_hash_table_destroy(lock_s_table);
	g_hash_table_destroy(lock_x_table);
	g_hash_table_destroy(wait_table);
}
