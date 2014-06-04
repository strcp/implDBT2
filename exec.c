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


GSList *unlocked_transaction_list = NULL;
GSList *aborted_transaction_list = NULL;
GSList *lock_waiting_list = NULL;

GHashTable *wait_table;
GHashTable *lock_s_table;
GHashTable *lock_x_table;

static void abort_transaction(struct operation *op);

int compare_trans(gconstpointer a, gconstpointer b) {
	return g_strcmp0(a, b);
}

void dump_operation(struct operation *op) {
	if (op == NULL)
		return;

	printf("[%d] [%s] [%s]\n", op->transaction, cmd_to_strcmd(op->cmd), op->var);
}

#ifdef DEBUG
static void dump_list(gpointer data, gpointer userdata)
{
	char *strtrans;

	if (data == NULL)
		return;

	strtrans = (char *)data;
	printf("\t%s\n", strtrans);
}

static void dump_unlocked_list()
{
	if (g_slist_length(unlocked_transaction_list) == 0)
		return;

	printf("UNLOCKED LIST:\n");
	g_slist_foreach(unlocked_transaction_list, dump_list, NULL);
}

static void dump_aborted_list()
{
	if (g_slist_length(aborted_transaction_list) == 0)
		return;

	printf("ABORTED LIST:\n");
	g_slist_foreach(aborted_transaction_list, dump_list, NULL);
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
#endif

// Checa se t1 trava t2
static int is_locking(int t1, int t2) {
	char *strt1 = g_strdup_printf("%d", t1);

	GSList *op_list = g_hash_table_lookup(wait_table, strt1);
	g_free(strt1);

	if (g_slist_length(op_list) == 0)
		return 0;

	struct operation *op = g_slist_nth_data(op_list, 0);
	if (op == NULL)
		return 0;

	GSList *xlock_list = g_hash_table_lookup(lock_x_table, op->var);
	if (xlock_list != NULL) {
		for (int j = 0; j < g_slist_length(xlock_list); j++) {
			struct operation *tmp_op = g_slist_nth_data(xlock_list, j);
			if (t2 == tmp_op->transaction) {
				return 1;
			}
		}
	}

	// Apenas um X_LOCK pode ser bloqueado por um S_LOCK
	if (op->cmd == CMD_LOCK_X) {
		GSList *slock_list = g_hash_table_lookup(lock_s_table, op->var);
		if (slock_list != NULL) {
			for (int j = 0; j < g_slist_length(slock_list); j++) {
				struct operation *tmp_op = g_slist_nth_data(slock_list, j);
				if (t2 == tmp_op->transaction) {
					return 1;
				}
			}
		}
	}

	return 0;
}

static void check_deadlocks() {
	int key1, key2;
	char *sk1, *sk2;

	if (lock_waiting_list == NULL)
		return;

	GList *trans_waiting = g_hash_table_get_keys(wait_table);

	for (int i = 0; i < g_list_length(trans_waiting); i++) {
		sk1 = g_list_nth_data(trans_waiting, i);
		if (sk1 == NULL)
			continue;

		for (int j = 0; j < g_list_length(trans_waiting); j++) {
			sk2 = g_list_nth_data(trans_waiting, j);
			if (sk2 == NULL)
				continue;

			key1 = atoi(sk1);
			key2 = atoi(sk2);
			if (key1 == key2)
				continue;

			if (is_locking(key1, key2) && is_locking(key2, key1)) {
				printf("* DEADLOCK DETECTED *\n");

				// Selecionando pela operação mais velha
				sk1 = g_strdup_printf("%d", key1);
				GSList *op_list = g_hash_table_lookup(wait_table, sk1);
				g_free(sk1);

				if (g_slist_length(op_list) == 0)
					continue;

				struct operation *op = g_slist_nth_data(op_list, 0);
				if (op == NULL)
					continue;

				abort_transaction(op);
				return;
			}
		}
	}
}

static int is_transaction_waiting(struct operation *op) {
	char *strtrans = g_strdup_printf("%d", op->transaction);
	struct operation *tmp_op;

	if (op == NULL)
		return 0;

	GSList *op_list = g_hash_table_lookup(wait_table, strtrans);

	if (op_list != NULL) {
		for (int i = 0; i < g_slist_length(op_list); i++) {
			tmp_op = g_slist_nth_data(op_list, i);
			if (((tmp_op->cmd == CMD_LOCK_S) || (tmp_op->cmd == CMD_LOCK_X)))
				return 1;
		}
	}

	return 0;
}

static void add_transaction_to_wait(struct operation *op) {
	if (op == NULL)
		return;

	char *strtrans = g_strdup_printf("%d", op->transaction);
	if (strtrans == NULL)
		return;

#ifdef DEBUG
	printf("ADDING TO WAIT: ");
	dump_operation(op);
#endif

	GSList *op_list = g_hash_table_lookup(wait_table, strtrans);
	op_list = g_slist_append(op_list, op);
	g_hash_table_insert(wait_table, strtrans, op_list);

	// Para ser checado depois pelo analisador de deadlocks.
	if ((op->cmd == CMD_LOCK_X) || (op->cmd == CMD_LOCK_S))
		lock_waiting_list = g_slist_append(lock_waiting_list, op);
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

	lock_waiting_list = g_slist_remove(lock_waiting_list, op);
}

int did_unlocked(struct operation *op) {
	char *strtrans;
	GSList *t = NULL;

	strtrans = g_strdup_printf("%d", op->transaction);
	if (strtrans == NULL)
		return -1;

	// Checando se a transição já fez unlocks
	t = g_slist_find_custom(unlocked_transaction_list, strtrans, compare_trans);
	g_free(strtrans);
	if (t != NULL)
		return 1;

	return 0;
}

static enum op_stats unlock_variable(struct operation *op, int force) {
	GSList *t = NULL;
	struct operation *tmp_op;
	enum op_stats stats;
	char *strtrans;

	if (op == NULL)
		return OP_ERROR;

	if (is_transaction_waiting(op) && !force)
		return OP_WAIT;

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
		goto END;

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

END:
	if (stats == OP_OK) {
		if (!did_unlocked(op)) {
			strtrans = g_strdup_printf("%d", op->transaction);
			unlocked_transaction_list = g_slist_append(unlocked_transaction_list, strtrans);
		}
	}

	return stats;
}

static int did_aborted(struct operation *op) {
	char *strtrans;
	GSList *t = NULL;

	strtrans = g_strdup_printf("%d", op->transaction);
	if (strtrans == NULL)
		return -1;

	t = g_slist_find_custom(aborted_transaction_list, strtrans, compare_trans);
	g_free(strtrans);
	if (t != NULL) {
		return 1;
	}

	return 0;
}

static void abort_transaction(struct operation *op) {
	char *strtrans;
	GList *keys = NULL;
	GSList *op_list = NULL;

	strtrans = g_strdup_printf("%d", op->transaction);
	aborted_transaction_list = g_slist_append(aborted_transaction_list, strtrans);
	printf("* ABORTING TRANSACTION: %s *\n", strtrans);

	// Removendo das tabelas de lock_s
	keys = g_hash_table_get_keys(lock_s_table);
	for (int i = 0; i < g_list_length(keys); i++) {
		char *key = g_list_nth_data(keys, i);
		op_list = g_hash_table_lookup(lock_s_table, key);
		for (int j = 0; j < g_slist_length(op_list); j++) {
			struct operation *tmp_op = g_slist_nth_data(op_list, j);
			if (op->transaction == tmp_op->transaction)
				op_list = g_slist_remove(op_list, tmp_op);
		}
		if (g_slist_length(op_list) == 0) {
			g_hash_table_remove(lock_s_table, key);
			g_slist_free(op_list);
		}
		else {
			g_hash_table_insert(lock_s_table, key, op_list);
		}
	}

	// Removendo das tabelas de lock_x
	keys = g_hash_table_get_keys(lock_x_table);
	for (int i = 0; i < g_list_length(keys); i++) {
		char *key = g_list_nth_data(keys, i);
		op_list = g_hash_table_lookup(lock_x_table, key);
		for (int j = 0; j < g_slist_length(op_list); j++) {
			struct operation *tmp_op = g_slist_nth_data(op_list, j);
			if (op->transaction == tmp_op->transaction)
				op_list = g_slist_remove(op_list, tmp_op);
		}
		if (g_slist_length(op_list) == 0) {
			g_hash_table_remove(lock_x_table, key);
			g_slist_free(op_list);
		}
		else {
			g_hash_table_insert(lock_x_table, key, op_list);
		}
	}


	op_list = g_hash_table_lookup(wait_table, strtrans);
	unlock_variable(op, 1);
	if (op_list != NULL) {
		g_hash_table_remove(wait_table, strtrans);
		g_slist_free(op_list);
#ifdef DEBUG
		dump_wait_table();
#endif
	}
}

static enum op_stats can_s_lock(struct operation *op) {
	GSList *t = NULL;

	if (did_unlocked(op))
		return OP_ERROR;

	// Checando se a variável já foi bloqueada exclusivamente por alguma
	// transição.
	t = g_hash_table_lookup(lock_x_table, op->var);
	if (t != NULL)
		return OP_WAIT;

	return OP_OK;
}

static enum op_stats can_x_lock(struct operation *op) {
	GSList *t = NULL;
	struct operation *tmp_op;

	if (did_unlocked(op))
		return OP_ERROR;

	// Checando se a variável já foi bloqueada exclusivamente por alguma
	// outra transição.
	t = g_hash_table_lookup(lock_x_table, op->var);
	if (t != NULL) {
		for (int i = 0; i < g_slist_length(t); i++) {
			tmp_op = g_slist_nth_data(t, i);
			if (tmp_op->transaction == op->transaction)
				continue;
			else
				return OP_WAIT;
		}
	}

	// Checando se a variável já foi bloqueada de maneira compartilhada por
	// alguma outra transição.
	t = g_hash_table_lookup(lock_s_table, op->var);
	if (t != NULL) {
		for (int i = 0; i < g_slist_length(t); i++) {
			tmp_op = g_slist_nth_data(t, i);
			if (tmp_op->transaction == op->transaction)
				continue;
			else
				return OP_WAIT;
		}
	}

	return OP_OK;
}

static enum op_stats can_write(struct operation *op) {
	GSList *t = NULL;
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
	GSList *t = NULL;
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
			stats = can_s_lock(op);
			if (stats == OP_OK) {
				t = g_hash_table_lookup(lock_s_table, op->var);
				t = g_slist_append(t, &(op->transaction));
				g_hash_table_insert(lock_s_table, op->var, t);
			}
			return stats;
		case CMD_LOCK_X:
			stats = can_x_lock(op);
			if (stats == OP_OK)
				x_lock(op);
			return stats;
		case CMD_UNLOCK:
			return unlock_variable(op, 0);
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
	GSList *clean_list = NULL;
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
//		else {
//			printf("EXWN: ");
//			dump_operation(op);
//		}
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
	aborted_transaction_list = NULL;

	int i = 0;
	while ((i < g_slist_length(op_list)) || (g_hash_table_size(wait_table) > 0)) {
		exec_waiting_operations();
		if (i < g_slist_length(op_list)) {
			op = g_slist_nth_data(op_list, i);
			if (did_aborted(op)) {
				i++;
				continue;
			}
			printf("EXEC: ");
			dump_operation(op);
			stats = operation_status(op);
			if (stats == OP_WAIT)
				add_transaction_to_wait(op);
			else if (stats != OP_OK) {
				printf("ERROR: ");
				dump_operation(op);
				abort_transaction(op);
			}
			i++;
		}
		check_deadlocks();
#ifdef DEBUG
		dump_wait_table();
		dump_lock_x_table();
		dump_lock_s_table();
#endif
	}

#ifdef DEBUG
	dump_lock_s_table();
	dump_lock_x_table();
	dump_wait_table();
	dump_unlocked_list();
	dump_aborted_list();
#endif

	if ((g_hash_table_size(lock_x_table) > 0) ||
			(g_hash_table_size(lock_s_table) > 0) ||
			(g_hash_table_size(wait_table) > 0))
		printf("ERROR!\n");

	g_hash_table_destroy(lock_s_table);
	g_hash_table_destroy(lock_x_table);
	g_hash_table_destroy(wait_table);
	g_slist_free(unlocked_transaction_list);
	g_slist_free(aborted_transaction_list);
}
