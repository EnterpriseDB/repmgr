/*
 * repmgr-action-node.c
 *
 * Implements actions available for any kind of node
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */


#include "repmgr.h"

#include "repmgr-client-global.h"
#include "repmgr-action-node.h"

void
do_node_status(void)
{
	PGconn	   	   *conn;

	int 			target_node_id = UNKNOWN_NODE_ID;
	t_node_info 	node_info = T_NODE_INFO_INITIALIZER;
	char			server_version[MAXLEN];
	char			cluster_size[MAXLEN];
	PQExpBufferData	output;

	KeyValueList	node_status = { NULL, NULL };
	KeyValueListCell *cell;

	ItemList 		warnings = { NULL, NULL };
	RecoveryType	recovery_type;

	if (strlen(config_file_options.conninfo))
		conn = establish_db_connection(config_file_options.conninfo, true);
	else
		conn = establish_db_connection_by_params(&source_conninfo, true);

	if (runtime_options.node_id != UNKNOWN_NODE_ID)
		target_node_id = runtime_options.node_id;
	else
		target_node_id = config_file_options.node_id;

	/* Check node exists and is really a standby */

	if (get_node_record(conn, target_node_id, &node_info) != RECORD_FOUND)
	{
		log_error(_("no record found for node %i"), target_node_id);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}
	(void) get_server_version(conn, server_version);

	if (get_cluster_size(conn, cluster_size) == false)
		strncpy(cluster_size, _("unknown"), MAXLEN);

	recovery_type = get_recovery_type(conn);

	get_node_replication_stats(conn, &node_info);

	key_value_list_set(
		&node_status,
		"PostgreSQL version",
		server_version);

	key_value_list_set(
		&node_status,
		"Total data size",
		cluster_size);

	key_value_list_set(
		&node_status,
		"Conninfo",
		node_info.conninfo);

	key_value_list_set(
		&node_status,
		"Role",
		get_node_type_string(node_info.type));

	switch (node_info.type)
	{
		case PRIMARY:
			if (recovery_type == RECTYPE_STANDBY)
			{
				item_list_append(
					&warnings,
					_("- node is registered as primary but running as standby"));
			}
			break;
		case STANDBY:
			if (recovery_type == RECTYPE_PRIMARY)
			{
				item_list_append(
					&warnings,
					_("- node is registered as standby but running as primary"));
			}
			break;
		case BDR:
		default:
			break;
	}


	if (node_info.max_wal_senders >= 0)
	{
		key_value_list_set_format(
			&node_status,
			"Replication connections",
			"%i (of maximal %i)",
			node_info.attached_wal_receivers,
			node_info.max_wal_senders);
	}
	else if (node_info.max_wal_senders == 0)
	{
		key_value_list_set_format(
			&node_status,
			"Replication connections",
			"disabled");
	}

	if (node_info.max_replication_slots > 0)
	{
		PQExpBufferData	slotinfo;
		initPQExpBuffer(&slotinfo);

		appendPQExpBuffer(
			&slotinfo,
			"%i (of maximal %i)",
			node_info.active_replication_slots + node_info.inactive_replication_slots,
			node_info.max_replication_slots);


		if (node_info.inactive_replication_slots > 0)
		{
			appendPQExpBuffer(
				&slotinfo,
				"; %i inactive",
				node_info.inactive_replication_slots);

			item_list_append_format(
				&warnings,
				_("- node has %i inactive replication slots"),
				node_info.inactive_replication_slots);
		}

		key_value_list_set(
			&node_status,
			"Replication slots",
			slotinfo.data);

		termPQExpBuffer(&slotinfo);
	}
	else if (node_info.max_replication_slots == 0)
	{
		key_value_list_set(
			&node_status,
			"Replication slots",
			"disabled");
	}

	initPQExpBuffer(&output);

	appendPQExpBuffer(
		&output,
		"Node \"%s\":\n",
		node_info.node_name);

	for (cell = node_status.head; cell; cell = cell->next)
	{
		appendPQExpBuffer(
			&output,
			"\t%s: %s\n", cell->key, cell->value);
	}

	puts(output.data);

	termPQExpBuffer(&output);

	if (warnings.head != NULL && runtime_options.terse == false)
	{
		log_warning(_("following issue(s) were detected:"));
		print_item_list(&warnings);
		log_hint(_("execute \"repmgr node check\" for more details"));
	}

}


void
do_node_check(void)
{
}
