/*
 * repmgr-action-node.c
 *
 * Implements actions available for any kind of node
 *
 * Copyright (c) 2ndQuadrant, 2010-2020
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <sys/stat.h>
#include <dirent.h>

#include "repmgr.h"
#include "controldata.h"
#include "dirutil.h"
#include "dbutils.h"
#include "compat.h"

#include "repmgr-client-global.h"
#include "repmgr-action-node.h"
#include "repmgr-action-standby.h"

static bool copy_file(const char *src_file, const char *dest_file);
static void format_archive_dir(PQExpBufferData *archive_dir);
static t_server_action parse_server_action(const char *action);

static void exit_optformat_error(const char *error, int errcode);

static void _do_node_service_list_actions(t_server_action action);
static void _do_node_status_is_shutdown_cleanly(void);
static void _do_node_archive_config(void);
static void _do_node_restore_config(void);

static void do_node_check_replication_connection(void);
static CheckStatus do_node_check_archive_ready(PGconn *conn, OutputMode mode, CheckStatusList *list_output);
static CheckStatus do_node_check_downstream(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output);
static CheckStatus do_node_check_upstream(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output);
static CheckStatus do_node_check_replication_lag(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output);
static CheckStatus do_node_check_role(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output);
static CheckStatus do_node_check_slots(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output);
static CheckStatus do_node_check_missing_slots(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output);
static CheckStatus do_node_check_data_directory(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output);
static CheckStatus do_node_check_replication_config_owner(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output);
static CheckStatus do_node_check_db_connection(PGconn *conn, OutputMode mode);

/*
 * NODE STATUS
 *
 * Can only be run on the local node, as it needs to be able to
 * read the data directory.
 *
 * Parameters:
 *   --is-shutdown-cleanly (for internal use only)
 *   --csv
 */

void
do_node_status(void)
{
	PGconn	   *conn = NULL;

	t_node_info node_info = T_NODE_INFO_INITIALIZER;
	char		cluster_size[MAXLEN];
	PQExpBufferData output;

	KeyValueList node_status = {NULL, NULL};
	KeyValueListCell *cell = NULL;
	NodeInfoList missing_slots = T_NODE_INFO_LIST_INITIALIZER;

	ItemList	warnings = {NULL, NULL};
	RecoveryType recovery_type = RECTYPE_UNKNOWN;
	ReplInfo	replication_info;
	t_recovery_conf recovery_conf = T_RECOVERY_CONF_INITIALIZER;

	char		data_dir[MAXPGPATH] = "";
	char		server_version_str[MAXVERSIONSTR] = "";

	/*
	 * A database connection is *not* required for this check
	 */
	if (runtime_options.is_shutdown_cleanly == true)
	{
		return _do_node_status_is_shutdown_cleanly();
	}

	init_replication_info(&replication_info);


	/* config file required, so we should have "conninfo" and "data_directory" */
	conn = establish_db_connection(config_file_options.conninfo, true);
	strncpy(data_dir, config_file_options.data_directory, MAXPGPATH);

	(void)get_server_version(conn, server_version_str);

	/* check node exists  */

	if (get_node_record_with_upstream(conn, config_file_options.node_id, &node_info) != RECORD_FOUND)
	{
		log_error(_("no record found for node %i"), config_file_options.node_id);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	if (get_cluster_size(conn, cluster_size) == false)
		strncpy(cluster_size, _("unknown"), MAXLEN);

	recovery_type = get_recovery_type(conn);

	get_node_replication_stats(conn, &node_info);

	key_value_list_set(&node_status,
					   "PostgreSQL version",
					   server_version_str);

	key_value_list_set(&node_status,
					   "Total data size",
					   cluster_size);

	key_value_list_set(&node_status,
					   "Conninfo",
					   node_info.conninfo);

	if (runtime_options.verbose == true)
	{
		uint64		local_system_identifier = get_system_identifier(config_file_options.data_directory);

		if (local_system_identifier == UNKNOWN_SYSTEM_IDENTIFIER)
		{
			key_value_list_set(&node_status,
							   "System identifier",
							   "unknown");
			item_list_append_format(&warnings,
									_("unable to retrieve system identifier from pg_control"));
		}
		else
		{
			key_value_list_set_format(&node_status,
									  "System identifier",
									  "%lu", local_system_identifier);
		}
	}

	key_value_list_set(&node_status,
					   "Role",
					   get_node_type_string(node_info.type));

	switch (node_info.type)
	{
		case PRIMARY:
			if (recovery_type == RECTYPE_STANDBY)
			{
				item_list_append(&warnings,
								 _("- node is registered as primary but running as standby"));
			}
			break;
		case STANDBY:
			if (recovery_type == RECTYPE_PRIMARY)
			{
				item_list_append(&warnings,
								 _("- node is registered as standby but running as primary"));
			}
			break;
		default:
			break;
	}

	if (guc_set(conn, "archive_mode", "=", "off"))
	{
		key_value_list_set(&node_status,
						   "WAL archiving",
						   "off");

		key_value_list_set(&node_status,
						   "Archive command",
						   "(none)");
	}
	else
	{
		/* "archive_mode" is not "off", i.e. one of "on", "always" */
		bool		enabled = true;
		PQExpBufferData archiving_status;
		char		archive_command[MAXLEN] = "";

		initPQExpBuffer(&archiving_status);

		/*
		 * if the node is a standby, and "archive_mode" is "on", archiving will
		 * actually be disabled.
		 */
		if (recovery_type == RECTYPE_STANDBY)
		{
			if (guc_set(conn, "archive_mode", "=", "on"))
				enabled = false;
		}

		if (enabled == true)
		{
			appendPQExpBufferStr(&archiving_status, "enabled");
		}
		else
		{
			appendPQExpBufferStr(&archiving_status, "disabled");
		}

		if (enabled == false && recovery_type == RECTYPE_STANDBY)
		{
			if (PQserverVersion(conn) >= 90500)
			{
				appendPQExpBufferStr(&archiving_status,
									 " (on standbys \"archive_mode\" must be set to \"always\" to be effective)");
			}
			else
			{
				appendPQExpBufferStr(&archiving_status,
									 " (\"archive_mode\" has no effect on standbys)");
			}
		}

		key_value_list_set(&node_status,
						   "WAL archiving",
						   archiving_status.data);

		termPQExpBuffer(&archiving_status);

		get_pg_setting(conn, "archive_command", archive_command);

		key_value_list_set(&node_status,
						   "Archive command",
						   archive_command);
	}

	{
		int			ready_files;

		ready_files = get_ready_archive_files(conn, data_dir);

		if (ready_files == ARCHIVE_STATUS_DIR_ERROR)
		{
			item_list_append_format(&warnings,
									"- unable to check archive_status directory\n");
		}
		else
		{
			if (runtime_options.output_mode == OM_CSV)
			{
				key_value_list_set_format(&node_status,
										  "WALs pending archiving",
										  "%i",
										  ready_files);
			}
			else
			{
				key_value_list_set_format(&node_status,
										  "WALs pending archiving",
										  "%i pending files",
										  ready_files);
			}
		}

		if (guc_set(conn, "archive_mode", "=", "off"))
		{
			key_value_list_set_output_mode(&node_status, "WALs pending archiving", OM_CSV);
		}

	}


	if (node_info.max_wal_senders >= 0)
	{
		/* In CSV mode, raw values supplied as well */
		key_value_list_set_format(&node_status,
								  "Replication connections",
								  "%i (of maximal %i)",
								  node_info.attached_wal_receivers,
								  node_info.max_wal_senders);
	}
	else if (node_info.max_wal_senders == 0)
	{
		key_value_list_set_format(&node_status,
								  "Replication connections",
								  "disabled");
	}

	/* check for attached nodes */
	{
		NodeInfoList downstream_nodes = T_NODE_INFO_LIST_INITIALIZER;
		NodeInfoListCell *node_cell = NULL;
		ItemList	missing_nodes = {NULL, NULL};
		int			missing_nodes_count = 0;
		int			expected_nodes_count = 0;

		get_downstream_node_records(conn, config_file_options.node_id, &downstream_nodes);

		/* if a witness node is present, we'll need to remove this from the total */
		expected_nodes_count = downstream_nodes.node_count;

		for (node_cell = downstream_nodes.head; node_cell; node_cell = node_cell->next)
		{
			/* skip witness server */
			if (node_cell->node_info->type == WITNESS)
			{
				expected_nodes_count --;
				continue;
			}

			if (is_downstream_node_attached(conn, node_cell->node_info->node_name, NULL) != NODE_ATTACHED)
			{
				missing_nodes_count++;
				item_list_append_format(&missing_nodes,
										"%s (ID: %i)",
										node_cell->node_info->node_name,
										node_cell->node_info->node_id);
			}
		}

		if (missing_nodes_count)
		{
			ItemListCell *missing_cell = NULL;

			item_list_append_format(&warnings,
									_("- %i of %i downstream nodes not attached:"),
									missing_nodes_count,
									expected_nodes_count);

			for (missing_cell = missing_nodes.head; missing_cell; missing_cell = missing_cell->next)
			{
				item_list_append_format(&warnings,
										"  - %s\n", missing_cell->string);
			}
		}
	}

	if (node_info.max_replication_slots == 0)
	{
		key_value_list_set(&node_status,
						   "Replication slots",
						   "disabled");
	}
	else
	{
		PQExpBufferData slotinfo;

		/*
		 * check for missing replication slots - we do this regardless of
		 * what "max_replication_slots" is set to, in case the downstream
		 * node was configured with "use_replication_slots=true" and is
		 * expecting a replication slot to be available
		 */
		get_downstream_nodes_with_missing_slot(conn,
											   config_file_options.node_id,
											   &missing_slots);

		if (missing_slots.node_count > 0)
		{
			NodeInfoListCell *missing_slot_cell = NULL;

			item_list_append_format(&warnings,
									_("- replication slots missing for following %i node(s):"),
									missing_slots.node_count);

			for (missing_slot_cell = missing_slots.head; missing_slot_cell; missing_slot_cell = missing_slot_cell->next)
			{
				item_list_append_format(&warnings,
										_("  - %s (ID: %i, slot name: \"%s\")"),
										missing_slot_cell->node_info->node_name,
										missing_slot_cell->node_info->node_id,
										missing_slot_cell->node_info->slot_name);
			}
		}

		initPQExpBuffer(&slotinfo);

		appendPQExpBuffer(&slotinfo,
						  "%i physical (of maximal %i; %i missing)",
						  node_info.active_replication_slots + node_info.inactive_replication_slots,
						  node_info.max_replication_slots,
						  missing_slots.node_count);

		if (node_info.inactive_replication_slots > 0)
		{
			KeyValueList inactive_replication_slots = {NULL, NULL};
			KeyValueListCell *cell = NULL;

			(void) get_inactive_replication_slots(conn, &inactive_replication_slots);

			appendPQExpBuffer(&slotinfo,
							  "; %i inactive",
							  node_info.inactive_replication_slots);

			item_list_append_format(&warnings,
									_("- node has %i inactive physical replication slots"),
									node_info.inactive_replication_slots);

			for (cell = inactive_replication_slots.head; cell; cell = cell->next)
			{
				item_list_append_format(&warnings,
										"  - %s", cell->key);
			}

			key_value_list_free(&inactive_replication_slots);
		}

		key_value_list_set(&node_status,
						   "Replication slots",
						   slotinfo.data);

		termPQExpBuffer(&slotinfo);
	}


	if (node_info.type == STANDBY)
	{
		key_value_list_set_format(&node_status,
								  "Upstream node",
								  "%s (ID: %i)",
								  node_info.upstream_node_name,
								  node_info.upstream_node_id);

		get_replication_info(conn, node_info.type, &replication_info);

		key_value_list_set_format(&node_status,
								  "Replication lag",
								  "%i seconds",
								  replication_info.replication_lag_time);

		key_value_list_set_format(&node_status,
								  "Last received LSN",
								  "%X/%X", format_lsn(replication_info.last_wal_receive_lsn));

		key_value_list_set_format(&node_status,
								  "Last replayed LSN",
								  "%X/%X", format_lsn(replication_info.last_wal_replay_lsn));
	}
	else
	{
		key_value_list_set(&node_status,
						   "Upstream node",
						   "(none)");
		key_value_list_set_output_mode(&node_status,
									   "Upstream node",
									   OM_CSV);

		key_value_list_set(&node_status,
						   "Replication lag",
						   "n/a");

		key_value_list_set(&node_status,
						   "Last received LSN",
						   "(none)");

		key_value_list_set_output_mode(&node_status,
									   "Last received LSN",
									   OM_CSV);

		key_value_list_set(&node_status,
						   "Last replayed LSN",
						   "(none)");

		key_value_list_set_output_mode(&node_status,
									   "Last replayed LSN",
									   OM_CSV);
	}


	parse_recovery_conf(data_dir, &recovery_conf);

	/* format output */
	initPQExpBuffer(&output);

	if (runtime_options.output_mode == OM_CSV)
	{
		appendPQExpBuffer(&output,
						  "\"Node name\",\"%s\"\n",
						  node_info.node_name);

		appendPQExpBuffer(&output,
						  "\"Node ID\",\"%i\"\n",
						  node_info.node_id);

		for (cell = node_status.head; cell; cell = cell->next)
		{
			appendPQExpBuffer(&output,
							  "\"%s\",\"%s\"\n",
							  cell->key, cell->value);
		}

		/* we'll add the raw data as well */
		appendPQExpBuffer(&output,
						  "\"max_wal_senders\",%i\n",
						  node_info.max_wal_senders);

		appendPQExpBuffer(&output,
						  "\"occupied_wal_senders\",%i\n",
						  node_info.attached_wal_receivers);

		appendPQExpBuffer(&output,
						  "\"max_replication_slots\",%i\n",
						  node_info.max_replication_slots);

		appendPQExpBuffer(&output,
						  "\"active_replication_slots\",%i\n",
						  node_info.active_replication_slots);

		/* output inactive slot information */
		appendPQExpBuffer(&output,
						  "\"inactive_replication_slots\",%i",
						  node_info.inactive_replication_slots);

		if (node_info.inactive_replication_slots)
		{
			KeyValueList inactive_replication_slots = {NULL, NULL};
			KeyValueListCell *cell = NULL;

			(void) get_inactive_replication_slots(conn, &inactive_replication_slots);
			for (cell = inactive_replication_slots.head; cell; cell = cell->next)
			{
				appendPQExpBuffer(&output,
								  ",\"%s\"", cell->key);
			}

			key_value_list_free(&inactive_replication_slots);
		}

		/* output missing slot information */

		appendPQExpBufferChar(&output, '\n');
		appendPQExpBuffer(&output,
						  "\"missing_replication_slots\",%i",
						  missing_slots.node_count);

		if (missing_slots.node_count > 0)
		{
			NodeInfoListCell *missing_slot_cell = NULL;

			for (missing_slot_cell = missing_slots.head; missing_slot_cell; missing_slot_cell = missing_slot_cell->next)
			{
				appendPQExpBuffer(&output,
								  ",\"%s\"", missing_slot_cell->node_info->slot_name);
			}
		}

	}
	else
	{
		appendPQExpBuffer(&output,
						  "Node \"%s\":\n",
						  node_info.node_name);

		for (cell = node_status.head; cell; cell = cell->next)
		{
			if (cell->output_mode == OM_NOT_SET)
				appendPQExpBuffer(&output,
								  "\t%s: %s\n",
								  cell->key, cell->value);
		}
	}

	puts(output.data);

	termPQExpBuffer(&output);

	if (warnings.head != NULL && runtime_options.terse == false && runtime_options.output_mode == OM_TEXT)
	{
		log_warning(_("following issue(s) were detected:"));
		print_item_list(&warnings);
		log_hint(_("execute \"repmgr node check\" for more details"));
	}

	clear_node_info_list(&missing_slots);
	key_value_list_free(&node_status);
	item_list_free(&warnings);
	PQfinish(conn);

	/*
	 * If warnings were noted, even if they're not displayed (e.g. in --csv node),
	 * that means something's not right so we need to emit a non-zero exit code.
	 */
	if (warnings.head != NULL)
	{
		exit(ERR_NODE_STATUS);
	}

	return;
}


/*
 * Returns information about the running state of the node.
 * For internal use during "standby switchover".
 *
 * Returns "longopt" output:
 *
 * --status=(RUNNING|SHUTDOWN|UNCLEAN_SHUTDOWN|UNKNOWN)
 * --last-checkpoint=...
 */

static void
_do_node_status_is_shutdown_cleanly(void)
{
	PGPing		ping_status;
	PQExpBufferData output;

	DBState		db_state;
	XLogRecPtr	checkPoint = InvalidXLogRecPtr;

	NodeStatus	node_status = NODE_STATUS_UNKNOWN;

	initPQExpBuffer(&output);

	appendPQExpBufferStr(&output,
					  "--state=");

	/* sanity-check we're dealing with a PostgreSQL directory */
	if (is_pg_dir(config_file_options.data_directory) == false)
	{
		appendPQExpBufferStr(&output, "UNKNOWN");
		printf("%s\n", output.data);
		termPQExpBuffer(&output);
		return;
	}

	ping_status = PQping(config_file_options.conninfo);

	switch (ping_status)
	{
		case PQPING_OK:
			node_status = NODE_STATUS_UP;
			break;
		case PQPING_REJECT:
			node_status = NODE_STATUS_UP;
			break;
		case PQPING_NO_ATTEMPT:
		case PQPING_NO_RESPONSE:
			/* status not yet clear */
			break;
	}

	/* check what pg_control says */

	if (get_db_state(config_file_options.data_directory, &db_state) == false)
	{
		/*
		 * Unable to retrieve the database state from pg_control
		 */
		node_status = NODE_STATUS_UNKNOWN;
		log_verbose(LOG_DEBUG, "unable to determine db state");
		goto return_state;
	}

	log_verbose(LOG_DEBUG, "db state now: %s", describe_db_state(db_state));

	if (db_state != DB_SHUTDOWNED && db_state != DB_SHUTDOWNED_IN_RECOVERY)
	{
		if (node_status != NODE_STATUS_UP)
		{
			node_status = NODE_STATUS_UNCLEAN_SHUTDOWN;
		}
		/* server is still responding but shutting down */
		else if (db_state == DB_SHUTDOWNING)
		{
			node_status = NODE_STATUS_SHUTTING_DOWN;
		}
	}

	checkPoint = get_latest_checkpoint_location(config_file_options.data_directory);

	if (checkPoint == InvalidXLogRecPtr)
	{
		/* unable to read pg_control, don't know what's happening */
		node_status = NODE_STATUS_UNKNOWN;
	}
	else if (node_status == NODE_STATUS_UNKNOWN)
	{
		/*
		 * if still "UNKNOWN" at this point, then the node must be cleanly shut
		 * down
		 */
		node_status = NODE_STATUS_DOWN;
	}


return_state:

	log_verbose(LOG_DEBUG, "node status determined as: %s",
				print_node_status(node_status));

	appendPQExpBuffer(&output,
					  "%s", print_node_status(node_status));

	if (node_status == NODE_STATUS_DOWN)
	{
		appendPQExpBuffer(&output,
						  " --last-checkpoint-lsn=%X/%X",
						  format_lsn(checkPoint));
	}

	printf("%s\n", output.data);
	termPQExpBuffer(&output);
	return;
}

static void
exit_optformat_error(const char *error, int errcode)
{
	PQExpBufferData output;

	Assert(runtime_options.output_mode == OM_OPTFORMAT);

	initPQExpBuffer(&output);

	appendPQExpBuffer(&output,
					  "--error=%s",
					  error);

	printf("%s\n", output.data);

	termPQExpBuffer(&output);

	exit(errcode);
}

/*
 * Configuration file required
 */
void
do_node_check(void)
{
	PGconn	   *conn = NULL;
	PQExpBufferData output;

	t_node_info node_info = T_NODE_INFO_INITIALIZER;

	CheckStatus return_code;
	CheckStatusList status_list = {NULL, NULL};
	CheckStatusListCell *cell = NULL;

	bool			issue_detected = false;
	bool			exit_on_connection_error = true;

	/* for internal use */
	if (runtime_options.has_passfile == true)
	{
		return_code = has_passfile() ? 0 : 1;

		exit(return_code);
	}

	/* for use by "standby switchover" */
	if (runtime_options.replication_connection == true)
	{
		do_node_check_replication_connection();
		exit(SUCCESS);
	}

	if (runtime_options.db_connection == true)
	{
		exit_on_connection_error = false;
	}

	/*
	 * If --optformat was provided, we'll assume this is a remote invocation
	 * and instead of exiting with an error, we'll return an error string to
	 * so the remote invoker will know what's happened.
	 */
	if (runtime_options.output_mode == OM_OPTFORMAT)
	{
		exit_on_connection_error = false;
	}


	if (config_file_options.conninfo[0] != '\0')
	{
		t_conninfo_param_list node_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;
		char	   *errmsg = NULL;
		bool		parse_success = false;

		initialize_conninfo_params(&node_conninfo, false);

		parse_success = parse_conninfo_string(config_file_options.conninfo,
											  &node_conninfo,
											  &errmsg, false);

		if (parse_success == false)
		{
			if (runtime_options.output_mode == OM_OPTFORMAT)
			{
				exit_optformat_error("CONNINFO_PARSE",
									 ERR_BAD_CONFIG);
			}

			log_error(_("unable to parse conninfo string \"%s\" for local node"),
					  config_file_options.conninfo);
			log_detail("%s", errmsg);

			exit(ERR_BAD_CONFIG);
		}

		/*
		 * If --superuser option provided, attempt to connect as the specified user
		 */

		if (runtime_options.superuser[0] != '\0')
		{
			conn = establish_db_connection_with_replacement_param(
				config_file_options.conninfo,
				"user",
				runtime_options.superuser,
				exit_on_connection_error);
		}
		else
		{
			conn = establish_db_connection_by_params(&node_conninfo, exit_on_connection_error);
		}
	}
	else
	{
		conn = establish_db_connection_by_params(&source_conninfo, exit_on_connection_error);
	}


	/*
	 * --db-connection option provided
	 */
	if (runtime_options.db_connection == true)
	{
		return_code = do_node_check_db_connection(conn, runtime_options.output_mode);
		PQfinish(conn);
		exit(return_code);
	}

	/*
	 * If we've reached here, and the connection is invalid, then --optformat was provided
	 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		exit_optformat_error("DB_CONNECTION",
							 ERR_DB_CONN);
	}

	if (get_node_record(conn, config_file_options.node_id, &node_info) != RECORD_FOUND)
	{
		log_error(_("no record found for node %i"), config_file_options.node_id);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* add replication statistics to node record */
	get_node_replication_stats(conn, &node_info);

	/*
	 * handle specific checks ======================
	 */
	if (runtime_options.archive_ready == true)
	{
		return_code = do_node_check_archive_ready(conn,
												  runtime_options.output_mode,
												  NULL);
		PQfinish(conn);
		exit(return_code);
	}

	if (runtime_options.upstream == true)
	{
		return_code = do_node_check_upstream(conn,
											 runtime_options.output_mode,
											 &node_info,
											 NULL);
		PQfinish(conn);
		exit(return_code);
	}

	if (runtime_options.downstream == true)
	{
		return_code = do_node_check_downstream(conn,
											   runtime_options.output_mode,
											   &node_info,
											   NULL);
		PQfinish(conn);
		exit(return_code);
	}

	if (runtime_options.replication_lag == true)
	{
		return_code = do_node_check_replication_lag(conn,
													runtime_options.output_mode,
													&node_info,
													NULL);
		PQfinish(conn);
		exit(return_code);
	}

	if (runtime_options.role == true)
	{
		return_code = do_node_check_role(conn,
										 runtime_options.output_mode,
										 &node_info,
										 NULL);
		PQfinish(conn);
		exit(return_code);
	}

	if (runtime_options.slots == true)
	{
		return_code = do_node_check_slots(conn,
										  runtime_options.output_mode,
										  &node_info,
										  NULL);
		PQfinish(conn);
		exit(return_code);
	}

	if (runtime_options.missing_slots == true)
	{
		return_code = do_node_check_missing_slots(conn,
												  runtime_options.output_mode,
												  &node_info,
												  NULL);
		PQfinish(conn);
		exit(return_code);
	}

	if (runtime_options.data_directory_config == true)
	{
		return_code = do_node_check_data_directory(conn,
												   runtime_options.output_mode,
												   &node_info,
												   NULL);
		PQfinish(conn);
		exit(return_code);
	}

	if (runtime_options.replication_config_owner == true)
	{
		return_code = do_node_check_replication_config_owner(conn,
													   runtime_options.output_mode,
													   &node_info,
													   NULL);
		PQfinish(conn);
		exit(return_code);
	}


	if (runtime_options.output_mode == OM_NAGIOS)
	{
		log_error(_("--nagios can only be used with a specific check"));
		log_hint(_("execute \"repmgr node --help\" for details"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* output general overview */

	initPQExpBuffer(&output);

	/* order functions are called is also output order */
	if (do_node_check_role(conn, runtime_options.output_mode, &node_info, &status_list) != CHECK_STATUS_OK)
		issue_detected = true;

	if (do_node_check_replication_lag(conn, runtime_options.output_mode, &node_info, &status_list) != CHECK_STATUS_OK)
		issue_detected = true;

	if (do_node_check_archive_ready(conn, runtime_options.output_mode, &status_list) != CHECK_STATUS_OK)
		issue_detected = true;

	if (do_node_check_upstream(conn, runtime_options.output_mode, &node_info, &status_list) != CHECK_STATUS_OK)
		issue_detected = true;

	if (do_node_check_downstream(conn, runtime_options.output_mode, &node_info, &status_list) != CHECK_STATUS_OK)
		issue_detected = true;

	if (do_node_check_slots(conn, runtime_options.output_mode, &node_info, &status_list) != CHECK_STATUS_OK)
		issue_detected = true;

	if (do_node_check_missing_slots(conn, runtime_options.output_mode, &node_info, &status_list) != CHECK_STATUS_OK)
		issue_detected = true;

	if (do_node_check_data_directory(conn, runtime_options.output_mode, &node_info, &status_list) != CHECK_STATUS_OK)
		issue_detected = true;

	if (runtime_options.output_mode == OM_CSV)
	{
		appendPQExpBuffer(&output,
						  "\"Node name\",\"%s\"\n",
						  node_info.node_name);

		appendPQExpBuffer(&output,
						  "\"Node ID\",\"%i\"\n",
						  node_info.node_id);

		for (cell = status_list.head; cell; cell = cell->next)
		{
			appendPQExpBuffer(&output,
							  "\"%s\",\"%s\"",
							  cell->item,
							  output_check_status(cell->status));

			if (strlen(cell->details))
			{
				appendPQExpBuffer(&output,
								  ",\"%s\"",
								  cell->details);
			}
			appendPQExpBufferChar(&output, '\n');
		}
	}
	else
	{
		appendPQExpBuffer(&output,
						  "Node \"%s\":\n",
						  node_info.node_name);

		for (cell = status_list.head; cell; cell = cell->next)
		{
			appendPQExpBuffer(&output,
							  "\t%s: %s",
							  cell->item,
							  output_check_status(cell->status));

			if (strlen(cell->details))
			{
				appendPQExpBuffer(&output,
								  " (%s)",
								  cell->details);
			}
			appendPQExpBufferChar(&output, '\n');
		}
	}


	printf("%s", output.data);
	termPQExpBuffer(&output);
	check_status_list_free(&status_list);

	PQfinish(conn);

	if (issue_detected == true)
	{
		exit(ERR_NODE_STATUS);
	}
}


static void
do_node_check_replication_connection(void)
{
	PGconn *local_conn = NULL;
	PGconn *repl_conn = NULL;
	t_node_info node_record = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status = RECORD_NOT_FOUND;
	PQExpBufferData output;


	initPQExpBuffer(&output);
	appendPQExpBufferStr(&output,
						 "--connection=");

	if (runtime_options.remote_node_id == UNKNOWN_NODE_ID)
	{
		appendPQExpBufferStr(&output, "UNKNOWN");
		printf("%s\n", output.data);
		termPQExpBuffer(&output);
		return;
	}

	/* retrieve remote node record from local database */
	local_conn = establish_db_connection(config_file_options.conninfo, false);

	if (PQstatus(local_conn) != CONNECTION_OK)
	{
		appendPQExpBufferStr(&output, "CONNECTION_ERROR");
		printf("%s\n", output.data);
		termPQExpBuffer(&output);
		return;
	}

	record_status = get_node_record(local_conn, runtime_options.remote_node_id, &node_record);
	PQfinish(local_conn);

	if (record_status != RECORD_FOUND)
	{
		appendPQExpBufferStr(&output, "UNKNOWN");
		printf("%s\n", output.data);
		termPQExpBuffer(&output);
		return;
	}

	repl_conn = establish_replication_connection_from_conninfo(node_record.conninfo,
															   node_record.repluser);

	if (PQstatus(repl_conn) != CONNECTION_OK)
	{
		appendPQExpBufferStr(&output, "BAD");
		printf("%s\n", output.data);
		termPQExpBuffer(&output);
		return;
	}

	PQfinish(repl_conn);

	appendPQExpBufferStr(&output, "OK");
	printf("%s\n", output.data);
	termPQExpBuffer(&output);

	return;
}



static CheckStatus
do_node_check_archive_ready(PGconn *conn, OutputMode mode, CheckStatusList *list_output)
{
	int			ready_archive_files = 0;
	CheckStatus status = CHECK_STATUS_UNKNOWN;
	PQExpBufferData details;

	if (mode == OM_CSV && list_output == NULL)
	{
		log_error(_("--csv output not provided with --archive-ready option"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	initPQExpBuffer(&details);

	ready_archive_files = get_ready_archive_files(conn, config_file_options.data_directory);

	if (ready_archive_files > config_file_options.archive_ready_critical)
	{
		status = CHECK_STATUS_CRITICAL;

		switch (mode)
		{
			case OM_OPTFORMAT:
				appendPQExpBuffer(&details,
								  "--files=%i --threshold=%i",
								  ready_archive_files, config_file_options.archive_ready_critical);
				break;
			case OM_NAGIOS:
				appendPQExpBuffer(&details,
								  "%i pending archive ready files | files=%i;%i;%i",
								  ready_archive_files,
								  ready_archive_files,
								  config_file_options.archive_ready_warning,
								  config_file_options.archive_ready_critical);
				break;
			case OM_TEXT:
				appendPQExpBuffer(&details,
								  "%i pending archive ready files, critical threshold: %i",
								  ready_archive_files, config_file_options.archive_ready_critical);
				break;

			default:
				break;
		}
	}
	else if (ready_archive_files > config_file_options.archive_ready_warning)
	{
		status = CHECK_STATUS_WARNING;

		switch (mode)
		{
			case OM_OPTFORMAT:
				appendPQExpBuffer(&details,
								  "--files=%i --threshold=%i",
								  ready_archive_files, config_file_options.archive_ready_warning);
				break;
			case OM_NAGIOS:
				appendPQExpBuffer(&details,
								  "%i pending archive ready files | files=%i;%i;%i",
								  ready_archive_files,
								  ready_archive_files,
								  config_file_options.archive_ready_warning,
								  config_file_options.archive_ready_critical);

				break;
			case OM_TEXT:
				appendPQExpBuffer(&details,
								  "%i pending archive ready files (threshold: %i)",
								  ready_archive_files, config_file_options.archive_ready_warning);
				break;

			default:
				break;
		}
	}
	else if (ready_archive_files < 0)
	{
		status = CHECK_STATUS_UNKNOWN;

		switch (mode)
		{
			case OM_OPTFORMAT:
				break;
			case OM_NAGIOS:
			case OM_TEXT:
				appendPQExpBufferStr(&details,
									 "unable to check archive_status directory");
				break;

			default:
				break;
		}
	}
	else
	{
		status = CHECK_STATUS_OK;

		switch (mode)
		{
			case OM_OPTFORMAT:
				appendPQExpBuffer(&details,
								  "--files=%i", ready_archive_files);
				break;
			case OM_NAGIOS:
				appendPQExpBuffer(&details,
								  "%i pending archive ready files | files=%i;%i;%i",
								  ready_archive_files,
								  ready_archive_files,
								  config_file_options.archive_ready_warning,
								  config_file_options.archive_ready_critical);
				break;
			case OM_TEXT:
				appendPQExpBuffer(&details,
								  "%i pending archive ready files", ready_archive_files);
				break;

			default:
				break;
		}
	}

	switch (mode)
	{
		case OM_OPTFORMAT:
			{
				printf("--status=%s %s\n",
					   output_check_status(status),
					   details.data);
			}
			break;
		case OM_NAGIOS:
			printf("REPMGR_ARCHIVE_READY %s: %s\n",
				   output_check_status(status),
				   details.data);
			break;
		case OM_CSV:
		case OM_TEXT:
			if (list_output != NULL)
			{
				check_status_list_set(list_output,
									  "WAL archiving",
									  status,
									  details.data);
			}
			else
			{
				printf("%s (%s)\n",
					   output_check_status(status),
					   details.data);
			}
		default:
			break;
	}

	termPQExpBuffer(&details);
	return status;
}


static CheckStatus
do_node_check_downstream(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output)
{
	NodeInfoList downstream_nodes = T_NODE_INFO_LIST_INITIALIZER;
	NodeInfoListCell *cell = NULL;
	int			missing_nodes_count = 0;
	int			expected_nodes_count = 0;
	CheckStatus status = CHECK_STATUS_OK;
	ItemList	missing_nodes = {NULL, NULL};
	ItemList	attached_nodes = {NULL, NULL};
	PQExpBufferData details;

	if (mode == OM_CSV && list_output == NULL)
	{
		log_error(_("--csv output not provided with --downstream option"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	initPQExpBuffer(&details);

	get_downstream_node_records(conn, config_file_options.node_id, &downstream_nodes);

	/* if a witness node is present, we'll need to remove this from the total */
	expected_nodes_count = downstream_nodes.node_count;

	for (cell = downstream_nodes.head; cell; cell = cell->next)
	{
		/* skip witness server */
		if (cell->node_info->type == WITNESS)
		{
			expected_nodes_count --;
			continue;
		}

		if (is_downstream_node_attached(conn, cell->node_info->node_name, NULL) != NODE_ATTACHED)
		{
			missing_nodes_count++;
			item_list_append_format(&missing_nodes,
									"%s (ID: %i)",
									cell->node_info->node_name,
									cell->node_info->node_id);
		}
		else
		{
			item_list_append_format(&attached_nodes,
									"%s (ID: %i)",
									cell->node_info->node_name,
									cell->node_info->node_id);
		}
	}

	if (node_info->type == WITNESS)
	{
		/* witness is not connecting to any upstream */
		appendPQExpBufferStr(&details,
							 _("N/A - node is a witness"));
	}
	else if (missing_nodes_count == 0)
	{
		if (expected_nodes_count == 0)
			appendPQExpBufferStr(&details,
								 "this node has no downstream nodes");
		else
			appendPQExpBuffer(&details,
							  "%i of %i downstream nodes attached",
							  expected_nodes_count - missing_nodes_count,
							  expected_nodes_count);
	}
	else
	{
		ItemListCell *missing_cell = NULL;
		bool		first = true;

		status = CHECK_STATUS_CRITICAL;

		appendPQExpBuffer(&details,
						  "%i of %i downstream nodes not attached",
						  missing_nodes_count,
						  expected_nodes_count);

		if (mode != OM_NAGIOS)
		{
			appendPQExpBufferStr(&details, "; missing: ");

			for (missing_cell = missing_nodes.head; missing_cell; missing_cell = missing_cell->next)
			{
				if (first == false)
					appendPQExpBufferStr(&details,
										 ", ");
				else
					first = false;

				if (first == false)
					appendPQExpBufferStr(&details, missing_cell->string);
			}
		}
	}

	switch (mode)
	{
		case OM_NAGIOS:
			{
				printf("REPMGR_DOWNSTREAM_SERVERS %s: %s | ",
					   output_check_status(status),
					   details.data);

				if (missing_nodes_count)
				{
					ItemListCell *missing_cell = NULL;
					bool		first = true;

					printf("missing: ");
					for (missing_cell = missing_nodes.head; missing_cell; missing_cell = missing_cell->next)
					{
						if (first == false)
							printf(", ");
						else
							first = false;

						if (first == false)
							printf("%s", missing_cell->string);
					}
				}

				if (expected_nodes_count - missing_nodes_count)
				{
					ItemListCell *attached_cell = NULL;
					bool		first = true;

					if (missing_nodes_count)
						printf("; ");
					printf("attached: ");
					for (attached_cell = attached_nodes.head; attached_cell; attached_cell = attached_cell->next)
					{
						if (first == false)
							printf(", ");
						else
							first = false;

						if (first == false)
							printf("%s", attached_cell->string);
					}
				}
				printf("\n");

			}
			break;
		case OM_CSV:
		case OM_TEXT:
			if (list_output != NULL)
			{
				check_status_list_set(list_output,
									  "Downstream servers",
									  status,
									  details.data);
			}
			else
			{
				printf("%s (%s)\n",
					   output_check_status(status),
					   details.data);
			}
		default:
			break;

	}
	termPQExpBuffer(&details);
	clear_node_info_list(&downstream_nodes);
	return status;
}


static CheckStatus
do_node_check_upstream(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output)
{
	PGconn	   *upstream_conn = NULL;
	t_node_info upstream_node_info = T_NODE_INFO_INITIALIZER;
	PQExpBufferData details;

	CheckStatus status = CHECK_STATUS_OK;

	if (mode == OM_CSV && list_output == NULL)
	{
		log_error(_("--csv output not provided with --upstream option"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	initPQExpBuffer(&details);

	if (node_info->type == WITNESS)
	{
		/* witness is not connecting to any upstream */
		appendPQExpBufferStr(&details,
							 _("N/A - node is a witness"));
	}
	else if (get_node_record(conn, node_info->upstream_node_id, &upstream_node_info) != RECORD_FOUND)
	{
		if (get_recovery_type(conn) == RECTYPE_STANDBY)
		{
			appendPQExpBuffer(&details,
							  _("node \"%s\" (ID: %i) is a standby but no upstream record found"),
							  node_info->node_name,
							  node_info->node_id);
			status = CHECK_STATUS_CRITICAL;
		}
		else
		{
			appendPQExpBufferStr(&details,
								 _("N/A - node is primary"));
		}
	}
	else
	{
		upstream_conn = establish_db_connection(upstream_node_info.conninfo, true);

		/* check our node is connected */
		if (is_downstream_node_attached(upstream_conn, config_file_options.node_name, NULL) != NODE_ATTACHED)
		{
			appendPQExpBuffer(&details,
							  _("node \"%s\" (ID: %i) is not attached to expected upstream node \"%s\" (ID: %i)"),
							  node_info->node_name,
							  node_info->node_id,
							  upstream_node_info.node_name,
							  upstream_node_info.node_id);
			status = CHECK_STATUS_CRITICAL;
		}
		else
		{
			appendPQExpBuffer(&details,
							  _("node \"%s\" (ID: %i) is attached to expected upstream node \"%s\" (ID: %i)"),
							  node_info->node_name,
							  node_info->node_id,
							  upstream_node_info.node_name,
							  upstream_node_info.node_id);
		}
	}

	switch (mode)
	{
		case OM_NAGIOS:
			{
				printf("REPMGR_UPSTREAM_SERVER %s: %s | ",
					   output_check_status(status),
					   details.data);
			}
			break;
		case OM_TEXT:
			if (list_output != NULL)
			{
				check_status_list_set(list_output,
									  "Upstream connection",
									  status,
									  details.data);
			}
			else
			{
				printf("%s (%s)\n",
					   output_check_status(status),
					   details.data);
			}
		default:
			break;
	}

	termPQExpBuffer(&details);

	return status;
}


static CheckStatus
do_node_check_replication_lag(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output)
{
	CheckStatus status = CHECK_STATUS_OK;
	int			lag_seconds = 0;
	PQExpBufferData details;

	if (mode == OM_CSV && list_output == NULL)
	{
		log_error(_("--csv output not provided with --replication-lag option"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	initPQExpBuffer(&details);

	if (node_info->recovery_type == RECTYPE_PRIMARY)
	{
		switch (mode)
		{
			case OM_OPTFORMAT:
				appendPQExpBufferStr(&details,
									 "--lag=0");
				break;
			case OM_NAGIOS:
				appendPQExpBuffer(&details,
								  "0 seconds | lag=0;%i;%i",
								  config_file_options.replication_lag_warning,
								  config_file_options.replication_lag_critical);
				break;
			case OM_TEXT:
				if (node_info->type == WITNESS)
				{
					appendPQExpBufferStr(&details,
										 "N/A - node is witness");
				}
				else
				{
					appendPQExpBufferStr(&details,
										 "N/A - node is primary");
				}
				break;
			default:
				break;
		}
	}
	else
	{
		lag_seconds = get_replication_lag_seconds(conn);

		log_debug("lag seconds: %i", lag_seconds);

		if (lag_seconds >= config_file_options.replication_lag_critical)
		{
			status = CHECK_STATUS_CRITICAL;

			switch (mode)
			{
				case OM_OPTFORMAT:
					appendPQExpBuffer(&details,
									  "--lag=%i --threshold=%i",
									  lag_seconds, config_file_options.replication_lag_critical);
					break;
				case OM_NAGIOS:
					appendPQExpBuffer(&details,
									  "%i seconds | lag=%i;%i;%i",
									  lag_seconds,
									  lag_seconds,
									  config_file_options.replication_lag_warning,
									  config_file_options.replication_lag_critical);
					break;
				case OM_TEXT:
					appendPQExpBuffer(&details,
									  "%i seconds, critical threshold: %i)",
									  lag_seconds, config_file_options.replication_lag_critical);
					break;

				default:
					break;
			}
		}
		else if (lag_seconds > config_file_options.replication_lag_warning)
		{
			status = CHECK_STATUS_WARNING;

			switch (mode)
			{
				case OM_OPTFORMAT:
					appendPQExpBuffer(&details,
									  "--lag=%i --threshold=%i",
									  lag_seconds, config_file_options.replication_lag_warning);
					break;
				case OM_NAGIOS:
					appendPQExpBuffer(&details,
									  "%i seconds | lag=%i;%i;%i",
									  lag_seconds,
									  lag_seconds,
									  config_file_options.replication_lag_warning,
									  config_file_options.replication_lag_critical);
					break;
				case OM_TEXT:
					appendPQExpBuffer(&details,
									  "%i seconds, warning threshold: %i)",
									  lag_seconds, config_file_options.replication_lag_warning);
					break;

				default:
					break;
			}
		}
		else if (lag_seconds == UNKNOWN_REPLICATION_LAG)
		{
			status = CHECK_STATUS_UNKNOWN;

			switch (mode)
			{
				case OM_OPTFORMAT:
					break;
				case OM_NAGIOS:
				case OM_TEXT:
					appendPQExpBufferStr(&details,
										 "unable to query replication lag");
					break;

				default:
					break;
			}
		}
		else
		{
			status = CHECK_STATUS_OK;

			switch (mode)
			{
				case OM_OPTFORMAT:
					appendPQExpBuffer(&details,
									  "--lag=%i",
									  lag_seconds);
					break;
				case OM_NAGIOS:
					appendPQExpBuffer(&details,
									  "%i seconds | lag=%i;%i;%i",
									  lag_seconds,
									  lag_seconds,
									  config_file_options.replication_lag_warning,
									  config_file_options.replication_lag_critical);
					break;
				case OM_TEXT:
					appendPQExpBuffer(&details,
									  "%i seconds",
									  lag_seconds);
					break;

				default:
					break;
			}
		}
	}

	switch (mode)
	{
		case OM_OPTFORMAT:
			printf("--status=%s %s\n",
				   output_check_status(status),
				   details.data);
			break;
		case OM_NAGIOS:
			printf("REPMGR_REPLICATION_LAG %s: %s\n",
				   output_check_status(status),
				   details.data);
			break;
		case OM_CSV:
		case OM_TEXT:
			if (list_output != NULL)
			{
				check_status_list_set(list_output,
									  "Replication lag",
									  status,
									  details.data);
			}
			else
			{
				printf("%s (%s)\n",
					   output_check_status(status),
					   details.data);
			}
		default:
			break;
	}

	termPQExpBuffer(&details);

	return status;
}


static CheckStatus
do_node_check_role(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output)
{

	CheckStatus status = CHECK_STATUS_OK;
	PQExpBufferData details;
	RecoveryType recovery_type = get_recovery_type(conn);

	if (mode == OM_CSV && list_output == NULL)
	{
		log_error(_("--csv output not provided with --role option"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	initPQExpBuffer(&details);

	switch (node_info->type)
	{
		case PRIMARY:
			if (recovery_type == RECTYPE_STANDBY)
			{
				status = CHECK_STATUS_CRITICAL;
				appendPQExpBufferStr(&details,
									 _("node is registered as primary but running as standby"));
			}
			else
			{
				appendPQExpBufferStr(&details,
									 _("node is primary"));
			}
			break;
		case STANDBY:
			if (recovery_type == RECTYPE_PRIMARY)
			{
				status = CHECK_STATUS_CRITICAL;
				appendPQExpBufferStr(&details,
									 _("node is registered as standby but running as primary"));
			}
			else
			{
				appendPQExpBufferStr(&details,
									 _("node is standby"));
			}
			break;
		case WITNESS:
			if (recovery_type == RECTYPE_STANDBY)
			{
				status = CHECK_STATUS_CRITICAL;
				appendPQExpBufferStr(&details,
									 _("node is registered as witness but running as standby"));
			}
			else
			{
				appendPQExpBufferStr(&details,
									 _("node is witness"));
			}
			break;
		default:
			break;
	}

	switch (mode)
	{
		case OM_NAGIOS:
			printf("REPMGR_SERVER_ROLE %s: %s\n",
				   output_check_status(status),
				   details.data);
			break;
		case OM_CSV:
		case OM_TEXT:
			if (list_output != NULL)
			{
				check_status_list_set(list_output,
									  "Server role",
									  status,
									  details.data);
			}
			else
			{
				printf("%s (%s)\n",
					   output_check_status(status),
					   details.data);
			}
		default:
			break;
	}

	termPQExpBuffer(&details);
	return status;

}


static CheckStatus
do_node_check_slots(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output)
{
	CheckStatus status = CHECK_STATUS_OK;
	PQExpBufferData details;

	if (mode == OM_CSV && list_output == NULL)
	{
		log_error(_("--csv output not provided with --slots option"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	initPQExpBuffer(&details);

	if (node_info->total_replication_slots == 0)
	{
		appendPQExpBufferStr(&details,
							 _("node has no physical replication slots"));
	}
	else if (node_info->inactive_replication_slots == 0)
	{
		appendPQExpBuffer(&details,
						  _("%i of %i physical replication slots are active"),
						  node_info->total_replication_slots,
						  node_info->total_replication_slots);
	}
	else if (node_info->inactive_replication_slots > 0)
	{
		status = CHECK_STATUS_CRITICAL;

		appendPQExpBuffer(&details,
						  _("%i of %i physical replication slots are inactive"),
						  node_info->inactive_replication_slots,
						  node_info->total_replication_slots);
	}

	switch (mode)
	{
		case OM_NAGIOS:
			printf("REPMGR_INACTIVE_SLOTS %s: %s | slots=%i;%i\n",
				   output_check_status(status),
				   details.data,
				   node_info->total_replication_slots,
				   node_info->inactive_replication_slots);
			break;
		case OM_CSV:
		case OM_TEXT:
			if (list_output != NULL)
			{
				check_status_list_set(list_output,
									  "Replication slots",
									  status,
									  details.data);
			}
			else
			{
				printf("%s (%s)\n",
					   output_check_status(status),
					   details.data);
			}
		default:
			break;
	}

	termPQExpBuffer(&details);
	return status;
}


static CheckStatus
do_node_check_missing_slots(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output)
{
	CheckStatus status = CHECK_STATUS_OK;
	PQExpBufferData details;
	NodeInfoList missing_slots = T_NODE_INFO_LIST_INITIALIZER;

	if (mode == OM_CSV && list_output == NULL)
	{
		log_error(_("--csv output not provided with --missing-slots option"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	initPQExpBuffer(&details);

	get_downstream_nodes_with_missing_slot(conn,
										   config_file_options.node_id,
										   &missing_slots);

	if (missing_slots.node_count == 0)
	{
		appendPQExpBufferStr(&details,
							 _("node has no missing physical replication slots"));
	}
	else
	{
		NodeInfoListCell *missing_slot_cell = NULL;
		bool first_element = true;

		status = CHECK_STATUS_CRITICAL;

		appendPQExpBuffer(&details,
						  _("%i physical replication slots are missing"),
						  missing_slots.node_count);

		if (missing_slots.node_count)
		{
			appendPQExpBufferStr(&details, ": ");

			for (missing_slot_cell = missing_slots.head; missing_slot_cell; missing_slot_cell = missing_slot_cell->next)
			{
				if (first_element == true)
				{
					first_element = false;
				}
				else
				{
					appendPQExpBufferStr(&details, ", ");
				}

				appendPQExpBufferStr(&details, missing_slot_cell->node_info->slot_name);
			}
		}
	}

	switch (mode)
	{
		case OM_NAGIOS:
		{
			printf("REPMGR_MISSING_SLOTS %s: %s | missing_slots=%i",
				   output_check_status(status),
				   details.data,
				   missing_slots.node_count);

			if (missing_slots.node_count)
			{
				NodeInfoListCell *missing_slot_cell = NULL;
				bool first_element = true;

				printf(";");

				for (missing_slot_cell = missing_slots.head; missing_slot_cell; missing_slot_cell = missing_slot_cell->next)
				{
					if (first_element == true)
					{
						first_element = false;
					}
					else
					{
						printf(",");
					}
					printf("%s", missing_slot_cell->node_info->slot_name);
				}
			}
			printf("\n");
			break;
		}
		case OM_CSV:
		case OM_TEXT:
			if (list_output != NULL)
			{
				check_status_list_set(list_output,
									  "Missing physical replication slots",
									  status,
									  details.data);
			}
			else
			{
				printf("%s (%s)\n",
					   output_check_status(status),
					   details.data);
			}
		default:
			break;
	}

	clear_node_info_list(&missing_slots);

	termPQExpBuffer(&details);
	return status;
}


CheckStatus
do_node_check_data_directory(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output)
{
	CheckStatus status = CHECK_STATUS_OK;
	char actual_data_directory[MAXPGPATH] = "";
	PQExpBufferData details;

	if (mode == OM_CSV && list_output == NULL)
	{
		log_error(_("--csv output not provided with --data-directory-config option"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	initPQExpBuffer(&details);

	/*
	 * Check actual data directory matches that in repmgr.conf; note this requires
	 * a superuser connection
	 */
	if (connection_has_pg_monitor_role(conn, "pg_read_all_settings") == true)
	{
		/* we expect to have a database connection */
		if (get_pg_setting(conn, "data_directory", actual_data_directory) == false)
		{
			appendPQExpBuffer(&details,
							  _("unable to determine current \"data_directory\""));
			status = CHECK_STATUS_UNKNOWN;
		}

		if (strncmp(actual_data_directory, config_file_options.data_directory, MAXPGPATH) != 0)
		{
			if (mode != OM_NAGIOS)
			{
				appendPQExpBuffer(&details,
								  _("configured \"data_directory\" is \"%s\"; "),
								  config_file_options.data_directory);
			}

			appendPQExpBuffer(&details,
							  "actual data directory is \"%s\"",
							  actual_data_directory);

			status = CHECK_STATUS_CRITICAL;
		}
		else
		{
			appendPQExpBuffer(&details,
							  _("configured \"data_directory\" is \"%s\""),
							  config_file_options.data_directory);
		}
	}
	/*
	 * If no superuser connection available, sanity-check that the configuration directory looks
	 * like a PostgreSQL directory and hope it's the right one.
	 */
	else
	{
		if (mode == OM_TEXT)
		{
			log_info(_("connection is not a superuser connection, falling back to simple check"));

			if (PQserverVersion(conn) >= 100000)
			{
				log_hint(_("provide a superuser with -S/--superuser, or add the \"%s\" user to role \"pg_read_all_settings\" or \"pg_monitor\""),
						   PQuser(conn));
			}
		}

		if (is_pg_dir(config_file_options.data_directory) == false)
		{
			if (mode == OM_NAGIOS)
			{
				appendPQExpBufferStr(&details,
								  _("configured \"data_directory\" is not a PostgreSQL data directory"));
			}
			else
			{
				appendPQExpBuffer(&details,
								  _("configured \"data_directory\" \"%s\" is not a PostgreSQL data directory"),
								  actual_data_directory);
			}

			status = CHECK_STATUS_CRITICAL;
		}
		else
		{
			appendPQExpBuffer(&details,
							  _("configured \"data_directory\" is \"%s\""),
							  config_file_options.data_directory);
		}
	}

	switch (mode)
	{
		case OM_OPTFORMAT:
			printf("--configured-data-directory=%s\n",
				   output_check_status(status));
			break;
		case OM_NAGIOS:
			printf("REPMGR_DATA_DIRECTORY %s: %s",
				   output_check_status(status),
				   config_file_options.data_directory);

			if (status == CHECK_STATUS_CRITICAL)
			{
				printf(" | %s", details.data);
			}
			puts("");
			break;
		case OM_CSV:
		case OM_TEXT:
			if (list_output != NULL)
			{
				check_status_list_set(list_output,
									  "Configured data directory",
									  status,
									  details.data);
			}
			else
			{
				printf("%s (%s)\n",
					   output_check_status(status),
					   details.data);
			}
		default:
			break;
	}

	termPQExpBuffer(&details);

	return status;
}

/*
 * This is not included in the general list output
 */
static
CheckStatus do_node_check_replication_config_owner(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output)
{
	CheckStatus status = CHECK_STATUS_OK;

	PQExpBufferData errmsg;
	PQExpBufferData details;

	if (mode != OM_OPTFORMAT)
	{
		log_error(_("--replication-config-owner option can only be used with --optformat"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	initPQExpBuffer(&errmsg);
	initPQExpBuffer(&details);

	if (check_replication_config_owner(PQserverVersion(conn),
									   config_file_options.data_directory,
									   &errmsg, &details) == false)
	{
		status = CHECK_STATUS_CRITICAL;
	}

	printf("--replication-config-owner=%s\n",
		   output_check_status(status));

	return status;
}


/*
 * This is not included in the general list output
 */
static CheckStatus
do_node_check_db_connection(PGconn *conn, OutputMode mode)
{
	CheckStatus status = CHECK_STATUS_OK;
	PQExpBufferData details;

	if (mode == OM_CSV)
	{
		log_error(_("--csv output not provided with --db-connection option"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* This check is for configuration diagnostics only */
	if (mode == OM_NAGIOS)
	{
		log_error(_("--nagios output not provided with --db-connection option"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	initPQExpBuffer(&details);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		t_conninfo_param_list conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;
		int c;

		status = CHECK_STATUS_CRITICAL;
		initialize_conninfo_params(&conninfo, false);
		conn_to_param_list(conn, &conninfo);

		appendPQExpBufferStr(&details,
							 "connection parameters used:");
		for (c = 0; c < conninfo.size && conninfo.keywords[c] != NULL; c++)
		{
			if (conninfo.values[c] != NULL && conninfo.values[c][0] != '\0')
			{
				appendPQExpBuffer(&details,
								  " %s=%s",
								  conninfo.keywords[c], conninfo.values[c]);
			}
		}

	}

	if (mode == OM_OPTFORMAT)
	{
		printf("--db-connection=%s\n",
			   output_check_status(status));
	}
	else if (mode == OM_TEXT)
	{
		printf("%s (%s)\n",
			   output_check_status(status),
			   details.data);
	}
	termPQExpBuffer(&details);

	return status;
}


void
do_node_service(void)
{
	t_server_action action = ACTION_UNKNOWN;
	char		data_dir[MAXPGPATH] = "";
	char		command[MAXLEN] = "";
	PQExpBufferData output;

	action = parse_server_action(runtime_options.action);

	if (action == ACTION_UNKNOWN)
	{
		log_error(_("unknown value \"%s\" provided for parameter --action"),
				  runtime_options.action);
		log_hint(_("valid values are \"start\", \"stop\", \"restart\", \"reload\" and \"promote\""));
		exit(ERR_BAD_CONFIG);
	}

	if (runtime_options.list_actions == true)
	{
		return _do_node_service_list_actions(action);
	}


	if (data_dir_required_for_action(action))
	{
		get_node_config_directory(data_dir);

		if (data_dir[0] == '\0')
		{
			log_error(_("unable to determine data directory for action"));
			exit(ERR_BAD_CONFIG);
		}
	}


	if ((action == ACTION_STOP || action == ACTION_RESTART) && runtime_options.checkpoint == true)
	{
		PGconn	   *conn = NULL;

		if (config_file_options.conninfo[0] != '\0')
		{
			/*
			 * If --superuser option provided, attempt to connect as the specified user
			 */
			if (runtime_options.superuser[0] != '\0')
			{
				conn = establish_db_connection_with_replacement_param(
					config_file_options.conninfo,
					"user",
					runtime_options.superuser,
					true);
			}
			else
			{
				conn = establish_db_connection(config_file_options.conninfo, true);
			}
		}
		else
		{
			conn = establish_db_connection_by_params(&source_conninfo, true);
		}

		if (is_superuser_connection(conn, NULL) == false)
		{
			if (runtime_options.dry_run == true)
			{
				log_warning(_("a CHECKPOINT would be issued here but no superuser connection is available"));
			}
			else
			{
				log_warning(_("a superuser connection is required to issue a CHECKPOINT"));
			}

			log_hint(_("provide a superuser with -S/--superuser"));
		}
		else
		{
			if (runtime_options.dry_run == true)
			{
				log_info(_("a CHECKPOINT would be issued here"));
			}
			else
			{

				log_notice(_("issuing CHECKPOINT on node \"%s\" (ID: %i) "),
						   config_file_options.node_name,
						   config_file_options.node_id);

				checkpoint(conn);
			}
		}

		PQfinish(conn);
	}

	get_server_action(action, command, data_dir);

	if (runtime_options.dry_run == true)
	{
		log_info(_("would execute server command \"%s\""), command);
		return;
	}

	/*
	 * log level is "DETAIL" here as this command is intended to be executed
	 * by another repmgr process (e.g. during standby switchover); that repmgr
	 * should emit a "NOTICE" about the intent of the command.
	 */
	log_detail(_("executing server command \"%s\""), command);

	initPQExpBuffer(&output);

	if (local_command(command, &output) == false)
	{
		termPQExpBuffer(&output);
		exit(ERR_LOCAL_COMMAND);
	}

	termPQExpBuffer(&output);
}


static void
_do_node_service_list_actions(t_server_action action)
{
	char		command[MAXLEN] = "";

	char		data_dir[MAXPGPATH] = "";

	bool		data_dir_required = false;

	/* do we need to provide a data directory for any of the actions? */
	if (data_dir_required_for_action(ACTION_START))
		data_dir_required = true;

	if (data_dir_required_for_action(ACTION_STOP))
		data_dir_required = true;

	if (data_dir_required_for_action(ACTION_RESTART))
		data_dir_required = true;

	if (data_dir_required_for_action(ACTION_RELOAD))
		data_dir_required = true;

	if (data_dir_required_for_action(ACTION_PROMOTE))
		data_dir_required = true;

	if (data_dir_required == true)
	{
		get_node_config_directory(data_dir);
	}

	/* show command for specific action only */
	if (action != ACTION_NONE)
	{
		get_server_action(action, command, data_dir);
		printf("%s\n", command);
		return;
	}

	puts(_("Following commands would be executed for each action:"));
	puts("");

	get_server_action(ACTION_START, command, data_dir);
	printf("    start: \"%s\"\n", command);

	get_server_action(ACTION_STOP, command, data_dir);
	printf("     stop: \"%s\"\n", command);

	get_server_action(ACTION_RESTART, command, data_dir);
	printf("  restart: \"%s\"\n", command);

	get_server_action(ACTION_RELOAD, command, data_dir);
	printf("   reload: \"%s\"\n", command);

	get_server_action(ACTION_PROMOTE, command, data_dir);
	printf("  promote: \"%s\"\n", command);

	puts("");

}


static t_server_action
parse_server_action(const char *action_name)
{
	if (action_name[0] == '\0')
		return ACTION_NONE;

	if (strcasecmp(action_name, "start") == 0)
		return ACTION_START;

	if (strcasecmp(action_name, "stop") == 0)
		return ACTION_STOP;

	if (strcasecmp(action_name, "restart") == 0)
		return ACTION_RESTART;

	if (strcasecmp(action_name, "reload") == 0)
		return ACTION_RELOAD;

	if (strcasecmp(action_name, "promote") == 0)
		return ACTION_PROMOTE;

	return ACTION_UNKNOWN;
}



/*
 * Rejoin a dormant (shut down) node to the replication cluster; this
 * is typically a former primary which needs to be demoted to a standby.
 *
 * Note that "repmgr node rejoin" is also executed by
 * "repmgr standby switchover" after promoting the new primary.
 *
 * Parameters:
 *   --dry-run
 *   --force-rewind[=VALUE]
 *   --config-files
 *   --config-archive-dir
 *   -W/--no-wait
 */
void
do_node_rejoin(void)
{
	PGconn	   *upstream_conn = NULL;
	RecoveryType primary_recovery_type = RECTYPE_UNKNOWN;
	PGconn	   *primary_conn = NULL;

	DBState		db_state;
	PGPing		status;
	bool		is_shutdown = true;
	int			server_version_num = UNKNOWN_SERVER_VERSION_NUM;
	bool		hide_standby_signal = true;

	PQExpBufferData command;
	PQExpBufferData command_output;
	PQExpBufferData follow_output;
	struct stat statbuf;
	t_node_info primary_node_record = T_NODE_INFO_INITIALIZER;
	t_node_info local_node_record = T_NODE_INFO_INITIALIZER;

	bool		success = true;
	int			follow_error_code = SUCCESS;

	/* check node is not actually running */
	status = PQping(config_file_options.conninfo);

	switch (status)
	{
		case PQPING_NO_ATTEMPT:
			log_error(_("unable to determine status of server"));
			exit(ERR_BAD_CONFIG);
		case PQPING_OK:
			is_shutdown = false;
			break;
		case PQPING_REJECT:
			is_shutdown = false;
			break;
		case PQPING_NO_RESPONSE:
			/* status not yet clear */
			break;
	}

	if (get_db_state(config_file_options.data_directory, &db_state) == false)
	{
		log_error(_("unable to determine database state from pg_control"));
		exit(ERR_BAD_CONFIG);
	}

	if (is_shutdown == false)
	{
		log_error(_("database is still running in state \"%s\""),
				  describe_db_state(db_state));
		log_hint(_("\"repmgr node rejoin\" cannot be executed on a running node"));
		exit(ERR_REJOIN_FAIL);
	}

	/*
	 * Server version number required to determine whether pg_rewind will run
	 * crash recovery (Pg 13 and later).
	 */
	server_version_num = get_pg_version(config_file_options.data_directory, NULL);

	if (server_version_num == UNKNOWN_SERVER_VERSION_NUM)
	{
		/* This is very unlikely to happen */
		log_error(_("unable to determine database version"));
		exit(ERR_BAD_CONFIG);
	}

	log_verbose(LOG_DEBUG, "server version number is: %i", server_version_num);

	/* check if cleanly shut down */
	if (db_state != DB_SHUTDOWNED && db_state != DB_SHUTDOWNED_IN_RECOVERY)
	{
		if (db_state == DB_SHUTDOWNING)
		{
			log_error(_("database is still shutting down"));
		}
		else if (server_version_num >= 130000 && runtime_options.force_rewind_used == true)
		{
			log_warning(_("database is not shut down cleanly"));
			log_detail(_("--force-rewind provided, pg_rewind will automatically perform recovery"));

			/*
			 * If pg_rewind is executed, the first change it will make
			 * is to start the server in single user mode, which will fail
			 * in the presence of "standby.signal", so we'll "hide" it
			 * (actually delete and recreate).
			 */
			hide_standby_signal = true;
		}
		else
		{
			/*
			 * If the database was not shut down cleanly, it *might* rejoin correctly
			 * after starting up and recovering, but better to ensure the database
			 * can recover before trying anything else.
			 */
			log_error(_("database is not shut down cleanly"));

			if (server_version_num >= 130000)
			{
				log_hint(_("provide --force-rewind to run recovery"));
			}
			else
			{
				if (runtime_options.force_rewind_used == true)
				{
					log_detail(_("pg_rewind will not be able to run"));
				}
				log_hint(_("database should be restarted then shut down cleanly after crash recovery completes"));
			}

			exit(ERR_REJOIN_FAIL);
		}
	}

	/* check provided upstream connection */
	upstream_conn = establish_db_connection_by_params(&source_conninfo, true);

	if (get_primary_node_record(upstream_conn, &primary_node_record) == false)
	{
		log_error(_("unable to retrieve primary node record"));
		log_hint(_("check the provided database connection string is for a \"repmgr\" database"));
		PQfinish(upstream_conn);
		exit(ERR_BAD_CONFIG);
	}

	/* connect to registered primary and check it's not in recovery */
	primary_conn = establish_db_connection(primary_node_record.conninfo, false);

	if (PQstatus(primary_conn) != CONNECTION_OK)
	{
		RecoveryType upstream_recovery_type = get_recovery_type(upstream_conn);

		log_error(_("unable to connect to current registered primary \"%s\" (ID: %i)"),
				  primary_node_record.node_name,
				  primary_node_record.node_id);
		log_detail(_("registered primary node conninfo is: \"%s\""),
				   primary_node_record.conninfo);
		/*
		 * Catch case where provided upstream is not in recovery, but is also
		 * not registered as primary
		 */

		if (upstream_recovery_type == RECTYPE_PRIMARY)
		{
			log_warning(_("provided upstream connection string is for a server which is not in recovery, but not registered as primary"));
			log_hint(_("fix repmgr metadata configuration before continuing"));
		}

		PQfinish(upstream_conn);
		exit(ERR_BAD_CONFIG);
	}

	PQfinish(upstream_conn);

	primary_recovery_type = get_recovery_type(primary_conn);

	if (primary_recovery_type != RECTYPE_PRIMARY)
	{
		log_error(_("primary server is registered as node \"%s\" (ID: %i), but server is not a primary"),
				  primary_node_record.node_name,
				  primary_node_record.node_id);
		/* TODO: hint about checking cluster */
		PQfinish(primary_conn);

		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Fetch the local node record - we'll need this later, and it acts as an
	 * additional sanity-check that the node is known to the primary.
	 */
	if (get_node_record(primary_conn, config_file_options.node_id, &local_node_record) != RECORD_FOUND)
	{
		log_error(_("unable to retrieve node record for the local node"));
		log_hint(_("check the local node is registered with the current primary \"%s\" (ID: %i)"),
				 primary_node_record.node_name,
				 primary_node_record.node_id);

		PQfinish(primary_conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Sanity-check replication slot availability
	 */
	if (config_file_options.use_replication_slots)
	{
		bool slots_available = check_replication_slots_available(primary_node_record.node_id,
																 primary_conn);
		if (slots_available == false)
		{
			PQfinish(primary_conn);
			exit(ERR_BAD_CONFIG);
		}
	}


	/*
	 * sanity-check that it will actually be possible to stream from the new upstream
	 */
	{
		bool can_follow;
		TimeLineID tli = get_min_recovery_end_timeline(config_file_options.data_directory);
		XLogRecPtr min_recovery_location = get_min_recovery_location(config_file_options.data_directory);

		/*
		 * It's possible this was a former primary, so the minRecoveryPoint*
		 * fields may be empty.
		 */

		if (min_recovery_location == InvalidXLogRecPtr)
			min_recovery_location = get_latest_checkpoint_location(config_file_options.data_directory);
		if (tli == 0)
			tli = get_timeline(config_file_options.data_directory);

		can_follow = check_node_can_attach(tli,
										   min_recovery_location,
										   primary_conn,
										   &primary_node_record,
										   true);

		if (can_follow == false)
		{
			PQfinish(primary_conn);
			exit(ERR_REJOIN_FAIL);
		}
	}


	/*
	 * --force-rewind specified - check prerequisites, and attempt to execute
  	 * (if --dry-run provided, just output the command which would be executed)
	 */

	if (runtime_options.force_rewind_used == true)
	{
		PQExpBufferData msg;
		PQExpBufferData	filebuf;
		int				ret;

		/*
		 * Check that pg_rewind can be used
		 */

		initPQExpBuffer(&msg);

		if (can_use_pg_rewind(primary_conn, config_file_options.data_directory, &msg) == false)
		{
			log_error(_("--force-rewind specified but pg_rewind cannot be used"));
			log_detail("%s", msg.data);
			termPQExpBuffer(&msg);
			PQfinish(primary_conn);

			exit(ERR_BAD_CONFIG);
		}

		appendPQExpBufferStr(&msg,
							 _("prerequisites for using pg_rewind are met"));

		if (runtime_options.dry_run == true)
		{
			log_info("%s", msg.data);
		}
		else
		{
			log_verbose(LOG_INFO, "%s", msg.data);
		}
		termPQExpBuffer(&msg);

		/*
		 * Archive requested configuration files.
		 *
		 * In --dry-run mode this acts as a check that the files can be archived, though
		 * errors will only be logged; any copied files will be deleted and --dry-run
		 * execution will continue.
		 */
		_do_node_archive_config();

		/* execute pg_rewind */
		initPQExpBuffer(&command);

		if (runtime_options.force_rewind_path[0] != '\0')
		{
			appendPQExpBuffer(&command,
							  "%s -D ",
							  runtime_options.force_rewind_path);
		}
		else
		{
			make_pg_path(&command, "pg_rewind");
			appendPQExpBufferStr(&command,
								 " -D ");
		}

		appendShellString(&command,
						  config_file_options.data_directory);

		appendPQExpBuffer(&command,
						  " --source-server='%s'",
						  primary_node_record.conninfo);

		if (runtime_options.dry_run == true)
		{
			log_info(_("pg_rewind would now be executed"));
			log_detail(_("pg_rewind command is:\n  %s"),
						 command.data);
		}
		else
		{
			log_notice(_("executing pg_rewind"));
			log_detail(_("pg_rewind command is \"%s\""),
					   command.data);

			/*
			 * In Pg13 and later, pg_rewind will attempt to start up a server which
			 * was not cleanly shut down in single user mode. This will fail if
			 * "standby.signal" is present. We'll remove it and restore it after
			 * pg_rewind runs.
			 */
			if (hide_standby_signal == true)
			{
				char	    standby_signal_file_path[MAXPGPATH] = "";

				log_notice(_("temporarily removing \"standby.signal\""));
				log_detail(_("this is required so pg_rewind can fix the unclean shutdown"));

				make_standby_signal_path(standby_signal_file_path);

				if (unlink(standby_signal_file_path) < 0 && errno != ENOENT)
				{
					log_error(_("unable to remove \"standby.signal\" file in data directory \"%s\""),
							  standby_signal_file_path);
					log_detail("%s", strerror(errno));
					exit(ERR_REJOIN_FAIL);
				}
			}

			initPQExpBuffer(&command_output);

			ret = local_command(command.data,
								&command_output);

			termPQExpBuffer(&command);

			if (hide_standby_signal == true)
			{
				/*
				 * Restore standby.signal if we previously removed it, regardless
				 * of whether the pg_rewind operation failed.
				 */
				log_notice(_("recreating \"standby.signal\""));
				write_standby_signal();
			}

			if (ret == false)
			{
				log_error(_("unable to execute pg_rewind"));
				log_detail("%s", command_output.data);

				termPQExpBuffer(&command_output);

				exit(ERR_REJOIN_FAIL);
			}

			termPQExpBuffer(&command_output);

			/* Restore any previously archived config files */
			_do_node_restore_config();

			initPQExpBuffer(&filebuf);

			/* remove any recovery.done file copied in by pg_rewind */
			appendPQExpBuffer(&filebuf,
							  "%s/recovery.done",
							  config_file_options.data_directory);

			if (stat(filebuf.data, &statbuf) == 0)
			{
				log_verbose(LOG_INFO, _("deleting \"recovery.done\""));

				if (unlink(filebuf.data) == -1)
				{
					log_warning(_("unable to delete \"%s\""),
								filebuf.data);
					log_detail("%s", strerror(errno));
				}
			}
			termPQExpBuffer(&filebuf);

			/*
			 * Delete any replication slots copied in by pg_rewind.
			 *
			 * TODO:
			 *  - from PostgreSQL 11, this will be handled by pg_rewind, so
			 *    we can skip this step from that version; see commit
			 *    266b6acb312fc440c1c1a2036aa9da94916beac6
			 *  - possibly delete contents of various other directories
			 *    as per the above commit for pre-PostgreSQL 11
			 */
			{
				PQExpBufferData slotdir_path;
				DIR			  *slotdir;
				struct dirent *slotdir_ent;

				initPQExpBuffer(&slotdir_path);

				appendPQExpBuffer(&slotdir_path,
								  "%s/pg_replslot",
								  config_file_options.data_directory);

				slotdir = opendir(slotdir_path.data);

				if (slotdir == NULL)
				{
					log_warning(_("unable to open replication slot directory \"%s\""),
								slotdir_path.data);
					log_detail("%s", strerror(errno));
				}
				else
				{
					while ((slotdir_ent = readdir(slotdir)) != NULL) {
						struct stat statbuf;
						PQExpBufferData slotdir_ent_path;

						if (strcmp(slotdir_ent->d_name, ".") == 0 || strcmp(slotdir_ent->d_name, "..") == 0)
							continue;

						initPQExpBuffer(&slotdir_ent_path);

						appendPQExpBuffer(&slotdir_ent_path,
										  "%s/%s",
										  slotdir_path.data,
										  slotdir_ent->d_name);

						if (stat(slotdir_ent_path.data, &statbuf) == 0 && !S_ISDIR(statbuf.st_mode))
						{
							termPQExpBuffer(&slotdir_ent_path);
							continue;
						}

						log_debug("deleting slot directory \"%s\"", slotdir_ent_path.data);
						if (rmdir_recursive(slotdir_ent_path.data) != 0 && errno != EEXIST)
						{
							log_warning(_("unable to delete replication slot directory \"%s\""), slotdir_ent_path.data);
							log_detail("%s", strerror(errno));
							log_hint(_("directory may need to be manually removed"));
						}

						termPQExpBuffer(&slotdir_ent_path);
					}

					closedir(slotdir);
				}
				termPQExpBuffer(&slotdir_path);
			}
		}
	}

	if (runtime_options.dry_run == true)
	{
		log_info(_("prerequisites for executing NODE REJOIN are met"));
		exit(SUCCESS);
	}

	initPQExpBuffer(&follow_output);

	/*
	 * do_standby_follow_internal() can handle situations where the follow
	 * target is not the primary, so requires database handles to both
	 * (even if they point to the same node). For the time being,
	 * "node rejoin" will only attach a standby to the primary.
	 */
	success = do_standby_follow_internal(primary_conn,
										 primary_conn,
										 &primary_node_record,
										 &follow_output,
										 ERR_REJOIN_FAIL,
										 &follow_error_code);

	if (success == false)
	{
		log_error(_("NODE REJOIN failed"));

		if (strlen(follow_output.data))
			log_detail("%s", follow_output.data);

		create_event_notification(primary_conn,
								  &config_file_options,
								  config_file_options.node_id,
								  "node_rejoin",
								  success,
								  follow_output.data);

		PQfinish(primary_conn);

		termPQExpBuffer(&follow_output);
		exit(follow_error_code);
	}

	/*
	 * Actively check that node actually started and connected to primary,
	 * if not exit with ERR_REJOIN_FAIL.
	 *
	 * This check can be overridden with -W/--no-wait, in which case a one-time
	 * check will be carried out.
	 */
	if (runtime_options.no_wait == false)
	{
		standy_join_status join_success = check_standby_join(primary_conn,
															 &primary_node_record,
															 &local_node_record);

		create_event_notification(primary_conn,
								  &config_file_options,
								  config_file_options.node_id,
								  "node_rejoin",
								  join_success == JOIN_SUCCESS ? true : false,
								  follow_output.data);

		if (join_success != JOIN_SUCCESS)
		{
			termPQExpBuffer(&follow_output);
			log_error(_("NODE REJOIN failed"));

			if (join_success == JOIN_FAIL_NO_PING) {
				log_detail(_("local node \"%s\" did not become available start after %i seconds"),
						   config_file_options.node_name,
						   config_file_options.node_rejoin_timeout);
			}
			else {
				log_detail(_("no active record for local node \"%s\" found in node \"%s\"'s \"pg_stat_replication\" table"),
						   config_file_options.node_name,
						   primary_node_record.node_name);
			}
			log_hint(_("check the PostgreSQL log on the local node"));

			exit(ERR_REJOIN_FAIL);
		}
	}
	else
	{
		/* -W/--no-wait provided - check once */
		NodeAttached node_attached = is_downstream_node_attached(primary_conn, config_file_options.node_name, NULL);
		if (node_attached == NODE_ATTACHED)
			success = true;
	}

	/*
	 * Handle replication slots:
	 *  - if a slot for the new upstream exists, delete that
	 *  - warn about any other inactive replication slots
	 */
	if (runtime_options.force_rewind_used == false && config_file_options.use_replication_slots)
	{
		PGconn	   *local_conn = NULL;
		local_conn = establish_db_connection(config_file_options.conninfo, false);

		if (PQstatus(local_conn) != CONNECTION_OK)
		{
			log_warning(_("unable to connect to local node to check replication slot status"));
			log_hint(_("execute \"repmgr node check\" to check inactive slots and drop manually if necessary"));
		}
		else
		{
			KeyValueList inactive_replication_slots = {NULL, NULL};
			KeyValueListCell *cell = NULL;
			int inactive_count = 0;
			PQExpBufferData slotinfo;

			drop_replication_slot_if_exists(local_conn,
											config_file_options.node_id,
											primary_node_record.slot_name);

			(void) get_inactive_replication_slots(local_conn, &inactive_replication_slots);

			initPQExpBuffer(&slotinfo);
			for (cell = inactive_replication_slots.head; cell; cell = cell->next)
			{
				appendPQExpBuffer(&slotinfo,
								  "  - %s (%s)", cell->key, cell->value);
				inactive_count++;
			}

			if (inactive_count > 0)
			{
				log_warning(_("%i inactive replication slots detected"), inactive_count);
				log_detail(_("inactive replication slots:\n%s"), slotinfo.data);
				log_hint(_("these replication slots may need to be removed manually"));
			}

			termPQExpBuffer(&slotinfo);

			PQfinish(local_conn);
		}
	}

	if (success == true)
	{
		log_notice(_("NODE REJOIN successful"));
		log_detail("%s", follow_output.data);
	}
	else
	{
		/*
		 * if we reach here, no record found in upstream node's pg_stat_replication
		 */
		log_notice(_("NODE REJOIN has completed but node is not yet reattached to upstream"));
		log_hint(_("you will need to manually check the node's replication status"));
	}
	termPQExpBuffer(&follow_output);

	return;
}


/*
 * Currently for testing purposes only, not documented;
 * use at own risk!
 */

void
do_node_control(void)
{
	PGconn	   *conn = NULL;
	pid_t	    wal_receiver_pid = UNKNOWN_PID;
	conn = establish_db_connection(config_file_options.conninfo, true);

	if (runtime_options.disable_wal_receiver == true)
	{
		wal_receiver_pid = disable_wal_receiver(conn);

		PQfinish(conn);

		if (wal_receiver_pid == UNKNOWN_PID)
			exit(ERR_BAD_CONFIG);

		exit(SUCCESS);
	}

	if (runtime_options.enable_wal_receiver == true)
	{
		wal_receiver_pid = enable_wal_receiver(conn, true);

		PQfinish(conn);

		if (wal_receiver_pid == UNKNOWN_PID)
			exit(ERR_BAD_CONFIG);

		exit(SUCCESS);
	}

	log_error(_("no option provided"));

	PQfinish(conn);
}


/*
 * For "internal" use by `node rejoin` on the local node when
 * called by "standby switchover" from the remote node.
 *
 * This archives any configuration files in the data directory, which may be
 * overwritten by pg_rewind.
 *
 * Requires configuration file, optionally --config-archive-dir
 */
static void
_do_node_archive_config(void)
{
	PQExpBufferData		archive_dir;
	struct stat statbuf;
	struct dirent *arcdir_ent;
	DIR		   *arcdir;

	KeyValueList config_files = {NULL, NULL};
	KeyValueListCell *cell = NULL;
	int			copied_count = 0;

	initPQExpBuffer(&archive_dir);
	format_archive_dir(&archive_dir);

	/* sanity-check directory path */
	if (stat(archive_dir.data, &statbuf) == -1)
	{
		if (errno != ENOENT)
		{
			log_error(_("error encountered when checking archive directory \"%s\""),
					  archive_dir.data);
			log_detail("%s", strerror(errno));
			termPQExpBuffer(&archive_dir);
			exit(ERR_BAD_CONFIG);
		}

		/* attempt to create and open the directory */
		if (mkdir(archive_dir.data, S_IRWXU) != 0 && errno != EEXIST)
		{
			log_error(_("unable to create temporary archive directory \"%s\""),
					  archive_dir.data);
			log_detail("%s", strerror(errno));
			termPQExpBuffer(&archive_dir);
			exit(ERR_BAD_CONFIG);
		}

		if (runtime_options.dry_run == true)
		{
			log_verbose(LOG_INFO, "temporary archive directory \"%s\" created", archive_dir.data);
		}
	}
	else if (!S_ISDIR(statbuf.st_mode))
	{
		log_error(_("\"%s\" exists but is not a directory"),
				  archive_dir.data);
		termPQExpBuffer(&archive_dir);
		exit(ERR_BAD_CONFIG);
	}

	arcdir = opendir(archive_dir.data);

	/* always attempt to open the directory */
	if (arcdir == NULL)
	{
		log_error(_("unable to open archive directory \"%s\""),
				  archive_dir.data);
		log_detail("%s", strerror(errno));
		termPQExpBuffer(&archive_dir);
		exit(ERR_BAD_CONFIG);
	}

	if (runtime_options.dry_run == false)
	{

		/*
		 * attempt to remove any existing files in the directory
		 * TODO: collate problem files into list
		 */
		while ((arcdir_ent = readdir(arcdir)) != NULL)
		{
			PQExpBufferData arcdir_ent_path;

			initPQExpBuffer(&arcdir_ent_path);

			appendPQExpBuffer(&arcdir_ent_path,
							  "%s/%s",
							  archive_dir.data,
							  arcdir_ent->d_name);

			if (stat(arcdir_ent_path.data, &statbuf) == 0 && !S_ISREG(statbuf.st_mode))
			{
				termPQExpBuffer(&arcdir_ent_path);
				continue;
			}

			if (unlink(arcdir_ent_path.data) == -1)
			{
				log_error(_("unable to delete file in temporary archive directory"));
				log_detail(_("file is:  \"%s\""), arcdir_ent_path.data);
				log_detail("%s", strerror(errno));
				closedir(arcdir);
				termPQExpBuffer(&arcdir_ent_path);
				exit(ERR_BAD_CONFIG);
			}

			termPQExpBuffer(&arcdir_ent_path);
		}
	}

	closedir(arcdir);


	/*
	 * extract list of config files from --config-files
	 */
	{
		int			i = 0;
		int			j = 0;
		int			config_file_len = strlen(runtime_options.config_files);

		char		filenamebuf[MAXPGPATH] = "";
		PQExpBufferData		pathbuf;

		for (j = 0; j < config_file_len; j++)
		{
			if (runtime_options.config_files[j] == ',')
			{
				int			filename_len = j - i;

				if (filename_len >= MAXPGPATH)
					filename_len = MAXPGPATH - 1;

				strncpy(filenamebuf, runtime_options.config_files + i, filename_len);

				filenamebuf[filename_len] = '\0';

				initPQExpBuffer(&pathbuf);

				appendPQExpBuffer(&pathbuf,
								  "%s/%s",
								  config_file_options.data_directory,
								  filenamebuf);

				key_value_list_set(&config_files,
								   filenamebuf,
								   pathbuf.data);
				termPQExpBuffer(&pathbuf);
				i = j + 1;
			}
		}

		if (i < config_file_len)
		{
			int			filename_len = config_file_len - i;

			strncpy(filenamebuf, runtime_options.config_files + i, filename_len);

			filenamebuf[filename_len] = '\0';

			initPQExpBuffer(&pathbuf);
			appendPQExpBuffer(&pathbuf,
							  "%s/%s",
							  config_file_options.data_directory,
							  filenamebuf);

			key_value_list_set(&config_files,
							   filenamebuf,
							   pathbuf.data);
			termPQExpBuffer(&pathbuf);
		}
	}


	for (cell = config_files.head; cell; cell = cell->next)
	{
		PQExpBufferData dest_file;

		initPQExpBuffer(&dest_file);

		appendPQExpBuffer(&dest_file,
						  "%s/%s",
						  archive_dir.data,
						  cell->key);

		if (stat(cell->value, &statbuf) == -1)
		{
			log_warning(_("specified file \"%s\" not found, skipping"),
						cell->value);
		}
		else
		{
			if (runtime_options.dry_run == true)
			{
				log_info("file \"%s\" would be copied to \"%s\"",
						 cell->key, dest_file.data);
				copied_count++;
			}
			else
			{
				log_verbose(LOG_DEBUG, "copying \"%s\" to \"%s\"",
							cell->key, dest_file.data);
				copy_file(cell->value, dest_file.data);
				copied_count++;
			}
		}

		termPQExpBuffer(&dest_file);
	}

	if (runtime_options.dry_run == true)
	{
		log_verbose(LOG_INFO, _("%i files would have been copied to \"%s\""),
					copied_count, archive_dir.data);
	}
	else
	{
		log_verbose(LOG_INFO, _("%i files copied to \"%s\""),
					copied_count, archive_dir.data);
	}

	if (runtime_options.dry_run == true)
	{
		/*
		 * Delete directory in --dry-run mode  - it should be empty unless it's been
		 * interfered with for some reason, in which case manual intervention is
		 * required
		 */
		if (rmdir(archive_dir.data) != 0 && errno != EEXIST)
		{
			log_warning(_("unable to delete directory \"%s\""), archive_dir.data);
			log_detail("%s", strerror(errno));
			log_hint(_("directory may need to be manually removed"));
		}
		else
		{
			log_verbose(LOG_INFO, "temporary archive directory \"%s\" deleted", archive_dir.data);
		}
	}

	termPQExpBuffer(&archive_dir);
}


/*
 * Intended mainly for "internal" use by `standby switchover`, which
 * calls this on the target server to restore any configuration files
 * to the data directory, which may have been overwritten by an operation
 * like pg_rewind
 *
 * Not designed to be called if the instance is running, but does
 * not currently check.
 *
 * Requires -D/--pgdata, optionally --config-archive-dir
 *
 * Removes --config-archive-dir after successful copy
 */

static void
_do_node_restore_config(void)
{
	PQExpBufferData		archive_dir;

	DIR		   *arcdir;
	struct dirent *arcdir_ent;
	int			copied_count = 0;
	bool		copy_ok = true;

	initPQExpBuffer(&archive_dir);

	format_archive_dir(&archive_dir);

	arcdir = opendir(archive_dir.data);

	if (arcdir == NULL)
	{
		log_error(_("unable to open archive directory \"%s\""),
				  archive_dir.data);
		log_detail("%s", strerror(errno));
		termPQExpBuffer(&archive_dir);
		exit(ERR_BAD_CONFIG);
	}

	while ((arcdir_ent = readdir(arcdir)) != NULL)
	{
		struct stat statbuf;
		PQExpBufferData		src_file_path;
		PQExpBufferData		dest_file_path;

		initPQExpBuffer(&src_file_path);

		appendPQExpBuffer(&src_file_path,
						  "%s/%s",
						  archive_dir.data,
						  arcdir_ent->d_name);

		/* skip non-files */
		if (stat(src_file_path.data, &statbuf) == 0 && !S_ISREG(statbuf.st_mode))
		{
			termPQExpBuffer(&src_file_path);
			continue;
		}

		initPQExpBuffer(&dest_file_path);

		appendPQExpBuffer(&dest_file_path,
						  "%s/%s",
						  config_file_options.data_directory,
						  arcdir_ent->d_name);

		log_verbose(LOG_DEBUG, "copying \"%s\" to \"%s\"",
					src_file_path.data, dest_file_path.data);

		if (copy_file(src_file_path.data, dest_file_path.data) == false)
		{
			copy_ok = false;
			log_warning(_("unable to copy \"%s\" to \"%s\""),
						arcdir_ent->d_name, runtime_options.data_dir);
		}
		else
		{
			unlink(src_file_path.data);
			copied_count++;
		}

		termPQExpBuffer(&dest_file_path);
		termPQExpBuffer(&src_file_path);
	}

	closedir(arcdir);

	log_notice(_("%i files copied to %s"),
			   copied_count,
			   config_file_options.data_directory);

	if (copy_ok == false)
	{
		log_warning(_("unable to copy all files from \"%s\""), archive_dir.data);
	}
	else
	{
		/*
		 * Finally, delete directory - it should be empty unless it's been
		 * interfered with for some reason, in which case manual intervention is
		 * required
		 */
		if (rmdir(archive_dir.data) != 0 && errno != EEXIST)
		{
			log_warning(_("unable to delete directory \"%s\""), archive_dir.data);
			log_detail("%s", strerror(errno));
			log_hint(_("directory may need to be manually removed"));
		}
		else
		{
			log_verbose(LOG_INFO, "directory \"%s\" deleted", archive_dir.data);
		}
	}

	termPQExpBuffer(&archive_dir);

	return;
}


static void
format_archive_dir(PQExpBufferData *archive_dir)
{
	appendPQExpBuffer(archive_dir,
					  "%s/repmgr-config-archive-%s",
					  runtime_options.config_archive_dir,
					  config_file_options.node_name);

	log_verbose(LOG_DEBUG, "using archive directory \"%s\"", archive_dir->data);
}


static bool
copy_file(const char *src_file, const char *dest_file)
{
	FILE	   *ptr_old,
			   *ptr_new;
	int			a = 0;

	ptr_old = fopen(src_file, "r");

	if (ptr_old == NULL)
		return false;

	ptr_new = fopen(dest_file, "w");

	if (ptr_new == NULL)
	{
		fclose(ptr_old);
		return false;
	}

	chmod(dest_file, S_IRUSR | S_IWUSR);

	while (1)
	{
		a = fgetc(ptr_old);

		if (!feof(ptr_old))
		{
			fputc(a, ptr_new);
		}
		else
		{
			break;
		}
	}

	fclose(ptr_new);
	fclose(ptr_old);

	return true;
}


void
do_node_help(void)
{
	print_help_header();

	printf(_("Usage:\n"));
	printf(_("    %s [OPTIONS] node status\n"), progname());
	printf(_("    %s [OPTIONS] node check\n"), progname());
	printf(_("    %s [OPTIONS] node rejoin\n"), progname());
	printf(_("    %s [OPTIONS] node service\n"), progname());
	puts("");

	printf(_("NODE STATUS\n"));
	puts("");
	printf(_("  \"node status\" displays an overview of a node's basic information and replication status.\n"));
	puts("");
	printf(_("  Configuration file required, runs on local node only.\n"));
	puts("");
	printf(_("    --csv                 emit output as CSV\n"));
	puts("");

	printf(_("NODE CHECK\n"));
	puts("");
	printf(_("  \"node check\" performs some health checks on a node from a replication perspective.\n"));
	puts("");
	printf(_("  Configuration file required, runs on local node only.\n"));
	puts("");
	printf(_("  Connection options:\n"));
	printf(_("    -S, --superuser=USERNAME  superuser to use, if repmgr user is not superuser\n"));
	puts("");
	printf(_("  Output options:\n"));
	printf(_("    --csv                     emit output as CSV (not available for individual check output)\n"));
	printf(_("    --nagios                  emit output in Nagios format (individual check output only)\n"));
	puts("");
	printf(_("  Following options check an individual status:\n"));
	printf(_("    --archive-ready           number of WAL files ready for archiving\n"));
	printf(_("    --downstream              whether all downstream nodes are connected\n"));
	printf(_("    --uptream                 whether the node is connected to its upstream\n"));
	printf(_("    --replication-lag         replication lag in seconds (standbys only)\n"));
	printf(_("    --role                    check node has expected role\n"));
	printf(_("    --slots                   check for inactive replication slots\n"));
	printf(_("    --missing-slots           check for missing replication slots\n"));
	printf(_("    --data-directory-config   check repmgr's data directory configuration\n"));

	puts("");

	printf(_("NODE REJOIN\n"));
	puts("");
	printf(_("  \"node rejoin\" enables a dormant (stopped) node to be rejoined to the replication cluster.\n"));
	puts("");
	printf(_("  Configuration file required, runs on local node only.\n"));
	puts("");
	printf(_("    --dry-run               check that the prerequisites are met for rejoining the node\n" \
			 "                              (including usability of \"pg_rewind\" if requested)\n"));
	printf(_("    --force-rewind[=VALUE]  execute \"pg_rewind\" if necessary\n"));
	printf(_("                              (9.3 and 9.4 - provide full \"pg_rewind\" path)\n"));

	printf(_("    --config-files          comma-separated list of configuration files to retain\n" \
			 "                            after executing \"pg_rewind\"\n"));
	printf(_("    --config-archive-dir    directory to temporarily store retained configuration files\n" \
			 "                              (default: /tmp)\n"));
	printf(_("    -W, --no-wait           don't wait for the node to rejoin cluster\n"));
	puts("");

	printf(_("NODE SERVICE\n"));
	puts("");
	printf(_("  \"node service\" executes a system service command to stop/start/restart/reload a node\n" \
			 "                   or optionally display which command would be executed\n"));
	puts("");
	printf(_("  Configuration file required, runs on local node only.\n"));
	puts("");
	printf(_("    --dry-run                 show what action would be performed, but don't execute it\n"));
	printf(_("    --action                  action to perform (one of \"start\", \"stop\", \"restart\" or \"reload\")\n"));
	printf(_("    --list-actions            show what command would be performed for each action\n"));
	printf(_("    --checkpoint              issue a CHECKPOINT before stopping or restarting the node\n"));
	printf(_("    -S, --superuser=USERNAME  superuser to use, if repmgr user is not superuser\n"));

	puts("");

	printf(_("%s home page: <%s>\n"), "repmgr", REPMGR_URL);
}
