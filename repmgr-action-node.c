/*
 * repmgr-action-node.c
 *
 * Implements actions available for any kind of node
 *
 * Copyright (c) 2ndQuadrant, 2010-2018
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

static void _do_node_service_list_actions(t_server_action action);
static void _do_node_status_is_shutdown_cleanly(void);
static void _do_node_archive_config(void);
static void _do_node_restore_config(void);

static CheckStatus do_node_check_archive_ready(PGconn *conn, OutputMode mode, CheckStatusList *list_output);
static CheckStatus do_node_check_downstream(PGconn *conn, OutputMode mode, CheckStatusList *list_output);
static CheckStatus do_node_check_replication_lag(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output);
static CheckStatus do_node_check_role(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output);
static CheckStatus do_node_check_slots(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output);

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
	char		server_version[MAXLEN];
	char		cluster_size[MAXLEN];
	PQExpBufferData output;

	KeyValueList node_status = {NULL, NULL};
	KeyValueListCell *cell = NULL;

	ItemList	warnings = {NULL, NULL};
	RecoveryType recovery_type = RECTYPE_UNKNOWN;
	ReplInfo	replication_info = T_REPLINFO_INTIALIZER;
	t_recovery_conf recovery_conf = T_RECOVERY_CONF_INITIALIZER;

	char		data_dir[MAXPGPATH] = "";

	if (runtime_options.is_shutdown_cleanly == true)
	{
		return _do_node_status_is_shutdown_cleanly();
	}

	/* config file required, so we should have "conninfo" and "data_directory" */
	conn = establish_db_connection(config_file_options.conninfo, true);
	strncpy(data_dir, config_file_options.data_directory, MAXPGPATH);

	server_version_num = get_server_version(conn, NULL);

	/* Check node exists and is really a standby */

	if (get_node_record(conn, config_file_options.node_id, &node_info) != RECORD_FOUND)
	{
		log_error(_("no record found for node %i"), config_file_options.node_id);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	(void) get_server_version(conn, server_version);

	if (get_cluster_size(conn, cluster_size) == false)
		strncpy(cluster_size, _("unknown"), MAXLEN);

	recovery_type = get_recovery_type(conn);

	get_node_replication_stats(conn, server_version_num, &node_info);

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

	if (runtime_options.verbose == true)
	{
		uint64		local_system_identifier = UNKNOWN_SYSTEM_IDENTIFIER;

		local_system_identifier = get_system_identifier(config_file_options.data_directory);

		key_value_list_set_format(
								  &node_status,
								  "System identifier",
								  "%lu", local_system_identifier);
	}

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

	if (guc_set(conn, "archive_mode", "=", "off"))
	{
		key_value_list_set(
						   &node_status,
						   "WAL archiving",
						   "off");

		key_value_list_set(
						   &node_status,
						   "Archive command",
						   "(none)");
	}
	else
	{
		bool		enabled = true;
		PQExpBufferData archiving_status;
		char		archive_command[MAXLEN] = "";

		initPQExpBuffer(&archiving_status);
		if (recovery_type == RECTYPE_STANDBY)
		{
			if (guc_set(conn, "archive_mode", "=", "on"))
				enabled = false;
		}

		if (enabled == true)
		{
			appendPQExpBuffer(&archiving_status, "enabled");
		}
		else
		{
			appendPQExpBuffer(&archiving_status, "disabled");
		}

		if (enabled == false && recovery_type == RECTYPE_STANDBY)
		{
			appendPQExpBuffer(&archiving_status, " (on standbys \"archive_mode\" must be set to \"always\" to be effective)");
		}

		key_value_list_set(
						   &node_status,
						   "WAL archiving",
						   archiving_status.data);

		termPQExpBuffer(&archiving_status);

		get_pg_setting(conn, "archive_command", archive_command);

		key_value_list_set(
						   &node_status,
						   "Archive command",
						   archive_command);
	}

	{
		int			ready_files;

		ready_files = get_ready_archive_files(conn, data_dir);

		if (runtime_options.output_mode == OM_CSV)
		{
			key_value_list_set_format(
									  &node_status,
									  "WALs pending archiving",
									  "%i",
									  ready_files);
		}
		else
		{
			key_value_list_set_format(
									  &node_status,
									  "WALs pending archiving",
									  "%i pending files",
									  ready_files);
		}

		if (guc_set(conn, "archive_mode", "=", "off"))
		{
			key_value_list_set_output_mode(&node_status, "WALs pending archiving", OM_CSV);
		}

	}


	if (node_info.max_wal_senders >= 0)
	{
		/* In CSV mode, raw values supplied as well */
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

	if (server_version_num < 90400)
	{
		key_value_list_set(&node_status,
						   "Replication slots",
						   "not available");
	}
	else if (node_info.max_replication_slots > 0)
	{
		PQExpBufferData slotinfo;

		initPQExpBuffer(&slotinfo);

		appendPQExpBuffer(
						  &slotinfo,
						  "%i (of maximal %i)",
						  node_info.active_replication_slots + node_info.inactive_replication_slots,
						  node_info.max_replication_slots);


		if (node_info.inactive_replication_slots > 0)
		{
			appendPQExpBuffer(&slotinfo,
							  "; %i inactive",
							  node_info.inactive_replication_slots);

			item_list_append_format(
									&warnings,
									_("- node has %i inactive replication slots"),
									node_info.inactive_replication_slots);
		}

		key_value_list_set(&node_status,
						   "Replication slots",
						   slotinfo.data);

		termPQExpBuffer(&slotinfo);
	}
	else if (node_info.max_replication_slots == 0)
	{
		key_value_list_set(&node_status,
						   "Replication slots",
						   "disabled");
	}


	if (node_info.type == STANDBY)
	{
		key_value_list_set_format(&node_status,
								  "Upstream node",
								  "%s (ID: %i)",
								  node_info.upstream_node_name,
								  node_info.upstream_node_id);

		get_replication_info(conn, &replication_info);

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

		appendPQExpBuffer(&output,
						  "\"inactive_replaction_slots\",%i\n",
						  node_info.inactive_replication_slots);

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

	if (warnings.head != NULL && runtime_options.terse == false)
	{
		log_warning(_("following issue(s) were detected:"));
		print_item_list(&warnings);
		/* add this when functionality implemented */
		/* log_hint(_("execute \"repmgr node check\" for more details")); */
	}

	key_value_list_free(&node_status);
	item_list_free(&warnings);
	PQfinish(conn);
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

static
void
_do_node_status_is_shutdown_cleanly(void)
{
	PGPing		ping_status;
	PQExpBufferData output;

	DBState		db_state;
	XLogRecPtr	checkPoint = InvalidXLogRecPtr;

	NodeStatus	node_status = NODE_STATUS_UNKNOWN;

	initPQExpBuffer(&output);

	appendPQExpBuffer(
					  &output,
					  "--state=");

	/* sanity-check we're dealing with a PostgreSQL directory */
	if (is_pg_dir(config_file_options.data_directory) == false)
	{
		appendPQExpBuffer(&output, "UNKNOWN");
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

	/* check what pg_controldata says */

	db_state = get_db_state(config_file_options.data_directory);

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

	/* unable to read pg_control, don't know what's happening */
	if (checkPoint == InvalidXLogRecPtr)
	{
		node_status = NODE_STATUS_UNKNOWN;
	}

	/*
	 * if still "UNKNOWN" at this point, then the node must be cleanly shut
	 * down
	 */
	else if (node_status == NODE_STATUS_UNKNOWN)
	{
		node_status = NODE_STATUS_DOWN;
	}

	log_verbose(LOG_DEBUG, "node status determined as: %s", print_node_status(node_status));

	switch (node_status)
	{
		case NODE_STATUS_UP:
			appendPQExpBuffer(&output, "RUNNING");
			break;
		case NODE_STATUS_SHUTTING_DOWN:
			appendPQExpBuffer(&output, "SHUTTING_DOWN");
			break;
		case NODE_STATUS_DOWN:
			appendPQExpBuffer(&output,
							  "SHUTDOWN --last-checkpoint-lsn=%X/%X",
							  format_lsn(checkPoint));
			break;
		case NODE_STATUS_UNCLEAN_SHUTDOWN:
			appendPQExpBuffer(&output, "UNCLEAN_SHUTDOWN");
			break;
		case NODE_STATUS_UNKNOWN:
			appendPQExpBuffer(&output, "UNKNOWN");
			break;
	}

	printf("%s\n", output.data);
	termPQExpBuffer(&output);
	return;
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


	/* internal */
	if (runtime_options.has_passfile == true)
	{
		return_code = has_passfile() ? 0 : 1;

		exit(return_code);
	}


	if (strlen(config_file_options.conninfo))
		conn = establish_db_connection(config_file_options.conninfo, true);
	else
		conn = establish_db_connection_by_params(&source_conninfo, true);

	if (get_node_record(conn, config_file_options.node_id, &node_info) != RECORD_FOUND)
	{
		log_error(_("no record found for node %i"), config_file_options.node_id);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	server_version_num = get_server_version(conn, NULL);

	/* add replication statistics to node record */
	get_node_replication_stats(conn, server_version_num, &node_info);

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

	if (runtime_options.downstream == true)
	{
		return_code = do_node_check_downstream(conn,
											   runtime_options.output_mode,
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

	if (runtime_options.output_mode == OM_NAGIOS)
	{
		log_error(_("--nagios can only be used with a specific check"));
		log_hint(_("execute \"repmgr node --help\" for details"));
		PQfinish(conn);
		return;
	}

	/* output general overview */

	initPQExpBuffer(&output);

	/* order functions are called is also output order */
	(void) do_node_check_role(conn, runtime_options.output_mode, &node_info, &status_list);
	(void) do_node_check_replication_lag(conn, runtime_options.output_mode, &node_info, &status_list);
	(void) do_node_check_archive_ready(conn, runtime_options.output_mode, &status_list);
	(void) do_node_check_downstream(conn, runtime_options.output_mode, &status_list);
	(void) do_node_check_slots(conn, runtime_options.output_mode, &node_info, &status_list);

	if (runtime_options.output_mode == OM_CSV)
	{
		/* TODO */
	}
	else
	{
		appendPQExpBuffer(
						  &output,
						  "Node \"%s\":\n",
						  node_info.node_name);

		for (cell = status_list.head; cell; cell = cell->next)
		{
			appendPQExpBuffer(
							  &output,
							  "\t%s: %s",
							  cell->item,
							  output_check_status(cell->status));

			if (strlen(cell->details))
			{
				appendPQExpBuffer(
								  &output,
								  " (%s)",
								  cell->details);
			}
			appendPQExpBuffer(&output, "\n");
		}
	}


	printf("%s", output.data);
	termPQExpBuffer(&output);
	check_status_list_free(&status_list);

	PQfinish(conn);
}


static CheckStatus
do_node_check_role(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output)
{

	CheckStatus status = CHECK_STATUS_OK;
	PQExpBufferData details;
	RecoveryType recovery_type = get_recovery_type(conn);

	if (mode == OM_CSV)
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
				appendPQExpBuffer(&details,
								  _("node is registered as primary but running as standby"));
			}
			else
			{
				appendPQExpBuffer(&details,
								  _("node is primary"));
			}
			break;
		case STANDBY:
			if (recovery_type == RECTYPE_PRIMARY)
			{
				status = CHECK_STATUS_CRITICAL;
				appendPQExpBuffer(&details,
								  _("node is registered as standby but running as primary"));
			}
			else
			{
				appendPQExpBuffer(&details,
								  _("node is standby"));
			}
			break;
		case BDR:
			{
				PQExpBufferData output;

				initPQExpBuffer(&output);
				if (is_bdr_db(conn, &output) == false)
				{
					status = CHECK_STATUS_CRITICAL;
					appendPQExpBuffer(&details,
									  "%s", output.data);
				}
				termPQExpBuffer(&output);

				if (status == CHECK_STATUS_OK)
				{
					if (is_active_bdr_node(conn, node_info->node_name) == false)
					{
						status = CHECK_STATUS_CRITICAL;
						appendPQExpBuffer(&details,
										  _("node is not an active BDR node"));
					}
					else
					{
						appendPQExpBuffer(&details,
										  _("node is an active BDR node"));
					}
				}
			}
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

	initPQExpBuffer(&details);

	if (server_version_num < 90400)
	{
		appendPQExpBuffer(&details,
						  _("replication slots not available for this PostgreSQL version"));
	}
	else if (node_info->total_replication_slots == 0)
	{
		appendPQExpBuffer(&details,
						  _("node has no replication slots"));
	}
	else if (node_info->inactive_replication_slots == 0)
	{
		appendPQExpBuffer(&details,
						  _("%i of %i replication slots are active"),
						  node_info->total_replication_slots,
						  node_info->total_replication_slots);
	}
	else if (node_info->inactive_replication_slots > 0)
	{
		status = CHECK_STATUS_CRITICAL;

		appendPQExpBuffer(&details,
						  _("%i of %i replication slots are inactive"),
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
do_node_check_archive_ready(PGconn *conn, OutputMode mode, CheckStatusList *list_output)
{
	int			ready_archive_files = 0;
	CheckStatus status = CHECK_STATUS_UNKNOWN;
	PQExpBufferData details;

	if (mode == OM_CSV)
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
				appendPQExpBuffer(
								  &details,
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
do_node_check_replication_lag(PGconn *conn, OutputMode mode, t_node_info *node_info, CheckStatusList *list_output)
{
	CheckStatus status = CHECK_STATUS_OK;
	int			lag_seconds = 0;
	PQExpBufferData details;

	if (mode == OM_CSV)
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
				appendPQExpBuffer(
								  &details,
								  "--lag=0");
				break;
			case OM_NAGIOS:
				appendPQExpBuffer(
								  &details,
								  "0 seconds | lag=0;%i;%i",
								  config_file_options.replication_lag_warning,
								  config_file_options.replication_lag_critical);
				break;
			case OM_TEXT:
				appendPQExpBuffer(
								  &details,
								  "N/A - node is primary");
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
		else if (lag_seconds < 0)
		{
			status = CHECK_STATUS_UNKNOWN;

			switch (mode)
			{
				case OM_OPTFORMAT:
					break;
				case OM_NAGIOS:
				case OM_TEXT:
					appendPQExpBuffer(
									  &details,
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
			{
				printf("--status=%s %s\n",
					   output_check_status(status),
					   details.data);
			}
			break;
		case OM_NAGIOS:
			printf("REPMGR_REPLICATION_LAG %s: %s\n",
				   output_check_status(status),
				   details.data);
			break;
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

/* TODO: ensure only runs on streaming replication nodes */
static CheckStatus
do_node_check_downstream(PGconn *conn, OutputMode mode, CheckStatusList *list_output)
{
	NodeInfoList downstream_nodes = T_NODE_INFO_LIST_INITIALIZER;
	NodeInfoListCell *cell = NULL;
	int			missing_nodes_count = 0;
	CheckStatus status = CHECK_STATUS_OK;
	ItemList	missing_nodes = {NULL, NULL};
	ItemList	attached_nodes = {NULL, NULL};
	PQExpBufferData details;

	initPQExpBuffer(&details);

	get_downstream_node_records(conn, config_file_options.node_id, &downstream_nodes);

	for (cell = downstream_nodes.head; cell; cell = cell->next)
	{
		if (is_downstream_node_attached(conn, cell->node_info->node_name) == false)
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

	if (missing_nodes_count == 0)
	{
		if (downstream_nodes.node_count == 0)
			appendPQExpBuffer(
							  &details,
							  "this node has no downstream nodes");
		else
			appendPQExpBuffer(
							  &details,
							  "%i of %i downstream nodes attached",
							  downstream_nodes.node_count,
							  downstream_nodes.node_count);
	}
	else
	{
		ItemListCell *missing_cell = NULL;
		bool		first = true;

		status = CHECK_STATUS_CRITICAL;

		appendPQExpBuffer(
						  &details,
						  "%i of %i downstream nodes not attached",
						  missing_nodes_count,
						  downstream_nodes.node_count);

		if (mode != OM_NAGIOS)
		{
			appendPQExpBuffer(
							  &details, "; missing: ");

			for (missing_cell = missing_nodes.head; missing_cell; missing_cell = missing_cell->next)
			{
				if (first == false)
					appendPQExpBuffer(
									  &details,
									  ", ");
				else
					first = false;

				if (first == false)
					appendPQExpBuffer(
									  &details,
									  "%s", missing_cell->string);
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

				if (downstream_nodes.node_count - missing_nodes_count)
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
		get_node_data_directory(data_dir);

		if (data_dir[0] == '\0')
		{
			log_error(_("unable to determine data directory for action"));
			exit(ERR_BAD_CONFIG);
		}
	}


	if ((action == ACTION_STOP || action == ACTION_RESTART) && runtime_options.checkpoint == true)
	{
		if (runtime_options.dry_run == true)
		{
			log_info(_("a CHECKPOINT would be issued here"));
		}
		else
		{
			PGconn	   *conn = NULL;

			if (strlen(config_file_options.conninfo))
				conn = establish_db_connection(config_file_options.conninfo, true);
			else
				conn = establish_db_connection_by_params(&source_conninfo, true);

			log_notice(_("issuing CHECKPOINT"));

			/* check superuser conn! */
			checkpoint(conn);

			PQfinish(conn);
		}
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
		get_node_data_directory(data_dir);
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
 */
void
do_node_rejoin(void)
{
	PGconn	   *upstream_conn = NULL;
	RecoveryType upstream_recovery_type = RECTYPE_UNKNOWN;
	DBState		db_state;
	PGPing		status;
	bool		is_shutdown = true;

	PQExpBufferData command;
	PQExpBufferData command_output;
	PQExpBufferData follow_output;
	struct stat statbuf;
	t_node_info primary_node_record = T_NODE_INFO_INITIALIZER;

	bool		success = true;
	int			server_version_num = UNKNOWN_SERVER_VERSION_NUM;

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

	db_state = get_db_state(config_file_options.data_directory);

	if (is_shutdown == false)
	{
		log_error(_("database is still running in state \"%s\""),
				  describe_db_state(db_state));
		log_hint(_("\"repmgr node rejoin\" cannot be executed on a running node"));
		exit(ERR_BAD_CONFIG);
	}

	/* check if cleanly shut down */
	if (db_state != DB_SHUTDOWNED && db_state != DB_SHUTDOWNED_IN_RECOVERY)
	{
		if (db_state == DB_SHUTDOWNING)
		{
			log_error(_("database is still shutting down"));
		}
		else
		{
			log_error(_("database is not shut down cleanly"));

			if (runtime_options.force_rewind == true)
			{
				log_detail(_("pg_rewind will not be able to run"));
			}
			log_hint(_("database should be restarted then shut down cleanly after crash recovery completes"));
			exit(ERR_BAD_CONFIG);
		}
	}


	/* check provided upstream connection */
	upstream_conn = establish_db_connection_by_params(&source_conninfo, true);

	/* sanity checks for 9.3 */
	server_version_num = get_server_version(upstream_conn, NULL);

	if (server_version_num < 90400)
		check_93_config();

	if (get_primary_node_record(upstream_conn, &primary_node_record) == false)
	{
		log_error(_("unable to retrieve primary node record"));
		log_hint(_("check the provided database connection string is for a \"repmgr\" database"));
		PQfinish(upstream_conn);
		exit(ERR_BAD_CONFIG);
	}

	PQfinish(upstream_conn);

	/* connect to registered primary and check it's not in recovery */
	upstream_conn = establish_db_connection(primary_node_record.conninfo, true);

	upstream_recovery_type = get_recovery_type(upstream_conn);

	if (upstream_recovery_type != RECTYPE_PRIMARY)
	{
		log_error(_("primary server is registered node \"%s\" (ID: %i), but server is not a primary"),
				  primary_node_record.node_name,
				  primary_node_record.node_id);
		/* TODO: hint about checking cluster */
		PQfinish(upstream_conn);

		exit(ERR_BAD_CONFIG);
	}

	/*
	 * If --force-rewind specified, check pg_rewind can be used, and
	 * pre-emptively fetch the list of configuration files which should be
	 * archived
	 */

	if (runtime_options.force_rewind == true)
	{
		PQExpBufferData reason;
		PQExpBufferData msg;

		initPQExpBuffer(&reason);

		if (can_use_pg_rewind(upstream_conn, config_file_options.data_directory, &reason) == false)
		{
			log_error(_("--force-rewind specified but pg_rewind cannot be used"));
			log_detail("%s", reason.data);
			termPQExpBuffer(&reason);
			PQfinish(upstream_conn);

			exit(ERR_BAD_CONFIG);
		}
		termPQExpBuffer(&reason);

		initPQExpBuffer(&msg);
		appendPQExpBuffer(&msg,
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
	}

	/*
	 * Forcibly rewind node if requested (this is mainly for use when this
	 * action is being executed by "repmgr standby switchover")
	 */
	if (runtime_options.force_rewind == true)
	{
		int			ret;
		PQExpBufferData		filebuf;

		_do_node_archive_config();

		/* execute pg_rewind */
		initPQExpBuffer(&command);

		appendPQExpBuffer(&command,
						  "%s -D ",
						  make_pg_path("pg_rewind"));

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

			PQfinish(upstream_conn);
			exit(SUCCESS);
		}

		log_notice(_("executing pg_rewind"));
		log_debug("pg_rewind command is:\n  %s",
				  command.data);

		initPQExpBuffer(&command_output);

		ret = local_command(
							command.data,
							&command_output);

		termPQExpBuffer(&command);

		if (ret == false)
		{
			log_error(_("unable to execute pg_rewind"));
			log_detail("%s", command_output.data);

			termPQExpBuffer(&command_output);

			exit(ERR_BAD_CONFIG);
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

		/* delete any replication slots copied in by pg_rewind */
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

					if(strcmp(slotdir_ent->d_name, ".") == 0 || strcmp(slotdir_ent->d_name, "..") == 0)
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
			}
			termPQExpBuffer(&slotdir_path);
		}
	}

	initPQExpBuffer(&follow_output);

	success = do_standby_follow_internal(upstream_conn,
										 &primary_node_record,
										 &follow_output);

	create_event_notification(upstream_conn,
							  &config_file_options,
							  config_file_options.node_id,
							  "node_rejoin",
							  success,
							  follow_output.data);

	PQfinish(upstream_conn);

	if (success == false)
	{
		log_notice(_("NODE REJOIN failed"));
		log_detail("%s", follow_output.data);

		termPQExpBuffer(&follow_output);
		exit(ERR_DB_QUERY);
	}

	log_notice(_("NODE REJOIN successful"));
	log_detail("%s", follow_output.data);

	termPQExpBuffer(&follow_output);
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
	}
	else if (!S_ISDIR(statbuf.st_mode))
	{
		log_error(_("\"%s\" exists but is not a directory"),
				  archive_dir.data);
		termPQExpBuffer(&archive_dir);
		exit(ERR_BAD_CONFIG);
	}

	arcdir = opendir(archive_dir.data);

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
		 * attempt to remove any existing files in the directory TODO: collate
		 * problem files into list
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

		closedir(arcdir);
	}

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

				if (filename_len > MAXPGPATH)
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
			strncpy(filenamebuf, runtime_options.config_files + i, config_file_len - i);

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
			log_verbose(LOG_INFO, "directory \"%s\" deleted", archive_dir.data);
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
	ptr_new = fopen(dest_file, "w");

	if (ptr_old == NULL)
		return false;

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
	printf(_("    --csv                 emit output as CSV\n"));
	printf(_("    --nagios              emit output in Nagios format (individual status output only)\n"));
	puts("");
	printf(_("  Following options check an individual status:\n"));
	printf(_("    --archive-ready       number of WAL files ready for archiving\n"));
	printf(_("    --downstream          whether all downstream nodes are connected\n"));
	printf(_("    --replication-lag     replication lag in seconds (standbys only)\n"));
	printf(_("    --role                check node has expected role\n"));
	printf(_("    --slots               check for inactive replication slots\n"));

	puts("");

	printf(_("NODE REJOIN\n"));
	puts("");
	printf(_("  \"node rejoin\" enables a dormant (stopped) node to be rejoined to the replication cluster.\n"));
	puts("");
	printf(_("  Configuration file required, runs on local node only.\n"));
	puts("");
	printf(_("    --dry-run             check that the prerequisites are met for rejoining the node\n" \
			 "                          (including usability of \"pg_rewind\" if requested)\n"));
	printf(_("    --force-rewind        execute \"pg_rewind\" if necessary\n"));
	printf(_("    --config-files        comma-separated list of configuration files to retain\n" \
			 "                          after executing \"pg_rewind\"\n"));
	printf(_("    --config-archive-dir  directory to temporarily store retained configuration files\n" \
			 "                          (default: /tmp)\n"));
	puts("");

	printf(_("NODE SERVICE\n"));
	puts("");
	printf(_("  \"node service\" executes a system service command to stop/start/restart/reload a node\n" \
			 "                   or optionally display which command would be executed\n"));
	puts("");
	printf(_("  Configuration file required, runs on local node only.\n"));
	puts("");
	printf(_("    --dry-run             show what action would be performed, but don't execute it\n"));
	printf(_("    --action              action to perform (one of \"start\", \"stop\", \"restart\" or \"reload\")\n"));
	printf(_("    --list-actions        show what command would be performed for each action\n"));
	puts("");



}
