/*
 * repmgr-action-node.c
 *
 * Implements actions available for any kind of node
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
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
static void format_archive_dir(char *archive_dir);
static t_server_action parse_server_action(const char *action);

static void _do_node_service_check(void);
static void _do_node_service_list_actions(t_server_action action);
static void _do_node_status_is_shutdown(void);
static void _do_node_archive_config(void);
static void _do_node_restore_config(void);


void
do_node_status(void)
{
	PGconn	   	   *conn = NULL;

	int 			target_node_id = UNKNOWN_NODE_ID;
	t_node_info 	node_info = T_NODE_INFO_INITIALIZER;
	char			server_version[MAXLEN];
	char			cluster_size[MAXLEN];
	PQExpBufferData	output;

	KeyValueList	node_status = { NULL, NULL };
	KeyValueListCell *cell = NULL;

	ItemList 		warnings = { NULL, NULL };
	RecoveryType	recovery_type = RECTYPE_UNKNOWN;
	ReplInfo 		replication_info = T_REPLINFO_INTIALIZER;
	t_recovery_conf recovery_conf = T_RECOVERY_CONF_INITIALIZER;

	char 	 	 	data_dir[MAXPGPATH] = "";



	if (runtime_options.is_shutdown == true)
	{
		return _do_node_status_is_shutdown();
	}

	if (strlen(config_file_options.conninfo))
		conn = establish_db_connection(config_file_options.conninfo, true);
	else
		conn = establish_db_connection_by_params(&source_conninfo, true);

	if (config_file_options.data_directory[0] != '\0')
	{
		strncpy(data_dir, config_file_options.data_directory, MAXPGPATH);
	}
	else
	{
		/* requires superuser */
		get_pg_setting(conn, "data_directory", data_dir);
	}

	server_version_num = get_server_version(conn, NULL);

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

	if (runtime_options.verbose == true)
	{
		uint64 	 	 	local_system_identifier = UNKNOWN_SYSTEM_IDENTIFIER;

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
		bool enabled = true;
		PQExpBufferData archiving_status;
		char archive_command[MAXLEN] = "";

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
		int ready_files;

		ready_files = get_ready_archive_files(conn, data_dir);

		key_value_list_set_format(
			&node_status,
			"WALs pending archiving",
			"%i pending files",
			ready_files);

		if (guc_set(conn, "archive_mode", "=", "off"))
		{
			key_value_list_set_output_mode(&node_status, "WALs pending archiving", OM_CSV);
		}

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


	if (node_info.type == STANDBY)
	{
		key_value_list_set_format(
			&node_status,
			"Upstream node",
			"%s (ID: %i)",
			node_info.node_name,
			node_info.node_id);

		get_replication_info(conn, &replication_info);

		key_value_list_set_format(
			&node_status,
			"Replication lag",
			"%i seconds",
			replication_info.replication_lag_time);

		key_value_list_set_format(
			&node_status,
			"Last received LSN",
			"%X/%X", format_lsn(replication_info.last_wal_receive_lsn));

		key_value_list_set_format(
			&node_status,
			"Last replayed LSN",
			"%X/%X", format_lsn(replication_info.last_wal_replay_lsn));
	}
	else
	{
		key_value_list_set(
			&node_status,
			"Upstream node",
			"(none)");
		key_value_list_set_output_mode(&node_status, "Upstream node", OM_CSV);

		key_value_list_set(
			&node_status,
			"Replication lag",
			"n/a");

		key_value_list_set(
			&node_status,
			"Last received LSN",
			"(none)");
		key_value_list_set_output_mode(&node_status, "Last received LSN", OM_CSV);

		key_value_list_set(
			&node_status,
			"Last replayed LSN",
			"(none)");
		key_value_list_set_output_mode(&node_status, "Last replayed LSN", OM_CSV);
	}


	parse_recovery_conf(data_dir, &recovery_conf);

	/* format output */
	initPQExpBuffer(&output);

	if (runtime_options.output_mode == OM_CSV)
	{
		/* output header */
		appendPQExpBuffer(
			&output,
			"\"Node name\",\"Node ID\",");

		for (cell = node_status.head; cell; cell = cell->next)
		{
			appendPQExpBuffer(
				&output,
				"\"%s\",", cell->key);
		}

		/* we'll add the raw data as well */
		appendPQExpBuffer(
			&output,
			"\"max_walsenders\",\"occupied_walsenders\",\"max_replication_slots\",\"active_replication_slots\",\"inactive_replaction_slots\"\n");

		/* output data */
		appendPQExpBuffer(
			&output,
			"\"%s\",%i,",
			node_info.node_name,
			node_info.node_id);

		for (cell = node_status.head; cell; cell = cell->next)
		{
			appendPQExpBuffer(
				&output,
				"\"%s\",", cell->value);
		}

		appendPQExpBuffer(
			&output,
			"%i,%i,%i,%i,%i\n",
			node_info.max_wal_senders,
			node_info.attached_wal_receivers,
			node_info.max_replication_slots,
			node_info.active_replication_slots,
			node_info.inactive_replication_slots);
	}
	else
	{
		appendPQExpBuffer(
			&output,
			"Node \"%s\":\n",
			node_info.node_name);

		for (cell = node_status.head; cell; cell = cell->next)
		{
			if (cell->output_mode == OM_NOT_SET)
				appendPQExpBuffer(
					&output,
					"\t%s: %s\n", cell->key, cell->value);
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
 * --status=(RUNNING|SHUTDOWN|UNKNOWN)
 * --last-checkpoint=...
 */

static
void _do_node_status_is_shutdown(void)
{
	PGPing status;
	PQExpBufferData output;

	bool is_shutdown = true;
	DBState db_state;
	XLogRecPtr checkPoint = InvalidXLogRecPtr;

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


	status = PQping(config_file_options.conninfo);

	switch (status)
	{
		case PQPING_OK:
			appendPQExpBuffer(&output, "RUNNING");
			is_shutdown = false;
			break;
		case PQPING_REJECT:
			appendPQExpBuffer(&output, "RUNNING");
			is_shutdown = false;
			break;
		case PQPING_NO_ATTEMPT:
		case PQPING_NO_RESPONSE:
			/* status not yet clear */
			break;
	}

	/* check what pg_controldata says */

	db_state = get_db_state(config_file_options.data_directory);

	if (db_state != DB_SHUTDOWNED && db_state != DB_SHUTDOWNED_IN_RECOVERY)
	{
		appendPQExpBuffer(&output, "RUNNING");
		is_shutdown = false;
	}


	checkPoint = get_latest_checkpoint_location(config_file_options.data_directory);

	/* unable to read pg_control, don't know what's happening */
	if (checkPoint == InvalidXLogRecPtr)
	{
		appendPQExpBuffer(&output, "UNKNOWN");
		is_shutdown = false;
	}

	/* server is running in some state - just output --status */
	if (is_shutdown == false)
	{
		printf("%s\n", output.data);
		termPQExpBuffer(&output);
		return;
	}

	appendPQExpBuffer(&output,
					  "SHUTDOWN --last-checkpoint-lsn=%X/%X",
					  format_lsn(checkPoint));

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
	PGconn *conn = NULL;
	PQExpBufferData output;

	t_node_info 	node_info = T_NODE_INFO_INITIALIZER;

	CheckStatusList status_list = { NULL, NULL };
	CheckStatusListCell *cell = NULL;


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

	/* handle specific checks
	 * ====================== */
	if (runtime_options.archiver == true)
	{
		(void) do_node_check_archiver(conn, runtime_options.output_mode, NULL);
		PQfinish(conn);
		return;
	}

	if (runtime_options.replication_lag == true)
	{
		(void) do_node_check_replication_lag(conn, runtime_options.output_mode, NULL);
		PQfinish(conn);
		return;
	}

	/* output general overview */

	initPQExpBuffer(&output);

	//(void) do_node_check_role(conn, runtime_options.output_mode, &output);
	(void) do_node_check_replication_lag(conn, runtime_options.output_mode, &status_list);
	(void) do_node_check_archiver(conn, runtime_options.output_mode, &status_list);


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

CheckStatus
do_node_check_archiver(PGconn *conn, OutputMode mode, CheckStatusList *list_output)
{
	int ready_archive_files = 0;
	CheckStatus status = CHECK_STATUS_UNKNOWN;
	PQExpBufferData details;

	if (mode == OM_CSV)
	{
		log_error(_("--csv output not provided with --archiver option"));
		exit(ERR_BAD_CONFIG);
	}

	initPQExpBuffer(&details);

	ready_archive_files = get_ready_archive_files(conn, config_file_options.data_directory);

	if (ready_archive_files > config_file_options.archiver_lag_critical)
	{
		status = CHECK_STATUS_CRITICAL;

		switch (mode)
		{
			case OM_OPTFORMAT:
				appendPQExpBuffer(
					&details,
					"--files=%i --threshold=%i",
					ready_archive_files, config_file_options.archiver_lag_critical);
				break;
			case OM_NAGIOS:
				appendPQExpBuffer(
					&details,
					"%i pending files (critical: %i)",
					ready_archive_files, config_file_options.archiver_lag_critical);
				break;
			case OM_TEXT:
				appendPQExpBuffer(
					&details,
					"%i pending files, threshold: %i",
					ready_archive_files, config_file_options.archiver_lag_critical);
				break;

			default:
				break;
		}
	}
	else if (ready_archive_files > config_file_options.archiver_lag_warning)
	{
		status = CHECK_STATUS_WARNING;

		switch (mode)
		{
			case OM_OPTFORMAT:
				appendPQExpBuffer(
					&details,
					"--files=%i --threshold=%i",
					ready_archive_files, config_file_options.archiver_lag_warning);
				break;
			case OM_NAGIOS:
				appendPQExpBuffer(
					&details,
					"%i pending files (warning: %i)",
					ready_archive_files, config_file_options.archiver_lag_warning);
				break;
			case OM_TEXT:
				appendPQExpBuffer(
					&details,
					"%i pending files (threshold: %i)",
					ready_archive_files, config_file_options.archiver_lag_warning);
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
				appendPQExpBuffer(
					&details,
					"--files=%i", ready_archive_files);
				break;
			case OM_NAGIOS:
			case OM_TEXT:
				appendPQExpBuffer(
					&details,
					"%i pending files",	ready_archive_files);
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
			printf("PG_ARCHIVER %s: %s\n",
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


CheckStatus
do_node_check_replication_lag(PGconn *conn, OutputMode mode, CheckStatusList *list_output)
{
	CheckStatus status = CHECK_STATUS_UNKNOWN;
	int lag_seconds = 0;
	PQExpBufferData details;

	if (mode == OM_CSV)
	{
		log_error(_("--csv output not provided with --replication-lag option"));
		exit(ERR_BAD_CONFIG);
	}

	initPQExpBuffer(&details);

	lag_seconds = get_replication_lag_seconds(conn);

	log_debug("lag seconds: %i", lag_seconds);

	if (lag_seconds >= config_file_options.replication_lag_critical)
	{
		status = CHECK_STATUS_CRITICAL;

		switch (mode)
		{
			case OM_OPTFORMAT:
				appendPQExpBuffer(
					&details,
					"--lag=%i --threshold=%i",
					lag_seconds, config_file_options.replication_lag_critical);
				break;
			case OM_NAGIOS:
				appendPQExpBuffer(
					&details,
					"%i seconds (critical: %i)",
					lag_seconds, config_file_options.replication_lag_critical);
				break;
			case OM_TEXT:
				appendPQExpBuffer(
					&details,
					"%i seconds, threshold: %i)",
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
				appendPQExpBuffer(
					&details,
					"--lag=%i --threshold=%i",
					lag_seconds, config_file_options.replication_lag_warning);
				break;
			case OM_NAGIOS:
				appendPQExpBuffer(
					&details,
					"%i seconds (warning: %i)",
					lag_seconds, config_file_options.replication_lag_warning);
				break;
			case OM_TEXT:
				appendPQExpBuffer(
					&details,
					"%i seconds, threshold: %i)",
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
				appendPQExpBuffer(
					&details,
					"--lag=%i",
					lag_seconds);
				break;
			case OM_NAGIOS:
			case OM_TEXT:
				appendPQExpBuffer(
					&details,
					"%i seconds",
					lag_seconds);
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
			printf("PG_REPLICATION_LAG %s: %s\n",
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


// --action=...
// --check
// --list -> list what would be executed for each action, filter to --action

// --checkpoint must be run as superuser - check connection
void
do_node_service(void)
{
	t_server_action action = ACTION_UNKNOWN;
	char data_dir[MAXPGPATH] = "";
	char command[MAXLEN] = "";
	PQExpBufferData output;

	action = parse_server_action(runtime_options.action);

	if (action == ACTION_UNKNOWN)
	{
		log_error(_("unknown value \"%s\" provided for parameter --action"),
				  runtime_options.action);
		log_hint(_("valid values are \"start\", \"stop\", \"restart\", \"reload\" and \"promote\""));
		exit(ERR_BAD_CONFIG);
	}

	if (runtime_options.check == true)
	{
		if (action != ACTION_NONE)
			log_warning(_("--action not required for --check"));

		return _do_node_service_check();
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
			PGconn *conn  = NULL;

			if (strlen(config_file_options.conninfo))
				conn = establish_db_connection(config_file_options.conninfo, true);
			else
				conn = establish_db_connection_by_params(&source_conninfo, true);

			log_notice(_("issuing CHECKPOINT"));

			// check superuser conn!
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

	log_notice(_("executing server command \"%s\""), command);

	initPQExpBuffer(&output);

	if (local_command(command, &output) == false)
	{
		exit(ERR_LOCAL_COMMAND);
	}

	termPQExpBuffer(&output);
}


static void
_do_node_service_check(void)
{
}


static void
_do_node_service_list_actions(t_server_action action)
{
	char command[MAXLEN] = "";

	char data_dir[MAXPGPATH] = "";

	bool data_dir_required = false;

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
 * Intended mainly for "internal" use by `node switchover`, which
 * calls this on the target server to excute pg_rewind on a demoted
 * primary with a forked (sic) timeline. Does not currently check
 * whether this is a useful thing to do.
 *
 * TODO: make this into a more generally useful function.
 */
void
do_node_rejoin(void)
{
	PGconn *upstream_conn = NULL;
	RecoveryType upstream_recovery_type = RECTYPE_UNKNOWN;
	DBState db_state;
	PGPing status;
	bool is_shutdown = true;

	PQExpBufferData command;
	PQExpBufferData command_output;
	PQExpBufferData follow_output;
	struct stat statbuf;
	char filebuf[MAXPGPATH] = "";
	t_node_info primary_node_record = T_NODE_INFO_INITIALIZER;

	bool success = true;

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
		log_error(_("database is not shut down cleanly"));

		if (runtime_options.force_rewind == true)
		{
			log_detail(_("pg_rewind will not be able to run"));
		}
		log_hint(_("database should be restarted and shut down cleanly after crash recovery completes"));
		exit(ERR_BAD_CONFIG);
	}


	/* check provided upstream connection */
	upstream_conn = establish_db_connection_by_params(&source_conninfo, true);

	/* establish_db_connection(runtime_options.upstream_conninfo, true); */

	if (get_primary_node_record(upstream_conn, &primary_node_record) == false)
	{
		log_error(_("unable to retrieve primary node record"));
		PQfinish(upstream_conn);
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
	 * Forcibly rewind node if requested (this is mainly for use when
	 * this action is being executed by "repmgr standby switchover")
	 */
	if (runtime_options.force_rewind == true)
	{
		int ret;

		_do_node_archive_config();

		/* execute pg_rewind */
		initPQExpBuffer(&command);

		appendPQExpBuffer(
			&command,
			"%s -D ",
			make_pg_path("pg_rewind"));

		appendShellString(
			&command,
			config_file_options.data_directory);

		appendPQExpBuffer(
			&command,
			" --source-server='%s'",
			primary_node_record.conninfo);

		log_notice(_("executing pg_rewind"));
		log_debug("pg_rewind command is:\n  %s",
				  command.data);

		initPQExpBuffer(&command_output);

		ret = local_command(
			command.data,
			&command_output);

		termPQExpBuffer(&command_output);
		termPQExpBuffer(&command);

		if (ret != 0)
		{
			log_error(_("unable to execute pg_rewind"));
			log_detail(_("see preceding output for details"));
			exit(ERR_BAD_CONFIG);
		}
		/* Restore any previously archived config files */
		_do_node_restore_config();

		/* remove any recovery.done file copied in by pg_rewind */
		snprintf(filebuf, MAXPGPATH,
				 "%s/recovery.done",
				 config_file_options.data_directory);

		if (stat(filebuf, &statbuf) == 0)
		{
			log_verbose(LOG_INFO, _("deleting \"recovery.done\""));

			if (unlink(filebuf) == -1)
			{
				log_warning(_("unable to delete \"%s\""),
							filebuf);
				log_detail("%s", strerror(errno));
			}
		}
	}

	initPQExpBuffer(&follow_output);

	success = do_standby_follow_internal(
		upstream_conn,
		&primary_node_record,
		&follow_output);

	create_event_notification(
		upstream_conn,
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
 * Intended mainly for "internal" use by `node switchover`, which
 * calls this on the target server to archive any configuration files
 * in the data directory, which may be overwritten by an operation
 * like pg_rewind
 *
 * Requires configuration file, optionally --config_archive_dir
 */
static void
_do_node_archive_config(void)
{
	char archive_dir[MAXPGPATH];
	struct stat statbuf;
	struct dirent *arcdir_ent;
	DIR			  *arcdir;


	KeyValueList	config_files = { NULL, NULL };
	KeyValueListCell *cell = NULL;
	int  copied_count = 0;

	format_archive_dir(archive_dir);

	/* sanity-check directory path */
	if (stat(archive_dir, &statbuf) == -1)
	{
		if (errno != ENOENT)
		{
			log_error(_("error encountered when checking archive directory \"%s\""),
					  archive_dir);
			log_detail("%s", strerror(errno));
			exit(ERR_BAD_CONFIG);
		}

		/* attempt to create and open the directory */
		if (mkdir(archive_dir, S_IRWXU) != 0 && errno != EEXIST)
		{
			log_error(_("unable to create temporary archive directory \"%s\""),
					  archive_dir);
			log_detail("%s", strerror(errno));
			exit(ERR_BAD_CONFIG);
		}


	}
	else if(!S_ISDIR(statbuf.st_mode))
    {
		log_error(_("\"%s\" exists but is not a directory"),
				  archive_dir);
		exit(ERR_BAD_CONFIG);
	}


	arcdir = opendir(archive_dir);

	if (arcdir == NULL)
	{
		log_error(_("unable to open archive directory \"%s\""),
				  archive_dir);
		log_detail("%s", strerror(errno));
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * attempt to remove any existing files in the directory
	 * TODO: collate problem files into list
	 */
	while ((arcdir_ent = readdir(arcdir)) != NULL)
	{
		char arcdir_ent_path[MAXPGPATH] = "";

		snprintf(arcdir_ent_path, MAXPGPATH,
				 "%s/%s",
				 archive_dir,
				 arcdir_ent->d_name);

		if (stat(arcdir_ent_path, &statbuf) == 0 && !S_ISREG(statbuf.st_mode))
		{
			continue;
		}

		if (unlink(arcdir_ent_path) == -1)
		{
			log_error(_("unable to delete file in temporary archive directory"));
			log_detail(_("file is:  \"%s\""), arcdir_ent_path);
			log_detail("%s", strerror(errno));
			closedir(arcdir);
			exit(ERR_BAD_CONFIG);
		}
	}

	closedir(arcdir);

	/*
	 * extract list of config files from --config-files
	 */
	{
		int i = 0;
		int j = 0;
		int config_file_len = strlen(runtime_options.config_files);

		char filenamebuf[MAXLEN] = "";
		char pathbuf[MAXPGPATH] = "";

		for (j = 0; j < config_file_len; j++)
		{
			if (runtime_options.config_files[j] == ',')
			{
				int filename_len = j - i;

				if (filename_len > MAXLEN)
					filename_len = MAXLEN - 1;

				strncpy(filenamebuf, runtime_options.config_files + i, filename_len);

				filenamebuf[filename_len] = '\0';

				snprintf(pathbuf, MAXPGPATH,
						 "%s/%s",
						 config_file_options.data_directory,
						 filenamebuf);

				key_value_list_set(
					&config_files,
					filenamebuf,
					pathbuf);

				i = j + 1;
			}
		}

		if (i < config_file_len)
		{
			strncpy(filenamebuf, runtime_options.config_files + i, config_file_len - i);
			snprintf(pathbuf, MAXPGPATH,
					 "%s/%s",
					 config_file_options.data_directory,
					 filenamebuf);
			key_value_list_set(
				&config_files,
				filenamebuf,
				pathbuf);
		}
	}


	for (cell = config_files.head; cell; cell = cell->next)
	{
		char dest_file[MAXPGPATH] = "";

		snprintf(dest_file, MAXPGPATH,
				 "%s/%s",
				 archive_dir,
				 cell->key);
		if (stat(cell->value, &statbuf) == -1)
		{
			log_warning(_("specified file \"%s\" not found, skipping"),
						cell->value);
		}
		else
		{
			log_verbose(LOG_DEBUG, "copying \"%s\" to \"%s\"",
						cell->key, dest_file);
			copy_file(cell->value, dest_file);
			copied_count++;
		}
	}


	log_verbose(LOG_INFO, _("%i files copied to \"%s\""),
				copied_count, archive_dir);
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
 * Requires -D/--pgdata, optionally --config_archive_dir
 *
 * Removes --config_archive_dir after successful copy
 */

static void
_do_node_restore_config(void)
{
	char archive_dir[MAXPGPATH] = "";

	DIR			  *arcdir;
	struct dirent *arcdir_ent;
	int			   copied_count = 0;
	bool		   copy_ok = true;

	format_archive_dir(archive_dir);

	arcdir = opendir(archive_dir);

	if (arcdir == NULL)
	{
		log_error(_("unable to open archive directory \"%s\""),
				  archive_dir);
		log_detail("%s", strerror(errno));
		exit(ERR_BAD_CONFIG);
	}

	while ((arcdir_ent = readdir(arcdir)) != NULL)
	{
		struct stat statbuf;
		char src_file_path[MAXPGPATH];
		char dest_file_path[MAXPGPATH];

		snprintf(src_file_path, MAXPGPATH,
				 "%s/%s",
				 archive_dir,
				 arcdir_ent->d_name);

		/* skip non-files */
		if (stat(src_file_path, &statbuf) == 0 && !S_ISREG(statbuf.st_mode))
		{
			continue;
		}

		snprintf(dest_file_path, MAXPGPATH,
				 "%s/%s",
				 config_file_options.data_directory,
				 arcdir_ent->d_name);

		log_verbose(LOG_DEBUG, "copying \"%s\" to \"%s\"",
					src_file_path, dest_file_path);

		if (copy_file(src_file_path, dest_file_path) == false)
		{
			copy_ok = false;
			log_warning(_("unable to copy \"%s\" to \"%s\""),
						arcdir_ent->d_name, runtime_options.data_dir);
		}
		else
		{
			unlink(src_file_path);
			copied_count++;
		}

	}
	closedir(arcdir);

	log_notice(_("%i files copied to %s"),
			   copied_count,
			   config_file_options.data_directory);

	if (copy_ok == false)
	{
		log_error(_("unable to copy all files from \"%s\""), archive_dir);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Finally, delete directory - it should be empty unless it's been interfered
	 * with for some reason, in which case manual intervention is required
	 */
	if (rmdir(archive_dir) != 0 && errno != EEXIST)
	{
		log_warning(_("unable to delete directory \"%s\""), archive_dir);
		log_detail("%s", strerror(errno));
		log_hint(_("directory may need to be manually removed"));
	}
	else
	{
		log_verbose(LOG_INFO, "directory \"%s\" deleted", archive_dir);
	}

	return;
}




static void
format_archive_dir(char *archive_dir)
{
	snprintf(archive_dir,
			 MAXPGPATH,
			 "%s/repmgr-config-archive-%s",
			 runtime_options.config_archive_dir,
			 config_file_options.node_name);

	log_verbose(LOG_DEBUG, "using archive directory \"%s\"", archive_dir);
}


static bool
copy_file(const char *src_file, const char *dest_file)
{
	FILE  *ptr_old, *ptr_new;
	int  a = 0;

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

	while(1)
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


