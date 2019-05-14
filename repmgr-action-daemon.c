/*
 * repmgr-action-daemon.c
 *
 * Implements repmgrd actions for the repmgr command line utility
 * Copyright (c) 2ndQuadrant, 2010-2019
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

#include <signal.h>
#include <sys/stat.h>			/* for stat() */

#include "repmgr.h"

#include "repmgr-client-global.h"
#include "repmgr-action-daemon.h"

#define REPMGR_DAEMON_STOP_START_WAIT 15
#define REPMGR_DAEMON_STATUS_START_HINT _("use \"repmgr daemon status\" to confirm that repmgrd was successfully started")
#define REPMGR_DAEMON_STATUS_STOP_HINT _("use \"repmgr daemon status\" to confirm that repmgrd was successfully stopped")

/*
 * Possibly also show:
 *  - repmgrd start time?
 *  - repmgrd mode
 *  - priority
 *  - whether promotion candidate (due to zero priority/different location)
 */

typedef enum
{
	STATUS_ID = 0,
	STATUS_NAME,
	STATUS_ROLE,
	STATUS_PG,
	STATUS_UPSTREAM_NAME,
	STATUS_LOCATION,
	STATUS_PRIORITY,
	STATUS_REPMGRD,
	STATUS_PID,
	STATUS_PAUSED,
	STATUS_UPSTREAM_LAST_SEEN
} StatusHeader;

#define STATUS_HEADER_COUNT 11

struct ColHeader headers_status[STATUS_HEADER_COUNT];

static void fetch_node_records(PGconn *conn, NodeInfoList *node_list);
static void _do_repmgr_pause(bool pause);


void
do_daemon_status(void)
{
	PGconn	   *conn = NULL;
	NodeInfoList nodes = T_NODE_INFO_LIST_INITIALIZER;
	NodeInfoListCell *cell = NULL;
	int i;
	RepmgrdInfo **repmgrd_info;
	ItemList	warnings = {NULL, NULL};
	bool		connection_error_found = false;

	/* Connect to local database to obtain cluster connection data */
	log_verbose(LOG_INFO, _("connecting to database"));

	if (strlen(config_file_options.conninfo))
		conn = establish_db_connection(config_file_options.conninfo, true);
	else
		conn = establish_db_connection_by_params(&source_conninfo, true);

	fetch_node_records(conn, &nodes);

	repmgrd_info = (RepmgrdInfo **) pg_malloc0(sizeof(RepmgrdInfo *) * nodes.node_count);

	if (repmgrd_info == NULL)
	{
		log_error(_("unable to allocate memory"));
		exit(ERR_OUT_OF_MEMORY);
	}

	strncpy(headers_status[STATUS_ID].title, _("ID"), MAXLEN);
	strncpy(headers_status[STATUS_NAME].title, _("Name"), MAXLEN);
	strncpy(headers_status[STATUS_ROLE].title, _("Role"), MAXLEN);
	strncpy(headers_status[STATUS_PG].title, _("Status"), MAXLEN);
	strncpy(headers_status[STATUS_UPSTREAM_NAME].title, _("Upstream"), MAXLEN);

	/* following only displayed with the --detail option */
	strncpy(headers_status[STATUS_LOCATION].title, _("Location"), MAXLEN);
	if (runtime_options.compact == true)
		strncpy(headers_status[STATUS_PRIORITY].title, _("Prio."), MAXLEN);
	else
		strncpy(headers_status[STATUS_PRIORITY].title, _("Priority"), MAXLEN);

	strncpy(headers_status[STATUS_REPMGRD].title, _("repmgrd"), MAXLEN);
	strncpy(headers_status[STATUS_PID].title, _("PID"), MAXLEN);
	strncpy(headers_status[STATUS_PAUSED].title, _("Paused?"), MAXLEN);

	if (runtime_options.compact == true)
		strncpy(headers_status[STATUS_UPSTREAM_LAST_SEEN].title, _("Upstr. last"), MAXLEN);
	else
		strncpy(headers_status[STATUS_UPSTREAM_LAST_SEEN].title, _("Upstream last seen"), MAXLEN);

	for (i = 0; i < STATUS_HEADER_COUNT; i++)
	{
		headers_status[i].max_length = strlen(headers_status[i].title);
		headers_status[i].display = true;
	}

	if (runtime_options.detail == false)
	{
		headers_status[STATUS_LOCATION].display = false;
		headers_status[STATUS_PRIORITY].display = false;
	}

	i = 0;

	for (cell = nodes.head; cell; cell = cell->next)
	{
		int j;
		PQExpBufferData node_status;
		PQExpBufferData upstream;

		repmgrd_info[i] = pg_malloc0(sizeof(RepmgrdInfo));
		repmgrd_info[i]->node_id = cell->node_info->node_id;
		repmgrd_info[i]->pid = UNKNOWN_PID;
		repmgrd_info[i]->recovery_type = RECTYPE_UNKNOWN;
		repmgrd_info[i]->paused = false;
		repmgrd_info[i]->running = false;
		repmgrd_info[i]->pg_running = true;
		repmgrd_info[i]->wal_paused_pending_wal = false;
		repmgrd_info[i]->upstream_last_seen = -1;

		cell->node_info->conn = establish_db_connection_quiet(cell->node_info->conninfo);

		if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
		{

			connection_error_found = true;

			if (runtime_options.verbose)
			{
				char		error[MAXLEN];

				strncpy(error, PQerrorMessage(cell->node_info->conn), MAXLEN);

				item_list_append_format(&warnings,
										"when attempting to connect to node \"%s\" (ID: %i), following error encountered :\n\"%s\"",
										cell->node_info->node_name, cell->node_info->node_id, trim(error));
			}
			else
			{
				item_list_append_format(&warnings,
										"unable to  connect to node \"%s\" (ID: %i)",
										cell->node_info->node_name, cell->node_info->node_id);
			}

			repmgrd_info[i]->pg_running = false;
			maxlen_snprintf(repmgrd_info[i]->repmgrd_running, "%s", _("n/a"));
			maxlen_snprintf(repmgrd_info[i]->pid_text, "%s", _("n/a"));
		}
		else
		{
			cell->node_info->node_status = NODE_STATUS_UP;
			cell->node_info->recovery_type = get_recovery_type(cell->node_info->conn);

			repmgrd_info[i]->pid = repmgrd_get_pid(cell->node_info->conn);

			repmgrd_info[i]->running = repmgrd_is_running(cell->node_info->conn);

			if (repmgrd_info[i]->running == true)
			{
				maxlen_snprintf(repmgrd_info[i]->repmgrd_running, "%s", _("running"));
			}
			else
			{
				maxlen_snprintf(repmgrd_info[i]->repmgrd_running, "%s", _("not running"));
			}

			if (repmgrd_info[i]->pid == UNKNOWN_PID)
			{
				maxlen_snprintf(repmgrd_info[i]->pid_text, "%s", _("n/a"));
			}
			else
			{
				maxlen_snprintf(repmgrd_info[i]->pid_text, "%i", repmgrd_info[i]->pid);
			}

			repmgrd_info[i]->paused = repmgrd_is_paused(cell->node_info->conn);

			repmgrd_info[i]->recovery_type = get_recovery_type(cell->node_info->conn);

			if (repmgrd_info[i]->recovery_type == RECTYPE_STANDBY)
			{
				repmgrd_info[i]->wal_paused_pending_wal = is_wal_replay_paused(cell->node_info->conn, true);

				if (repmgrd_info[i]->wal_paused_pending_wal == true)
				{
					item_list_append_format(&warnings,
											_("WAL replay is paused on node \"%s\" (ID: %i) with WAL replay pending; this node cannot be manually promoted  until WAL replay is resumed"),
											cell->node_info->node_name, cell->node_info->node_id);
				}
			}

			repmgrd_info[i]->upstream_last_seen = get_upstream_last_seen(cell->node_info->conn, cell->node_info->type);
			if (repmgrd_info[i]->upstream_last_seen < 0)
			{
				maxlen_snprintf(repmgrd_info[i]->upstream_last_seen_text, "%s", _("n/a"));
			}
			else
			{
				if (runtime_options.compact == true)
				{
					maxlen_snprintf(repmgrd_info[i]->upstream_last_seen_text, _("%i sec(s) ago"), repmgrd_info[i]->upstream_last_seen);
				}
				else
				{
					maxlen_snprintf(repmgrd_info[i]->upstream_last_seen_text, _("%i second(s) ago"), repmgrd_info[i]->upstream_last_seen);
				}
			}
		}

		initPQExpBuffer(&node_status);
		initPQExpBuffer(&upstream);

		(void)format_node_status(cell->node_info, &node_status, &upstream, &warnings);
		snprintf(repmgrd_info[i]->pg_running_text, sizeof(cell->node_info->details),
				 "%s", node_status.data);

		snprintf(cell->node_info->upstream_node_name, sizeof(cell->node_info->upstream_node_name),
				 "%s", upstream.data);

		termPQExpBuffer(&node_status);
		termPQExpBuffer(&upstream);

		PQfinish(cell->node_info->conn);

		headers_status[STATUS_NAME].cur_length = strlen(cell->node_info->node_name);
		headers_status[STATUS_ROLE].cur_length = strlen(get_node_type_string(cell->node_info->type));
		headers_status[STATUS_PG].cur_length = strlen(repmgrd_info[i]->pg_running_text);
		headers_status[STATUS_UPSTREAM_NAME].cur_length = strlen(cell->node_info->upstream_node_name);

		if (runtime_options.detail == true)
		{
			PQExpBufferData buf;

			headers_status[STATUS_LOCATION].cur_length = strlen(cell->node_info->location);

			initPQExpBuffer(&buf);
			appendPQExpBuffer(&buf, "%i", cell->node_info->priority);
			headers_status[STATUS_PRIORITY].cur_length = strlen(buf.data);
			termPQExpBuffer(&buf);
		}

		headers_status[STATUS_PID].cur_length = strlen(repmgrd_info[i]->pid_text);
		headers_status[STATUS_REPMGRD].cur_length = strlen(repmgrd_info[i]->repmgrd_running);

		headers_status[STATUS_UPSTREAM_LAST_SEEN].cur_length = strlen(repmgrd_info[i]->upstream_last_seen_text);

		for (j = 0; j < STATUS_HEADER_COUNT; j++)
		{
			if (headers_status[j].cur_length > headers_status[j].max_length)
			{
				headers_status[j].max_length = headers_status[j].cur_length;
			}
		}

		i++;
	}

	/* Print column header row (text mode only) */
	if (runtime_options.output_mode == OM_TEXT)
	{
		print_status_header(STATUS_HEADER_COUNT, headers_status);
	}

	i = 0;

	for (cell = nodes.head; cell; cell = cell->next)
	{
		if (runtime_options.output_mode == OM_CSV)
		{
			int running = repmgrd_info[i]->running ? 1 : 0;
			int paused = repmgrd_info[i]->paused ? 1 : 0;

			/* If PostgreSQL is not running, repmgrd status is unknown */
			if (repmgrd_info[i]->pg_running == false)
			{
				running = -1;
				paused = -1;
			}

			printf("%i,%s,%s,%i,%i,%i,%i,%i,%i,%s\n",
				   cell->node_info->node_id,
				   cell->node_info->node_name,
				   get_node_type_string(cell->node_info->type),
				   repmgrd_info[i]->pg_running ? 1 : 0,
				   running,
				   repmgrd_info[i]->pid,
				   paused,
				   cell->node_info->priority,
				   repmgrd_info[i]->pid == UNKNOWN_PID
				     ? -1
 				     : repmgrd_info[i]->upstream_last_seen,
				   cell->node_info->location);
		}
		else
		{
			printf(" %-*i ",  headers_status[STATUS_ID].max_length, cell->node_info->node_id);
			printf("| %-*s ", headers_status[STATUS_NAME].max_length, cell->node_info->node_name);
			printf("| %-*s ", headers_status[STATUS_ROLE].max_length, get_node_type_string(cell->node_info->type));
			printf("| %-*s ", headers_status[STATUS_PG].max_length, repmgrd_info[i]->pg_running_text);
			printf("| %-*s ", headers_status[STATUS_UPSTREAM_NAME].max_length, cell->node_info->upstream_node_name);

			if (runtime_options.detail == true)
			{
				printf("| %-*s ", headers_status[STATUS_LOCATION].max_length, cell->node_info->location);
				printf("| %-*i ", headers_status[STATUS_PRIORITY].max_length, cell->node_info->priority);
			}

			printf("| %-*s ", headers_status[STATUS_REPMGRD].max_length, repmgrd_info[i]->repmgrd_running);
			printf("| %-*s ", headers_status[STATUS_PID].max_length, repmgrd_info[i]->pid_text);

			if (repmgrd_info[i]->pid == UNKNOWN_PID)
			{
				printf("| %-*s ", headers_status[STATUS_PAUSED].max_length, _("n/a"));
				printf("| %-*s ", headers_status[STATUS_UPSTREAM_LAST_SEEN].max_length, _("n/a"));

			}
			else
			{
				printf("| %-*s ", headers_status[STATUS_PAUSED].max_length, repmgrd_info[i]->paused ? _("yes") : _("no"));

				printf("| %-*s ", headers_status[STATUS_UPSTREAM_LAST_SEEN].max_length, repmgrd_info[i]->upstream_last_seen_text);
			}

			printf("\n");
		}

		pfree(repmgrd_info[i]);
		i++;
	}

	pfree(repmgrd_info);

	/* emit any warnings */

	if (warnings.head != NULL && runtime_options.terse == false && runtime_options.output_mode != OM_CSV)
	{
		ItemListCell *cell = NULL;

		printf(_("\nWARNING: following issues were detected\n"));
		for (cell = warnings.head; cell; cell = cell->next)
		{
			printf(_("  - %s\n"), cell->string);
		}

		if (runtime_options.verbose == false && connection_error_found == true)
		{
			log_hint(_("execute with --verbose option to see connection error messages"));
		}
	}
}

void
do_daemon_pause(void)
{
	_do_repmgr_pause(true);
}

void
do_daemon_unpause(void)
{
	_do_repmgr_pause(false);
}


static void
_do_repmgr_pause(bool pause)
{
	PGconn	   *conn = NULL;
	NodeInfoList nodes = T_NODE_INFO_LIST_INITIALIZER;
	NodeInfoListCell *cell = NULL;
	int i;
	int error_nodes = 0;

	/* Connect to local database to obtain cluster connection data */
	log_verbose(LOG_INFO, _("connecting to database"));

	if (strlen(config_file_options.conninfo))
		conn = establish_db_connection(config_file_options.conninfo, true);
	else
		conn = establish_db_connection_by_params(&source_conninfo, true);

	fetch_node_records(conn, &nodes);

	i = 0;

	for (cell = nodes.head; cell; cell = cell->next)
	{
		log_verbose(LOG_DEBUG, "pausing node %i (%s)",
					cell->node_info->node_id,
					cell->node_info->node_name);
		cell->node_info->conn = establish_db_connection_quiet(cell->node_info->conninfo);

		if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
		{
			log_warning(_("unable to connect to node %i"),
						cell->node_info->node_id);
			error_nodes++;
		}
		else
		{
			if (runtime_options.dry_run == true)
			{
				if (pause == true)
				{
					log_info(_("would pause node %i (%s) "),
							 cell->node_info->node_id,
							 cell->node_info->node_name);
				}
				else
				{
					log_info(_("would unpause node %i (%s) "),
							 cell->node_info->node_id,
							 cell->node_info->node_name);
				}
			}
			else
			{
				bool success = repmgrd_pause(cell->node_info->conn, pause);

				if (success == false)
					error_nodes++;

				log_notice(_("node %i (%s) %s"),
						   cell->node_info->node_id,
						   cell->node_info->node_name,
						   success == true
								? pause == true ? "paused" : "unpaused"
		   						: pause == true ? "not paused" : "not unpaused");
			}
			PQfinish(cell->node_info->conn);
		}
		i++;
	}

	if (error_nodes > 0)
	{
		if (pause == true)
		{
			log_error(_("unable to pause %i node(s)"), error_nodes);
		}
		else
		{
			log_error(_("unable to unpause %i node(s)"), error_nodes);
		}

		log_hint(_("execute \"repmgr daemon status\" to view current status"));

		exit(ERR_REPMGRD_PAUSE);
	}

	exit(SUCCESS);
}



void
fetch_node_records(PGconn *conn, NodeInfoList *node_list)
{
	bool success = get_all_node_records_with_upstream(conn, node_list);

	if (success == false)
	{
		/* get_all_node_records() will display any error message */
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	if (node_list->node_count == 0)
	{
		log_error(_("no node records were found"));
		log_hint(_("ensure at least one node is registered"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}
}


void
do_daemon_start(void)
{
	PGconn	   *conn = NULL;
	PQExpBufferData repmgrd_command;
	PQExpBufferData output_buf;
	bool		success;

	if (config_file_options.repmgrd_service_start_command[0] == '\0')
	{
		log_error(_("\"repmgrd_service_start_command\" is not set"));
		log_hint(_("set \"repmgrd_service_start_command\" in \"repmgr.conf\""));
		exit(ERR_BAD_CONFIG);
	}

	log_verbose(LOG_INFO, _("connecting to local node"));

	conn = establish_db_connection(config_file_options.conninfo, false);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		/* TODO: if PostgreSQL is not available, have repmgrd loop and retry connection */
		log_error(_("unable to connect to local node"));
		log_detail(_("PostgreSQL must be running before \"repmgrd\" can be started"));
		exit(ERR_DB_CONN);
	}

	/*
	 * if local connection available, check if repmgr.so is installed, and
	 * whether repmgrd is running
	 */
	check_shared_library(conn);

	if (is_repmgrd_running(conn) == true)
	{
		pid_t		pid = UNKNOWN_PID;

		log_error(_("repmgrd appears to be running already"));

		pid = repmgrd_get_pid(conn);

		if (pid != UNKNOWN_PID)
			log_detail(_("repmgrd PID is %i"), pid);
		else
			log_warning(_("unable to determine repmgrd PID"));

		PQfinish(conn);
		exit(ERR_REPMGRD_SERVICE);
	}

	PQfinish(conn);


	initPQExpBuffer(&repmgrd_command);
	appendPQExpBufferStr(&repmgrd_command,
						 config_file_options.repmgrd_service_start_command);

	if (runtime_options.dry_run == true)
	{
		log_info(_("prerequisites for starting repmgrd met"));
		log_detail("following command would be executed:\n  %s", repmgrd_command.data);
		exit(SUCCESS);
	}

	log_notice(_("executing: \"%s\""), repmgrd_command.data);

	initPQExpBuffer(&output_buf);

	success = local_command(repmgrd_command.data, &output_buf);
	termPQExpBuffer(&repmgrd_command);

	if (success == false)
	{
		log_error(_("unable to start repmgrd"));
		if (output_buf.data[0] != '\0')
			log_detail("%s", output_buf.data);
		termPQExpBuffer(&output_buf);
		exit(ERR_REPMGRD_SERVICE);
	}

	termPQExpBuffer(&output_buf);

	if (runtime_options.no_wait == true || runtime_options.wait == 0)
	{
		log_hint(REPMGR_DAEMON_STATUS_START_HINT);
	}
	else
	{
		int i = 0;
		int timeout = REPMGR_DAEMON_STOP_START_WAIT;

		if (runtime_options.wait_provided)
			timeout = runtime_options.wait;

		conn = establish_db_connection(config_file_options.conninfo, false);

		if (PQstatus(conn) != CONNECTION_OK)
		{
			log_notice(_("unable to connect to local node"));
			log_hint(REPMGR_DAEMON_STATUS_START_HINT);
			exit(ERR_DB_CONN);
		}

		for (;;)
		{
			if (is_repmgrd_running(conn) == true)
			{
				log_notice(_("repmgrd was successfully started"));
				PQfinish(conn);
				break;
			}

			if (i == timeout)
			{
				PQfinish(conn);
				log_error(_("repmgrd does not appear to have started after %i seconds"),
						  timeout);
				log_hint(REPMGR_DAEMON_STATUS_START_HINT);
				exit(ERR_REPMGRD_SERVICE);
			}

			log_debug("sleeping 1 second; %i of %i attempts to determine if repmgrd is running",
					  i, runtime_options.wait);
			sleep(1);
			i++;
		}
	}
}


void do_daemon_stop(void)
{
	PGconn	   *conn = NULL;
	PQExpBufferData repmgrd_command;
	PQExpBufferData output_buf;
	bool		success;
	bool		have_db_connection = true;
	pid_t		pid = UNKNOWN_PID;

	if (config_file_options.repmgrd_service_stop_command[0] == '\0')
	{
		log_error(_("\"repmgrd_service_stop_command\" is not set"));
		log_hint(_("set \"repmgrd_service_stop_command\" in \"repmgr.conf\""));
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * if local connection available, check if repmgr.so is installed, and
	 * whether repmgrd is running
	 */
	log_verbose(LOG_INFO, _("connecting to local node"));

	conn = establish_db_connection(config_file_options.conninfo, false);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		/*
		 * a PostgreSQL connection is not required to stop repmgrd,
		 */
		log_warning(_("unable to connect to local node"));
		have_db_connection = false;
	}
	else
	{
		check_shared_library(conn);

		if (is_repmgrd_running(conn) == false)
 		{
			log_error(_("repmgrd appears to be stopped already"));
			PQfinish(conn);
			exit(ERR_REPMGRD_SERVICE);
		}

		/* Attempt to fetch the PID, in case we need it later */
		pid = repmgrd_get_pid(conn);
		log_debug("retrieved pid is %i", pid);
	}

	PQfinish(conn);

	initPQExpBuffer(&repmgrd_command);

	appendPQExpBufferStr(&repmgrd_command,
						 config_file_options.repmgrd_service_stop_command);

	if (runtime_options.dry_run == true)
	{
		log_info(_("prerequisites for stopping repmgrd met"));
		log_detail("following command would be executed:\n  %s", repmgrd_command.data);
		exit(SUCCESS);
	}

	log_notice(_("executing: \"%s\""), repmgrd_command.data);

	initPQExpBuffer(&output_buf);

	success = local_command(repmgrd_command.data, &output_buf);
	termPQExpBuffer(&repmgrd_command);

	if (success == false)
	{
		log_error(_("unable to stop repmgrd"));
		if (output_buf.data[0] != '\0')
			log_detail("%s", output_buf.data);
		termPQExpBuffer(&output_buf);
		exit(ERR_REPMGRD_SERVICE);
	}

	termPQExpBuffer(&output_buf);

	if (runtime_options.no_wait == true || runtime_options.wait == 0)
	{
		if (have_db_connection == true)
			log_hint(REPMGR_DAEMON_STATUS_STOP_HINT);
	}
	else
	{
		int i = 0;
		int timeout = REPMGR_DAEMON_STOP_START_WAIT;
		/*
		 *
		 */
		if (pid == UNKNOWN_PID)
		{
			/*
			 * XXX attempt to get pidfile from config
			 *   and get contents
			 *   ( see check_and_create_pid_file() )
			 * if PID still unknown, exit here
			 */
			log_warning(_("unable to determine repmgrd PID"));

			if (have_db_connection == true)
				log_hint(REPMGR_DAEMON_STATUS_STOP_HINT);

			exit(ERR_REPMGRD_SERVICE);
		}

		if (runtime_options.wait_provided)
			timeout = runtime_options.wait;

		for (;;)
		{
			if (kill(pid, 0) == -1)
			{
				if (errno == ESRCH)
				{
					log_notice(_("repmgrd was successfully stopped"));
					exit(SUCCESS);
				}
				else
				{
					log_error(_("unable to determine status of process with PID %i"), pid);
					log_detail("%s", strerror(errno));
					exit(ERR_REPMGRD_SERVICE);
				}
			}


			if (i == timeout)
			{
				log_error(_("repmgrd does not appear to have stopped after %i seconds"),
						  timeout);

				if (have_db_connection == true)
					log_hint(REPMGR_DAEMON_STATUS_START_HINT);

				exit(ERR_REPMGRD_SERVICE);
			}

			log_debug("sleeping 1 second; %i of %i attempts to determine if repmgrd with PID %i is running",
					  i, timeout, pid);
			sleep(1);
			i++;
		}
	}
}


void do_daemon_help(void)
{
	print_help_header();

	printf(_("Usage:\n"));
	printf(_("    %s [OPTIONS] daemon status\n"),  progname());
	printf(_("    %s [OPTIONS] daemon pause\n"),   progname());
	printf(_("    %s [OPTIONS] daemon unpause\n"), progname());
	printf(_("    %s [OPTIONS] daemon start\n"),   progname());
	printf(_("    %s [OPTIONS] daemon stop\n"),    progname());
	puts("");

	printf(_("DAEMON STATUS\n"));
	puts("");
	printf(_("  \"daemon status\" shows the status of repmgrd on each node in the cluster\n"));
	puts("");
	printf(_("    --csv                     emit output as CSV\n"));
	printf(_("    --detail                  show additional detail\n"));
	printf(_("    --verbose                 show text of database connection error messages\n"));
	puts("");

	printf(_("DAEMON START\n"));
	puts("");
	printf(_("  \"daemon start\" attempts to start repmgrd\n"));
	puts("");
	printf(_("    --dry-run               check prerequisites but don't start repmgrd\n"));
	printf(_("    -w/--wait               wait for repmgrd to start (default: %i seconds)\n"), REPMGR_DAEMON_STOP_START_WAIT);
	printf(_("    --no-wait               don't wait for repmgrd to start\n"));
	puts("");

	printf(_("DAEMON STOP\n"));
	puts("");
	printf(_("  \"daemon stop\" attempts to stop repmgrd\n"));
	puts("");
	printf(_("    --dry-run               check prerequisites but don't stop repmgrd\n"));
	printf(_("    -w/--wait               wait for repmgrd to stop (default: %i seconds)\n"), REPMGR_DAEMON_STOP_START_WAIT);
	printf(_("    --no-wait               don't wait for repmgrd to stop\n"));
	puts("");

	printf(_("DAEMON PAUSE\n"));
	puts("");
	printf(_("  \"daemon pause\" instructs repmgrd on each node to pause failover detection\n"));
	puts("");
	printf(_("    --dry-run               check if nodes are reachable but don't pause repmgrd\n"));
	puts("");

	printf(_("DAEMON UNPAUSE\n"));
	puts("");
	printf(_("  \"daemon unpause\"  instructs repmgrd on each node to resume failover detection\n"));
	puts("");
	printf(_("    --dry-run               check if nodes are reachable but don't unpause repmgrd\n"));
	puts("");

	puts("");
}
