/*
 * repmgr-action-daemon.c
 *
 * Implements repmgrd actions for the repmgr command line utility
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

#include <signal.h>
#include <sys/stat.h>			/* for stat() */

#include "repmgr.h"

#include "repmgr-client-global.h"
#include "repmgr-action-daemon.h"

#define REPMGR_SERVICE_STOP_START_WAIT 15
#define REPMGR_SERVICE_STATUS_START_HINT _("use \"repmgr service status\" to confirm that repmgrd was successfully started")
#define REPMGR_SERVICE_STATUS_STOP_HINT _("use \"repmgr service status\" to confirm that repmgrd was successfully stopped")

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
		log_hint(REPMGR_SERVICE_STATUS_START_HINT);
	}
	else
	{
		int i = 0;
		int timeout = REPMGR_SERVICE_STOP_START_WAIT;

		if (runtime_options.wait_provided)
			timeout = runtime_options.wait;

		conn = establish_db_connection(config_file_options.conninfo, false);

		if (PQstatus(conn) != CONNECTION_OK)
		{
			log_notice(_("unable to connect to local node"));
			log_hint(REPMGR_SERVICE_STATUS_START_HINT);
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
				log_hint(REPMGR_SERVICE_STATUS_START_HINT);
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
			log_hint(REPMGR_SERVICE_STATUS_STOP_HINT);
	}
	else
	{
		int i = 0;
		int timeout = REPMGR_SERVICE_STOP_START_WAIT;
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
				log_hint(REPMGR_SERVICE_STATUS_STOP_HINT);

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
					log_hint(REPMGR_SERVICE_STATUS_START_HINT);

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
	printf(_("    %s [OPTIONS] daemon start\n"),   progname());
	printf(_("    %s [OPTIONS] daemon stop\n"),    progname());

	puts("");

	printf(_("DAEMON START\n"));
	puts("");
	printf(_("  \"daemon start\" attempts to start repmgrd on the local node\n"));
	puts("");
	printf(_("    --dry-run               check prerequisites but don't start repmgrd\n"));
	printf(_("    -w/--wait               wait for repmgrd to start (default: %i seconds)\n"), REPMGR_SERVICE_STOP_START_WAIT);
	printf(_("    --no-wait               don't wait for repmgrd to start\n"));
	puts("");

	printf(_("DAEMON STOP\n"));
	puts("");
	printf(_("  \"daemon stop\" attempts to stop repmgrd on the local node\n"));
	puts("");
	printf(_("    --dry-run               check prerequisites but don't stop repmgrd\n"));
	printf(_("    -w/--wait               wait for repmgrd to stop (default: %i seconds)\n"), REPMGR_SERVICE_STOP_START_WAIT);
	printf(_("    --no-wait               don't wait for repmgrd to stop\n"));
	puts("");

	puts("");

	printf(_("%s home page: <%s>\n"), "repmgr", REPMGR_URL);
}
