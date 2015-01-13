/*
 * repmgrd.c - Replication manager daemon
 * Copyright (C) 2ndQuadrant, 2010-2015
 *
 * This module connects to the nodes of a replication cluster and monitors
 * how far are they from master
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


/* ZZZ - remove superfluous debugging output */

#include <signal.h>

#include <sys/types.h>
#include <sys/stat.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "repmgr.h"
#include "config.h"
#include "log.h"
#include "strutil.h"
#include "version.h"

/* Required PostgreSQL headers */
#include "access/xlogdefs.h"

/*
 * Struct to keep info about the nodes, used in the voting process in
 * do_failover()
 */
typedef struct s_node_info
{
	int			node_id;
	char		conninfo_str[MAXLEN];
	XLogRecPtr	xlog_location;
	bool		is_ready;
	bool		is_visible;
	bool		is_witness;
}	t_node_info;


/* Local info */
t_configuration_options local_options;
int			my_local_mode = STANDBY_MODE;
PGconn	   *my_local_conn = NULL;

/* Primary info */
t_configuration_options primary_options;

PGconn	   *primary_conn = NULL;

const char *progname;

char	   *config_file = DEFAULT_CONFIG_FILE;
bool		verbose = false;
bool		monitoring_history = false;

bool		failover_done = false;

char	   *pid_file = NULL;

t_configuration_options config = T_CONFIGURATION_OPTIONS_INITIALIZER;

static void help(const char *progname);
static void usage(void);
static void check_cluster_configuration(PGconn *conn);
static void check_node_configuration(void);

static void standby_monitor(void);
static void witness_monitor(void);
static bool check_connection(PGconn *conn, const char *type);
static void update_shared_memory(char *last_wal_standby_applied);
static void update_registration(void);
static void do_failover(void);

static XLogRecPtr lsn_to_xlogrecptr(char *lsn, bool *format_ok);

/*
 * Flag to mark SIGHUP. Whenever the main loop comes around it
 * will reread the configuration file.
 */
static volatile sig_atomic_t got_SIGHUP = false;

static void handle_sighup(SIGNAL_ARGS);
static void handle_sigint(SIGNAL_ARGS);

static void terminate(int retval);

#ifndef WIN32
static void setup_event_handlers(void);
#endif

static void do_daemonize(void);
static void check_and_create_pid_file(const char *pid_file);

static void
close_connections()
{
	if (primary_conn != NULL && PQisBusy(primary_conn) == 1)
		cancel_query(primary_conn, local_options.master_response_timeout);

	if (my_local_conn != NULL)
		PQfinish(my_local_conn);

	if (primary_conn != NULL && primary_conn != my_local_conn)
		PQfinish(primary_conn);

	primary_conn = NULL;
	my_local_conn = NULL;
}


int
main(int argc, char **argv)
{
	static struct option long_options[] =
	{
		{"config-file", required_argument, NULL, 'f'},
		{"verbose", no_argument, NULL, 'v'},
		{"monitoring-history", no_argument, NULL, 'm'},
		{"daemonize", no_argument, NULL, 'd'},
		{"pid-file", required_argument, NULL, 'p'},
		{NULL, 0, NULL, 0}
	};

	int			optindex;
	int			c,
				ret;
	bool		daemonize = false;
	FILE	   *fd;

	int			server_version_num = 0;
	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			help(progname);
			exit(SUCCESS);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			printf("%s %s (PostgreSQL %s)\n", progname, REPMGR_VERSION, PG_VERSION);
			exit(SUCCESS);
		}
	}

	while ((c = getopt_long(argc, argv, "f:v:mdp:", long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'f':
				config_file = optarg;
				break;
			case 'v':
				verbose = true;
				break;
			case 'm':
				monitoring_history = true;
				break;
			case 'd':
				daemonize = true;
				break;
			case 'p':
				pid_file = optarg;
				break;
			default:
				usage();
				exit(ERR_BAD_CONFIG);
		}
	}

	if (daemonize)
	{
		do_daemonize();
	}

	if (pid_file)
	{
		check_and_create_pid_file(pid_file);
	}

#ifndef WIN32
	setup_event_handlers();
#endif

	/*
	 * Read the configuration file: repmgr.conf
	 */
	parse_config(config_file, &local_options);
	if (local_options.node == -1)
	{
		log_err(_("Node information is missing. "
				  "Check the configuration file, or provide one if you have not done so.\n"));
		terminate(ERR_BAD_CONFIG);
	}

	fd = freopen("/dev/null", "r", stdin);
	if (fd == NULL)
	{
		fprintf(stderr, "error reopening stdin to '/dev/null': %s",
				strerror(errno));
	}

	fd = freopen("/dev/null", "w", stdout);
	if (fd == NULL)
	{
		fprintf(stderr, "error reopening stdout to '/dev/null': %s",
				strerror(errno));
	}

	logger_init(&local_options, progname, local_options.loglevel,
				local_options.logfacility);
	if (verbose)
		logger_min_verbose(LOG_INFO);

	if (log_type == REPMGR_SYSLOG)
	{
		fd = freopen("/dev/null", "w", stderr);

		if (fd == NULL)
		{
			fprintf(stderr, "error reopening stderr to '/dev/null': %s",
					strerror(errno));
		}
	}

	/* Initialise the repmgr schema name */
	maxlen_snprintf(repmgr_schema, "%s%s", DEFAULT_REPMGR_SCHEMA_PREFIX,
			 local_options.cluster_name);

	log_info(_("%s Connecting to database '%s'\n"), progname,
			 local_options.conninfo);
	my_local_conn = establish_db_connection(local_options.conninfo, true);

	/* Verify that server is a supported version */
	log_info(_("%s connected to database, checking its state\n"), progname);
	server_version_num = get_server_version(my_local_conn, NULL);
	if(server_version_num < MIN_SUPPORTED_VERSION_NUM)
	{
		if (server_version_num > 0)
			log_err(_("%s requires PostgreSQL %s or better\n"),
					progname,
					MIN_SUPPORTED_VERSION
				);
		terminate(ERR_BAD_CONFIG);
	}


	/*
	 * MAIN LOOP This loops cycles at startup and once per failover and
	 * Requisites: - my_local_conn needs to be already setted with an active
	 * connection - no master connection
	 */
	do
	{
		log_debug("main loop...\n");
		/*
		 * Set my server mode, establish a connection to primary and start
		 * monitor
		 */
		ret = is_witness(my_local_conn,
						 local_options.cluster_name, local_options.node);

		if (ret == 1)
			my_local_mode = WITNESS_MODE;
		else if (ret == 0)
		{
			ret = is_standby(my_local_conn);

			if (ret == 1)
				my_local_mode = STANDBY_MODE;
			else if (ret == 0)	/* is the master */
				my_local_mode = PRIMARY_MODE;
		}

		/*
		 * XXX we did this before changing is_standby() to return int; we
		 * should not exit at this point, but for now we do until we have a
		 * better strategy
		 */
		if (ret == -1)
			terminate(1);

		switch (my_local_mode)
		{
			case PRIMARY_MODE:
				primary_options.node = local_options.node;
				strncpy(primary_options.conninfo, local_options.conninfo,
						MAXLEN);
				primary_conn = my_local_conn;

				check_cluster_configuration(my_local_conn);
				check_node_configuration();

				if (reload_config(config_file, &local_options))
				{
					PQfinish(my_local_conn);
					my_local_conn = establish_db_connection(local_options.conninfo, true);
					primary_conn = my_local_conn;
					update_registration();
				}

				log_info(_("%s Starting continuous primary connection check\n"),
						 progname);

				/*
				 * Check that primary is still alive, and standbies are
				 * sending info
				 */

				/*
				 * Every local_options.monitor_interval_secs seconds, do
				 * master checks XXX Check that standbies are sending info
				 */
				do
				{
					log_debug("primary check loop...\n");
					if (check_connection(primary_conn, "master"))
					{
						/*
						 * CheckActiveStandbiesConnections();
						 * CheckInactiveStandbies();
						 */
						sleep(local_options.monitor_interval_secs);
					}
					else
					{
						/*
						 * XXX May we do something more verbose ?
						 */
						terminate(1);
					}

					if (got_SIGHUP)
					{
						/*
						 * if we can reload, then could need to change
						 * my_local_conn
						 */
						if (reload_config(config_file, &local_options))
						{
							PQfinish(my_local_conn);
							my_local_conn = establish_db_connection(local_options.conninfo, true);
							primary_conn = my_local_conn;

							if (*local_options.logfile)
							{
								FILE	   *fd;

								fd = freopen(local_options.logfile, "a", stderr);
								if (fd == NULL)
								{
									fprintf(stderr, "error reopening stderr to '%s': %s",
									 local_options.logfile, strerror(errno));
								}

							}

							update_registration();
						}
						got_SIGHUP = false;
					}
				} while (!failover_done);
				break;

			case WITNESS_MODE:
			case STANDBY_MODE:
				/* I need the id of the primary as well as a connection to it */
				log_info(_("%s Connecting to primary for cluster '%s'\n"),
						 progname, local_options.cluster_name);
				primary_conn = get_master_connection(my_local_conn,
													 local_options.cluster_name,
													 &primary_options.node, NULL);
				if (primary_conn == NULL)
				{
					terminate(ERR_BAD_CONFIG);
				}

				check_cluster_configuration(my_local_conn);
				check_node_configuration();

				if (reload_config(config_file, &local_options))
				{
					PQfinish(my_local_conn);
					my_local_conn = establish_db_connection(local_options.conninfo, true);
					update_registration();
				}

				/*
				 * Every local_options.monitor_interval_secs seconds, do
				 * checks
				 */
				if (my_local_mode == WITNESS_MODE)
				{
					log_info(_("%s Starting continuous witness node monitoring\n"),
							 progname);
				}
				else if (my_local_mode == STANDBY_MODE)
				{
					log_info(_("%s Starting continuous standby node monitoring\n"),
							 progname);
				}

				do
				{
					log_debug("standby check loop...\n");

					if (my_local_mode == WITNESS_MODE)
						witness_monitor();
					else if (my_local_mode == STANDBY_MODE)
					{
						standby_monitor();
						log_debug(_("returned from standby_monitor()\n"));
					}
					sleep(local_options.monitor_interval_secs);

					if (got_SIGHUP)
					{
						/*
						 * if we can reload, then could need to change
						 * my_local_conn
						 */
						if (reload_config(config_file, &local_options))
						{
							PQfinish(my_local_conn);
							my_local_conn = establish_db_connection(local_options.conninfo, true);
							update_registration();
						}
						got_SIGHUP = false;
					}
					if(failover_done)
					{
						log_debug(_("standby check loop will terminate\n"));
					}
				} while (!failover_done);
				break;
			default:
				log_err(_("%s: Unrecognized mode for node %d\n"), progname,
						local_options.node);
		}

		log_debug(_("end of main loop\n"));

		failover_done = false;

	} while (true);

	/* close the connection to the database and cleanup */
	close_connections();

	/* Shuts down logging system */
	logger_shutdown();

	return 0;
}


/*
 * witness_monitor()
 *
 * Monitors witness server; attempt to find and connect to new primary
 * if existing primary connection is lost
 */
static void
witness_monitor(void)
{
	char		monitor_witness_timestamp[MAXLEN];
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];
	bool        connection_ok;

	/*
	 * Check if master is available; if not, assume failover situation
	 * and try to determine new master. There may be a delay between detection
	 * of a missing master and promotion of a standby by that standby's
	 * rempgrd, so we'll loop for a while before giving up.
	 */
	connection_ok = check_connection(primary_conn, "master");

	if(connection_ok == false)
	{
		int			connection_retries;
		log_debug(_("Old primary node ID: %i\n"), primary_options.node);

		/* We need to wait a while for the new primary to be promoted */
		log_info(
			_("Waiting %i seconds for a new master to be promoted...\n"),
			local_options.master_response_timeout
			);

		sleep(local_options.master_response_timeout);

		/* Attempt to find the new master */
		for (connection_retries = 0; connection_retries < local_options.reconnect_attempts; connection_retries++)
		{
			log_info(
				_("Attempt %i of %i to determine new master...\n"),
				connection_retries + 1,
				local_options.reconnect_attempts
				);
			primary_conn = get_master_connection(my_local_conn,
											 local_options.cluster_name, &primary_options.node, NULL);

			if (PQstatus(primary_conn) != CONNECTION_OK)
			{
				log_warning(
					_("Unable to determine a valid master server; waiting %i seconds to retry...\n"),
					local_options.reconnect_intvl
					);
				PQfinish(primary_conn);
				sleep(local_options.reconnect_intvl);
			}
			else
			{
				log_debug(_("New master found with node ID: %i\n"), primary_options.node);
				connection_ok = true;
				break;
			}
		}

		if(connection_ok == false)
		{
			log_err(_("Unable to determine a valid master server, exiting...\n"));
			terminate(ERR_DB_CON);
		}

	}

	/* Fast path for the case where no history is requested */
	if (!monitoring_history)
		return;

	/*
	 * Cancel any query that is still being executed, so i can insert the
	 * current record
	 */
	if (!cancel_query(primary_conn, local_options.master_response_timeout))
		return;
	if (wait_connection_availability(primary_conn,
								 local_options.master_response_timeout) != 1)
		return;

	/* Get local xlog info */
	sqlquery_snprintf(sqlquery, "SELECT CURRENT_TIMESTAMP");

	res = PQexec(my_local_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s\n"), PQerrorMessage(my_local_conn));
		PQclear(res);
		/* if there is any error just let it be and retry in next loop */
		return;
	}

	strcpy(monitor_witness_timestamp, PQgetvalue(res, 0, 0));
	PQclear(res);

	/*
	 * Build the SQL to execute on primary
	 */
	sqlquery_snprintf(sqlquery,
					  "INSERT INTO %s.repl_monitor "
					  "           (primary_node, standby_node, "
					  "            last_monitor_time, last_apply_time, "
					  "            last_wal_primary_location, last_wal_standby_location, "
					  "            replication_lag, apply_lag )"
					  "      VALUES(%d, %d, "
					  "             '%s'::TIMESTAMP WITH TIME ZONE, NULL, "
					  "             pg_current_xlog_location(), NULL, "
					  "             0, 0) ",
					  get_repmgr_schema_quoted(my_local_conn),
					  primary_options.node,
					  local_options.node,
					  monitor_witness_timestamp);

	/*
	 * Execute the query asynchronously, but don't check for a result. We will
	 * check the result next time we pause for a monitor step.
	 */
	log_debug("witness_monitor: %s\n", sqlquery);
	if (PQsendQuery(primary_conn, sqlquery) == 0)
		log_warning(_("Query could not be sent to primary. %s\n"),
					PQerrorMessage(primary_conn));
}


/*
 * Insert monitor info, this is basically the time and xlog replayed,
 * applied on standby and current xlog location in primary.
 * Also do the math to see how far are we in bytes for being uptodate
 */
static void
standby_monitor(void)
{
	PGresult   *res;
	char		monitor_standby_timestamp[MAXLEN];
	char		last_wal_primary_location[MAXLEN];
	char		last_wal_standby_received[MAXLEN];
	char		last_wal_standby_applied[MAXLEN];
	char		last_wal_standby_applied_timestamp[MAXLEN];
	char		sqlquery[QUERY_STR_LEN];

	XLogRecPtr	lsn_primary;
	XLogRecPtr	lsn_standby_received;
	XLogRecPtr	lsn_standby_applied;

	int			connection_retries,
				ret;
	bool		did_retry = false;

	/*
	 * Check if the master is still available, if after 5 minutes of retries
	 * we cannot reconnect, try to get a new master.
	 */
	check_connection(primary_conn, "master");	/* this take up to
												 * local_options.reconnect_atte
												 * mpts *
												 * local_options.reconnect_intv
												 * l seconds */

	if (!check_connection(my_local_conn, "standby"))
	{
		log_err("Failed to connect to local node, exiting!\n");
		terminate(1);
	}

	if (PQstatus(primary_conn) != CONNECTION_OK)
	{
		PQfinish(primary_conn);
		primary_conn = NULL;

		if (local_options.failover == MANUAL_FAILOVER)
		{
			log_err(_("We couldn't reconnect to master. Now checking if another node has been promoted.\n"));

			for (connection_retries = 0; connection_retries < local_options.reconnect_attempts; connection_retries++)
			{
				primary_conn = get_master_connection(my_local_conn,
					local_options.cluster_name, &primary_options.node, NULL);
				if (PQstatus(primary_conn) == CONNECTION_OK)
				{
					/*
					 * Connected, we can continue the process so break the
					 * loop
					 */
					log_err(_("Connected to node %d, continue monitoring.\n"),
							primary_options.node);
					break;
				}
				else
				{
					log_err(
					    _("We haven't found a new master, waiting %i seconds before retry...\n"),
					    local_options.retry_promote_interval_secs
					    );

					sleep(local_options.retry_promote_interval_secs);
				}
			}

			if (PQstatus(primary_conn) != CONNECTION_OK)
			{
				log_err(_("We couldn't reconnect for long enough, exiting...\n"));
				terminate(ERR_DB_CON);
			}
		}
		else if (local_options.failover == AUTOMATIC_FAILOVER)
		{
			/*
			 * When we returns from this function we will have a new primary
			 * and a new primary_conn
			 */
			do_failover();
			log_debug("standby_monitor() - returning from do_failover()\n");
			return;
		}
	}

	/* Check if we still are a standby, we could have been promoted */
	do
	{
		log_debug("standby_monitor() - checking if still standby\n");
		ret = is_standby(my_local_conn);

		switch (ret)
		{
			case 0:
				/*
				 * This situation can occur if `pg_ctl promote` was manually executed
				 * on the node. If the original master is still running after this
				 * node has been promoted, we're in a "two brain" situation which
				 * will require manual resolution as there's no way of determing
				 * which master is the correct one.
				 *
				 * XXX check if the original master is still active and display a
				 * warning
				 */
				log_err(_("It seems like we have been promoted, so exit from monitoring...\n"));
				terminate(1);
				break;

			case -1:
				log_err(_("Standby node disappeared, trying to reconnect...\n"));
				did_retry = true;

				if (!check_connection(my_local_conn, "standby"))
				{
					terminate(0);
				}

				break;
		}
	} while (ret == -1);

	if (did_retry)
	{
		log_info(_("standby connection got back up again!\n"));
	}

	/* Fast path for the case where no history is requested */
	if (!monitoring_history)
		return;

	/*
	 * Cancel any query that is still being executed, so i can insert the
	 * current record
	 */
	if (!cancel_query(primary_conn, local_options.master_response_timeout))
		return;
	if (wait_connection_availability(primary_conn, local_options.master_response_timeout) != 1)
		return;

	/* Get local xlog info */
	sqlquery_snprintf(sqlquery,
					  "SELECT CURRENT_TIMESTAMP, pg_last_xlog_receive_location(), "
					  "pg_last_xlog_replay_location(), pg_last_xact_replay_timestamp() ");

	res = PQexec(my_local_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s\n"), PQerrorMessage(my_local_conn));
		PQclear(res);
		/* if there is any error just let it be and retry in next loop */
		return;
	}

	strncpy(monitor_standby_timestamp, PQgetvalue(res, 0, 0), MAXLEN);
	strncpy(last_wal_standby_received, PQgetvalue(res, 0, 1), MAXLEN);
	strncpy(last_wal_standby_applied, PQgetvalue(res, 0, 2), MAXLEN);
	strncpy(last_wal_standby_applied_timestamp, PQgetvalue(res, 0, 3), MAXLEN);
	PQclear(res);

	/* Get primary xlog info */
	sqlquery_snprintf(sqlquery, "SELECT pg_current_xlog_location()");

	res = PQexec(primary_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s\n"), PQerrorMessage(primary_conn));
		PQclear(res);
		return;
	}

	strncpy(last_wal_primary_location, PQgetvalue(res, 0, 0), MAXLEN);
	PQclear(res);

	/* Calculate the lag */
	lsn_primary = lsn_to_xlogrecptr(last_wal_primary_location, NULL);
	lsn_standby_received = lsn_to_xlogrecptr(last_wal_standby_received, NULL);
	lsn_standby_applied = lsn_to_xlogrecptr(last_wal_standby_applied, NULL);

	/*
	 * Build the SQL to execute on primary
	 */
	sqlquery_snprintf(sqlquery,
					  "INSERT INTO %s.repl_monitor "
					  "           (primary_node, standby_node, "
					  "            last_monitor_time, last_apply_time, "
					  "            last_wal_primary_location, last_wal_standby_location, "
					  "            replication_lag, apply_lag ) "
					  "      VALUES(%d, %d, "
					  "             '%s'::TIMESTAMP WITH TIME ZONE, '%s'::TIMESTAMP WITH TIME ZONE, "
					  "             '%s', '%s', "
					  "             %llu, %llu) ",
					  get_repmgr_schema_quoted(primary_conn),
					  primary_options.node, local_options.node,
					   monitor_standby_timestamp, last_wal_standby_applied_timestamp,
					  last_wal_primary_location, last_wal_standby_received,
					  (long long unsigned int)(lsn_primary - lsn_standby_received),
					  (long long unsigned int)(lsn_standby_received - lsn_standby_applied));

	/*
	 * Execute the query asynchronously, but don't check for a result. We will
	 * check the result next time we pause for a monitor step.
	 */
	log_debug("standby_monitor: %s\n", sqlquery);
	if (PQsendQuery(primary_conn, sqlquery) == 0)
		log_warning(_("Query could not be sent to primary. %s\n"),
					PQerrorMessage(primary_conn));
}


static void
do_failover(void)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];

	int			total_nodes = 0;
	int			visible_nodes = 0;
	int			ready_nodes = 0;

	bool		candidate_found = false;

	int			i;
	int			r;

	XLogRecPtr	xlog_recptr;
	bool		lsn_format_ok;

	char		last_wal_standby_applied[MAXLEN];

	PGconn	   *node_conn = NULL;

	/*
	 * will get info about until 50 nodes, which seems to be large enough for
	 * most scenarios
	 */
	t_node_info nodes[FAILOVER_NODES_MAX_CHECK];

	/* initialize to keep compiler quiet */
	t_node_info best_candidate = {-1, "", InvalidXLogRecPtr, false, false, false};

	/* get a list of standby nodes, including myself */
	sprintf(sqlquery,
			"SELECT id, conninfo, witness "
			"  FROM %s.repl_nodes "
			" WHERE cluster = '%s' "
			" ORDER BY priority, id "
			" LIMIT %i ",
			get_repmgr_schema_quoted(my_local_conn),
			local_options.cluster_name,
			FAILOVER_NODES_MAX_CHECK);

	res = PQexec(my_local_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Unable to retrieve node records: %s\n"), PQerrorMessage(my_local_conn));
		PQclear(res);
		terminate(ERR_DB_QUERY);
	}

	/*
	 * total nodes that are registered
	 */
	total_nodes = PQntuples(res);
	log_debug(_("%s: there are %d nodes registered\n"), progname, total_nodes);

	/*
	 * Build an array with the nodes and indicate which ones are visible and
	 * ready
	 */
	for (i = 0; i < total_nodes; i++)
	{
		nodes[i].node_id = atoi(PQgetvalue(res, i, 0));
		strncpy(nodes[i].conninfo_str, PQgetvalue(res, i, 1), MAXLEN);
		nodes[i].is_witness = (strcmp(PQgetvalue(res, i, 2), "t") == 0) ? true : false;

		/*
		 * Initialize on false so if we can't reach this node we know that
		 * later
		 */
		nodes[i].is_visible = false;
		nodes[i].is_ready = false;

		nodes[i].xlog_location = 0;

		log_debug(_("%s: node=%d conninfo=\"%s\" witness=%s\n"),
				  progname, nodes[i].node_id, nodes[i].conninfo_str,
				  (nodes[i].is_witness) ? "true" : "false");

		node_conn = establish_db_connection(nodes[i].conninfo_str, false);

		/* if we can't see the node just skip it */
		if (PQstatus(node_conn) != CONNECTION_OK)
		{
			if (node_conn != NULL)
				PQfinish(node_conn);

			continue;
		}

		visible_nodes++;
		nodes[i].is_visible = true;

		PQfinish(node_conn);
	}
	PQclear(res);

	log_debug(_("Total nodes counted: registered=%d, visible=%d\n"),
			  total_nodes, visible_nodes);

	/*
	 * am i on the group that should keep alive? if i see less than half of
	 * total_nodes then i should do nothing
	 */
	if (visible_nodes < (total_nodes / 2.0))
	{
		log_err(_("Can't reach most of the nodes.\n"
				  "Let the other standby servers decide which one will be the primary.\n"
		"Manual action will be needed to re-add this node to the cluster.\n"));
		terminate(ERR_FAILOVER_FAIL);
	}

	/* Query all the nodes to determine which ones are ready */
	for (i = 0; i < total_nodes; i++)
	{
		log_debug("checking node %i...\n", nodes[i].node_id);
		/* if the node is not visible, skip it */
		if (!nodes[i].is_visible)
			continue;

		/* if the node is a witness node, skip it */
		if (nodes[i].is_witness)
			continue;

		node_conn = establish_db_connection(nodes[i].conninfo_str, false);

		/*
		 * XXX This shouldn't happen, if this happens it means this is a major
		 * problem maybe network outages? anyway, is better for a human to
		 * react
		 */
		if (PQstatus(node_conn) != CONNECTION_OK)
		{
			log_err(_("It seems new problems are arising, manual intervention is needed\n"));
			terminate(ERR_FAILOVER_FAIL);
		}

		sqlquery_snprintf(sqlquery, "SELECT pg_last_xlog_receive_location()");
		res = PQexec(node_conn, sqlquery);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			log_info(_("Can't get node's last standby location: %s\n"),
					 PQerrorMessage(node_conn));
			log_info(_("Connection details: %s\n"), nodes[i].conninfo_str);
			PQclear(res);
			PQfinish(node_conn);
			terminate(ERR_FAILOVER_FAIL);
		}

		xlog_recptr = lsn_to_xlogrecptr(PQgetvalue(res, 0, 0), &lsn_format_ok);

		log_debug(_("LSN of node %i is: %s\n"), nodes[i].node_id, PQgetvalue(res, 0, 0));

		/* If position is 0/0, error */
		if(xlog_recptr == InvalidXLogRecPtr)
		{
			PQclear(res);
			PQfinish(node_conn);
			log_info(_("InvalidXLogRecPtr detected on standby node %i\n"), nodes[i].node_id);
			terminate(ERR_FAILOVER_FAIL);
		}

		nodes[i].xlog_location = xlog_recptr;

		PQclear(res);
		PQfinish(node_conn);
	}

	/* last we get info about this node, and update shared memory */
	sprintf(sqlquery, "SELECT pg_last_xlog_receive_location()");
	res = PQexec(my_local_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s.\nReport an invalid value to not be "
				  " considered as new primary and exit.\n"),
				PQerrorMessage(my_local_conn));
		PQclear(res);
		sprintf(last_wal_standby_applied, "'%X/%X'", 0, 0);
		update_shared_memory(last_wal_standby_applied);
		terminate(ERR_DB_QUERY);
	}
	/* write last location in shared memory */
	update_shared_memory(PQgetvalue(res, 0, 0));
	PQclear(res);

	/* Wait for each node to come up and report a valid LSN */
	for (i = 0; i < total_nodes; i++)
	{
		log_debug(_("is_ready check for node %i\n"), nodes[i].node_id);
		while (!nodes[i].is_ready)
		{
			/*
			 * the witness will always be masked as ready if it's still not
			 * marked that way and avoid a useless query
			 */
			if (nodes[i].is_witness)
			{
				if (!nodes[i].is_ready)
				{
					nodes[i].is_ready = true;
					ready_nodes++;
				}
				break;
			}

			/* if the node is not visible, skip it */
			if (!nodes[i].is_visible)
				break;

			/* if the node is ready there is nothing to check, skip it too */
			/* ZZZ is this check pointless? */
			if (nodes[i].is_ready)
				break;

			node_conn = establish_db_connection(nodes[i].conninfo_str, false);

			/*
			 * XXX This shouldn't happen, if this happens it means this is a
			 * major problem maybe network outages? anyway, is better for a
			 * human to react
			 */
			if (PQstatus(node_conn) != CONNECTION_OK)
			{
				/* XXX */
				log_info(_("At this point, it could be some race conditions "
						"that are acceptable, assume the node is restarting "
						   "and starting failover procedure\n"));
				break;
			}

			sqlquery_snprintf(sqlquery,
							  "SELECT %s.repmgr_get_last_standby_location()",
							  get_repmgr_schema_quoted(node_conn));
			res = PQexec(node_conn, sqlquery);
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				log_err(_("PQexec failed: %s.\nReport an invalid value to not"
						  "be considered as new primary and exit.\n"),
						PQerrorMessage(node_conn));
				PQclear(res);
				PQfinish(node_conn);
				terminate(ERR_DB_QUERY);
			}

			xlog_recptr = lsn_to_xlogrecptr(PQgetvalue(res, 0, 0), &lsn_format_ok);

			/* If position reported as "invalid", check for format error or
			 * empty string; oherwise position is 0/0 and we need to continue
			 * looping until a valid LSN is reported
			 */
			if(xlog_recptr == InvalidXLogRecPtr)
			{
				log_debug("Invalid LSN returned - '%s'", PQgetvalue(res, 0, 0));

				if(lsn_format_ok == false)
				{
					/* Unable to parse value returned by `repmgr_get_last_standby_location()` */
					if(*PQgetvalue(res, 0, 0) == '\0')
					{
						log_crit("Whoops, seems as if shared_preload_libraries=repmgr_funcs is not set!\n");
						exit(ERR_BAD_CONFIG);
					}

					/*
					 * Very unlikely to happen; in the absence of any better
					 * strategy keep checking
					 */
					log_warning(_("Unable to parse LSN \"%s\"\n"),
								PQgetvalue(res, 0, 0));
				}

				PQclear(res);
				PQfinish(node_conn);

				/* If position is 0/0, keep checking */
				continue;
			}

			if (nodes[i].xlog_location < xlog_recptr)
			{
				nodes[i].xlog_location = xlog_recptr;
			}

			log_debug(_("LSN of node %i is: %s\n"), nodes[i].node_id, PQgetvalue(res, 0, 0));
			PQclear(res);
			PQfinish(node_conn);

			ready_nodes++;
			nodes[i].is_ready = true;
		}
	}

	/* Close the connection to this server */
	PQfinish(my_local_conn);
	my_local_conn = NULL;

	/*
	 * determine which one is the best candidate to promote to primary
	 */
	for (i = 0; i < total_nodes; i++)
	{
		/* witness is never a good candidate */
		if (nodes[i].is_witness)
			continue;

		if (!nodes[i].is_ready || !nodes[i].is_visible)
			continue;

		if (!candidate_found)
		{
			/*
			 * start with the first ready node, and then move on to the next
			 * one
			 */
			best_candidate.node_id = nodes[i].node_id;
			best_candidate.xlog_location = nodes[i].xlog_location;
			best_candidate.is_ready = nodes[i].is_ready;
			best_candidate.is_witness = nodes[i].is_witness;
			candidate_found = true;
		}

		/*
		 * Nodes are retrieved ordered by priority, so if the current best
		 * candidate is lower than the next node's wal location then assign
		 * next node as the new best candidate.
		 */
		if (best_candidate.xlog_location < nodes[i].xlog_location)
		{
			best_candidate.node_id = nodes[i].node_id;
			best_candidate.xlog_location = nodes[i].xlog_location;
			best_candidate.is_ready = nodes[i].is_ready;
			best_candidate.is_witness = nodes[i].is_witness;
		}
	}

	/* Terminate if no candidate found */
	if (!candidate_found)
	{
		log_err(_("%s: No suitable candidate for promotion found; terminating.\n"),
				progname);
		terminate(ERR_FAILOVER_FAIL);
	}

	/* once we know who is the best candidate, promote it */
	if (best_candidate.node_id == local_options.node)
	{
		// ZZZ from loop above this should never happen anyway???
		// in fact do_failover is never called if rempgrd is running on a witness,
		// as it's only called by standby_monitor()

		if (best_candidate.is_witness)
		{
			log_err(_("%s: Node selected as new master is a witness. Can't be promoted.\n"),
					progname);
			terminate(ERR_FAILOVER_FAIL);
		}

		/* wait */
		sleep(5);

		if (verbose)
			log_info(_("%s: This node is the best candidate to be the new primary, promoting...\n"),
					 progname);
		log_debug(_("promote command is: \"%s\"\n"),
				  local_options.promote_command);

		if (log_type == REPMGR_STDERR && *local_options.logfile)
		{
			fflush(stderr);
		}

		r = system(local_options.promote_command);
		if (r != 0)
		{
			log_err(_("%s: promote command failed. You could check and try it manually.\n"),
					progname);
			terminate(ERR_BAD_CONFIG);
		}
	}
	else
	{
		/* wait */
		sleep(10);

		if (verbose)
			log_info(_("%s: Node %d is the best candidate to be the new primary, we should follow it...\n"),
					 progname, best_candidate.node_id);
		log_debug(_("follow command is: \"%s\"\n"), local_options.follow_command);

		/*
		 * New Primary need some time to be promoted. The follow command
		 * should take care of that.
		 */
		if (log_type == REPMGR_STDERR && *local_options.logfile)
		{
			fflush(stderr);
		}

		r = system(local_options.follow_command);
		if (r != 0)
		{
			log_err(_("%s: follow command failed. You could check and try it manually.\n"),
					progname);
			terminate(ERR_BAD_CONFIG);
		}
	}

	log_debug("failover done\n");
	/* to force it to re-calculate mode and master node */
	failover_done = true;

	/* and reconnect to the local database */
	my_local_conn = establish_db_connection(local_options.conninfo, true);
}


static bool
check_connection(PGconn *conn, const char *type)
{
	int			connection_retries;

	/*
	 * Check if the master is still available if after
	 * local_options.reconnect_attempts * local_options.reconnect_intvl
	 * seconds of retries we cannot reconnect return false
	 */
	for (connection_retries = 0; connection_retries < local_options.reconnect_attempts; connection_retries++)
	{
		if (!is_pgup(conn, local_options.master_response_timeout))
		{
			log_warning(_("%s: Connection to %s has been lost, trying to recover... %i seconds before failover decision\n"),
						progname,
						type,
						(local_options.reconnect_intvl * (local_options.reconnect_attempts - connection_retries)));
			/* wait local_options.reconnect_intvl seconds between retries */
			sleep(local_options.reconnect_intvl);
		}
		else
		{
			if (connection_retries > 0)
			{
				log_info(_("%s: Connection to %s has been restored.\n"),
						 progname, type);
			}
			return true;
		}
	}
	if (!is_pgup(conn, local_options.master_response_timeout))
	{
		log_err(_("%s: Unable to reconnect to master after %i seconds...\n"),
				progname,
				local_options.master_response_timeout
			);

		return false;
	}
	return true;
}


static void
check_cluster_configuration(PGconn *conn)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];

	log_info(_("%s Checking cluster configuration with schema '%s'\n"),
			 progname, get_repmgr_schema());
	sqlquery_snprintf(sqlquery,
					  "SELECT oid FROM pg_class "
					  " WHERE oid = '%s.repl_nodes'::regclass ",
					  get_repmgr_schema());
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s\n"), PQerrorMessage(conn));
		PQclear(res);
		terminate(ERR_DB_QUERY);
	}

	/*
	 * If there isn't any results then we have not configured a primary node
	 * yet in repmgr or the connection string is pointing to the wrong
	 * database.
	 *
	 * XXX if we are the primary, should we try to create the tables needed?
	 */
	if (PQntuples(res) == 0)
	{
		log_err(_("The replication cluster is not configured\n"));
		PQclear(res);
		terminate(ERR_BAD_CONFIG);
	}
	PQclear(res);
}


static void
check_node_configuration(void)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];

	/*
	 * Check if this node has an entry in `repl_nodes`
	 */
	log_info(_("%s Checking node %d in cluster '%s'\n"),
			 progname, local_options.node, local_options.cluster_name);

	sqlquery_snprintf(sqlquery,
					  "SELECT COUNT(*) "
					  "  FROM %s.repl_nodes "
					  " WHERE id = %d "
					  "   AND cluster = '%s' ",
					  get_repmgr_schema_quoted(my_local_conn),
					  local_options.node,
					  local_options.cluster_name);

	res = PQexec(my_local_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s\n"), PQerrorMessage(my_local_conn));
		PQclear(res);
		terminate(ERR_BAD_CONFIG);
	}

	/*
	 * If there isn't any results then we have not configured this node yet in
	 * repmgr, if that is the case we will insert the node to the cluster,
	 * except if it is a witness
	 */
	if (PQntuples(res) == 0)
	{
		PQclear(res);

		if (my_local_mode == WITNESS_MODE)
		{
			log_err(_("The witness is not configured\n"));
			terminate(ERR_BAD_CONFIG);
		}

		/* Adding the node */
		log_info(_("%s Adding node %d to cluster '%s'\n"),
				 progname, local_options.node, local_options.cluster_name);
		sqlquery_snprintf(sqlquery,
						  "INSERT INTO %s.repl_nodes"
						  "           (id, cluster, name, conninfo, priority, witness) "
						  "    VALUES (%d, '%s', '%s', '%s', 0, FALSE) ",
						  get_repmgr_schema_quoted(primary_conn),
						  local_options.node,
						  local_options.cluster_name,
						  local_options.node_name,
						  local_options.conninfo);

		if (!PQexec(primary_conn, sqlquery))
		{
			log_err(_("Cannot insert node details, %s\n"),
					PQerrorMessage(primary_conn));
			terminate(ERR_BAD_CONFIG);
		}
	}
	else
	{
		PQclear(res);
	}
}


/*
 * lsn_to_xlogrecptr()
 *
 * Convert an LSN represented as a string to an XLogRecPtr;
 * optionally set a flag to indicated the provided string
 * could not be parsed
 */
static XLogRecPtr
lsn_to_xlogrecptr(char *lsn, bool *format_ok)
{
	uint32 xlogid;
	uint32 xrecoff;

	if (sscanf(lsn, "%X/%X", &xlogid, &xrecoff) != 2)
	{
		if(format_ok != NULL)
			*format_ok = false;
		log_err(_("wrong log location format: %s\n"), lsn);
		return 0;
	}

	if(format_ok != NULL)
		*format_ok = true;

	return (((XLogRecPtr) xlogid * 16 * 1024 * 1024 * 255) + xrecoff);
}

void
usage(void)
{
	log_err(_("%s: Replicator manager daemon \n"), progname);
	log_err(_("Try \"%s --help\" for more information.\n"), progname);
}


void
help(const char *progname)
{
	printf(_("Usage: %s [OPTIONS]\n"), progname);
	printf(_("Replicator manager daemon for PostgreSQL.\n"));
	printf(_("\nOptions:\n"));
	printf(_("  --help                    show this help, then exit\n"));
	printf(_("  --version                 output version information, then exit\n"));
	printf(_("  -v, --verbose             output verbose activity information\n"));
	printf(_("  -m, --monitoring-history  track advance or lag of the replication in every standby in repl_monitor\n"));
	printf(_("  -f, --config-file=PATH    path to the configuration file\n"));
	printf(_("  -d, --daemonize           detach process from foreground\n"));
	printf(_("  -p, --pid-file=PATH       write a PID file\n"));
	printf(_("\n%s monitors a cluster of servers.\n"), progname);
}


#ifndef WIN32
static void
handle_sigint(SIGNAL_ARGS)
{
	terminate(0);
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
handle_sighup(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}

static void
setup_event_handlers(void)
{
	pqsignal(SIGHUP, handle_sighup);
	pqsignal(SIGINT, handle_sigint);
	pqsignal(SIGTERM, handle_sigint);
}
#endif

static void
terminate(int retval)
{
	close_connections();
	logger_shutdown();

	if (pid_file)
	{
		unlink(pid_file);
	}

	log_info("Terminating...\n");

	exit(retval);
}


static void
update_shared_memory(char *last_wal_standby_applied)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];

	sprintf(sqlquery,
			"SELECT %s.repmgr_update_standby_location('%s')",
			get_repmgr_schema_quoted(my_local_conn),
			last_wal_standby_applied);

	/* If an error happens, just inform about that and continue */
	res = PQexec(my_local_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_warning(_("Cannot update this standby's shared memory: %s\n"),
					PQerrorMessage(my_local_conn));
		/* XXX is this enough reason to terminate this repmgrd? */
	}
	else if (strcmp(PQgetvalue(res, 0, 0), "f") == 0)
	{
		/* this surely is more than enough reason to exit */
		log_crit(_("Cannot update this standby's shared memory, maybe shared_preload_libraries=repmgr_funcs is not set?\n"));
		exit(ERR_BAD_CONFIG);
	}

	PQclear(res);
}

static void
update_registration(void)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];

	sqlquery_snprintf(sqlquery,
					  "UPDATE %s.repl_nodes "
					  "   SET conninfo = '%s', "
					  "       priority = %d "
					  " WHERE id = %d ",
					  get_repmgr_schema_quoted(primary_conn),
					  local_options.conninfo,
					  local_options.priority,
					  local_options.node);

	res = PQexec(primary_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("Cannot update registration: %s\n"),
				PQerrorMessage(primary_conn));
		terminate(ERR_DB_CON);
	}
	PQclear(res);
}

static void
do_daemonize()
{
	char	   *ptr,
				path[MAXLEN];
	pid_t		pid = fork();
	int			ret;

	switch (pid)
	{
		case -1:
			log_err("Error in fork(): %s\n", strerror(errno));
			exit(ERR_SYS_FAILURE);
			break;

		case 0:			/* child process */
			pid = setsid();
			if (pid == (pid_t) -1)
			{
				log_err("Error in setsid(): %s\n", strerror(errno));
				exit(ERR_SYS_FAILURE);
			}

			/* ensure that we are no longer able to open a terminal */
			pid = fork();

			if (pid == -1)		/* error case */
			{
				log_err("Error in fork(): %s\n", strerror(errno));
				exit(ERR_SYS_FAILURE);
				break;
			}

			if (pid != 0)		/* parent process */
			{
				exit(0);
			}

			/* a child just flows along */

			memset(path, 0, MAXLEN);

			for (ptr = config_file + strlen(config_file); ptr > config_file; --ptr)
			{
				if (*ptr == '/')
				{
					strncpy(path, config_file, ptr - config_file);
				}
			}

			if (*path == '\0')
			{
				*path = '/';
			}

			ret = chdir(path);
			if (ret != 0)
			{
				log_err("Error changing directory to '%s': %s", path,
						strerror(errno));
			}

			break;

		default:				/* parent process */
			exit(0);
	}
}

static void
check_and_create_pid_file(const char *pid_file)
{
	struct stat st;
	FILE	   *fd;
	char		buff[MAXLEN];
	pid_t		pid;
	size_t		nread;

	if (stat(pid_file, &st) != -1)
	{
		memset(buff, 0, MAXLEN);

		fd = fopen(pid_file, "r");

		if (fd == NULL)
		{
			log_err("PID file %s exists but could not opened for reading. "
					"If repmgrd is no longer alive remove the file and restart repmgrd.\n",
					pid_file);
			exit(ERR_BAD_CONFIG);
		}

		nread = fread(buff, MAXLEN - 1, 1, fd);

		if (nread == 0 && ferror(fd))
		{
			log_err("Error reading PID file '%s', giving up...\n", pid_file);
			exit(ERR_BAD_CONFIG);
		}

		fclose(fd);

		pid = atoi(buff);

		if (pid != 0)
		{
			if (kill(pid, 0) != -1)
			{
				log_err("PID file %s exists and seems to contain a valid PID. "
						"If repmgrd is no longer alive remove the file and restart repmgrd.\n",
						pid_file);
				exit(ERR_BAD_CONFIG);
			}
		}
	}

	fd = fopen(pid_file, "w");
	if (fd == NULL)
	{
		log_err("Could not open PID file %s!\n", pid_file);
		exit(ERR_BAD_CONFIG);
	}

	fprintf(fd, "%d", getpid());
	fclose(fd);
}
