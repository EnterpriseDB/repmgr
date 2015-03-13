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
 *
 */

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
#include "pqexpbuffer.h"

/*
 * Struct to store node information
 */
typedef struct s_node_info
{
	int			node_id;
    int			upstream_node_id;
	char		conninfo_str[MAXLEN];
	XLogRecPtr	xlog_location;
    t_server_type type;
	bool		is_ready;
	bool		is_visible;
	char		slot_name[MAXLEN];
	bool		active;
}	t_node_info;



/* Local info */
t_configuration_options local_options;
PGconn	   *my_local_conn = NULL;

/* Primary info */
t_configuration_options primary_options;

PGconn	   *primary_conn = NULL;

const char *progname;

char	   *config_file = DEFAULT_CONFIG_FILE;
bool		verbose = false;
bool		monitoring_history = false;
t_node_info node_info;

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
static bool set_local_node_failed(void);

static bool update_node_record_set_primary(PGconn *conn, int this_node_id, int old_primary_node_id);
static bool update_node_record_set_upstream(PGconn *conn, int this_node_id, int new_upstream_node_id);

static void update_shared_memory(char *last_wal_standby_applied);
static void update_registration(void);
static void do_primary_failover(void);
static bool do_upstream_standby_failover(t_node_info upstream_node);

static t_node_info get_node_info(PGconn *conn, char *cluster, int node_id);
static t_server_type parse_node_type(const char *type);
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
	int			c;
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

	log_info(_("connecting to database '%s'\n"),
			 local_options.conninfo);
	my_local_conn = establish_db_connection(local_options.conninfo, true);

	/* Verify that server is a supported version */
	log_info(_("connected to database, checking its state\n"));
	server_version_num = get_server_version(my_local_conn, NULL);
	if(server_version_num < MIN_SUPPORTED_VERSION_NUM)
	{
		if (server_version_num > 0)
			log_err(_("%s requires PostgreSQL %s or later\n"),
					progname,
					MIN_SUPPORTED_VERSION
				);
		terminate(ERR_BAD_CONFIG);
	}

	/* Retrieve record for this node from the database */
	node_info = get_node_info(my_local_conn, local_options.cluster_name, local_options.node);

	log_debug("node id is %i, upstream is %i\n", node_info.node_id, node_info.upstream_node_id);

	/*
	 * MAIN LOOP This loops cycles at startup and once per failover and
	 * Requisites: - my_local_conn needs to be already setted with an active
	 * connection - no master connection
	 */
	do
	{
		/*
		 * Set my server mode, establish a connection to primary and start
		 * monitor
		 */

		switch (node_info.type)
		{
			case PRIMARY:
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

				log_info(_("starting continuous primary connection check\n"));

				/*
                                 * Check that primary is still alive.
                                 * XXX We should also check that the
                                 * standby servers are sending info
				 */

				/*
				 * Every local_options.monitor_interval_secs seconds, do
				 * master checks
				 */
				do
				{
					if (check_connection(primary_conn, "master"))
					{
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

			case WITNESS:
			case STANDBY:

				/* We need the node id of the primary server as well as a connection to it */
				log_info(_("connecting to master for cluster '%s'\n"),
						 local_options.cluster_name);

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
				if (node_info.type == WITNESS)
				{
					log_info(_("starting continuous witness node monitoring\n"));
				}
				else if (node_info.type == STANDBY)
				{
					log_info(_("starting continuous standby node monitoring\n"));
				}

				do
				{
					log_debug("standby check loop...\n");

					if (node_info.type == WITNESS)
					{
						witness_monitor();
					}
					else if (node_info.type == STANDBY)
					{
						standby_monitor();
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
				log_err(_("unrecognized mode for node %d\n"),
						local_options.node);
		}

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
	 * repmgrd, so we'll loop for a while before giving up.
	 */
	connection_ok = check_connection(primary_conn, "master");

	if(connection_ok == false)
	{
		int			connection_retries;
		log_debug(_("old primary node ID: %i\n"), primary_options.node);

		/* We need to wait a while for the new primary to be promoted */
		log_info(
			_("waiting %i seconds for a new master to be promoted...\n"),
			local_options.master_response_timeout
			);

		sleep(local_options.master_response_timeout);

		/* Attempt to find the new master */
		for (connection_retries = 0; connection_retries < local_options.reconnect_attempts; connection_retries++)
		{
			log_info(
				_("attempt %i of %i to determine new master...\n"),
				connection_retries + 1,
				local_options.reconnect_attempts
				);
			primary_conn = get_master_connection(my_local_conn,
												 local_options.cluster_name, &primary_options.node, NULL);

			if (PQstatus(primary_conn) != CONNECTION_OK)
			{
				log_warning(
					_("unable to determine a valid master server; waiting %i seconds to retry...\n"),
					local_options.reconnect_intvl
					);
				PQfinish(primary_conn);
				sleep(local_options.reconnect_intvl);
			}
			else
			{
				log_debug(_("new master found with node ID: %i\n"), primary_options.node);
				connection_ok = true;

				/*
				 * Update the repl_nodes table from the new primary to reflect the changed
				 * node configuration
				 *
				 * XXX it would be neat to be able to handle this with e.g. table-based
				 * logical replication
				 */
				copy_configuration(primary_conn, my_local_conn, local_options.cluster_name);

				break;
			}
		}

		if(connection_ok == false)
		{
			log_err(_("unable to determine a valid master server, exiting...\n"));
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
	if (PQsendQuery(primary_conn, sqlquery) == 0)
		log_warning(_("query could not be sent to master: %s\n"),
					PQerrorMessage(primary_conn));
}


// ZZZ update description
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

	PGconn	   *upstream_conn;
	int			upstream_node_id;
	t_node_info upstream_node;

	int			active_primary_id;
	const char *type = NULL;

	/*
	 * Verify that the local node is still available - if not there's
	 * no point in doing much else anyway
	 */

	if (!check_connection(my_local_conn, "standby"))
	{
		set_local_node_failed();
		log_err(_("failed to connect to local node, terminating!\n"));
		terminate(1);
	}

	upstream_conn = get_upstream_connection(my_local_conn,
											local_options.cluster_name,
											local_options.node,
											&upstream_node_id, NULL);

	type = upstream_node_id == primary_options.node
		? "master"
		: "upstream";

	// ZZZ "5 minutes"?
	/*
	 * Check if the upstream node is still available, if after 5 minutes of retries
	 * we cannot reconnect, try to get a new upstream node.
	 */

	check_connection(upstream_conn, type);	/* this takes up to
											 * local_options.reconnect_attempts
											 * local_options.reconnect_intvl seconds
											 */


	if (PQstatus(upstream_conn) != CONNECTION_OK)
	{
		PQfinish(upstream_conn);
		upstream_conn = NULL;

		if (local_options.failover == MANUAL_FAILOVER)
		{
			log_err(_("Unable to reconnect to %s. Now checking if another node has been promoted.\n"), type);

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
					log_err(_("connected to node %d, continuing monitoring.\n"),
							primary_options.node);
					break;
				}
				else
				{
					log_err(
					    _("no new master found, waiting %i seconds before retry...\n"),
					    local_options.retry_promote_interval_secs
					    );

					sleep(local_options.retry_promote_interval_secs);
				}
			}

			if (PQstatus(primary_conn) != CONNECTION_OK)
			{
				log_err(_("Unable to reconnet to master after %i attempts, terminating...\n"),
						local_options.reconnect_attempts);
				terminate(ERR_DB_CON);
			}
		}
		else if (local_options.failover == AUTOMATIC_FAILOVER)
		{
			/*
			 * When we returns from this function we will have a new primary
			 * and a new primary_conn
			 */

			/*
			 * Failover handling is handled differently depending on whether
			 * the failed node is the primary or a cascading standby
			 */
			upstream_node = get_node_info(my_local_conn, local_options.cluster_name, node_info.upstream_node_id);

            if(upstream_node.type == PRIMARY)
            {
                log_debug(_("failure detected on master node (%i); attempting to promote a standby\n"),
                          node_info.upstream_node_id);
                do_primary_failover();
            }
            else
            {
                log_debug(_("failure detected on upstream node %i; attempting to reconnect to new upstream node\n"),
                          node_info.upstream_node_id);

				if(!do_upstream_standby_failover(upstream_node))
				{
					log_err(_("unable to reconnect to new upstream node, terminating...\n"));
					terminate(1);
				}
            }
			return;
		}
	}

	PQfinish(upstream_conn);

	/* Check if we still are a standby, we could have been promoted */
	do
	{
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
				log_err(_("standby node has disappeared, trying to reconnect...\n"));
				did_retry = true;

				if (!check_connection(my_local_conn, "standby"))
				{
					set_local_node_failed();
					terminate(0);
				}

				break;
		}
	} while (ret == -1);

	if (did_retry)
	{
		log_info(_("standby connection recovered!\n"));
	}

	/* Fast path for the case where no history is requested */
	if (!monitoring_history)
		return;


	/*
	 * If original primary has gone away we'll need to get the new one
	 * from the upstream node to write monitoring information
	 */

	upstream_node = get_node_info(my_local_conn, local_options.cluster_name, node_info.upstream_node_id);

	sprintf(sqlquery,
			"SELECT id "
			"  FROM %s.repl_nodes "
			" WHERE type = 'master' "
			"   AND active IS TRUE ",
			get_repmgr_schema_quoted(my_local_conn));

	res = PQexec(my_local_conn, sqlquery);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("standby_monitor() - query error:%s\n"), PQerrorMessage(my_local_conn));
		PQclear(res);

		/* Not a fatal error, just means no monitoring records will be written */
		return;
	}

	if(PQntuples(res) == 0)
	{
		log_err(_("standby_monitor(): no active master found\n"));
		PQclear(res);
		return;
	}

	active_primary_id = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);

	if(active_primary_id != primary_options.node)
	{
		log_notice(_("connecting to active master (node %i)...\n"), active_primary_id); \
		if(primary_conn != NULL)
		{
			PQfinish(primary_conn);
		}
		primary_conn = get_master_connection(my_local_conn,
											 local_options.cluster_name,
											 &primary_options.node, NULL);

	}

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
		log_warning(_("query could not be sent to primary. %s\n"),
					PQerrorMessage(primary_conn));
}


/*
 * do_primary_failover()
 *
 * Handles failover to new cluster primary
 */

static void
do_primary_failover(void)
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

    /* Store details of the failed node here */
    t_node_info failed_primary = {-1, NO_UPSTREAM_NODE, "", InvalidXLogRecPtr, UNKNOWN, false, false};

	/* initialize to keep compiler quiet */
	t_node_info best_candidate = {-1, NO_UPSTREAM_NODE, "", InvalidXLogRecPtr, UNKNOWN, false, false};

	/* get a list of standby nodes, including myself */
	sprintf(sqlquery,
			"SELECT id, conninfo, type, upstream_node_id "
			"  FROM %s.repl_nodes "
			" WHERE cluster = '%s' "
            "   AND active IS TRUE "
			" ORDER BY priority, id "
			" LIMIT %i ",
			get_repmgr_schema_quoted(my_local_conn),
			local_options.cluster_name,
			FAILOVER_NODES_MAX_CHECK);

	res = PQexec(my_local_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("unable to retrieve node records: %s\n"), PQerrorMessage(my_local_conn));
		PQclear(res);
		PQfinish(my_local_conn);
		terminate(ERR_DB_QUERY);
	}

	/*
	 * total nodes that are registered
	 */
	total_nodes = PQntuples(res);
	log_debug(_("there are %d nodes registered\n"), total_nodes);

	/*
	 * Build an array with the nodes and indicate which ones are visible and
	 * ready
	 */
	for (i = 0; i < total_nodes; i++)
	{
		nodes[i].node_id = atoi(PQgetvalue(res, i, 0));

		strncpy(nodes[i].conninfo_str, PQgetvalue(res, i, 1), MAXCONNINFO);

		nodes[i].type = parse_node_type(PQgetvalue(res, i, 2));

		if(nodes[i].type == PRIMARY)
		{
			failed_primary.node_id = nodes[i].node_id;
			failed_primary.xlog_location = nodes[i].xlog_location;
			failed_primary.is_ready = nodes[i].is_ready;
		}

		nodes[i].upstream_node_id = atoi(PQgetvalue(res, i, 3));

		/*
		 * Initialize on false so if we can't reach this node we know that
		 * later
		 */
		nodes[i].is_visible = false;
		nodes[i].is_ready = false;

		nodes[i].xlog_location = InvalidXLogRecPtr;

		log_debug(_("node=%d conninfo=\"%s\" type=%s\n"),
				  nodes[i].node_id, nodes[i].conninfo_str,
				  PQgetvalue(res, i, 2));

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

	log_debug(_("total nodes counted: registered=%d, visible=%d\n"),
			  total_nodes, visible_nodes);

	/*
	 * am i on the group that should keep alive? if i see less than half of
	 * total_nodes then i should do nothing
	 */
	if (visible_nodes < (total_nodes / 2.0))
	{
		log_err(_("Unable to reach most of the nodes.\n"
				  "Let the other standby servers decide which one will be the primary.\n"
				  "Manual action will be needed to re-add this node to the cluster.\n"));
		terminate(ERR_FAILOVER_FAIL);
	}

	/* Query all available nodes to determine readiness and LSN */
	for (i = 0; i < total_nodes; i++)
	{
		log_debug("checking node %i...\n", nodes[i].node_id);

		/* if the node is not visible, skip it */
		if (!nodes[i].is_visible)
			continue;

		/* if the node is a witness node, skip it */
		if (nodes[i].type == WITNESS)
			continue;

		/* if node does not have same upstream node, skip it */
		if (nodes[i].upstream_node_id != node_info.upstream_node_id)
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
			log_info(_("unable to retrieve node's last standby location: %s\n"),
					 PQerrorMessage(node_conn));
			log_debug(_("connection details: %s\n"), nodes[i].conninfo_str);
			PQclear(res);
			PQfinish(node_conn);
			terminate(ERR_FAILOVER_FAIL);
		}

		xlog_recptr = lsn_to_xlogrecptr(PQgetvalue(res, 0, 0), &lsn_format_ok);

		log_debug(_("LSN of node %i is: %s\n"), nodes[i].node_id, PQgetvalue(res, 0, 0));

		PQclear(res);
		PQfinish(node_conn);

		/* If position is 0/0, error */
		if(xlog_recptr == InvalidXLogRecPtr)
		{
			log_err(_("InvalidXLogRecPtr detected on standby node %i\n"), nodes[i].node_id);
			terminate(ERR_FAILOVER_FAIL);
		}

		nodes[i].xlog_location = xlog_recptr;
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
		/*
		 * ensure witness server is marked as ready, and skip
		 * LSN check
		 */
		if (nodes[i].type == WITNESS)
		{
			if (!nodes[i].is_ready)
			{
				nodes[i].is_ready = true;
				ready_nodes++;
			}
			continue;
		}

		/* if the node is not visible, skip it */
		if (!nodes[i].is_visible)
			continue;

		/* if node does not have same upstream node, skip it */
		if (nodes[i].upstream_node_id != node_info.upstream_node_id)
			continue;

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
			continue;
		}

		while (!nodes[i].is_ready)
		{

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
			 * empty string; otherwise position is 0/0 and we need to continue
			 * looping until a valid LSN is reported
			 */
			if(xlog_recptr == InvalidXLogRecPtr)
			{
				if(lsn_format_ok == false)
				{
					/* Unable to parse value returned by `repmgr_get_last_standby_location()` */
					if(*PQgetvalue(res, 0, 0) == '\0')
					{
						log_crit(
							_("unable to obtain LSN from node %i"), nodes[i].node_id
							);
						log_info(
							_("please check that 'shared_preload_libraries=repmgr_funcs' is set in postgresql.conf\n")
							);

						PQclear(res);
						PQfinish(node_conn);
						exit(ERR_BAD_CONFIG);
					}

					/*
					 * Very unlikely to happen; in the absence of any better
					 * strategy keep checking
					 */
					log_warning(_("unable to parse LSN \"%s\"\n"),
								PQgetvalue(res, 0, 0));
				}
				else
				{
					log_debug(
						_("invalid LSN returned from node %i: '%s'\n"),
						nodes[i].node_id,
						PQgetvalue(res, 0, 0)
						);
				}

				PQclear(res);

				/* If position is 0/0, keep checking */
				continue;
			}

			if (nodes[i].xlog_location < xlog_recptr)
			{
				nodes[i].xlog_location = xlog_recptr;
			}

			log_debug(_("LSN of node %i is: %s\n"), nodes[i].node_id, PQgetvalue(res, 0, 0));
			PQclear(res);

			ready_nodes++;
			nodes[i].is_ready = true;
		}

		PQfinish(node_conn);
	}

	/* Close the connection to this server */
	PQfinish(my_local_conn);
	my_local_conn = NULL;

	/*
	 * determine which one is the best candidate to promote to primary
	 */
	for (i = 0; i < total_nodes; i++)
	{
		/* witness server can never be a candidate */
		if (nodes[i].type == WITNESS)
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
			strncpy(best_candidate.conninfo_str, nodes[i].conninfo_str, MAXCONNINFO);
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
			strncpy(best_candidate.conninfo_str, nodes[i].conninfo_str, MAXCONNINFO);
		}
	}

	/* Terminate if no candidate found */
	if (!candidate_found)
	{
		log_err(_("no suitable candidate for promotion found; terminating.\n"));
		terminate(ERR_FAILOVER_FAIL);
	}

	/* if local node is the best candidate, promote it */
	if (best_candidate.node_id == local_options.node)
	{
		/* wait */
		sleep(5);

		if (verbose)
			log_info(_("this node is the best candidate to be the new master, promoting...\n"));

		log_debug(_("promote command is: \"%s\"\n"),
				  local_options.promote_command);

		if (log_type == REPMGR_STDERR && *local_options.logfile)
		{
			fflush(stderr);
		}

		r = system(local_options.promote_command);
		if (r != 0)
		{
			log_err(_("promote command failed. You could check and try it manually.\n"));

			terminate(ERR_DB_QUERY);
		}

		/* and reconnect to the local database */
		my_local_conn = establish_db_connection(local_options.conninfo, true);

		/* update node information to reflect new status */
		if(update_node_record_set_primary(my_local_conn, node_info.node_id, failed_primary.node_id) == false)
		{
			terminate(ERR_DB_QUERY);
		}

		/* update internal record for this node*/
		node_info = get_node_info(my_local_conn, local_options.cluster_name, local_options.node);
	}
    /* local node not promotion candidate - find the new primary */
	else
	{
		PGconn	   *new_primary_conn;

		/* wait */
		sleep(10);

		if (verbose)
			log_info(_("node %d is the best candidate to be the new primary, we should follow it...\n"),
					 best_candidate.node_id);
		log_debug(_("follow command is: \"%s\"\n"), local_options.follow_command);

		/*
		 * The new primary may some time to be promoted. The follow command
		 * should take care of that.
		 */
		if (log_type == REPMGR_STDERR && *local_options.logfile)
		{
			fflush(stderr);
		}

		/*
		 * If 9.4 or later, and replication slots in use, we'll need to create a
		 * slot on the new primary
		 */
		new_primary_conn = establish_db_connection(best_candidate.conninfo_str, true);

		if(local_options.use_replication_slots)
		{
			if(create_replication_slot(new_primary_conn, node_info.slot_name) == false)
			{
				log_err(("Unable to create slot '%s' on the primary node: %s\n"),
						node_info.slot_name,
						PQerrorMessage(new_primary_conn)
					);

				PQfinish(new_primary_conn);
				terminate(ERR_DB_QUERY);
			}
		}

		r = system(local_options.follow_command);
		if (r != 0)
		{
			log_err(_("follow command failed. You could check and try it manually.\n"));
			terminate(ERR_BAD_CONFIG);
		}

		/* and reconnect to the local database */
		my_local_conn = establish_db_connection(local_options.conninfo, true);

		/* update node information to reflect new status */
		if(update_node_record_set_upstream(new_primary_conn, node_info.node_id, best_candidate.node_id) == false)
		{
			terminate(ERR_BAD_CONFIG);
		}

		/* update internal record for this node*/
		node_info = get_node_info(new_primary_conn, local_options.cluster_name, local_options.node);

		PQfinish(new_primary_conn);
	}

	/* to force it to re-calculate mode and master node */
	// ^ ZZZ check that behaviour ^
	failover_done = true;
}


/*
 * do_upstream_standby_failover()
 *
 * Attach cascaded standby to new upstream server
 *
 * Currently we will try to attach to the failed upstream's upstream.
 * It might be worth providing a selection of reconnection strategies
 * as different behaviour might be desirable in different situations;
 * or maybe the option not to reconnect might be required?
 */
static bool
do_upstream_standby_failover(t_node_info upstream_node)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];
	int			upstream_node_id = node_info.upstream_node_id;
	int			r;

	log_debug(_("do_upstream_standby_failover(): performing failover for node %i\n"),
              node_info.node_id);

	/*
	 * Verify that we can still talk to the cluster primary even though
	 * node upstream is not available
	 */
	if (!check_connection(primary_conn, "master"))
	{
		log_err(_("do_upstream_standby_failover(): Unable to connect to last known master node\n"));
		return false;
	}

	while(1)
	{
		sqlquery_snprintf(sqlquery,
						  "SELECT id, active, upstream_node_id, type, conninfo "
						  "  FROM %s.repl_nodes "
						  " WHERE id = %i ",
						  get_repmgr_schema_quoted(primary_conn),
						  upstream_node_id);

		res = PQexec(primary_conn, sqlquery);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			log_err(_("unable to query cluster master: %s\n"), PQerrorMessage(primary_conn));
			PQclear(res);
			return false;
		}

		if(PQntuples(res) == 0)
		{
			log_err(_("no node with id %i found"), upstream_node_id);
			PQclear(res);
			return false;
		}

		/* upstream node is inactive */
		if(strcmp(PQgetvalue(res, 0, 1), "f") == 0)
		{
			/*
			 * Upstream node is an inactive primary, meaning no there are no direct
			 * upstream nodes available to reattach to.
			 *
			 * XXX For now we'll simply terminate, however it would make sense to
			 * provide an option to either try and find the current primary and/or
			 * a strategy to connect to a different upstream node
			 */
			if(strcmp(PQgetvalue(res, 0, 4), "master") == 0)
			{
				log_err(_("unable to find active master node\n"));
				PQclear(res);
				return false;
			}

			upstream_node_id = atoi(PQgetvalue(res, 0, 2));
		}
		else
		{
			upstream_node_id = atoi(PQgetvalue(res, 0, 0));

			log_notice(_("found active upstream node with id %i\n"), upstream_node_id);
			PQclear(res);
			break;
		}

		PQclear(res);
		sleep(local_options.reconnect_intvl);
	}

	/* Close the connection to this server */
	PQfinish(my_local_conn);
	my_local_conn = NULL;

	/* Follow new upstream */
	r = system(local_options.follow_command);
	if (r != 0)
	{
		log_err(_("follow command failed. You could check and try it manually.\n"));
		terminate(ERR_BAD_CONFIG);
	}

	if(update_node_record_set_upstream(primary_conn, node_info.node_id, upstream_node_id) == false)
	{
		terminate(ERR_BAD_CONFIG);
	}

	my_local_conn = establish_db_connection(local_options.conninfo, true);

	return true;
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
			log_warning(_("connection to %s has been lost, trying to recover... %i seconds before failover decision\n"),
						type,
						(local_options.reconnect_intvl * (local_options.reconnect_attempts - connection_retries)));
			/* wait local_options.reconnect_intvl seconds between retries */
			sleep(local_options.reconnect_intvl);
		}
		else
		{
			if (connection_retries > 0)
			{
				log_info(_("connection to %s has been restored.\n"), type);
			}
			return true;
		}
	}

	if (!is_pgup(conn, local_options.master_response_timeout))
	{
		log_err(_("unable to reconnect to %s after %i seconds...\n"),
				type,
				local_options.master_response_timeout
			);

		return false;
	}

	return true;
}


/*
 * set_local_node_failed()
 *
 * If failure of the local node is detected, attempt to connect
 * to the current primary server (as stored in the global variable
 * `primary_conn`) and update its record to failed.
 */

static bool
set_local_node_failed(void)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];
	int			active_primary_node_id = -1;
	char		primary_conninfo[MAXLEN];

	if (!check_connection(primary_conn, "master"))
	{
		log_err(_("set_local_node_failed(): Unable to connect to last known primary node\n"));
		return false;
	}

	/*
	 * Check that the node `primary_conn` is connected to is node is still
	 * primary - it's just about conceivable that it might have become a
	 * standby of a new primary in the intervening period
	 */

	sqlquery_snprintf(sqlquery,
					  "SELECT id, conninfo "
					  "  FROM %s.repl_nodes "
					  " WHERE type = 'master' "
					  "   AND active IS TRUE ",
					  get_repmgr_schema_quoted(primary_conn));

	res = PQexec(primary_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("unable to obtain record for active master: %s\n"),
				PQerrorMessage(primary_conn));

		return false;
	}

	if(!PQntuples(res))
	{
		log_err(_("no active master record found\n"));
		return false;
	}

	active_primary_node_id = atoi(PQgetvalue(res, 0, 0));
	strncpy(primary_conninfo, PQgetvalue(res, 0, 1), MAXLEN);
	PQclear(res);

	if(active_primary_node_id != primary_options.node)
	{
		log_notice(_("current active master is %i; attempting to connect\n"),
			active_primary_node_id);
		PQfinish(primary_conn);
		primary_conn = establish_db_connection(primary_conninfo, false);

		if(PQstatus(primary_conn) != CONNECTION_OK)
		{
			log_err(_("unable to connect to active master\n"));
			return false;
		}

		log_notice(_("Connection to new master was successful\n"));
	}


	/*
	 * Attempt to set own record as inactive
	 */
	sqlquery_snprintf(sqlquery,
					  "UPDATE %s.repl_nodes "
					  "   SET active = FALSE "
					  " WHERE id = %i ",
					  get_repmgr_schema_quoted(primary_conn),
					  node_info.node_id);

	res = PQexec(primary_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("unable to set local node %i as inactive on primary: %s\n"),
				node_info.node_id,
				PQerrorMessage(primary_conn));

		return false;
	}

	log_notice(_("marking this node (%i) as inactive on primary\n"), node_info.node_id);
	return true;
}


static void
check_cluster_configuration(PGconn *conn)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];

	log_info(_("checking cluster configuration with schema '%s'\n"), get_repmgr_schema());

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
		log_err(_("the replication cluster is not configured\n"));
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
	log_info(_("checking node %d in cluster '%s'\n"),
			 local_options.node, local_options.cluster_name);

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

		if (node_info.type == WITNESS)
		{
			log_err(_("The witness is not configured\n"));
			terminate(ERR_BAD_CONFIG);
		}

		/* Adding the node */
		log_info(_("adding node %d to cluster '%s'\n"),
				 local_options.node, local_options.cluster_name);
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
			log_err(_("unable to insert node details, %s\n"),
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
		log_err(_("incorrect log location format: %s\n"), lsn);
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


t_node_info
get_node_info(PGconn *conn, char *cluster, int node_id)
{
	char		sqlquery[QUERY_STR_LEN];
	PGresult   *res;

	t_node_info node_info = {-1, NO_UPSTREAM_NODE, "", InvalidXLogRecPtr, UNKNOWN, false, false};

	sprintf(sqlquery,
			"SELECT id, upstream_node_id, conninfo, type, slot_name, active "
			"  FROM %s.repl_nodes "
			" WHERE cluster = '%s' "
			"   AND id = %i",
			get_repmgr_schema_quoted(conn),
			local_options.cluster_name,
			node_id);

	log_debug("get_node_info(): %s\n", sqlquery);

	res = PQexec(my_local_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Unable to retrieve record for node %i: %s\n"), node_id, PQerrorMessage(conn));
		PQclear(res);
		terminate(ERR_DB_QUERY);
	}

	if (!PQntuples(res)) {
		log_warning(_("No record found record for node %i\n"), node_id);
		PQclear(res);
		node_info.node_id = -1;
		return node_info;
	}

	node_info.node_id = atoi(PQgetvalue(res, 0, 0));
	node_info.upstream_node_id = atoi(PQgetvalue(res, 0, 1));
	strncpy(node_info.conninfo_str, PQgetvalue(res, 0, 2), MAXLEN);
	node_info.type = parse_node_type(PQgetvalue(res, 0, 3));
	strncpy(node_info.slot_name, PQgetvalue(res, 0, 4), MAXLEN);
	node_info.active = (strcmp(PQgetvalue(res, 0, 5), "t") == 0)
		? true
		: false;

	PQclear(res);

	return node_info;
}


static t_server_type
parse_node_type(const char *type)
{
	if(strcmp(type, "master") == 0)
	{
		return PRIMARY;
	}
	else if(strcmp(type, "standby") == 0)
	{
		return STANDBY;
	}
	else if(strcmp(type, "witness") == 0)
	{
		return WITNESS;
	}

	return UNKNOWN;
}


static bool
update_node_record_set_primary(PGconn *conn, int this_node_id, int old_primary_node_id)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];

	log_debug(_("Setting failed node %i inactive; marking node %i as primary\n"), old_primary_node_id, this_node_id);

	res = PQexec(conn, "BEGIN");

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("Unable to begin transaction: %s\n"),
				PQerrorMessage(conn));

		PQclear(res);
		return false;
	}

	PQclear(res);

	sqlquery_snprintf(sqlquery,
					  "  UPDATE %s.repl_nodes "
					  "     SET active = FALSE "
					  "   WHERE cluster = '%s' "
					  "     AND id = %i ",
					  get_repmgr_schema_quoted(conn),
					  local_options.cluster_name,
					  old_primary_node_id);

	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("Unable to set old primary node %i as inactive: %s\n"),
				old_primary_node_id,
				PQerrorMessage(conn));
		PQclear(res);

		PQexec(conn, "ROLLBACK");
		return false;
	}

	PQclear(res);

	sqlquery_snprintf(sqlquery,
					  "  UPDATE %s.repl_nodes "
					  "     SET type = 'master', "
					  "         upstream_node_id = NULL "
					  "   WHERE cluster = '%s' "
					  "     AND id = %i ",
					  get_repmgr_schema_quoted(conn),
					  local_options.cluster_name,
					  this_node_id);

	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("Unable to set current node %i as active primary: %s\n"),
				this_node_id,
				PQerrorMessage(conn));
		PQclear(res);

		PQexec(conn, "ROLLBACK");
		return false;
	}

	PQclear(res);

	res = PQexec(conn, "COMMIT");

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("Unable to set commit transaction: %s\n"),
				PQerrorMessage(conn));
		PQclear(res);

		return false;
	}

	PQclear(res);

	return true;
}


static bool
update_node_record_set_upstream(PGconn *conn, int this_node_id, int new_upstream_node_id)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];

	log_debug(_("update_node_record_set_upstream(): Updating node %i's upstream node to %i\n"), this_node_id, new_upstream_node_id);

	sqlquery_snprintf(sqlquery,
					  "  UPDATE %s.repl_nodes "
					  "     SET upstream_node_id = %i "
					  "   WHERE cluster = '%s' "
					  "     AND id = %i ",
					  get_repmgr_schema_quoted(conn),
					  new_upstream_node_id,
					  local_options.cluster_name,
					  this_node_id);
	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("Unable to set new upstream node id: %s\n"),
				PQerrorMessage(conn));
		PQclear(res);

		return false;
	}

	PQclear(res);

	return true;
}
