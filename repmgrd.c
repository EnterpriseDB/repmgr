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

/* Master info */
t_configuration_options master_options;

PGconn	   *master_conn = NULL;

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

static bool update_node_record_set_upstream(PGconn *conn, int this_node_id, int new_upstream_node_id);

static void update_shared_memory(char *last_wal_standby_applied);
static void update_registration(void);
static void do_master_failover(void);
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
	if (master_conn != NULL && PQisBusy(master_conn) == 1)
		cancel_query(master_conn, local_options.master_response_timeout);

	if (my_local_conn != NULL)
		PQfinish(my_local_conn);

	if (master_conn != NULL && master_conn != my_local_conn)
		PQfinish(master_conn);

	master_conn = NULL;
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
	bool        startup_event_logged = false;

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
	 * Parse the configuration file, if provided. If no configuration file
	 * was provided, or one was but was incomplete, parse_config() will
	 * abort anyway, with an appropriate message.
	 *
	 * XXX it might be desirable to create an event record for this, in
	 * which case we'll need to refactor parse_config() not to abort,
	 * and return the error message.
	 */
	parse_config(config_file, &local_options);

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
		{
			log_err(_("%s requires PostgreSQL %s or later\n"),
					progname,
					MIN_SUPPORTED_VERSION) ;
		}
		else
		{
			log_err(_("unable to determine PostgreSQL server version\n"));
		}

		terminate(ERR_BAD_CONFIG);
	}

	/* Retrieve record for this node from the local database */
	node_info = get_node_info(my_local_conn, local_options.cluster_name, local_options.node);

	/* No node record found - exit gracefully */
	if(node_info.node_id == NODE_NOT_FOUND)
	{
		log_err(_("No metadata record found for this node - terminating\n"));
		log_notice(_("HINT: was this node registered with 'repmgr (master|standby) register'?\n"));
		terminate(ERR_BAD_CONFIG);
	}

	log_debug("node id is %i, upstream is %i\n", node_info.node_id, node_info.upstream_node_id);

	/*
	 * MAIN LOOP This loops cycles at startup and once per failover and
	 * Requisites: - my_local_conn needs to be already setted with an active
	 * connection - no master connection
	 */
	do
	{
		/*
		 * Set my server mode, establish a connection to master and start
		 * monitor
		 */

		switch (node_info.type)
		{
			case MASTER:
				master_options.node = local_options.node;
				strncpy(master_options.conninfo, local_options.conninfo,
						MAXLEN);
				master_conn = my_local_conn;

				check_cluster_configuration(my_local_conn);
				check_node_configuration();

				if (reload_config(config_file, &local_options))
				{
					PQfinish(my_local_conn);
					my_local_conn = establish_db_connection(local_options.conninfo, true);
					master_conn = my_local_conn;
					update_registration();
				}

				/* Log startup event */
				if(startup_event_logged == false)
				{
					create_event_record(master_conn,
										&local_options,
										local_options.node,
										"repmgrd_start",
										true,
										NULL);
					startup_event_logged = true;
				}

				log_info(_("starting continuous master connection check\n"));

				/*
				 * Check that master is still alive.
				 * XXX We should also check that the
				 * standby servers are sending info
				 */

				/*
				 * Every local_options.monitor_interval_secs seconds, do
				 * master checks
				 */
				do
				{
					if (check_connection(master_conn, "master"))
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
							master_conn = my_local_conn;

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

				/* We need the node id of the master server as well as a connection to it */
				log_info(_("connecting to master node '%s'\n"),
						 local_options.cluster_name);

				master_conn = get_master_connection(my_local_conn,
													 local_options.cluster_name,
													 &master_options.node, NULL);

				if (master_conn == NULL)
				{
					PQExpBufferData errmsg;
					initPQExpBuffer(&errmsg);

					appendPQExpBuffer(&errmsg,
									  _("unable to connect to master node '%s'"),
									  local_options.cluster_name);

					log_err("%s\n", errmsg.data);

					create_event_record(NULL,
										&local_options,
										local_options.node,
										"repmgrd_shutdown",
										false,
										errmsg.data);

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
				/* Log startup event */
				if(startup_event_logged == false)
				{
					create_event_record(master_conn,
										&local_options,
										local_options.node,
										"repmgrd_start",
										true,
										NULL);
					startup_event_logged = true;
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
 * Monitors witness server; attempt to find and connect to new master
 * if existing master connection is lost
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
	connection_ok = check_connection(master_conn, "master");

	if(connection_ok == false)
	{
		int			connection_retries;
		log_debug(_("old master node ID: %i\n"), master_options.node);

		/* We need to wait a while for the new master to be promoted */
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
			master_conn = get_master_connection(my_local_conn,
												 local_options.cluster_name, &master_options.node, NULL);

			if (PQstatus(master_conn) != CONNECTION_OK)
			{
				log_warning(
					_("unable to determine a valid master server; waiting %i seconds to retry...\n"),
					local_options.reconnect_intvl
					);
				PQfinish(master_conn);
				sleep(local_options.reconnect_intvl);
			}
			else
			{
				log_debug(_("new master found with node ID: %i\n"), master_options.node);
				connection_ok = true;

				/*
				 * Update the repl_nodes table from the new master to reflect the changed
				 * node configuration
				 *
				 * XXX it would be neat to be able to handle this with e.g. table-based
				 * logical replication
				 */
				copy_configuration(master_conn, my_local_conn, local_options.cluster_name);

				break;
			}
		}

		if(connection_ok == false)
		{
			PQExpBufferData errmsg;
			initPQExpBuffer(&errmsg);

			appendPQExpBuffer(&errmsg,
							  _("unable to determine a valid master node, terminating..."));

			log_err("%s\n", errmsg.data);

			create_event_record(NULL,
								&local_options,
								local_options.node,
								"repmgrd_shutdown",
								false,
								errmsg.data);

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
	if (!cancel_query(master_conn, local_options.master_response_timeout))
		return;
	if (wait_connection_availability(master_conn,
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
	 * Build the SQL to execute on master
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
					  master_options.node,
					  local_options.node,
					  monitor_witness_timestamp);

	/*
	 * Execute the query asynchronously, but don't check for a result. We will
	 * check the result next time we pause for a monitor step.
	 */
	if (PQsendQuery(master_conn, sqlquery) == 0)
		log_warning(_("query could not be sent to master: %s\n"),
					PQerrorMessage(master_conn));
}


/*
 * standby_monitor()
 *
 * Monitor standby server and handle failover situation. Also insert
 * monitoring information if configured.
 */
static void
standby_monitor(void)
{
	PGresult   *res;
	char		monitor_standby_timestamp[MAXLEN];
	char		last_wal_master_location[MAXLEN];
	char		last_wal_standby_received[MAXLEN];
	char		last_wal_standby_applied[MAXLEN];
	char		last_wal_standby_applied_timestamp[MAXLEN];
	char		sqlquery[QUERY_STR_LEN];

	XLogRecPtr	lsn_master;
	XLogRecPtr	lsn_standby_received;
	XLogRecPtr	lsn_standby_applied;

	int			connection_retries,
				ret;
	bool		did_retry = false;

	PGconn	   *upstream_conn;
	int			upstream_node_id;
	t_node_info upstream_node;

	int			active_master_id;
	const char *type = NULL;

	/*
	 * Verify that the local node is still available - if not there's
	 * no point in doing much else anyway
	 */

	if (!check_connection(my_local_conn, "standby"))
	{
		PQExpBufferData errmsg;

		set_local_node_failed();

		initPQExpBuffer(&errmsg);

		appendPQExpBuffer(&errmsg,
						  _("failed to connect to local node, node marked as failed and terminating!"));

		log_err("%s\n", errmsg.data);

		create_event_record(master_conn,
							&local_options,
							local_options.node,
							"repmgrd_shutdown",
							false,
							errmsg.data);

		terminate(ERR_DB_CON);
	}

	upstream_conn = get_upstream_connection(my_local_conn,
											local_options.cluster_name,
											local_options.node,
											&upstream_node_id, NULL);

	type = upstream_node_id == master_options.node
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
				master_conn = get_master_connection(my_local_conn,
					local_options.cluster_name, &master_options.node, NULL);
				if (PQstatus(master_conn) == CONNECTION_OK)
				{
					/*
					 * Connected, we can continue the process so break the
					 * loop
					 */
					log_err(_("connected to node %d, continuing monitoring.\n"),
							master_options.node);
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

			if (PQstatus(master_conn) != CONNECTION_OK)
			{
				PQExpBufferData errmsg;
				initPQExpBuffer(&errmsg);

				appendPQExpBuffer(&errmsg,
								  _("Unable to reconnect to master after %i attempts, terminating..."),
								  local_options.reconnect_attempts);

				log_err("%s\n", errmsg.data);

				create_event_record(NULL,
									&local_options,
									local_options.node,
									"repmgrd_shutdown",
									false,
									errmsg.data);

				terminate(ERR_DB_CON);
			}
		}
		else if (local_options.failover == AUTOMATIC_FAILOVER)
		{
			/*
			 * When we returns from this function we will have a new master
			 * and a new master_conn
			 */

			/*
			 * Failover handling is handled differently depending on whether
			 * the failed node is the master or a cascading standby
			 */
			upstream_node = get_node_info(my_local_conn, local_options.cluster_name, node_info.upstream_node_id);

            if(upstream_node.type == MASTER)
            {
                log_debug(_("failure detected on master node (%i); attempting to promote a standby\n"),
                          node_info.upstream_node_id);
                do_master_failover();
            }
            else
            {
                log_debug(_("failure detected on upstream node %i; attempting to reconnect to new upstream node\n"),
                          node_info.upstream_node_id);

				if(!do_upstream_standby_failover(upstream_node))
				{
					PQExpBufferData errmsg;
					initPQExpBuffer(&errmsg);

					appendPQExpBuffer(&errmsg,
									  _("unable to reconnect to new upstream node, terminating..."));

					log_err("%s\n", errmsg.data);

					create_event_record(master_conn,
										&local_options,
										local_options.node,
										"repmgrd_shutdown",
										false,
										errmsg.data);

					terminate(ERR_DB_CON);
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
	 * If original master has gone away we'll need to get the new one
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

	active_master_id = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);

	if(active_master_id != master_options.node)
	{
		log_notice(_("connecting to active master (node %i)...\n"), active_master_id); \
		if(master_conn != NULL)
		{
			PQfinish(master_conn);
		}
		master_conn = get_master_connection(my_local_conn,
											 local_options.cluster_name,
											 &master_options.node, NULL);

	}

	/*
	 * Cancel any query that is still being executed, so i can insert the
	 * current record
	 */
	if (!cancel_query(master_conn, local_options.master_response_timeout))
		return;
	if (wait_connection_availability(master_conn, local_options.master_response_timeout) != 1)
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

	/* Get master xlog info */
	sqlquery_snprintf(sqlquery, "SELECT pg_current_xlog_location()");

	res = PQexec(master_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s\n"), PQerrorMessage(master_conn));
		PQclear(res);
		return;
	}

	strncpy(last_wal_master_location, PQgetvalue(res, 0, 0), MAXLEN);
	PQclear(res);

	/* Calculate the lag */
	lsn_master = lsn_to_xlogrecptr(last_wal_master_location, NULL);
	lsn_standby_received = lsn_to_xlogrecptr(last_wal_standby_received, NULL);
	lsn_standby_applied = lsn_to_xlogrecptr(last_wal_standby_applied, NULL);

	/*
	 * Build the SQL to execute on master
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
					  get_repmgr_schema_quoted(master_conn),
					  master_options.node, local_options.node,
					   monitor_standby_timestamp, last_wal_standby_applied_timestamp,
					  last_wal_master_location, last_wal_standby_received,
					  (long long unsigned int)(lsn_master - lsn_standby_received),
					  (long long unsigned int)(lsn_standby_received - lsn_standby_applied));

	/*
	 * Execute the query asynchronously, but don't check for a result. We will
	 * check the result next time we pause for a monitor step.
	 */
	log_debug("standby_monitor: %s\n", sqlquery);
	if (PQsendQuery(master_conn, sqlquery) == 0)
		log_warning(_("query could not be sent to master. %s\n"),
					PQerrorMessage(master_conn));
}


/*
 * do_master_failover()
 *
 * Handles failover to new cluster master
 */

static void
do_master_failover(void)
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
    t_node_info failed_master = {-1, NO_UPSTREAM_NODE, "", InvalidXLogRecPtr, UNKNOWN, false, false};

	/* Store details of the best candidate for promotion to master here */
	t_node_info best_candidate = {-1, NO_UPSTREAM_NODE, "", InvalidXLogRecPtr, UNKNOWN, false, false};

	/* get a list of standby nodes, including myself */
	sprintf(sqlquery,
			"SELECT id, conninfo, type, upstream_node_id "
			"  FROM %s.repl_nodes "
			" WHERE cluster = '%s' "
            "   AND active IS TRUE "
			"   AND priority > 0 "
			" ORDER BY priority DESC, id "
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
	log_debug(_("%d active nodes registered\n"), total_nodes);

	/*
	 * Build an array with the nodes and indicate which ones are visible and
	 * ready
	 */
	for (i = 0; i < total_nodes; i++)
	{
		nodes[i].node_id = atoi(PQgetvalue(res, i, 0));

		strncpy(nodes[i].conninfo_str, PQgetvalue(res, i, 1), MAXCONNINFO);

		nodes[i].type = parse_node_type(PQgetvalue(res, i, 2));

		/* Copy details of the failed node */
		/* XXX only node_id is actually used later */
		if(nodes[i].type == MASTER)
		{
			failed_master.node_id = nodes[i].node_id;
			failed_master.xlog_location = nodes[i].xlog_location;
			failed_master.is_ready = nodes[i].is_ready;
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
				  "Let the other standby servers decide which one will be the master.\n"
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
		/* XXX do we need to terminate ourselves if the queried node has a problem? */
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
				  " considered as new master and exit.\n"),
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
						  "be considered as new master and exit.\n"),
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
	 * determine which one is the best candidate to promote to master
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
			 * If no candidate has been found so far, the first visible and ready
			 * node becomes the best candidate by default
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
		PQExpBufferData event_details;

		initPQExpBuffer(&event_details);
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


		/* update internal record for this node */
		node_info = get_node_info(my_local_conn, local_options.cluster_name, local_options.node);

		appendPQExpBuffer(&event_details,
						  _("node %i promoted to master; old master %i marked as failed"),
						  node_info.node_id,
						  failed_master.node_id);

		/* my_local_conn is now the master */
		create_event_record(my_local_conn,
							&local_options,
							node_info.node_id,
							"repmgrd_failover_promote",
							true,
							event_details.data);

	}
    /* local node not promotion candidate - find the new master */
	else
	{
		PGconn	   *new_master_conn;
		PQExpBufferData event_details;

		initPQExpBuffer(&event_details);
		/* wait */
		sleep(10);

		if (verbose)
			log_info(_("node %d is the best candidate to be the new master, we should follow it...\n"),
					 best_candidate.node_id);
		log_debug(_("follow command is: \"%s\"\n"), local_options.follow_command);

		/*
		 * The new master may some time to be promoted. The follow command
		 * should take care of that.
		 */
		if (log_type == REPMGR_STDERR && *local_options.logfile)
		{
			fflush(stderr);
		}

		/*
		 * If 9.4 or later, and replication slots in use, we'll need to create a
		 * slot on the new master
		 */
		new_master_conn = establish_db_connection(best_candidate.conninfo_str, true);

		if(local_options.use_replication_slots)
		{
			if(create_replication_slot(new_master_conn, node_info.slot_name) == false)
			{

				appendPQExpBuffer(&event_details,
								  _("Unable to create slot '%s' on the master node: %s"),
								  node_info.slot_name,
								  PQerrorMessage(new_master_conn));

				log_err("%s\n", event_details.data);

				create_event_record(new_master_conn,
									&local_options,
									node_info.node_id,
									"repmgrd_failover_follow",
									false,
									event_details.data);

				PQfinish(new_master_conn);
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
		if(update_node_record_set_upstream(new_master_conn, node_info.node_id, best_candidate.node_id) == false)
		{
			appendPQExpBuffer(&event_details,
							  _("Unable to update node record for node %i (following new upstream node %i)"),
							  node_info.node_id,
							  best_candidate.node_id);

			log_err("%s\n", event_details.data);

			create_event_record(new_master_conn,
								&local_options,
								node_info.node_id,
								"repmgrd_failover_follow",
								false,
								event_details.data);

			terminate(ERR_BAD_CONFIG);
		}

		/* update internal record for this node*/
		node_info = get_node_info(new_master_conn, local_options.cluster_name, local_options.node);
		appendPQExpBuffer(&event_details,
						  _("Node %i now following new upstream node %i"),
						  node_info.node_id,
						  best_candidate.node_id);

		create_event_record(new_master_conn,
							&local_options,
							node_info.node_id,
							"repmgrd_failover_follow",
							true,
							event_details.data);

		PQfinish(new_master_conn);
		termPQExpBuffer(&event_details);
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
	 * Verify that we can still talk to the cluster master even though
	 * node upstream is not available
	 */
	if (!check_connection(master_conn, "master"))
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
						  get_repmgr_schema_quoted(master_conn),
						  upstream_node_id);

		res = PQexec(master_conn, sqlquery);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			log_err(_("unable to query cluster master: %s\n"), PQerrorMessage(master_conn));
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
			 * Upstream node is an inactive master, meaning no there are no direct
			 * upstream nodes available to reattach to.
			 *
			 * XXX For now we'll simply terminate, however it would make sense to
			 * provide an option to either try and find the current master and/or
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

	if(update_node_record_set_upstream(master_conn, node_info.node_id, upstream_node_id) == false)
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
	 * Check if the node is still available if after
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
 * to the current master server (as stored in the global variable
 * `master_conn`) and update its record to failed.
 */

static bool
set_local_node_failed(void)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];
	int			active_master_node_id = NODE_NOT_FOUND;
	char		master_conninfo[MAXLEN];

	if (!check_connection(master_conn, "master"))
	{
		log_err(_("set_local_node_failed(): Unable to connect to last known master node\n"));
		return false;
	}

	/*
	 * Check that the node `master_conn` is connected to is node is still
	 * master - it's just about conceivable that it might have become a
	 * standby of a new master in the intervening period
	 */

	sqlquery_snprintf(sqlquery,
					  "SELECT id, conninfo "
					  "  FROM %s.repl_nodes "
					  " WHERE type = 'master' "
					  "   AND active IS TRUE ",
					  get_repmgr_schema_quoted(master_conn));

	res = PQexec(master_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("unable to obtain record for active master: %s\n"),
				PQerrorMessage(master_conn));

		return false;
	}

	if(!PQntuples(res))
	{
		log_err(_("no active master record found\n"));
		return false;
	}

	active_master_node_id = atoi(PQgetvalue(res, 0, 0));
	strncpy(master_conninfo, PQgetvalue(res, 0, 1), MAXLEN);
	PQclear(res);

	if(active_master_node_id != master_options.node)
	{
		log_notice(_("current active master is %i; attempting to connect\n"),
			active_master_node_id);
		PQfinish(master_conn);
		master_conn = establish_db_connection(master_conninfo, false);

		if(PQstatus(master_conn) != CONNECTION_OK)
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
					  get_repmgr_schema_quoted(master_conn),
					  node_info.node_id);

	res = PQexec(master_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("unable to set local node %i as inactive on master: %s\n"),
				node_info.node_id,
				PQerrorMessage(master_conn));

		return false;
	}

	log_notice(_("marking this node (%i) as inactive on master\n"), node_info.node_id);
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
	 * If there isn't any results then we have not configured a master node
	 * yet in repmgr or the connection string is pointing to the wrong
	 * database.
	 *
	 * XXX if we are the master, should we try to create the tables needed?
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
						  get_repmgr_schema_quoted(master_conn),
						  local_options.node,
						  local_options.cluster_name,
						  local_options.node_name,
						  local_options.conninfo);

		if (!PQexec(master_conn, sqlquery))
		{
			log_err(_("unable to insert node details, %s\n"),
					PQerrorMessage(master_conn));
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

	log_info(_("%s terminating...\n"), progname);

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
					  get_repmgr_schema_quoted(master_conn),
					  local_options.conninfo,
					  local_options.priority,
					  local_options.node);

	res = PQexec(master_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		PQExpBufferData errmsg;
		initPQExpBuffer(&errmsg);

		appendPQExpBuffer(&errmsg,
						  _("unable to update registration: %s"),
						  PQerrorMessage(master_conn));

		log_err("%s\n", errmsg.data);

		create_event_record(master_conn,
							&local_options,
							local_options.node,
							"repmgrd_shutdown",
							false,
							errmsg.data);
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

	t_node_info node_info = { NODE_NOT_FOUND, NO_UPSTREAM_NODE, "", InvalidXLogRecPtr, UNKNOWN, false, false};

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
		PQExpBufferData errmsg;
		initPQExpBuffer(&errmsg);

		appendPQExpBuffer(&errmsg,
						  _("unable to retrieve record for node %i: %s"),
						  node_id,
						  PQerrorMessage(conn));

		log_err("%s\n", errmsg.data);

		create_event_record(NULL,
							&local_options,
							local_options.node,
							"repmgrd_shutdown",
							false,
							errmsg.data);

		PQclear(res);
		terminate(ERR_DB_QUERY);
	}

	if (!PQntuples(res)) {
		log_warning(_("No record found record for node %i\n"), node_id);
		PQclear(res);
		node_info.node_id = NODE_NOT_FOUND;
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
		return MASTER;
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
