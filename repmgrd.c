/*
 * repmgrd.c - Replication manager daemon
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
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
#include "log.h"
#include "version.h"

/* Message strings passed in repmgrSharedState->location */

#define PASSIVE_NODE "PASSIVE_NODE"
#define LSN_QUERY_ERROR "LSN_QUERY_ERROR"

/* Local info */
t_configuration_options local_options = T_CONFIGURATION_OPTIONS_INITIALIZER;
PGconn	   *my_local_conn = NULL;

/* Master info */
t_configuration_options master_options = T_CONFIGURATION_OPTIONS_INITIALIZER;

PGconn	   *master_conn = NULL;

char	   *config_file = "";
bool		verbose = false;
bool		monitoring_history = false;
t_node_info node_info;

bool		failover_done = false;

/*
 * when `failover=manual`, and the upstream server has gone away,
 * this flag is set to indicate we should connect to whatever the
 * current master is to update monitoring information
 */
bool		manual_mode_upstream_disconnected = false;

char	   *pid_file = NULL;
int			server_version_num = 0;

static void help(void);
static void usage(void);
static void check_cluster_configuration(PGconn *conn);
static void check_node_configuration(void);

static void standby_monitor(void);
static void witness_monitor(void);
static bool check_connection(PGconn **conn, const char *type, const char *conninfo);
static bool set_local_node_status(void);

static void update_shared_memory(char *last_wal_standby_applied);
static void update_registration(void);
static void do_master_failover(void);
static bool do_upstream_standby_failover(t_node_info upstream_node);

static t_node_info get_node_info(PGconn *conn, char *cluster, int node_id);
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
	if (PQstatus(master_conn) == CONNECTION_OK && PQisBusy(master_conn) == 1)
		cancel_query(master_conn, local_options.master_response_timeout);


	if (PQstatus(my_local_conn) == CONNECTION_OK)
		PQfinish(my_local_conn);

	if (PQstatus(master_conn) == CONNECTION_OK)
		PQfinish(master_conn);
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
		{"help", no_argument, NULL, OPT_HELP},
		{"version", no_argument, NULL, 'V'},
		{NULL, 0, NULL, 0}
	};

	int			optindex;
	int			c;
	bool		daemonize = false;
	bool        startup_event_logged = false;

	FILE	   *fd;

	set_progname(argv[0]);

	/* Disallow running as root to prevent directory ownership problems */
	if (geteuid() == 0)
	{
		fprintf(stderr,
				_("%s: cannot be run as root\n"
				  "Please log in (using, e.g., \"su\") as the "
				  "(unprivileged) user that owns "
				  "the data directory.\n"
				),
				progname());
		exit(1);
	}


	while ((c = getopt_long(argc, argv, "?Vf:vmdp:", long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case '?':
				/* Actual help option given */
				if (strcmp(argv[optind - 1], "-?") == 0)
				{
					help();
					exit(SUCCESS);
				}
				/* unknown option reported by getopt */
				else
					goto unknown_option;
				break;
			case OPT_HELP:
				help();
				exit(SUCCESS);
			case 'V':
				printf("%s %s (PostgreSQL %s)\n", progname(), REPMGR_VERSION, PG_VERSION);
				exit(SUCCESS);
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
		unknown_option:
				usage();
				exit(ERR_BAD_CONFIG);
		}
	}


	/*
	 * Tell the logger we're a daemon - this will ensure any output logged
	 * before the logger is initialized will be formatted correctly
	 */
	logger_output_mode = OM_DAEMON;

	/*
	 * Parse the configuration file, if provided. If no configuration file
	 * was provided, or one was but was incomplete, parse_config() will
	 * abort anyway, with an appropriate message.
	 *
	 * XXX it might be desirable to create an event record for this, in
	 * which case we'll need to refactor parse_config() not to abort,
	 * and return the error message.
	 */
	load_config(config_file, verbose, &local_options, argv[0]);

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

	logger_init(&local_options, progname());

	if (verbose)
		logger_set_verbose();

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
	/* XXX check this handles quoting properly */
	maxlen_snprintf(repmgr_schema, "%s%s", DEFAULT_REPMGR_SCHEMA_PREFIX,
			 local_options.cluster_name);

	log_info(_("connecting to database '%s'\n"),
			 local_options.conninfo);
	my_local_conn = establish_db_connection(local_options.conninfo, true);

	/* Verify that server is a supported version */
	log_info(_("connected to database, checking its state\n"));

	server_version_num = get_server_version(my_local_conn, NULL);

	if (server_version_num < MIN_SUPPORTED_VERSION_NUM)
	{
		if (server_version_num > 0)
		{
			log_err(_("%s requires PostgreSQL %s or later\n"),
					progname(),
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

	/*
	 * No node record found - exit gracefully
	 *
	 * Note: it's highly unlikely this situation will occur when starting
	 * repmgrd on a witness, unless someone goes to the trouble of
	 * deleting the node record from the previously copied table.
	 */

	if (node_info.node_id == NODE_NOT_FOUND)
	{
		log_err(_("No metadata record found for this node - terminating\n"));
		log_hint(_("Check that 'repmgr (master|standby) register' was executed for this node\n"));
		terminate(ERR_BAD_CONFIG);
	}

	log_debug("node id is %i, upstream is %i\n", node_info.node_id, node_info.upstream_node_id);

    /*
     * Check if node record is active - if not, and `failover=automatic`, the node
     * won't be considered as a promotion candidate; this often happens when
     * a failed primary is recloned and the node was not re-registered, giving
     * the impression failover capability is there when it's not. In this case
     * abort with an error and a hint about registering.
     *
     * If `failover=manual`, repmgrd can continue to passively monitor the node, but
     * we should nevertheless issue a warning and the same hint.
     */

    if (node_info.active == false)
    {
        char *hint = "Check that 'repmgr (master|standby) register' was executed for this node";

        switch (local_options.failover)
        {
            case AUTOMATIC_FAILOVER:
                log_err(_("This node is marked as inactive and cannot be used for failover\n"));
                log_hint(_("%s\n"), hint);
                terminate(ERR_BAD_CONFIG);

            case MANUAL_FAILOVER:
                log_warning(_("This node is marked as inactive and will be passively monitored only\n"));
                log_hint(_("%s\n"), hint);
                break;

            default:
                /* This should never happen */
                log_err(_("Unknown failover mode %i\n"), local_options.failover);
                terminate(ERR_BAD_CONFIG);
        }

    }

	/*
	 * MAIN LOOP This loops cycles at startup and once per failover and
	 * Requisites:
	 *  - my_local_conn must have an active connection to the monitored node
	 *  - master_conn must not be open
	 */
	do
	{
		/* Timer for repl_nodes synchronisation interval */
		int sync_repl_nodes_elapsed = 0;

		/*
		 * Set my server mode, establish a connection to master and start
		 * monitoring
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

				if (reload_config(&local_options))
				{
					PQfinish(my_local_conn);
					my_local_conn = establish_db_connection(local_options.conninfo, true);
					master_conn = my_local_conn;
					update_registration();
				}

				/* Log startup event */
				if (startup_event_logged == false)
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
					if (check_connection(&master_conn, "master", NULL))
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
						 * if we can reload the configuration file, then could need to change
						 * my_local_conn
						 */
						if (reload_config(&local_options))
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
				log_info(_("connecting to master node of cluster '%s'\n"),
						 local_options.cluster_name);

				master_conn = get_master_connection(my_local_conn,
													local_options.cluster_name,
													&master_options.node, NULL);

				if (PQstatus(master_conn) != CONNECTION_OK)
				{
					PQExpBufferData errmsg;
					initPQExpBuffer(&errmsg);

					appendPQExpBuffer(&errmsg,
									  _("unable to connect to master node"));

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

				if (reload_config(&local_options))
				{
					PQfinish(my_local_conn);
					my_local_conn = establish_db_connection(local_options.conninfo, true);
					update_registration();
				}

				/* Log startup event */
				if (startup_event_logged == false)
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
					if (node_info.type == STANDBY)
					{
						log_verbose(LOG_DEBUG, "standby check loop...\n");
						standby_monitor();
					}
					else if (node_info.type == WITNESS)
					{
						log_verbose(LOG_DEBUG, "witness check loop...\n");
						witness_monitor();
					}

					sleep(local_options.monitor_interval_secs);

					/*
					 * On a witness node, regularly resync the repl_nodes table
					 * to keep up with any changes on the primary
					 *
					 * TODO: only resync the table if changes actually detected
					 */
					if (node_info.type == WITNESS)
					{
						sync_repl_nodes_elapsed += local_options.monitor_interval_secs;
						log_debug(_("seconds since last node record sync: %i (sync interval: %i)\n"), sync_repl_nodes_elapsed, local_options.witness_repl_nodes_sync_interval_secs);
						if(sync_repl_nodes_elapsed >= local_options.witness_repl_nodes_sync_interval_secs)
						{
							log_debug(_("Resyncing repl_nodes table\n"));
							witness_copy_node_records(master_conn, my_local_conn, local_options.cluster_name);
							sync_repl_nodes_elapsed = 0;
						}
					}

					if (got_SIGHUP)
					{
						/*
						 * if we can reload, then could need to change
						 * my_local_conn
						 */
						if (reload_config(&local_options))
						{
							PQfinish(my_local_conn);
							my_local_conn = establish_db_connection(local_options.conninfo, true);
							update_registration();
						}
						got_SIGHUP = false;
					}

					if (failover_done)
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
	connection_ok = check_connection(&master_conn, "master", NULL);

	if (connection_ok == false)
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
					local_options.reconnect_interval
					);
				PQfinish(master_conn);
				sleep(local_options.reconnect_interval);
			}
			else
			{
				log_info(_("new master found with node ID: %i\n"), master_options.node);
				connection_ok = true;

				/*
				 * Update the repl_nodes table from the new master to reflect the changed
				 * node configuration
				 *
				 * It would be neat to be able to handle this with e.g. table-based
				 * logical replication if available in core
				 */
				witness_copy_node_records(master_conn, my_local_conn, local_options.cluster_name);

				break;
			}
		}

		if (connection_ok == false)
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

	/* Get timestamp for monitoring update */
	sqlquery_snprintf(sqlquery, "SELECT CURRENT_TIMESTAMP");

	res = PQexec(my_local_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s\n"), PQerrorMessage(my_local_conn));
		PQclear(res);
		/* if there is any error just let it be and retry in next loop */
		return;
	}

	strncpy(monitor_witness_timestamp, PQgetvalue(res, 0, 0), MAXLEN);
	PQclear(res);

	/*
	 * Build the SQL to execute on master
	 */
	if (server_version_num >= 100000)
	{
		sqlquery_snprintf(sqlquery,
						  "INSERT INTO %s.repl_monitor "
						  "           (primary_node, standby_node, "
						  "            last_monitor_time, last_apply_time, "
						  "            last_wal_primary_location, last_wal_standby_location, "
						  "            replication_lag, apply_lag )"
						  "      VALUES(%d, %d, "
						  "             '%s'::TIMESTAMP WITH TIME ZONE, NULL, "
						  "             pg_catalog.pg_current_wal_lsn(), NULL, "
						  "             0, 0) ",
						  get_repmgr_schema_quoted(my_local_conn),
						  master_options.node,
						  local_options.node,
						  monitor_witness_timestamp);
	}
	else
	{
		sqlquery_snprintf(sqlquery,
						  "INSERT INTO %s.repl_monitor "
						  "           (primary_node, standby_node, "
						  "            last_monitor_time, last_apply_time, "
						  "            last_wal_primary_location, last_wal_standby_location, "
						  "            replication_lag, apply_lag )"
						  "      VALUES(%d, %d, "
						  "             '%s'::TIMESTAMP WITH TIME ZONE, NULL, "
						  "             pg_catalog.pg_current_xlog_location(), NULL, "
						  "             0, 0) ",
						  get_repmgr_schema_quoted(my_local_conn),
						  master_options.node,
						  local_options.node,
						  monitor_witness_timestamp);
	}

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
	char		sqlquery[QUERY_STR_LEN];

	char		monitor_standby_timestamp[MAXLEN];
	char		last_wal_primary_location[MAXLEN];
	char		last_xlog_receive_location[MAXLEN];
	char		last_xlog_replay_location[MAXLEN];
	char		last_xact_replay_timestamp[MAXLEN];
	bool		receiving_streamed_wal = true;

	XLogRecPtr	lsn_master_current_xlog_location;
	XLogRecPtr	lsn_last_xlog_receive_location;
	XLogRecPtr	lsn_last_xlog_replay_location;

	long long unsigned int replication_lag;
	long long unsigned int apply_lag;

	int			connection_retries,
				ret;
	bool		did_retry = false;

	PGconn	   *upstream_conn;
	char		upstream_conninfo[MAXCONNINFO];
	int			upstream_node_id;

	int			active_master_id;
	const char *upstream_node_type = NULL;



	/*
	 * Verify that the local node is still available - if not there's
	 * no point in doing much else anyway
	 */

	if (!check_connection(&my_local_conn, "standby", NULL))
	{
		PQExpBufferData errmsg;

		set_local_node_status();

		initPQExpBuffer(&errmsg);

		appendPQExpBuffer(&errmsg,
						  _("failed to connect to local node, node marked as failed!"));

		log_err("%s\n", errmsg.data);

		goto continue_monitoring_standby;
	}

	/*
	 * Standby has `failover` set to manual and is disconnected from
	 * replication following a prior upstream node failure - we'll
	 * find the master to be able to write monitoring information, if
	 * required
	 */
	if (manual_mode_upstream_disconnected == true)
	{
		upstream_conn = get_master_connection(my_local_conn,
												local_options.cluster_name,
												&upstream_node_id,
												upstream_conninfo);
		upstream_node_type = "master";
	}
	else
	{
		upstream_conn = get_upstream_connection(my_local_conn,
												local_options.cluster_name,
												local_options.node,
												&upstream_node_id,
												upstream_conninfo);

		upstream_node_type = (upstream_node_id == master_options.node)
			? "master"
			: "upstream";
	}

	/*
	 * Check that the upstream node is still available
	 * If not, initiate failover process
	 *
	 * This takes up to local_options.reconnect_attempts *
	 * local_options.reconnect_interval seconds
	 */

	check_connection(&upstream_conn, upstream_node_type, upstream_conninfo);

	if (PQstatus(upstream_conn) != CONNECTION_OK)
	{
		int previous_master_node_id = master_options.node;

		PQfinish(upstream_conn);
		upstream_conn = NULL;

		/*
		 * When `failover=manual`, no actual failover will be performed, instead
		 * the following happens:
		 *  - find the new master
		 *  - create an event notification `standby_disconnect_manual`
		 *  - set a flag to indicate we're disconnected from replication,
		 */
		if (local_options.failover == MANUAL_FAILOVER)
		{
			log_err(_("Unable to reconnect to %s. Now checking if another node has been promoted.\n"), upstream_node_type);

			/*
			 * Set the location string in shared memory to indicate to other
			 * repmgrd instances that we're *not* a promotion candidate and
			 * that other repmgrd instance should not expect location updates
			 * from us
			 */

			update_shared_memory(PASSIVE_NODE);

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
					log_notice(_("connected to node %d, continuing monitoring.\n"),
							master_options.node);
					break;
				}
				else
				{
					/*
					 * XXX this is the only place where `retry_promote_interval_secs`
					 * is used - this parameter should be renamed or possibly be replaced
					 */
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

			/*
			 * connected to a master - is it the same as the former upstream?
			 * if not:
			 *  - create event standby_disconnect
			 *  - set global "disconnected_manual_standby"
			 */

			if (previous_master_node_id != master_options.node)
			{
				PQExpBufferData errmsg;
				initPQExpBuffer(&errmsg);

				appendPQExpBuffer(&errmsg,
								  _("node %i is in manual failover mode and is now disconnected from replication"),
								  local_options.node);

				log_verbose(LOG_DEBUG, "old master: %i; current: %i\n", previous_master_node_id, master_options.node);

				manual_mode_upstream_disconnected = true;

				create_event_record(master_conn,
									&local_options,
									local_options.node,
									"standby_disconnect_manual",
									/* here "true" indicates the action has occurred as expected */
									true,
									errmsg.data);

			}
		}
		else if (local_options.failover == AUTOMATIC_FAILOVER)
		{
			/*
			 * When we return from this function we will have a new master
			 * and a new master_conn
			 *
			 * Failover handling is handled differently depending on whether
			 * the failed node is the master or a cascading standby
			 */
			t_node_info upstream_node;

			upstream_node = get_node_info(my_local_conn, local_options.cluster_name, upstream_node_id);

			if (upstream_node.type == MASTER)
			{
				log_debug(_("failure detected on master node (%i); attempting to promote a standby\n"),
						  node_info.upstream_node_id);
				do_master_failover();
			}
			else
			{
				log_debug(_("failure detected on upstream node %i; attempting to reconnect to new upstream node\n"),
						  node_info.upstream_node_id);

				if (!do_upstream_standby_failover(upstream_node))
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

  continue_monitoring_standby:
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
				 * We should log a message so the user knows of the situation at hand.
				 *
				 * XXX check if the original master is still active and display a warning
				 * XXX add event notification
				 */
				log_err(_("It seems this server was promoted manually (not by repmgr) so you might by in the presence of a split-brain.\n"));
				log_err(_("Check your cluster and manually fix any anomaly.\n"));
				terminate(1);
				break;

			case -1:
				log_err(_("standby node has disappeared, trying to reconnect...\n"));
				did_retry = true;

				if (!check_connection(&my_local_conn, "standby", NULL))
				{
					set_local_node_status();
					/*
					 * Let's continue checking, and if the postgres server on the
					 * standby comes back up, we will activate it again
					 */
				}

				break;
		}
	} while (ret == -1);

	if (did_retry)
	{
		/*
		 * There's a possible situation where the standby went down for some reason
		 * (maintenance for example) and is now up and maybe connected once again to
		 * the stream. If we set the local standby node as failed and it's now running
		 * and receiving replication data, we should activate it again.
		 */
		set_local_node_status();
		log_info(_("standby connection recovered!\n"));
	}

	/* Fast path for the case where no history is requested */
	if (!monitoring_history)
		return;

	/*
	 * If original master has gone away we'll need to get the new one
	 * from the upstream node to write monitoring information
	 */

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

	if (PQntuples(res) == 0)
	{
		log_err(_("standby_monitor(): no active master found\n"));
		PQclear(res);
		return;
	}

	active_master_id = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);

	if (active_master_id != master_options.node)
	{
		log_notice(_("connecting to active master (node %i)...\n"), active_master_id);
		if (master_conn != NULL)
		{
			PQfinish(master_conn);
		}
		master_conn = get_master_connection(my_local_conn,
											 local_options.cluster_name,
											 &master_options.node, NULL);
	}
	if (PQstatus(master_conn) != CONNECTION_OK)
		PQreset(master_conn);

	/*
	 * Cancel any query that is still being executed, so i can insert the
	 * current record
	 */
	if (!cancel_query(master_conn, local_options.master_response_timeout))
		return;
	if (wait_connection_availability(master_conn, local_options.master_response_timeout) != 1)
		return;

	/* Get local xlog info
	 *
	 * If receive_location is NULL, we're in archive recovery and not streaming WAL
	 * If receive_location is less than replay location, we were streaming WAL but are
	 *   somehow disconnected and evidently in archive recovery
	 */

	if (server_version_num >= 100000)
	{
		sqlquery_snprintf(sqlquery,
						  " SELECT ts, "
						  "        CASE WHEN (receive_location IS NULL OR receive_location < replay_location) "
						  "          THEN replay_location "
						  "          ELSE receive_location"
						  "        END AS receive_location,"
						  "        replay_location, "
						  "        replay_timestamp, "
						  "        COALESCE(receive_location, '0/0') >= replay_location AS receiving_streamed_wal "
						  "   FROM (SELECT CURRENT_TIMESTAMP AS ts, "
						  "         pg_catalog.pg_last_wal_receive_lsn()  AS receive_location, "
						  "         pg_catalog.pg_last_wal_replay_lsn()   AS replay_location, "
						  "         pg_catalog.pg_last_xact_replay_timestamp() AS replay_timestamp "
						  "        ) q ");

	}
	else
	{
		sqlquery_snprintf(sqlquery,
						  " SELECT ts, "
						  "        CASE WHEN (receive_location IS NULL OR receive_location < replay_location) "
						  "          THEN replay_location "
						  "          ELSE receive_location"
						  "        END AS receive_location,"
						  "        replay_location, "
						  "        replay_timestamp, "
						  "        COALESCE(receive_location, '0/0') >= replay_location AS receiving_streamed_wal "
						  "   FROM (SELECT CURRENT_TIMESTAMP AS ts, "
						  "         pg_catalog.pg_last_xlog_receive_location() AS receive_location, "
						  "         pg_catalog.pg_last_xlog_replay_location()  AS replay_location, "
						  "         pg_catalog.pg_last_xact_replay_timestamp() AS replay_timestamp "
						  "        ) q ");
	}


	res = PQexec(my_local_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s\n"), PQerrorMessage(my_local_conn));
		PQclear(res);
		/* if there is any error just let it be and retry in next loop */
		return;
	}

	strncpy(monitor_standby_timestamp,  PQgetvalue(res, 0, 0), MAXLEN);
	strncpy(last_xlog_receive_location, PQgetvalue(res, 0, 1), MAXLEN);
	strncpy(last_xlog_replay_location,  PQgetvalue(res, 0, 2), MAXLEN);
	strncpy(last_xact_replay_timestamp, PQgetvalue(res, 0, 3), MAXLEN);

	receiving_streamed_wal = (strcmp(PQgetvalue(res, 0, 4), "t") == 0)
		? true
		: false;

	if (receiving_streamed_wal == false)
	{
		log_verbose(LOG_DEBUG, _("standby %i not connected to streaming replication"), local_options.node);
	}

	PQclear(res);

	/*
	 * Get master xlog position
	 *
	 * TODO: investigate whether pg_current_xlog_insert_location() would be a better
	 * choice; see: https://github.com/2ndQuadrant/repmgr/issues/189
	 */

	if (server_version_num >= 100000)
		sqlquery_snprintf(sqlquery, "SELECT pg_catalog.pg_current_wal_lsn()");
	else
		sqlquery_snprintf(sqlquery, "SELECT pg_catalog.pg_current_xlog_location()");

	res = PQexec(master_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s\n"), PQerrorMessage(master_conn));
		PQclear(res);
		return;
	}

	strncpy(last_wal_primary_location, PQgetvalue(res, 0, 0), MAXLEN);
	PQclear(res);

	lsn_master_current_xlog_location = lsn_to_xlogrecptr(last_wal_primary_location, NULL);
	lsn_last_xlog_receive_location = lsn_to_xlogrecptr(last_xlog_receive_location, NULL);
	lsn_last_xlog_replay_location = lsn_to_xlogrecptr(last_xlog_replay_location, NULL);

	if (lsn_last_xlog_receive_location >= lsn_last_xlog_replay_location)
	{
		apply_lag = (long long unsigned int)lsn_last_xlog_receive_location - lsn_last_xlog_replay_location;
	}
	else
	{
		/* This should never happen, but in case it does set apply lag to zero */
		log_warning("Standby receive (%s) location appears less than standby replay location (%s)\n",
					last_xlog_receive_location,
					last_xlog_replay_location);
		apply_lag = 0;
	}


	/* Calculate replication lag */
	if (lsn_master_current_xlog_location >= lsn_last_xlog_receive_location)
	{
		replication_lag = (long long unsigned int)(lsn_master_current_xlog_location - lsn_last_xlog_receive_location);
	}
	else
	{
		/* This should never happen, but in case it does set replication lag to zero */
		log_warning("Master xlog (%s) location appears less than standby receive location (%s)\n",
					last_wal_primary_location,
					last_xlog_receive_location);
		replication_lag = 0;
	}

	/*
	 * Build the SQL to execute on master
	 */
	sqlquery_snprintf(sqlquery,
					  "INSERT INTO %s.repl_monitor "
					  "           (primary_node, "
					  "            standby_node, "
					  "            last_monitor_time, "
					  "            last_apply_time, "
					  "            last_wal_primary_location, "
					  "            last_wal_standby_location, "
					  "            replication_lag, "
					  "            apply_lag ) "
					  "     VALUES(%d, "
					  "            %d, "
					  "            '%s'::TIMESTAMP WITH TIME ZONE, "
					  "            '%s'::TIMESTAMP WITH TIME ZONE, "
					  "            '%s', "
					  "            '%s', "
					  "            %llu, "
					  "            %llu) ",
					  get_repmgr_schema_quoted(master_conn),
					  master_options.node,
					  local_options.node,
					  monitor_standby_timestamp,
					  last_xact_replay_timestamp,
					  last_wal_primary_location,
					  last_xlog_receive_location,
					  replication_lag,
					  apply_lag);

	/*
	 * Execute the query asynchronously, but don't check for a result. We will
	 * check the result next time we pause for a monitor step.
	 */
	log_verbose(LOG_DEBUG, "standby_monitor:() %s\n", sqlquery);

	if (PQsendQuery(master_conn, sqlquery) == 0)
	{
		log_warning(_("query could not be sent to master: %s\n"),
					PQerrorMessage(master_conn));
	}
	else
	{
		sqlquery_snprintf(sqlquery,
						  "SELECT %s.repmgr_update_last_updated();",
						  get_repmgr_schema_quoted(my_local_conn));
		res = PQexec(my_local_conn, sqlquery);

		/* not critical if the above query fails*/
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			log_warning(_("unable to set last_updated: %s\n"), PQerrorMessage(my_local_conn));

		PQclear(res);
	}
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

	int			total_active_nodes = 0;
	int			visible_nodes = 0;
	int			ready_nodes = 0;

	bool		candidate_found = false;

	int			i;
	int			r;

	XLogRecPtr	xlog_recptr;
	bool		lsn_format_ok;

	PGconn	   *node_conn = NULL;

	/*
	 * will get info about until 50 nodes, which seems to be large enough for
	 * most scenarios
	 */
	t_node_info nodes[FAILOVER_NODES_MAX_CHECK];

	/* Store details of the failed node here */
	t_node_info failed_master = T_NODE_INFO_INITIALIZER;

	/* Store details of the best candidate for promotion to master here */
	t_node_info best_candidate = T_NODE_INFO_INITIALIZER;

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
		terminate(ERR_DB_QUERY);
	}

	total_active_nodes = PQntuples(res);
	log_debug(_("%d active nodes registered\n"), total_active_nodes);

	/*
	 * Build an array with the nodes and indicate which ones are visible and
	 * ready
	 */
	for (i = 0; i < total_active_nodes; i++)
	{
		char node_type[MAXLEN];

		nodes[i] = (t_node_info) T_NODE_INFO_INITIALIZER;

		nodes[i].node_id = atoi(PQgetvalue(res, i, 0));

		strncpy(nodes[i].conninfo_str, PQgetvalue(res, i, 1), MAXCONNINFO);
		strncpy(node_type, PQgetvalue(res, i, 2), MAXLEN);

		nodes[i].type = parse_node_type(node_type);

		nodes[i].upstream_node_id = atoi(PQgetvalue(res, i, 3));

		/*
		 * Initialize on false so if we can't reach this node we know that
		 * later
		 */
		nodes[i].is_visible = false;
		nodes[i].is_ready = false;

		log_debug(_("node=%i conninfo=\"%s\" type=%s\n"),
				  nodes[i].node_id,
				  nodes[i].conninfo_str,
				  node_type);

		/* Copy details of the failed master node */
		if (nodes[i].type == MASTER)
		{
			/* XXX only node_id is currently used */
			failed_master.node_id = nodes[i].node_id;

			/*
			 * XXX experimental
			 *
			 * Currently an attempt is made to connect to the master,
			 * which is very likely to be a waste of time at this point, as we'll
			 * have spent the last however many seconds trying to do just that
			 * in check_connection() before deciding it's gone away.
			 *
			 * If the master did come back at this point, the voting algorithm should decide
			 * it's the "best candidate" anyway and no standby will promote itself or
			 * attempt to follow another server.
			 *
			 * If we don't try and connect to the master here (and the code generally
			 * assumes it's failed anyway) but it does come back any time from here
			 * onwards, promotion will fail and the promotion candidate will
			 * notice the reappearance.
			 *
			 * TLDR version: by skipping the master connection attempt (and the chances
			 * the master would reappear between the last attempt in check_connection()
			 * and now are minimal) we can remove useless cycles during the failover process;
			 * if the master does reappear it will be caught before later anyway.
			 */

			continue;
		}

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
			  total_active_nodes, visible_nodes);

	/*
	 * Am I on the group that should keep alive? If I see less than half of
	 * total_active_nodes then I should do nothing
	 */
	if (visible_nodes < (total_active_nodes / 2.0))
	{
		log_err(_("Unable to reach most of the nodes.\n"
				  "Let the other standby servers decide which one will be the master.\n"
				  "Manual action will be needed to re-add this node to the cluster.\n"));
		terminate(ERR_FAILOVER_FAIL);
	}

	/* Query all available nodes to determine readiness and LSN */
	for (i = 0; i < total_active_nodes; i++)
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

		if (server_version_num >= 100000)
			sqlquery_snprintf(sqlquery, "SELECT pg_catalog.pg_last_wal_receive_lsn()");
		else
			sqlquery_snprintf(sqlquery, "SELECT pg_catalog.pg_last_xlog_receive_location()");

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
		if (xlog_recptr == InvalidXLogRecPtr)
		{
			log_err(_("InvalidXLogRecPtr detected on standby node %i\n"), nodes[i].node_id);
			terminate(ERR_FAILOVER_FAIL);
		}

		nodes[i].xlog_location = xlog_recptr;
	}

	/* last we get info about this node, and update shared memory */

	if (server_version_num >= 100000)
		sprintf(sqlquery, "SELECT pg_catalog.pg_last_wal_receive_lsn()");
	else
		sprintf(sqlquery, "SELECT pg_catalog.pg_last_xlog_receive_location()");

	res = PQexec(my_local_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s.\nReport an invalid value to not be "
				  " considered as new master and exit.\n"),
				PQerrorMessage(my_local_conn));
		PQclear(res);

		update_shared_memory(LSN_QUERY_ERROR);
		terminate(ERR_DB_QUERY);
	}
	/* write last location in shared memory */
	update_shared_memory(PQgetvalue(res, 0, 0));
	PQclear(res);

	/* Wait for each node to come up and report a valid LSN */
	for (i = 0; i < total_active_nodes; i++)
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
			log_info(_("At this point, it could be some race conditions "
					   "that are acceptable, assume the node is restarting "
					   "and starting failover procedure\n"));
			continue;
		}

		while (!nodes[i].is_ready)
		{
			char location_value[MAXLEN];

			sqlquery_snprintf(sqlquery,
							  "SELECT %s.repmgr_get_last_standby_location()",
							  get_repmgr_schema_quoted(node_conn));
			res = PQexec(node_conn, sqlquery);
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				log_err(_("PQexec failed: %s.\nReport an invalid value to not "
						  "be considered as new master and exit.\n"),
						PQerrorMessage(node_conn));
				PQclear(res);
				PQfinish(node_conn);
				terminate(ERR_DB_QUERY);
			}

			/* Copy the returned value as we'll need to reference it a few times */
			strncpy(location_value, PQgetvalue(res, 0, 0), MAXLEN);
			PQclear(res);

			xlog_recptr = lsn_to_xlogrecptr(location_value, &lsn_format_ok);

			/* If position reported as "invalid", check for format error or
			 * empty string; otherwise position is 0/0 and we need to continue
			 * looping until a valid LSN is reported
			 */
			if (xlog_recptr == InvalidXLogRecPtr)
			{
				bool continue_loop = true;

				if (lsn_format_ok == false)
				{

					/*
					 * The node is indicating it is not a promotion candidate -
					 * in this case we can store its invalid LSN to ensure it
					 * can't be a promotion candidate when comparing locations
					 */
					if (strcmp(location_value, PASSIVE_NODE) == 0)
					{
						log_debug("node %i is passive mode\n", nodes[i].node_id);
						log_info(_("node %i will not be considered for promotion\n"), nodes[i].node_id);
						nodes[i].xlog_location = InvalidXLogRecPtr;
						continue_loop = false;
					}
					/*
					 * This should probably never happen but if it does, rule the
					 * node out as a promotion candidate
					 */
					else if (strcmp(location_value, LSN_QUERY_ERROR) == 0)
					{
						log_warning(_("node %i is unable to update its shared memory and will not be considered for promotion\n"), nodes[i].node_id);
						nodes[i].xlog_location = InvalidXLogRecPtr;
						continue_loop = false;
					}

					/* Unable to parse value returned by `repmgr_get_last_standby_location()` */
					else if (*location_value == '\0')
					{
						log_crit(
							_("unable to obtain LSN from node %i"), nodes[i].node_id
							);
						log_hint(
							_("please check that 'shared_preload_libraries=repmgr_funcs' is set in postgresql.conf\n")
							);

						PQfinish(node_conn);
						/* XXX shouldn't we just ignore this node? */
						exit(ERR_BAD_CONFIG);
					}

					/*
					 * Very unlikely to happen; in the absence of any better
					 * strategy keep checking
					 */
					else {
						log_warning(_("unable to parse LSN \"%s\"\n"),
									location_value);
					}
				}
				else
				{
					log_debug(
						_("invalid LSN returned from node %i: '%s'\n"),
						nodes[i].node_id,
						location_value);
				}

				/*
				 * If the node is still reporting an InvalidXLogRecPtr, it means
				 * its repmgrd hasn't yet had time to update it (either with a valid
				 * XLogRecPtr or a message) so we continue looping.
				 *
				 * XXX we should add a timeout here to prevent infinite looping
				 * if the other node's repmgrd is not up
				 */
				if (continue_loop == true)
					continue;
			}

			if (nodes[i].xlog_location < xlog_recptr)
			{
				nodes[i].xlog_location = xlog_recptr;
			}

			log_debug(_("LSN of node %i is: %s\n"), nodes[i].node_id, location_value);

			ready_nodes++;
			nodes[i].is_ready = true;
		}

		PQfinish(node_conn);
	}


	/*
	 * determine which one is the best candidate to promote to master
	 */
	for (i = 0; i < total_active_nodes; i++)
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

	log_debug("best candidate node id is %i\n", best_candidate.node_id);

	/* if local node is the best candidate, promote it */
	if (best_candidate.node_id == local_options.node)
	{
		PQExpBufferData event_details;

		/* Close the connection to this server */
		PQfinish(my_local_conn);
		my_local_conn = NULL;

		initPQExpBuffer(&event_details);
		/* wait */
		sleep(5);

		log_notice(_("this node is the best candidate to be the new master, promoting...\n"));

		log_debug("promote command is: \"%s\"\n",
				  local_options.promote_command);

		if (log_type == REPMGR_STDERR && *local_options.logfile)
		{
			fflush(stderr);
		}

		r = system(local_options.promote_command);
		if (r != 0)
		{
			/*
			 * Check whether the primary reappeared, which will have caused the
			 * promote command to fail
			 */
			my_local_conn = establish_db_connection(local_options.conninfo, false);

			if (my_local_conn != NULL)
			{
				int master_node_id;

				master_conn = get_master_connection(my_local_conn,
													local_options.cluster_name,
													&master_node_id, NULL);

				if (master_conn != NULL && master_node_id == failed_master.node_id)
				{
					log_notice(_("Original master reappeared before this standby was promoted - no action taken\n"));

					/* XXX log an event here?  */

					PQfinish(master_conn);
					master_conn = NULL;

					/* no failover occurred but we'll want to restart connections */
					failover_done = true;
					return;
				}
			}

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
		int master_node_id;

		initPQExpBuffer(&event_details);

		/* wait */
		sleep(10);

		/*
		 * Check whether the primary reappeared while we were waiting, so we
		 * don't end up following the promotion candidate
		 */

		master_conn = get_master_connection(my_local_conn,
											local_options.cluster_name,
											&master_node_id, NULL);

		if (master_conn != NULL && master_node_id == failed_master.node_id)
		{
			log_notice(_("Original master reappeared - no action taken\n"));

			PQfinish(master_conn);
			/* no failover occurred but we'll want to restart connections */
			failover_done = true;
			return;
		}


		/* Close the connection to this server */
		PQfinish(my_local_conn);
		my_local_conn = NULL;

		/* XXX double-check the promotion candidate did become the new primary */

		log_notice(_("node %d is the best candidate for new master, attempting to follow...\n"),
				 best_candidate.node_id);

		/*
		 * The new master may some time to be promoted. The follow command
		 * should take care of that.
		 */
		if (log_type == REPMGR_STDERR && *local_options.logfile)
		{
			fflush(stderr);
		}


		log_debug(_("executing follow command: \"%s\"\n"), local_options.follow_command);

		r = system(local_options.follow_command);
		if (r != 0)
		{
			appendPQExpBuffer(&event_details,
							  _("Unable to execute follow command:\n %s"),
							  local_options.follow_command);

			log_err("%s\n", event_details.data);

			/* It won't be possible to write to the event notification
			 * table but we should be able to generate an external notification
			 * if required.
			 */
			create_event_record(NULL,
								&local_options,
								node_info.node_id,
								"repmgrd_failover_follow",
								false,
								event_details.data);

			terminate(ERR_BAD_CONFIG);
		}

		/* and reconnect to the local database */
		my_local_conn = establish_db_connection(local_options.conninfo, true);

		/* update internal record for this node*/
		new_master_conn = establish_db_connection(best_candidate.conninfo_str, true);

		node_info = get_node_info(new_master_conn, local_options.cluster_name, local_options.node);
		appendPQExpBuffer(&event_details,
						  _("node %i now following new upstream node %i"),
						  node_info.node_id,
						  best_candidate.node_id);

		log_notice("%s\n", event_details.data);

		create_event_record(new_master_conn,
							&local_options,
							node_info.node_id,
							"repmgrd_failover_follow",
							true,
							event_details.data);

		PQfinish(new_master_conn);
		termPQExpBuffer(&event_details);
	}

	/*
	 * setting "failover_done" to true will cause the node's monitoring loop
	 * to restart in the appropriate mode for the node's (possibly new) role
	 */
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
 *
 * XXX check this handles replication slots gracefully
 */
static bool
do_upstream_standby_failover(t_node_info upstream_node)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];
	int			upstream_node_id = node_info.upstream_node_id;
	int			r;
	PQExpBufferData event_details;

	log_debug(_("do_upstream_standby_failover(): performing failover for node %i\n"),
              node_info.node_id);

	/*
	 * Verify that we can still talk to the cluster master even though
	 * node upstream is not available
	 */
	if (!check_connection(&master_conn, "master", NULL))
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

		if (PQntuples(res) == 0)
		{
			log_err(_("no node with id %i found\n"), upstream_node_id);
			PQclear(res);
			return false;
		}

		/* upstream node is inactive */
		if (strcmp(PQgetvalue(res, 0, 1), "f") == 0)
		{
			/*
			 * Upstream node is an inactive master, meaning no there are no direct
			 * upstream nodes available to reattach to.
			 *
			 * XXX For now we'll simply terminate, however it would make sense to
			 * provide an option to either try and find the current master and/or
			 * a strategy to connect to a different upstream node
			 */
			if (strcmp(PQgetvalue(res, 0, 4), "master") == 0)
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
		sleep(local_options.reconnect_interval);
	}

	/* Close the connection to this server */
	PQfinish(my_local_conn);
	my_local_conn = NULL;

	initPQExpBuffer(&event_details);

	/* Follow new upstream */
	r = system(local_options.follow_command);
	if (r != 0)
	{
		appendPQExpBuffer(&event_details,
						  _("Unable to execute follow command:\n %s"),
						  local_options.follow_command);

		log_err("%s\n", event_details.data);

		/* It won't be possible to write to the event notification
		 * table but we should be able to generate an external notification
		 * if required.
		 */
		create_event_record(NULL,
							&local_options,
							node_info.node_id,
							"repmgrd_failover_follow",
							false,
							event_details.data);

		termPQExpBuffer(&event_details);

		terminate(ERR_BAD_CONFIG);
	}

	if (update_node_record_set_upstream(master_conn, local_options.cluster_name, node_info.node_id, upstream_node_id) == false)
	{
		appendPQExpBuffer(&event_details,
						  _("Unable to set node %i's new upstream ID to %i"),
						  node_info.node_id,
						  upstream_node_id);
		create_event_record(NULL,
							&local_options,
							node_info.node_id,
							"repmgrd_failover_follow",
							false,
							event_details.data);

		termPQExpBuffer(&event_details);

		terminate(ERR_BAD_CONFIG);
	}

	appendPQExpBuffer(&event_details,
					  _("node %i is now following upstream node %i"),
					  node_info.node_id,
					  upstream_node_id);

	create_event_record(NULL,
						&local_options,
						node_info.node_id,
						"repmgrd_failover_follow",
						true,
						event_details.data);

	termPQExpBuffer(&event_details);

	my_local_conn = establish_db_connection(local_options.conninfo, true);

	return true;
}



static bool
check_connection(PGconn **conn, const char *type, const char *conninfo)
{
	int			connection_retries;

	/*
	 * Check if the node is still available if after
	 * local_options.reconnect_attempts * local_options.reconnect_interval
	 * seconds of retries we cannot reconnect return false
	 */
	for (connection_retries = 0; connection_retries < local_options.reconnect_attempts; connection_retries++)
	{
		if (*conn == NULL)
		{
			if (conninfo == NULL)
			{
				log_err("INTERNAL ERROR: *conn == NULL && conninfo == NULL\n");
				terminate(ERR_INTERNAL);
			}
			*conn = establish_db_connection(conninfo, false);
		}
		if (!is_pgup(*conn, local_options.master_response_timeout))
		{
			log_warning(_("connection to %s has been lost, trying to recover... %i seconds before failover decision\n"),
						type,
						(local_options.reconnect_interval * (local_options.reconnect_attempts - connection_retries)));
			/* wait local_options.reconnect_interval seconds between retries */
			sleep(local_options.reconnect_interval);
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

	if (!is_pgup(*conn, local_options.master_response_timeout))
	{
		log_err(_("unable to reconnect to %s (timeout %i seconds)...\n"),
				type,
				local_options.master_response_timeout
			);

		return false;
	}

	return true;
}


/*
 * set_local_node_status()
 *
 * Attempt to connect to the current master server (as stored in the global
 * variable `master_conn`) and set the local node's status to the result
 * of `is_standby(my_local_conn)`. Normally this will be used to mark
 * a node as failed, but in some circumstances we may be marking it
 * as recovered.
 */

static bool
set_local_node_status(void)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];
	int			active_master_node_id = NODE_NOT_FOUND;
	char		master_conninfo[MAXLEN];
	bool		local_node_status;

	if (!check_connection(&master_conn, "master", NULL))
	{
		log_err(_("set_local_node_status(): Unable to connect to last known master node\n"));
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

	if (!PQntuples(res))
	{
		log_err(_("no active master record found\n"));
		return false;
	}

	active_master_node_id = atoi(PQgetvalue(res, 0, 0));
	strncpy(master_conninfo, PQgetvalue(res, 0, 1), MAXLEN);
	PQclear(res);

	if (active_master_node_id != master_options.node)
	{
		log_notice(_("current active master is %i; attempting to connect\n"),
			active_master_node_id);
		PQfinish(master_conn);
		master_conn = establish_db_connection(master_conninfo, false);

		if (PQstatus(master_conn) != CONNECTION_OK)
		{
			log_err(_("unable to connect to active master\n"));
			return false;
		}

		log_notice(_("Connection to new master was successful\n"));
	}


	/*
	 * Attempt to set the active record to the correct value.
	 */

	local_node_status = (is_standby(my_local_conn) == 1);

	if (!update_node_record_status(master_conn,
					    local_options.cluster_name,
					    node_info.node_id,
					    "standby",
					    node_info.upstream_node_id,
					    local_node_status))
	{
		log_err(_("unable to set local node %i as %s on master: %s\n"),
				node_info.node_id,
				local_node_status == false ? "inactive" : "active",
				PQerrorMessage(master_conn));

		return false;
	}

	log_notice(_("marking this node (%i) as %s on master\n"),
			   node_info.node_id,
			   local_node_status == false ? "inactive" : "active");

	return true;
}


static void
check_cluster_configuration(PGconn *conn)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];

	log_info(_("checking cluster configuration with schema '%s'\n"), get_repmgr_schema());

	sqlquery_snprintf(sqlquery,
					  "SELECT oid FROM pg_catalog.pg_class "
					  " WHERE oid = '%s.repl_nodes'::regclass ",
					  get_repmgr_schema_quoted(master_conn));

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

		/* XXX use create_node_record() */
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
		if (format_ok != NULL)
			*format_ok = false;
		log_warning(_("incorrect log location format: %s\n"), lsn);
		return 0;
	}

	if (format_ok != NULL)
		*format_ok = true;

	return (XLogRecPtr) ((uint64) xlogid) << 32 | (uint64) xrecoff;
}

void
usage(void)
{
	log_err(_("%s: replication management daemon for PostgreSQL\n"), progname());
	log_err(_("Try \"%s --help\" for more information.\n"), progname());
}


void
help(void)
{
	printf(_("%s: replication management daemon for PostgreSQL\n"), progname());
	printf(_("\n"));
	printf(_("Usage:\n"));
	printf(_("  %s [OPTIONS]\n"), progname());
	printf(_("\n"));
	printf(_("Options:\n"));
	printf(_("  -?, --help                show this help, then exit\n"));
	printf(_("  -V, --version             output version information, then exit\n"));
	printf(_("  -v, --verbose             output verbose activity information\n"));
	printf(_("  -m, --monitoring-history  track advance or lag of the replication in every standby in repl_monitor\n"));
	printf(_("  -f, --config-file=PATH    path to the configuration file\n"));
	printf(_("  -d, --daemonize           detach process from foreground\n"));
	printf(_("  -p, --pid-file=PATH       write a PID file\n"));
	printf(_("\n"));
	printf(_("%s monitors a cluster of servers and optionally performs failover.\n"), progname());
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

	log_info(_("%s terminating...\n"), progname());

	exit(retval);
}


static void
update_shared_memory(char *last_xlog_replay_location)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];

	sprintf(sqlquery,
			"SELECT %s.repmgr_update_standby_location('%s')",
			get_repmgr_schema_quoted(my_local_conn),
			last_xlog_replay_location);

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
	int res;

	t_node_info node_info = T_NODE_INFO_INITIALIZER;

	res = get_node_record(conn, cluster, node_id, &node_info);

	if (res == -1)
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

		PQfinish(conn);
		conn = NULL;

		terminate(ERR_DB_QUERY);
	}

	if (res == 0)
	{
		log_warning(_("No record found for node %i\n"), node_id);
	}

	return node_info;
}
