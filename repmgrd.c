/*
 * repmgrd.c - Replication manager daemon
 * Copyright (C) 2ndQuadrant, 2010-2012
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

/* PostgreSQL's headers needed to export some functionality */
#include "access/xlogdefs.h"
#include "libpq/pqsignal.h"

/*
 * we do not export InvalidXLogRecPtr so we need to define it
 * but since 9.3 it will be defined in xlogdefs.h which we include
 * so better to ask if it's defined to be future proof
 */
#ifndef InvalidXLogRecPtr
const XLogRecPtr InvalidXLogRecPtr = {0, 0};
#endif

#if PG_VERSION_NUM >= 90300
    #define XLAssign(a, b) \
        a = b

    #define XLAssignValue(a, xlogid, xrecoff) \
        a = xrecoff

    #define XLByteLT(a, b) \
        (a < b)

#else
    #define XLAssign(a, b) \
        a.xlogid  = b.xlogid; \
        a.xrecoff = b.xrecoff

    #define XLAssignValue(a, uxlogid, uxrecoff) \
        a.xlogid = uxlogid; \
        a.xrecoff = uxrecoff
#endif


/*
 * Struct to keep info about the nodes, used in the voting process in
 * do_failover()
 */
typedef struct nodeInfo
{
	int nodeId;
	char conninfostr[MAXLEN];
	XLogRecPtr xlog_location;
	bool is_ready;
	bool is_visible;
	bool is_witness;
} nodeInfo;


char    myClusterName[MAXLEN];

/* Local info */
t_configuration_options local_options;
int     myLocalMode = STANDBY_MODE;
PGconn *myLocalConn = NULL;

/* Primary info */
t_configuration_options primary_options;

PGconn *primaryConn = NULL;

char sqlquery[QUERY_STR_LEN];

const char *progname;

char	*config_file = DEFAULT_CONFIG_FILE;
bool	verbose = false;
bool	monitoring_history = false;
char	repmgr_schema[MAXLEN];

bool	failover_done = false;

char	*pid_file = NULL;

/*
 * should initialize with {0} to be ANSI complaint ? but this raises
 * error with gcc -Wall
 */
t_configuration_options config = T_CONFIGURATION_OPTIONS_INITIALIZER;

static void help(const char* progname);
static void usage(void);
static void checkClusterConfiguration(PGconn *conn, PGconn *primary);
static void checkNodeConfiguration(char *conninfo);

static void StandbyMonitor(void);
static void WitnessMonitor(void);
static bool CheckConnection(PGconn *conn, const char *type);
static void update_shared_memory(char *last_wal_standby_applied);
static void update_registration(void);
static void do_failover(void);

static unsigned long long int walLocationToBytes(char *wal_location);

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

static void do_daemonize();
static void check_and_create_pid_file(const char *pid_file);

#define CloseConnections()	\
	if (PQisBusy(primaryConn) == 1) \
		(void) CancelQuery(primaryConn, local_options.master_response_timeout); \
	if (myLocalConn != NULL) \
		PQfinish(myLocalConn);	\
	if (primaryConn != NULL && primaryConn != myLocalConn) \
		PQfinish(primaryConn);


int
main(int argc, char **argv)
{
	static struct option long_options[] =
	{
		{"config", required_argument, NULL, 'f'},
		{"verbose", no_argument, NULL, 'v'},
		{"monitoring-history", no_argument, NULL, 'm'},
		{"daemonize", no_argument, NULL, 'd'},
		{"pid-file", required_argument, NULL, 'p'},
		{NULL, 0, NULL, 0}
	};

	int			optindex;
	int			c, ret;
	bool		daemonize = false;

	char standby_version[MAXVERSIONSTR], *ret_ver;

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

	freopen("/dev/null", "r", stdin);
	freopen("/dev/null", "w", stdout);

	logger_init(&local_options, progname, local_options.loglevel, local_options.logfacility);
	if (verbose)
		logger_min_verbose(LOG_INFO);

	if (log_type == REPMGR_SYSLOG)
	{
		fclose(stderr);
	}

	snprintf(repmgr_schema, MAXLEN, "%s%s", DEFAULT_REPMGR_SCHEMA_PREFIX, local_options.cluster_name);

	log_info(_("%s Connecting to database '%s'\n"), progname, local_options.conninfo);
	myLocalConn = establishDBConnection(local_options.conninfo, true);

	/* should be v9 or better */
	log_info(_("%s Connected to database, checking its state\n"), progname);
	ret_ver = pg_version(myLocalConn, standby_version);
	if (ret_ver == NULL || strcmp(standby_version, "") == 0)
	{
		if(ret_ver != NULL)
			log_err(_("%s needs standby to be PostgreSQL 9.0 or better\n"), progname);
		terminate(ERR_BAD_CONFIG);
	}


	/*
	 * MAIN LOOP
	 * This loops cicles once per failover and at startup
	 * Requisites:
	 *   - myLocalConn needs to be already setted with an active connection
	 *   - no master connection
 	 */
	do
	{
		/*
		 * Set my server mode, establish a connection to primary
		 * and start monitor
		 */
		ret = is_witness(myLocalConn, repmgr_schema, local_options.cluster_name, local_options.node);

		if (ret == 1)
			myLocalMode = WITNESS_MODE;
		else if (ret == 0)
		{
			ret = is_standby(myLocalConn);

			if (ret == 1)
				myLocalMode = STANDBY_MODE;
			else if (ret == 0) /* is the master */
				myLocalMode = PRIMARY_MODE;
		}

        /* XXX we did this before changing is_standby() to return int; we
		 * should not exit at this point, but for now we do until we have a
		 * better strategy */
		if (ret == -1)
			terminate(1);

		switch (myLocalMode)
		{
			case PRIMARY_MODE:
				primary_options.node = local_options.node;
				strncpy(primary_options.conninfo, local_options.conninfo, MAXLEN);
				primaryConn = myLocalConn;

				checkClusterConfiguration(myLocalConn, primaryConn);
				checkNodeConfiguration(local_options.conninfo);

				if (reload_configuration(config_file, &local_options))
				{
					PQfinish(myLocalConn);
					myLocalConn = establishDBConnection(local_options.conninfo, true);
					primaryConn = myLocalConn;
					update_registration();
				}

				log_info(_("%s Starting continuous primary connection check\n"), progname);

				/* Check that primary is still alive, and standbies are sending info */

				/*
				 * Every local_options.monitor_interval_secs seconds, do master checks
				 * XXX
				 * Check that standbies are sending info
				 */
				do
				{
					if (CheckConnection(primaryConn, "master"))
					{
						/*
							CheckActiveStandbiesConnections();
							CheckInactiveStandbies();
						*/
						sleep(local_options.monitor_interval_secs);
					}
					else
					{
						/* XXX
						 * May we do something more verbose ?
						 */
						terminate(1);
					}

					if (got_SIGHUP)
					{
						/* if we can reload, then could need to change myLocalConn */
						if (reload_configuration(config_file, &local_options))
						{
							PQfinish(myLocalConn);
							myLocalConn = establishDBConnection(local_options.conninfo, true);
							primaryConn = myLocalConn;

							if (*local_options.logfile)
							{
								freopen(local_options.logfile, "a", stderr);
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
				primaryConn = getMasterConnection(myLocalConn, repmgr_schema,
				                                  local_options.cluster_name,
				                                  &primary_options.node, NULL);
				if (primaryConn == NULL)
				{
					terminate(ERR_BAD_CONFIG);
				}

				checkClusterConfiguration(myLocalConn, primaryConn);
				checkNodeConfiguration(local_options.conninfo);

				if (reload_configuration(config_file, &local_options))
				{
					PQfinish(myLocalConn);
					myLocalConn = establishDBConnection(local_options.conninfo, true);
					update_registration();
				}

				/*
				 * Every local_options.monitor_interval_secs seconds, do checks
				 */
				if (myLocalMode == WITNESS_MODE)
				{
					log_info(_("%s Starting continuous witness node monitoring\n"), progname);
				}
				else if (myLocalMode == STANDBY_MODE)
				{
					log_info(_("%s Starting continuous standby node monitoring\n"), progname);
				}

				do
				{
					if (myLocalMode == WITNESS_MODE)
						WitnessMonitor();
					else if (myLocalMode == STANDBY_MODE)
						StandbyMonitor();
					sleep(local_options.monitor_interval_secs);

					if (got_SIGHUP)
					{
						/* if we can reload, then could need to change myLocalConn */
						if (reload_configuration(config_file, &local_options))
						{
							PQfinish(myLocalConn);
							myLocalConn = establishDBConnection(local_options.conninfo, true);
							update_registration();
						}
						got_SIGHUP = false;
					}
				} while (!failover_done);
				break;
			default:
				log_err(_("%s: Unrecognized mode for node %d\n"), progname, local_options.node);
		}

		failover_done = false;

	} while (true);

	/* Prevent a double-free */
	if (primaryConn == myLocalConn)
		myLocalConn = NULL;

	/* close the connection to the database and cleanup */
	CloseConnections();

	/* Shuts down logging system */
	logger_shutdown();

	return 0;
}

/*
 *
 */
static void
WitnessMonitor(void)
{
	char monitor_witness_timestamp[MAXLEN];
	PGresult	*res;

	/*
	 * Check if the master is still available, if after 5 minutes of retries
	 * we cannot reconnect, return false.
	 */
	CheckConnection(primaryConn, "master"); /* this take up to local_options.reconnect_attempts * local_options.reconnect_intvl seconds */

	if (PQstatus(primaryConn) != CONNECTION_OK)
	{
		/*
		 * If we can't reconnect, just exit...
		 * XXX we need to make witness connect to the new master
		 */
		terminate(0);
	}

	/* Fast path for the case where no history is requested */
	if (!monitoring_history)
		return;

	/*
	 * Cancel any query that is still being executed,
	 * so i can insert the current record
	 */
	if (!CancelQuery(primaryConn, local_options.master_response_timeout))
		return;
	if (wait_connection_availability(primaryConn, local_options.master_response_timeout) != 1)
		return;

	/* Get local xlog info */
	sqlquery_snprintf(sqlquery, "SELECT CURRENT_TIMESTAMP ");

	res = PQexec(myLocalConn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s\n"), PQerrorMessage(myLocalConn));
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
	                  "VALUES(%d, %d, '%s'::timestamp with time zone, "
	                  " pg_current_xlog_location(), null,  "
	                  " 0, 0)",
	                  repmgr_schema, primary_options.node, local_options.node, monitor_witness_timestamp);

	/*
	 * Execute the query asynchronously, but don't check for a result. We
	 * will check the result next time we pause for a monitor step.
	 */
	log_debug("WitnessMonitor: %s\n", sqlquery);
	if (PQsendQuery(primaryConn, sqlquery) == 0)
		log_warning(_("Query could not be sent to primary. %s\n"),
		            PQerrorMessage(primaryConn));
}


/*
 * Insert monitor info, this is basically the time and xlog replayed,
 * applied on standby and current xlog location in primary.
 * Also do the math to see how far are we in bytes for being uptodate
 */
static void
StandbyMonitor(void)
{
	PGresult *res;
	char monitor_standby_timestamp[MAXLEN];
	char last_wal_primary_location[MAXLEN];
	char last_wal_standby_received[MAXLEN];
	char last_wal_standby_applied[MAXLEN];

	unsigned long long int lsn_primary;
	unsigned long long int lsn_standby_received;
	unsigned long long int lsn_standby_applied;

	int	connection_retries, ret;
	bool did_retry = false;

	/*
	 * Check if the master is still available, if after 5 minutes of retries
	 * we cannot reconnect, try to get a new master.
	 */
	CheckConnection(primaryConn, "master"); /* this take up to local_options.reconnect_attempts * local_options.reconnect_intvl seconds */

	if (!CheckConnection(myLocalConn, "standby"))
	{
		terminate(1);
	}

	if (PQstatus(primaryConn) != CONNECTION_OK)
	{
		if (local_options.failover == MANUAL_FAILOVER)
		{
			log_err(_("We couldn't reconnect to master. Now checking if another node has been promoted.\n"));
			for (connection_retries = 0; connection_retries < 6; connection_retries++)
			{
				primaryConn = getMasterConnection(myLocalConn, repmgr_schema,
				                                  local_options.cluster_name, &primary_options.node, NULL);
				if (PQstatus(primaryConn) == CONNECTION_OK)
				{
					/* Connected, we can continue the process so break the loop */
					log_err(_("Connected to node %d, continue monitoring.\n"), primary_options.node);
					break;
				}
				else
				{
					log_err(_("We haven't found a new master, waiting before retry...\n"));
					/* wait local_options.retry_promote_interval_secs minutes before retries,
					 * after 6 failures (6 * local_options.monitor_interval_secs
					 * seconds) we stop trying */
					sleep(local_options.retry_promote_interval_secs);
				}
			}

			if (PQstatus(primaryConn) != CONNECTION_OK)
			{
				log_err(_("We couldn't reconnect for long enough, exiting...\n"));
				terminate(ERR_DB_CON);
			}
		}
		else if (local_options.failover == AUTOMATIC_FAILOVER)
		{
			/*
			 * When we returns from this function we will have a new primary and
			 * a new primaryConn
			 */
			do_failover();
			return;
		}
	}

	/* Check if we still are a standby, we could have been promoted */
	do {
		ret = is_standby(myLocalConn);

		switch (ret)
		{
		case 0:
			log_err(_("It seems like we have been promoted, so exit from monitoring...\n"));
			terminate(1);
			break;

		case -1:
			log_err(_("Standby node disappeared, trying to reconnect...\n"));
			did_retry = true;

			if (!CheckConnection(myLocalConn, "standby"))
			{
				terminate(0);
			}

			break;
		}
	} while(ret == -1);

	if (did_retry)
	{
		log_info(_("standby connection got back up again!\n"));
	}

	/* Fast path for the case where no history is requested */
	if (!monitoring_history)
		return;

	/*
	 * Cancel any query that is still being executed,
	 * so i can insert the current record
	 */
	if (!CancelQuery(primaryConn, local_options.master_response_timeout))
		return;
	if (wait_connection_availability(primaryConn, local_options.master_response_timeout) != 1)
		return;

	/* Get local xlog info */
	sqlquery_snprintf(
	    sqlquery,
	    "SELECT CURRENT_TIMESTAMP, pg_last_xlog_receive_location(), "
	    "pg_last_xlog_replay_location()");

	res = PQexec(myLocalConn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s\n"), PQerrorMessage(myLocalConn));
		PQclear(res);
		/* if there is any error just let it be and retry in next loop */
		return;
	}

	strncpy(monitor_standby_timestamp, PQgetvalue(res, 0, 0), MAXLEN);
	strncpy(last_wal_standby_received , PQgetvalue(res, 0, 1), MAXLEN);
	strncpy(last_wal_standby_applied , PQgetvalue(res, 0, 2), MAXLEN);
	PQclear(res);

	/* Get primary xlog info */
	sqlquery_snprintf(sqlquery, "SELECT pg_current_xlog_location() ");

	res = PQexec(primaryConn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s\n"), PQerrorMessage(primaryConn));
		PQclear(res);
		return;
	}

	strncpy(last_wal_primary_location, PQgetvalue(res, 0, 0), MAXLEN);
	PQclear(res);

	/* Calculate the lag */
	lsn_primary = walLocationToBytes(last_wal_primary_location);
	lsn_standby_received = walLocationToBytes(last_wal_standby_received);
	lsn_standby_applied = walLocationToBytes(last_wal_standby_applied);

	/*
	 * Build the SQL to execute on primary
	 */
	sqlquery_snprintf(sqlquery,
	                  "INSERT INTO %s.repl_monitor "
	                  "VALUES(%d, %d, '%s'::timestamp with time zone, "
	                  " '%s', '%s', "
	                  " %lld, %lld)", repmgr_schema,
	                  primary_options.node, local_options.node, monitor_standby_timestamp,
	                  last_wal_primary_location,
	                  last_wal_standby_received,
	                  (lsn_primary - lsn_standby_received),
	                  (lsn_standby_received - lsn_standby_applied));

	/*
	 * Execute the query asynchronously, but don't check for a result. We
	 * will check the result next time we pause for a monitor step.
	 */
	log_debug("StandbyMonitor: %s\n", sqlquery);
	if (PQsendQuery(primaryConn, sqlquery) == 0)
		log_warning(_("Query could not be sent to primary. %s\n"),
		            PQerrorMessage(primaryConn));
}


static void
do_failover(void)
{
	PGresult *res;
	char 	sqlquery[8192];

	int		total_nodes = 0;
	int		visible_nodes = 0;
	int     ready_nodes = 0;

	bool	find_best = false;
	bool	witness = false;

	int		i;
	int		r;

	uint32  uxlogid;
	uint32  uxrecoff;
	XLogRecPtr xlog_recptr;

	char last_wal_standby_applied[MAXLEN];

	PGconn	*nodeConn = NULL;

	/*
	 * will get info about until 50 nodes,
	 * which seems to be large enough for most scenarios
	 */
	nodeInfo nodes[50];

	/* initialize to keep compiler quiet */
	nodeInfo best_candidate = {-1, "", InvalidXLogRecPtr, false, false, false};

	/* get a list of standby nodes, including myself */
	sprintf(sqlquery, "SELECT id, conninfo, witness "
	        "  FROM %s.repl_nodes "
	        " WHERE cluster = '%s' "
	        " ORDER BY priority, id ",
	        repmgr_schema, local_options.cluster_name);

	res = PQexec(myLocalConn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Can't get nodes' info: %s\n"), PQerrorMessage(myLocalConn));
		PQclear(res);
		terminate(ERR_DB_QUERY);
	}

	/*
	 * total nodes that are registered
	 */
	total_nodes = PQntuples(res);
	log_debug(_("%s: there are %d nodes registered\n"), progname, total_nodes);

	/* Build an array with the nodes and indicate which ones are visible and ready */
	for (i = 0; i < total_nodes; i++)
	{
		nodes[i].nodeId = atoi(PQgetvalue(res, i, 0));
		strncpy(nodes[i].conninfostr, PQgetvalue(res, i, 1), MAXLEN);
		nodes[i].is_witness = (strcmp(PQgetvalue(res, i, 2), "t") == 0) ? true : false;

		/* Initialize on false so if we can't reach this node we know that later */
		nodes[i].is_visible = false;
		nodes[i].is_ready = false;

		XLAssignValue(nodes[i].xlog_location, 0, 0);

		log_debug(_("%s: node=%d conninfo=\"%s\" witness=%s\n"),
					progname, nodes[i].nodeId, nodes[i].conninfostr, (nodes[i].is_witness) ? "true" : "false");

		nodeConn = establishDBConnection(nodes[i].conninfostr, false);

		/* if we can't see the node just skip it */
		if (PQstatus(nodeConn) != CONNECTION_OK)
			continue;

		visible_nodes++;
		nodes[i].is_visible = true;

		PQfinish(nodeConn);
	}
	PQclear(res);

	log_debug(_("Total nodes counted: registered=%d, visible=%d\n"), total_nodes, visible_nodes);

	/*
	 * am i on the group that should keep alive?
	 * if i see less than half of total_nodes then i should do nothing
	 */
	if (visible_nodes < (total_nodes / 2.0))
	{
		log_err(_("Can't reach most of the nodes.\n"
		          "Let the other standby servers decide which one will be the primary.\n"
		          "Manual action will be needed to readd this node to the cluster.\n"));
		terminate(ERR_FAILOVER_FAIL);
	}

	/* Query all the nodes to determine which ones are ready */
	for (i = 0; i < total_nodes; i++)
	{
		/* if the node is not visible, skip it */
		if (!nodes[i].is_visible)
			continue;

		if (nodes[i].is_witness)
			continue;

		nodeConn = establishDBConnection(nodes[i].conninfostr, false);
		/* XXX
		 * This shouldn't happen, if this happens it means this is a major problem
		 * maybe network outages? anyway, is better for a human to react
		 */
		if (PQstatus(nodeConn) != CONNECTION_OK)
		{
			log_err(_("It seems new problems are arising, manual intervention is needed\n"));
			terminate(ERR_FAILOVER_FAIL);
		}

		uxlogid = 0;
		uxrecoff = 0;

		sqlquery_snprintf(sqlquery, "SELECT pg_last_xlog_receive_location()");
		res = PQexec(nodeConn, sqlquery);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			log_info(_("Can't get node's last standby location: %s\n"), PQerrorMessage(nodeConn));
			log_info(_("Connection details: %s\n"), nodes[i].conninfostr);
			PQclear(res);
			PQfinish(nodeConn);
			terminate(ERR_FAILOVER_FAIL);
		}

		if (sscanf(PQgetvalue(res, 0, 0), "%X/%X", &uxlogid, &uxrecoff) != 2)
			log_info(_("could not parse transaction log location \"%s\"\n"), PQgetvalue(res, 0, 0));

		log_debug("XLog position of node %d: log id=%u (%X), offset=%u (%X)\n",
					nodes[i].nodeId, uxlogid, uxlogid, uxrecoff, uxrecoff);

		/* If position is 0/0, error */
		if (uxlogid == 0 && uxrecoff == 0)
		{
			PQclear(res);
			PQfinish(nodeConn);
			log_info(_("InvalidXLogRecPtr detected in a standby\n"));
			terminate(ERR_FAILOVER_FAIL);
		}

		XLAssignValue(nodes[i].xlog_location, uxlogid, uxrecoff);

		PQclear(res);
		PQfinish(nodeConn);
	}

	/* last we get info about this node, and update shared memory */
	sprintf(sqlquery, "SELECT pg_last_xlog_receive_location()");
	res = PQexec(myLocalConn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s.\nReport an invalid value to not be considered as new primary and exit.\n"), PQerrorMessage(myLocalConn));
		PQclear(res);
		sprintf(last_wal_standby_applied, "'%X/%X'", 0, 0);
		update_shared_memory(last_wal_standby_applied);
		terminate(ERR_DB_QUERY);
	}

	/* write last location in shared memory */
	update_shared_memory(PQgetvalue(res, 0, 0));
	PQclear(res);

	for (i = 0; i < total_nodes; i++)
	{
		while (!nodes[i].is_ready)
		{
			/*
        	 * the witness will always be masked as ready if it's still
        	 * not marked that way and avoid a useless query
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
			if (nodes[i].is_ready)
				break;

            nodeConn = establishDBConnection(nodes[i].conninfostr, false);
            /* XXX
             * This shouldn't happen, if this happens it means this is a major problem
             * maybe network outages? anyway, is better for a human to react
             */
            if (PQstatus(nodeConn) != CONNECTION_OK)
            {
				/* XXX */
                log_info(_("At this point, it could be some race conditions that are acceptable, assume the node is restarting and starting failover procedure\n"));
				break;
            }

			uxlogid = 0;
			uxrecoff = 0;

			sqlquery_snprintf(sqlquery, "SELECT %s.repmgr_get_last_standby_location()", repmgr_schema);
			res = PQexec(nodeConn, sqlquery);
            if (PQresultStatus(res) != PGRES_TUPLES_OK)
            {
                log_err(_("PQexec failed: %s.\nReport an invalid value to not be considered as new primary and exit.\n"), PQerrorMessage(nodeConn));
                PQclear(res);
                PQfinish(nodeConn);
                terminate(ERR_DB_QUERY);
			}

            if (sscanf(PQgetvalue(res, 0, 0), "%X/%X", &uxlogid, &uxrecoff) != 2)
			{
                log_info(_("could not parse transaction log location \"%s\"\n"), PQgetvalue(res, 0, 0));

				/* we can't do anything but fail at this point... */
				if (*PQgetvalue(res, 0, 0) == '\0')
				{
					log_crit("Whoops, seems as if shared_preload_libraries=repmgr_funcs is not set!\n");
					exit(ERR_BAD_CONFIG);
				}
			}


            PQclear(res);
            PQfinish(nodeConn);
            /* If position is 0/0, keep checking */
            if (uxlogid == 0 && uxrecoff == 0)
                continue;

			XLAssignValue(xlog_recptr, uxlogid, uxrecoff);

			if (XLByteLT(nodes[i].xlog_location, xlog_recptr))
			{
				XLAssignValue(nodes[i].xlog_location, uxlogid, uxrecoff);
			}

            log_debug("Last XLog position of node %d: log id=%u (%X), offset=%u (%X)\n",
                        nodes[i].nodeId, uxlogid,  uxlogid,
										 uxrecoff, uxrecoff);

			ready_nodes++;
			nodes[i].is_ready = true;
		}
	}

	/* Close the connection to this server */
	PQfinish(myLocalConn);

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

		if (!find_best)
		{
			/* start with the first ready node, and then move on to the next one */
			best_candidate.nodeId                = nodes[i].nodeId;
			XLAssign(best_candidate.xlog_location, nodes[i].xlog_location);
			best_candidate.is_ready              = nodes[i].is_ready;
			best_candidate.is_witness            = nodes[i].is_witness;
			find_best = true;
		}

		/* we use the macros provided by xlogdefs.h to compare XLogRecPtr */
		/*
		 * Nodes are retrieved ordered by priority, so if the current
		 * best candidate is lower than the next node's wal location
		 * then assign next node as the new best candidate.
		 */
		if (XLByteLT(best_candidate.xlog_location, nodes[i].xlog_location))
		{
			best_candidate.nodeId                = nodes[i].nodeId;
			XLAssign(best_candidate.xlog_location, nodes[i].xlog_location);
			best_candidate.is_ready              = nodes[i].is_ready;
			best_candidate.is_witness            = nodes[i].is_witness;
		}
	}

	/* once we know who is the best candidate, promote it */
	if (find_best && (best_candidate.nodeId == local_options.node))
	{
		if (best_candidate.is_witness)
		{
			log_err(_("%s: Node selected as new master is a witness. Can't be promoted.\n"), progname);
			terminate(ERR_FAILOVER_FAIL);
		}

		/* wait */
		sleep(5);

		if (verbose)
			log_info(_("%s: This node is the best candidate to be the new primary, promoting...\n"),
			         progname);
		log_debug(_("promote command is: \"%s\"\n"), local_options.promote_command);

		if (log_type == REPMGR_STDERR && *local_options.logfile)
		{
			fflush(stderr);
			fsync(fileno(stderr));
		}

		r = system(local_options.promote_command);
		if (r != 0)
		{
			log_err(_("%s: promote command failed. You could check and try it manually.\n"), progname);
			terminate(ERR_BAD_CONFIG);
		}
	}
	else if (find_best)
	{
		/* wait */
		sleep(10);

		if (verbose)
			log_info(_("%s: Node %d is the best candidate to be the new primary, we should follow it...\n"),
			         progname, best_candidate.nodeId);
		log_debug(_("follow command is: \"%s\"\n"), local_options.follow_command);
		/*
		 * New Primary need some time to be promoted.
		 * The follow command should take care of that.
		 */
		if (log_type == REPMGR_STDERR && *local_options.logfile)
		{
			fflush(stderr);
			fsync(fileno(stderr));
		}

		r = system(local_options.follow_command);
		if (r != 0)
		{
			log_err(_("%s: follow command failed. You could check and try it manually.\n"), progname);
			terminate(ERR_BAD_CONFIG);
		}
	}
	else
	{
		log_err(_("%s: Did not find candidates. You should check and try manually.\n"), progname);
		terminate(ERR_FAILOVER_FAIL);
	}

	/* to force it to re-calculate mode and master node */
	failover_done = true;

	/* and reconnect to the local database */
	myLocalConn = establishDBConnection(local_options.conninfo, true);
}


static bool
CheckConnection(PGconn *conn, const char *type)
{
	int	connection_retries;

	/*
	 * Check if the master is still available
	 * if after local_options.reconnect_attempts * local_options.reconnect_intvl seconds of retries
	 * we cannot reconnect
	 * return false
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
			if ( connection_retries > 0)
			{
				log_info(_("%s: Connection to %s has been restored.\n"), progname, type);
			}
			return true;
		}
	}
	if (!is_pgup(conn, local_options.master_response_timeout))
	{
		log_err(_("%s: We couldn't reconnect for long enough, exiting...\n"), progname);
		/* XXX Anything else to do here? */
		return false;
	}
	return true;
}


static void
checkClusterConfiguration(PGconn *conn, PGconn *primary)
{
	PGresult   *res;

	log_info(_("%s Checking cluster configuration with schema '%s'\n"),
	         progname, repmgr_schema);
	sqlquery_snprintf(sqlquery, "SELECT oid FROM pg_class "
	                  " WHERE oid = '%s.repl_nodes'::regclass",
	                  repmgr_schema);
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
checkNodeConfiguration(char *conninfo)
{
	PGresult   *res;

	/*
	 * Check if we have my node information in repl_nodes
	 */
	log_info(_("%s Checking node %d in cluster '%s'\n"),
	         progname, local_options.node, local_options.cluster_name);
	sqlquery_snprintf(sqlquery, "SELECT * FROM %s.repl_nodes "
	                  " WHERE id = %d AND cluster = '%s' ",
	                  repmgr_schema, local_options.node,
	                  local_options.cluster_name);

	res = PQexec(myLocalConn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s\n"), PQerrorMessage(myLocalConn));
		PQclear(res);
		terminate(ERR_BAD_CONFIG);
	}

	/*
	 * If there isn't any results then we have not configured this node yet
	 * in repmgr, if that is the case we will insert the node to the cluster,
	 * except if it is a witness
	 */
	if (PQntuples(res) == 0)
	{
		PQclear(res);

		if (myLocalMode == WITNESS_MODE)
		{
			log_err(_("The witness is not configured\n"));
			terminate(ERR_BAD_CONFIG);
		}

		/* Adding the node */
		log_info(_("%s Adding node %d to cluster '%s'\n"),
		         progname, local_options.node, local_options.cluster_name);
		sqlquery_snprintf(sqlquery, "INSERT INTO %s.repl_nodes "
		                  "VALUES (%d, '%s', '%s', '%s', 0, 'f')",
		                  repmgr_schema, local_options.node,
		                  local_options.cluster_name,
		                  local_options.node_name,
		                  local_options.conninfo);

		if (!PQexec(primaryConn, sqlquery))
		{
			log_err(_("Cannot insert node details, %s\n"),
			        PQerrorMessage(primaryConn));
			terminate(ERR_BAD_CONFIG);
		}
	}
	else
	{
		PQclear(res);
	}
}


static unsigned long long int
walLocationToBytes(char *wal_location)
{
	unsigned int xlogid;
	unsigned int xrecoff;

	if (sscanf(wal_location, "%X/%X", &xlogid, &xrecoff) != 2)
	{
		log_err(_("wrong log location format: %s\n"), wal_location);
		return 0;
	}
	return (( (long long) xlogid * 16 * 1024 * 1024 * 255) + xrecoff);
}


void usage(void)
{
	log_err(_("%s: Replicator manager daemon \n"), progname);
	log_err(_("Try \"%s --help\" for more information.\n"), progname);
}


void help(const char *progname)
{
	printf(_("Usage: %s [OPTIONS]\n"), progname);
	printf(_("Replicator manager daemon for PostgreSQL.\n"));
	printf(_("\nOptions:\n"));
	printf(_("  --help                    show this help, then exit\n"));
	printf(_("  --version                 output version information, then exit\n"));
	printf(_("  --verbose                 output verbose activity information\n"));
	printf(_("  --monitoring-history      track advance or lag of the replication in every standby in repl_monitor\n"));
	printf(_("  -f, --config_file=PATH    configuration file\n"));
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
	CloseConnections();
	logger_shutdown();

	if (pid_file)
	{
		unlink(pid_file);
	}

	exit(retval);
}


static void
update_shared_memory(char *last_wal_standby_applied)
{
	PGresult *res;

	sprintf(sqlquery, "SELECT %s.repmgr_update_standby_location('%s')",
	        repmgr_schema, last_wal_standby_applied);

	/* If an error happens, just inform about that and continue */
	res = PQexec(myLocalConn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_warning(_("Cannot update this standby's shared memory: %s\n"), PQerrorMessage(myLocalConn));
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
	PGresult *res;

	sqlquery_snprintf(sqlquery, "UPDATE %s.repl_nodes "
	                  "   SET conninfo = '%s', "
	                  "       priority = %d "
	                  " WHERE id = %d",
	                  repmgr_schema, local_options.conninfo, local_options.priority, local_options.node);

	res = PQexec(primaryConn, sqlquery);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("Cannot update registration: %s\n"), PQerrorMessage(primaryConn));
		terminate(ERR_DB_CON);
	}
	PQclear(res);
}

static void
do_daemonize()
{
	pid_t pid = fork();
	switch (pid)
	{
	case -1:
		log_err("Error in fork(): %s\n", strerror(errno));
		exit(ERR_SYS_FAILURE);
		break;

	case 0: /* child process */
		pid = setsid();
		if (pid == (pid_t)-1)
		{
			log_err("Error in setsid(): %s\n", strerror(errno));
			exit(ERR_SYS_FAILURE);
		}

		/* ensure that we are no longer able to open a terminal */
		pid = fork();

		if(pid == -1) /* error case */
		{
			log_err("Error in fork(): %s\n", strerror(errno));
			exit(ERR_SYS_FAILURE);
			break;
		}

		if (pid != 0) /* parent process */
		{
			exit(0);
		}

		/* a child just flows along */

		break;

	default: /* parent process */
		exit(0);
	}
}

static void
check_and_create_pid_file(const char *pid_file)
{
	struct stat st;
	FILE *fd;
	char buff[MAXLEN];
	pid_t pid;

	if (stat(pid_file, &st) != -1)
	{
		memset(buff, 0, MAXLEN);

		fd = fopen(pid_file, "r");

		if (fd == NULL)
		{
			log_err("PID file %s exists but could not opened for reading. If repmgrd is no longer alive remove the file and restart repmgrd.\n", pid_file);
			exit(ERR_BAD_CONFIG);
		}

		fread(buff, MAXLEN - 1, 1, fd);
		fclose(fd);

		pid = atoi(buff);

		if (pid != 0)
		{
			if (kill(pid, 0) != -1)
			{
				log_err("PID file %s exists and seems to contain a valid PID. If repmgrd is no longer alive remove the file and restart repmgrd.\n", pid_file);
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
