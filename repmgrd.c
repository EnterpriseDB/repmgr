/*
 * repmgrd.c - Replication manager daemon
 * Copyright (C) 2ndQuadrant, 2010-2011
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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "repmgr.h"
#include "config.h"
#include "log.h"
#include "strutil.h"

#include "access/xlogdefs.h"
#include "libpq/pqsignal.h"

/*
 * Struct to keep info about the nodes, used in the voting process in
 * do_failover()
 */
typedef struct nodeInfo
{
	int nodeId;
	XLogRecPtr xlog_location;
	bool is_ready;
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
char	repmgr_schema[MAXLEN];

/*
 * should initialize with {0} to be ANSI complaint ? but this raises
 * error with gcc -Wall
 */
t_configuration_options config = {};

static void help(const char* progname);
static void usage(void);
static void checkClusterConfiguration(PGconn *conn, PGconn *primary);
static void checkNodeConfiguration(char *conninfo);
static void CancelQuery(void);

static void StandbyMonitor(void);
static void WitnessMonitor(void);
static bool CheckPrimaryConnection(void);
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
static void setup_event_handlers(void);

#define CloseConnections()	\
	if (PQisBusy(primaryConn) == 1) \
		CancelQuery(); \
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
		{NULL, 0, NULL, 0}
	};

	int			optindex;
	int			c;

	char standby_version[MAXVERSIONSTR];

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
			printf("%s (PostgreSQL) " PG_VERSION "\n", progname);
			exit(SUCCESS);
		}
	}

	while ((c = getopt_long(argc, argv, "f:v", long_options, &optindex)) != -1)
	{
		switch (c)
		{
		case 'f':
			config_file = optarg;
			break;
		case 'v':
			verbose = true;
			break;
		default:
			usage();
			exit(ERR_BAD_CONFIG);
		}
	}

	setup_event_handlers();

	/*
	 * Read the configuration file: repmgr.conf
	 */
	parse_config(config_file, &local_options);
	if (local_options.node == -1)
	{
		log_err(_("Node information is missing. "
		          "Check the configuration file, or provide one if you have not done so.\n"));
		exit(ERR_BAD_CONFIG);
	}

	logger_init(progname, local_options.loglevel, local_options.logfacility);
	if (verbose)
		logger_min_verbose(LOG_INFO);

	snprintf(repmgr_schema, MAXLEN, "%s%s", DEFAULT_REPMGR_SCHEMA_PREFIX, local_options.cluster_name);

	log_info(_("%s Connecting to database '%s'\n"), progname, local_options.conninfo);
	myLocalConn = establishDBConnection(local_options.conninfo, true);

	/* should be v9 or better */
	log_info(_("%s Connected to database, checking its state\n"), progname);
	pg_version(myLocalConn, standby_version);
	if (strcmp(standby_version, "") == 0)
	{
		PQfinish(myLocalConn);
		log_err(_("%s needs standby to be PostgreSQL 9.0 or better\n"), progname);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Set my server mode, establish a connection to primary
	 * and start monitor
	 */
	if (is_witness(myLocalConn, repmgr_schema, local_options.cluster_name, local_options.node))
		myLocalMode = WITNESS_MODE;
	else if (is_standby(myLocalConn))
		myLocalMode = STANDBY_MODE;
	else /* is the master */
		myLocalMode = PRIMARY_MODE;

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
			update_registration();
		}

		log_info(_("%s Starting continuous primary connection check\n"), progname);
		/* Check that primary is still alive, and standbies are sending info */
		/*
		 * Every SLEEP_MONITOR seconds, do master checks
		 * XXX
		 * Check that standbies are sending info
		*/
		for (;;)
		{
			if (CheckPrimaryConnection())
			{
				/*
									CheckActiveStandbiesConnections();
									CheckInactiveStandbies();
				*/
				sleep(SLEEP_MONITOR);
			}
			else
			{
				/* XXX
				 * May we do something more verbose ?
				 */
				exit (1);
			}

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
		}
		break;
	case WITNESS_MODE:
	case STANDBY_MODE:
		/* I need the id of the primary as well as a connection to it */
		log_info(_("%s Connecting to primary for cluster '%s'\n"),
		         progname, local_options.cluster_name);
		primaryConn = getMasterConnection(myLocalConn, repmgr_schema, local_options.node,
		                                  local_options.cluster_name,
		                                  &primary_options.node, NULL);
		if (primaryConn == NULL)
		{
			CloseConnections();
			exit(ERR_BAD_CONFIG);
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
		 * Every SLEEP_MONITOR seconds, do checks
		 */
		if (myLocalMode == WITNESS_MODE)
		{
			log_info(_("%s Starting continuous witness node monitoring\n"), progname);
		}
		else if (myLocalMode == STANDBY_MODE)
		{
			log_info(_("%s Starting continuous standby node monitoring\n"), progname);
		}

		for (;;)
		{
			if (myLocalMode == WITNESS_MODE)
				WitnessMonitor();
			else if (myLocalMode == STANDBY_MODE)
				StandbyMonitor();
			sleep(SLEEP_MONITOR);

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
		}
		break;
	default:
		log_err(_("%s: Unrecognized mode for node %d\n"), progname, local_options.node);
	}

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
	CheckPrimaryConnection(); // this take up to NUM_RETRY * SLEEP_RETRY seconds

	if (PQstatus(primaryConn) != CONNECTION_OK)
	{
		/*
		 * If we can't reconnect, just exit...
		 * XXX we need to make witness connect to the new master
		 */
		PQfinish(myLocalConn);
		exit(0);
	}

	/*
	 * first check if there is a command being executed,
	 * and if that is the case, cancel the query so i can
	 * insert the current record
	 */
	if (PQisBusy(primaryConn) == 1)
		CancelQuery();

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

	int	connection_retries;

	/*
	 * Check if the master is still available, if after 5 minutes of retries
	 * we cannot reconnect, try to get a new master.
	 */
	CheckPrimaryConnection(); // this take up to NUM_RETRY * SLEEP_RETRY seconds

	if (PQstatus(primaryConn) != CONNECTION_OK)
	{
		if (local_options.failover == MANUAL_FAILOVER)
		{
			log_err(_("We couldn't reconnect to master. Now checking if another node has been promoted.\n"));
			for (connection_retries = 0; connection_retries < 6; connection_retries++)
			{
				primaryConn = getMasterConnection(myLocalConn, repmgr_schema, local_options.node,
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
					/* wait 5 minutes before retries, after 6 failures (30 minutes) we stop trying */
					sleep(300);
				}
			}

			if (PQstatus(primaryConn) != CONNECTION_OK)
			{
				log_err(_("We couldn't reconnect for long enough, exiting...\n"));
				exit(ERR_DB_CON);
			}
		}
		else if (local_options.failover == AUTOMATIC_FAILOVER)
		{
			/*
			 * When we returns from this function we will have a new primary and
			 * a new primaryConn
			 */
			do_failover();
		}
	}

	/* Check if we still are a standby, we could have been promoted */
	if (!is_standby(myLocalConn))
	{
		log_err(_("It seems like we have been promoted, so exit from monitoring...\n"));
		CloseConnections();
		exit(ERR_PROMOTED);
	}

	/*
	 * first check if there is a command being executed,
	 * and if that is the case, cancel the query so i can
	 * insert the current record
	 */
	if (PQisBusy(primaryConn) == 1)
		CancelQuery();

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
	if (PQsendQuery(primaryConn, sqlquery) == 0)
		log_warning(_("Query could not be sent to primary. %s\n"),
		            PQerrorMessage(primaryConn));
}


static void
do_failover(void)
{
	PGresult *res1;
	PGresult *res2;
	char 	sqlquery[8192];

	int		total_nodes = 0;
	int		visible_nodes = 0;
	bool	find_best = false;

	int		i;
	int		r;

	int 	node;
	char	nodeConninfo[MAXLEN];

	unsigned int uxlogid;
	unsigned int uxrecoff;
	char last_wal_standby_applied[MAXLEN];

	PGconn	*nodeConn = NULL;

	/*
	 * will get info about until 50 nodes,
	 * which seems to be large enough for most scenarios
	 */
	nodeInfo nodes[50];
	nodeInfo best_candidate;

	/* first we get info about this node, and update shared memory */
	sprintf(sqlquery, "SELECT pg_last_xlog_replay_location()");
	res1 = PQexec(myLocalConn, sqlquery);
	if (PQresultStatus(res1) != PGRES_TUPLES_OK)
	{
		log_err(_("PQexec failed: %s.\nReport an invalid value to not be considered as new primary and exit.\n"), PQerrorMessage(myLocalConn));
		PQclear(res1);
		sprintf(last_wal_standby_applied, "'%X/%X'", 0, 0);
		update_shared_memory(last_wal_standby_applied);
		exit(ERR_DB_QUERY);
	}

	/* write last location in shared memory */
	update_shared_memory(PQgetvalue(res1, 0, 0));

	/*
	 * we sleep the monitor time + one second
	 * we bet it should be enough for other repmgrd to update their own data
	 */
	sleep(SLEEP_MONITOR + 1);

	/* get a list of standby nodes, including myself */
	sprintf(sqlquery, "SELECT * "
	        "  FROM %s.repl_nodes "
	        " WHERE id IN (SELECT standby_node FROM %s.repl_status) "
	        "   AND cluster = '%s' "
	        " ORDER BY priority ",
	        repmgr_schema, repmgr_schema, local_options.cluster_name);

	res1 = PQexec(myLocalConn, sqlquery);
	if (PQresultStatus(res1) != PGRES_TUPLES_OK)
	{
		log_err(_("Can't get nodes info: %s\n"), PQerrorMessage(myLocalConn));
		PQclear(res1);
		PQfinish(myLocalConn);
		exit(ERR_DB_QUERY);
	}

	/* ask for the locations */
	for (i = 0; i < PQntuples(res1); i++)
	{
		node = atoi(PQgetvalue(res1, i, 0));
		/* Initialize on false so if we can't reach this node we know that later */
		nodes[i].is_ready = false;
		strncpy(nodeConninfo, PQgetvalue(res1, i, 2), MAXLEN);
		nodeConn = establishDBConnection(nodeConninfo, false);
		/* if we can't see the node just skip it */
		if (PQstatus(nodeConn) != CONNECTION_OK)
			continue;

		sqlquery_snprintf(sqlquery, "SELECT repmgr_get_last_standby_location()");
		res2 = PQexec(nodeConn, sqlquery);
		if (PQresultStatus(res2) != PGRES_TUPLES_OK)
		{
			log_info(_("Can't get node's last standby location: %s\n"), PQerrorMessage(nodeConn));
			log_info(_("Connection details: %s\n"), nodeConninfo);
			PQclear(res2);
			PQfinish(nodeConn);
			continue;
		}

		visible_nodes++;

		if (sscanf(PQgetvalue(res2, 0, 0), "%X/%X", &uxlogid, &uxrecoff) != 2)
			log_info(_("could not parse transaction log location \"%s\"\n"), PQgetvalue(res2, 0, 0));

		nodes[i].nodeId = node;
		nodes[i].xlog_location.xlogid = uxlogid;
		nodes[i].xlog_location.xrecoff = uxrecoff;
		nodes[i].is_ready = true;

		PQclear(res2);
		PQfinish(nodeConn);
	}
	PQclear(res1);
	/* Close the connection to this server */
	PQfinish(myLocalConn);

	/*
	 * total nodes that are registered, include master which is a node but was
	 * not counted because it's not a standby
	 */
	total_nodes = i + 1;

	/*
	 * am i on the group that should keep alive?
	 * if i see less than half of total_nodes then i should do nothing
	 */
	if (visible_nodes < (total_nodes / 2.0))
	{
		log_err(_("Can't reach most of the nodes.\n"
		          "Let the other standby servers decide which one will be the primary.\n"
		          "Manual action will be needed to readd this node to the cluster.\n"));
		exit(ERR_FAILOVER_FAIL);
	}

	/*
	 * determine which one is the best candidate to promote to primary
	 */
	for (i = 0; i < total_nodes - 1; i++)
	{
		if (!nodes[i].is_ready)
			continue;
		else if (!find_best)
		{
			/* start with the first ready node, and then move on to the next one */
			best_candidate.nodeId                = nodes[i].nodeId;
			best_candidate.xlog_location.xlogid  = nodes[i].xlog_location.xlogid;
			best_candidate.xlog_location.xrecoff = nodes[i].xlog_location.xrecoff;
			best_candidate.is_ready              = nodes[i].is_ready;
			find_best = true;
		}

		/* we use the macros provided by xlogdefs.h to compare XLogPtr */
		/*
		 * Nodes are retrieved ordered by priority, so if the current
		 * best candidate is lower or equal to the next node's wal location
		 * then assign next node as the new best candidate.
		 */
		if (XLByteLE(best_candidate.xlog_location, nodes[i].xlog_location))
		{
			best_candidate.nodeId                = nodes[i].nodeId;
			best_candidate.xlog_location.xlogid  = nodes[i].xlog_location.xlogid;
			best_candidate.xlog_location.xrecoff = nodes[i].xlog_location.xrecoff;
			best_candidate.is_ready              = nodes[i].is_ready;
		}
	}

	/* once we know who is the best candidate, promote it */
	if (find_best && (best_candidate.nodeId == local_options.node))
	{
		if (verbose)
			log_info(_("%s: This node is the best candidate to be the new primary, promoting...\n"),
			         progname);
		log_debug(_("promote command is: \"%s\"\n"), local_options.promote_command);
		r = system(local_options.promote_command);
		if (r != 0)
		{
			log_err(_("%s: promote command failed. You could check and try it manually.\n"), progname);
			exit(ERR_BAD_CONFIG);
		}
	}
	else if (find_best)
	{
		if (verbose)
			log_info(_("%s: Node %d is the best candidate to be the new primary, we should follow it...\n"),
			         progname, best_candidate.nodeId);
		log_debug(_("follow command is: \"%s\"\n"), local_options.follow_command);
		/*
		 * New Primary need some time to be promoted.
		 * The follow command should take care of that.
		 */
		r = system(local_options.follow_command);
		if (r != 0)
		{
			log_err(_("%s: follow command failed. You could check and try it manually.\n"), progname);
			exit(ERR_BAD_CONFIG);
		}
	}
	else
	{
		log_err(_("%s: Did not find candidates. You should check and try manually.\n"), progname);
		exit(ERR_FAILOVER_FAIL);
	}

	/* and reconnect to the local database */
	myLocalConn = establishDBConnection(local_options.conninfo, true);
}


static bool
CheckPrimaryConnection(void)
{
	int	connection_retries;

	/*
	 * Check if the master is still available
	 * if after NUM_RETRY * SLEEP_RETRY seconds of retries
	 * we cannot reconnect
	 * return false
	 */
	for (connection_retries = 0; connection_retries < NUM_RETRY; connection_retries++)
	{
		if (!is_pgup(primaryConn))
		{
			log_warning(_("%s: Connection to master has been lost, trying to recover... %i seconds before failover decision\n"), progname, (SLEEP_RETRY*(NUM_RETRY-connection_retries)));
			/* wait SLEEP_RETRY seconds between retries */
			sleep(SLEEP_RETRY);
		}
		else
		{
			log_info(_("%s: Connection to master has been restored.\n"), progname);
			break;
		}
	}
	if (!is_pgup(primaryConn))
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
		CloseConnections();
		exit(ERR_DB_QUERY);
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
		CloseConnections();
		exit(ERR_BAD_CONFIG);
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
		CloseConnections();
		exit(ERR_BAD_CONFIG);
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
			CloseConnections();
			exit(ERR_BAD_CONFIG);
		}

		/* Adding the node */
		log_info(_("%s Adding node %d to cluster '%s'\n"),
		         progname, local_options.node, local_options.cluster_name);
		sqlquery_snprintf(sqlquery, "INSERT INTO %s.repl_nodes "
		                  "VALUES (%d, '%s', '%s', 'f')",
		                  repmgr_schema, local_options.node,
		                  local_options.cluster_name,
		                  local_options.conninfo);

		if (!PQexec(primaryConn, sqlquery))
		{
			log_err(_("Cannot insert node details, %s\n"),
			        PQerrorMessage(primaryConn));
			CloseConnections();
			exit(ERR_BAD_CONFIG);
		}
	}
	PQclear(res);
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
	printf(_("  -f, --config_file=PATH    configuration file\n"));
	printf(_("\n%s monitors a cluster of servers.\n"), progname);
}


#ifndef WIN32
static void
handle_sigint(SIGNAL_ARGS)
{
	CloseConnections();
	exit(1);
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
}
#endif


static void
CancelQuery(void)
{
	char errbuf[ERRBUFF_SIZE];
	PGcancel *pgcancel;

	pgcancel = PQgetCancel(primaryConn);

	if (!pgcancel || PQcancel(pgcancel, errbuf, ERRBUFF_SIZE) == 0)
		log_warning(_("Can't stop current query: %s\n"), errbuf);

	PQfreeCancel(pgcancel);
}


static void
update_shared_memory(char *last_wal_standby_applied)
{
	PGresult *res;

	sprintf(sqlquery, "SELECT repmgr_update_standby_location('%s')",
	        last_wal_standby_applied);

	/* If an error happens, just inform about that and continue */
	res = PQexec(myLocalConn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_warning(_("Cannot update this standby's shared memory: %s\n"), PQerrorMessage(myLocalConn));
		/* XXX is this enough reason to terminate this repmgrd? */
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
		CloseConnections();
		exit(ERR_DB_CON);
	}
	PQclear(res);
}
