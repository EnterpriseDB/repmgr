/*
 * repmgrd.c - Replication manager daemon
 * Copyright (C) 2ndQuadrant, 2010
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

#include "libpq/pqsignal.h"


/* Local info */
t_configuration_options local_options;
int     myLocalMode = STANDBY_MODE;
PGconn *myLocalConn;

/* Primary info */
t_configuration_options primary_options;
PGconn *primaryConn;

char sqlquery[QUERY_STR_LEN];

const char *progname;

char	*config_file = DEFAULT_CONFIG_FILE;
bool	verbose = false;

// should initialize with {0} to be ANSI complaint ? but this raises error with gcc -Wall
repmgr_config config = {};

static void help(const char* progname);
static void usage(void);
static void checkClusterConfiguration(void);
static void checkNodeConfiguration(char *conninfo);
static void CancelQuery(void);

static void MonitorExecute(void);

static unsigned long long int walLocationToBytes(char *wal_location);

static void handle_sigint(SIGNAL_ARGS);
static void setup_cancel_handler(void);

#define CloseConnections()	\
						if (PQisBusy(primaryConn) == 1) \
							CancelQuery(); \
						if (myLocalConn != NULL) \
							PQfinish(myLocalConn);	\
						if (primaryConn != NULL) \
							PQfinish(primaryConn);

/*
 * Every 3 seconds, insert monitor info
 */
#define MonitorCheck() \
						for (;;) \
						{ \
							MonitorExecute(); \
							sleep(3); \
						}


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
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			printf("%s (PostgreSQL) " PG_VERSION "\n", progname);
			exit(0);
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
			exit(1);
		}
	}

	setup_cancel_handler();

	/*
	 * Read the configuration file: repmgr.conf
	 */
	parse_config(config_file, &local_options);
	if (local_options.node == -1)
	{
		log_err("Node information is missing. "
		        "Check the configuration file.\n");
		exit(1);
	}
	logger_init(progname, local_options.loglevel, local_options.logfacility);

	myLocalConn = establishDBConnection(local_options.conninfo, true);

	/* should be v9 or better */
	pg_version(myLocalConn, standby_version);
	if (strcmp(standby_version, "") == 0)
	{
		PQfinish(myLocalConn);
		log_err(_("%s needs standby to be PostgreSQL 9.0 or better\n"), progname);
		exit(1);
	}

	/*
	 * Set my server mode, establish a connection to primary
	 * and start monitor
	 */
	myLocalMode = is_standby(myLocalConn) ? STANDBY_MODE : PRIMARY_MODE;
	if (myLocalMode == PRIMARY_MODE)
	{
		primary_options.node = local_options.node;
		strncpy(primary_options.conninfo, local_options.conninfo, MAXLEN);
		primaryConn = myLocalConn;
	}
	else
	{
		/* I need the id of the primary as well as a connection to it */
		primaryConn = getMasterConnection(myLocalConn, local_options.node, local_options.cluster_name, &primary_options.node);
		if (primaryConn == NULL)
			exit(1);
	}

	checkClusterConfiguration();
	checkNodeConfiguration(local_options.conninfo);
	if (myLocalMode == STANDBY_MODE)
	{
		MonitorCheck();
	}

	/* close the connection to the database and cleanup */
	CloseConnections();
	
	/* Shuts down logging system */
	logger_shutdown();

	return 0;
}


/*
 * Insert monitor info, this is basically the time and xlog replayed,
 * applied on standby and current xlog location in primary.
 * Also do the math to see how far are we in bytes for being uptodate
 */
static void
MonitorExecute(void)
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
	for (connection_retries = 0; connection_retries < 15; connection_retries++)
	{
		if (PQstatus(primaryConn) != CONNECTION_OK)
		{
			log_warning(_("Connection to master has been lost, trying to recover..."));
			/* wait 20 seconds between retries */
			sleep(20);

			PQreset(primaryConn);
		}
		else
		{
			log_notice(_("Connection to master has been restored, continue monitoring."));
			break;
		}
	}
	if (PQstatus(primaryConn) != CONNECTION_OK)
	{
		log_err(_("We couldn't reconnect to master. Now checking if another node has been promoted."));
		for (connection_retries = 0; connection_retries < 6; connection_retries++)
		{
			primaryConn = getMasterConnection(myLocalConn, local_options.node, local_options.cluster_name, &primary_options.node);
			if (PQstatus(primaryConn) == CONNECTION_OK)
			{
				/* Connected, we can continue the process so break the loop */
				log_err(_("Connected to node %d, continue monitoring."), primary_options.node);
				break;
			}
			else
			{
				log_err(_("We haven't found a new master, waiting before retry..."));
				/* wait 5 minutes before retries, after 6 failures (30 minutes) we stop trying */
				sleep(300);
			}
		}
	}
	if (PQstatus(primaryConn) != CONNECTION_OK)
	{
		log_err(_("We couldn't reconnect for long enough, exiting..."));
		exit(1);
	}

	/* Check if we still are a standby, we could have been promoted */
	if (!is_standby(myLocalConn))
	{
		log_err(_("It seems like we have been promoted, so exit from monitoring..."));
		CloseConnections();
		exit(1);
	}

	/*
	 * first check if there is a command being executed,
	 * and if that is the case, cancel the query so i can
	 * insert the current record
	 */
	if (PQisBusy(primaryConn) == 1)
		CancelQuery();

	/* Get local xlog info */
	snprintf(sqlquery, QUERY_STR_LEN,
	        "SELECT CURRENT_TIMESTAMP, pg_last_xlog_receive_location(), "
	        "pg_last_xlog_replay_location()");

	res = PQexec(myLocalConn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err("PQexec failed: %s\n", PQerrorMessage(myLocalConn));
		PQclear(res);
		/* if there is any error just let it be and retry in next loop */
		return;
	}

	strncpy(monitor_standby_timestamp, PQgetvalue(res, 0, 0), MAXLEN);
	strncpy(last_wal_standby_received , PQgetvalue(res, 0, 1), MAXLEN);
	strncpy(last_wal_standby_applied , PQgetvalue(res, 0, 2), MAXLEN);
	PQclear(res);

	/* Get primary xlog info */
	snprintf(sqlquery, QUERY_STR_LEN, "SELECT pg_current_xlog_location() ");

	res = PQexec(primaryConn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err("PQexec failed: %s\n", PQerrorMessage(primaryConn));
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
	snprintf(sqlquery,
	        QUERY_STR_LEN, "INSERT INTO repmgr_%s.repl_monitor "
	        "VALUES(%d, %d, '%s'::timestamp with time zone, "
	        " '%s', '%s', "
	        " %lld, %lld)", local_options.cluster_name,
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
		log_warning("Query could not be sent to primary. %s\n",
		        PQerrorMessage(primaryConn));
}


static void
checkClusterConfiguration(void)
{
	PGresult   *res;

	snprintf(sqlquery, QUERY_STR_LEN, "SELECT oid FROM pg_class "
	        " WHERE oid = 'repmgr_%s.repl_nodes'::regclass",
	        local_options.cluster_name);
	res = PQexec(myLocalConn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err("PQexec failed: %s\n", PQerrorMessage(myLocalConn));
		PQclear(res);
		PQfinish(myLocalConn);
		PQfinish(primaryConn);
		exit(1);
	}

	/*
	 * If there isn't any results then we have not configured a primary node yet
	 * in repmgr or the connection string is pointing to the wrong database.
	 * XXX if we are the primary, should we try to create the tables needed?
	 */
	if (PQntuples(res) == 0)
	{
		log_err("The replication cluster is not configured\n");
		PQclear(res);
		PQfinish(myLocalConn);
		PQfinish(primaryConn);
		exit(1);
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
	snprintf(sqlquery, QUERY_STR_LEN, "SELECT * FROM repmgr_%s.repl_nodes "
	        " WHERE id = %d AND cluster = '%s' ",
	        local_options.cluster_name, local_options.node, local_options.cluster_name);

	res = PQexec(myLocalConn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err("PQexec failed: %s\n", PQerrorMessage(myLocalConn));
		PQclear(res);
		PQfinish(myLocalConn);
		PQfinish(primaryConn);
		exit(1);
	}

	/*
	 * If there isn't any results then we have not configured this node yet
	 * in repmgr, if that is the case we will insert the node to the cluster
	 */
	if (PQntuples(res) == 0)
	{
		PQclear(res);
		/* Adding the node */
		snprintf(sqlquery, QUERY_STR_LEN, "INSERT INTO repmgr_%s.repl_nodes "
		        "VALUES (%d, '%s', '%s')",
		        local_options.cluster_name, local_options.node, local_options.cluster_name, local_options.conninfo);

		if (!PQexec(primaryConn, sqlquery))
		{
			log_err("Cannot insert node details, %s\n",
			        PQerrorMessage(primaryConn));
			PQfinish(myLocalConn);
			PQfinish(primaryConn);
			exit(1);
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
		log_err("wrong log location format: %s\n", wal_location);
		return 0;
	}
	return ((xlogid * 16 * 1024 * 1024 * 255) + xrecoff);
}


void usage(void)
{
	log_err(_("\n\n%s: Replicator manager daemon \n"), progname);
	log_err(_("Try \"%s --help\" for more information.\n"), progname);
}

void help(const char *progname)
{
	printf(_("\n%s: Replicator manager daemon \n"), progname);
	printf(_("Usage:\n"));
	printf(_(" %s [OPTIONS]\n"), progname);
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
}

static void
setup_cancel_handler(void)
{
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
		log_warning("Can't stop current query: %s", errbuf);

	PQfreeCancel(pgcancel);
}
