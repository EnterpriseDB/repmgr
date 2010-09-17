/*
 * repmgrd.c
 * Copyright (c) 2ndQuadrant, 2010
 *
 * Replication manager daemon
 * This module connects to the nodes of a replication cluster and monitors
 * how far are they from master 
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "repmgr.h"

char    myClusterName[MAXLEN];

/* Local info */
int     myLocalMode = STANDBY_MODE;
int     myLocalId   = -1;
PGconn *myLocalConn;

/* Primary info */
int		primaryId;
char	primaryConninfo[MAXLEN];
PGconn *primaryConn;


void checkClusterConfiguration(void);
void checkNodeConfiguration(char *conninfo);
void getPrimaryConnection(void);

void MonitorCheck(void);
void MonitorExecute(void);

unsigned long long int walLocationToBytes(char *wal_location);


int
main(int argc, char **argv)
{
    char conninfo[MAXLEN]; 

	/*
	 * Read the configuration file: repmgr.conf
     */
	parse_config(myClusterName, &myLocalId, conninfo);
	if (myLocalId == -1) 
	{
		fprintf(stderr, "Node information is missing. "
						"Check the configuration file.\n");
		exit(1);
	}

    myLocalConn = establishDBConnection(conninfo, true);

    /*
     * Set my server mode, establish a connection to primary
	 * and start monitor
     */
	myLocalMode = is_standby(myLocalConn) ? STANDBY_MODE : PRIMARY_MODE;
	checkClusterConfiguration();
	checkNodeConfiguration(conninfo);
	if (myLocalMode == STANDBY_MODE)
	{
		/* I need the id of the primary as well as a connection to it */
		getPrimaryConnection();
		MonitorCheck();		
		PQfinish(primaryConn);
	}

    /* close the connection to the database and cleanup */
    PQfinish(myLocalConn);

    return 0;
}


/*
 * This function ask if we are in recovery, if false we are the primary else 
 * we are a standby
 */


void
getPrimaryConnection(void)
{
    PGresult *res1;
    PGresult *res2;
	int		  i;

    res1 = PQexec(myLocalConn, "SELECT * FROM repl_nodes");
    if (PQresultStatus(res1) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't get nodes info: %s\n", PQerrorMessage(myLocalConn));
        PQclear(res1);
        PQfinish(myLocalConn);
		exit(1);
    }

	for (i = 0; i < PQntuples(res1); i++)
    {
		primaryId   = atoi(PQgetvalue(res1, i, 0));
		strcpy(primaryConninfo, PQgetvalue(res1, i, 2));
		primaryConn = establishDBConnection(primaryConninfo, false);

    	res2 = PQexec(primaryConn, "SELECT pg_is_in_recovery()");
    	if (PQresultStatus(res2) != PGRES_TUPLES_OK)
    	{
    	    fprintf(stderr, "Can't get nodes info: %s\n", PQerrorMessage(primaryConn));
			PQclear(res1);
    	    PQclear(res2);
        	PQfinish(primaryConn);
        	PQfinish(myLocalConn);
			exit(1);
    	}

		if (strcmp(PQgetvalue(res2, 0, 0), "f") == 0)
		{
			PQclear(res2);
			PQclear(res1);
			/* On the primary the monitor check is asynchronous */
    		res1 = PQexec(primaryConn, "SET synchronous_commit TO off");
			PQclear(res1);
			return;
		}
		else
		{
			PQclear(res2);
			PQfinish(primaryConn);
			primaryId = -1;
		}
    }

	/* If we finish this loop without finding a primary then
     * we doesn't have the info or the primary has failed (or we 
     * reached max_connections or superuser_reserved_connections, 
     * anything else i'm missing?),
	 * Probably we will need to check the error to know if we need 
     * to start failover procedure o just fix some situation on the
     * standby.
     */
   	fprintf(stderr, "There isn't a primary node\n");
	PQclear(res1);
    PQfinish(myLocalConn);
	exit(1);
}


void
MonitorCheck(void) { 
	/*
	 * Every 3 seconds, insert monitor info
     */
	for (;;) 
	{  
		MonitorExecute(); 
		sleep(3); 
	} 
}


/*
 * Check if its time for next monitor call and if so, do it.
 */

void
MonitorExecute(void)
{
    PGresult *res;
	char sqlquery[8192];
	char monitor_standby_timestamp[MAXLEN];
	char last_wal_primary_location[MAXLEN];
	char last_wal_standby_received[MAXLEN];
	char last_wal_standby_applied[MAXLEN];

	unsigned long long int lsn_primary;
	unsigned long long int lsn_standby_received;
	unsigned long long int lsn_standby_applied;

	/* Get local xlog info */
    sprintf(sqlquery,
            "SELECT CURRENT_TIMESTAMP, pg_last_xlog_receive_location(), "
                   "pg_last_xlog_replay_location()");

    res = PQexec(myLocalConn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "PQexec failed: %s\n", PQerrorMessage(myLocalConn));
        PQclear(res);
        return;
    }

    strcpy(monitor_standby_timestamp, PQgetvalue(res, 0, 0));
    strcpy(last_wal_standby_received , PQgetvalue(res, 0, 1));
    strcpy(last_wal_standby_applied , PQgetvalue(res, 0, 2));
    PQclear(res);

	/* Get primary xlog info */
    sprintf(sqlquery, "SELECT pg_current_xlog_location() ");

    res = PQexec(primaryConn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "PQexec failed: %s\n", PQerrorMessage(primaryConn));
        PQclear(res);
        return;
    }

    strcpy(last_wal_primary_location, PQgetvalue(res, 0, 0));
    PQclear(res);

	/* Calculate the lag */
	lsn_primary = walLocationToBytes(last_wal_primary_location);
	lsn_standby_received = walLocationToBytes(last_wal_standby_received);
	lsn_standby_applied = walLocationToBytes(last_wal_standby_applied);

	/*
	 * Build the SQL to execute on primary
	 */
	sprintf(sqlquery,
				"INSERT INTO repl_status "
				"VALUES(%d, %d, '%s'::timestamp with time zone, "
                      " '%s', '%s', "
					  " %lld, %lld)",
                primaryId, myLocalId, monitor_standby_timestamp, 
				last_wal_primary_location, 
				last_wal_standby_received, 
				(lsn_primary - lsn_standby_received),
				(lsn_standby_received - lsn_standby_applied));

	/*
	 * Execute the query asynchronously, but don't check for a result. We
	 * will check the result next time we pause for a monitor step.
	 */
	if (!PQexec(primaryConn, sqlquery))
		fprintf(stderr, "replication monitor insert failed: %s\n",
						PQerrorMessage(primaryConn));
}


void
checkClusterConfiguration(void)
{
    PGresult   *res;

    res = PQexec(myLocalConn, "SELECT oid FROM pg_class "
							  " WHERE relname = 'repl_nodes'");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "PQexec failed: %s\n", PQerrorMessage(myLocalConn));
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
        fprintf(stderr, "The replication cluster is not configured\n");
        PQclear(res);
        PQfinish(myLocalConn);
        PQfinish(primaryConn);
		exit(1);
	}
	PQclear(res);
}


void
checkNodeConfiguration(char *conninfo)
{
    PGresult   *res;
	char sqlquery[8192];

	/*
	 * Check if we have my node information in repl_nodes
	 */
	sprintf(sqlquery, "SELECT * FROM repl_nodes "
 					  " WHERE id = %d AND cluster = '%s' ",
					  myLocalId, myClusterName);

    res = PQexec(myLocalConn, sqlquery); 
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "PQexec failed: %s\n", PQerrorMessage(myLocalConn));
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
		sprintf(sqlquery, "INSERT INTO repl_nodes "
						  "VALUES (%d, '%s', '%s')",
						  myLocalId, myClusterName, conninfo); 

    	if (!PQexec(primaryConn, sqlquery))
		{
			fprintf(stderr, "Cannot insert node details, %s\n",
							PQerrorMessage(primaryConn));
			PQfinish(myLocalConn);
			PQfinish(primaryConn);
			exit(1);
		}
	}
	PQclear(res);
}


unsigned long long int 
walLocationToBytes(char *wal_location)
{
    unsigned int xlogid;
    unsigned int xrecoff;

    if (sscanf(wal_location, "%X/%X", &xlogid, &xrecoff) != 2)
    {
        fprintf(stderr, "wrong log location format: %s\n", wal_location);
        return 0;
    }
    return ((xlogid * 16 * 1024 * 1024 * 255) + xrecoff);
}
