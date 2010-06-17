/*
 * main.c
 * Copyright (c) 2ndQuadrant, 2010
 *
 * Replication manager
 * This module connects to the nodes of a replication cluster and monitors
 * how far are they from master 
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "repmgr.h"
#include "access/transam.h"

char    myClusterName[MAXLEN];

/* Local info */
int     myLocalMode = STANDBY_MODE;
int     myLocalId   = -1;
PGconn *myLocalConn;

/* Primary info */
int		primaryId;
char	primaryConninfo[MAXLEN];
PGconn *primaryConn;


void setMyLocalMode(void);
void checkClusterConfiguration(void);
void checkNodeConfiguration(char *conninfo);
void getPrimaryConnection(void);
void getLocalMonitoredInfo(char *currTimestamp, char *xlogLocation, 
						   char *xlogTimestamp);

void MonitorCheck(void);
void MonitorExecute(void);
TransactionId startSleepingTransaction(PGconn * conn, bool stop_current);


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
						"Check the configuration file.");
		exit(1);
	}

    myLocalConn = establishDBConnection(conninfo, true);

    /*
     * Set my server mode, establish a connection to primary
	 * and start monitor
     */
	setMyLocalMode();
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
setMyLocalMode(void)
{
    PGresult   *res;

    res = PQexec(myLocalConn, "SELECT pg_is_in_recovery()");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't query server mode: %s", PQerrorMessage(myLocalConn));
        PQclear(res);
        PQfinish(myLocalConn);
		exit(1);
    }

	if (strcmp(PQgetvalue(res, 0, 0), "f") == 0)
		myLocalMode = PRIMARY_MODE;
	else
		myLocalMode = STANDBY_MODE;	

	PQclear(res);
}


void
getPrimaryConnection(void)
{
    PGresult *res1;
    PGresult *res2;
	int		  i;

    res1 = PQexec(myLocalConn, "SELECT * FROM repl_nodes");
    if (PQresultStatus(res1) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't get nodes info: %s", PQerrorMessage(myLocalConn));
        PQclear(res1);
        PQfinish(myLocalConn);
		exit(1);
    }

	for (i = 0; i < PQntuples(res1); i++)
    {
		primaryId   = atoi(PQgetvalue(res1, i, 0));
		strcmp(primaryConninfo, PQgetvalue(res1, i, 1));
		primaryConn = establishDBConnection(primaryConninfo, false);

    	res2 = PQexec(primaryConn, "SELECT pg_is_in_recovery()");
    	if (PQresultStatus(res2) != PGRES_TUPLES_OK)
    	{
    	    fprintf(stderr, "Can't get nodes info: %s", PQerrorMessage(primaryConn));
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
   	fprintf(stderr, "There isn't a primary node");
	PQclear(res1);
    PQfinish(myLocalConn);
	exit(1);
}


void
getLocalMonitoredInfo(char *currTimestamp, char *xlogLocation, char *xlogTimestamp)
{
	PGresult *res;
	char sqlquery[8192];

	sprintf(sqlquery, 
			"SELECT CURRENT_TIMESTAMP, pg_last_xlog_receive_location(), "
			      " get_last_xlog_replay_timestamp()");

   	res = PQexec(myLocalConn, sqlquery); 
   	if (PQresultStatus(res) != PGRES_TUPLES_OK)
   	{
   	    fprintf(stderr, "PQexec failed: %s", PQerrorMessage(myLocalConn));
   	    PQclear(res);
       	PQfinish(myLocalConn);
		exit(1);
   	}

	strcpy(currTimestamp, PQgetvalue(res, 0, 0));
	strcpy(xlogLocation , PQgetvalue(res, 0, 1));
	strcpy(xlogTimestamp, PQgetvalue(res, 0, 2));
	PQclear(res);
	return;
}

void
MonitorCheck(void) { 
	PGconn *p1;
	PGconn *p2;

	TransactionId p1_xid;
	TransactionId p2_xid;
	TransactionId local_xmin;

	int cicles = 0;

	/* 
	 * We are trying to avoid cleanup on primary for records that are still 
	 * visible on standby
	 */
	p1 = establishDBConnection(primaryConninfo, false);
	p2 = establishDBConnection(primaryConninfo, false);

	p1_xid = startSleepingTransaction(p1, false);
	if (p1_xid == InvalidTransactionId)
	{
		PQfinish(myLocalConn);
		PQfinish(primaryConn);
		exit(1);
	}
	p2_xid = startSleepingTransaction(p2, false);
	if (p2_xid == InvalidTransactionId)
	{
		PQfinish(p1);
		PQfinish(myLocalConn);
		PQfinish(primaryConn);
		exit(1);
	}

	/* 
	 * We are using two long running transactions on primary to avoid
	 * cleanup of records that are still visible on this standby.
	 * Every second check if we can let advance the cleanup to avoid
     * bloat on primary.
	 *
	 * Every 3 cicles (around 3 seconds), insert monitor info
     */
	for (;;) 
	{  
		PGresult *res;

		res = PQexec(myLocalConn, "SELECT get_oldest_xmin()");
    	if (PQresultStatus(res) != PGRES_TUPLES_OK)
    	{
    	    fprintf(stderr, "PQexec failed: %s", PQerrorMessage(myLocalConn));
    	    PQclear(res);
    	}
		local_xmin = atol(PQgetvalue(res, 0, 0));
		PQclear(res);

		if (TransactionIdPrecedes(p1_xid, local_xmin) &&
		    TransactionIdPrecedes(p2_xid, local_xmin))
		{
			if (TransactionIdPrecedes(p1_xid, p2_xid))
			{
				p1_xid = startSleepingTransaction(p1, true);
				if (p1_xid == InvalidTransactionId)
				{
					PQfinish(p1);
					PQfinish(p2);
					PQfinish(myLocalConn);
					PQfinish(primaryConn);
					exit(1);
				}
			}
			else
			{
				p2_xid = startSleepingTransaction(p2, true);
				if (p2_xid == InvalidTransactionId)
				{
					PQfinish(p1);
					PQfinish(p2);
					PQfinish(myLocalConn);
					PQfinish(primaryConn);
					exit(1);
				}
			}
		}
		PQclear(res);
	
		if (cicles++ >= 3)
			MonitorExecute(); 
		sleep(1); 
	} 
}


/*
 * Check if its time for next monitor call and if so, do it.
 */

void
MonitorExecute(void)
{
	char sqlquery[8192];
	char monitor_timestamp[MAXLEN];
	char last_wal_location[MAXLEN];
	char last_wal_timestamp[MAXLEN];

	getLocalMonitoredInfo(monitor_timestamp, 
						  last_wal_location,
						  last_wal_timestamp);

	/*
	 * Build the SQL to execute on primary
	 */
	sprintf(sqlquery,
				"INSERT INTO repl_status "
				"VALUES(%d, %d, '%s'::timestamp with time zone, "
                      " pg_current_xlog_location(), '%s', "
					  " '%s'::timestamp with time zone, "
					  " CURRENT_TIMESTAMP - '%s'::timestamp with time zone) ",
                primaryId, myLocalId, monitor_timestamp, 
				last_wal_location, last_wal_timestamp, 
				last_wal_timestamp);

	/*
	 * Execute the query asynchronously, but don't check for a result. We
	 * will check the result next time we pause for a monitor step.
	 */
	if (!PQexec(primaryConn, sqlquery))
		fprintf(stderr, "replication monitor insert failed: %s",
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
        fprintf(stderr, "PQexec failed: %s", PQerrorMessage(myLocalConn));
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
        fprintf(stderr, "The replication cluster is not configured");
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
        fprintf(stderr, "PQexec failed: %s", PQerrorMessage(myLocalConn));
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
			fprintf(stderr, "Cannot insert node details, %s",
							PQerrorMessage(primaryConn));
			PQfinish(myLocalConn);
			PQfinish(primaryConn);
			exit(1);
		}
	}
	PQclear(res);
}


TransactionId
startSleepingTransaction(PGconn *conn, bool stop_current) {
	TransactionId txid;
	PGresult *res;

	/* 
	 * if stop_current is set we cancel any query that is currently
	 * executing on this connection
	 */
	if (stop_current)
	{
		char errbuf[256];

		if (PQcancel(PQgetCancel(conn), errbuf, 256) == 0)
			fprintf(stderr, "Can't stop current query: %s", errbuf);

		if (PQisBusy(conn) == 1)
			return InvalidTransactionId;
		else
			PQexec(conn, "ROLLBACK");
	}
		
	if (!PQexec(conn, "BEGIN"))
	{
		fprintf(stderr, "Can't start a transaction on primary. Error: %s",
						PQerrorMessage(conn));
		return InvalidTransactionId;
	}

	res = PQexec(conn, "SELECT txid_current()");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "PQexec failed: %s", PQerrorMessage(conn));
        PQclear(res);
		return InvalidTransactionId;
    }
	txid = atol(PQgetvalue(res, 0, 0));
	PQclear(res);
	
	/* Let this transaction sleep */
	PQsendQuery(conn, "SELECT pg_sleep(10000000000)");
	return txid;
}


/*
 * TransactionIdPrecedes --- is id1 logically < id2?
 * This function was copied from src/backend/access/transam/transam.c
 */
bool
TransactionIdPrecedes(TransactionId id1, TransactionId id2)
{
    /*
     * If either ID is a permanent XID then we can just do unsigned
     * comparison.  If both are normal, do a modulo-2^31 comparison.
     */
    int32       diff;

    if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
        return (id1 < id2);

    diff = (int32) (id1 - id2);
    return (diff < 0);
}

