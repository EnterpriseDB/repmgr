/*
 * dbutils.c
 * Copyright (c) 2ndQuadrant, 2010
 *
 * Database connection/management functions
 *
 */

#include "repmgr.h"

PGconn *
establishDBConnection(const char *conninfo, const bool exit_on_error)
{
	PGconn *conn;
    /* Make a connection to the database */
    conn = PQconnectdb(conninfo);
    /* Check to see that the backend connection was successfully made */
    if ((PQstatus(conn) != CONNECTION_OK))
    {
        fprintf(stderr, "Connection to database failed: %s", 
					PQerrorMessage(conn));
		if (exit_on_error)
		{
    	    PQfinish(conn);
			exit(1);
		}
    }

	return conn;
}



bool
is_standby(PGconn *conn)
{
    PGresult   *res;
	bool		result;

    res = PQexec(conn, "SELECT pg_is_in_recovery()");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't query server mode: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
		exit(1);
    }

	if (strcmp(PQgetvalue(res, 0, 0), "f") == 0)
		result = false;
	else
		result = true;	

	PQclear(res);
	return result;
}


bool
is_supported_version(PGconn *conn)
{
	PGresult	*res;
	int			major_version;

    res = PQexec(conn, "SELECT split_part(split_part(version(), ' ', 2), '.', 1)");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
		fprintf(stderr, "PQexec failed: %s", PQerrorMessage(conn));
        PQclear(res);
		PQfinish(conn);
		exit(1);
    }
    major_version = atoi(PQgetvalue(res, 0, 0));
    PQclear(res);

	return (major_version >= 9) ? true : false;
}


bool
guc_setted(PGconn *conn, const char *parameter, const char *op, const char *value)
{
	PGresult	*res;
	char		sqlquery[8192];

	sprintf(sqlquery, "SELECT true FROM pg_settings "
					  " WHERE name = '%s' AND setting %s '%s'",
					  parameter, op, value);

    res = PQexec(conn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
		fprintf(stderr, "PQexec failed: %s", PQerrorMessage(conn));
        PQclear(res);
		PQfinish(conn);
		exit(1);
    }
	if (PQntuples(res) == 0)
	{
		PQclear(res);
	   	return false;
	}
	PQclear(res);

	return true;
}


const char *
get_cluster_size(PGconn *conn)
{
	PGresult	*res;
	const char		*size;
	char		sqlquery[8192];

	sprintf(sqlquery, "SELECT pg_size_pretty(SUM(pg_database_size(oid))::bigint) "
					  "  FROM pg_database ");

    res = PQexec(conn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
		fprintf(stderr, "PQexec failed: %s", PQerrorMessage(conn));
        PQclear(res);
		PQfinish(conn);
		exit(1);
    }
   	size = PQgetvalue(res, 0, 0);
	PQclear(res);
	return size;
}

/*
 * get a connection to master by reading repl_nodes, creating a connection 
 * to each node (one at a time) and finding if it is a master or a standby
 */
PGconn *
getMasterConnection(PGconn *standby_conn, int id, char *cluster, int *master_id)
{
	PGconn	 *master_conn = NULL;
    PGresult *res1;
    PGresult *res2;
	char 	 sqlquery[8192];
	char	 master_conninfo[8192];
	int		 i;

	/* find all nodes belonging to this cluster */
	sprintf(sqlquery, "SELECT * FROM repmgr_%s.repl_nodes "
 					  " WHERE cluster = '%s' and id <> %d",
					  cluster, cluster, id);

    res1 = PQexec(standby_conn, sqlquery);
    if (PQresultStatus(res1) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't get nodes info: %s\n", PQerrorMessage(standby_conn));
        PQclear(res1);
		PQfinish(standby_conn);
		exit(1);
    }

	for (i = 0; i < PQntuples(res1); i++)
    {
		/* initialize with the values of the current node being processed */
		*master_id = atoi(PQgetvalue(res1, i, 0));
		strcpy(master_conninfo, PQgetvalue(res1, i, 2));
		master_conn = establishDBConnection(master_conninfo, false);

		/* 
		 * I can't use the is_standby() function here because on error that 
  		 * function closes the connection i pass and exit, but i still need to close
		 * standby_conn
		 */
    	res2 = PQexec(master_conn, "SELECT pg_is_in_recovery()");
    	if (PQresultStatus(res2) != PGRES_TUPLES_OK)
    	{
    	    fprintf(stderr, "Can't get nodes info: %s\n", PQerrorMessage(master_conn));
    	    PQclear(res2);
			PQfinish(master_conn);
			continue;
    	}

		/* if false, this is the master */
		if (strcmp(PQgetvalue(res2, 0, 0), "f") == 0)
		{
			PQclear(res2);
			PQclear(res1);
			return master_conn;
		}
		else
		{
			/* if it is a standby clear info */
			PQclear(res2);
			PQfinish(master_conn);
			*master_id = -1;
		}
    }

	/* If we finish this loop without finding a master then
     * we doesn't have the info or the master has failed (or we 
     * reached max_connections or superuser_reserved_connections, 
     * anything else i'm missing?),
	 * Probably we will need to check the error to know if we need 
     * to start failover procedure or just fix some situation on the
     * standby.
     */
   	fprintf(stderr, "There isn't a master node in the cluster\n");
	PQclear(res1);
	PQfinish(master_conn);
	return (PGconn *) NULL;
}

