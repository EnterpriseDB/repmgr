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
	if (PQgetisnull(res, 0, 0))
	{
		PQclear(res);
		return false;
	}
   	if (strcmp(PQgetvalue(res, 0, 0), "f") == 0)
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
