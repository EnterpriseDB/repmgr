/*
 * dbutils.c
 * Copyright (c) 2ndQuadrant, 2010
 *
 * Database connections/managements functions
 * XXX At least i can create another function here to avoid code duplication
 *     on the main file
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
