/*
 * dbutils.c - Database connection/management functions
 * Copyright (C) 2ndQuadrant, 2010-2015
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

#include <unistd.h>
#include <time.h>
#include <sys/time.h>

#include "repmgr.h"
#include "strutil.h"
#include "log.h"

PGconn *
establish_db_connection(const char *conninfo, const bool exit_on_error)
{
	/* Make a connection to the database */
	PGconn	   *conn = NULL;
	char		connection_string[MAXLEN];

	strcpy(connection_string, conninfo);
	strcat(connection_string, " fallback_application_name='repmgr'");
	conn = PQconnectdb(connection_string);

	/* Check to see that the backend connection was successfully made */
	if ((PQstatus(conn) != CONNECTION_OK))
	{
		log_err(_("Connection to database failed: %s\n"),
				PQerrorMessage(conn));

		if (exit_on_error)
		{
			PQfinish(conn);
			exit(ERR_DB_CON);
		}
	}

	return conn;
}

PGconn *
establish_db_connection_by_params(const char *keywords[], const char *values[],
								  const bool exit_on_error)
{
	/* Make a connection to the database */
	PGconn	   *conn = PQconnectdbParams(keywords, values, true);

	/* Check to see that the backend connection was successfully made */
	if ((PQstatus(conn) != CONNECTION_OK))
	{
		log_err(_("Connection to database failed: %s\n"),
				PQerrorMessage(conn));
		if (exit_on_error)
		{
			PQfinish(conn);
			exit(ERR_DB_CON);
		}
	}

	return conn;
}

int
is_standby(PGconn *conn)
{
	PGresult   *res;
	int			result = 0;

	res = PQexec(conn, "SELECT pg_is_in_recovery()");

	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Can't query server mode: %s"),
				PQerrorMessage(conn));
		result = -1;
	}
	else if (PQntuples(res) == 1 && strcmp(PQgetvalue(res, 0, 0), "t") == 0)
		result = 1;

	PQclear(res);
	return result;
}



int
is_witness(PGconn *conn, char *schema, char *cluster, int node_id)
{
	PGresult   *res;
	int			result = 0;
	char		sqlquery[QUERY_STR_LEN];

	sqlquery_snprintf(sqlquery, "SELECT witness from %s.repl_nodes where cluster = '%s' and id = %d",
					  schema, cluster, node_id);
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Can't query server mode: %s"), PQerrorMessage(conn));
		result = -1;
	}
	else if (PQntuples(res) == 1 && strcmp(PQgetvalue(res, 0, 0), "t") == 0)
		result = 1;

	PQclear(res);
	return result;
}


/* check the PQStatus and try to 'select 1' to confirm good connection */
bool
is_pgup(PGconn *conn, int timeout)
{
	char		sqlquery[QUERY_STR_LEN];

	/* Check the connection status twice in case it changes after reset */
	bool		twice = false;

	/* Check the connection status twice in case it changes after reset */
	for (;;)
	{
		if (PQstatus(conn) != CONNECTION_OK)
		{
			if (twice)
				return false;
			PQreset(conn);		/* reconnect */
			twice = true;
		}
		else
		{
			/*
			 * Send a SELECT 1 just to check if the connection is OK
			 */
			if (!cancel_query(conn, timeout))
				goto failed;
			if (wait_connection_availability(conn, timeout) != 1)
				goto failed;

			sqlquery_snprintf(sqlquery, "SELECT 1");
			if (PQsendQuery(conn, sqlquery) == 0)
			{
				log_warning(_("PQsendQuery: Query could not be sent to primary. %s\n"),
							PQerrorMessage(conn));
				goto failed;
			}
			if (wait_connection_availability(conn, timeout) != 1)
				goto failed;

			break;

	failed:

			/*
			 * we need to retry, because we might just have loose the
			 * connection once
			 */
			if (twice)
				return false;
			PQreset(conn);		/* reconnect */
			twice = true;
		}
	}
	return true;
}


/*
 * If postgreSQL version is 9 or superior returns the major version
 * if 8 or inferior returns an empty string
 */
char *
pg_version(PGconn *conn, char *major_version)
{
	PGresult   *res;

	int			major_version1;
	char	   *major_version2;

	res = PQexec(conn,
				 "WITH pg_version(ver) AS "
				 "(SELECT split_part(version(), ' ', 2)) "
				 "SELECT split_part(ver, '.', 1), split_part(ver, '.', 2) "
				 "FROM pg_version");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Version check PQexec failed: %s"),
				PQerrorMessage(conn));
		PQclear(res);
		return NULL;
	}

	major_version1 = atoi(PQgetvalue(res, 0, 0));
	major_version2 = PQgetvalue(res, 0, 1);

	if (major_version1 >= 9)
	{
		/* form a major version string */
		xsnprintf(major_version, MAXVERSIONSTR, "%d.%s", major_version1,
				  major_version2);
	}
	else
		strcpy(major_version, "");

	PQclear(res);

	return major_version;
}


int
guc_set(PGconn *conn, const char *parameter, const char *op,
		const char *value)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];
	int			retval = 1;

	sqlquery_snprintf(sqlquery, "SELECT true FROM pg_settings "
					  " WHERE name = '%s' AND setting %s '%s'",
					  parameter, op, value);

	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("GUC setting check PQexec failed: %s"),
				PQerrorMessage(conn));
		retval = -1;
	}
	else if (PQntuples(res) == 0)
	{
		retval = 0;
	}

	PQclear(res);

	return retval;
}

/**
 * Just like guc_set except with an extra parameter containing the name of
 * the pg datatype so that the comparison can be done properly.
 */
int
guc_set_typed(PGconn *conn, const char *parameter, const char *op,
			  const char *value, const char *datatype)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];
	int			retval = 1;

	sqlquery_snprintf(sqlquery, "SELECT true FROM pg_settings "
					  " WHERE name = '%s' AND setting::%s %s '%s'::%s",
					  parameter, datatype, op, value, datatype);

	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("GUC setting check PQexec failed: %s"),
				PQerrorMessage(conn));
		retval = -1;
	}
	else if (PQntuples(res) == 0)
	{
		retval = 0;
	}

	PQclear(res);

	return retval;
}


const char *
get_cluster_size(PGconn *conn)
{
	PGresult   *res;
	const char *size = NULL;
	char		sqlquery[QUERY_STR_LEN];

	sqlquery_snprintf(
					  sqlquery,
				 "SELECT pg_size_pretty(SUM(pg_database_size(oid))::bigint) "
					  "	 FROM pg_database ");

	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Get cluster size PQexec failed: %s"),
				PQerrorMessage(conn));
	}
	else
	{
		size = PQgetvalue(res, 0, 0);
	}

	PQclear(res);
	return size;
}

/*
 * get a connection to master by reading repl_nodes, creating a connection
 * to each node (one at a time) and finding if it is a master or a standby
 *
 * NB: If master_conninfo_out may be NULL.	If it is non-null, it is assumed to
 * point to allocated memory of MAXCONNINFO in length, and the master server
 * connection string is placed there.
 */
PGconn *
get_master_connection(PGconn *standby_conn, char *schema, char *cluster,
					  int *master_id, char *master_conninfo_out)
{
	PGconn	   *master_conn = NULL;
	PGresult   *res1;
	PGresult   *res2;
	char		sqlquery[QUERY_STR_LEN];
	char		master_conninfo_stack[MAXCONNINFO];
	char	   *master_conninfo = &*master_conninfo_stack;
	char		schema_quoted[MAXLEN];

	int			i;

	/*
	 * If the caller wanted to get a copy of the connection info string, sub
	 * out the local stack pointer for the pointer passed by the caller.
	 */
	if (master_conninfo_out != NULL)
		master_conninfo = master_conninfo_out;

	/*
	 * XXX: This is copied in at least two other procedures
	 *
	 * Assemble the unquoted schema name
	 */
	{
		char	   *identifier = PQescapeIdentifier(standby_conn, schema,
													strlen(schema));

		maxlen_snprintf(schema_quoted, "%s", identifier);
		PQfreemem(identifier);
	}

	/* find all nodes belonging to this cluster */
	log_info(_("finding node list for cluster '%s'\n"),
			 cluster);

	sqlquery_snprintf(sqlquery, "SELECT id, conninfo FROM %s.repl_nodes "
					  " WHERE cluster = '%s' and not witness",
					  schema_quoted, cluster);

	res1 = PQexec(standby_conn, sqlquery);
	if (PQresultStatus(res1) != PGRES_TUPLES_OK)
	{
		log_err(_("Can't get nodes info: %s\n"),
				PQerrorMessage(standby_conn));
		PQclear(res1);
		return NULL;
	}

	for (i = 0; i < PQntuples(res1); i++)
	{
		/* initialize with the values of the current node being processed */
		*master_id = atoi(PQgetvalue(res1, i, 0));
		strncpy(master_conninfo, PQgetvalue(res1, i, 1), MAXCONNINFO);
		log_info(_("checking role of cluster node '%s'\n"),
				 master_conninfo);
		master_conn = establish_db_connection(master_conninfo, false);

		if (PQstatus(master_conn) != CONNECTION_OK)
			continue;

		/*
		 * Can't use the is_standby() function here because on error that
		 * function closes the connection passed and exits.  This still needs
		 * to close master_conn first.
		 */
		res2 = PQexec(master_conn, "SELECT pg_is_in_recovery()");

		if (PQresultStatus(res2) != PGRES_TUPLES_OK)
		{
			log_err(_("Can't get recovery state from this node: %s\n"),
					PQerrorMessage(master_conn));
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

	/*
	 * If we finish this loop without finding a master then we doesn't have
	 * the info or the master has failed (or we reached max_connections or
	 * superuser_reserved_connections, anything else I'm missing?).
	 *
	 * Probably we will need to check the error to know if we need to start
	 * failover procedure or just fix some situation on the standby.
	 */
	PQclear(res1);
	return NULL;
}


/*
 * wait until current query finishes ignoring any results, this could be an
 * async command or a cancelation of a query
 * return 1 if Ok; 0 if any error ocurred; -1 if timeout reached
 */
int
wait_connection_availability(PGconn *conn, long long timeout)
{
	PGresult   *res;
	fd_set		read_set;
	int			sock = PQsocket(conn);
	struct timeval tmout,
				before,
				after;
	struct timezone tz;

	/* recalc to microseconds */
	timeout *= 1000000;

	while (timeout > 0)
	{
		if (PQconsumeInput(conn) == 0)
		{
			log_warning(_("wait_connection_availability: could not receive data from connection. %s\n"),
						PQerrorMessage(conn));
			return 0;
		}

		if (PQisBusy(conn) == 0)
		{
			do
			{
				res = PQgetResult(conn);
				PQclear(res);
			} while (res != NULL);

			break;
		}


		tmout.tv_sec = 0;
		tmout.tv_usec = 250000;

		FD_ZERO(&read_set);
		FD_SET(sock, &read_set);

		gettimeofday(&before, &tz);
		if (select(sock, &read_set, NULL, NULL, &tmout) == -1)
		{
			log_warning(
						_("wait_connection_availability: select() returned with error: %s"),
						strerror(errno));
			return -1;
		}
		gettimeofday(&after, &tz);

		timeout -= (after.tv_sec * 1000000 + after.tv_usec) -
			(before.tv_sec * 1000000 + before.tv_usec);
	}


	if (timeout >= 0)
	{
		return 1;
	}

	log_warning(_("wait_connection_availability: timeout reached"));
	return -1;
}


bool
cancel_query(PGconn *conn, int timeout)
{
	char		errbuf[ERRBUFF_SIZE];
	PGcancel   *pgcancel;

	if (wait_connection_availability(conn, timeout) != 1)
		return false;

	pgcancel = PQgetCancel(conn);

	if (pgcancel == NULL)
		return false;

	/*
	 * PQcancel can only return 0 if socket()/connect()/send() fails, in any
	 * of those cases we can assume something bad happened to the connection
	 */
	if (PQcancel(pgcancel, errbuf, ERRBUFF_SIZE) == 0)
	{
		log_warning(_("Can't stop current query: %s\n"), errbuf);
		PQfreeCancel(pgcancel);
		return false;
	}

	PQfreeCancel(pgcancel);

	return true;
}
