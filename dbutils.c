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
#include "config.h"
#include "strutil.h"
#include "log.h"

char repmgr_schema[MAXLEN] = "";
char repmgr_schema_quoted[MAXLEN] = "";

PGconn *
establish_db_connection(const char *conninfo, const bool exit_on_error)
{
	/* Make a connection to the database */
	PGconn	   *conn = NULL;
	char		connection_string[MAXLEN];

	strcpy(connection_string, conninfo);
	strcat(connection_string, " fallback_application_name='repmgr'");

	log_debug(_("connecting to: '%s'\n"), connection_string);

	conn = PQconnectdb(connection_string);

	/* Check to see that the backend connection was successfully made */
	if ((PQstatus(conn) != CONNECTION_OK))
	{
		log_err(_("connection to database failed: %s\n"),
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
		log_err(_("connection to database failed: %s\n"),
				PQerrorMessage(conn));
		if (exit_on_error)
		{
			PQfinish(conn);
			exit(ERR_DB_CON);
		}
	}

	return conn;
}


bool
check_cluster_schema(PGconn *conn)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];

	sqlquery_snprintf(sqlquery,
					  "SELECT 1 FROM pg_namespace WHERE nspname = '%s'",
					  get_repmgr_schema());

	log_debug(_("check_cluster_schema(): %s\n"), sqlquery);
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("check_cluster_schema(): unable to check cluster schema: %s\n"),
				PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	if (PQntuples(res) == 0)
	{
		/* schema doesn't exist */
		log_debug(_("check_cluster_schema(): schema '%s' doesn't exist\n"), get_repmgr_schema());
		PQclear(res);

		return false;
	}

	PQclear(res);

	return true;
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
 * Return the id of the active primary node, or -1 if no
 * record available.
 *
 * This reports the value stored in the database only and
 * does not verify whether the node is actually available
 */
int
get_primary_node_id(PGconn *conn, char *cluster)
{
	char		sqlquery[QUERY_STR_LEN];
	PGresult   *res;
	int			retval;

	sqlquery_snprintf(sqlquery,
					  "SELECT id               "
					  "  FROM %s.repl_nodes    "
					  " WHERE cluster = '%s'   "
					  "   AND type = 'primary' "
					  "   AND active IS TRUE   ",
					  get_repmgr_schema_quoted(conn),
					  cluster);

	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("get_primary_node_id(): query failed\n%s\n"),
				PQerrorMessage(conn));
		retval = -1;
	}
	else if (PQntuples(res) == 0)
	{
		log_warning(_("get_primary_node_id(): no active primary found\n"));
		retval = -1;
	}
	else
	{
		retval = atoi(PQgetvalue(res, 0, 0));
	}
	PQclear(res);

	return retval;
}


/*
 * Return the server version number for the connection provided
 */
int
get_server_version(PGconn *conn, char *server_version)
{
	PGresult   *res;
	res = PQexec(conn,
				 "SELECT current_setting('server_version_num'), "
				 "       current_setting('server_version')");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("unable to determine server version number:\n%s"),
				PQerrorMessage(conn));
		PQclear(res);
		return -1;
	}

	if(server_version != NULL)
		strcpy(server_version, PQgetvalue(res, 0, 0));

	return atoi(PQgetvalue(res, 0, 0));
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

	sqlquery_snprintf(sqlquery,
					  "SELECT true FROM pg_settings "
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


bool
get_cluster_size(PGconn *conn, char *size)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];

	sqlquery_snprintf(
					  sqlquery,
				 "SELECT pg_size_pretty(SUM(pg_database_size(oid))::bigint) "
					  "	 FROM pg_database ");

	res = PQexec(conn, sqlquery);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("get_cluster_size(): PQexec failed: %s"),
				PQerrorMessage(conn));

		PQclear(res);
		return false;
	}

	strncpy(size, PQgetvalue(res, 0, 0), MAXLEN);

	PQclear(res);
	return true;
}



bool
get_pg_setting(PGconn *conn, const char *setting, char *output)
{
	char		sqlquery[QUERY_STR_LEN];
	PGresult   *res;
	int			i;
	bool        success = true;

	sqlquery_snprintf(sqlquery,
					  "SELECT name, setting "
					  " FROM pg_settings WHERE name = '%s'",
					  setting);

	log_debug(_("get_pg_setting(): %s\n"), sqlquery);

	res = PQexec(conn, sqlquery);

	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("get_pg_setting() - PQexec failed: %s"),
				PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		if (strcmp(PQgetvalue(res, i, 0), setting) == 0)
		{
			strncpy(output, PQgetvalue(res, i, 1), MAXLEN);
			success = true;
			break;
		}
		else
		{
			log_err(_("unknown parameter: %s"), PQgetvalue(res, i, 0));
		}
	}

	if(success == true)
	{
		log_debug(_("get_pg_setting(): returned value is '%s'\n"), output);
	}

	PQclear(res);

	return success;
}



/*
 * get_primary_connection()
 *
 * Returns connection to cluster's primary node
 *
 * This assumes that the primary node information in `repl_nodes`
 * is correct. See get_master_connection(), which polls all known
 * servers to find out which one is currently primary.
 */
PGconn *
get_primary_connection(PGconn *standby_conn, char *cluster,
						int *primary_node_id_ptr, char *primary_conninfo_out)
{
	PGconn	   *primary_conn = NULL;
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];
	char		primary_conninfo_stack[MAXCONNINFO];
	char	   *primary_conninfo = &*primary_conninfo_stack;

	/*
	 * If the caller wanted to get a copy of the connection info string, sub
	 * out the local stack pointer for the pointer passed by the caller.
	 */
	if (primary_conninfo_out != NULL)
		primary_conninfo = primary_conninfo_out;

	sqlquery_snprintf(sqlquery,
					  "    SELECT n.conninfo, n.name, n.id "
					  "      FROM %s.repl_nodes n "
					  "     WHERE n.cluster = '%s' "
					  "       AND n.type = 'primary' ",
					  get_repmgr_schema_quoted(standby_conn),
					  cluster);

	log_debug("get_primary_connection(): %s\n", sqlquery);

	res = PQexec(standby_conn, sqlquery);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("unable to get conninfo for master server: %s\n"),
				PQerrorMessage(standby_conn));
		PQclear(res);
		return NULL;
	}

	if(!PQntuples(res))
	{
		log_notice(_("no master server record found"));
		PQclear(res);
		return NULL;
	}

	strncpy(primary_conninfo, PQgetvalue(res, 0, 0), MAXCONNINFO);

	if(primary_node_id_ptr != NULL)
		*primary_node_id_ptr = atoi(PQgetvalue(res, 0, 1));

	PQclear(res);

	log_debug("conninfo is: '%s'\n", primary_conninfo);
	primary_conn = establish_db_connection(primary_conninfo, false);

	if (PQstatus(primary_conn) != CONNECTION_OK)
	{
		log_err(_("unable to connect to master node: %s\n"),
				PQerrorMessage(primary_conn));
		return NULL;
	}

	return primary_conn;
}


/*
 * get_upstream_connection()
 *
 * Returns connection to node's upstream node
 *
 * NOTE: will attempt to connect even if node is marked as inactive
 */
PGconn *
get_upstream_connection(PGconn *standby_conn, char *cluster, int node_id,
						int *upstream_node_id_ptr, char *upstream_conninfo_out)
{
	PGconn	   *upstream_conn = NULL;
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];
	char		upstream_conninfo_stack[MAXCONNINFO];
	char	   *upstream_conninfo = &*upstream_conninfo_stack;

	/*
	 * If the caller wanted to get a copy of the connection info string, sub
	 * out the local stack pointer for the pointer passed by the caller.
	 */
	if (upstream_conninfo_out != NULL)
		upstream_conninfo = upstream_conninfo_out;

	sqlquery_snprintf(sqlquery,
					  "    SELECT un.conninfo, un.name, un.id "
					  "      FROM %s.repl_nodes un "
					  "INNER JOIN %s.repl_nodes n "
					  "        ON (un.id = n.upstream_node_id AND un.cluster = n.cluster)"
					  "     WHERE n.cluster = '%s' "
					  "       AND n.id = %i ",
					  get_repmgr_schema_quoted(standby_conn),
					  get_repmgr_schema_quoted(standby_conn),
					  cluster,
					  node_id);

	log_debug("get_upstream_connection(): %s\n", sqlquery);

	res = PQexec(standby_conn, sqlquery);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("unable to get conninfo for upstream server: %s\n"),
				PQerrorMessage(standby_conn));
		PQclear(res);
		return NULL;
	}

	if(!PQntuples(res))
	{
		log_notice(_("no record found for upstream server"));
		PQclear(res);
		return NULL;
	}

	strncpy(upstream_conninfo, PQgetvalue(res, 0, 0), MAXCONNINFO);

	if(upstream_node_id_ptr != NULL)
		*upstream_node_id_ptr = atoi(PQgetvalue(res, 0, 1));

	PQclear(res);

	log_debug("conninfo is: '%s'\n", upstream_conninfo);
	upstream_conn = establish_db_connection(upstream_conninfo, false);

	if (PQstatus(upstream_conn) != CONNECTION_OK)
	{
		log_err(_("unable to connect to upstream node: %s\n"),
				PQerrorMessage(upstream_conn));
		return NULL;
	}

	return upstream_conn;
}


/*
 * get a connection to master by reading repl_nodes, creating a connection
 * to each node (one at a time) and finding if it is a master or a standby
 *
 * NB: If master_conninfo_out may be NULL.	If it is non-null, it is assumed to
 * point to allocated memory of MAXCONNINFO in length, and the master server
 * connection string is placed there.
 *
 * To get the primary node from the metadata (i.e. without polling all servers),
 * use `get_primary_connection()`.
 */

PGconn *
get_master_connection(PGconn *standby_conn, char *cluster,
					  int *master_id, char *master_conninfo_out)
{
	PGconn	   *master_conn = NULL;
	PGresult   *res1;
	PGresult   *res2;
	char		sqlquery[QUERY_STR_LEN];
	char		master_conninfo_stack[MAXCONNINFO];
	char	   *master_conninfo = &*master_conninfo_stack;

	int			i,
				node_id;

	if(master_id != NULL)
	{
		*master_id = -1;
	}

	/* find all nodes belonging to this cluster */
	log_info(_("finding node list for cluster '%s'\n"),
			 cluster);

	sqlquery_snprintf(sqlquery,
					  "SELECT id, conninfo "
					  "  FROM %s.repl_nodes "
					  " WHERE cluster = '%s' "
					  "   AND type != 'witness' ",
					  get_repmgr_schema_quoted(standby_conn),
					  cluster);

	res1 = PQexec(standby_conn, sqlquery);
	if (PQresultStatus(res1) != PGRES_TUPLES_OK)
	{
		log_err(_("unable to retrieve node records: %s\n"),
				PQerrorMessage(standby_conn));
		PQclear(res1);
		return NULL;
	}

	for (i = 0; i < PQntuples(res1); i++)
	{
		/* initialize with the values of the current node being processed */
		node_id = atoi(PQgetvalue(res1, i, 0));
		strncpy(master_conninfo, PQgetvalue(res1, i, 1), MAXCONNINFO);
		log_info(_("checking role of cluster node '%i'\n"),
				 node_id);
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
			log_err(_("unable to retrieve recovery state from this node: %s\n"),
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
			log_debug(_("get_master_connection(): current primary node is %i\n"), node_id);

			if(master_id != NULL)
			{
				*master_id = node_id;
			}

			return master_conn;
		}
		else
		{
			/* if it is a standby, clear info */
			PQclear(res2);
			PQfinish(master_conn);
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

char *
get_repmgr_schema(void)
{
	return repmgr_schema;
}


char *
get_repmgr_schema_quoted(PGconn *conn)
{
	if(strcmp(repmgr_schema_quoted, "") == 0)
	{
		char	   *identifier = PQescapeIdentifier(conn, repmgr_schema,
													strlen(repmgr_schema));

		maxlen_snprintf(repmgr_schema_quoted, "%s", identifier);
		PQfreemem(identifier);
	}

	return repmgr_schema_quoted;
}


bool
create_replication_slot(PGconn *conn, char *slot_name)
{
	char		sqlquery[QUERY_STR_LEN];
	PGresult   *res;

	sqlquery_snprintf(sqlquery,
					  "SELECT * FROM pg_create_physical_replication_slot('%s')",
					  slot_name);

	log_debug(_("create_replication_slot(): Creating slot '%s' on primary\n"), slot_name);

	res = PQexec(conn, sqlquery);
	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("unable to create slot '%s' on the primary node: %s\n"),
				slot_name,
				PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	PQclear(res);
	return true;
}


bool
start_backup(PGconn *conn, char *first_wal_segment)
{
	char		sqlquery[QUERY_STR_LEN];
	PGresult   *res;

	sqlquery_snprintf(
					  sqlquery,
	  "SELECT pg_xlogfile_name(pg_start_backup('repmgr_standby_clone_%ld'))",
					  time(NULL));

	log_debug(_("standby clone: %s\n"), sqlquery);

	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("unable to start backup: %s\n"), PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	if (first_wal_segment != NULL)
	{
		char	   *first_wal_seg_pq = PQgetvalue(res, 0, 0);
		size_t		buf_sz = strlen(first_wal_seg_pq);

		first_wal_segment = malloc(buf_sz + 1);
		xsnprintf(first_wal_segment, buf_sz + 1, "%s", first_wal_seg_pq);
	}

	PQclear(res);

	return true;
}


bool
stop_backup(PGconn *conn, char *last_wal_segment)
{
	char		sqlquery[QUERY_STR_LEN];
	PGresult   *res;

	sqlquery_snprintf(sqlquery, "SELECT pg_xlogfile_name(pg_stop_backup())");

	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("unable to stop backup: %s\n"), PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	if (last_wal_segment != NULL)
	{
		char	   *last_wal_seg_pq = PQgetvalue(res, 0, 0);
		size_t		buf_sz = strlen(last_wal_seg_pq);

		last_wal_segment =  malloc(buf_sz + 1);
		xsnprintf(last_wal_segment, buf_sz + 1, "%s", last_wal_seg_pq);
	}

	PQclear(res);

	return true;
}


bool
set_config_bool(PGconn *conn, const char *config_param, bool state)
{
	char		sqlquery[QUERY_STR_LEN];
	PGresult   *res;

	sqlquery_snprintf(sqlquery,
					  "SET %s TO %s",
					  config_param,
					  state ? "TRUE" : "FALSE");

	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err("unable to set '%s': %s\n", config_param, PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	PQclear(res);

	return true;
}


/*
 * copy_configuration()
 *
 * Copy records in master's `repl_nodes` table to witness database
 *
 * This is used by `repmgr` when setting up the witness database, and
 * `repmgrd` after a failover event occurs
 */
bool
copy_configuration(PGconn *masterconn, PGconn *witnessconn, char *cluster_name)
{
	char		sqlquery[MAXLEN];
	PGresult   *res;
	int			i;

	sqlquery_snprintf(sqlquery, "TRUNCATE TABLE %s.repl_nodes", get_repmgr_schema_quoted(witnessconn));
	log_debug("copy_configuration: %s\n", sqlquery);
	res = PQexec(witnessconn, sqlquery);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "Cannot clean node details in the witness, %s\n",
				PQerrorMessage(witnessconn));
		return false;
	}

	sqlquery_snprintf(sqlquery,
					  "SELECT id, type, upstream_node_id, name, conninfo, priority, slot_name FROM %s.repl_nodes",
					  get_repmgr_schema_quoted(masterconn));
	res = PQexec(masterconn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Can't get configuration from master: %s\n",
				PQerrorMessage(masterconn));
		PQclear(res);
		return false;
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		bool node_record_created;
		char *witness = PQgetvalue(res, i, 4);

		log_debug(_("copy_configuration(): %s\n"), witness);

		node_record_created = create_node_record(witnessconn,
												 "copy_configuration",
												 atoi(PQgetvalue(res, i, 0)),
												 PQgetvalue(res, i, 1),
												 strlen(PQgetvalue(res, i, 2))
												   ? atoi(PQgetvalue(res, i, 2))
												   : NO_UPSTREAM_NODE,
												 cluster_name,
												 PQgetvalue(res, i, 3),
												 PQgetvalue(res, i, 4),
												 atoi(PQgetvalue(res, i, 5)),
												 strlen(PQgetvalue(res, i, 6))
													? PQgetvalue(res, i, 6)
													: NULL
												 );

		if (node_record_created == false)
		{
			fprintf(stderr, "Unable to copy node record to witness database: %s\n",
					PQerrorMessage(witnessconn));
			return false;
		}
	}
	PQclear(res);

	return true;
}


/*
 * create_node_record()
 *
 * Create an entry in the `repl_nodes` table.
 *
 * XXX we should pass the record parameters as a struct.
 */
bool
create_node_record(PGconn *conn, char *action, int node, char *type, int upstream_node, char *cluster_name, char *node_name, char *conninfo, int priority, char *slot_name)
{
	char		sqlquery[QUERY_STR_LEN];
	char		upstream_node_id[MAXLEN];
	char		slot_name_buf[MAXLEN];
	PGresult   *res;

	if(upstream_node == NO_UPSTREAM_NODE)
	{
		/*
		 * No explicit upstream node id provided for standby - attempt to
		 * get primary node id
		 */
		if(strcmp(type, "standby") == 0)
		{
			int primary_node_id = get_primary_node_id(conn, cluster_name);
			maxlen_snprintf(upstream_node_id, "%i", primary_node_id);
		}
		else
		{
			maxlen_snprintf(upstream_node_id, "%s", "NULL");
		}
	}
	else
	{
		maxlen_snprintf(upstream_node_id, "%i", upstream_node);
	}

	if(slot_name != NULL && slot_name[0])
	{
		maxlen_snprintf(slot_name_buf, "'%s'", slot_name);
	}
	else
	{
		maxlen_snprintf(slot_name_buf, "%s", "NULL");
	}

	sqlquery_snprintf(sqlquery,
					  "INSERT INTO %s.repl_nodes "
					  "       (id, type, upstream_node_id, cluster, "
					  "        name, conninfo, slot_name, priority) "
					  "VALUES (%i, '%s', %s, '%s', '%s', '%s', %s, %i) ",
					  get_repmgr_schema_quoted(conn),
					  node,
					  type,
					  upstream_node_id,
					  cluster_name,
					  node_name,
					  conninfo,
					  slot_name_buf,
					  priority);

	if(action != NULL)
	{
		log_debug(_("%s: %s\n"), action, sqlquery);
	}

	res = PQexec(conn, sqlquery);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_warning(_("Cannot insert node details, %s\n"),
					PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	PQclear(res);

	return true;
}


bool
delete_node_record(PGconn *conn, int node, char *action)
{
	char		sqlquery[QUERY_STR_LEN];
	PGresult   *res;

	sqlquery_snprintf(sqlquery,
					  "DELETE FROM %s.repl_nodes "
					  " WHERE id = %d",
					  get_repmgr_schema_quoted(conn),
					  node);
	if(action != NULL)
	{
		log_debug(_("%s: %s\n"), action, sqlquery);
	}

	res = PQexec(conn, sqlquery);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_warning(_("Cannot delete node details, %s\n"),
					PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	PQclear(res);
	return true;
}

