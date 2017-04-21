/*
 * dbutils.c - Database connection/management functions
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 *
 */

#include <unistd.h>
#include <time.h>
#include <sys/time.h>

#include "repmgr.h"


#include "catalog/pg_control.h"

static PGconn *_establish_db_connection(const char *conninfo,
										const bool exit_on_error,
										const bool log_notice,
										const bool verbose_only);
static bool _set_config(PGconn *conn, const char *config_param, const char *sqlquery);
/* ==================== */
/* Connection functions */
/* ==================== */

/*
 * _establish_db_connection()
 *
 * Connect to a database using a conninfo string.
 *
 * NOTE: *do not* use this for replication connections; instead use:
 *   establish_db_connection_by_params()
 */

static PGconn *
_establish_db_connection(const char *conninfo, const bool exit_on_error, const bool log_notice, const bool verbose_only)
{
	PGconn	   *conn = NULL;
	char		connection_string[MAXLEN];

	strncpy(connection_string, conninfo, MAXLEN);

	/* TODO: only set if not already present */
	strcat(connection_string, " fallback_application_name='repmgr'");

	log_debug(_("connecting to: '%s'"), connection_string);

	conn = PQconnectdb(connection_string);

	/* Check to see that the backend connection was successfully made */
	if ((PQstatus(conn) != CONNECTION_OK))
	{
		bool emit_log = true;

		if (verbose_only == true && verbose_logging == false)
			emit_log = false;

		if (emit_log)
		{
			if (log_notice)
			{
				log_notice(_("connection to database failed: %s"),
						   PQerrorMessage(conn));
			}
			else
			{
				log_error(_("connection to database failed: %s"),
						  PQerrorMessage(conn));
			}
		}

		if (exit_on_error)
		{
			PQfinish(conn);
			exit(ERR_DB_CON);
		}
	}

	/*
	 * set "synchronous_commit" to "local" in case synchronous replication is in use
	 *
	 * XXX set this explicitly before any write operations
	 */

	else if (set_config(conn, "synchronous_commit", "local") == false)
	{
		if (exit_on_error)
		{
			PQfinish(conn);
			exit(ERR_DB_CON);
		}
	}

	return conn;
}


/*
 * Establish a database connection, optionally exit on error
 */
PGconn *
establish_db_connection(const char *conninfo, const bool exit_on_error)
{
	return _establish_db_connection(conninfo, exit_on_error, false, false);
}

/* ========================== */
/* GUC manipulation functions */
/* ========================== */

static bool
_set_config(PGconn *conn, const char *config_param, const char *sqlquery)
{
	PGresult   *res;

	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_error("unable to set '%s': %s", config_param, PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	PQclear(res);

	return true;
}

bool
set_config(PGconn *conn, const char *config_param,  const char *config_value)
{
	char		sqlquery[MAX_QUERY_LEN];

	sqlquery_snprintf(sqlquery,
					  "SET %s TO '%s'",
					  config_param,
					  config_value);

	log_verbose(LOG_DEBUG, "set_config():\n%s", sqlquery);

	return _set_config(conn, config_param, sqlquery);
}

bool
set_config_bool(PGconn *conn, const char *config_param, bool state)
{
	char		sqlquery[MAX_QUERY_LEN];

	sqlquery_snprintf(sqlquery,
					  "SET %s TO %s",
					  config_param,
					  state ? "TRUE" : "FALSE");

	log_verbose(LOG_DEBUG, "set_config_bool():\n%s\n", sqlquery);

	return _set_config(conn, config_param, sqlquery);
}

/* ============================ */
/* Server information functions */
/* ============================ */

/*
 * Return the server version number for the connection provided
 */
int
get_server_version(PGconn *conn, char *server_version)
{
	PGresult   *res;
	res = PQexec(conn,
				 "SELECT pg_catalog.current_setting('server_version_num'), "
				 "       pg_catalog.current_setting('server_version')");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to determine server version number:\n%s"),
				  PQerrorMessage(conn));
		PQclear(res);
		return -1;
	}

	if (server_version != NULL)
		strcpy(server_version, PQgetvalue(res, 0, 0));

	return atoi(PQgetvalue(res, 0, 0));
}
