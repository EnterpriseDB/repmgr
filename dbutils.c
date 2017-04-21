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


PGconn *
establish_db_connection_as_user(const char *conninfo,
								const char *user,
								const bool exit_on_error)
{
	PGconn	   *conn = NULL;
	t_conninfo_param_list conninfo_params;
	bool	    parse_success;
	char	   *errmsg = NULL;

	initialize_conninfo_params(&conninfo_params, false);

	parse_success = parse_conninfo_string(conninfo, &conninfo_params, errmsg, true);

	if (parse_success == false)
	{
		log_error(_("unable to pass provided conninfo string:\n  %s"), errmsg);
		return NULL;
	}

	param_set(&conninfo_params, "user", user);

	conn = establish_db_connection_by_params((const char**)conninfo_params.keywords,
											 (const char**)conninfo_params.values,
											 false);

	return conn;
}


PGconn *
establish_db_connection_by_params(const char *keywords[], const char *values[],
								  const bool exit_on_error)
{
	PGconn	   *conn;
	bool	    replication_connection = false;
	int	   	    i;

	/* Connect to the database using the provided parameters */
	conn = PQconnectdbParams(keywords, values, true);

	/* Check to see that the backend connection was successfully made */
	if ((PQstatus(conn) != CONNECTION_OK))
	{
		log_error(_("connection to database failed:\n   %s"),
				  PQerrorMessage(conn));
		if (exit_on_error)
		{
			PQfinish(conn);
			exit(ERR_DB_CON);
		}
	}
	else
	{
		/*
		 * set "synchronous_commit" to "local" in case synchronous replication is in
		 * use (provided this is not a replication connection)
		 */

		for (i = 0; keywords[i]; i++)
		{
			if (strcmp(keywords[i], "replication") == 0)
				replication_connection = true;
		}

		if (replication_connection == false && set_config(conn, "synchronous_commit", "local") == false)
		{
			if (exit_on_error)
			{
				PQfinish(conn);
				exit(ERR_DB_CON);
			}
		}
	}

	return conn;
}

/* =============================== */
/* conninfo manipulation functions */
/* =============================== */


void
initialize_conninfo_params(t_conninfo_param_list *param_list, bool set_defaults)
{
	PQconninfoOption *defs = NULL;
	PQconninfoOption *def;
	int c;

	defs = PQconndefaults();
	param_list->size = 0;

	/* Count maximum number of parameters */
	for (def = defs; def->keyword; def++)
		param_list->size ++;

	/* Initialize our internal parameter list */
	param_list->keywords = pg_malloc0(sizeof(char *) * (param_list->size + 1));
	param_list->values = pg_malloc0(sizeof(char *) * (param_list->size + 1));

	for (c = 0; c < param_list->size; c++)
	{
		param_list->keywords[c] = NULL;
		param_list->values[c] = NULL;
	}

	if (set_defaults == true)
	{
		/* Pre-set any defaults */

		for (def = defs; def->keyword; def++)
		{
			if (def->val != NULL && def->val[0] != '\0')
			{
				param_set(param_list, def->keyword, def->val);
			}
		}
	}
}


void
copy_conninfo_params(t_conninfo_param_list *dest_list, t_conninfo_param_list *source_list)
{
	int c;
	for (c = 0; c < source_list->size && source_list->keywords[c] != NULL; c++)
	{
		if (source_list->values[c] != NULL && source_list->values[c][0] != '\0')
		{
			param_set(dest_list, source_list->keywords[c], source_list->values[c]);
		}
	}
}

void
param_set(t_conninfo_param_list *param_list, const char *param, const char *value)
{
	int c;
	int value_len = strlen(value) + 1;

	/*
	 * Scan array to see if the parameter is already set - if not, replace it
	 */
	for (c = 0; c < param_list->size && param_list->keywords[c] != NULL; c++)
	{
		if (strcmp(param_list->keywords[c], param) == 0)
		{
			if (param_list->values[c] != NULL)
				pfree(param_list->values[c]);

			param_list->values[c] = pg_malloc0(value_len);
			strncpy(param_list->values[c], value, value_len);

			return;
		}
	}

	/*
	 * Parameter not in array - add it and its associated value
	 */
	if (c < param_list->size)
	{
		int param_len = strlen(param) + 1;
		param_list->keywords[c] = pg_malloc0(param_len);
		param_list->values[c] = pg_malloc0(value_len);

		strncpy(param_list->keywords[c], param, param_len);
		strncpy(param_list->values[c], value, value_len);
	}

	/*
	 * It's theoretically possible a parameter couldn't be added as
	 * the array is full, but it's highly improbable so we won't
	 * handle it at the moment.
	 */
}


char *
param_get(t_conninfo_param_list *param_list, const char *param)
{
	int c;

	for (c = 0; c < param_list->size && param_list->keywords[c] != NULL; c++)
	{
		if (strcmp(param_list->keywords[c], param) == 0)
		{
			if (param_list->values[c] != NULL && param_list->values[c][0] != '\0')
				return param_list->values[c];
            else
                return NULL;
		}
	}

	return NULL;
}


/*
 * Parse a conninfo string into a t_conninfo_param_list
 *
 * See conn_to_param_list() to do the same for a PQconn
 */
bool
parse_conninfo_string(const char *conninfo_str, t_conninfo_param_list *param_list, char *errmsg, bool ignore_application_name)
{
	PQconninfoOption *connOptions;
	PQconninfoOption *option;

	connOptions = PQconninfoParse(conninfo_str, &errmsg);

	if (connOptions == NULL)
		return false;

	for (option = connOptions; option && option->keyword; option++)
	{
		/* Ignore non-set or blank parameter values*/
		if ((option->val == NULL) ||
		   (option->val != NULL && option->val[0] == '\0'))
			continue;

		/* Ignore application_name */
		if (ignore_application_name == true && strcmp(option->keyword, "application_name") == 0)
			continue;

		param_set(param_list, option->keyword, option->val);
	}

	return true;
}

/*
 * Parse a PQconn into a t_conninfo_param_list
 *
 * See parse_conninfo_string() to do the same for a conninfo string
 */
void
conn_to_param_list(PGconn *conn, t_conninfo_param_list *param_list)
{
	PQconninfoOption *connOptions;
	PQconninfoOption *option;

	connOptions = PQconninfo(conn);
	for (option = connOptions; option && option->keyword; option++)
	{
		/* Ignore non-set or blank parameter values*/
		if ((option->val == NULL) ||
		   (option->val != NULL && option->val[0] == '\0'))
			continue;

		param_set(param_list, option->keyword, option->val);
	}
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

int
is_standby(PGconn *conn)
{
	PGresult   *res;
	int			result = 0;
	char	   *sqlquery = "SELECT pg_catalog.pg_is_in_recovery()";

	log_verbose(LOG_DEBUG, "is_standby(): %s", sqlquery);

	res = PQexec(conn, sqlquery);

	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to determine if server is in recovery: %s"),
				PQerrorMessage(conn));
		result = -1;
	}
	else if (PQntuples(res) == 1 && strcmp(PQgetvalue(res, 0, 0), "t") == 0)
	{
		result = 1;
	}

	PQclear(res);
	return result;
}
