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
#include "dbutils.h"

#include "catalog/pg_control.h"

static PGconn *_establish_db_connection(const char *conninfo,
										const bool exit_on_error,
										const bool log_notice,
										const bool verbose_only);

static bool _set_config(PGconn *conn, const char *config_param, const char *sqlquery);
static int _get_node_record(PGconn *conn, char *sqlquery, t_node_info *node_info);
static void _populate_node_record(PGresult *res, t_node_info *node_info, int row);
static bool _create_update_node_record(PGconn *conn, char *action, t_node_info *node_info);
static bool		_create_event_record(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details, t_event_info *event_info);


/* ==================== */
/* Connection functions */
/* ==================== */

/*
 * _establish_db_connection()
 *
 * Connect to a database using a conninfo string.
 *
 * NOTE: *do not* use this for replication connections; instead use:
 *	 establish_db_connection_by_params()
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
			log_detail(_("attempted to connect using:\n  %s"),
					   connection_string);
		}

		if (exit_on_error)
		{
			PQfinish(conn);
			exit(ERR_DB_CONN);
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
			exit(ERR_DB_CONN);
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

/*
 * Attempt to establish a database connection, never exit on error, only
 * output error messages if --verbose option used
 */
PGconn *
establish_db_connection_quiet(const char *conninfo)
{
	return _establish_db_connection(conninfo, false, false, true);
}

PGconn *
establish_db_connection_as_user(const char *conninfo,
								const char *user,
								const bool exit_on_error)
{
	PGconn	   *conn = NULL;
	t_conninfo_param_list conninfo_params;
	bool		parse_success;
	char	   *errmsg = NULL;

	initialize_conninfo_params(&conninfo_params, false);

	parse_success = parse_conninfo_string(conninfo, &conninfo_params, errmsg, true);

	if (parse_success == false)
	{
		log_error(_("unable to pass provided conninfo string:\n	 %s"), errmsg);
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
	bool		replication_connection = false;
	int			i;

	/* Connect to the database using the provided parameters */
	conn = PQconnectdbParams(keywords, values, true);

	/* Check to see that the backend connection was successfully made */
	if ((PQstatus(conn) != CONNECTION_OK))
	{
		log_error(_("connection to database failed:\n	%s"),
				  PQerrorMessage(conn));
		if (exit_on_error)
		{
			PQfinish(conn);
			exit(ERR_DB_CONN);
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
				exit(ERR_DB_CONN);
			}
		}
	}

	return conn;
}


bool
is_superuser_connection(PGconn *conn, t_connection_user *userinfo)
{
	char			 *current_user;
	const char		 *superuser_status;
	bool			  is_superuser;

	current_user = PQuser(conn);
	superuser_status = PQparameterStatus(conn, "is_superuser");
	is_superuser = (strcmp(superuser_status, "on") == 0) ? true : false;

	if (userinfo != NULL)
	{
		strncpy(userinfo->username, current_user, MAXLEN);
		userinfo->is_superuser = is_superuser;
	}

	return is_superuser;
}


/* =============================== */
/* conninfo manipulation functions */
/* =============================== */


/*
 * get_conninfo_value()
 *
 * Extract the value represented by 'keyword' in 'conninfo' and copy
 * it to the 'output' buffer.
 *
 * Returns true on success, or false on failure (conninfo string could
 * not be parsed, or provided keyword not found).
 */

bool
get_conninfo_value(const char *conninfo, const char *keyword, char *output)
{
	PQconninfoOption *conninfo_options;
	PQconninfoOption *conninfo_option;

	conninfo_options = PQconninfoParse(conninfo, NULL);

	if (conninfo_options == NULL)
	{
		log_error(_("unable to parse provided conninfo string \"%s\""), conninfo);
		return false;
	}

	for (conninfo_option = conninfo_options; conninfo_option->keyword != NULL; conninfo_option++)
	{
		if (strcmp(conninfo_option->keyword, keyword) == 0)
		{
			if (conninfo_option->val != NULL && conninfo_option->val[0] != '\0')
			{
				strncpy(output, conninfo_option->val, MAXLEN);
				break;
			}
		}
	}

	PQconninfoFree(conninfo_options);

	return true;
}


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
		/* Ignore non-set or blank parameter values */
		if ((option->val == NULL) ||
		   (option->val != NULL && option->val[0] == '\0'))
			continue;

		param_set(param_list, option->keyword, option->val);
	}
}


/*
 * Converts param list to string; caller must free returned pointer
 */
char *
param_list_to_string(t_conninfo_param_list *param_list)
{
	int c;
	PQExpBufferData conninfo_buf;
	char *conninfo_str;
	int len;

	initPQExpBuffer(&conninfo_buf);

	for (c = 0; c < param_list->size && param_list->keywords[c] != NULL; c++)
	{
		if (param_list->values[c] != NULL && param_list->values[c][0] != '\0')
		{
			if (c > 0)
				appendPQExpBufferChar(&conninfo_buf, ' ');

			appendPQExpBuffer(&conninfo_buf,
							  "%s=%s",
							  param_list->keywords[c],
							  param_list->values[c]);
		}
	}

	len = strlen(conninfo_buf.data) + 1;
	conninfo_str = pg_malloc0(len);

	strncpy(conninfo_str, conninfo_buf.data, len);

	termPQExpBuffer(&conninfo_buf);

	return conninfo_str;
}


/* ===================== */
/* transaction functions */
/* ===================== */

bool
begin_transaction(PGconn *conn)
{
	PGresult   *res;

	log_verbose(LOG_DEBUG, "begin_transaction()");

	res = PQexec(conn, "BEGIN");

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_error(_("unable to begin transaction:\n	 %s"),
				  PQerrorMessage(conn));

		PQclear(res);
		return false;
	}

	PQclear(res);

	return true;
}


bool
commit_transaction(PGconn *conn)
{
	PGresult   *res;

	log_verbose(LOG_DEBUG, "commit_transaction()");

	res = PQexec(conn, "COMMIT");

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_error(_("unable to commit transaction:\n  %s"),
				  PQerrorMessage(conn));
		PQclear(res);

		return false;
	}

	PQclear(res);

	return true;
}


bool
rollback_transaction(PGconn *conn)
{
	PGresult   *res;

	log_verbose(LOG_DEBUG, "rollback_transaction()");

	res = PQexec(conn, "ROLLBACK");

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_error(_("unable to rollback transaction:\n	%s"),
				  PQerrorMessage(conn));
		PQclear(res);

		return false;
	}

	PQclear(res);

	return true;
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
set_config(PGconn *conn, const char *config_param,	const char *config_value)
{
	PQExpBufferData	  query;

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SET %s TO '%s'",
					  config_param,
					  config_value);

	log_verbose(LOG_DEBUG, "set_config():\n  %s", query.data);

	return _set_config(conn, config_param, query.data);
}

bool
set_config_bool(PGconn *conn, const char *config_param, bool state)
{
	PQExpBufferData	  query;

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SET %s TO %s",
					  config_param,
					  state ? "TRUE" : "FALSE");

	log_verbose(LOG_DEBUG, "set_config_bool():\n  %s", query.data);

	return _set_config(conn, config_param, query.data);
}


int
guc_set(PGconn *conn, const char *parameter, const char *op,
		const char *value)
{
	PQExpBufferData	  query;
	PGresult   *res;
	int			retval = 1;

	char *escaped_parameter = escape_string(conn, parameter);
	char *escaped_value     = escape_string(conn, value);

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SELECT true FROM pg_catalog.pg_settings "
					  " WHERE name = '%s' AND setting %s '%s'",
					  escaped_parameter, op, escaped_value);

	log_verbose(LOG_DEBUG, "guc_set():\n%s", query.data);

	res = PQexec(conn, query.data);

	termPQExpBuffer(&query);
	pfree(escaped_parameter);
	pfree(escaped_value);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("guc_set(): unable to execute query\n%s"),
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
	PQExpBufferData	  query;
	PGresult   *res;
	int			retval = 1;

	char *escaped_parameter = escape_string(conn, parameter);
	char *escaped_value     = escape_string(conn, value);

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SELECT true FROM pg_catalog.pg_settings "
					  " WHERE name = '%s' AND setting::%s %s '%s'::%s",
					  parameter, datatype, op, value, datatype);

	log_verbose(LOG_DEBUG, "guc_set_typed():\n%s", query.data);

	res = PQexec(conn, query.data);

	termPQExpBuffer(&query);
	pfree(escaped_parameter);
	pfree(escaped_value);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("guc_set_typed(): unable to execute query\n  %s"),
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
get_pg_setting(PGconn *conn, const char *setting, char *output)
{
	PQExpBufferData	  query;
	PGresult   *res;
	int			i;
	bool        success = false;

	char *escaped_setting = escape_string(conn, setting);

	if (escaped_setting == NULL)
	{
		log_error(_("unable to escape setting '%s'"), setting);
		return false;
	}

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SELECT name, setting "
					  "  FROM pg_catalog.pg_settings WHERE name = '%s'",
					  escaped_setting);

	log_verbose(LOG_DEBUG, "get_pg_setting(): %s\n", query.data);

	res = PQexec(conn, query.data);

	termPQExpBuffer(&query);
	pfree(escaped_setting);

	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("get_pg_setting() - PQexec failed: %s"),
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
			/* XXX highly unlikely this would ever happen */
			log_error(_("get_pg_setting(): unknown parameter \"%s\""), PQgetvalue(res, i, 0));
		}
	}

	if (success == true)
	{
		log_verbose(LOG_DEBUG, _("get_pg_setting(): returned value is \"%s\""), output);
	}

	PQclear(res);

	return success;
}


/* ============================ */
/* Server information functions */
/* ============================ */


bool
get_cluster_size(PGconn *conn, char *size)
{
	PQExpBufferData	  query;
	PGresult   *res;

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SELECT pg_catalog.pg_size_pretty(SUM(pg_catalog.pg_database_size(oid))::bigint) "
					  "	 FROM pg_catalog.pg_database ");

	log_verbose(LOG_DEBUG, "get_cluster_size():\n%s\n", query.data);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("get_cluster_size(): unable to execute query\n%s"),
				  PQerrorMessage(conn));

		PQclear(res);
		return false;
	}

	strncpy(size, PQgetvalue(res, 0, 0), MAXLEN);

	PQclear(res);
	return true;
}

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
		log_error(_("unable to determine if server is in recovery:\n  %s"),
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

/*
 * Read the node list from the provided connection and attempt to connect to each node
 * in turn to definitely establish if it's the cluster primary.
 *
 * The node list is returned in the order which makes it likely that the
 * current primary will be returned first, reducing the number of speculative
 * connections which need to be made to other nodes.
 *
 * If master_conninfo_out points to allocated memory of MAXCONNINFO in length,
 * the primary server's conninfo string will be copied there.
 */

PGconn *
get_master_connection(PGconn *conn,
					  int *master_id, char *master_conninfo_out)
{
	PQExpBufferData	  query;

	PGconn	   *remote_conn = NULL;
	PGresult   *res;

	char		remote_conninfo_stack[MAXCONNINFO];
	char	   *remote_conninfo = &*remote_conninfo_stack;

	int			i,
				node_id;

	/*
	 * If the caller wanted to get a copy of the connection info string, sub
	 * out the local stack pointer for the pointer passed by the caller.
	 */
	if (master_conninfo_out != NULL)
		remote_conninfo = master_conninfo_out;

	if (master_id != NULL)
	{
		*master_id = NODE_NOT_FOUND;
	}

	/* find all registered nodes  */
	log_info(_("retrieving node list"));

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "  SELECT node_id, conninfo, "
					  "         CASE WHEN type = 'master' THEN 1 ELSE 2 END AS type_priority"
					  "	   FROM repmgr.nodes "
					  "   WHERE type != 'witness' "
					  "ORDER BY active DESC, type_priority, priority, node_id");

	log_verbose(LOG_DEBUG, "get_master_connection():\n%s", query.data);

	res = PQexec(conn, query.data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to retrieve node records:\n	 %s"),
				  PQerrorMessage(conn));
		PQclear(res);
		return NULL;
	}

	termPQExpBuffer(&query);

	for (i = 0; i < PQntuples(res); i++)
	{
		int is_node_standby;

		/* initialize with the values of the current node being processed */
		node_id = atoi(PQgetvalue(res, i, 0));
		strncpy(remote_conninfo, PQgetvalue(res, i, 1), MAXCONNINFO);
		log_verbose(LOG_INFO,
					_("checking role of cluster node '%i'"),
					node_id);
		remote_conn = establish_db_connection(remote_conninfo, false);

		if (PQstatus(remote_conn) != CONNECTION_OK)
			continue;

		is_node_standby = is_standby(remote_conn);

		if (is_node_standby == -1)
		{
			log_error(_("unable to retrieve recovery state from node %i:\n	%s"),
					  node_id,
					  PQerrorMessage(remote_conn));
			PQfinish(remote_conn);
			continue;
		}

		/* if is_standby() returns 0, queried node is the master */
		if (is_node_standby == 0)
		{
			PQclear(res);
			log_debug(_("get_master_connection(): current master node is %i"), node_id);

			if (master_id != NULL)
			{
				*master_id = node_id;
			}

			return remote_conn;
		}

		PQfinish(remote_conn);
	}

	PQclear(res);
	return NULL;
}



/*
 * Return the id of the active master node, or NODE_NOT_FOUND if no
 * record available.
 *
 * This reports the value stored in the database only and
 * does not verify whether the node is actually available
 */
int
get_master_node_id(PGconn *conn)
{
	PQExpBufferData	  query;
	PGresult   *res;
	int			retval;

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SELECT node_id		  "
					  "	 FROM repmgr.nodes	  "
					  " WHERE type = 'master' "
					  "   AND active IS TRUE  ");

	log_verbose(LOG_DEBUG, "get_master_node_id():\n%s", query.data);

	res = PQexec(conn, query.data);

	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("get_master_node_id(): query failed\n  %s"),
				  PQerrorMessage(conn));
		retval = NODE_NOT_FOUND;
	}
	else if (PQntuples(res) == 0)
	{
		log_verbose(LOG_WARNING, _("get_master_node_id(): no active primary found\n"));
		retval = NODE_NOT_FOUND;
	}
	else
	{
		retval = atoi(PQgetvalue(res, 0, 0));
	}
	PQclear(res);

	return retval;
}



/* ================ */
/* result functions */
/* ================ */

bool atobool(const char *value)
{
	return (strcmp(value, "t") == 0)
		? true
		: false;
}


/* =================== */
/* extension functions */
/* =================== */

t_extension_status
get_repmgr_extension_status(PGconn *conn)
{
	PQExpBufferData	  query;
	PGresult		 *res;

	/* TODO: check version */

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "	  SELECT ae.name, e.extname "
					  "     FROM pg_catalog.pg_available_extensions ae "
					  "LEFT JOIN pg_catalog.pg_extension e "
					  "       ON e.extname=ae.name "
					  "	   WHERE ae.name='repmgr' ");

	res = PQexec(conn, query.data);

	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to execute extension query:\n	%s"),
				PQerrorMessage(conn));
		PQclear(res);

		return REPMGR_UNKNOWN;
	}

	/* 1. Check extension is actually available */

	if (PQntuples(res) == 0)
	{
		return REPMGR_UNAVAILABLE;
	}

	/* 2. Check if extension installed */
	if (PQgetisnull(res, 0, 1) == 0)
	{
		return REPMGR_INSTALLED;
	}

	return REPMGR_AVAILABLE;
}


/* ===================== */
/* Node record functions */
/* ===================== */


static int
_get_node_record(PGconn *conn, char *sqlquery, t_node_info *node_info)
{
	int         ntuples;
	PGresult   *res;

	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		return NODE_RECORD_QUERY_ERROR;
	}

	ntuples = PQntuples(res);

	if (ntuples == 0)
	{
		PQclear(res);
		return NODE_RECORD_NOT_FOUND;
	}

	_populate_node_record(res, node_info, 0);

	PQclear(res);

	return ntuples;
}


static void
_populate_node_record(PGresult *res, t_node_info *node_info, int row)
{
	node_info->node_id = atoi(PQgetvalue(res, row, 0));
	node_info->type = parse_node_type(PQgetvalue(res, row, 1));

	if (PQgetisnull(res, row, 2))
	{
		node_info->upstream_node_id = NO_UPSTREAM_NODE;
	}
	else
	{
		node_info->upstream_node_id = atoi(PQgetvalue(res, row, 2));
	}

	strncpy(node_info->node_name, PQgetvalue(res, row, 3), MAXLEN);
	strncpy(node_info->conninfo, PQgetvalue(res, row, 4), MAXLEN);
	strncpy(node_info->slot_name, PQgetvalue(res, row, 5), MAXLEN);
	node_info->priority = atoi(PQgetvalue(res, row, 6));
	node_info->active = atobool(PQgetvalue(res, row, 7));

	/* Set remaining struct fields with default values */
	node_info->is_ready = false;
	node_info->is_visible = false;
	node_info->xlog_location = InvalidXLogRecPtr;
}


t_server_type
parse_node_type(const char *type)
{
	if (strcmp(type, "master") == 0)
	{
		return MASTER;
	}
	else if (strcmp(type, "standby") == 0)
	{
		return STANDBY;
	}
	else if (strcmp(type, "witness") == 0)
	{
		return WITNESS;
	}
	else if (strcmp(type, "bdr") == 0)
	{
		return BDR;
	}

	return UNKNOWN;
}

const char *
get_node_type_string(t_server_type type)
{
	switch(type)
	{
		case MASTER:
			return "master";
		case STANDBY:
			return "standby";
		case WITNESS:
			return "witness";
		case BDR:
			return "bdr";
		/* this should never happen */
		case UNKNOWN:
		default:
			log_error(_("unknown node type %i"), type);
			return "unknown";
	}
}


int
get_node_record(PGconn *conn, int node_id, t_node_info *node_info)
{
	PQExpBufferData	  query;
	int		    result;

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SELECT node_id, type, upstream_node_id, node_name, conninfo, slot_name, priority, active"
					  "  FROM repmgr.nodes "
					  " WHERE node_id = %i",
					  node_id);

	log_verbose(LOG_DEBUG, "get_node_record():\n%s", query.data);

	result = _get_node_record(conn, query.data, node_info);
	termPQExpBuffer(&query);

	if (result == NODE_RECORD_NOT_FOUND)
	{
		log_verbose(LOG_DEBUG, "get_node_record(): no record found for node %i", node_id);
	}

	return result;
}


int
get_node_record_by_name(PGconn *conn, const char *node_name, t_node_info *node_info)
{
	PQExpBufferData	  query;
	int 		result;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
		"SELECT node_id, type, upstream_node_id, node_name, conninfo, slot_name, priority, active"
		"  FROM repmgr.nodes "
		" WHERE node_name = '%s' ",
		node_name);

	log_verbose(LOG_DEBUG, "get_node_record_by_name():\n  %s", query.data);

	result = _get_node_record(conn, query.data, node_info);

	termPQExpBuffer(&query);

	if (result == 0)
	{
		log_verbose(LOG_DEBUG, "get_node_record(): no record found for node %s",
					node_name);
	}

	return result;
}



bool
create_node_record(PGconn *conn, char *repmgr_action, t_node_info *node_info)
{
	if (repmgr_action != NULL)
		log_verbose(LOG_DEBUG, "create_node_record(): action is \"%s\"", repmgr_action);

	return _create_update_node_record(conn, "create", node_info);
}

bool
update_node_record(PGconn *conn, char *repmgr_action, t_node_info *node_info)
{
	if (repmgr_action != NULL)
		log_verbose(LOG_DEBUG, "update_node_record(): action is \"%s\"", repmgr_action);

	return _create_update_node_record(conn, "update", node_info);
}

static bool
_create_update_node_record(PGconn *conn, char *action, t_node_info *node_info)
{
	PQExpBufferData	query;
	char			node_id[MAXLEN];
	char			priority[MAXLEN];

	char			upstream_node_id[MAXLEN];
	char		   *upstream_node_id_ptr = NULL;

	char			slot_name[MAXLEN];
	char		   *slot_name_ptr = NULL;

	int				param_count = 8;
	const char	   *param_values[param_count];

	PGresult   	   *res;

	maxlen_snprintf(node_id, "%i", node_info->node_id);
	maxlen_snprintf(priority, "%i", node_info->priority);

	if (node_info->upstream_node_id == NO_UPSTREAM_NODE && node_info->type == STANDBY)
	{
		/*
		 * No explicit upstream node id provided for standby - attempt to
		 * get primary node id
		 */
		int primary_node_id = get_master_node_id(conn);
		maxlen_snprintf(upstream_node_id, "%i", primary_node_id);
		upstream_node_id_ptr = upstream_node_id;
	}
	else if (node_info->upstream_node_id != NO_UPSTREAM_NODE)
	{
		maxlen_snprintf(upstream_node_id, "%i", node_info->upstream_node_id);
		upstream_node_id_ptr = upstream_node_id;
	}

	if (node_info->slot_name[0])
		maxlen_snprintf(slot_name, "%s", node_info->slot_name);


	param_values[0] = get_node_type_string(node_info->type);
	param_values[1] = upstream_node_id_ptr;
	param_values[2] = node_info->node_name;
	param_values[3] = node_info->conninfo;
	param_values[4] = slot_name_ptr;
	param_values[5] = priority;
	param_values[6] = node_info->active == true ? "TRUE" : "FALSE";
	param_values[7] = node_id;

	initPQExpBuffer(&query);

	if (strcmp(action, "create") == 0)
	{
		appendPQExpBuffer(&query,
						  "INSERT INTO repmgr.nodes "
						  "       (node_id, type, upstream_node_id, "
						  "        node_name, conninfo, slot_name, "
						  "        priority, active) "
						  "VALUES ($8, $1, $2, $3, $4, $5, $6, $7) ");
	}
	else
	{
		appendPQExpBuffer(&query,
						  "UPDATE repmgr.nodes SET "
						  "       type = $1, "
						  "       upstream_node_id = $2, "
						  "       node_name = $3, "
						  "       conninfo = $4, "
						  "       slot_name = $5, "
						  "       priority = $6, "
						  "       active = $7 "
						  " WHERE node_id = $8 ");
	}


    res = PQexecParams(conn,
                       query.data,
                       param_count,
                       NULL,
                       param_values,
                       NULL,
                       NULL,
                       0);

	termPQExpBuffer(&query);

	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_error(_("unable to %s node record:\n  %s"),
				  action,
				  PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	PQclear(res);

	return true;
}

bool
update_node_record_set_master(PGconn *conn, int this_node_id)
{
	PQExpBufferData	  query;
	PGresult   *res;

	log_debug(_("setting node %i as master and marking existing master as failed"),
			  this_node_id);

	begin_transaction(conn);

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "  UPDATE repmgr.nodes "
					  "     SET active = FALSE "
					  "   WHERE type = 'master' "
					  "     AND active IS TRUE ");

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_error(_("unable to set old master node as inactive:\n  %s"),
				  PQerrorMessage(conn));
		PQclear(res);

		rollback_transaction(conn);
		return false;
	}

	PQclear(res);
	termPQExpBuffer(&query);

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "  UPDATE repmgr.nodes"
					  "     SET type = 'master', "
					  "         upstream_node_id = NULL "
					  "   WHERE node_id = %i ",
					  this_node_id);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_error(_("unable to set current node %i as active master:\n  %s"),
				this_node_id,
				PQerrorMessage(conn));
		PQclear(res);

		rollback_transaction(conn);
		return false;
	}

	PQclear(res);

	return commit_transaction(conn);
}



bool
delete_node_record(PGconn *conn, int node)
{
	PQExpBufferData	  query;
	PGresult   *res;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "DELETE FROM repmgr.nodes "
					  " WHERE node_id = %d",
					  node);

	log_verbose(LOG_DEBUG, "delete_node_record():\n  %s", query.data);

	res = PQexec(conn, query.data);

	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_error(_("unable to delete node record:\n  %s"),
					PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	PQclear(res);
	return true;

}

/* ====================== */
/* event record functions */
/* ====================== */

/*
 * create_event_record()
 *
 * If `conn` is not NULL, insert a record into the events table.
 *
 * If configuration parameter `event_notification_command` is set, also
 * attempt to execute that command.
 *
 * Returns true if all operations succeeded, false if one or more failed.
 *
 * Note this function may be called with `conn` set to NULL in cases where
 * the master node is not available and it's therefore not possible to write
 * an event record. In this case, if `event_notification_command` is set, a
 * user-defined notification to be generated; if not, this function will have
 * no effect.
 */
bool
create_event_record(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details)
{
	t_event_info event_info = T_EVENT_INFO_INITIALIZER;

	return _create_event_record(conn, options, node_id, event, successful, details, &event_info);
}


/*
 * create_event_record_extended()
 *
 * The caller may need to pass additional parameters to the event notification
 * command (currently only the conninfo string of another node)

 */
bool
create_event_record_extended(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details, t_event_info *event_info)
{
	return _create_event_record(conn, options, node_id, event, successful, details, event_info);
}

static bool
_create_event_record(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details, t_event_info *event_info)
{
	PQExpBufferData	  query;
	PGresult   *res;
	char		event_timestamp[MAXLEN] = "";
	bool		success = true;

	/*
	 * Only attempt to write a record if a connection handle was provided.
	 * Also check that the repmgr schema has been properly initialised - if
	 * not it means no configuration file was provided, which can happen with
	 * e.g. `repmgr standby clone`, and we won't know which schema to write to.
	 */
	if (conn != NULL && PQstatus(conn) == CONNECTION_OK)
	{
		int n_node_id = htonl(node_id);
		char *t_successful = successful ? "TRUE" : "FALSE";

		const char *values[4] = { (char *)&n_node_id,
								  event,
								  t_successful,
								  details
					  			};

		int lengths[4] = { sizeof(n_node_id),
						   0,
						   0,
						   0
			  			 };

		int binary[4] = {1, 0, 0, 0};

		initPQExpBuffer(&query);
		appendPQExpBuffer(&query,
						  " INSERT INTO repmgr.events ( "
						  "             node_id, "
						  "             event, "
						  "             successful, "
						  "             details "
						  "            ) "
						  "      VALUES ($1, $2, $3, $4) "
						  "   RETURNING event_timestamp ");

		log_verbose(LOG_DEBUG, "create_event_record():\n  %s", query.data);

		res = PQexecParams(conn,
						   query.data,
						   4,
						   NULL,
						   values,
						   lengths,
						   binary,
						   0);

		termPQExpBuffer(&query);

		if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			/* we don't treat this as an error */
			log_warning(_("unable to create event record:\n  %s"),
						PQerrorMessage(conn));

			success = false;
		}
		else
		{
			/* Store timestamp to send to the notification command */
			strncpy(event_timestamp, PQgetvalue(res, 0, 0), MAXLEN);
		}

		PQclear(res);
	}

	/*
	 * If no database connection provided, or the query failed, generate a
	 * current timestamp ourselves. This isn't quite the same
	 * format as PostgreSQL, but is close enough for diagnostic use.
	 */
	if (!strlen(event_timestamp))
	{
		time_t now;
		struct tm	ts;

		time(&now);
		ts = *localtime(&now);
		strftime(event_timestamp, MAXLEN, "%Y-%m-%d %H:%M:%S%z", &ts);
	}

	log_verbose(LOG_DEBUG, "create_event_record(): Event timestamp is \"%s\"\n", event_timestamp);

	/* an event notification command was provided - parse and execute it */
	if (strlen(options->event_notification_command))
	{
		char		parsed_command[MAXPGPATH];
		const char *src_ptr;
		char	   *dst_ptr;
		char	   *end_ptr;
		int	   	    r;

		/*
		 * If configuration option 'event_notifications' was provided,
		 * check if this event is one of the ones listed; if not listed,
		 * don't execute the notification script.
		 *
		 * (If 'event_notifications' was not provided, we assume the script
		 * should be executed for all events).
		 */
		if (options->event_notifications.head != NULL)
		{
			EventNotificationListCell *cell;
			bool notify_ok = false;

			for (cell = options->event_notifications.head; cell; cell = cell->next)
			{
				if (strcmp(event, cell->event_type) == 0)
				{
					notify_ok = true;
					break;
				}
			}

			/*
			 * Event type not found in the 'event_notifications' list - return early
			 */
			if (notify_ok == false)
			{
				log_debug(_("Not executing notification script for event type '%s'\n"), event);
				return success;
			}
		}

		dst_ptr = parsed_command;
		end_ptr = parsed_command + MAXPGPATH - 1;
		*end_ptr = '\0';

		for(src_ptr = options->event_notification_command; *src_ptr; src_ptr++)
		{
			if (*src_ptr == '%')
			{
				switch (src_ptr[1])
				{
					case 'n':
						/* %n: node id */
						src_ptr++;
						snprintf(dst_ptr, end_ptr - dst_ptr, "%i", node_id);
						dst_ptr += strlen(dst_ptr);
						break;
					case 'a':
						/* %a: node name */
						src_ptr++;
						if (event_info->node_name != NULL)
						{
							log_debug("node_name: %s\n", event_info->node_name);
							strlcpy(dst_ptr, event_info->node_name, end_ptr - dst_ptr);
							dst_ptr += strlen(dst_ptr);
						}
						break;
					case 'e':
						/* %e: event type */
						src_ptr++;
						strlcpy(dst_ptr, event, end_ptr - dst_ptr);
						dst_ptr += strlen(dst_ptr);
						break;
					case 'd':
						/* %d: details */
						src_ptr++;
						if (details != NULL)
						{
							strlcpy(dst_ptr, details, end_ptr - dst_ptr);
							dst_ptr += strlen(dst_ptr);
						}
						break;
					case 's':
						/* %s: successful */
						src_ptr++;
						strlcpy(dst_ptr, successful ? "1" : "0", end_ptr - dst_ptr);
						dst_ptr += strlen(dst_ptr);
						break;
					case 't':
						/* %t: timestamp */
						src_ptr++;
						strlcpy(dst_ptr, event_timestamp, end_ptr - dst_ptr);
						dst_ptr += strlen(dst_ptr);
						break;
					case 'c':
						/* %c: conninfo for next available node */
						src_ptr++;
						if (event_info->conninfo_str != NULL)
						{
							log_debug("conninfo: %s\n", event_info->conninfo_str);

							strlcpy(dst_ptr, event_info->conninfo_str, end_ptr - dst_ptr);
							dst_ptr += strlen(dst_ptr);
						}
						break;
					default:
						/* otherwise treat the % as not special */
						if (dst_ptr < end_ptr)
							*dst_ptr++ = *src_ptr;
						break;
				}
			}
			else
			{
				if (dst_ptr < end_ptr)
					*dst_ptr++ = *src_ptr;
			}
		}

		*dst_ptr = '\0';

		log_debug("create_event_record(): executing\n%s", parsed_command);

		r = system(parsed_command);
		if (r != 0)
		{
			log_warning(_("unable to execute event notification command"));
			log_info(_("parsed event notification command was:\n  %s"), parsed_command);
			success = false;
		}
	}

	return success;
}


/* ========================== */
/* replication slot functions */
/* ========================== */

bool
create_replication_slot(PGconn *conn, char *slot_name, int server_version_num, PQExpBufferData *error_msg)
{
	PQExpBufferData		query;
	int					query_res;
	PGresult  		   *res;
	t_replication_slot  slot_info;

	/*
	 * Check whether slot exists already; if it exists and is active, that
	 * means another active standby is using it, which creates an error situation;
	 * if not we can reuse it as-is
	 */

	query_res = get_slot_record(conn, slot_name, &slot_info);

	if (query_res)
	{
		if (strcmp(slot_info.slot_type, "physical") != 0)
		{
			appendPQExpBuffer(error_msg,
							  _("slot '%s' exists and is not a physical slot\n"),
							  slot_name);
			return false;
		}

		if (slot_info.active == false)
		{
			// XXX is this a good idea?
			log_debug("replication slot '%s' exists but is inactive; reusing",
					  slot_name);

			return true;
		}

		appendPQExpBuffer(error_msg,
						  _("slot '%s' already exists as an active slot\n"),
						  slot_name);
		return false;
	}

	initPQExpBuffer(&query);

	/* In 9.6 and later, reserve the LSN straight away */
	if (server_version_num >= 90600)
	{
		appendPQExpBuffer(&query,
						  "SELECT * FROM pg_catalog.pg_create_physical_replication_slot('%s', TRUE)",
						  slot_name);
	}
	else
	{
		appendPQExpBuffer(&query,
						  "SELECT * FROM pg_catalog.pg_create_physical_replication_slot('%s')",
						  slot_name);
	}

	log_debug(_("create_replication_slot(): Creating slot '%s' on upstream"), slot_name);
	log_verbose(LOG_DEBUG, "create_replication_slot():\n%s", query.data);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		appendPQExpBuffer(error_msg,
						  _("unable to create slot '%s' on the master node: %s\n"),
						  slot_name,
						  PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	PQclear(res);
	return true;
}


bool
drop_replication_slot(PGconn *conn, char *slot_name)
{
	PQExpBufferData	  query;
	PGresult   *res;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT pg_drop_replication_slot('%s')",
					  slot_name);

	log_verbose(LOG_DEBUG, "drop_replication_slot():\n  %s", query.data);

	res = PQexec(conn, query.data);

	termPQExpBuffer(&query);

	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to drop replication slot \"%s\":\n  %s"),
				  slot_name,
				  PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	log_verbose(LOG_DEBUG, "replication slot \"%s\" successfully dropped",
				slot_name);

	return true;
}


int
get_slot_record(PGconn *conn, char *slot_name, t_replication_slot *record)
{
	PQExpBufferData	  query;
	PGresult   *res;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT slot_name, slot_type, active "
                      "  FROM pg_catalog.pg_replication_slots "
					  " WHERE slot_name = '%s' ",
					  slot_name);

	log_verbose(LOG_DEBUG, "get_slot_record():\n%s", query.data);

	res = PQexec(conn, query.data);

	termPQExpBuffer(&query);

	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to query pg_replication_slots:\n  %s"),
				PQerrorMessage(conn));
		PQclear(res);
		return -1;
	}

	if (!PQntuples(res))
	{
		return 0;
	}

	strncpy(record->slot_name, PQgetvalue(res, 0, 0), MAXLEN);
	strncpy(record->slot_type, PQgetvalue(res, 0, 1), MAXLEN);
	record->active = (strcmp(PQgetvalue(res, 0, 2), "t") == 0)
		? true
		: false;

	PQclear(res);

	return 1;
}


/* ================ */
/* backup functions */
/* ================ */

// XXX is first_wal_segment actually used anywhere?
bool
start_backup(PGconn *conn, char *first_wal_segment, bool fast_checkpoint, int server_version_num)
{
	PQExpBufferData	  query;
	PGresult   *res;

	initPQExpBuffer(&query);

	if (server_version_num >= 100000)
	{
		appendPQExpBuffer(&query,
						  "SELECT pg_catalog.pg_walfile_name(pg_catalog.pg_start_backup('repmgr_standby_clone_%ld', %s))",
						  time(NULL),
						  fast_checkpoint ? "TRUE" : "FALSE");
	}
	else
	{
		appendPQExpBuffer(&query,
						  "SELECT pg_catalog.pg_xlogfile_name(pg_catalog.pg_start_backup('repmgr_standby_clone_%ld', %s))",
						  time(NULL),
						  fast_checkpoint ? "TRUE" : "FALSE");
	}

	log_verbose(LOG_DEBUG, "start_backup():\n  %s", query.data);

	res = PQexec(conn, query.data);

	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to start backup:\n  %s"), PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	if (first_wal_segment != NULL)
	{
		char	   *first_wal_seg_pq = PQgetvalue(res, 0, 0);
		size_t		buf_sz = strlen(first_wal_seg_pq);

		first_wal_segment = pg_malloc0(buf_sz + 1);
		snprintf(first_wal_segment, buf_sz + 1, "%s", first_wal_seg_pq);
	}

	PQclear(res);

	return true;
}


bool
stop_backup(PGconn *conn, char *last_wal_segment, int server_version_num)
{
	PQExpBufferData	  query;
	PGresult   *res;

	initPQExpBuffer(&query);

	if (server_version_num >= 100000)
	{
		appendPQExpBuffer(&query,
						  "SELECT pg_catalog.pg_walfile_name(pg_catalog.pg_stop_backup())");
	}
	else
	{
		appendPQExpBuffer(&query,
						  "SELECT pg_catalog.pg_xlogfile_name(pg_catalog.pg_stop_backup())");
	}

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to stop backup:\n  %s"), PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	if (last_wal_segment != NULL)
	{
		char	   *last_wal_seg_pq = PQgetvalue(res, 0, 0);
		size_t		buf_sz = strlen(last_wal_seg_pq);

		last_wal_segment = pg_malloc0(buf_sz + 1);
		snprintf(last_wal_segment, buf_sz + 1, "%s", last_wal_seg_pq);
	}

	PQclear(res);

	return true;
}
