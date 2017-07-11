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

static PGconn  *_get_primary_connection(PGconn *standby_conn, int *primary_id, char *primary_conninfo_out, bool quiet);

static bool _set_config(PGconn *conn, const char *config_param, const char *sqlquery);
static RecordStatus _get_node_record(PGconn *conn, char *sqlquery, t_node_info *node_info);
static void _populate_node_record(PGresult *res, t_node_info *node_info, int row);

static void _populate_node_records(PGresult *res, NodeInfoList *node_list);

static bool _create_update_node_record(PGconn *conn, char *action, t_node_info *node_info);
static bool	_create_event(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details, t_event_info *event_info, bool send_notification);

/* ================= */
/* utility functions */
/* ================= */

XLogRecPtr
parse_lsn(const char *str)
{
	XLogRecPtr ptr = InvalidXLogRecPtr;
	uint32 high, low;

	if (sscanf(str, "%x/%x", &high, &low) == 2)
		ptr = (((XLogRecPtr)high) << 32) + (XLogRecPtr)low;

	return ptr;
}


/*
 * Wrap query with appropriate DDL function, if required.
 */
void
wrap_ddl_query(PQExpBufferData *query_buf, int replication_type, const char *fmt, ...)
{
	va_list		arglist;
	char		buf[MAXLEN];

	if (replication_type == REPLICATION_TYPE_BDR)
	{
		appendPQExpBuffer(query_buf, "SELECT bdr.bdr_replicate_ddl_command($repmgr$");
	}

	va_start(arglist, fmt);
	vsnprintf(buf, MAXLEN, fmt, arglist);
	va_end(arglist);

	appendPQExpBuffer(query_buf, "%s", buf);

	if (replication_type == REPLICATION_TYPE_BDR)
	{
		appendPQExpBuffer(query_buf, "$repmgr$)");
	}
}


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


PGconn
*establish_primary_db_connection(PGconn *conn,
								const bool exit_on_error)
{
	t_node_info  primary_node_info = T_NODE_INFO_INITIALIZER;
	bool primary_record_found;

	primary_record_found = get_primary_node_record(conn, &primary_node_info);

	if (primary_record_found == false)
	{
		return NULL;
	}

	return establish_db_connection(primary_node_info.conninfo,
								   exit_on_error);
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
		bool		replication_connection = false;
		int			i;

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

	PQconninfoFree(connOptions);

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

			/* XXX escape value */
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
	int         server_version_num;

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
		strcpy(server_version, PQgetvalue(res, 0, 1));

	server_version_num = atoi(PQgetvalue(res, 0, 0));

    PQclear(res);
	return server_version_num;
}


RecoveryType
get_recovery_type(PGconn *conn)
{
	PGresult   *res;
	RecoveryType recovery_type = RECTYPE_PRIMARY;

	char	   *sqlquery = "SELECT pg_catalog.pg_is_in_recovery()";

	log_verbose(LOG_DEBUG, "get_recovery_type(): %s", sqlquery);

	res = PQexec(conn, sqlquery);

	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to determine if server is in recovery:\n  %s"),
				PQerrorMessage(conn));
		recovery_type = RECTYPE_UNKNOWN;
	}
	else if (PQntuples(res) == 1 && strcmp(PQgetvalue(res, 0, 0), "t") == 0)
	{
		recovery_type = RECTYPE_STANDBY;
	}

	PQclear(res);
	return recovery_type;
}

/*
 * Read the node list from the provided connection and attempt to connect to each node
 * in turn to definitely establish if it's the cluster primary.
 *
 * The node list is returned in the order which makes it likely that the
 * current primary will be returned first, reducing the number of speculative
 * connections which need to be made to other nodes.
 *
 * If primary_conninfo_out points to allocated memory of MAXCONNINFO in length,
 * the primary server's conninfo string will be copied there.
 */

PGconn *
_get_primary_connection(PGconn *conn,
					  int *primary_id, char *primary_conninfo_out, bool quiet)
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
	if (primary_conninfo_out != NULL)
		remote_conninfo = primary_conninfo_out;

	if (primary_id != NULL)
	{
		*primary_id = NODE_NOT_FOUND;
	}

	/* find all registered nodes  */
	log_info(_("retrieving node list"));

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "  SELECT node_id, conninfo, "
					  "         CASE WHEN type = 'primary' THEN 1 ELSE 2 END AS type_priority"
					  "	   FROM repmgr.nodes "
					  "   WHERE active IS TRUE "
					  "ORDER BY active DESC, type_priority, priority, node_id");

	log_verbose(LOG_DEBUG, "get_primary_connection():\n%s", query.data);

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
		RecoveryType recovery_type;

		/* initialize with the values of the current node being processed */
		node_id = atoi(PQgetvalue(res, i, 0));
		strncpy(remote_conninfo, PQgetvalue(res, i, 1), MAXCONNINFO);
		log_verbose(LOG_INFO,
					_("checking role of node '%i'"),
					node_id);

		if (quiet)
		{
			remote_conn = establish_db_connection_quiet(remote_conninfo);
		}
		else
		{
			remote_conn = establish_db_connection(remote_conninfo, false);
		}

		if (PQstatus(remote_conn) != CONNECTION_OK)
			continue;

		recovery_type = get_recovery_type(remote_conn);

		if (recovery_type == RECTYPE_UNKNOWN)
		{
			log_error(_("unable to retrieve recovery state from node %i:\n	%s"),
					  node_id,
					  PQerrorMessage(remote_conn));
			PQfinish(remote_conn);
			continue;
		}

		if (recovery_type == RECTYPE_PRIMARY)
		{
			PQclear(res);
			log_debug(_("get_primary_connection(): current primary node is %i"), node_id);

			if (primary_id != NULL)
			{
				*primary_id = node_id;
			}

			return remote_conn;
		}

		PQfinish(remote_conn);
	}

	PQclear(res);
	return NULL;
}

PGconn *
get_primary_connection(PGconn *conn,
					  int *primary_id, char *primary_conninfo_out)
{
	return _get_primary_connection(conn, primary_id, primary_conninfo_out, false);
}


PGconn *
get_primary_connection_quiet(PGconn *conn,
							int *primary_id, char *primary_conninfo_out)
{
	return _get_primary_connection(conn, primary_id, primary_conninfo_out, true);
}


/*
 * Return the id of the active primary node, or NODE_NOT_FOUND if no
 * record available.
 *
 * This reports the value stored in the database only and
 * does not verify whether the node is actually available
 */
int
get_primary_node_id(PGconn *conn)
{
	PQExpBufferData	  query;
	PGresult   *res;
	int			retval;

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SELECT node_id		  "
					  "	 FROM repmgr.nodes	  "
					  " WHERE type = 'primary' "
					  "   AND active IS TRUE  ");

	log_verbose(LOG_DEBUG, "get_primary_node_id():\n%s", query.data);

	res = PQexec(conn, query.data);

	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("get_primary_node_id(): query failed\n  %s"),
				  PQerrorMessage(conn));
		retval = NODE_NOT_FOUND;
	}
	else if (PQntuples(res) == 0)
	{
		log_verbose(LOG_WARNING, _("get_primary_node_id(): no active primary found\n"));
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

ExtensionStatus
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


static RecordStatus
_get_node_record(PGconn *conn, char *sqlquery, t_node_info *node_info)
{
	int         ntuples;
	PGresult   *res;

	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		return RECORD_ERROR;
	}

	ntuples = PQntuples(res);

	if (ntuples == 0)
	{
		PQclear(res);
		return RECORD_NOT_FOUND;
	}

	_populate_node_record(res, node_info, 0);

	PQclear(res);

	return RECORD_FOUND;
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
	strncpy(node_info->repluser, PQgetvalue(res, row, 5), NAMEDATALEN);
	strncpy(node_info->slot_name, PQgetvalue(res, row, 6), MAXLEN);
	strncpy(node_info->location, PQgetvalue(res, row, 7), MAXLEN);
	node_info->priority = atoi(PQgetvalue(res, row, 8));
	node_info->active = atobool(PQgetvalue(res, row, 9));

	/* Set remaining struct fields with default values */
	node_info->is_ready = false;
	node_info->is_visible = false;
	node_info->last_wal_receive_lsn = InvalidXLogRecPtr;
}


t_server_type
parse_node_type(const char *type)
{
	if (strcmp(type, "primary") == 0)
	{
		return PRIMARY;
	}
	else if (strcmp(type, "standby") == 0)
	{
		return STANDBY;
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
		case PRIMARY:
			return "primary";
		case STANDBY:
			return "standby";
		case BDR:
			return "bdr";
		/* this should never happen */
		case UNKNOWN:
		default:
			log_error(_("unknown node type %i"), type);
			return "unknown";
	}
}


RecordStatus
get_node_record(PGconn *conn, int node_id, t_node_info *node_info)
{
	PQExpBufferData	  query;
	RecordStatus	  result;

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SELECT node_id, type, upstream_node_id, node_name, conninfo, repluser, slot_name, location, priority, active"
					  "  FROM repmgr.nodes "
					  " WHERE node_id = %i",
					  node_id);

	log_verbose(LOG_DEBUG, "get_node_record():\n%s", query.data);

	result = _get_node_record(conn, query.data, node_info);
	termPQExpBuffer(&query);

	if (result == RECORD_NOT_FOUND)
	{
		log_verbose(LOG_DEBUG, "get_node_record(): no record found for node %i", node_id);
	}

	return result;
}


RecordStatus
get_node_record_by_name(PGconn *conn, const char *node_name, t_node_info *node_info)
{
	PQExpBufferData	  query;
	RecordStatus	  record_status;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
		"SELECT node_id, type, upstream_node_id, node_name, conninfo, repluser, slot_name, location, priority, active"
		"  FROM repmgr.nodes "
		" WHERE node_name = '%s' ",
		node_name);

	log_verbose(LOG_DEBUG, "get_node_record_by_name():\n  %s", query.data);

	record_status = _get_node_record(conn, query.data, node_info);

	termPQExpBuffer(&query);

	if (record_status == RECORD_NOT_FOUND)
	{
		log_verbose(LOG_DEBUG, "get_node_record(): no record found for node %s",
					node_name);
	}

	return record_status;
}


t_node_info *
get_node_record_pointer(PGconn *conn, int node_id)
{
	t_node_info *node_info = pg_malloc0(sizeof(t_node_info));
	RecordStatus record_status;

	record_status = get_node_record(conn, node_id, node_info);

	if (record_status != RECORD_FOUND)
	{
		pfree(node_info);
		return NULL;
	}

	return node_info;
}


bool
get_primary_node_record(PGconn *conn, t_node_info *node_info)
{
	RecordStatus record_status;

	int primary_node_id = get_primary_node_id(conn);

	if (primary_node_id == UNKNOWN_NODE_ID)
	{
		return false;
	}

	record_status = get_node_record(conn, primary_node_id, node_info);

	return record_status == RECORD_FOUND ? true : false;
}


/*
 * Get the local node record; if this fails, exit. Many operations
 * depend on this being available, so we'll centralize the check
 * and failure messages here.
 */
bool
get_local_node_record(PGconn *conn, int node_id, t_node_info *node_info)
{
	RecordStatus	     record_status;

	record_status = get_node_record(conn, node_id, node_info);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve record for local node"));
		log_detail(_("local node id is  %i"), node_id);
		log_hint(_("check this node was correctly registered"));

		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	return true;
}


static
void _populate_node_records(PGresult *res, NodeInfoList *node_list)
{
	int				i;

	node_list->head = NULL;
	node_list->tail = NULL;
	node_list->node_count = 0;

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		return;
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		NodeInfoListCell *cell;
		cell = (NodeInfoListCell *) pg_malloc0(sizeof(NodeInfoListCell));

		cell->node_info = pg_malloc0(sizeof(t_node_info));

		_populate_node_record(res, cell->node_info, i);

		if (node_list->tail)
			node_list->tail->next = cell;
		else
			node_list->head = cell;

		node_list->tail = cell;
		node_list->node_count++;
	}

	return;
}


void
get_downstream_node_records(PGconn *conn, int node_id, NodeInfoList *node_list)
{
	PQExpBufferData	query;
	PGresult   *res;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "  SELECT node_id, type, upstream_node_id, node_name, conninfo, repluser, slot_name, location, priority, active"
					  "    FROM repmgr.nodes "
					  "   WHERE upstream_node_id = %i "
					  "ORDER BY node_id ",
					  node_id);

	log_verbose(LOG_DEBUG, "get_downstream_node_records():\n%s", query.data);

	res = PQexec(conn, query.data);

	termPQExpBuffer(&query);

	_populate_node_records(res, node_list);

	return;
}


void
get_active_sibling_node_records(PGconn *conn, int node_id, int upstream_node_id, NodeInfoList *node_list)
{
	PQExpBufferData	query;
	PGresult   *res;

	clear_node_info_list(node_list);

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "  SELECT node_id, type, upstream_node_id, node_name, conninfo, repluser, slot_name, location, priority, active"
					  "    FROM repmgr.nodes "
					  "   WHERE upstream_node_id = %i "
					  "     AND node_id != %i "
					  "     AND active IS TRUE "
					  "ORDER BY node_id ",
					  upstream_node_id,
					  node_id);

	log_verbose(LOG_DEBUG, "get_active_sibling_node_records():\n%s", query.data);

	res = PQexec(conn, query.data);

	termPQExpBuffer(&query);

	/* res cleared by this function */
	_populate_node_records(res, node_list);

	return;
}


void
get_node_records_by_priority(PGconn *conn, NodeInfoList *node_list)
{
	PQExpBufferData	query;
	PGresult   *res;

	clear_node_info_list(node_list);

	initPQExpBuffer(&query);

	appendPQExpBuffer(
		&query,
		"  SELECT node_id, type, upstream_node_id, node_name, conninfo, repluser, slot_name, location, priority, active"
		"    FROM repmgr.nodes "
		"ORDER BY priority DESC, node_name ");

	log_verbose(LOG_DEBUG, "get_node_records_by_priority():\n%s", query.data);

	res = PQexec(conn, query.data);

	termPQExpBuffer(&query);

	/* res cleared by this function */
	_populate_node_records(res, node_list);

	return;
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

	int				param_count = 10;
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
		int primary_node_id = get_primary_node_id(conn);
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
	param_values[4] = node_info->repluser;
	param_values[5] = slot_name_ptr;
	param_values[6] = node_info->location;
	param_values[7] = priority;
	param_values[8] = node_info->active == true ? "TRUE" : "FALSE";
	param_values[9] = node_id;

	initPQExpBuffer(&query);

	if (strcmp(action, "create") == 0)
	{
		appendPQExpBuffer(&query,
						  "INSERT INTO repmgr.nodes "
						  "       (node_id, type, upstream_node_id, "
						  "        node_name, conninfo, repluser, slot_name, "
						  "        location, priority, active) "
						  "VALUES ($10, $1, $2, $3, $4, $5, $6, $7, $8, $9) ");
	}
	else
	{
		appendPQExpBuffer(&query,
						  "UPDATE repmgr.nodes SET "
						  "       type = $1, "
						  "       upstream_node_id = $2, "
						  "       node_name = $3, "
						  "       conninfo = $4, "
						  "       repluser = $5, "
						  "       slot_name = $6, "
						  "       location = $7, "
						  "       priority = $8, "
						  "       active = $9 "
						  " WHERE node_id = $10 ");
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
update_node_record_set_active(PGconn *conn, int this_node_id, bool active)
{
	PQExpBufferData	  query;
	PGresult   *res;

	initPQExpBuffer(&query);

	appendPQExpBuffer(
		&query,
		"UPDATE repmgr.nodes SET active = %s "
		" WHERE id = %i",
		active == true ? "TRUE" : "FALSE",
		this_node_id);

	log_verbose(LOG_DEBUG, "update_node_record_set_active():\n  %s", query.data);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_error(_("unable to update node record:\n  %s"),
				  PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	PQclear(res);

	return true;
}


bool
update_node_record_set_primary(PGconn *conn, int this_node_id)
{
	PQExpBufferData	  query;
	PGresult   *res;

	log_debug(_("setting node %i as primary and marking existing primary as failed"),
			  this_node_id);

	begin_transaction(conn);

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "  UPDATE repmgr.nodes "
					  "     SET active = FALSE "
					  "   WHERE type = 'primary' "
					  "     AND active IS TRUE ");

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_error(_("unable to set old primary node as inactive:\n  %s"),
				  PQerrorMessage(conn));
		PQclear(res);

		rollback_transaction(conn);
		return false;
	}

	PQclear(res);

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "  UPDATE repmgr.nodes"
					  "     SET type = 'primary', "
					  "         upstream_node_id = NULL "
					  "   WHERE node_id = %i ",
					  this_node_id);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_error(_("unable to set current node %i as active primary:\n  %s"),
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
update_node_record_set_upstream(PGconn *conn, int this_node_id, int new_upstream_node_id)
{
	PQExpBufferData	  query;
	PGresult   *res;

	log_debug(_("update_node_record_set_upstream(): Updating node %i's upstream node to %i"),
			  this_node_id, new_upstream_node_id);

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "  UPDATE repmgr.nodes "
					  "     SET upstream_node_id = %i "
					  "   WHERE node_id = %i ",
					  new_upstream_node_id,
					  this_node_id);

	log_verbose(LOG_DEBUG, "update_node_record_set_upstream():\n%s\n", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_error(_("unable to set new upstream node id:\n  %s"),
				PQerrorMessage(conn));
		PQclear(res);

		return false;
	}

	PQclear(res);

	return true;
}


/*
 * Update node record following change of status
 * (e.g. inactive primary converted to standby)
 */
bool
update_node_record_status(PGconn *conn, int this_node_id, char *type, int upstream_node_id, bool active)
{
	PQExpBufferData	  query;
	PGresult   *res;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "  UPDATE repmgr.nodes "
					  "     SET type = '%s', "
					  "         upstream_node_id = %i, "
					  "         active = %s "
					  "   WHERE node_id = %i ",
					  type,
					  upstream_node_id,
					  active ? "TRUE" : "FALSE",
					  this_node_id);

	log_verbose(LOG_DEBUG, "update_node_record_status():\n  %s", query.data);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_error(_("unable to update node record:\n  %s"),
				PQerrorMessage(conn));
		PQclear(res);

		return false;
	}

	PQclear(res);

	return true;
}


/*
 * Update node record's "conninfo" and "priority" fields. Called by repmgrd
 * following a configuration file reload.
 */
bool
update_node_record_conn_priority(PGconn *conn, t_configuration_options *options)
{
	PQExpBufferData	  query;
	PGresult   *res;

	initPQExpBuffer(&query);

	appendPQExpBuffer(
		&query,
		"UPDATE repmgr.nodes "
		"   SET conninfo = '%s', "
		"       priority = %d "
		" WHERE id = %d ",
		options->conninfo,
		options->priority,
		options->node_id);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{

		PQclear(res);
		return false;
	}

	PQclear(res);
	return true;
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



void
clear_node_info_list(NodeInfoList *nodes)
{
	NodeInfoListCell *cell;
	NodeInfoListCell *next_cell;

	log_debug("clear_node_info_list() - closing open connections");

	/* close any open connections */
	for (cell = nodes->head; cell; cell = cell->next)
	{
		if (cell->node_info->conn != NULL)
		{
			PQfinish(cell->node_info->conn);
			cell->node_info->conn = NULL;
		}
	}

	log_debug("clear_node_info_list() - unlinking");

	cell = nodes->head;

	while (cell != NULL)
	{
		next_cell = cell->next;
		pfree(cell->node_info);
		pfree(cell);
		cell = next_cell;
	}
	nodes->head = NULL;
	nodes->tail = NULL;
	nodes->node_count = 0;
}


/* ====================== */
/* event record functions */
/* ====================== */


/*
 * create_event_record()
 *
 * Create a record in the "events" table, but don't execute the
 * "event_notification_command".
 */

bool
create_event_record(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details)
{
	/* create dummy t_event_info */
	t_event_info event_info = T_EVENT_INFO_INITIALIZER;

	return _create_event(conn, options, node_id, event, successful, details, &event_info, false);
}

/*
 * create_event_notification()
 *
 * If `conn` is not NULL, insert a record into the events table.
 *
 * If configuration parameter "event_notification_command" is set, also
 * attempt to execute that command.
 *
 * Returns true if all operations succeeded, false if one or more failed.
 *
 * Note this function may be called with "conn" set to NULL in cases where
 * the primary node is not available and it's therefore not possible to write
 * an event record. In this case, if `event_notification_command` is set, a
 * user-defined notification to be generated; if not, this function will have
 * no effect.
 */
bool
create_event_notification(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details)
{
	/* create dummy t_event_info */
	t_event_info event_info = T_EVENT_INFO_INITIALIZER;

	return _create_event(conn, options, node_id, event, successful, details, &event_info, true);
}


/*
 * create_event_notification_extended()
 *
 * The caller may need to pass additional parameters to the event notification
 * command (currently only the conninfo string of another node)

 */
bool
create_event_notification_extended(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details, t_event_info *event_info)
{
	return _create_event(conn, options, node_id, event, successful, details, event_info, true);
}


static bool
_create_event(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details, t_event_info *event_info, bool send_notification)
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

		log_verbose(LOG_DEBUG, "_create_event():\n  %s", query.data);

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

	log_verbose(LOG_DEBUG, "_create_event(): Event timestamp is \"%s\"", event_timestamp);

	/* an event notification command was provided - parse and execute it */
	if (send_notification == true && strlen(options->event_notification_command))
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
							log_verbose(LOG_DEBUG, "node_name: %s\n", event_info->node_name);
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

		log_debug("_create_event(): executing\n%s", parsed_command);

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
	RecordStatus		record_status;
	PGresult  		   *res;
	t_replication_slot  slot_info;

	/*
	 * Check whether slot exists already; if it exists and is active, that
	 * means another active standby is using it, which creates an error situation;
	 * if not we can reuse it as-is
	 */

	record_status = get_slot_record(conn, slot_name, &slot_info);

	if (record_status == RECORD_FOUND)
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

	log_debug(_("create_replication_slot(): creating slot '%s' on upstream"), slot_name);
	log_verbose(LOG_DEBUG, "create_replication_slot():\n%s", query.data);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		appendPQExpBuffer(error_msg,
						  _("unable to create slot '%s' on the upstream node: %s\n"),
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
					  "SELECT pg_catalog.pg_drop_replication_slot('%s')",
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


RecordStatus
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
		return RECORD_ERROR;
	}

	if (!PQntuples(res))
	{
		return RECORD_NOT_FOUND;
	}

	strncpy(record->slot_name, PQgetvalue(res, 0, 0), MAXLEN);
	strncpy(record->slot_type, PQgetvalue(res, 0, 1), MAXLEN);
	record->active = (strcmp(PQgetvalue(res, 0, 2), "t") == 0)
		? true
		: false;

	PQclear(res);

	return RECORD_FOUND;
}


/* ============================ */
/* asynchronous query functions */
/* ============================ */

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


/*
 * Wait until current query finishes, ignoring any results.
 * Usually this will be an async query or query cancellation.
 *
 * Returns 1 for success; 0 if any error ocurred; -1 if timeout reached.
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
			log_warning(_("wait_connection_availability(): could not receive data from connection. %s\n"),
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
						_("wait_connection_availability(): select() returned with error:\n  %s"),
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

	log_warning(_("wait_connection_availability(): timeout reached"));
	return -1;
}


/* =========================== */
/* node availability functions */
/* =========================== */

bool
is_server_available(const char *conninfo)
{
	PGPing status = PQping(conninfo);

	if (status == PQPING_OK)
		return true;

	return false;
}


/*
 * node voting functions
 *
 * These are intended to run under repmgrd and rely on shared memory
 */

NodeVotingStatus
get_voting_status(PGconn *conn)
{
	PGresult		   *res;
	NodeVotingStatus	voting_status;

	res = PQexec(conn, "SELECT repmgr.get_voting_status()");

	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to query repmgr.get_voting_status():\n  %s"),
				PQerrorMessage(conn));
		PQclear(res);
		return VS_UNKNOWN;
	}

	voting_status = atoi(PQgetvalue(res, 0, 0));

	PQclear(res);
	return voting_status;
}

int
request_vote(PGconn *conn, t_node_info *this_node, t_node_info *other_node, int electoral_term)
{
	PQExpBufferData	  query;
	PGresult   *res;
	int lsn_diff;

	other_node->last_wal_receive_lsn = InvalidXLogRecPtr;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT repmgr.request_vote(%i, %i)",
					  this_node->node_id,
					  electoral_term);
/*					  "SELECT repmgr.request_vote(%i, '%X/%X'::pg_lsn)",
					  this_node->node_id,
					  (uint32) (last_wal_receive_lsn >> 32),
					  (uint32) last_wal_receive_lsn);*/

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	/* check for NULL */
	if (PQgetisnull(res, 0, 0))
	{
		log_debug("XXX NULL returned by repmgr.request_vote()");
		return 0;
	}

	other_node->last_wal_receive_lsn = parse_lsn(PQgetvalue(res, 0, 0));

	PQclear(res);

	lsn_diff = this_node->last_wal_receive_lsn - other_node->last_wal_receive_lsn;

	log_debug("lsn_diff %i", lsn_diff);

	/* we're ahead */
	if (lsn_diff > 0)
	{
		log_debug("local node is ahead");
		return 1;
	}


	/* other node is ahead */
	if (lsn_diff < 0)
	{
		log_debug("other node is ahead");
		return 0;
	}

	/* tiebreak */

	/* other node is higher priority */
	if (this_node->priority < other_node->priority)
	{
		log_debug("other node has higher priority");
		return 0;
	}

	/* still tiebreak - we're the candidate, so we win */
	log_debug("win by default");
	return 1;

}


int
set_voting_status_initiated(PGconn *conn)
{
	PGresult		   *res;
	int		   		   electoral_term;

	res = PQexec(conn, "SELECT repmgr.set_voting_status_initiated()");

	electoral_term = atoi(PQgetvalue(res, 0, 0));

	PQclear(res);

	return electoral_term;
}


bool
announce_candidature(PGconn *conn, t_node_info *this_node, t_node_info *other_node, int electoral_term)
{
	PQExpBufferData	  query;
	PGresult   *res;

	bool retval;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT repmgr.other_node_is_candidate(%i, %i)",
					  this_node->node_id,
					  electoral_term);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	retval = (strcmp(PQgetvalue(res, 0, 0), "t") == 0)
		? true
		: false;

	PQclear(res);

	return retval;
}

void
notify_follow_primary(PGconn *conn, int primary_node_id)
{
	PQExpBufferData	  query;
	PGresult   *res;


	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT repmgr.notify_follow_primary(%i)",
					  primary_node_id);
	log_verbose(LOG_DEBUG, "notify_follow_primary():\n  %s", query.data);

	// XXX handle failure
	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to execute repmgr.notify_follow_primary():\n  %s"),
				PQerrorMessage(conn));
	}

	PQclear(res);
	return;
}


bool
get_new_primary(PGconn *conn, int *primary_node_id)
{
	PQExpBufferData	  query;
	PGresult   *res;

	int new_primary_node_id;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT repmgr.get_new_primary()");

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);
	// XXX handle error

	new_primary_node_id = atoi(PQgetvalue(res, 0, 0));

	if (new_primary_node_id == UNKNOWN_NODE_ID)
	{
		PQclear(res);
		return false;
	}

	PQclear(res);

	*primary_node_id = new_primary_node_id;

	return true;
}


void
reset_voting_status(PGconn *conn)
{
	PQExpBufferData	  query;
	PGresult   *res;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT repmgr.reset_voting_status()");

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	// COMMAND_OK?
	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to execute repmgr..reset_voting_status():\n  %s"),
				PQerrorMessage(conn));
	}

	PQclear(res);
	return;
}


/* ============================ */
/* replication status functions */
/* ============================ */

XLogRecPtr
get_last_wal_receive_location(PGconn *conn)
{
	PGresult		   *res;
	XLogRecPtr		    ptr = InvalidXLogRecPtr;

	res = PQexec(conn, "SELECT pg_catalog.pg_last_wal_receive_lsn()");

	if (PQresultStatus(res) == PGRES_TUPLES_OK)
	{
		ptr = parse_lsn(PQgetvalue(res, 0, 0));
	}

	PQclear(res);

	return ptr;
}

/* ============= */
/* BDR functions */
/* ============= */

bool
is_bdr_db(PGconn *conn)
{
	PQExpBufferData	  query;
	PGresult		 *res;
	bool			  is_bdr_db;

	initPQExpBuffer(&query);

	appendPQExpBuffer(
		&query,
		"SELECT COUNT(*) FROM pg_catalog.pg_namespace WHERE nspname='bdr'");

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0)
	{
		is_bdr_db = false;
	}
	else
	{
		is_bdr_db = atoi(PQgetvalue(res, 0, 0)) == 1 ? true : false;
	}

	PQclear(res);

	return is_bdr_db;
}


bool
is_bdr_repmgr(PGconn *conn)
{
	PQExpBufferData	  query;
	PGresult   *res;
	int		non_bdr_nodes;

	initPQExpBuffer(&query);

	appendPQExpBuffer(
		&query,
		"SELECT COUNT(*)"
		"  FROM repmgr.nodes"
		" WHERE type != 'bdr' ");

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0)
	{
		PQclear(res);
		return false;
	}

	non_bdr_nodes = atoi(PQgetvalue(res, 0, 0));

	PQclear(res);

	return (non_bdr_nodes == 0) ? true : false;
}


bool
is_table_in_bdr_replication_set(PGconn *conn, const char *tablename, const char *set)
{
	PQExpBufferData		query;
	PGresult			*res;
	bool				in_replication_set;

	initPQExpBuffer(&query);

	appendPQExpBuffer(
		&query,
		"SELECT COUNT(*) "
		"  FROM UNNEST(bdr.table_get_replication_sets('repmgr.%s')) AS repset "
		" WHERE repset='%s' ",
		tablename,
		set);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0)
	{
		in_replication_set = false;
	}
	else
	{
		in_replication_set = atoi(PQgetvalue(res, 0, 0)) == 1 ? true : false;
	}

	PQclear(res);

	return in_replication_set;
}



bool
add_table_to_bdr_replication_set(PGconn *conn, const char *tablename, const char *set)
{
	PQExpBufferData		query;
	PGresult		   *res;

	initPQExpBuffer(&query);

	appendPQExpBuffer(
		&query,
		"SELECT bdr.table_set_replication_sets('repmgr.%s', '{%s}')",
		tablename,
		set);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to add table 'repmgr.%s' to replication set '%s':\n  %s"),
				  tablename,
				  set,
				  PQerrorMessage(conn));

		if (res != NULL)
			PQclear(res);

		return false;
	}
	PQclear(res);

	return true;
}


bool
bdr_node_exists(PGconn *conn, const char *node_name)
{
	PQExpBufferData		query;
	PGresult		   *res;
	bool				node_exists;

	initPQExpBuffer(&query);

	appendPQExpBuffer(
		&query,
		"SELECT COUNT(*)"
		"  FROM bdr.bdr_nodes"
		" WHERE node_name = '%s'",
		node_name);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		node_exists = false;
	}
	else
	{
		node_exists = atoi(PQgetvalue(res, 0, 0)) == 1 ? true : false;
	}

	PQclear(res);

	return node_exists;
}


void
add_extension_tables_to_bdr_replication_set(PGconn *conn)
{
	PQExpBufferData		query;
	PGresult		   *res;

	initPQExpBuffer(&query);

	appendPQExpBuffer(
		&query,
		"    SELECT c.relname "
		"      FROM pg_class c "
        "INNER JOIN pg_namespace n "
		"        ON c.relnamespace = n.oid "
		"     WHERE n.nspname = 'repmgr' "
		"       AND c.relkind = 'r' ");

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		//
	}
	else
	{
		int i;

		for (i = 0; i < PQntuples(res); i++)
		{
			add_table_to_bdr_replication_set(
				conn,
				PQgetvalue(res, i, 0),
				"repmgr");
		}
	}

	PQclear(res);

	return;
}
