/*
 * dbutils.c - Database connection/management functions
 *
 * Copyright (c) 2ndQuadrant, 2010-2018
 *
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
 */

#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <dirent.h>
#include <arpa/inet.h>

#include "repmgr.h"
#include "dbutils.h"
#include "controldata.h"
#include "dirutil.h"

#define NODE_RECORD_PARAM_COUNT 11


/*
 * This is set by is_bdr_db(), which is called by every BDR-related
 * action anyway; this is required to be able to generate appropriate
 * queries for versions 2 and 3.
 */
int			bdr_version_num = UNKNOWN_BDR_VERSION_NUM;

static void log_db_error(PGconn *conn, const char *query_text, const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));

static PGconn *_establish_db_connection(const char *conninfo,
						 const bool exit_on_error,
						 const bool log_notice,
						 const bool verbose_only);

static PGconn *_get_primary_connection(PGconn *standby_conn, int *primary_id, char *primary_conninfo_out, bool quiet);

static bool _set_config(PGconn *conn, const char *config_param, const char *sqlquery);
static RecordStatus _get_node_record(PGconn *conn, char *sqlquery, t_node_info *node_info);
static void _populate_node_record(PGresult *res, t_node_info *node_info, int row);

static void _populate_node_records(PGresult *res, NodeInfoList *node_list);

static bool _create_update_node_record(PGconn *conn, char *action, t_node_info *node_info);
static bool _create_event(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details, t_event_info *event_info, bool send_notification);

static bool _is_bdr_db(PGconn *conn, PQExpBufferData *output, bool quiet);
static void _populate_bdr_node_record(PGresult *res, t_bdr_node_info *node_info, int row);
static void _populate_bdr_node_records(PGresult *res, BdrNodeInfoList *node_list);

void
log_db_error(PGconn *conn, const char *query_text, const char *fmt,...)
{
	va_list		ap;

	va_start(ap, fmt);

	log_error(fmt, ap);

	va_end(ap);

	if (conn != NULL && PQstatus(conn) == CONNECTION_OK)
	{
		log_detail("%s", PQerrorMessage(conn));
	}

	if (query_text != NULL)
	{
		log_detail("query text is:\n%s", query_text);
	}
}

/* ================= */
/* utility functions */
/* ================= */

XLogRecPtr
parse_lsn(const char *str)
{
	XLogRecPtr	ptr = InvalidXLogRecPtr;
	uint32		high,
				low;

	if (sscanf(str, "%x/%x", &high, &low) == 2)
		ptr = (((XLogRecPtr) high) << 32) + (XLogRecPtr) low;

	return ptr;
}


/*
 * Wrap query with appropriate DDL function, if required.
 */
void
wrap_ddl_query(PQExpBufferData *query_buf, int replication_type, const char *fmt,...)
{
	va_list		arglist;
	char		buf[MAXLEN];

	if (replication_type == REPLICATION_TYPE_BDR)
	{
		if (bdr_version_num < 3)
			appendPQExpBufferStr(query_buf, "SELECT bdr.bdr_replicate_ddl_command($repmgr$");
		else
			appendPQExpBufferStr(query_buf, "SELECT bdr.replicate_ddl_command($repmgr$");
	}

	va_start(arglist, fmt);
	vsnprintf(buf, MAXLEN, fmt, arglist);
	va_end(arglist);

	appendPQExpBufferStr(query_buf, buf);

	if (replication_type == REPLICATION_TYPE_BDR)
	{
		appendPQExpBufferStr(query_buf, "$repmgr$)");
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
	char	   *connection_string = NULL;
	char	   *errmsg = NULL;

	t_conninfo_param_list conninfo_params = T_CONNINFO_PARAM_LIST_INITIALIZER;
	bool		parse_success = false;

	initialize_conninfo_params(&conninfo_params, false);

	parse_success = parse_conninfo_string(conninfo, &conninfo_params, &errmsg, false);

	if (parse_success == false)
	{
		log_error(_("unable to pass provided conninfo string:\n	 %s"), errmsg);
		free_conninfo_params(&conninfo_params);
		return NULL;
	}

	/* set some default values if not explicitly provided */
	param_set_ine(&conninfo_params, "connect_timeout", "2");
	param_set_ine(&conninfo_params, "fallback_application_name", "repmgr");

	connection_string = param_list_to_string(&conninfo_params);

	log_debug(_("connecting to: \"%s\""), connection_string);

	conn = PQconnectdb(connection_string);

	/* Check to see that the backend connection was successfully made */
	if ((PQstatus(conn) != CONNECTION_OK))
	{
		bool		emit_log = true;

		if (verbose_only == true && verbose_logging == false)
			emit_log = false;

		if (emit_log)
		{
			if (log_notice)
			{
				log_notice(_("connection to database failed:\n  %s"),
						   PQerrorMessage(conn));
			}
			else
			{
				log_error(_("connection to database failed:\n  %s"),
						  PQerrorMessage(conn));
			}
			log_detail(_("attempted to connect using:\n  %s"),
					   connection_string);
		}

		if (exit_on_error)
		{
			PQfinish(conn);
			free_conninfo_params(&conninfo_params);
			exit(ERR_DB_CONN);
		}
	}

	/*
	 * set "synchronous_commit" to "local" in case synchronous replication is
	 * in use
	 *
	 * XXX set this explicitly before any write operations
	 */

	else if (set_config(conn, "synchronous_commit", "local") == false)
	{
		if (exit_on_error)
		{
			PQfinish(conn);
			free_conninfo_params(&conninfo_params);
			exit(ERR_DB_CONN);
		}
	}

	pfree(connection_string);
	free_conninfo_params(&conninfo_params);

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
establish_primary_db_connection(PGconn *conn,
								const bool exit_on_error)
{
	t_node_info primary_node_info = T_NODE_INFO_INITIALIZER;
	bool		primary_record_found = get_primary_node_record(conn, &primary_node_info);

	if (primary_record_found == false)
	{
		return NULL;
	}

	return establish_db_connection(primary_node_info.conninfo,
								   exit_on_error);
}


PGconn *
establish_db_connection_by_params(t_conninfo_param_list *param_list,
								  const bool exit_on_error)
{
	PGconn	   *conn = NULL;

	/* set some default values if not explicitly provided */
	param_set_ine(param_list, "connect_timeout", "2");
	param_set_ine(param_list, "fallback_application_name", "repmgr");

	/* Connect to the database using the provided parameters */
	conn = PQconnectdbParams((const char **) param_list->keywords, (const char **) param_list->values, true);

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
		bool		is_replication_connection = false;
		int			i;

		/*
		 * set "synchronous_commit" to "local" in case synchronous replication
		 * is in use (provided this is not a replication connection)
		 */

		for (i = 0; param_list->keywords[i]; i++)
		{
			if (strcmp(param_list->keywords[i], "replication") == 0)
				is_replication_connection = true;
		}

		if (is_replication_connection == false && set_config(conn, "synchronous_commit", "local") == false)
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
	char	   *current_user = NULL;
	const char *superuser_status = NULL;
	bool		is_superuser = false;

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


void
close_connection(PGconn **conn)
{
	if (*conn == NULL)
		return;

	PQfinish(*conn);

	*conn = NULL;
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
	PQconninfoOption *conninfo_options = NULL;
	PQconninfoOption *conninfo_option = NULL;

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


/*
 * Get a default conninfo value for the provided parameter, and copy
 * it to the 'output' buffer.
 *
 * Returns true on success, or false on failure (provided keyword not found).
 *
 */
bool
get_conninfo_default_value(const char *param, char *output, int maxlen)
{
	PQconninfoOption *defs = NULL;
	PQconninfoOption *def = NULL;
	bool found = false;

	defs = PQconndefaults();

	for (def = defs; def->keyword; def++)
	{
		if (strncmp(def->keyword, param, maxlen) == 0)
		{
			strncpy(output, def->val, maxlen);
			found = true;
		}
	}

	PQconninfoFree(defs);

	return found;
}


void
initialize_conninfo_params(t_conninfo_param_list *param_list, bool set_defaults)
{
	PQconninfoOption *defs = NULL;
	PQconninfoOption *def = NULL;
	int			c;

	defs = PQconndefaults();
	param_list->size = 0;

	/* Count maximum number of parameters */
	for (def = defs; def->keyword; def++)
		param_list->size++;

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

	PQconninfoFree(defs);
}


void
free_conninfo_params(t_conninfo_param_list *param_list)
{
	int			c;

	for (c = 0; c < param_list->size; c++)
	{
		if (param_list->keywords != NULL && param_list->keywords[c] != NULL)
			pfree(param_list->keywords[c]);

		if (param_list->values != NULL && param_list->values[c] != NULL)
			pfree(param_list->values[c]);
	}

	if (param_list->keywords != NULL)
		pfree(param_list->keywords);

	if (param_list->values != NULL)
		pfree(param_list->values);
}



void
copy_conninfo_params(t_conninfo_param_list *dest_list, t_conninfo_param_list *source_list)
{
	int			c;

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
	int			c;
	int			value_len = strlen(value) + 1;

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
		int			param_len = strlen(param) + 1;

		param_list->keywords[c] = pg_malloc0(param_len);
		param_list->values[c] = pg_malloc0(value_len);

		strncpy(param_list->keywords[c], param, param_len);
		strncpy(param_list->values[c], value, value_len);
	}

	/*
	 * It's theoretically possible a parameter couldn't be added as the array
	 * is full, but it's highly improbable so we won't handle it at the
	 * moment.
	 */
}


/*
 * Like param_set(), but will only set the parameter if it doesn't exist
 */
void
param_set_ine(t_conninfo_param_list *param_list, const char *param, const char *value)
{
	int			c;
	int			value_len = strlen(value) + 1;

	/*
	 * Scan array to see if the parameter is already set - if so, do nothing
	 */
	for (c = 0; c < param_list->size && param_list->keywords[c] != NULL; c++)
	{
		if (strcmp(param_list->keywords[c], param) == 0)
		{
			/* parameter exists, do nothing */
			return;
		}
	}

	/*
	 * Parameter not in array - add it and its associated value
	 */
	if (c < param_list->size)
	{
		int			param_len = strlen(param) + 1;

		param_list->keywords[c] = pg_malloc0(param_len);
		param_list->values[c] = pg_malloc0(value_len);

		strncpy(param_list->keywords[c], param, param_len);
		strncpy(param_list->values[c], value, value_len);
	}
}


char *
param_get(t_conninfo_param_list *param_list, const char *param)
{
	int			c;

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
 * See conn_to_param_list() to do the same for a PGconn
 *
 * "ignore_local_params": ignores those parameters specific
 * to a local installation, i.e. when parsing an upstream
 * node's conninfo string for inclusion into "primary_conninfo",
 * don't copy that node's values
 */
bool
parse_conninfo_string(const char *conninfo_str, t_conninfo_param_list *param_list, char **errmsg, bool ignore_local_params)
{
	PQconninfoOption *connOptions = NULL;
	PQconninfoOption *option = NULL;

	connOptions = PQconninfoParse(conninfo_str, errmsg);

	if (connOptions == NULL)
		return false;

	for (option = connOptions; option && option->keyword; option++)
	{
		/* Ignore non-set or blank parameter values */
		if ((option->val == NULL) ||
			(option->val != NULL && option->val[0] == '\0'))
			continue;

		/* Ignore settings specific to the upstream node */
		if (ignore_local_params == true)
		{
			if (strcmp(option->keyword, "application_name") == 0)
				continue;
			if (strcmp(option->keyword, "passfile") == 0)
				continue;
			if (strcmp(option->keyword, "servicefile") == 0)
				continue;
		}

		param_set(param_list, option->keyword, option->val);
	}

	PQconninfoFree(connOptions);

	return true;
}


/*
 * Parse a PGconn into a t_conninfo_param_list
 *
 * See parse_conninfo_string() to do the same for a conninfo string
 *
 * NOTE: the current use case for this is to take an active connection,
 * replace the existing username (typically replacing it with the superuser
 * or replication user name), and make a new connection as that user.
 * If the "password" field is set, it will cause any connection made with
 * these parameters to fail (unless of course the password happens to be the
 * same). Therefore we remove the password altogether, and rely on it being
 * available via .pgpass.
 */
void
conn_to_param_list(PGconn *conn, t_conninfo_param_list *param_list)
{
	PQconninfoOption *connOptions = NULL;
	PQconninfoOption *option = NULL;

	connOptions = PQconninfo(conn);
	for (option = connOptions; option && option->keyword; option++)
	{
		/* Ignore non-set or blank parameter values */
		if ((option->val == NULL) ||
			(option->val != NULL && option->val[0] == '\0'))
			continue;

		/* Ignore "password" */
		if (strcmp(option->keyword, "password") == 0)
			continue;

		param_set(param_list, option->keyword, option->val);
	}

	PQconninfoFree(connOptions);
}


/*
 * Converts param list to string; caller must free returned pointer
 */
char *
param_list_to_string(t_conninfo_param_list *param_list)
{
	int			c;
	PQExpBufferData conninfo_buf;
	char	   *conninfo_str = NULL;
	int			len = 0;

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


/*
 * check whether the libpq version in use recognizes the "passfile" parameter
 * (should be 9.6 and later)
 */
bool
has_passfile(void)
{
	PQconninfoOption *defs = PQconndefaults();
	PQconninfoOption *def = NULL;
    bool has_passfile = false;

   	for (def = defs; def->keyword; def++)
    {
        if (strcmp(def->keyword, "passfile") == 0)
        {
            has_passfile = true;
            break;
        }
    }

	PQconninfoFree(defs);

	return has_passfile;
}



/* ===================== */
/* transaction functions */
/* ===================== */

bool
begin_transaction(PGconn *conn)
{
	PGresult   *res = NULL;

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
	PGresult   *res = NULL;

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
	PGresult   *res = NULL;

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
	bool		success = true;
	PGresult   *res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(conn, sqlquery, "_set_config(): unable to set \"%s\"", config_param);
		success = false;
	}

	PQclear(res);

	return success;
}


bool
set_config(PGconn *conn, const char *config_param, const char *config_value)
{
	PQExpBufferData query;
	bool		result = false;

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SET %s TO '%s'",
					  config_param,
					  config_value);

	log_verbose(LOG_DEBUG, "set_config():\n  %s", query.data);

	result = _set_config(conn, config_param, query.data);

	termPQExpBuffer(&query);

	return result;
}

bool
set_config_bool(PGconn *conn, const char *config_param, bool state)
{
	PQExpBufferData query;
	bool		result = false;

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SET %s TO %s",
					  config_param,
					  state ? "TRUE" : "FALSE");

	log_verbose(LOG_DEBUG, "set_config_bool():\n  %s", query.data);


	result = _set_config(conn, config_param, query.data);

	termPQExpBuffer(&query);

	return result;
}


int
guc_set(PGconn *conn, const char *parameter, const char *op,
		const char *value)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	int			retval = 1;

	char	   *escaped_parameter = escape_string(conn, parameter);
	char	   *escaped_value = escape_string(conn, value);

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SELECT true FROM pg_catalog.pg_settings "
					  " WHERE name = '%s' AND setting %s '%s'",
					  escaped_parameter, op, escaped_value);

	log_verbose(LOG_DEBUG, "guc_set():\n%s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("guc_set(): unable to execute query"));
		retval = -1;
	}
	else if (PQntuples(res) == 0)
	{
		retval = 0;
	}

	pfree(escaped_parameter);
	pfree(escaped_value);
	termPQExpBuffer(&query);
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
	PQExpBufferData query;
	PGresult   *res = NULL;
	int			retval = 1;

	char	   *escaped_parameter = escape_string(conn, parameter);
	char	   *escaped_value = escape_string(conn, value);

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SELECT true FROM pg_catalog.pg_settings "
					  " WHERE name = '%s' AND setting::%s %s '%s'::%s",
					  parameter, datatype, op, value, datatype);

	log_verbose(LOG_DEBUG, "guc_set_typed():\n%s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("guc_set_typed(): unable to execute query"));
		retval = -1;
	}
	else if (PQntuples(res) == 0)
	{
		retval = 0;
	}

	pfree(escaped_parameter);
	pfree(escaped_value);
	termPQExpBuffer(&query);
	PQclear(res);

	return retval;
}


bool
get_pg_setting(PGconn *conn, const char *setting, char *output)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	int			i;
	bool		success = false;

	char	   *escaped_setting = escape_string(conn, setting);

	if (escaped_setting == NULL)
	{
		log_error(_("unable to escape setting \"%s\""), setting);
		return false;
	}

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SELECT name, setting "
					  "  FROM pg_catalog.pg_settings WHERE name = '%s'",
					  escaped_setting);

	log_verbose(LOG_DEBUG, "get_pg_setting():\n  %s", query.data);

	res = PQexec(conn, query.data);

	pfree(escaped_setting);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_pg_setting() - unable to execute query"));

		termPQExpBuffer(&query);
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

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}


/* ============================ */
/* Server information functions */
/* ============================ */


bool
get_cluster_size(PGconn *conn, char *size)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		success = true;

	initPQExpBuffer(&query);
	appendPQExpBufferStr(&query,
						 "SELECT pg_catalog.pg_size_pretty(pg_catalog.sum(pg_catalog.pg_database_size(oid))::bigint) "
						 "	 FROM pg_catalog.pg_database ");

	log_verbose(LOG_DEBUG, "get_cluster_size():\n%s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_cluster_size(): unable to execute query"));
		success = false;
	}
	else
	{
		strncpy(size, PQgetvalue(res, 0, 0), MAXLEN);
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}


/*
 * Return the server version number for the connection provided
 */
int
get_server_version(PGconn *conn, char *server_version_buf)
{
	PGresult   *res = NULL;
	int			_server_version_num = UNKNOWN_SERVER_VERSION_NUM;

	const char	   *sqlquery =
		"SELECT pg_catalog.current_setting('server_version_num'), "
		"       pg_catalog.current_setting('server_version')";

	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, sqlquery, _("unable to determine server version number"));
		PQclear(res);

		return UNKNOWN_SERVER_VERSION_NUM;
	}

	_server_version_num = atoi(PQgetvalue(res, 0, 0));

	if (server_version_buf != NULL)
	{
		int			i;
		char		_server_version_buf[MAXVERSIONSTR] = "";

		memset(_server_version_buf, 0, MAXVERSIONSTR);

		/*
		 * Some distributions may add extra info after the actual version number,
		 * e.g. "10.4 (Debian 10.4-2.pgdg90+1)", so copy everything up until the
		 * first space.
		 */

		strncpy(_server_version_buf, PQgetvalue(res, 0, 1), MAXVERSIONSTR);

		for (i = 0; i < MAXVERSIONSTR; i++)
		{
			if (_server_version_buf[i] == ' ')
				break;

			*server_version_buf++ = _server_version_buf[i];
		}
	}

	PQclear(res);

	return _server_version_num;
}


RecoveryType
get_recovery_type(PGconn *conn)
{
	PGresult   *res = NULL;
	RecoveryType recovery_type = RECTYPE_UNKNOWN;

	const char	   *sqlquery = "SELECT pg_catalog.pg_is_in_recovery()";

	log_verbose(LOG_DEBUG, "get_recovery_type(): %s", sqlquery);

	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn,
					 sqlquery,
					 _("unable to determine if server is in recovery"));

		recovery_type = RECTYPE_UNKNOWN;
	}
	else if (PQntuples(res) == 1)
	{
		if (strcmp(PQgetvalue(res, 0, 0), "f") == 0)
		{
			recovery_type = RECTYPE_PRIMARY;
		}
		else
		{
			recovery_type = RECTYPE_STANDBY;
		}
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
	PQExpBufferData query;

	PGconn	   *remote_conn = NULL;
	PGresult   *res = NULL;

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
	log_verbose(LOG_INFO, _("searching for primary node"));

	initPQExpBuffer(&query);
	appendPQExpBufferStr(&query,
						 "  SELECT node_id, conninfo, "
						 "         CASE WHEN type = 'primary' THEN 1 ELSE 2 END AS type_priority"
						 "	   FROM repmgr.nodes "
						 "   WHERE active IS TRUE "
						 "     AND type != 'witness' "
						 "ORDER BY active DESC, type_priority, priority, node_id");

	log_verbose(LOG_DEBUG, "get_primary_connection():\n%s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("_get_primary_connection(): unable to retrieve node records"));

		termPQExpBuffer(&query);
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
					_("checking if node %i is primary"),
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
		{
			PQfinish(remote_conn);
			remote_conn = NULL;
			continue;
		}

		recovery_type = get_recovery_type(remote_conn);

		if (recovery_type == RECTYPE_UNKNOWN)
		{
			log_warning(_("unable to retrieve recovery state from node %i"),
						node_id);

			PQfinish(remote_conn);
			continue;
		}

		if (recovery_type == RECTYPE_PRIMARY)
		{
			PQclear(res);
			log_verbose(LOG_INFO, _("current primary node is %i"), node_id);

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
	PQExpBufferData query;
	PGresult   *res = NULL;
	int			retval = NODE_NOT_FOUND;

	initPQExpBuffer(&query);
	appendPQExpBufferStr(&query,
						 "SELECT node_id		  "
						 "	 FROM repmgr.nodes    "
						 " WHERE type = 'primary' "
						 "   AND active IS TRUE  ");

	log_verbose(LOG_DEBUG, "get_primary_node_id():\n%s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_primary_node_id(): unable to execute query"));
		retval = UNKNOWN_NODE_ID;
	}
	else if (PQntuples(res) == 0)
	{
		log_verbose(LOG_WARNING, _("get_primary_node_id(): no active primary found"));
		retval = NODE_NOT_FOUND;
	}
	else
	{
		retval = atoi(PQgetvalue(res, 0, 0));
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return retval;
}


bool
get_replication_info(PGconn *conn, ReplInfo *replication_info)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		success = true;

	initPQExpBuffer(&query);
	appendPQExpBufferStr(&query,
						 " SELECT ts, "
						 "        last_wal_receive_lsn, "
						 "        last_wal_replay_lsn, "
						 "        last_xact_replay_timestamp, "
						 "        CASE WHEN (last_wal_receive_lsn = last_wal_replay_lsn) "
						 "          THEN 0::INT "
						 "        ELSE "
						 "          EXTRACT(epoch FROM (pg_catalog.clock_timestamp() - last_xact_replay_timestamp))::INT "
						 "        END AS replication_lag_time, "
						 "        COALESCE(last_wal_receive_lsn, '0/0') >= last_wal_replay_lsn AS receiving_streamed_wal "
						 "   FROM ( ");

	if (PQserverVersion(conn) >= 100000)
	{
		appendPQExpBufferStr(&query,
							 " SELECT CURRENT_TIMESTAMP AS ts, "
							 "        pg_catalog.pg_last_wal_receive_lsn()       AS last_wal_receive_lsn, "
							 "        pg_catalog.pg_last_wal_replay_lsn()        AS last_wal_replay_lsn, "
							 "        pg_catalog.pg_last_xact_replay_timestamp() AS last_xact_replay_timestamp ");
	}
	else
	{
		appendPQExpBufferStr(&query,
							 " SELECT CURRENT_TIMESTAMP AS ts, "
							 "        pg_catalog.pg_last_xlog_receive_location() AS last_wal_receive_lsn, "
							 "        pg_catalog.pg_last_xlog_replay_location()  AS last_wal_replay_lsn, "
							 "        pg_catalog.pg_last_xact_replay_timestamp() AS last_xact_replay_timestamp ");
	}

	appendPQExpBufferStr(&query,
						 "          ) q ");

	log_verbose(LOG_DEBUG, "get_replication_info():\n%s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK || !PQntuples(res))
	{
		log_db_error(conn, query.data, _("get_replication_info(): unable to execute query"));

		success = false;
	}
	else
	{
		strncpy(replication_info->current_timestamp, PQgetvalue(res, 0, 0), MAXLEN);
		replication_info->last_wal_receive_lsn = parse_lsn(PQgetvalue(res, 0, 1));
		replication_info->last_wal_replay_lsn = parse_lsn(PQgetvalue(res, 0, 2));
		strncpy(replication_info->last_xact_replay_timestamp, PQgetvalue(res, 0, 3), MAXLEN);
		replication_info->replication_lag_time = atoi(PQgetvalue(res, 0, 4));
		replication_info->receiving_streamed_wal = atobool(PQgetvalue(res, 0, 5));
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}


int
get_ready_archive_files(PGconn *conn, const char *data_directory)
{
	char		archive_status_dir[MAXPGPATH] = "";
	struct stat statbuf;
	struct dirent *arcdir_ent;
	DIR		   *arcdir;

	int			ready_count = 0;

	if (PQserverVersion(conn) >= 100000)
	{
		snprintf(archive_status_dir, MAXPGPATH,
				 "%s/pg_wal/archive_status",
				 data_directory);
	}
	else
	{
		snprintf(archive_status_dir, MAXPGPATH,
				 "%s/pg_xlog/archive_status",
				 data_directory);
	}

	/* sanity-check directory path */
	if (stat(archive_status_dir, &statbuf) == -1)
	{
		log_error(_("unable to access archive_status directory \"%s\""),
				  archive_status_dir);
		log_detail("%s", strerror(errno));
		/* XXX magic number */
		return -1;
	}

	arcdir = opendir(archive_status_dir);

	if (arcdir == NULL)
	{
		log_error(_("unable to open archive directory \"%s\""),
				  archive_status_dir);
		log_detail("%s", strerror(errno));
		/* XXX magic number */
		return -1;
	}

	while ((arcdir_ent = readdir(arcdir)) != NULL)
	{
		struct stat statbuf;
		char		file_path[MAXPGPATH] = "";
		int			basenamelen = 0;

		snprintf(file_path, MAXPGPATH,
				 "%s/%s",
				 archive_status_dir,
				 arcdir_ent->d_name);

		/* skip non-files */
		if (stat(file_path, &statbuf) == 0 && !S_ISREG(statbuf.st_mode))
		{
			continue;
		}

		basenamelen = (int) strlen(arcdir_ent->d_name) - 6;

		/*
		 * count anything ending in ".ready"; for a more precise
		 * implementation see: src/backend/postmaster/pgarch.c
		 */
		if (strcmp(arcdir_ent->d_name + basenamelen, ".ready") == 0)
			ready_count++;
	}

	closedir(arcdir);

	return ready_count;
}


int
get_replication_lag_seconds(PGconn *conn)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	int			lag_seconds = 0;

	initPQExpBuffer(&query);

	if (PQserverVersion(conn) >= 100000)
	{
		appendPQExpBufferStr(&query,
							 " SELECT CASE WHEN (pg_catalog.pg_last_wal_receive_lsn() = pg_catalog.pg_last_wal_replay_lsn()) ");

	}
	else
	{
		appendPQExpBufferStr(&query,
							 " SELECT CASE WHEN (pg_catalog.pg_last_xlog_receive_location() = pg_catalog.pg_last_xlog_replay_location()) ");
	}

	appendPQExpBufferStr(&query,
						 "          THEN 0 "
						 "        ELSE EXTRACT(epoch FROM (pg_catalog.clock_timestamp() - pg_catalog.pg_last_xact_replay_timestamp()))::INT "
						 "          END "
						 "        AS lag_seconds");

	res = PQexec(conn, query.data);
	log_verbose(LOG_DEBUG, "get_replication_lag_seconds():\n%s", query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_warning("%s", PQerrorMessage(conn));
		PQclear(res);

		/* XXX magic number */
		return -1;
	}

	if (!PQntuples(res))
	{
		return -1;
	}

	lag_seconds = atoi(PQgetvalue(res, 0, 0));

	PQclear(res);
	return lag_seconds;
}


bool
identify_system(PGconn *repl_conn, t_system_identification *identification)
{
	PGresult   *res = NULL;

	res = PQexec(repl_conn, "IDENTIFY_SYSTEM;");

	if (PQresultStatus(res) != PGRES_TUPLES_OK || !PQntuples(res))
	{
		PQclear(res);
		return false;
	}

	identification->system_identifier = atol(PQgetvalue(res, 0, 0));
	identification->timeline = atoi(PQgetvalue(res, 0, 1));
	identification->xlogpos = parse_lsn(PQgetvalue(res, 0, 2));

	PQclear(res);
	return true;
}

TimeLineHistoryEntry *
get_timeline_history(PGconn *repl_conn, TimeLineID tli)
{
	PQExpBufferData query;
	PGresult   *res = NULL;

	int			nfields;
	TimeLineID	file_tli;
	uint32		switchpoint_hi;
	uint32		switchpoint_lo;
	TimeLineHistoryEntry *history;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "TIMELINE_HISTORY %i",
					  (int)tli);

	res = PQexec(repl_conn, query.data);
	log_verbose(LOG_DEBUG, "get_timeline_history():\n%s", query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(repl_conn, query.data, _("get_timeline_history(): unable to execute query"));
		termPQExpBuffer(&query);
		PQclear(res);
		return NULL;
	}

	termPQExpBuffer(&query);

	if (PQntuples(res) != 1 || PQnfields(res) != 2)
	{
		log_error(_("unexpected response to TIMELINE_HISTORY command"));
		log_detail(_("got %i rows and %i fields, expected %i rows and %i fields"),
				   PQntuples(res), PQnfields(res), 1, 2);
		PQclear(res);
		return NULL;
	}

	nfields = sscanf(PQgetvalue(res, 0, 1),
					 "%u\t%X/%X",
					 &file_tli, &switchpoint_hi, &switchpoint_lo);

	if (nfields != 3)
	{
		log_error(_("unable to parse timeline history file content"));
		log_detail(_("content is: \"%s\""), PQgetvalue(res, 0, 1));
		PQclear(res);
		return NULL;
	}

	PQclear(res);

	history = (TimeLineHistoryEntry *) palloc(sizeof(TimeLineHistoryEntry));
	history->tli = file_tli;
	history->begin = InvalidXLogRecPtr; /* we don't care about this */
	history->end = ((uint64) (switchpoint_hi)) << 32 | (uint64) switchpoint_lo;

	return history;
}


/* =============================== */
/* repmgrd shared memory functions */
/* =============================== */

bool
repmgrd_set_local_node_id(PGconn *conn, int local_node_id)
{
	PQExpBufferData query;
	PGresult   *res = NULL;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT repmgr.set_local_node_id(%i)",
					  local_node_id);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		return false;
	}

	PQclear(res);
	return true;
}


int
repmgrd_get_local_node_id(PGconn *conn)
{
	PGresult   *res = NULL;
	int			local_node_id = UNKNOWN_NODE_ID;

	const char *sqlquery = "SELECT repmgr.get_local_node_id()";

	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, sqlquery, _("repmgrd_get_local_node_id(): unable to execute query"));
	}
	else if (!PQgetisnull(res, 0, 0))
	{
		local_node_id = atoi(PQgetvalue(res, 0, 0));
	}

	PQclear(res);

	return local_node_id;
}


/*
 * Function that checks if the primary is in exclusive backup mode.
 * We'll use this when executing an action can conflict with an exclusive
 * backup.
 */
BackupState
server_in_exclusive_backup_mode(PGconn *conn)
{
	BackupState backup_state = BACKUP_STATE_UNKNOWN;
	const char *sqlquery = "SELECT pg_catalog.pg_is_in_backup()";
	PGresult   *res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, sqlquery, _("unable to retrieve information regarding backup mode of node"));

		backup_state = BACKUP_STATE_UNKNOWN;
	}
	else if (atobool(PQgetvalue(res, 0, 0)) == true)
	{
		backup_state = BACKUP_STATE_IN_BACKUP;
	}
	else
	{
		backup_state = BACKUP_STATE_NO_BACKUP;
	}

	PQclear(res);

	return backup_state;
}


void
repmgrd_set_pid(PGconn *conn, pid_t repmgrd_pid, const char *pidfile)
{
	PQExpBufferData query;
	PGresult   *res = NULL;

	log_verbose(LOG_DEBUG, "repmgrd_set_pid(): pid is %i", (int) repmgrd_pid);

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT repmgr.set_repmgrd_pid(%i, '%s')",
					  (int) repmgrd_pid, pidfile);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to execute \"SELECT repmgr.set_repmgrd_pid()\""));
		log_detail("%s", PQerrorMessage(conn));
	}

	PQclear(res);

	return;
}


pid_t
repmgrd_get_pid(PGconn *conn)
{
	PGresult   *res = NULL;
	pid_t		repmgrd_pid = UNKNOWN_PID;

	res = PQexec(conn, "SELECT repmgr.get_repmgrd_pid()");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to execute \"SELECT repmgr.get_repmgrd_pid()\""));
		log_detail("%s", PQerrorMessage(conn));
	}
	else if (!PQgetisnull(res, 0, 0))
	{
		repmgrd_pid = atoi(PQgetvalue(res, 0, 0));
	}

	PQclear(res);

	return repmgrd_pid;
}


bool
repmgrd_is_running(PGconn *conn)
{
	PGresult   *res = NULL;
	bool		is_running = false;

	res = PQexec(conn, "SELECT repmgr.repmgrd_is_running()");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to execute \"SELECT repmgr.repmgrd_is_running()\""));
		log_detail("%s", PQerrorMessage(conn));
	}
	else if (!PQgetisnull(res, 0, 0))
	{
		is_running = atobool(PQgetvalue(res, 0, 0));
	}

	PQclear(res);

	return is_running;
}


bool
repmgrd_is_paused(PGconn *conn)
{
	PGresult   *res = NULL;
	bool		is_paused = false;

	res = PQexec(conn, "SELECT repmgr.repmgrd_is_paused()");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to execute \"SELECT repmgr.repmgrd_is_paused()\""));
		log_detail("%s", PQerrorMessage(conn));
	}
	else if (!PQgetisnull(res, 0, 0))
	{
		is_paused = atobool(PQgetvalue(res, 0, 0));
	}

	PQclear(res);

	return is_paused;
}


bool
repmgrd_pause(PGconn *conn, bool pause)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		success = true;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT repmgr.repmgrd_pause(%s)",
					  pause == true ? "TRUE" : "FALSE");
	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to execute \"SELECT repmgr.repmgrd_pause()\""));
		log_detail("%s", PQerrorMessage(conn));

		success = false;
	}

	PQclear(res);

	return success;
}

/* ================ */
/* result functions */
/* ================ */

bool
atobool(const char *value)
{
	return (strcmp(value, "t") == 0)
		? true
		: false;
}


/* =================== */
/* extension functions */
/* =================== */

ExtensionStatus
get_repmgr_extension_status(PGconn *conn, t_extension_versions *extversions)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	ExtensionStatus status = REPMGR_UNKNOWN;

	/* TODO: check version */

	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 "	  SELECT ae.name, e.extname, "
						 "           ae.default_version, "
						 "           (((ae.default_version::NUMERIC::INT) * 10000) + (ae.default_version::NUMERIC - ae.default_version::NUMERIC::INT) * 1000)::INT AS available, "
						 "           ae.installed_version, "
						 "           (((ae.installed_version::NUMERIC::INT) * 10000) + (ae.installed_version::NUMERIC - ae.installed_version::NUMERIC::INT) * 1000)::INT AS installed "
						 "     FROM pg_catalog.pg_available_extensions ae "
						 "LEFT JOIN pg_catalog.pg_extension e "
						 "       ON e.extname=ae.name "
						 "	   WHERE ae.name='repmgr' ");

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_repmgr_extension_status(): unable to execute extension query"));
		status = REPMGR_UNKNOWN;
	}

	/* 1. Check extension is actually available */
	else if (PQntuples(res) == 0)
	{
		status = REPMGR_UNAVAILABLE;
	}

	/* 2. Check if extension installed */
	else if (PQgetisnull(res, 0, 1) == 0)
	{
		int available_version = atoi(PQgetvalue(res, 0, 3));
		int installed_version = atoi(PQgetvalue(res, 0, 5));

		/* caller wants to know which versions are installed/available */
		if (extversions != NULL)
		{
			strncpy(extversions->default_version, PQgetvalue(res, 0, 2), 7);
			extversions->default_version_num = available_version;
			strncpy(extversions->installed_version, PQgetvalue(res, 0, 4), 7);
			extversions->installed_version_num = installed_version;
		}

		if (available_version > installed_version)
		{
			status = REPMGR_OLD_VERSION_INSTALLED;
		}
		else
		{
			status = REPMGR_INSTALLED;
		}
	}
	else
	{
		status = REPMGR_AVAILABLE;
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return status;
}

/* ========================= */
/* node management functions */
/* ========================= */

/* assumes superuser connection */
void
checkpoint(PGconn *conn)
{
	PGresult   *res = NULL;

	res = PQexec(conn, "CHECKPOINT");

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(conn, NULL, _("unable to execute CHECKPOINT"));
	}

	PQclear(res);
	return;
}

/* assumes superuser connection */
bool
vacuum_table(PGconn *primary_conn, const char *table)
{
	PQExpBufferData query;
	bool		success = true;
	PGresult   *res = NULL;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query, "VACUUM %s", table);

	res = PQexec(primary_conn, query.data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(primary_conn, NULL, _("unable to vacuum table \"%s\""), table);
		success = false;
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}

/*
 * For use in PostgreSQL 12 and later
 */
bool
promote_standby(PGconn *conn, bool wait, int wait_seconds)
{
	PQExpBufferData query;
	bool		success = true;
	PGresult   *res = NULL;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT pg_catalog.pg_promote(wait := %s",
					  wait ? "TRUE" : "FALSE");

	if (wait_seconds > 0)
	{
		appendPQExpBuffer(&query,
						  ", wait_seconds := %i",
						  wait_seconds);
	}

	appendPQExpBufferStr(&query, ")");

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("unable to execute pg_promote()"));
		success = false;
	}
	else
	{
		/* NOTE: if "wait" is false, pg_promote() will always return true */
		success = atobool(PQgetvalue(res, 0, 0));
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}


/* ===================== */
/* Node record functions */
/* ===================== */


static RecordStatus
_get_node_record(PGconn *conn, char *sqlquery, t_node_info *node_info)
{
	int			ntuples = 0;
	PGresult   *res = NULL;

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
	strncpy(node_info->config_file, PQgetvalue(res, row, 10), MAXPGPATH);

	/* This won't normally be set */
	strncpy(node_info->upstream_node_name, PQgetvalue(res, row, 11), MAXLEN);

	/* Set remaining struct fields with default values */
	node_info->node_status = NODE_STATUS_UNKNOWN;
	node_info->recovery_type = RECTYPE_UNKNOWN;
	node_info->last_wal_receive_lsn = InvalidXLogRecPtr;
	node_info->monitoring_state = MS_NORMAL;
	node_info->conn = NULL;
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
	switch (type)
	{
		case PRIMARY:
			return "primary";
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


RecordStatus
get_node_record(PGconn *conn, int node_id, t_node_info *node_info)
{
	PQExpBufferData query;
	RecordStatus result;

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "SELECT " REPMGR_NODES_COLUMNS
					  "  FROM repmgr.nodes n "
					  " WHERE n.node_id = %i",
					  node_id);

	log_verbose(LOG_DEBUG, "get_node_record():\n  %s", query.data);

	result = _get_node_record(conn, query.data, node_info);
	termPQExpBuffer(&query);

	if (result == RECORD_NOT_FOUND)
	{
		log_verbose(LOG_DEBUG, "get_node_record(): no record found for node %i", node_id);
	}

	return result;
}


RecordStatus
get_node_record_with_upstream(PGconn *conn, int node_id, t_node_info *node_info)
{
	PQExpBufferData query;
	RecordStatus result;

	initPQExpBuffer(&query);
	appendPQExpBuffer(&query,
					  "    SELECT n.node_id, n.type, n.upstream_node_id, n.node_name, n.conninfo, n.repluser, "
					  "           n.slot_name, n.location, n.priority, n.active, n.config_file, un.node_name AS upstream_node_name "
					  "      FROM repmgr.nodes n "
					  " LEFT JOIN repmgr.nodes un "
					  "        ON un.node_id = n.upstream_node_id"
					  " WHERE n.node_id = %i",
					  node_id);

	log_verbose(LOG_DEBUG, "get_node_record():\n  %s", query.data);

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
	PQExpBufferData query;
	RecordStatus record_status = RECORD_NOT_FOUND;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT " REPMGR_NODES_COLUMNS
					  "  FROM repmgr.nodes n "
					  " WHERE n.node_name = '%s' ",
					  node_name);

	log_verbose(LOG_DEBUG, "get_node_record_by_name():\n  %s", query.data);

	record_status = _get_node_record(conn, query.data, node_info);

	termPQExpBuffer(&query);

	if (record_status == RECORD_NOT_FOUND)
	{
		log_verbose(LOG_DEBUG, "get_node_record_by_name(): no record found for node %s",
					node_name);
	}

	return record_status;
}


t_node_info *
get_node_record_pointer(PGconn *conn, int node_id)
{
	t_node_info *node_info = pg_malloc0(sizeof(t_node_info));
	RecordStatus record_status = RECORD_NOT_FOUND;

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
	RecordStatus record_status = RECORD_NOT_FOUND;

	int			primary_node_id = get_primary_node_id(conn);

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
	RecordStatus record_status = get_node_record(conn, node_id, node_info);

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
void
_populate_node_records(PGresult *res, NodeInfoList *node_list)
{
	int			i;

	clear_node_info_list(node_list);

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


bool
get_all_node_records(PGconn *conn, NodeInfoList *node_list)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool success = true;
	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 "  SELECT " REPMGR_NODES_COLUMNS
						 "    FROM repmgr.nodes n "
						 "ORDER BY n.node_id ");

	log_verbose(LOG_DEBUG, "get_all_node_records():\n%s", query.data);

	res = PQexec(conn, query.data);

	/* this will return an empty list if there was an error executing the query */
	_populate_node_records(res, node_list);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_all_node_records(): unable to execute query"));
		success = false;
	}

	PQclear(res);
	termPQExpBuffer(&query);

	return success;
}


void
get_downstream_node_records(PGconn *conn, int node_id, NodeInfoList *node_list)
{
	PQExpBufferData query;
	PGresult   *res = NULL;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "  SELECT " REPMGR_NODES_COLUMNS
					  "    FROM repmgr.nodes n "
					  "   WHERE n.upstream_node_id = %i "
					  "ORDER BY n.node_id ",
					  node_id);

	log_verbose(LOG_DEBUG, "get_downstream_node_records():\n%s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_downstream_node_records(): unable to execute query"));
	}

	termPQExpBuffer(&query);

	/* this will return an empty list if there was an error executing the query */
	_populate_node_records(res, node_list);

	PQclear(res);

	return;
}


void
get_active_sibling_node_records(PGconn *conn, int node_id, int upstream_node_id, NodeInfoList *node_list)
{
	PQExpBufferData query;
	PGresult   *res = NULL;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "  SELECT " REPMGR_NODES_COLUMNS
					  "    FROM repmgr.nodes n "
					  "   WHERE n.upstream_node_id = %i "
					  "     AND n.node_id != %i "
					  "     AND n.active IS TRUE "
					  "ORDER BY n.node_id ",
					  upstream_node_id,
					  node_id);

	log_verbose(LOG_DEBUG, "get_active_sibling_node_records():\n%s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_active_sibling_records(): unable to execute query"));
	}

	termPQExpBuffer(&query);

	/* this will return an empty list if there was an error executing the query */
	_populate_node_records(res, node_list);

	PQclear(res);

	return;
}


void
get_node_records_by_priority(PGconn *conn, NodeInfoList *node_list)
{
	PQExpBufferData query;
	PGresult   *res = NULL;

	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 "  SELECT " REPMGR_NODES_COLUMNS
						 "    FROM repmgr.nodes n "
						 "ORDER BY n.priority DESC, n.node_name ");

	log_verbose(LOG_DEBUG, "get_node_records_by_priority():\n%s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_node_records_by_priority(): unable to execute query"));
	}

	termPQExpBuffer(&query);

	/* this will return an empty list if there was an error executing the query */
	_populate_node_records(res, node_list);

	PQclear(res);

	return;
}

/*
 * return all node records together with their upstream's node name,
 * if available.
 */
bool
get_all_node_records_with_upstream(PGconn *conn, NodeInfoList *node_list)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		success = true;

	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 "    SELECT n.node_id, n.type, n.upstream_node_id, n.node_name, n.conninfo, n.repluser, "
						 "           n.slot_name, n.location, n.priority, n.active, n.config_file, un.node_name AS upstream_node_name "
						 "      FROM repmgr.nodes n "
						 " LEFT JOIN repmgr.nodes un "
						 "        ON un.node_id = n.upstream_node_id"
						 "  ORDER BY n.node_id ");

	log_verbose(LOG_DEBUG, "get_all_node_records_with_upstream():\n%s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_all_node_records_with_upstream(): unable to retrieve node records"));
		success = false;
	}


	/* this will return an empty list if there was an error executing the query */
	_populate_node_records(res, node_list);

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}



bool
get_downstream_nodes_with_missing_slot(PGconn *conn, int this_node_id, NodeInfoList *node_list)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		success = true;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "   SELECT " REPMGR_NODES_COLUMNS
					  "     FROM repmgr.nodes n "
					  "LEFT JOIN pg_catalog.pg_replication_slots rs "
					  "       ON rs.slot_name = n.slot_name "
					  "    WHERE n.slot_name IS NOT NULL"
					  "      AND rs.slot_name IS NULL "
					  "      AND n.upstream_node_id = %i "
					  "      AND n.type = 'standby'",
					  this_node_id);

	log_verbose(LOG_DEBUG, "get_all_node_records_with_missing_slot():\n%s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_downstream_nodes_with_missing_slot(): unable to retrieve node records"));
		success = false;
	}

	/* this will return an empty list if there was an error executing the query */
	_populate_node_records(res, node_list);

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
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
	PQExpBufferData query;
	char		node_id[MAXLEN] = "";
	char		priority[MAXLEN] = "";

	char		upstream_node_id[MAXLEN] = "";
	char	   *upstream_node_id_ptr = NULL;

	char	   *slot_name_ptr = NULL;

	int			param_count = NODE_RECORD_PARAM_COUNT;
	const char *param_values[NODE_RECORD_PARAM_COUNT];

	PGresult   *res;
	bool		success = true;

	maxlen_snprintf(node_id, "%i", node_info->node_id);
	maxlen_snprintf(priority, "%i", node_info->priority);

	if (node_info->upstream_node_id == NO_UPSTREAM_NODE && node_info->type == STANDBY)
	{
		/*
		 * No explicit upstream node id provided for standby - attempt to get
		 * primary node id
		 */
		int			primary_node_id = get_primary_node_id(conn);

		maxlen_snprintf(upstream_node_id, "%i", primary_node_id);
		upstream_node_id_ptr = upstream_node_id;
	}
	else if (node_info->upstream_node_id != NO_UPSTREAM_NODE)
	{
		maxlen_snprintf(upstream_node_id, "%i", node_info->upstream_node_id);
		upstream_node_id_ptr = upstream_node_id;
	}

	if (node_info->slot_name[0] != '\0')
	{
		slot_name_ptr = node_info->slot_name;
	}


	param_values[0] = get_node_type_string(node_info->type);
	param_values[1] = upstream_node_id_ptr;
	param_values[2] = node_info->node_name;
	param_values[3] = node_info->conninfo;
	param_values[4] = node_info->repluser;
	param_values[5] = slot_name_ptr;
	param_values[6] = node_info->location;
	param_values[7] = priority;
	param_values[8] = node_info->active == true ? "TRUE" : "FALSE";
	param_values[9] = node_info->config_file;
	param_values[10] = node_id;

	initPQExpBuffer(&query);

	if (strcmp(action, "create") == 0)
	{
		appendPQExpBufferStr(&query,
							 "INSERT INTO repmgr.nodes "
							 "       (node_id, type, upstream_node_id, "
							 "        node_name, conninfo, repluser, slot_name, "
							 "        location, priority, active, config_file) "
							 "VALUES ($11, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ");
	}
	else
	{
		appendPQExpBufferStr(&query,
							 "UPDATE repmgr.nodes SET "
							 "       type = $1, "
							 "       upstream_node_id = $2, "
							 "       node_name = $3, "
							 "       conninfo = $4, "
							 "       repluser = $5, "
							 "       slot_name = $6, "
							 "       location = $7, "
							 "       priority = $8, "
							 "       active = $9, "
							 "       config_file = $10 "
							 " WHERE node_id = $11 ");
	}

	res = PQexecParams(conn,
					   query.data,
					   param_count,
					   NULL,
					   param_values,
					   NULL,
					   NULL,
					   0);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(conn, query.data,
					 _("_create_update_node_record(): unable to %s node record for node \"%s\" (ID: %i)"),
					 action,
					 node_info->node_name,
					 node_info->node_id);

		success = false;
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}


bool
update_node_record_set_active(PGconn *conn, int this_node_id, bool active)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		success = true;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "UPDATE repmgr.nodes SET active = %s "
					  " WHERE node_id = %i",
					  active == true ? "TRUE" : "FALSE",
					  this_node_id);

	log_verbose(LOG_DEBUG, "update_node_record_set_active():\n  %s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(conn, query.data,
					 _("update_node_record_set_active(): unable to update node record"));
		success = false;
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}


bool
update_node_record_set_active_standby(PGconn *conn, int this_node_id)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		success = true;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "UPDATE repmgr.nodes "
					  "   SET type = 'standby', "
					  "       active = TRUE "
					  " WHERE node_id = %i",
					  this_node_id);

	log_verbose(LOG_DEBUG, "update_node_record_set_active_standby():\n  %s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(conn, query.data, _("update_node_record_set_active_standby(): unable to update node record"));
		success = false;
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}


bool
update_node_record_set_primary(PGconn *conn, int this_node_id)
{
	PQExpBufferData query;
	PGresult   *res = NULL;

	log_debug(_("setting node %i as primary and marking existing primary as failed"),
			  this_node_id);

	begin_transaction(conn);

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "  UPDATE repmgr.nodes "
					  "     SET active = FALSE "
					  "   WHERE type = 'primary' "
					  "     AND active IS TRUE "
					  "     AND node_id != %i ",
					  this_node_id);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(conn, query.data,
					 _("update_node_record_set_primary(): unable to set old primary node as inactive"));

		termPQExpBuffer(&query);
		PQclear(res);

		rollback_transaction(conn);

		return false;
	}

	termPQExpBuffer(&query);
	PQclear(res);

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "  UPDATE repmgr.nodes"
					  "     SET type = 'primary', "
					  "         upstream_node_id = NULL, "
					  "         active = TRUE "
					  "   WHERE node_id = %i ",
					  this_node_id);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(conn, query.data,
					 _("unable to set current node %i as active primary"),
					 this_node_id);

		termPQExpBuffer(&query);
		PQclear(res);

		rollback_transaction(conn);

		return false;
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return commit_transaction(conn);
}


bool
update_node_record_set_upstream(PGconn *conn, int this_node_id, int new_upstream_node_id)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		success = true;

	log_debug(_("update_node_record_set_upstream(): Updating node %i's upstream node to %i"),
			  this_node_id, new_upstream_node_id);

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "  UPDATE repmgr.nodes "
					  "     SET upstream_node_id = %i "
					  "   WHERE node_id = %i ",
					  new_upstream_node_id,
					  this_node_id);

	log_verbose(LOG_DEBUG, "update_node_record_set_upstream():\n%s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(conn, query.data, _("update_node_record_set_upstream(): unable to set new upstream node id"));

		success = false;
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}


/*
 * Update node record following change of status
 * (e.g. inactive primary converted to standby)
 */
bool
update_node_record_status(PGconn *conn, int this_node_id, char *type, int upstream_node_id, bool active)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		success = true;

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

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(conn, query.data,
					 _("update_node_record_status(): unable to update node record status for node %i"),
					 this_node_id);

		success = false;
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}


/*
 * Update node record's "conninfo" and "priority" fields. Called by repmgrd
 * following a configuration file reload.
 */
bool
update_node_record_conn_priority(PGconn *conn, t_configuration_options *options)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		success = true;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "UPDATE repmgr.nodes "
					  "   SET conninfo = '%s', "
					  "       priority = %d "
					  " WHERE node_id = %d ",
					  options->conninfo,
					  options->priority,
					  options->node_id);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		success = false;
	}

	PQclear(res);

	return success;
}


/*
 * Copy node records from primary to witness servers.
 *
 * This is used when initially registering a witness server, and
 * by repmgrd to update the node records when required.
 */

bool
witness_copy_node_records(PGconn *primary_conn, PGconn *witness_conn)
{
	PGresult   *res = NULL;
	NodeInfoList nodes = T_NODE_INFO_LIST_INITIALIZER;
	NodeInfoListCell *cell = NULL;

	begin_transaction(witness_conn);

	/* Defer constraints */

	res = PQexec(witness_conn, "SET CONSTRAINTS ALL DEFERRED");

	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(witness_conn, NULL, ("witness_copy_node_records(): unable to defer constraints"));

		rollback_transaction(witness_conn);
		PQclear(res);

		return false;
	}

	PQclear(res);

	/* truncate existing records */

	if (truncate_node_records(witness_conn) == false)
	{
		rollback_transaction(witness_conn);

		return false;
	}

	get_all_node_records(primary_conn, &nodes);

	for (cell = nodes.head; cell; cell = cell->next)
	{
		create_node_record(witness_conn, NULL, cell->node_info);
	}

	/* and done */
	commit_transaction(witness_conn);

	clear_node_info_list(&nodes);

	return true;
}


bool
delete_node_record(PGconn *conn, int node)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		success = true;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "DELETE FROM repmgr.nodes "
					  " WHERE node_id = %i",
					  node);

	log_verbose(LOG_DEBUG, "delete_node_record():\n  %s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(conn, query.data, _("delete_node_record(): unable to delete node record"));

		success = false;
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}

bool
truncate_node_records(PGconn *conn)
{
	PGresult   *res = NULL;
	bool		success = true;

	res = PQexec(conn, "TRUNCATE TABLE repmgr.nodes");

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(conn, NULL, _("truncate_node_records(): unable to truncate table \"repmgr.nodes\""));

		success = false;
	}

	PQclear(res);

	return success;
}


bool
update_node_record_slot_name(PGconn *primary_conn, int node_id, char *slot_name)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		success = true;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  " UPDATE repmgr.nodes "
					  "    SET slot_name = '%s' "
					  "  WHERE node_id = %i ",
					  slot_name,
					  node_id);

	res = PQexec(primary_conn, query.data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(primary_conn, query.data, _("unable to set node record slot name"));

		success = false;
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}


void
get_node_replication_stats(PGconn *conn, t_node_info *node_info)
{
	PQExpBufferData query;
	PGresult   *res = NULL;

	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 " SELECT pg_catalog.current_setting('max_wal_senders')::INT AS max_wal_senders, "
						 "        (SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_replication) AS attached_wal_receivers, ");

	/* no replication slots in PostgreSQL 9.3 */
	if (PQserverVersion(conn) < 90400)
	{
		appendPQExpBufferStr(&query,
							 "        0 AS max_replication_slots, "
							 "        0 AS total_replication_slots, "
							 "        0 AS active_replication_slots, "
							 "        0 AS inactive_replication_slots, ");
	}
	else
	{
		appendPQExpBufferStr(&query,
							 "        current_setting('max_replication_slots')::INT AS max_replication_slots, "
							 "        (SELECT pg_catalog.count(*) FROM pg_catalog.pg_replication_slots) AS total_replication_slots, "
							 "        (SELECT pg_catalog.count(*) FROM pg_catalog.pg_replication_slots WHERE active IS TRUE)  AS active_replication_slots, "
							 "        (SELECT pg_catalog.count(*) FROM pg_catalog.pg_replication_slots WHERE active IS FALSE) AS inactive_replication_slots, ");
	}


	appendPQExpBufferStr(&query,
						 "        pg_catalog.pg_is_in_recovery() AS in_recovery");

	log_verbose(LOG_DEBUG, "get_node_replication_stats():\n%s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_warning(_("unable to retrieve node replication statistics"));
		log_detail("%s", PQerrorMessage(conn));
		log_detail("%s", query.data);

		termPQExpBuffer(&query);
		PQclear(res);

		return;
	}

	node_info->max_wal_senders = atoi(PQgetvalue(res, 0, 0));
	node_info->attached_wal_receivers = atoi(PQgetvalue(res, 0, 1));
	node_info->max_replication_slots = atoi(PQgetvalue(res, 0, 2));
	node_info->total_replication_slots = atoi(PQgetvalue(res, 0, 3));
	node_info->active_replication_slots = atoi(PQgetvalue(res, 0, 4));
	node_info->inactive_replication_slots = atoi(PQgetvalue(res, 0, 5));
	node_info->recovery_type = strcmp(PQgetvalue(res, 0, 6), "f") == 0 ? RECTYPE_PRIMARY : RECTYPE_STANDBY;

	termPQExpBuffer(&query);
	PQclear(res);

	return;
}


bool
is_downstream_node_attached(PGconn *conn, char *node_name)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	int			c = 0;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  " SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_replication "
					  "  WHERE application_name = '%s'",
					  node_name);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_verbose(LOG_WARNING, _("unable to query pg_stat_replication"));
		log_detail("%s", PQerrorMessage(conn));
		log_detail("%s", query.data);

		termPQExpBuffer(&query);
		PQclear(res);

		return false;
	}

	if (PQntuples(res) != 1)
	{
		log_verbose(LOG_WARNING, _("unexpected number of tuples (%i) returned"), PQntuples(res));

		termPQExpBuffer(&query);
		PQclear(res);

		return false;
	}

	c = atoi(PQgetvalue(res, 0, 0));

	termPQExpBuffer(&query);
	PQclear(res);

	if (c == 0)
	{
		log_verbose(LOG_WARNING, _("node \"%s\" not found in \"pg_stat_replication\""), node_name);

		return false;
	}

	if (c > 1)
		log_verbose(LOG_WARNING, _("multiple entries with \"application_name\" set to  \"%s\" found in \"pg_stat_replication\""),
					node_name);

	return true;
}


void
set_primary_last_seen(PGconn *conn)
{
	PQExpBufferData query;
	PGresult   *res = NULL;

	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 "SELECT repmgr.set_primary_last_seen()");

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("unable to execute repmgr.set_primary_last_seen()"));
	}

	termPQExpBuffer(&query);
	PQclear(res);

}

int
get_primary_last_seen(PGconn *conn)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	int primary_last_seen = -1;

	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 "SELECT repmgr.get_primary_last_seen()");

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("unable to execute repmgr.get_primary_last_seen()"));
	}
	else
	{
		primary_last_seen = atoi(PQgetvalue(res, 0, 0));
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return primary_last_seen;
}


void
clear_node_info_list(NodeInfoList *nodes)
{
	NodeInfoListCell *cell = NULL;
	NodeInfoListCell *next_cell = NULL;

	log_verbose(LOG_DEBUG, "clear_node_info_list() - closing open connections");

	/* close any open connections */
	for (cell = nodes->head; cell; cell = cell->next)
	{

		if (PQstatus(cell->node_info->conn) == CONNECTION_OK)
		{
			PQfinish(cell->node_info->conn);
			cell->node_info->conn = NULL;
		}
	}

	log_verbose(LOG_DEBUG, "clear_node_info_list() - unlinking");

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


/* ================================================ */
/* PostgreSQL configuration file location functions */
/* ================================================ */

bool
get_datadir_configuration_files(PGconn *conn, KeyValueList *list)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	int			i;
	bool		success = true;

	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 "WITH files AS ( "
						 "  WITH dd AS ( "
						 "   SELECT setting "
						 "     FROM pg_catalog.pg_settings "
						 "    WHERE name = 'data_directory') "
						 "   SELECT distinct(sourcefile) AS config_file"
						 "     FROM dd, pg_catalog.pg_settings ps "
						 "    WHERE ps.sourcefile IS NOT NULL "
						 "      AND ps.sourcefile ~ ('^' || dd.setting) "
						 "       UNION "
						 "   SELECT ps.setting  AS config_file"
						 "     FROM dd, pg_catalog.pg_settings ps "
						 "    WHERE ps.name IN ('config_file', 'hba_file', 'ident_file') "
						 "      AND ps.setting ~ ('^' || dd.setting) "
						 ") "
						 "  SELECT config_file, "
						 "         pg_catalog.regexp_replace(config_file, '^.*\\/','') AS filename "
						 "    FROM files "
						 "ORDER BY config_file");

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("get_datadir_configuration_files(): unable to retrieve configuration file information"));

		success = false;
	}
	else
	{
		for (i = 0; i < PQntuples(res); i++)
		{
			key_value_list_set(list,
							   PQgetvalue(res, i, 1),
							   PQgetvalue(res, i, 0));
		}
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}


bool
get_configuration_file_locations(PGconn *conn, t_configfile_list *list)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	int			i;

	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 "  WITH dd AS ( "
						 "    SELECT setting AS data_directory"
						 "      FROM pg_catalog.pg_settings "
						 "     WHERE name = 'data_directory' "
						 "  ) "
						 "    SELECT DISTINCT(sourcefile), "
						 "           pg_catalog.regexp_replace(sourcefile, '^.*\\/', '') AS filename, "
						 "           sourcefile ~ ('^' || dd.data_directory) AS in_data_dir "
						 "      FROM dd, pg_catalog.pg_settings ps "
						 "     WHERE sourcefile IS NOT NULL "
						 "  ORDER BY 1 ");

	log_verbose(LOG_DEBUG, "get_configuration_file_locations():\n  %s",
				query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("get_configuration_file_locations(): unable to retrieve configuration file locations"));

		termPQExpBuffer(&query);
		PQclear(res);

		return false;
	}

	/*
	 * allocate memory for config file array - number of rows returned from
	 * above query + 2 for pg_hba.conf, pg_ident.conf
	 */

	config_file_list_init(list, PQntuples(res) + 2);

	for (i = 0; i < PQntuples(res); i++)
	{
		config_file_list_add(list,
							 PQgetvalue(res, i, 0),
							 PQgetvalue(res, i, 1),
							 atobool(PQgetvalue(res, i, 2)));
	}

	termPQExpBuffer(&query);
	PQclear(res);

	/* Fetch locations of pg_hba.conf and pg_ident.conf */
	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 "  WITH dd AS ( "
						 "    SELECT setting AS data_directory"
						 "      FROM pg_catalog.pg_settings "
						 "     WHERE name = 'data_directory' "
						 "  ) "
						 "    SELECT ps.setting, "
						 "           pg_catalog.regexp_replace(setting, '^.*\\/', '') AS filename, "
						 "           ps.setting ~ ('^' || dd.data_directory) AS in_data_dir "
						 "      FROM dd, pg_catalog.pg_settings ps "
						 "     WHERE ps.name IN ('hba_file', 'ident_file') "
						 "  ORDER BY 1 ");

	log_verbose(LOG_DEBUG, "get_configuration_file_locations():\n  %s",
				query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("get_configuration_file_locations(): unable to retrieve configuration file locations"));

		termPQExpBuffer(&query);
		PQclear(res);

		return false;
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		config_file_list_add(list,
							 PQgetvalue(res, i, 0),
							 PQgetvalue(res, i, 1),
							 atobool(PQgetvalue(res, i, 2)));
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return true;
}


void
config_file_list_init(t_configfile_list *list, int max_size)
{
	list->size = max_size;
	list->entries = 0;
	list->files = pg_malloc0(sizeof(t_configfile_info *) * max_size);

	if (list->files == NULL)
	{
		log_error(_("config_file_list_init(): unable to allocate memory; terminating"));
		exit(ERR_OUT_OF_MEMORY);
	}
}


void
config_file_list_add(t_configfile_list *list, const char *file, const char *filename, bool in_data_dir)
{
	/* Failsafe to prevent entries being added beyond the end */
	if (list->entries == list->size)
		return;

	list->files[list->entries] = pg_malloc0(sizeof(t_configfile_info));

	if (list->files[list->entries] == NULL)
	{
		log_error(_("config_file_list_add(): unable to allocate memory; terminating"));
		exit(ERR_OUT_OF_MEMORY);
	}


	strncpy(list->files[list->entries]->filepath, file, MAXPGPATH);
	canonicalize_path(list->files[list->entries]->filepath);


	strncpy(list->files[list->entries]->filename, filename, MAXPGPATH);
	list->files[list->entries]->in_data_directory = in_data_dir;

	list->entries++;
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
	PQExpBufferData query;
	PGresult   *res = NULL;
	char		event_timestamp[MAXLEN] = "";
	bool		success = true;

	log_verbose(LOG_DEBUG, "_create_event(): event is \"%s\" for node %i", event, node_id);

	/*
	 * Only attempt to write a record if a connection handle was provided.
	 * Also check that the repmgr schema has been properly initialised - if
	 * not it means no configuration file was provided, which can happen with
	 * e.g. `repmgr standby clone`, and we won't know which schema to write
	 * to.
	 */
	if (conn != NULL && PQstatus(conn) == CONNECTION_OK)
	{
		int			n_node_id = htonl(node_id);
		char	   *t_successful = successful ? "TRUE" : "FALSE";

		const char *values[4] = {(char *) &n_node_id,
			event,
			t_successful,
			details
		};

		int			lengths[4] = {sizeof(n_node_id),
			0,
			0,
			0
		};

		int			binary[4] = {1, 0, 0, 0};

		initPQExpBuffer(&query);
		appendPQExpBufferStr(&query,
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


		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			/* we don't treat this as a fatal error */
			log_warning(_("unable to create event record"));
			log_detail("%s", PQerrorMessage(conn));
			log_detail("%s", query.data);

			success = false;
		}
		else
		{
			/* Store timestamp to send to the notification command */
			strncpy(event_timestamp, PQgetvalue(res, 0, 0), MAXLEN);
		}

		termPQExpBuffer(&query);
		PQclear(res);
	}

	/*
	 * If no database connection provided, or the query failed, generate a
	 * current timestamp ourselves. This isn't quite the same format as
	 * PostgreSQL, but is close enough for diagnostic use.
	 */
	if (!strlen(event_timestamp))
	{
		time_t		now;
		struct tm	ts;

		time(&now);
		ts = *localtime(&now);
		strftime(event_timestamp, MAXLEN, "%Y-%m-%d %H:%M:%S%z", &ts);
	}

	log_verbose(LOG_DEBUG, "_create_event(): Event timestamp is \"%s\"", event_timestamp);

	/* an event notification command was provided - parse and execute it */
	if (send_notification == true && strlen(options->event_notification_command))
	{
		char		parsed_command[MAXPGPATH] = "";
		const char *src_ptr = NULL;
		char	   *dst_ptr = NULL;
		char	   *end_ptr = NULL;
		int			r = 0;

		log_verbose(LOG_DEBUG, "_create_event(): command is '%s'", options->event_notification_command);
		/*
		 * If configuration option 'event_notifications' was provided, check
		 * if this event is one of the ones listed; if not listed, don't
		 * execute the notification script.
		 *
		 * (If 'event_notifications' was not provided, we assume the script
		 * should be executed for all events).
		 */
		if (options->event_notifications.head != NULL)
		{
			EventNotificationListCell *cell = NULL;
			bool		notify_ok = false;

			for (cell = options->event_notifications.head; cell; cell = cell->next)
			{
				if (strcmp(event, cell->event_type) == 0)
				{
					notify_ok = true;
					break;
				}
			}

			/*
			 * Event type not found in the 'event_notifications' list - return
			 * early
			 */
			if (notify_ok == false)
			{
				log_debug(_("not executing notification script for event type \"%s\""), event);
				return success;
			}
		}

		dst_ptr = parsed_command;
		end_ptr = parsed_command + MAXPGPATH - 1;
		*end_ptr = '\0';

		for (src_ptr = options->event_notification_command; *src_ptr; src_ptr++)
		{
			if (*src_ptr == '%')
			{
				switch (src_ptr[1])
				{
					case '%':
						/* %%: replace with % */
						if (dst_ptr < end_ptr)
						{
							src_ptr++;
							*dst_ptr++ = *src_ptr;
						}
						break;
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
							log_verbose(LOG_DEBUG, "node_name: %s", event_info->node_name);
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
							PQExpBufferData details_escaped;
							initPQExpBuffer(&details_escaped);

							escape_double_quotes(details, &details_escaped);

							strlcpy(dst_ptr, details_escaped.data, end_ptr - dst_ptr);
							dst_ptr += strlen(dst_ptr);
							termPQExpBuffer(&details_escaped);
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
							log_debug("conninfo: %s", event_info->conninfo_str);

							strlcpy(dst_ptr, event_info->conninfo_str, end_ptr - dst_ptr);
							dst_ptr += strlen(dst_ptr);
						}
						break;
					case 'p':
						/* %p: primary id ("standby_switchover": former primary id) */
						src_ptr++;
						if (event_info->node_id != UNKNOWN_NODE_ID)
						{
							PQExpBufferData node_id;
							initPQExpBuffer(&node_id);
							appendPQExpBuffer(&node_id,
											  "%i", event_info->node_id);
							strlcpy(dst_ptr, node_id.data, end_ptr - dst_ptr);
							dst_ptr += strlen(dst_ptr);
							termPQExpBuffer(&node_id);
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

		log_info(_("executing notification command for event \"%s\""),
				 event);

		log_detail(_("command is:\n  %s"), parsed_command);
		r = system(parsed_command);
		if (r != 0)
		{
			log_warning(_("unable to execute event notification command"));
			log_detail(_("parsed event notification command was:\n  %s"), parsed_command);
			success = false;
		}
	}

	return success;
}


PGresult *
get_event_records(PGconn *conn, int node_id, const char *node_name, const char *event, bool all, int limit)
{
	PGresult   *res;

	PQExpBufferData query;
	PQExpBufferData where_clause;


	initPQExpBuffer(&query);
	initPQExpBuffer(&where_clause);

	/* LEFT JOIN used here as a node record may have been removed */
	appendPQExpBufferStr(&query,
						 "   SELECT e.node_id, n.node_name, e.event, e.successful, "
						 "          pg_catalog.to_char(e.event_timestamp, 'YYYY-MM-DD HH24:MI:SS') AS timestamp, "
						 "          e.details "
						 "     FROM repmgr.events e "
						 "LEFT JOIN repmgr.nodes n ON e.node_id = n.node_id ");

	if (node_id != UNKNOWN_NODE_ID)
	{
		append_where_clause(&where_clause,
							"n.node_id=%i", node_id);
	}
	else if (node_name[0] != '\0')
	{
		char	   *escaped = escape_string(conn, node_name);

		if (escaped == NULL)
		{
			log_error(_("unable to escape value provided for node name"));
			log_detail(_("node name is: \"%s\""), node_name);
		}
		else
		{
			append_where_clause(&where_clause,
								"n.node_name='%s'",
								escaped);
			pfree(escaped);
		}
	}

	if (event[0] != '\0')
	{
		char	   *escaped = escape_string(conn, event);

		if (escaped == NULL)
		{
			log_error(_("unable to escape value provided for event"));
			log_detail(_("event is: \"%s\""), event);
		}
		else
		{
			append_where_clause(&where_clause,
								"e.event='%s'",
								escaped);
			pfree(escaped);
		}
	}

	appendPQExpBuffer(&query, "\n%s\n",
					  where_clause.data);

	appendPQExpBufferStr(&query,
						 " ORDER BY e.event_timestamp DESC");

	if (all == false && limit > 0)
	{
		appendPQExpBuffer(&query, " LIMIT %i",
						  limit);
	}

	log_debug("do_cluster_event():\n%s", query.data);
	res = PQexec(conn, query.data);

	termPQExpBuffer(&query);
	termPQExpBuffer(&where_clause);

	return res;
}


/* ========================== */
/* replication slot functions */
/* ========================== */


void
create_slot_name(char *slot_name, int node_id)
{
	maxlen_snprintf(slot_name, "repmgr_slot_%i", node_id);
}


bool
create_replication_slot(PGconn *conn, char *slot_name, PQExpBufferData *error_msg)
{
	PQExpBufferData query;
	RecordStatus record_status = RECORD_NOT_FOUND;
	PGresult   *res = NULL;
	t_replication_slot slot_info = T_REPLICATION_SLOT_INITIALIZER;

	/*
	 * Check whether slot exists already; if it exists and is active, that
	 * means another active standby is using it, which creates an error
	 * situation; if not we can reuse it as-is
	 */

	record_status = get_slot_record(conn, slot_name, &slot_info);

	if (record_status == RECORD_FOUND)
	{
		if (strcmp(slot_info.slot_type, "physical") != 0)
		{
			appendPQExpBuffer(error_msg,
							  _("slot \"%s\" exists and is not a physical slot\n"),
							  slot_name);
			return false;
		}

		if (slot_info.active == false)
		{
			/* XXX is this a good idea? */
			log_debug("replication slot \"%s\" exists but is inactive; reusing",
					  slot_name);

			return true;
		}

		appendPQExpBuffer(error_msg,
						  _("slot \"%s\" already exists as an active slot\n"),
						  slot_name);
		return false;
	}

	initPQExpBuffer(&query);

	/* In 9.6 and later, reserve the LSN straight away */
	if (PQserverVersion(conn) >= 90600)
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

	log_debug(_("create_replication_slot(): creating slot \"%s\" on upstream"), slot_name);
	log_verbose(LOG_DEBUG, "create_replication_slot():\n%s", query.data);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		appendPQExpBuffer(error_msg,
						  _("unable to create slot \"%s\" on the upstream node: %s\n"),
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
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		success = true;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT pg_catalog.pg_drop_replication_slot('%s')",
					  slot_name);

	log_verbose(LOG_DEBUG, "drop_replication_slot():\n  %s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("drop_replication_slot(): unable to drop replication slot \"%s\""),
					 slot_name);

		success = false;
	}
	else
	{
		log_verbose(LOG_DEBUG, "replication slot \"%s\" successfully dropped",
					slot_name);
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}


RecordStatus
get_slot_record(PGconn *conn, char *slot_name, t_replication_slot *record)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	RecordStatus record_status = RECORD_FOUND;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT slot_name, slot_type, active "
					  "  FROM pg_catalog.pg_replication_slots "
					  " WHERE slot_name = '%s' ",
					  slot_name);

	log_verbose(LOG_DEBUG, "get_slot_record():\n%s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("get_slot_record(): unable to query pg_replication_slots"));

		record_status = RECORD_ERROR;
	}
	else if (!PQntuples(res))
	{
		record_status = RECORD_NOT_FOUND;
	}
	else
	{
		strncpy(record->slot_name, PQgetvalue(res, 0, 0), MAXLEN);
		strncpy(record->slot_type, PQgetvalue(res, 0, 1), MAXLEN);
		record->active = atobool(PQgetvalue(res, 0, 2));
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return record_status;
}


int
get_free_replication_slot_count(PGconn *conn)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	int			free_slots = 0;

	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 " SELECT pg_catalog.current_setting('max_replication_slots')::INT - "
						 "        pg_catalog.count(*) AS free_slots"
						 "   FROM pg_catalog.pg_replication_slots");

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("get_free_replication_slot_count(): unable to execute replication slot query"));

		free_slots = -1;
	}
	else if (PQntuples(res) == 0)
	{
		free_slots = -1;
	}
	else
	{
		free_slots = atoi(PQgetvalue(res, 0, 0));
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return free_slots;
}


int
get_inactive_replication_slots(PGconn *conn, KeyValueList *list)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	int			i, inactive_slots = 0;

	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 "   SELECT slot_name, slot_type "
						 "     FROM pg_catalog.pg_replication_slots "
						 "    WHERE active IS FALSE "
						 " ORDER BY slot_name ");

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("get_inactive_replication_slots(): unable to execute replication slot query"));

		inactive_slots = -1;
	}
	else
	{
		inactive_slots = PQntuples(res);

		for (i = 0; i < inactive_slots; i++)
		{
			key_value_list_set(list,
							   PQgetvalue(res, i, 0),
							   PQgetvalue(res, i, 1));
		}
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return inactive_slots;
}



/* ==================== */
/* tablespace functions */
/* ==================== */

bool
get_tablespace_name_by_location(PGconn *conn, const char *location, char *name)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool	    success = true;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT spcname "
					  "  FROM pg_catalog.pg_tablespace "
					  " WHERE pg_catalog.pg_tablespace_location(oid) = '%s'",
					  location);

	log_verbose(LOG_DEBUG, "get_tablespace_name_by_location():\n%s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("get_tablespace_name_by_location(): unable to execute tablespace query"));
		success = false;
	}
	else if (PQntuples(res) == 0)
	{
		success = false;
	}
	else
	{
		strncpy(name, PQgetvalue(res, 0, 0), MAXLEN);
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}

/* ============================ */
/* asynchronous query functions */
/* ============================ */

bool
cancel_query(PGconn *conn, int timeout)
{
	char		errbuf[ERRBUFF_SIZE] = "";
	PGcancel   *pgcancel = NULL;

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
		log_warning(_("unable to stop current query:\n  %s"), errbuf);
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
	PGresult   *res = NULL;
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
			log_warning(_("wait_connection_availability(): could not receive data from connection:\n  %s"),
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
	PGPing		status = PQping(conninfo);

	log_verbose(LOG_DEBUG, "is_server_available(): ping status for %s is %i", conninfo, (int)status);
	if (status == PQPING_OK)
		return true;

	return false;
}


bool
is_server_available_params(t_conninfo_param_list *param_list)
{
	PGPing		status = PQpingParams((const char **) param_list->keywords,
									  (const char **) param_list->values,
									  false);

	/* deparsing the param_list adds overhead, so only do it if needed  */
	if (log_level == LOG_DEBUG)
	{
		char *conninfo_str = param_list_to_string(param_list);
		log_verbose(LOG_DEBUG, "is_server_available_params(): ping status for %s is %i", conninfo_str, (int)status);
		pfree(conninfo_str);
	}

	if (status == PQPING_OK)
		return true;

	return false;
}



/*
 * Simple throw-away query to stop a connection handle going stale.
 */
ExecStatusType
connection_ping(PGconn *conn)
{
	PGresult   *res = PQexec(conn, "SELECT TRUE");
	ExecStatusType ping_result;

	log_verbose(LOG_DEBUG, "connection_ping(): result is %s", PQresStatus(PQresultStatus(res)));

	ping_result = PQresultStatus(res);
	PQclear(res);

	return ping_result;
}



/* ==================== */
/* monitoring functions */
/* ==================== */

void
add_monitoring_record(PGconn *primary_conn,
					  PGconn *local_conn,
					  int primary_node_id,
					  int local_node_id,
					  char *monitor_standby_timestamp,
					  XLogRecPtr primary_last_wal_location,
					  XLogRecPtr last_wal_receive_lsn,
					  char *last_xact_replay_timestamp,
					  long long unsigned int replication_lag_bytes,
					  long long unsigned int apply_lag_bytes
)
{
	PQExpBufferData query;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "INSERT INTO repmgr.monitoring_history "
					  "           (primary_node_id, "
					  "            standby_node_id, "
					  "            last_monitor_time, "
					  "            last_apply_time, "
					  "            last_wal_primary_location, "
					  "            last_wal_standby_location, "
					  "            replication_lag, "
					  "            apply_lag ) "
					  "     VALUES(%i, "
					  "            %i, "
					  "            '%s'::TIMESTAMP WITH TIME ZONE, "
					  "            '%s'::TIMESTAMP WITH TIME ZONE, "
					  "            '%X/%X', "
					  "            '%X/%X', "
					  "            %llu, "
					  "            %llu) ",
					  primary_node_id,
					  local_node_id,
					  monitor_standby_timestamp,
					  last_xact_replay_timestamp,
					  format_lsn(primary_last_wal_location),
					  format_lsn(last_wal_receive_lsn),
					  replication_lag_bytes,
					  apply_lag_bytes);

	log_verbose(LOG_DEBUG, "standby_monitor:()\n%s", query.data);

	if (PQsendQuery(primary_conn, query.data) == 0)
	{
		log_warning(_("query could not be sent to primary:\n  %s"),
					PQerrorMessage(primary_conn));
	}
	else
	{
		PGresult   *res = PQexec(local_conn, "SELECT repmgr.standby_set_last_updated()");

		/* not critical if the above query fails */
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			log_warning(_("add_monitoring_record(): unable to set last_updated:\n  %s"),
						PQerrorMessage(local_conn));

		PQclear(res);
	}

	termPQExpBuffer(&query);

	return;
}


int
get_number_of_monitoring_records_to_delete(PGconn *primary_conn, int keep_history, int node_id)
{
	PQExpBufferData query;
	int				record_count = -1;
	PGresult	   *res = NULL;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT pg_catalog.count(*) "
					  "  FROM repmgr.monitoring_history "
					  " WHERE pg_catalog.age(pg_catalog.now(), last_monitor_time) >= '%d days'::interval",
					  keep_history);

	if (node_id != UNKNOWN_NODE_ID)
	{
		appendPQExpBuffer(&query,
						  "  AND standby_node_id = %i", node_id);
	}

	log_verbose(LOG_DEBUG, "get_number_of_monitoring_records_to_delete():\n  %s", query.data);

	res = PQexec(primary_conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(primary_conn, query.data,
					 _("get_number_of_monitoring_records_to_delete(): unable to query number of monitoring records to clean up"));
	}
	else
	{
		record_count = atoi(PQgetvalue(res, 0, 0));
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return record_count;
}


bool
delete_monitoring_records(PGconn *primary_conn, int keep_history, int node_id)
{
	PQExpBufferData query;
	bool			success = true;
	PGresult	   *res = NULL;

	initPQExpBuffer(&query);

	if (keep_history > 0 || node_id != UNKNOWN_NODE_ID)
	{
		appendPQExpBuffer(&query,
						  "DELETE FROM repmgr.monitoring_history "
						  " WHERE pg_catalog.age(pg_catalog.now(), last_monitor_time) >= '%d days'::INTERVAL ",
						  keep_history);

		if (node_id != UNKNOWN_NODE_ID)
		{
			appendPQExpBuffer(&query,
							  "  AND standby_node_id = %i", node_id);
		}
	}
	else
	{
		appendPQExpBufferStr(&query,
							 "TRUNCATE TABLE repmgr.monitoring_history");
	}

	res = PQexec(primary_conn, query.data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(primary_conn, query.data,
					 _("delete_monitoring_records(): unable to delete monitoring records"));
		success = false;
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return success;
}

/*
 * node voting functions
 *
 * These are intended to run under repmgrd and mainly rely on shared memory
 */

int
get_current_term(PGconn *conn)
{
	PGresult   *res = NULL;
	int term = VOTING_TERM_NOT_SET;

	res = PQexec(conn, "SELECT term FROM repmgr.voting_term");

	/* it doesn't matter if for whatever reason the table has no rows */

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, NULL,
					 _("get_current_term(): unable to query \"repmgr.voting_term\""));
	}
	else if (PQntuples(res) > 0)
	{
		term = atoi(PQgetvalue(res, 0, 0));
	}

	PQclear(res);
	return term;
}


void
initialize_voting_term(PGconn *conn)
{
	PGresult   *res = NULL;

	int current_term = get_current_term(conn);

	if (current_term == VOTING_TERM_NOT_SET)
	{
		res = PQexec(conn, "INSERT INTO repmgr.voting_term (term) VALUES (1)");
	}
	else
	{
		res = PQexec(conn, "UPDATE repmgr.voting_term SET term = 1");
	}

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(conn, NULL, _("unable to initialize repmgr.voting_term"));
	}

	PQclear(res);
	return;
}


void
increment_current_term(PGconn *conn)
{
	PGresult   *res = NULL;

	res = PQexec(conn, "UPDATE repmgr.voting_term SET term = term + 1");

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(conn, NULL, _("unable to increment repmgr.voting_term"));
	}

	PQclear(res);
	return;
}


bool
announce_candidature(PGconn *conn, t_node_info *this_node, t_node_info *other_node, int electoral_term)
{
	PQExpBufferData query;
	PGresult   *res = NULL;

	bool		retval = false;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT repmgr.other_node_is_candidate(%i, %i)",
					  this_node->node_id,
					  electoral_term);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_db_error(conn, query.data, _("announce_candidature(): unable to execute repmgr.other_node_is_candidate()"));
	}
	else
	{
		retval = atobool(PQgetvalue(res, 0, 0));
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return retval;
}


void
notify_follow_primary(PGconn *conn, int primary_node_id)
{
	PQExpBufferData query;
	PGresult   *res = NULL;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT repmgr.notify_follow_primary(%i)",
					  primary_node_id);

	log_verbose(LOG_DEBUG, "notify_follow_primary():\n  %s", query.data);

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("unable to execute repmgr.notify_follow_primary()"));
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return;
}


bool
get_new_primary(PGconn *conn, int *primary_node_id)
{
	PGresult   *res = NULL;
	int			new_primary_node_id = UNKNOWN_NODE_ID;
	bool		success = true;

	const char *sqlquery = "SELECT repmgr.get_new_primary()";

	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, sqlquery, _("unable to execute repmgr.get_new_primary()"));
		success = false;
	}
	else if (PQgetisnull(res, 0, 0))
	{
		success = false;
	}
	else
	{
		new_primary_node_id = atoi(PQgetvalue(res, 0, 0));
	}

	PQclear(res);

	/*
	 * repmgr.get_new_primary() will return UNKNOWN_NODE_ID if
	 * "follow_new_primary" is false
	 */
	if (new_primary_node_id == UNKNOWN_NODE_ID)
		success = false;

	*primary_node_id = new_primary_node_id;

	return success;
}


void
reset_voting_status(PGconn *conn)
{
	PGresult   *res = NULL;

	const char *sqlquery = "SELECT repmgr.reset_voting_status()";

	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, sqlquery, _("unable to execute repmgr.reset_voting_status()"));
	}

	PQclear(res);
	return;
}


/* ============================ */
/* replication status functions */
/* ============================ */


XLogRecPtr
get_current_wal_lsn(PGconn *conn)
{
	PGresult   *res = NULL;
	XLogRecPtr	ptr = InvalidXLogRecPtr;

	if (PQserverVersion(conn) >= 100000)
	{
		res = PQexec(conn, "SELECT pg_catalog.pg_current_wal_lsn()");
	}
	else
	{
		res = PQexec(conn, "SELECT pg_catalog.pg_current_xlog_location()");
	}

	if (PQresultStatus(res) == PGRES_TUPLES_OK)
	{
		ptr = parse_lsn(PQgetvalue(res, 0, 0));
	}

	PQclear(res);

	return ptr;
}

XLogRecPtr
get_last_wal_receive_location(PGconn *conn)
{
	PGresult   *res = NULL;
	XLogRecPtr	ptr = InvalidXLogRecPtr;

	if (PQserverVersion(conn) >= 100000)
	{
		res = PQexec(conn, "SELECT pg_catalog.pg_last_wal_receive_lsn()");
	}
	else
	{
		res = PQexec(conn, "SELECT pg_catalog.pg_last_xlog_receive_location()");
	}

	if (PQresultStatus(res) == PGRES_TUPLES_OK)
	{
		ptr = parse_lsn(PQgetvalue(res, 0, 0));
	}

	PQclear(res);

	return ptr;
}

XLogRecPtr
get_current_lsn(PGconn *conn)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	XLogRecPtr	ptr = InvalidXLogRecPtr;

	initPQExpBuffer(&query);

	if (PQserverVersion(conn) >= 100000)
	{
		appendPQExpBufferStr(&query,
							 " WITH lsn_states AS ( "
							 "  SELECT "
							 "    CASE WHEN pg_catalog.pg_is_in_recovery() IS FALSE "
							 "      THEN pg_catalog.pg_current_wal_lsn() "
							 "      ELSE NULL "
							 "    END "
							 "      AS current_wal_lsn, "
							 "    CASE WHEN pg_catalog.pg_is_in_recovery() IS TRUE "
							 "      THEN pg_catalog.pg_last_wal_receive_lsn() "
							 "      ELSE NULL "
							 "    END "
							 "      AS last_wal_receive_lsn, "
							 "    CASE WHEN pg_catalog.pg_is_in_recovery() IS TRUE "
							 "      THEN pg_catalog.pg_last_wal_replay_lsn() "
							 "      ELSE NULL "
							 "     END "
							 "       AS last_wal_replay_lsn "
							 " ) ");
	}
	else
	{
		appendPQExpBufferStr(&query,
							 " WITH lsn_states AS ( "
							 "  SELECT "
							 "    CASE WHEN pg_catalog.pg_is_in_recovery() IS FALSE "
							 "      THEN pg_catalog.pg_current_xlog_location() "
							 "      ELSE NULL "
							 "    END "
							 "      AS current_wal_lsn, "
							 "    CASE WHEN pg_catalog.pg_is_in_recovery() IS TRUE "
							 "      THEN pg_catalog.pg_last_xlog_receive_location() "
							 "      ELSE NULL "
							 "    END "
							 "      AS last_wal_receive_lsn, "
							 "    CASE WHEN pg_catalog.pg_is_in_recovery() IS TRUE "
							 "      THEN pg_catalog.pg_last_xlog_replay_location() "
							 "      ELSE NULL "
							 "     END "
							 "       AS last_wal_replay_lsn "
							 " ) ");
	}

	appendPQExpBufferStr(&query,
						 " SELECT "
						 "   CASE WHEN pg_catalog.pg_is_in_recovery() IS FALSE "
						 "     THEN current_wal_lsn "
						 "     ELSE "
						 "       CASE WHEN last_wal_receive_lsn IS NULL "
						 "       THEN last_wal_replay_lsn "
						 "         ELSE "
						 "           CASE WHEN last_wal_replay_lsn > last_wal_receive_lsn "
						 "             THEN last_wal_replay_lsn "
						 "             ELSE last_wal_receive_lsn "
						 "           END "
						 "       END "
						 "   END "
						 "     AS current_lsn "
						 "   FROM lsn_states ");

	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("unable to execute get_current_lsn()"));
	}
	else if (!PQgetisnull(res, 0, 0))
	{
		ptr = parse_lsn(PQgetvalue(res, 0, 0));
	}

	termPQExpBuffer(&query);
	PQclear(res);

	return ptr;
}

/* ============= */
/* BDR functions */
/* ============= */


static bool
_is_bdr_db(PGconn *conn, PQExpBufferData *output, bool quiet)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		is_bdr_db = false;

	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 " SELECT (pg_catalog.regexp_matches(extversion, '^\\d+'))[1] AS major_version "
						 "  FROM pg_catalog.pg_extension "
						 " WHERE extname = 'bdr' ");

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0)
	{
		is_bdr_db = false;
		bdr_version_num = UNKNOWN_BDR_VERSION_NUM;
	}
	else
	{
		is_bdr_db = true;
		bdr_version_num = atoi(PQgetvalue(res, 0, 0));
	}

	PQclear(res);

	log_verbose(LOG_DEBUG, "BDR ext version number is %i", bdr_version_num);

	if (is_bdr_db == false)
	{
		const char *warning = _("BDR extension is not available for this database");

		if (output != NULL)
			appendPQExpBufferStr(output, warning);
		else if (quiet == false)
			log_warning("%s", warning);

		return is_bdr_db;
	}

	if (bdr_version_num < 3)
	{
		initPQExpBuffer(&query);

		appendPQExpBufferStr(&query,
							 "SELECT bdr.bdr_is_active_in_db()");
		res = PQexec(conn, query.data);
		termPQExpBuffer(&query);

		is_bdr_db = atobool(PQgetvalue(res, 0, 0));

		if (is_bdr_db == false)
		{
			const char *warning = _("BDR extension available for this database, but the database is not configured for BDR");

			if (output != NULL)
				appendPQExpBufferStr(output, warning);
			else if (quiet == false)
				log_warning("%s", warning);
		}

		PQclear(res);
	}


	return is_bdr_db;
}


bool
is_bdr_db(PGconn *conn, PQExpBufferData *output)
{
	return _is_bdr_db(conn, output, false);
}


bool
is_bdr_db_quiet(PGconn *conn)
{
	return _is_bdr_db(conn, NULL, true);
}


int
get_bdr_version_num(void)
{
	return bdr_version_num;
}

bool
is_active_bdr_node(PGconn *conn, const char *node_name)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		is_active_bdr_node = false;

	initPQExpBuffer(&query);

	if (bdr_version_num < 3)
	{
		appendPQExpBuffer(&query,
						  "    SELECT COALESCE(s.active, TRUE) AS active"
						  "      FROM bdr.bdr_nodes n "
						  " LEFT JOIN pg_catalog.pg_replication_slots s "
						  "        ON s.slot_name=bdr.bdr_format_slot_name(n.node_sysid, n.node_timeline, n.node_dboid, (SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database())) "
						  "     WHERE n.node_name='%s' ",
						  node_name);
	}
	else
	{
		appendPQExpBuffer(&query,
						  "     SELECT COALESCE(s.active, FALSE) AS active"
						  "       FROM bdr.node bn "
						  " INNER JOIN pglogical.node pn "
						  "         ON (pn.node_id = bn.pglogical_node_id) "
						  "  LEFT JOIN pg_catalog.pg_replication_slots s "
						  "         ON s.slot_name=bn.local_slot_name "
						  "      WHERE pn.node_name='%s' ",
						  node_name);
	}

	log_verbose(LOG_DEBUG, "is_active_bdr_node():\n  %s", query.data);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	/* we don't care if the query fails */
	if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0)
	{
		is_active_bdr_node = false;
	}
	else
	{
		is_active_bdr_node = atobool(PQgetvalue(res, 0, 0));
	}

	PQclear(res);

	return is_active_bdr_node;
}


bool
is_bdr_repmgr(PGconn *conn)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	int			non_bdr_nodes = 0;

	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 "SELECT pg_catalog.count(*)"
						 "  FROM repmgr.nodes n"
						 " WHERE n.type != 'bdr' ");

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



/*
 * Get name of default BDR replication set.
 *
 * Caller must free provided value.
 */
char *
get_default_bdr_replication_set(PGconn *conn)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	char	   *default_replication_set = NULL;
	int			namelen;

	if (bdr_version_num < 3)
	{
		/* For BDR2, we use a custom replication set */
		namelen = strlen(BDR2_REPLICATION_SET_NAME);
		default_replication_set = pg_malloc0(namelen + 1);
		strncpy(default_replication_set, BDR2_REPLICATION_SET_NAME, namelen);

		return default_replication_set;
	}

	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 "     SELECT rs.set_name "
						 "       FROM pglogical.replication_set rs "
						 " INNER JOIN bdr.node_group ng "
						 "         ON ng.node_group_default_repset = rs.set_id ");

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0)
	{
		log_warning(_("unable to retrieve default BDR replication set name"));

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			log_detail("%s", PQerrorMessage(conn));

		PQclear(res);
		return NULL;
	}

	namelen = strlen(PQgetvalue(res, 0, 0));
	default_replication_set = pg_malloc0(namelen + 1);

	strncpy(default_replication_set, PQgetvalue(res, 0, 0), namelen);

	PQclear(res);

	return default_replication_set;
}


bool
is_table_in_bdr_replication_set(PGconn *conn, const char *tablename, const char *set)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		in_replication_set = false;

	initPQExpBuffer(&query);

	if (bdr_version_num < 3)
	{
		appendPQExpBuffer(&query,
						  "SELECT pg_catalog.count(*) "
						  "  FROM pg_catalog.unnest(bdr.table_get_replication_sets('repmgr.%s')) AS repset "
						  " WHERE repset='%s' ",
						  tablename,
						  set);
	}
	else
	{
		appendPQExpBuffer(&query,
						  "     SELECT pg_catalog.count(*) "
						  "       FROM pglogical.replication_set s "
						  " INNER JOIN pglogical.replication_set_table st "
						  "         ON s.set_id = st.set_id "
						  "      WHERE s.set_name = '%s' "
						  "        AND st.set_reloid = 'repmgr.%s'::REGCLASS ",
						  set,
						  tablename);

	}

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
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		success = true;

	initPQExpBuffer(&query);

	if (bdr_version_num < 3)
	{
		appendPQExpBuffer(&query,
						  "SELECT bdr.table_set_replication_sets('repmgr.%s', '{%s}')",
						  tablename,
						  set);
	}
	else
	{
		appendPQExpBuffer(&query,
						  " SELECT bdr.replication_set_add_table( "
						  "   relation := 'repmgr.%s', "
						  "   set_name := '%s' "
						  " ) ",
						  tablename,
						  set);
	}

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to add table \"repmgr.%s\" to replication set \"%s\""),
				  tablename,
				  set);
		log_detail("%s", PQerrorMessage(conn));

		success = false;
	}

	PQclear(res);

	return success;
}


bool
bdr_node_name_matches(PGconn *conn, const char *node_name, PQExpBufferData *bdr_local_node_name)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		node_exists = false;

	initPQExpBuffer(&query);

	if (bdr_version_num < 3)
	{
		appendPQExpBufferStr(&query,
							 "SELECT bdr.bdr_get_local_node_name() AS node_name");
	}
	else
	{
		appendPQExpBufferStr(&query,
							 "SELECT node_name FROM bdr.local_node_info()");
	}

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		node_exists = false;
	}
	else
	{
		node_exists = true;
		appendPQExpBuffer(bdr_local_node_name,
						  "%s", PQgetvalue(res, 0, 0));
	}

	PQclear(res);

	return node_exists;
}


ReplSlotStatus
get_bdr_node_replication_slot_status(PGconn *conn, const char *node_name)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	ReplSlotStatus status = SLOT_UNKNOWN;

	initPQExpBuffer(&query);

	if (bdr_version_num < 3)
	{
		appendPQExpBuffer(&query,
						  " SELECT s.active "
						  "   FROM pg_catalog.pg_replication_slots s "
						  "  WHERE slot_name = "
						  "    (SELECT bdr.bdr_format_slot_name(node_sysid, node_timeline, node_dboid, datoid) "
						  "   FROM bdr.bdr_nodes "
						  " WHERE node_name = '%s') ",
						  node_name);
	}
	else
	{
		appendPQExpBuffer(&query,
						  "     SELECT COALESCE(s.active, FALSE) AS active"
						  "       FROM bdr.node bn "
						  " INNER JOIN pglogical.node pn "
						  "         ON (pn.node_id = bn.pglogical_node_id) "
						  " INNER JOIN pg_catalog.pg_replication_slots s "
						  "         ON s.slot_name=bn.local_slot_name "
						  "      WHERE pn.node_name='%s' ",
						  node_name);
	}

	log_verbose(LOG_DEBUG, "get_bdr_node_replication_slot_status():\n  %s", query.data);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0)
	{
		status = SLOT_UNKNOWN;
	}
	else
	{
		status = (atobool(PQgetvalue(res, 0, 0)) == true)
			? SLOT_ACTIVE
			: SLOT_INACTIVE;
	}

	PQclear(res);

	return status;
}


void
get_bdr_other_node_name(PGconn *conn, int node_id, char *node_name)
{
	PQExpBufferData query;
	PGresult   *res = NULL;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  " SELECT n.node_name "
					  "   FROM repmgr.nodes n "
					  "  WHERE n.node_id != %i",
					  node_id);

	log_verbose(LOG_DEBUG, "get_bdr_other_node_name():\n  %s", query.data);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) == PGRES_TUPLES_OK)
	{
		strncpy(node_name, PQgetvalue(res, 0, 0), MAXLEN);
	}
	else
	{
		log_warning(_("get_bdr_other_node_name(): unable to execute query\n  %s"),
					PQerrorMessage(conn));
	}
	PQclear(res);

	return;
}


/*
 * For BDR 2.x only
 */
void
add_extension_tables_to_bdr_replication_set(PGconn *conn)
{
	PQExpBufferData query;
	PGresult   *res = NULL;

	initPQExpBuffer(&query);

	appendPQExpBufferStr(&query,
						 "    SELECT c.relname "
						 "      FROM pg_class c "
						 "INNER JOIN pg_namespace n "
						 "        ON c.relnamespace = n.oid "
						 "     WHERE n.nspname = 'repmgr' "
						 "       AND c.relkind = 'r' ");

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		/* XXX log error */
	}
	else
	{
		int			i;

		for (i = 0; i < PQntuples(res); i++)
		{
			add_table_to_bdr_replication_set(conn,
											 PQgetvalue(res, i, 0),
											 "repmgr");
		}
	}

	PQclear(res);

	return;
}

void
get_all_bdr_node_records(PGconn *conn, BdrNodeInfoList *node_list)
{
	PQExpBufferData query;
	PGresult   *res = NULL;

	initPQExpBuffer(&query);

	if (bdr_version_num < 3)
	{
		appendPQExpBufferStr(&query,
							 "  SELECT " BDR2_NODES_COLUMNS
							 "    FROM bdr.bdr_nodes "
							 "ORDER BY node_seq_id ");
	}
	else
	{
		appendPQExpBufferStr(&query,
							 "     SELECT " BDR3_NODES_COLUMNS
							 "       FROM bdr.node_summary ns "
							 "   ORDER BY node_name");
	}

	log_verbose(LOG_DEBUG, "get_all_bdr_node_records():\n%s", query.data);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	_populate_bdr_node_records(res, node_list);

	PQclear(res);
	return;
}

RecordStatus
get_bdr_node_record_by_name(PGconn *conn, const char *node_name, t_bdr_node_info *node_info)
{
	PQExpBufferData query;
	PGresult   *res = NULL;

	initPQExpBuffer(&query);

	if (bdr_version_num < 3)
	{
		appendPQExpBuffer(&query,
						  "  SELECT " BDR2_NODES_COLUMNS
						  "    FROM bdr.bdr_nodes "
						  "   WHERE node_name = '%s'",
						  node_name);
	}
	else
	{
		appendPQExpBuffer(&query,
						  "     SELECT " BDR3_NODES_COLUMNS
						  "       FROM bdr.node_summary ns "
						  "      WHERE ns.node_name = '%s'",
						  node_name);
	}

	log_verbose(LOG_DEBUG, "get_bdr_node_record_by_name():\n%s", query.data);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to retrieve BDR node record for \"%s\":\n  %s"),
				  node_name,
				  PQerrorMessage(conn));

		PQclear(res);
		return RECORD_ERROR;
	}

	if (PQntuples(res) == 0)
	{
		PQclear(res);
		return RECORD_NOT_FOUND;
	}

	_populate_bdr_node_record(res, node_info, 0);

	PQclear(res);

	return RECORD_FOUND;
}


static
void
_populate_bdr_node_records(PGresult *res, BdrNodeInfoList *node_list)
{
	int			i;

	clear_node_info_list((NodeInfoList *) node_list);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		return;
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		BdrNodeInfoListCell *cell;

		cell = (BdrNodeInfoListCell *) pg_malloc0(sizeof(BdrNodeInfoListCell));

		cell->node_info = pg_malloc0(sizeof(t_bdr_node_info));

		_populate_bdr_node_record(res, cell->node_info, i);

		if (node_list->tail)
			node_list->tail->next = cell;
		else
			node_list->head = cell;

		node_list->tail = cell;
		node_list->node_count++;
	}

	return;
}


static void
_populate_bdr_node_record(PGresult *res, t_bdr_node_info *node_info, int row)
{
	strncpy(node_info->node_sysid, PQgetvalue(res, row, 0), MAXLEN);
	node_info->node_timeline = atoi(PQgetvalue(res, row, 1));
	node_info->node_dboid = atoi(PQgetvalue(res, row, 2));
	strncpy(node_info->node_name, PQgetvalue(res, row, 3), MAXLEN);
	strncpy(node_info->node_local_dsn, PQgetvalue(res, row, 4), MAXLEN);
	strncpy(node_info->peer_state_name, PQgetvalue(res, row, 5), MAXLEN);
}


bool
am_bdr_failover_handler(PGconn *conn, int node_id)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		am_handler = false;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "SELECT repmgr.am_bdr_failover_handler(%i)",
					  node_id);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to execute function repmgr.am_bdr_failover_handler():\n  %s"),
				  PQerrorMessage(conn));
		PQclear(res);
		return false;
	}


	am_handler = atobool(PQgetvalue(res, 0, 0));

	PQclear(res);

	return am_handler;
}

void
unset_bdr_failover_handler(PGconn *conn)
{
	PGresult   *res = NULL;

	res = PQexec(conn, "SELECT repmgr.unset_bdr_failover_handler()");

	PQclear(res);
	return;
}


bool
bdr_node_has_repmgr_set(PGconn *conn, const char *node_name)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		has_repmgr_set = false;

	if (bdr_version_num >= 3)
		return true;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  " SELECT pg_catalog.count(*) "
					  "   FROM pg_catalog.unnest(bdr.connection_get_replication_sets('%s') AS repset "
					  "  WHERE repset = '%s'",
					  node_name,
					  BDR2_REPLICATION_SET_NAME);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0)
	{
		has_repmgr_set = false;
	}
	else
	{
		has_repmgr_set = atoi(PQgetvalue(res, 0, 0)) == 1 ? true : false;
	}

	PQclear(res);

	return has_repmgr_set;
}


bool
bdr_node_set_repmgr_set(PGconn *conn, const char *node_name)
{
	PQExpBufferData query;
	PGresult   *res = NULL;
	bool		success = true;

	if (bdr_version_num >= 3)
		return true;

	initPQExpBuffer(&query);

	/*
	 * Here we extract a list of existing replication sets, add 'repmgr', and
	 * set the replication sets to the new list.
	 */
	appendPQExpBuffer(&query,
					  " SELECT bdr.connection_set_replication_sets( "
					  "   ARRAY( "
					  "     SELECT repset::TEXT "
					  "       FROM pg_catalog.unnest(bdr.connection_get_replication_sets('%s')) AS repset "
					  "         UNION "
					  "     SELECT '%s'::TEXT "
					  "   ), "
					  "   '%s' "
					  " ) ",
					  node_name,
					  BDR2_REPLICATION_SET_NAME,
					  node_name);

	log_debug("bdr_node_set_repmgr_set():\n%s", query.data);

	res = PQexec(conn, query.data);
	termPQExpBuffer(&query);


	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_debug("result status: %s", PQresStatus(PQresultStatus(res)));
		log_error(_("unable to create replication set \"repmgr\""));
		log_detail("%s", PQerrorMessage(conn));
		success = false;
	}

	PQclear(res);

	return success;
}



/* miscellaneous debugging functions */

const char *
print_node_status(NodeStatus node_status)
{
	switch (node_status)
	{
		case NODE_STATUS_UNKNOWN:
			return "UNKNOWN";
		case NODE_STATUS_UP:
			return "UP";
		case NODE_STATUS_SHUTTING_DOWN:
			return "SHUTTING_DOWN";
		case NODE_STATUS_DOWN:
			return "DOWN";
		case NODE_STATUS_UNCLEAN_SHUTDOWN:
			return "UNCLEAN_SHUTDOWN";
	}

	return "UNIDENTIFIED_STATUS";
}


const char *
print_pqping_status(PGPing ping_status)
{
	switch (ping_status)
	{
		case PQPING_OK:
			return "PQPING_OK";
		case PQPING_REJECT:
			return "PQPING_REJECT";
		case PQPING_NO_RESPONSE:
			return "PQPING_NO_RESPONSE";
		case PQPING_NO_ATTEMPT:
			return "PQPING_NO_ATTEMPT";
	}

	return "PQPING_UNKNOWN_STATUS";
}
