/*
 * config.c - Functions to parse the config file
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.	 If not, see <http://www.gnu.org/licenses/>.
 *
 */

#include <sys/stat.h>			/* for stat() */

#include "config.h"
#include "log.h"
#include "strutil.h"
#include "repmgr.h"

static void parse_event_notifications_list(t_configuration_options *options, const char *arg);
static void tablespace_list_append(t_configuration_options *options, const char *arg);
static void exit_with_errors(ItemList *config_errors);

const static char *_progname = NULL;
static char config_file_path[MAXPGPATH];
static bool config_file_provided = false;
bool config_file_found = false;


void
set_progname(const char *argv0)
{
	_progname = get_progname(argv0);
}

const char *
progname(void)
{
	return _progname;
}

/*
 * load_config()
 *
 * Set default options and overwrite with values from provided configuration
 * file.
 *
 * Returns true if a configuration file could be parsed, otherwise false.
 *
 * Any *repmgrd-specific* configuration options added/changed in this function must also be
 * added/changed in reload_config()
 *
 * NOTE: this function is called before the logger is set up, so we need
 * to handle the verbose option ourselves; also the default log level is NOTICE,
 * so we can't use DEBUG.
 */
bool
load_config(const char *config_file, bool verbose, t_configuration_options *options, char *argv0)
{
	struct stat stat_config;

	/*
	 * If a configuration file was provided, check it exists, otherwise
	 * emit an error and terminate. We assume that if a user explicitly
	 * provides a configuration file, they'll want to make sure it's
	 * used and not fall back to any of the defaults.
	 */
	if (config_file[0])
	{
		strncpy(config_file_path, config_file, MAXPGPATH);
		canonicalize_path(config_file_path);

		if (stat(config_file_path, &stat_config) != 0)
		{
			log_err(_("provided configuration file \"%s\" not found: %s\n"),
					config_file,
					strerror(errno)
				);
			exit(ERR_BAD_CONFIG);
		}

		if (verbose == true)
		{
			log_notice(_("using configuration file \"%s\"\n"), config_file);
		}

		config_file_provided = true;
		config_file_found = true;
	}

	/*
	 * If no configuration file was provided, attempt to find a default file
	 * in this order:
	 *	- current directory
	 *	- /etc/repmgr.conf
	 *	- default sysconfdir
	 *
	 * here we just check for the existence of the file; parse_config()
	 * will handle read errors etc.
	 */
	if (config_file_provided == false)
	{
		char		my_exec_path[MAXPGPATH];
		char		sysconf_etc_path[MAXPGPATH];

		/* 1. "./repmgr.conf" */
		if (verbose == true)
		{
			log_notice(_("looking for configuration file in current directory\n"));
		}

		snprintf(config_file_path, MAXPGPATH, "./%s", CONFIG_FILE_NAME);
		canonicalize_path(config_file_path);

		if (stat(config_file_path, &stat_config) == 0)
		{
			config_file_found = true;
			goto end_search;
		}

		/* 2. "/etc/repmgr.conf" */
		if (verbose == true)
		{
			log_notice(_("looking for configuration file in /etc\n"));
		}

		snprintf(config_file_path, MAXPGPATH, "/etc/%s", CONFIG_FILE_NAME);
		if (stat(config_file_path, &stat_config) == 0)
		{
			config_file_found = true;
			goto end_search;
		}

		/* 3. default sysconfdir */
		if (find_my_exec(argv0, my_exec_path) < 0)
		{
			fprintf(stderr, _("%s: could not find own program executable\n"), argv0);
			exit(EXIT_FAILURE);
		}

		get_etc_path(my_exec_path, sysconf_etc_path);

		if (verbose == true)
		{
			log_notice(_("looking for configuration file in %s\n"), sysconf_etc_path);
		}

		snprintf(config_file_path, MAXPGPATH, "%s/%s", sysconf_etc_path, CONFIG_FILE_NAME);
		if (stat(config_file_path, &stat_config) == 0)
		{
			config_file_found = true;
			goto end_search;
		}

	end_search:
		if (config_file_found == true)
		{
			if (verbose == true)
			{
				log_notice(_("configuration file found at: %s\n"), config_file_path);
			}
		}
		else
		{
			if (verbose == true)
			{
				log_notice(_("no configuration file provided or found\n"));
			}
		}
	}

	return parse_config(options);
}


bool
parse_config(t_configuration_options *options)
{
	/* Collate configuration file errors here for friendlier reporting */
	static ItemList config_errors = { NULL, NULL };

	_parse_config(options, &config_errors);

	if (config_errors.head != NULL)
	{
		exit_with_errors(&config_errors);
	}

	return true;
}


/*
 * Parse configuration file; if any errors are encountered,
 * list them and exit.
 *
 * Ensure any default values set here are synced with repmgr.conf.sample
 * and any other documentation.
 */
void
_parse_config(t_configuration_options *options, ItemList *error_list)
{
	FILE	   *fp;
	char	   *s,
				buf[MAXLINELENGTH];
	char		name[MAXLEN];
	char		value[MAXLEN];

	/* For sanity-checking provided conninfo string */
	PQconninfoOption *conninfo_options;
	char	   *conninfo_errmsg = NULL;

	bool		node_found = false;

	/* Initialize configuration options with sensible defaults
	 * note: the default log level is set in log.c and does not need
	 * to be initialised here
	 */
	memset(options->cluster_name, 0, sizeof(options->cluster_name));
	options->node = UNKNOWN_NODE_ID;
	options->upstream_node = NO_UPSTREAM_NODE;
	options->use_replication_slots = 0;
	memset(options->conninfo, 0, sizeof(options->conninfo));
	memset(options->barman_server, 0, sizeof(options->barman_server));
	memset(options->barman_config, 0, sizeof(options->barman_config));
	options->failover = MANUAL_FAILOVER;
	options->priority = DEFAULT_PRIORITY;
	memset(options->node_name, 0, sizeof(options->node_name));
	memset(options->promote_command, 0, sizeof(options->promote_command));
	memset(options->follow_command, 0, sizeof(options->follow_command));
	memset(options->service_stop_command, 0, sizeof(options->service_stop_command));
	memset(options->service_start_command, 0, sizeof(options->service_start_command));
	memset(options->service_restart_command, 0, sizeof(options->service_restart_command));
	memset(options->service_reload_command, 0, sizeof(options->service_reload_command));
	memset(options->service_promote_command, 0, sizeof(options->service_promote_command));
	memset(options->rsync_options, 0, sizeof(options->rsync_options));
	memset(options->ssh_options, 0, sizeof(options->ssh_options));
	memset(options->pg_bindir, 0, sizeof(options->pg_bindir));
	memset(options->pg_ctl_options, 0, sizeof(options->pg_ctl_options));
	memset(options->pg_basebackup_options, 0, sizeof(options->pg_basebackup_options));
	memset(options->restore_command, 0, sizeof(options->restore_command));

	/* default master_response_timeout is 60 seconds */
	options->master_response_timeout = 60;

	/* default to 6 reconnection attempts at intervals of 10 seconds */
	options->reconnect_attempts = 6;
	options->reconnect_interval = 10;

	options->monitor_interval_secs = 2;
	options->retry_promote_interval_secs = 300;

	/* default to resyncing repl_nodes table every 30 seconds on the witness server */
	options->witness_repl_nodes_sync_interval_secs = 30;

	memset(options->event_notification_command, 0, sizeof(options->event_notification_command));
	options->event_notifications.head = NULL;
	options->event_notifications.tail = NULL;

	options->tablespace_mapping.head = NULL;
	options->tablespace_mapping.tail = NULL;

	/*
	 * If no configuration file available (user didn't specify and none found
	 * in the default locations), return with default values
	 */
	if (config_file_found == false)
	{
		log_verbose(LOG_NOTICE, _("no configuration file provided and no default file found - "
					 "continuing with default values\n"));
		return;
	}

	fp = fopen(config_file_path, "r");

	/*
	 * A configuration file has been found, either provided by the user
	 * or found in one of the default locations. If we can't open it,
	 * fail with an error.
	 */
	if (fp == NULL)
	{
		if (config_file_provided)
		{
			log_err(_("unable to open provided configuration file \"%s\"; terminating\n"), config_file_path);
		}
		else
		{
			log_err(_("unable to open default configuration file  \"%s\"; terminating\n"), config_file_path);
		}

		exit(ERR_BAD_CONFIG);
	}

	/* Read file */
	while ((s = fgets(buf, sizeof buf, fp)) != NULL)
	{
		bool known_parameter = true;

		/* Parse name/value pair from line */
		parse_line(buf, name, value);

		/* Skip blank lines */
		if (!strlen(name))
			continue;

		/* Skip comments */
		if (name[0] == '#')
			continue;

		/* Copy into correct entry in parameters struct */
		if (strcmp(name, "cluster") == 0)
			strncpy(options->cluster_name, value, MAXLEN);
		else if (strcmp(name, "node") == 0)
		{
			options->node = repmgr_atoi(value, "node", error_list, false);
			node_found = true;
		}
		else if (strcmp(name, "upstream_node") == 0)
			options->upstream_node = repmgr_atoi(value, "upstream_node", error_list, false);
		else if (strcmp(name, "conninfo") == 0)
			strncpy(options->conninfo, value, MAXLEN);
		else if (strcmp(name, "barman_server") == 0)
			strncpy(options->barman_server, value, MAXLEN);
		else if (strcmp(name, "barman_config") == 0)
			strncpy(options->barman_config, value, MAXLEN);
		else if (strcmp(name, "rsync_options") == 0)
			strncpy(options->rsync_options, value, QUERY_STR_LEN);
		else if (strcmp(name, "ssh_options") == 0)
			strncpy(options->ssh_options, value, QUERY_STR_LEN);
		else if (strcmp(name, "loglevel") == 0)
			strncpy(options->loglevel, value, MAXLEN);
		else if (strcmp(name, "logfacility") == 0)
			strncpy(options->logfacility, value, MAXLEN);
		else if (strcmp(name, "failover") == 0)
		{
			char		failoverstr[MAXLEN];

			strncpy(failoverstr, value, MAXLEN);

			if (strcmp(failoverstr, "manual") == 0)
			{
				options->failover = MANUAL_FAILOVER;
			}
			else if (strcmp(failoverstr, "automatic") == 0)
			{
				options->failover = AUTOMATIC_FAILOVER;
			}
			else
			{
				item_list_append(error_list, _("value for 'failover' must be 'automatic' or 'manual'\n"));
			}
		}
		else if (strcmp(name, "priority") == 0)
			options->priority = repmgr_atoi(value, "priority", error_list, true);
		else if (strcmp(name, "node_name") == 0)
			strncpy(options->node_name, value, MAXLEN);
		else if (strcmp(name, "promote_command") == 0)
			strncpy(options->promote_command, value, MAXLEN);
		else if (strcmp(name, "follow_command") == 0)
			strncpy(options->follow_command, value, MAXLEN);
		else if (strcmp(name, "service_stop_command") == 0)
			strncpy(options->service_stop_command, value, MAXLEN);
		else if (strcmp(name, "service_start_command") == 0)
			strncpy(options->service_start_command, value, MAXLEN);
		else if (strcmp(name, "service_restart_command") == 0)
			strncpy(options->service_restart_command, value, MAXLEN);
		else if (strcmp(name, "service_reload_command") == 0)
			strncpy(options->service_reload_command, value, MAXLEN);
		else if (strcmp(name, "service_promote_command") == 0)
			strncpy(options->service_promote_command, value, MAXLEN);
		else if (strcmp(name, "master_response_timeout") == 0)
			options->master_response_timeout = repmgr_atoi(value, "master_response_timeout", error_list, false);
		/*
		 * 'primary_response_timeout' as synonym for 'master_response_timeout' -
		 * we'll switch terminology in a future release (3.1?)
		 */
		else if (strcmp(name, "primary_response_timeout") == 0)
			options->master_response_timeout = repmgr_atoi(value, "primary_response_timeout", error_list, false);
		else if (strcmp(name, "reconnect_attempts") == 0)
			options->reconnect_attempts = repmgr_atoi(value, "reconnect_attempts", error_list, false);
		else if (strcmp(name, "reconnect_interval") == 0)
			options->reconnect_interval = repmgr_atoi(value, "reconnect_interval", error_list, false);
		else if (strcmp(name, "pg_bindir") == 0)
			strncpy(options->pg_bindir, value, MAXLEN);
		else if (strcmp(name, "pg_ctl_options") == 0)
			strncpy(options->pg_ctl_options, value, MAXLEN);
		else if (strcmp(name, "pg_basebackup_options") == 0)
			strncpy(options->pg_basebackup_options, value, MAXLEN);
		else if (strcmp(name, "logfile") == 0)
			strncpy(options->logfile, value, MAXLEN);
		else if (strcmp(name, "monitor_interval_secs") == 0)
			options->monitor_interval_secs = repmgr_atoi(value, "monitor_interval_secs", error_list, false);
		else if (strcmp(name, "retry_promote_interval_secs") == 0)
			options->retry_promote_interval_secs = repmgr_atoi(value, "retry_promote_interval_secs", error_list, false);
		else if (strcmp(name, "witness_repl_nodes_sync_interval_secs") == 0)
			options->witness_repl_nodes_sync_interval_secs = repmgr_atoi(value, "witness_repl_nodes_sync_interval_secs", error_list, false);
		else if (strcmp(name, "use_replication_slots") == 0)
			/* XXX we should have a dedicated boolean argument format */
			options->use_replication_slots = repmgr_atoi(value, "use_replication_slots", error_list, false);
		else if (strcmp(name, "event_notification_command") == 0)
			strncpy(options->event_notification_command, value, MAXLEN);
		else if (strcmp(name, "event_notifications") == 0)
			parse_event_notifications_list(options, value);
		else if (strcmp(name, "tablespace_mapping") == 0)
			tablespace_list_append(options, value);
		else if (strcmp(name, "restore_command") == 0)
			strncpy(options->restore_command, value, MAXLEN);
		else
		{
			known_parameter = false;
			log_warning(_("%s/%s: unknown name/value pair provided; ignoring\n"), name, value);
		}

		/*
		 * Raise an error if a known parameter is provided with an empty value.
		 * Currently there's no reason why empty parameters are needed; if
		 * we want to accept those, we'd need to add stricter default checking,
		 * as currently e.g. an empty `node` value will be converted to '0'.
		 */
		if (known_parameter == true && !strlen(value)) {
			char	   error_message_buf[MAXLEN] = "";
			snprintf(error_message_buf,
					 MAXLEN,
					 _("no value provided for parameter \"%s\""),
					 name);

			item_list_append(error_list, error_message_buf);
		}
	}

	fclose(fp);


	if (node_found == false)
	{
		item_list_append(error_list, _("\"node\": parameter was not found"));
	}
	else if (options->node == 0)
	{
		item_list_append(error_list, _("\"node\": must be greater than zero"));
	}
	else if (options->node < 0)
	{
		item_list_append(error_list, _("\"node\": must be a positive signed 32 bit integer, i.e. 2147483647 or less"));
	}

	if (strlen(options->conninfo))
	{

		/* Sanity check the provided conninfo string
		 *
		 * NOTE: PQconninfoParse() verifies the string format and checks for valid options
		 * but does not sanity check values
		 */
		conninfo_options = PQconninfoParse(options->conninfo, &conninfo_errmsg);
		if (conninfo_options == NULL)
		{
			char	   error_message_buf[MAXLEN] = "";
			snprintf(error_message_buf,
					 MAXLEN,
					 _("\"conninfo\": %s"),
					 conninfo_errmsg);

			item_list_append(error_list, error_message_buf);
		}

		PQconninfoFree(conninfo_options);
	}
}


char *
trim(char *s)
{
	/* Initialize start, end pointers */
	char	   *s1 = s,
			   *s2 = &s[strlen(s) - 1];

	/* If string is empty, no action needed */
	if (s2 < s1)
		return s;

	/* Trim and delimit right side */
	while ((isspace(*s2)) && (s2 >= s1))
		--s2;
	*(s2 + 1) = '\0';

	/* Trim left side */
	while ((isspace(*s1)) && (s1 < s2))
		++s1;

	/* Copy finished string */
	memmove(s, s1, s2 - s1);
	s[s2 - s1 + 1] = '\0';

	return s;
}

void
parse_line(char *buf, char *name, char *value)
{
	int			i = 0;
	int			j = 0;

	/*
	 * Extract parameter name, if present
	 */
	for (; i < MAXLEN; ++i)
	{

		if (buf[i] == '=')
			break;

		switch(buf[i])
		{
			/* Ignore whitespace */
			case ' ':
			case '\n':
			case '\r':
			case '\t':
				continue;
			default:
				name[j++] = buf[i];
		}
	}
	name[j] = '\0';

	/*
	 * Ignore any whitespace following the '=' sign
	 */
	for (; i < MAXLEN; ++i)
	{
		if (buf[i+1] == ' ')
			continue;
		if (buf[i+1] == '\t')
			continue;

		break;
	}

	/*
	 * Extract parameter value
	 */
	j = 0;
	for (++i; i < MAXLEN; ++i)
		if (buf[i] == '\'')
			continue;
		else if (buf[i] == '#')
			break;
		else if (buf[i] != '\n')
			value[j++] = buf[i];
		else
			break;
	value[j] = '\0';
	trim(value);
}


/*
 * reload_config()
 *
 * This is only called by repmgrd after receiving a SIGHUP or when a monitoring
 * loop is started up; it therefore only needs to reload options required
 * by repmgrd, which are as follows:
 *
 * changeable options:
 * - failover
 * - follow_command
 * - logfacility
 * - logfile
 * - loglevel
 * - master_response_timeout
 * - monitor_interval_secs
 * - priority
 * - promote_command
 * - reconnect_attempts
 * - reconnect_interval
 * - retry_promote_interval_secs
 * - witness_repl_nodes_sync_interval_secs
 *
 * non-changeable options:
 * - cluster_name
 * - conninfo
 * - node
 * - node_name
 *
 * extract with something like:
 *	 grep local_options\\. repmgrd.c | perl -n -e '/local_options\.([\w_]+)/ && print qq|$1\n|;' | sort | uniq

 */
bool
reload_config(t_configuration_options *orig_options)
{
	PGconn	   *conn;
	t_configuration_options new_options = T_CONFIGURATION_OPTIONS_INITIALIZER;
	bool	  config_changed = false;
	bool	  log_config_changed = false;

	static ItemList config_errors = { NULL, NULL };

	/*
	 * Re-read the configuration file: repmgr.conf
	 */
	log_info(_("reloading configuration file\n"));

	_parse_config(&new_options, &config_errors);

	if (config_errors.head != NULL)
	{
		/* XXX dump errors to log */
		log_warning(_("unable to parse new configuration, retaining current configuration\n"));
		return false;
	}

	/* The following options cannot be changed */
	if (strcmp(new_options.cluster_name, orig_options->cluster_name) != 0)
	{
		log_warning(_("cluster_name cannot be changed, retaining current configuration\n"));
		return false;
	}

	if (new_options.node != orig_options->node)
	{
		log_warning(_("node ID cannot be changed, retaining current configuration\n"));
		return false;
	}

	if (strcmp(new_options.node_name, orig_options->node_name) != 0)
	{
		log_warning(_("node_name cannot be changed, keeping current configuration\n"));
		return false;
	}

	if (strcmp(orig_options->conninfo, new_options.conninfo) != 0)
	{
		/* Test conninfo string works*/
		conn = establish_db_connection(new_options.conninfo, false);
		if (!conn || (PQstatus(conn) != CONNECTION_OK))
		{
			log_warning(_("'conninfo' string is not valid, retaining current configuration\n"));
			return false;
		}
		PQfinish(conn);
	}

	/*
	 * No configuration problems detected - copy any changed values
	 *
	 * NB: keep these in the same order as in config.h to make it easier
	 * to manage them
	 */

	/* failover */
	if (orig_options->failover != new_options.failover)
	{
		orig_options->failover = new_options.failover;
		config_changed = true;
	}

	/* follow_command */
	if (strcmp(orig_options->follow_command, new_options.follow_command) != 0)
	{
		strcpy(orig_options->follow_command, new_options.follow_command);
		config_changed = true;
	}

	/* master_response_timeout */
	if (orig_options->master_response_timeout != new_options.master_response_timeout)
	{
		orig_options->master_response_timeout = new_options.master_response_timeout;
		config_changed = true;
	}

	/* monitor_interval_secs */
	if (orig_options->monitor_interval_secs != new_options.monitor_interval_secs)
	{
		orig_options->monitor_interval_secs = new_options.monitor_interval_secs;
		config_changed = true;
	}

	/* priority */
	if (orig_options->priority != new_options.priority)
	{
		orig_options->priority = new_options.priority;
		config_changed = true;
	}

	/* promote_command */
	if (strcmp(orig_options->promote_command, new_options.promote_command) != 0)
	{
		strcpy(orig_options->promote_command, new_options.promote_command);
		config_changed = true;
	}

	/* reconnect_attempts */
	if (orig_options->reconnect_attempts != new_options.reconnect_attempts)
	{
		orig_options->reconnect_attempts = new_options.reconnect_attempts;
		config_changed = true;
	}

	/* reconnect_interval */
	if (orig_options->reconnect_interval != new_options.reconnect_interval)
	{
		orig_options->reconnect_interval = new_options.reconnect_interval;
		config_changed = true;
	}

	/* retry_promote_interval_secs */
	if (orig_options->retry_promote_interval_secs != new_options.retry_promote_interval_secs)
	{
		orig_options->retry_promote_interval_secs = new_options.retry_promote_interval_secs;
		config_changed = true;
	}


	/* witness_repl_nodes_sync_interval_secs */
	if (orig_options->witness_repl_nodes_sync_interval_secs != new_options.witness_repl_nodes_sync_interval_secs)
	{
		orig_options->witness_repl_nodes_sync_interval_secs = new_options.witness_repl_nodes_sync_interval_secs;
		config_changed = true;
	}

	/*
	 * Handle changes to logging configuration
	 */
	if (strcmp(orig_options->logfacility, new_options.logfacility) != 0)
	{
		strcpy(orig_options->logfacility, new_options.logfacility);
		log_config_changed = true;
	}

	if (strcmp(orig_options->logfile, new_options.logfile) != 0)
	{
		strcpy(orig_options->logfile, new_options.logfile);
		log_config_changed = true;
	}


	if (strcmp(orig_options->loglevel, new_options.loglevel) != 0)
	{
		strcpy(orig_options->loglevel, new_options.loglevel);
		log_config_changed = true;
	}

	if (log_config_changed == true)
	{
		log_notice(_("restarting logging with changed parameters\n"));
		logger_shutdown();
		logger_init(orig_options, progname());
	}

	if (config_changed == true)
	{
		log_notice(_("configuration file reloaded with changed parameters\n"));
	}
	/*
	 * if logging configuration changed, don't say the configuration didn't
	 * change, as it clearly has.
	 */
	else if (log_config_changed == false)
	{
		log_info(_("configuration has not changed\n"));
	}

	return config_changed;
}


void
item_list_append(ItemList *item_list, char *error_message)
{
	ItemListCell *cell;

	cell = (ItemListCell *) pg_malloc0(sizeof(ItemListCell));

	if (cell == NULL)
	{
		log_err(_("unable to allocate memory; terminating.\n"));
		exit(ERR_BAD_CONFIG);
	}

	cell->string = pg_malloc0(MAXLEN);
	strncpy(cell->string, error_message, MAXLEN);

	if (item_list->tail)
	{
		item_list->tail->next = cell;
	}
	else
	{
		item_list->head = cell;
	}

	item_list->tail = cell;
}


/*
 * Convert provided string to an integer using strtol;
 * on error, if a callback is provided, pass the error message to that,
 * otherwise exit
 */
int
repmgr_atoi(const char *value, const char *config_item, ItemList *error_list, bool allow_negative)
{
	char	  *endptr;
	long	   longval = 0;
	char	   error_message_buf[MAXLEN] = "";

	/* It's possible that some versions of strtol() don't treat an empty
	 * string as an error.
	 */

	if (*value == '\0')
	{
		snprintf(error_message_buf,
				 MAXLEN,
				 _("no value provided for \"%s\""),
				 config_item);
	}
	else
	{
		errno = 0;
		longval = strtol(value, &endptr, 10);

		if (value == endptr || errno)
		{
			snprintf(error_message_buf,
					 MAXLEN,
					 _("\"%s\": invalid value (provided: \"%s\")"),
					 config_item, value);
		}
	}

	/* Disallow negative values for most parameters */
	if (allow_negative == false && longval < 0)
	{
		snprintf(error_message_buf,
				 MAXLEN,
				 _("\"%s\" must be zero or greater (provided: %s)"),
				 config_item, value);
	}

	/* Error message buffer is set */
	if (error_message_buf[0] != '\0')
	{
		if (error_list == NULL)
		{
			log_err("%s\n", error_message_buf);
			exit(ERR_BAD_CONFIG);
		}

		item_list_append(error_list, error_message_buf);
	}

	return (int32) longval;
}


/*
 * Split argument into old_dir and new_dir and append to tablespace mapping
 * list.
 *
 * Adapted from pg_basebackup.c
 */
static void
tablespace_list_append(t_configuration_options *options, const char *arg)
{
	TablespaceListCell *cell;
	char	   *dst;
	char	   *dst_ptr;
	const char *arg_ptr;

	cell = (TablespaceListCell *) pg_malloc0(sizeof(TablespaceListCell));
	if (cell == NULL)
	{
		log_err(_("unable to allocate memory; terminating\n"));
		exit(ERR_BAD_CONFIG);
	}

	dst_ptr = dst = cell->old_dir;
	for (arg_ptr = arg; *arg_ptr; arg_ptr++)
	{
		if (dst_ptr - dst >= MAXPGPATH)
		{
			log_err(_("directory name too long\n"));
			exit(ERR_BAD_CONFIG);
		}

		if (*arg_ptr == '\\' && *(arg_ptr + 1) == '=')
			;					/* skip backslash escaping = */
		else if (*arg_ptr == '=' && (arg_ptr == arg || *(arg_ptr - 1) != '\\'))
		{
			if (*cell->new_dir)
			{
				log_err(_("multiple \"=\" signs in tablespace mapping\n"));
				exit(ERR_BAD_CONFIG);
			}
			else
			{
				dst = dst_ptr = cell->new_dir;
			}
		}
		else
			*dst_ptr++ = *arg_ptr;
	}

	if (!*cell->old_dir || !*cell->new_dir)
	{
		log_err(_("invalid tablespace mapping format \"%s\", must be \"OLDDIR=NEWDIR\"\n"),
				arg);
		exit(ERR_BAD_CONFIG);
	}

	canonicalize_path(cell->old_dir);
	canonicalize_path(cell->new_dir);

	if (options->tablespace_mapping.tail)
		options->tablespace_mapping.tail->next = cell;
	else
		options->tablespace_mapping.head = cell;

	options->tablespace_mapping.tail = cell;
}

/*
 * parse_event_notifications_list()
 *
 *
 */

static void
parse_event_notifications_list(t_configuration_options *options, const char *arg)
{
	const char *arg_ptr;
	char		event_type_buf[MAXLEN] = "";
	char	   *dst_ptr = event_type_buf;


	for (arg_ptr = arg; arg_ptr <= (arg + strlen(arg)); arg_ptr++)
	{
		/* ignore whitespace */
		if (*arg_ptr == ' ' || *arg_ptr == '\t')
		{
			continue;
		}

		/*
		 * comma (or end-of-string) should mark the end of an event type -
		 * just as long as there was something preceding it
		 */
		if ((*arg_ptr == ',' || *arg_ptr == '\0') && event_type_buf[0] != '\0')
		{
			EventNotificationListCell *cell;

			cell = (EventNotificationListCell *) pg_malloc0(sizeof(EventNotificationListCell));

			if (cell == NULL)
			{
				log_err(_("unable to allocate memory; terminating\n"));
				exit(ERR_BAD_CONFIG);
			}

			strncpy(cell->event_type, event_type_buf, MAXLEN);

			if (options->event_notifications.tail)
			{
				options->event_notifications.tail->next = cell;
			}
			else
			{
				options->event_notifications.head = cell;
			}

			options->event_notifications.tail = cell;

			memset(event_type_buf, 0, MAXLEN);
			dst_ptr = event_type_buf;
		}
		/* ignore duplicated commas */
		else if (*arg_ptr == ',')
		{
			continue;
		}
		else
		{
			*dst_ptr++ = *arg_ptr;
		}
	}
}



static void
exit_with_errors(ItemList *config_errors)
{
	ItemListCell *cell;

	log_err(_("%s: following errors were found in the configuration file.\n"), progname());

	for (cell = config_errors->head; cell; cell = cell->next)
	{
		log_err("%s\n", cell->string);
	}

	exit(ERR_BAD_CONFIG);
}

