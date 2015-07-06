/*
 * config.c - Functions to parse the config file
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

#include <sys/stat.h>			/* for stat() */

#include "config.h"
#include "log.h"
#include "strutil.h"
#include "repmgr.h"

static void parse_event_notifications_list(t_configuration_options *options, const char *arg);
static void tablespace_list_append(t_configuration_options *options, const char *arg);


/*
 * parse_config()
 *
 * Set default options and overwrite with values from provided configuration
 * file.
 *
 * Returns true if a configuration file could be parsed, otherwise false.
 *
 * Any configuration options changed in this function must also be changed in
 * reload_config()
 */
bool
parse_config(const char *config_file, t_configuration_options *options)
{
	char	   *s,
				buff[MAXLINELENGTH];
	char		config_file_buf[MAXLEN];
	char		name[MAXLEN];
	char		value[MAXLEN];
	bool 		config_file_provided = false;
	FILE	   *fp;

	/* Sanity checks */

	/*
	 * If a configuration file was provided, check it exists, otherwise
	 * emit an error
	 */
	if (config_file[0])
	{
		struct stat config;

		strncpy(config_file_buf, config_file, MAXLEN);
		canonicalize_path(config_file_buf);

		if(stat(config_file_buf, &config) != 0)
		{
			log_err(_("provided configuration file '%s' not found: %s\n"),
					config_file,
					strerror(errno)
				);
			exit(ERR_BAD_CONFIG);
		}

		config_file_provided = true;
	}

	/*
	 * If no configuration file was provided, set to a default file
	 * which `parse_config()` will attempt to read if it exists
	 */
	else
	{
		strncpy(config_file_buf, DEFAULT_CONFIG_FILE, MAXLEN);
	}


	fp = fopen(config_file_buf, "r");

	/*
	 * Since some commands don't require a config file at all, not having one
	 * isn't necessarily a problem.
	 *
	 * If the user explictly provided a configuration file and we can't
	 * read it we'll raise an error.
	 *
	 * If no configuration file was provided, we'll try and read the default\
	 * file if it exists and is readable, but won't worry if it's not.
	 */
	if (fp == NULL)
	{
		if(config_file_provided)
		{
			log_err(_("unable to open provided configuration file '%s'; terminating\n"), config_file_buf);
			exit(ERR_BAD_CONFIG);
		}

		log_notice(_("no configuration file provided and default file '%s' not found - "
					 "continuing with default values\n"),
				   DEFAULT_CONFIG_FILE);
		return false;
	}

	/* Initialize configuration options with sensible defaults */
	memset(options->cluster_name, 0, sizeof(options->cluster_name));
	options->node = -1;
	options->upstream_node = NO_UPSTREAM_NODE;
	memset(options->conninfo, 0, sizeof(options->conninfo));
	options->failover = MANUAL_FAILOVER;
	options->priority = DEFAULT_PRIORITY;
	memset(options->node_name, 0, sizeof(options->node_name));
	memset(options->promote_command, 0, sizeof(options->promote_command));
	memset(options->follow_command, 0, sizeof(options->follow_command));
	memset(options->rsync_options, 0, sizeof(options->rsync_options));
	memset(options->ssh_options, 0, sizeof(options->ssh_options));
	memset(options->pg_bindir, 0, sizeof(options->pg_bindir));
	memset(options->pg_ctl_options, 0, sizeof(options->pg_ctl_options));
	memset(options->pg_basebackup_options, 0, sizeof(options->pg_basebackup_options));

	/* default master_response_timeout is 60 seconds */
	options->master_response_timeout = 60;

	/* default to 6 reconnection attempts at intervals of 10 seconds */
	options->reconnect_attempts = 6;
	options->reconnect_intvl = 10;

	options->monitor_interval_secs = 2;
	options->retry_promote_interval_secs = 300;

	memset(options->event_notification_command, 0, sizeof(options->event_notification_command));

	options->tablespace_mapping.head = NULL;
	options->tablespace_mapping.tail = NULL;



	/* Read next line */
	while ((s = fgets(buff, sizeof buff, fp)) != NULL)
	{
		bool known_parameter = true;

		/* Parse name/value pair from line */
		parse_line(buff, name, value);

		/* Skip blank lines */
		if(!strlen(name))
			continue;

		/* Skip comments */
		if (name[0] == '#')
			continue;

		/* Copy into correct entry in parameters struct */
		if (strcmp(name, "cluster") == 0)
			strncpy(options->cluster_name, value, MAXLEN);
		else if (strcmp(name, "node") == 0)
			options->node = atoi(value);
		else if (strcmp(name, "upstream_node") == 0)
			options->upstream_node = atoi(value);
		else if (strcmp(name, "conninfo") == 0)
			strncpy(options->conninfo, value, MAXLEN);
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
				log_err(_("value for 'failover' must be 'automatic' or 'manual'\n"));
				exit(ERR_BAD_CONFIG);
			}
		}
		else if (strcmp(name, "priority") == 0)
			options->priority = atoi(value);
		else if (strcmp(name, "node_name") == 0)
			strncpy(options->node_name, value, MAXLEN);
		else if (strcmp(name, "promote_command") == 0)
			strncpy(options->promote_command, value, MAXLEN);
		else if (strcmp(name, "follow_command") == 0)
			strncpy(options->follow_command, value, MAXLEN);
		else if (strcmp(name, "master_response_timeout") == 0)
			options->master_response_timeout = atoi(value);
		else if (strcmp(name, "reconnect_attempts") == 0)
			options->reconnect_attempts = atoi(value);
		else if (strcmp(name, "reconnect_interval") == 0)
			options->reconnect_intvl = atoi(value);
		else if (strcmp(name, "pg_bindir") == 0)
			strncpy(options->pg_bindir, value, MAXLEN);
		else if (strcmp(name, "pg_ctl_options") == 0)
			strncpy(options->pg_ctl_options, value, MAXLEN);
		else if (strcmp(name, "pg_basebackup_options") == 0)
			strncpy(options->pg_basebackup_options, value, MAXLEN);
		else if (strcmp(name, "logfile") == 0)
			strncpy(options->logfile, value, MAXLEN);
		else if (strcmp(name, "monitor_interval_secs") == 0)
			options->monitor_interval_secs = atoi(value);
		else if (strcmp(name, "retry_promote_interval_secs") == 0)
			options->retry_promote_interval_secs = atoi(value);
		else if (strcmp(name, "use_replication_slots") == 0)
			options->use_replication_slots = atoi(value);
		else if (strcmp(name, "event_notification_command") == 0)
			strncpy(options->event_notification_command, value, MAXLEN);
		else if (strcmp(name, "event_notifications") == 0)
			parse_event_notifications_list(options, value);
		else if (strcmp(name, "tablespace_mapping") == 0)
			tablespace_list_append(options, value);
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
		if(known_parameter == true && !strlen(value)) {
			log_err(_("no value provided for parameter '%s'\n"), name);
			exit(ERR_BAD_CONFIG);
		}
	}

	fclose(fp);

	/* Check config settings */

	/* The following checks are for the presence of the parameter */
	if (*options->cluster_name == '\0')
	{
		log_err(_("required parameter 'cluster' was not found\n"));
		exit(ERR_BAD_CONFIG);
	}

	if (options->node == -1)
	{
		log_err(_("required parameter 'node' was not found\n"));
		exit(ERR_BAD_CONFIG);
	}

	if (options->node == 0)
	{
		log_err(_("'node' must be an integer > 0\n"));
		exit(ERR_BAD_CONFIG);
	}

	if (*options->node_name == '\0')
	{
		log_err(_("required parameter 'node_name' was not found\n"));
		exit(ERR_BAD_CONFIG);
	}

	if (*options->conninfo == '\0')
	{
		log_err(_("required parameter 'conninfo' was not found\n"));
		exit(ERR_BAD_CONFIG);
	}

	/* The following checks are for valid parameter values */
	if (options->master_response_timeout <= 0)
	{
		log_err(_("'master_response_timeout' must be greater than zero\n"));
		exit(ERR_BAD_CONFIG);
	}

	if (options->reconnect_attempts < 0)
	{
		log_err(_("'reconnect_attempts' must be zero or greater\n"));
		exit(ERR_BAD_CONFIG);
	}

	if (options->reconnect_intvl < 0)
	{
		log_err(_("'reconnect_interval' must be zero or greater\n"));
		exit(ERR_BAD_CONFIG);
	}

	return true;
}


char *
trim(char *s)
{
	/* Initialize start, end pointers */
	char	   *s1 = s,
			   *s2 = &s[strlen(s) - 1];

	/* If string is empty, no action needed */
	if(s2 < s1)
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
parse_line(char *buff, char *name, char *value)
{
	int			i = 0;
	int			j = 0;

	/*
	 * Extract parameter name, if present
	 */
	for (; i < MAXLEN; ++i)
	{

		if (buff[i] == '=')
			break;

		switch(buff[i])
		{
			/* Ignore whitespace */
			case ' ':
			case '\n':
			case '\r':
			case '\t':
				continue;
			default:
				name[j++] = buff[i];
		}
	}
	name[j] = '\0';

	/*
	 * Ignore any whitespace following the '=' sign
	 */
	for (; i < MAXLEN; ++i)
	{
		if (buff[i+1] == ' ')
			continue;
		if (buff[i+1] == '\t')
			continue;

		break;
	}

	/*
	 * Extract parameter value
	 */
	j = 0;
	for (++i; i < MAXLEN; ++i)
		if (buff[i] == '\'')
			continue;
		else if (buff[i] != '\n')
			value[j++] = buff[i];
		else
			break;
	value[j] = '\0';
	trim(value);
}

bool
reload_config(char *config_file, t_configuration_options * orig_options)
{
	PGconn	   *conn;
	t_configuration_options new_options;
	bool	  config_changed = false;

	/*
	 * Re-read the configuration file: repmgr.conf
	 */
	log_info(_("reloading configuration file and updating repmgr tables\n"));

	parse_config(config_file, &new_options);
	if (new_options.node == -1)
	{
		log_warning(_("unable to parse new configuration, retaining current configuration\n"));
		return false;
	}

	if (strcmp(new_options.cluster_name, orig_options->cluster_name) != 0)
	{
		log_warning(_("unable to change cluster name, retaining current configuration\n"));
		return false;
	}

	if (new_options.node != orig_options->node)
	{
		log_warning(_("unable to change node ID, retaining current configuration\n"));
		return false;
	}

	if (strcmp(new_options.node_name, orig_options->node_name) != 0)
	{
		log_warning(_("unable to change standby name, keeping current configuration\n"));
		return false;
	}

	if (new_options.failover != MANUAL_FAILOVER && new_options.failover != AUTOMATIC_FAILOVER)
	{
		log_warning(_("new value for 'failover' must be 'automatic' or 'manual'\n"));
		return false;
	}

	if (new_options.master_response_timeout <= 0)
	{
		log_warning(_("new value for 'master_response_timeout' must be greater than zero\n"));
		return false;
	}

	if (new_options.reconnect_attempts < 0)
	{
		log_warning(_("new value for 'reconnect_attempts' must be zero or greater\n"));
		return false;
	}

	if (new_options.reconnect_intvl < 0)
	{
		log_warning(_("new value for 'reconnect_interval' must be zero or greater\n"));
		return false;
	}

	if(strcmp(orig_options->conninfo, new_options.conninfo) != 0)
	{
		/* Test conninfo string */
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

	/* cluster_name */
	if(strcmp(orig_options->cluster_name, new_options.cluster_name) != 0)
	{
		strcpy(orig_options->cluster_name, new_options.cluster_name);
		config_changed = true;
	}

	/* conninfo */
	if(strcmp(orig_options->conninfo, new_options.conninfo) != 0)
	{
		strcpy(orig_options->conninfo, new_options.conninfo);
		config_changed = true;
	}

	/* node */
	if(orig_options->node != new_options.node)
	{
		orig_options->node = new_options.node;
		config_changed = true;
	}

	/* failover */
	if(orig_options->failover != new_options.failover)
	{
		orig_options->failover = new_options.failover;
		config_changed = true;
	}

	/* priority */
	if(orig_options->priority != new_options.priority)
	{
		orig_options->priority = new_options.priority;
		config_changed = true;
	}

	/* node_name */
	if(strcmp(orig_options->node_name, new_options.node_name) != 0)
	{
		strcpy(orig_options->node_name, new_options.node_name);
		config_changed = true;
	}

	/* promote_command */
	if(strcmp(orig_options->promote_command, new_options.promote_command) != 0)
	{
		strcpy(orig_options->promote_command, new_options.promote_command);
		config_changed = true;
	}

	/* follow_command */
	if(strcmp(orig_options->follow_command, new_options.follow_command) != 0)
	{
		strcpy(orig_options->follow_command, new_options.follow_command);
		config_changed = true;
	}

	/*
	 * XXX These ones can change with a simple SIGHUP?
	 *
	 * strcpy (orig_options->loglevel, new_options.loglevel); strcpy
	 * (orig_options->logfacility, new_options.logfacility);
	 *
	 * logger_shutdown(); XXX do we have progname here ? logger_init(progname,
	 * orig_options.loglevel, orig_options.logfacility);
	 */

	/* rsync_options */
	if(strcmp(orig_options->rsync_options, new_options.rsync_options) != 0)
	{
		strcpy(orig_options->rsync_options, new_options.rsync_options);
		config_changed = true;
	}

	/* ssh_options */
	if(strcmp(orig_options->ssh_options, new_options.ssh_options) != 0)
	{
		strcpy(orig_options->ssh_options, new_options.ssh_options);
		config_changed = true;
	}

	/* master_response_timeout */
	if(orig_options->master_response_timeout != new_options.master_response_timeout)
	{
		orig_options->master_response_timeout = new_options.master_response_timeout;
		config_changed = true;
	}

	/* reconnect_attempts */
	if(orig_options->reconnect_attempts != new_options.reconnect_attempts)
	{
		orig_options->reconnect_attempts = new_options.reconnect_attempts;
		config_changed = true;
	}

	/* reconnect_intvl */
	if(orig_options->reconnect_intvl != new_options.reconnect_intvl)
	{
		orig_options->reconnect_intvl = new_options.reconnect_intvl;
		config_changed = true;
	}

	/* pg_ctl_options */
	if(strcmp(orig_options->pg_ctl_options, new_options.pg_ctl_options) != 0)
	{
		strcpy(orig_options->pg_ctl_options, new_options.pg_ctl_options);
		config_changed = true;
	}

	/* pg_basebackup_options */
	if(strcmp(orig_options->pg_basebackup_options, new_options.pg_basebackup_options) != 0)
	{
		strcpy(orig_options->pg_basebackup_options, new_options.pg_basebackup_options);
		config_changed = true;
	}

	/* monitor_interval_secs */
	if(orig_options->monitor_interval_secs != new_options.monitor_interval_secs)
	{
		orig_options->monitor_interval_secs = new_options.monitor_interval_secs;
		config_changed = true;
	}

	/* retry_promote_interval_secs */
	if(orig_options->retry_promote_interval_secs != new_options.retry_promote_interval_secs)
	{
		orig_options->retry_promote_interval_secs = new_options.retry_promote_interval_secs;
		config_changed = true;
	}

	/* use_replication_slots */
	if(orig_options->use_replication_slots != new_options.use_replication_slots)
	{
		orig_options->use_replication_slots = new_options.use_replication_slots;
		config_changed = true;
	}

	if(config_changed == true)
	{
		log_debug(_("reload_config(): configuration has changed\n"));
	}
	else
	{
		log_debug(_("reload_config(): configuration has not changed\n"));
	}

	return config_changed;
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
	if(cell == NULL)
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
	char	    event_type_buf[MAXLEN] = "";
	char	   *dst_ptr = event_type_buf;


	for (arg_ptr = arg; arg_ptr <= (arg + strlen(arg)); arg_ptr++)
	{
		/* ignore whitespace */
		if(*arg_ptr == ' ' || *arg_ptr == '\t')
		{
			continue;
		}

		/*
		 * comma (or end-of-string) should mark the end of an event type -
		 * just as long as there was something preceding it
		 */
		if((*arg_ptr == ',' || *arg_ptr == '\0') && event_type_buf[0] != '\0')
		{
			EventNotificationListCell *cell;

			cell = (EventNotificationListCell *) pg_malloc0(sizeof(EventNotificationListCell));

			if(cell == NULL)
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
		else if(*arg_ptr == ',')
		{
			continue;
		}
		else
		{
			*dst_ptr++ = *arg_ptr;
		}
	}
}
