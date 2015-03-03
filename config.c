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
    bool 		config_file_provided = true;
	FILE	   *fp;

	/* Sanity checks */

	/*
	 * If a configuration file was provided, check it exists, otherwise
	 * emit an error
	 */
	if (config_file[0])
	{
		struct stat config;
		if(stat(config_file, &config) != 0)
		{
			log_err(_("Provided configuration file '%s' not found: %s\n"),
					config_file,
					strerror(errno)
				);
			exit(ERR_BAD_CONFIG);
		}
		strncpy(config_file_buf, config_file, MAXLEN);
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
			log_err(_("Unable to open provided configuration file '%s' - terminating\n"), config_file_buf);
			exit(ERR_BAD_CONFIG);
		}

		log_notice(_("No configuration file provided and default file '%s' not found - "
					 "continuing with default values\n"),
				   DEFAULT_CONFIG_FILE);
		return false;
	}

	/* Initialize */
	memset(options->cluster_name, 0, sizeof(options->cluster_name));
	options->node = -1;
	memset(options->conninfo, 0, sizeof(options->conninfo));
	options->failover = MANUAL_FAILOVER;
	options->priority = 0;
	memset(options->node_name, 0, sizeof(options->node_name));
	memset(options->promote_command, 0, sizeof(options->promote_command));
	memset(options->follow_command, 0, sizeof(options->follow_command));
	memset(options->rsync_options, 0, sizeof(options->rsync_options));
	memset(options->ssh_options, 0, sizeof(options->ssh_options));
	memset(options->pg_bindir, 0, sizeof(options->pg_bindir));
	memset(options->pgctl_options, 0, sizeof(options->pgctl_options));
	memset(options->pg_basebackup_options, 0, sizeof(options->pg_basebackup_options));

	/* if nothing has been provided defaults to 60 */
	options->master_response_timeout = 60;

	/* it defaults to 6 retries with a time between retries of 10s */
	options->reconnect_attempts = 6;
	options->reconnect_intvl = 10;

	options->monitor_interval_secs = 2;
	options->retry_promote_interval_secs = 300;

	options->tablespace_mapping.head = NULL;
	options->tablespace_mapping.tail = NULL;



	/* Read next line */
	while ((s = fgets(buff, sizeof buff, fp)) != NULL)
	{
		/* Skip blank lines and comments */
		if (buff[0] == '\n' || buff[0] == '#')
			continue;

		/* Parse name/value pair from line */
		parse_line(buff, name, value);

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
				options->failover = MANUAL_FAILOVER;
			else if (strcmp(failoverstr, "automatic") == 0)
				options->failover = AUTOMATIC_FAILOVER;
			else
			{
				log_warning(_("value for failover option is incorrect, it should be automatic or manual. Defaulting to manual.\n"));
				options->failover = MANUAL_FAILOVER;
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
			strncpy(options->pgctl_options, value, MAXLEN);
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
		else if (strcmp(name, "tablespace_mapping") == 0)
			tablespace_list_append(options, value);
		else
			log_warning(_("%s/%s: Unknown name/value pair!\n"), name, value);
	}

	/* Close file */
	fclose(fp);

	/* Check config settings */
	if (*options->cluster_name == '\0')
	{
		log_err(_("Cluster name is missing. Check the configuration file.\n"));
		exit(ERR_BAD_CONFIG);
	}

	if (options->node == -1)
	{
		log_err(_("Node information is missing. Check the configuration file.\n"));
		exit(ERR_BAD_CONFIG);
	}

	if (options->master_response_timeout <= 0)
	{
		log_err(_("Master response timeout must be greater than zero. Check the configuration file.\n"));
		exit(ERR_BAD_CONFIG);
	}

	if (options->reconnect_attempts < 0)
	{
		log_err(_("Reconnect attempts must be zero or greater. Check the configuration file.\n"));
		exit(ERR_BAD_CONFIG);
	}

	if (options->reconnect_intvl <= 0)
	{
		log_err(_("Reconnect intervals must be zero or greater. Check the configuration file.\n"));
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
	 * first we find the name of the parameter
	 */
	for (; i < MAXLEN; ++i)
	{
		if (buff[i] != '=')
			name[j++] = buff[i];
		else
			break;
	}
	name[j] = '\0';

	/*
	 * Now the value
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
	log_info(_("Reloading configuration file and updating repmgr tables\n"));

	parse_config(config_file, &new_options);
	if (new_options.node == -1)
	{
		log_warning(_("Cannot load new configuration, will keep current one.\n"));
		return false;
	}

	if (strcmp(new_options.cluster_name, orig_options->cluster_name) != 0)
	{
		log_warning(_("Cannot change cluster name, will keep current configuration.\n"));
		return false;
	}

	if (new_options.node != orig_options->node)
	{
		log_warning(_("Cannot change node number, will keep current configuration.\n"));
		return false;
	}

	if (strcmp(new_options.node_name, orig_options->node_name) != 0)
	{
		log_warning(_("Cannot change standby name, will keep current configuration.\n"));
		return false;
	}

	if (new_options.failover != MANUAL_FAILOVER && new_options.failover != AUTOMATIC_FAILOVER)
	{
		log_warning(_("New value for failover is not valid. Should be MANUAL or AUTOMATIC.\n"));
		return false;
	}

	if (new_options.master_response_timeout <= 0)
	{
		log_warning(_("New value for master_response_timeout is not valid. Should be greater than zero.\n"));
		return false;
	}

	if (new_options.reconnect_attempts < 0)
	{
		log_warning(_("New value for reconnect_attempts is not valid. Should be greater or equal than zero.\n"));
		return false;
	}

	if (new_options.reconnect_intvl < 0)
	{
		log_warning(_("New value for reconnect_interval is not valid. Should be greater or equal than zero.\n"));
		return false;
	}

	if(strcmp(orig_options->conninfo, new_options.conninfo) != 0)
	{
		/* Test conninfo string */
		conn = establish_db_connection(new_options.conninfo, false);
		if (!conn || (PQstatus(conn) != CONNECTION_OK))
		{
			log_warning(_("conninfo string is not valid, will keep current configuration.\n"));
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

	/* pgctl_options */
	if(strcmp(orig_options->pgctl_options, new_options.pgctl_options) != 0)
	{
		strcpy(orig_options->pgctl_options, new_options.pgctl_options);
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
		log_debug(_("reload_config(): configuration has not changed"));
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

	cell = (TablespaceListCell *) malloc(sizeof(TablespaceListCell));
	if(cell == NULL)
	{
		log_err(_("Unable to allocate memory. Terminating.\n"));
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
