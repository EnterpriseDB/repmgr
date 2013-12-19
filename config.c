/*
 * config.c - Functions to parse the config file
 * Copyright (C) 2ndQuadrant, 2010-2012
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

#include "config.h"
#include "log.h"
#include "strutil.h"
#include "repmgr.h"

void
parse_config(const char *config_file, t_configuration_options *options)
{
	char *s, buff[MAXLINELENGTH];
	char name[MAXLEN];
	char value[MAXLEN];

	FILE *fp = fopen (config_file, "r");

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

	/* if nothing has been provided defaults to 60 */
	options->master_response_timeout = 60;

	/* it defaults to 6 retries with a time between retries of 10s */
	options->reconnect_attempts = 6;
	options->reconnect_intvl = 10;

	/*
	 * Since some commands don't require a config file at all, not
	 * having one isn't necessarily a problem.
	 */
	if (fp == NULL)
	{
		log_err(_("Did not find the configuration file '%s', continuing\n"), config_file);
		return;
	}

	/* Read next line */
	while ((s = fgets (buff, sizeof buff, fp)) != NULL)
	{
		/* Skip blank lines and comments */
		if (buff[0] == '\n' || buff[0] == '#')
			continue;

		/* Parse name/value pair from line */
		parse_line(buff, name, value);

		/* Copy into correct entry in parameters struct */
		if (strcmp(name, "cluster") == 0)
			strncpy (options->cluster_name, value, MAXLEN);
		else if (strcmp(name, "node") == 0)
			options->node = atoi(value);
		else if (strcmp(name, "conninfo") == 0)
			strncpy (options->conninfo, value, MAXLEN);
		else if (strcmp(name, "rsync_options") == 0)
			strncpy (options->rsync_options, value, QUERY_STR_LEN);
		else if (strcmp(name, "ssh_options") == 0)
			strncpy (options->ssh_options, value, QUERY_STR_LEN);
		else if (strcmp(name, "loglevel") == 0)
			strncpy (options->loglevel, value, MAXLEN);
		else if (strcmp(name, "logfacility") == 0)
			strncpy (options->logfacility, value, MAXLEN);
		else if (strcmp(name, "failover") == 0)
		{
			char failoverstr[MAXLEN];
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
		else
			log_warning(_("%s/%s: Unknown name/value pair!\n"), name, value);
	}

	/* Close file */
	fclose (fp);

	/* Check config settings */
	if (strnlen(options->cluster_name, MAXLEN)==0)
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
}


char *
trim (char *s)
{
	/* Initialize start, end pointers */
	char *s1 = s, *s2 = &s[strlen (s) - 1];

	/* Trim and delimit right side */
	while ( (isspace (*s2)) && (s2 >= s1) )
		--s2;
	*(s2+1) = '\0';

	/* Trim left side */
	while ( (isspace (*s1)) && (s1 < s2) )
		++s1;

	/* Copy finished string */
	strcpy (s, s1);
	return s;
}

void
parse_line(char *buff, char *name, char *value)
{
	int i = 0;
	int j = 0;

	/*
	 * first we find the name of the parameter
	 */
	for ( ; i < MAXLEN; ++i)
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
	for ( ++i ; i < MAXLEN; ++i)
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
reload_configuration(char *config_file, t_configuration_options *orig_options)
{
	PGconn	*conn;

	t_configuration_options new_options;

	/*
	 * Re-read the configuration file: repmgr.conf
	 */
	log_info(_("Reloading configuration file and updating repmgr tables\n"));
	parse_config(config_file, &new_options);
	if (new_options.node == -1)
	{
		log_warning(_("\nCannot load new configuration, will keep current one.\n"));
		return false;
	}

	if (strcmp(new_options.cluster_name, orig_options->cluster_name) != 0)
	{
		log_warning(_("\nCannot change cluster name, will keep current configuration.\n"));
		return false;
	}

	if (new_options.node != orig_options->node)
	{
		log_warning(_("\nCannot change node number, will keep current configuration.\n"));
		return false;
	}

	if (new_options.node_name != orig_options->node_name)
	{
		log_warning(_("\nCannot change standby name, will keep current configuration.\n"));
		return false;
	}

	if (new_options.failover != MANUAL_FAILOVER && new_options.failover != AUTOMATIC_FAILOVER)
	{
		log_warning(_("\nNew value for failover is not valid. Should be MANUAL or AUTOMATIC.\n"));
		return false;
	}

	if (new_options.master_response_timeout <= 0)
	{
		log_warning(_("\nNew value for master_response_timeout is not valid. Should be greater than zero.\n"));
		return false;
	}

	if (new_options.reconnect_attempts < 0)
	{
		log_warning(_("\nNew value for reconnect_attempts is not valid. Should be greater or equal than zero.\n"));
		return false;
	}

	if (new_options.reconnect_intvl < 0)
	{
		log_warning(_("\nNew value for reconnect_interval is not valid. Should be greater or equal than zero.\n"));
		return false;
	}

	/* Test conninfo string */
	conn = establishDBConnection(new_options.conninfo, false);
	if (!conn || (PQstatus(conn) != CONNECTION_OK))
	{
		log_warning(_("\nconninfo string is not valid, will keep current configuration.\n"));
		return false;
	}
	PQfinish(conn);

	/* Configuration seems ok, will load new values */
	strcpy(orig_options->cluster_name, new_options.cluster_name);
	orig_options->node = new_options.node;
	strcpy(orig_options->conninfo, new_options.conninfo);
	orig_options->failover = new_options.failover;
	orig_options->priority = new_options.priority;
	strcpy(orig_options->node_name, new_options.node_name);
	strcpy(orig_options->promote_command, new_options.promote_command);
	strcpy(orig_options->follow_command, new_options.follow_command);
	strcpy(orig_options->rsync_options, new_options.rsync_options);
	strcpy(orig_options->ssh_options, new_options.ssh_options);
	orig_options->master_response_timeout = new_options.master_response_timeout;
	orig_options->reconnect_attempts = new_options.reconnect_attempts;
	orig_options->reconnect_intvl = new_options.reconnect_intvl;
	/*
	 * XXX These ones can change with a simple SIGHUP?

		strcpy (orig_options->loglevel, new_options.loglevel);
		strcpy (orig_options->logfacility, new_options.logfacility);

		logger_shutdown();
		XXX do we have progname here ?
		logger_init(progname, orig_options.loglevel, orig_options.logfacility);
	*/

	return true;
}
