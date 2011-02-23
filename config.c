/*
 * config.c - Functions to parse the config file
 * Copyright (C) 2ndQuadrant, 2010-2011
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
#include "repmgr.h"
#include "strutil.h"

void
parse_config(const char* config_file, t_configuration_options* options)
{
	char *s, buff[MAXLINELENGTH];
	char name[MAXLEN];
	char value[MAXLEN];

	FILE *fp = fopen (config_file, "r");

	/*
	 * Since some commands don't require a config file at all, not
	 * having one isn't necessarily a problem.
	 */
	if (fp == NULL)
	{
		fprintf(stderr, _("Did not find the configuration file '%s', continuing\n"), config_file);
		return;
	}

	/* Initialize */
	memset(options->cluster_name, 0, sizeof(options->cluster_name));
	options->node = -1;
	memset(options->conninfo, 0, sizeof(options->conninfo));
	memset(options->rsync_options, 0, sizeof(options->rsync_options));

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
		else if (strcmp(name, "loglevel") == 0)
			strncpy (options->loglevel, value, MAXLEN);
		else if (strcmp(name, "logfacility") == 0)
			strncpy (options->logfacility, value, MAXLEN);
		else
			printf ("WARNING: %s/%s: Unknown name/value pair!\n", name, value);
	}

	/* Close file */
	fclose (fp);

	/* Check config settings */
	if (strnlen(options->cluster_name, MAXLEN)==0)
	{
		fprintf(stderr, "Cluster name is missing. "
		        "Check the configuration file.\n");
		exit(ERR_BAD_CONFIG);
	}

	if (options->node == -1)
	{
		fprintf(stderr, "Node information is missing. "
		        "Check the configuration file.\n");
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
