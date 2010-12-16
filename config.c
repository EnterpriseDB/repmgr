/*
 * config.c
 * Copyright (c) 2ndQuadrant, 2010
 *
 * Functions to parse the config file
 */

#include "repmgr.h"

void
parse_config(const char *config_file, char *cluster_name, int *node, char *conninfo)
{
	char *s, buff[MAXLEN];
	char name[MAXLEN];
	char value[MAXLEN];

	FILE *fp = fopen (config_file, "r");

	if (fp == NULL)
		return;

	/* Read next line */
	while ((s = fgets (buff, sizeof buff, fp)) != NULL)
	{
		/* Skip blank lines and comments */
		if (buff[0] == '\n' || buff[0] == '#')
			continue;

		/* Parse name/value pair from line */
		parse_line(buff, name, value);

		/* Copy into correct entry in parameters struct */
		if (strncmp(name, "cluster", 7) == 0)
			strncpy (cluster_name, value, MAXLEN);
		else if (strncmp(name, "node", 4) == 0)
			*node = atoi(value);
		else if (strncmp(name, "conninfo", 8) == 0)
			strncpy (conninfo, value, MAXLEN);
		else
			printf ("WARNING: %s/%s: Unknown name/value pair!\n", name, value);
	}


  	/* Close file */
  	fclose (fp);
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
	for ( ++i ; i < MAXLEN; i++)
		if (buff[i] == '\'')
			continue;
		else if (buff[i] != '\n')
			value[j++] = buff[i];
		else
			break;
	value[j] = '\0';
	trim(value);
}
