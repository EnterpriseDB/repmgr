/*
 * config.c
 * Copyright (c) 2ndQuadrant, 2010
 *
 * Functions to parse the config file
 */

#include "repmgr.h"

void
parse_config(char *cluster_name, int *node, char *conninfo)
{
	char *s, buff[256];
	FILE *fp = fopen (CONFIG_FILE, "r");

	if (fp == NULL)
    	return;

	/* Read next line */
	while ((s = fgets (buff, sizeof buff, fp)) != NULL)
	{
		char name[MAXLEN];
		char value[MAXLEN];

    	/* Skip blank lines and comments */
    	if (buff[0] == '\n' || buff[0] == '#')
    		continue;

    	/* Parse name/value pair from line */
		parse_line(buff, name, value);

    	/* Copy into correct entry in parameters struct */
    	if (strcmp(name, "cluster") == 0)
    		strncpy (cluster_name, value, MAXLEN);
    	else if (strcmp(name, "node") == 0)
    		*node = atoi(value);
    	else if (strcmp(name, "conninfo") == 0)
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
    s2--;
  *(s2+1) = '\0';

  /* Trim left side */
  while ( (isspace (*s1)) && (s1 < s2) )
    s1++;

  /* Copy finished string */
  strcpy (s, s1);
  return s;
}

void
parse_line(char *buff, char *name, char *value)
{
	int i;
	int j;

	/*
	 * first we find the name of the parameter
	 */
	j = 0;
	for (i = 0; i < MAXLEN; i++)
	{
		if (buff[i] != '=')
			name[j++] = buff[i];
		else
			break;
	}
	name[j] = '\0';

	i++;
	/*
	 * Now the value
	 */ 
	j = 0;
	for ( ; i < MAXLEN; i++)
		if (buff[i] == '\'')
			continue;
		else if (buff[i] != '\n')
			value[j++] = buff[i];
		else
			break;
	value[j] = '\0';
    trim(value);
}
