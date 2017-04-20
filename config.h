/*
 * config.h
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 *
 */

#ifndef _REPMGR_CONFIG_H_
#define _REPMGR_CONFIG_H_

typedef struct
{
    int			node_id;
    char		node_name[MAXLEN];
	char		loglevel[MAXLEN];
	char		logfacility[MAXLEN];
	char		logfile[MAXLEN];

}	t_configuration_options;

/*
 * The following will initialize the structure with a minimal set of options;
 * actual defaults are set in parse_config() before parsing the configuration file
 */

#define T_CONFIGURATION_OPTIONS_INITIALIZER { \
		/* node settings */ \
		UNKNOWN_NODE_ID, "", \
		/* log settings */ \
        "", "", ""}

typedef struct ItemListCell
{
	struct ItemListCell *next;
	char			    *string;
} ItemListCell;

typedef struct ItemList
{
	ItemListCell *head;
	ItemListCell *tail;
} ItemList;


void		set_progname(const char *argv0);
const char *progname(void);

bool		load_config(const char *config_file, bool verbose, t_configuration_options *options, char *argv0);
bool		parse_config(t_configuration_options *options);
bool		reload_config(t_configuration_options *orig_options);

void		item_list_append(ItemList *item_list, char *error_message);

#endif
