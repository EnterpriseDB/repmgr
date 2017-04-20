/*
 * config.c - parse repmgr.conf and other configuration-related functionality
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include <sys/stat.h>			/* for stat() */

#include "repmgr.h"
#include "config.h"

const static char *_progname = NULL;
static void	_parse_config(t_configuration_options *options, ItemList *error_list);
static void	exit_with_errors(ItemList *config_errors);


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

bool
load_config(const char *config_file, bool verbose, t_configuration_options *options, char *argv0)
{
    return true;
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

static void
_parse_config(t_configuration_options *options, ItemList *error_list)
{

}


bool
reload_config(t_configuration_options *orig_options)
{
    return true;
}


static void
exit_with_errors(ItemList *config_errors)
{
}

void
item_list_append(ItemList *item_list, char *error_message)
{
	ItemListCell *cell;

	cell = (ItemListCell *) pg_malloc0(sizeof(ItemListCell));

	if (cell == NULL)
	{
		//log_err(_("unable to allocate memory; terminating.\n"));
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
