/*
 * config.c - parse repmgr.conf and other configuration-related functionality
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include <sys/stat.h>			/* for stat() */

#include "repmgr.h"
#include "config.h"

const static char *_progname = NULL;

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
