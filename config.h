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

void		item_list_append(ItemList *item_list, char *error_message);



#endif
