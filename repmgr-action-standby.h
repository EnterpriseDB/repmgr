/*
 * repmgr-action-standby.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#ifndef _REPMGR_ACTION_STANDBY_H_
#define _REPMGR_ACTION_STANDBY_H_

extern void do_standby_clone(void);
extern void do_standby_register(void);
extern void do_standby_unregister(void);

typedef struct
{
	char filepath[MAXPGPATH];
	char filename[MAXPGPATH];
	bool in_data_directory;
} t_configfile_info;


typedef struct
{
	int    size;
	int    entries;
	t_configfile_info **files;
} t_configfile_list;

#define T_CONFIGFILE_LIST_INITIALIZER { 0, 0, NULL }


#endif
