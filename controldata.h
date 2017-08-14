/*
 * controldata.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */

#ifndef _CONTROLDATA_H_
#define _CONTROLDATA_H_

#include "postgres_fe.h"
#include "catalog/pg_control.h"

typedef struct
{
	bool control_file_processed;
	ControlFileData *control_file;
} ControlFileInfo;

#define T_CONTROLFILEINFO_INITIALIZER { false, NULL }

extern DBState get_db_state(const char *data_directory);
extern const char * describe_db_state(DBState state);
extern int get_data_checksum_version(const char *data_directory);
extern uint64 get_system_identifier(const char *data_directory);
extern XLogRecPtr get_latest_checkpoint_location(const char *data_directory);

#endif /* _CONTROLDATA_H_ */
