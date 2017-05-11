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

extern DBState
get_db_state(char *data_directory);

extern const char *
describe_db_state(DBState state);

#endif /* _CONTROLDATA_H_ */
