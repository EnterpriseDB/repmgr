/*
 * repmgr.h
 * Copyright (c) 2ndQuadrant, 2010-2011
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

#ifndef _REPMGR_H_
#define _REPMGR_H_

#include "postgres_fe.h"
#include "getopt_long.h"
#include "libpq-fe.h"

#include "strutil.h"
#include "dbutils.h"
#include "errcode.h"

#define PRIMARY_MODE		0
#define STANDBY_MODE		1
#define WITNESS_MODE		2

#include "config.h"
#define MAXFILENAME		1024
#define ERRBUFF_SIZE		512

#define DEFAULT_CONFIG_FILE		"./repmgr.conf"
#define DEFAULT_WAL_KEEP_SEGMENTS	"5000"
#define DEFAULT_DEST_DIR		"."
#define DEFAULT_MASTER_PORT		"5432"
#define DEFAULT_DBNAME			"postgres"
#define DEFAULT_REPMGR_SCHEMA_PREFIX	"repmgr_"

#define MANUAL_FAILOVER		0
#define AUTOMATIC_FAILOVER	1

/* Run time options type */
typedef struct
{

	char dbname[MAXLEN];
	char host[MAXLEN];
	char username[MAXLEN];
	char dest_dir[MAXFILENAME];
	char config_file[MAXFILENAME];
	char remote_user[MAXLEN];
	char wal_keep_segments[MAXLEN];
	bool verbose;
	bool force;
	bool ignore_rsync_warn;

	char masterport[MAXLEN];
	char localport[MAXLEN];

} t_runtime_options;

#define SLEEP_MONITOR		2
#define SLEEP_RETRY			3
#define NUM_RETRY			40



#endif
