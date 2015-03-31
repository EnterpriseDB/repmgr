/*
 * repmgr.h
 * Copyright (c) 2ndQuadrant, 2010-2015
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
#include "libpq-fe.h"


#include "getopt_long.h"

#include "strutil.h"
#include "dbutils.h"
#include "errcode.h"
#include "config.h"

#define MIN_SUPPORTED_VERSION		"9.3"
#define MIN_SUPPORTED_VERSION_NUM	90300

#include "config.h"
#define MAXFILENAME		1024
#define ERRBUFF_SIZE	512

#define DEFAULT_CONFIG_FILE		"./repmgr.conf"
#define DEFAULT_WAL_KEEP_SEGMENTS	"5000"
#define DEFAULT_DEST_DIR		"."
#define DEFAULT_MASTER_PORT		"5432"
#define DEFAULT_DBNAME			"postgres"
#define DEFAULT_REPMGR_SCHEMA_PREFIX	"repmgr_"
#define DEFAULT_PRIORITY		100
#define FAILOVER_NODES_MAX_CHECK 50

#define MANUAL_FAILOVER		0
#define AUTOMATIC_FAILOVER	1
#define NODE_NOT_FOUND      -1
#define NO_UPSTREAM_NODE	-1


typedef enum {
    UNKNOWN = 0,
	MASTER,
	STANDBY,
	WITNESS
} t_server_type;



/* Run time options type */
typedef struct
{

	char		dbname[MAXLEN];
	char		host[MAXLEN];
	char		username[MAXLEN];
	char		dest_dir[MAXFILENAME];
	char		config_file[MAXFILENAME];
	char		remote_user[MAXLEN];
	char		superuser[MAXLEN];
	char		wal_keep_segments[MAXLEN];
	bool		verbose;
	bool		force;
	bool		wait_for_master;
	bool		ignore_rsync_warn;
	bool		initdb_no_pwprompt;
	bool		rsync_only;
	bool		fast_checkpoint;
	bool		ignore_external_config_files;
	char		masterport[MAXLEN];
	char		localport[MAXLEN];

	/* parameter used by CLUSTER CLEANUP */
	int			keep_history;

	char		pg_bindir[MAXLEN];

	char		recovery_min_apply_delay[MAXLEN];
}	t_runtime_options;

#define T_RUNTIME_OPTIONS_INITIALIZER { "", "", "", "", "", "", "", DEFAULT_WAL_KEEP_SEGMENTS, false, false, false, false, false, false, false, false, "", "", 0, "", "" }

extern char		repmgr_schema[MAXLEN];

typedef struct ErrorListCell
{
	struct ErrorListCell *next;
	char			     *error_message;
} ErrorListCell;

typedef struct ErrorList
{
	ErrorListCell *head;
	ErrorListCell *tail;
} ErrorList;

#endif
