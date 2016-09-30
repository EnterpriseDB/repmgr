/*
 * repmgr.h
 * Copyright (c) 2ndQuadrant, 2010-2016
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

#include <libpq-fe.h>
#include <postgres_fe.h>
#include <getopt_long.h>
#include "pqexpbuffer.h"

#include "strutil.h"
#include "dbutils.h"
#include "errcode.h"
#include "config.h"
#include "dirmod.h"

#define MIN_SUPPORTED_VERSION		"9.3"
#define MIN_SUPPORTED_VERSION_NUM	90300

#define ERRBUFF_SIZE	512

#define DEFAULT_WAL_KEEP_SEGMENTS	"0"
#define DEFAULT_DEST_DIR		"."
#define DEFAULT_REPMGR_SCHEMA_PREFIX	"repmgr_"
#define DEFAULT_PRIORITY		100
#define FAILOVER_NODES_MAX_CHECK 50

#define MANUAL_FAILOVER		0
#define AUTOMATIC_FAILOVER	1
#define NODE_NOT_FOUND      -1
#define NO_UPSTREAM_NODE	-1
#define UNKNOWN_NODE_ID     -1

/* command line options without short versions */
#define OPT_HELP                         1
#define OPT_CHECK_UPSTREAM_CONFIG        2
#define OPT_RECOVERY_MIN_APPLY_DELAY     3
#define OPT_COPY_EXTERNAL_CONFIG_FILES   4
#define OPT_CONFIG_ARCHIVE_DIR           5
#define OPT_PG_REWIND                    6
#define OPT_PWPROMPT                     7
#define OPT_CSV                          8
#define OPT_NODE                         9
#define OPT_WITHOUT_BARMAN               10
#define OPT_NO_UPSTREAM_CONNECTION       11
#define OPT_REGISTER_WAIT                12
#define OPT_CLUSTER                      13

/* deprecated command line options */
#define OPT_INITDB_NO_PWPROMPT           999
#define OPT_IGNORE_EXTERNAL_CONFIG_FILES 998

#define CONFIG_FILE_SAMEPATH 1
#define CONFIG_FILE_PGDATA 2


/* Run time options type */
typedef struct
{
	char		dbname[MAXLEN];
	char		host[MAXLEN];
	char		username[MAXLEN];
	char		dest_dir[MAXPGPATH];
	char		config_file[MAXPGPATH];
	char		remote_user[MAXLEN];
	char		superuser[MAXLEN];
	char		wal_keep_segments[MAXLEN];
	bool		verbose;
	bool		terse;
	bool		force;
	bool		wait_for_master;
	bool		ignore_rsync_warn;
	bool		witness_pwprompt;
	bool		rsync_only;
	bool		fast_checkpoint;
	bool		csv_mode;
	bool		without_barman;
	bool		no_upstream_connection;
	bool		copy_external_config_files;
	int			copy_external_config_files_destination;
	bool		wait_register_sync;
	int			wait_register_sync_seconds;
	char		masterport[MAXLEN];
	/*
	 * configuration file parameters which can be overridden on the
	 * command line
	 */
	char		loglevel[MAXLEN];

	/* parameter used by STANDBY SWITCHOVER */
	char		remote_config_file[MAXLEN];
	char		pg_rewind[MAXPGPATH];
	char		pg_ctl_mode[MAXLEN];
	/* parameter used by STANDBY {ARCHIVE_CONFIG | RESTORE_CONFIG} */
	char		config_archive_dir[MAXLEN];
	/* parameter used by CLUSTER CLEANUP */
	int			keep_history;
	/* parameter used by {STANDBY|WITNESS} UNREGISTER */
	int			node;

	char		pg_bindir[MAXLEN];

	char		recovery_min_apply_delay[MAXLEN];
}	t_runtime_options;

#define T_RUNTIME_OPTIONS_INITIALIZER { "", "", "", "", "", "", "", DEFAULT_WAL_KEEP_SEGMENTS, false, false, false, false, false, false, false, false, false, false, false, false, CONFIG_FILE_SAMEPATH, false, 0, "", "", "", "", "fast", "", 0, UNKNOWN_NODE_ID, "", ""}

struct BackupLabel
{
	XLogRecPtr start_wal_location;
	char start_wal_file[MAXLEN];
	XLogRecPtr checkpoint_location;
	char backup_from[MAXLEN];
	char backup_method[MAXLEN];
	char start_time[MAXLEN];
	char label[MAXLEN];
	XLogRecPtr min_failover_slot_lsn;
};


typedef struct
{
	char		slot[MAXLEN];
	char		xlog_method[MAXLEN];
} t_basebackup_options;

#define T_BASEBACKUP_OPTIONS_INITIALIZER { "", "" }

typedef struct
{
	int    size;
	char **keywords;
	char **values;
} t_conninfo_param_list;

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


typedef struct
{
	int node_id;
	int node_status;
} t_node_status_rec;

typedef struct
{
	int node_id;
	char node_name[MAXLEN];
	t_node_status_rec **node_status_list;
} t_node_matrix_rec;

typedef struct
{
	int node_id;
	char node_name[MAXLEN];
	t_node_matrix_rec **matrix_list_rec;
} t_node_status_cube;



#endif
