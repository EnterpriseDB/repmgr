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

#include "strutil.h"
#include "dbutils.h"
#include "errcode.h"
#include "config.h"
#include "dirmod.h"

#define MIN_SUPPORTED_VERSION		"9.3"
#define MIN_SUPPORTED_VERSION_NUM	90300

#define ERRBUFF_SIZE	512

#define DEFAULT_WAL_KEEP_SEGMENTS	"5000"
#define DEFAULT_DEST_DIR		"."
#define DEFAULT_REPMGR_SCHEMA_PREFIX	"repmgr_"
#define DEFAULT_PRIORITY		100
#define FAILOVER_NODES_MAX_CHECK 50

#define MANUAL_FAILOVER		0
#define AUTOMATIC_FAILOVER	1
#define NODE_NOT_FOUND      -1
#define NO_UPSTREAM_NODE	-1
#define UNKNOWN_NODE_ID     -1



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
	bool		ignore_external_config_files;
	bool		csv_mode;
	bool		without_barman;
	char		pg_ctl_mode[MAXLEN];
	char		masterport[MAXLEN];
	/*
	 * configuration file parameters which can be overridden on the
	 * command line
	 */
	char		loglevel[MAXLEN];

	/* parameter used by STANDBY SWITCHOVER */
	char		remote_config_file[MAXLEN];
	char		pg_rewind[MAXPGPATH];
	/* parameter used by STANDBY {ARCHIVE_CONFIG | RESTORE_CONFIG} */
	char		config_archive_dir[MAXLEN];
	/* parameter used by CLUSTER CLEANUP */
	int			keep_history;

	char		pg_bindir[MAXLEN];

	char		recovery_min_apply_delay[MAXLEN];

	/* deprecated command line options */
	char		localport[MAXLEN];
	bool		initdb_no_pwprompt;
}	t_runtime_options;

#define T_RUNTIME_OPTIONS_INITIALIZER { "", "", "", "", "", "", "", DEFAULT_WAL_KEEP_SEGMENTS, false, false, false, false, false, false, false, false, false, false, false, "smart", "", "", "", "", "", 0, "", "", "", false }

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

extern char		repmgr_schema[MAXLEN];
extern bool		config_file_found;

#endif
