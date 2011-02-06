/*
 * repmgr.h
 * Copyright (c) 2ndQuadrant, 2010
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

#include "dbutils.h"

#define PRIMARY_MODE		0
#define STANDBY_MODE		1

#define MAXLEN			80
#define MAXVERSIONSTR		16
#define QUERY_STR_LEN		8192

#include "config.h"
#define MAXFILENAME		1024
#define MAXLINELENGTH	4096
#define ERRBUFF_SIZE		512

#define DEFAULT_CONFIG_FILE		"./repmgr.conf"
#define DEFAULT_WAL_KEEP_SEGMENTS	"5000"
#define DEFAULT_DEST_DIR		"."
#define DEFAULT_MASTER_PORT		"5432"
#define DEFAULT_DBNAME			"postgres"
#define DEFAULT_REPMGR_SCHEMA_PREFIX	"repmgr_"

/* Exit return code */

#define ERR_BAD_CONFIG 1
#define ERR_BAD_RSYNC 2
#define ERR_STOP_BACKUP 3
#define ERR_NO_RESTART 4

/* Run time options type */
typedef struct {

	char dbname[MAXLEN];
	char host[MAXLEN];
	char username[MAXLEN];
	char dest_dir[MAXFILENAME];
	char config_file[MAXFILENAME];
	char remote_user[MAXLEN];
	char wal_keep_segments[MAXLEN];
	bool verbose;
	bool force;

	char masterport[MAXLEN];

} t_runtime_options;

#endif
