/*
 * repmgr-client-global.h
 * Copyright (c) 2ndQuadrant, 2010-2019
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
 */

#ifndef _REPMGR_CLIENT_GLOBAL_H_
#define _REPMGR_CLIENT_GLOBAL_H_

#include "configfile.h"

/* values for --copy-external-config-files */
#define CONFIG_FILE_SAMEPATH 1
#define CONFIG_FILE_PGDATA 2

/* default value for "cluster event --limit"*/
#define CLUSTER_EVENT_LIMIT 20

typedef struct
{
	/* configuration metadata */
	bool		conninfo_provided;
	bool		connection_param_provided;
	bool		host_param_provided;
	bool		limit_provided;
	bool		wait_provided;

	/* general configuration options */
	char		config_file[MAXPGPATH];
	bool		dry_run;
	bool		force;
	char		pg_bindir[MAXLEN];	/* overrides setting in repmgr.conf */
	int			wait;
	bool		no_wait;
	bool		compact;

	/* logging options */
	char		log_level[MAXLEN];	/* overrides setting in repmgr.conf */
	bool		log_to_file;
	bool		quiet;
	bool		terse;
	bool		verbose;

	/* output options */
	bool		csv;
	bool		nagios;
	bool		optformat;

	/* standard connection options */
	char		dbname[MAXLEN];
	char		host[MAXLEN];
	char		username[MAXLEN];
	char		port[MAXLEN];

	/* other connection options */
	char		remote_user[MAXLEN];
	char		superuser[MAXLEN];

	/* general node options */
	int			node_id;
	char		node_name[MAXLEN];
	char		data_dir[MAXPGPATH];
	int			remote_node_id;

	/* "standby clone" options */
	bool		copy_external_config_files;
	int			copy_external_config_files_destination;
	bool		fast_checkpoint;
	bool		rsync_only;
	bool		no_upstream_connection;
	char		recovery_min_apply_delay[MAXLEN];
	char		replication_user[MAXLEN];
	char		upstream_conninfo[MAXLEN];
	bool		without_barman;
	bool		recovery_conf_only;

	/* "standby clone"/"standby follow" options */
	int			upstream_node_id;

	/* "standby register" options */
	bool		wait_register_sync;
	int			wait_register_sync_seconds;
	int			wait_start;

	/* "standby switchover" options */
	bool		always_promote;
	bool		force_rewind_used;
	char		force_rewind_path[MAXPGPATH];
	bool		siblings_follow;
	bool		repmgrd_no_pause;

	/* "node status" options */
	bool		is_shutdown_cleanly;

	/* "node check" options */
	bool		archive_ready;
	bool		downstream;
	bool		replication_lag;
	bool		role;
	bool		slots;
	bool		missing_slots;
	bool		has_passfile;
	bool		replication_connection;
	bool		data_directory_config;

	/* "node rejoin" options */
	char		config_files[MAXLEN];

	/* "node service" options */
	char		action[MAXLEN];
	bool		check;
	bool		list_actions;
	bool		checkpoint;

	/* "cluster event" options */
	bool		all;
	char		event[MAXLEN];
	int			limit;

	/* "cluster cleanup" options */
	int			keep_history;

	/* following options for internal use */
	char		config_archive_dir[MAXPGPATH];
	OutputMode	output_mode;
	bool		disable_wal_receiver;
	bool		enable_wal_receiver;
} t_runtime_options;

#define T_RUNTIME_OPTIONS_INITIALIZER { \
		/* configuration metadata */ \
		false, false, false, false, false,	\
		/* general configuration options */	\
		"", false, false, "", -1, false, false, \
		/* logging options */ \
		"", false, false, false, false,	\
		/* output options */ \
		false, false, false,  \
		/* database connection options */ \
		"", "", "",	"", \
		/* other connection options */ \
		"",	"", \
		/* general node options */ \
		UNKNOWN_NODE_ID, "", "", UNKNOWN_NODE_ID, \
		/* "standby clone" options */ \
		false, CONFIG_FILE_SAMEPATH, false, false, false, "", "", "", \
		false, false, \
		/* "standby clone"/"standby follow" options */ \
		NO_UPSTREAM_NODE, \
		/* "standby register" options */ \
		false, -1, DEFAULT_WAIT_START,   \
		/* "standby switchover" options */ \
		false, false, "", false, false,	\
		/* "node status" options */ \
		false, \
		/* "node check" options */ \
		false, false, false, false, false, false, false, false,	false, \
		/* "node rejoin" options */ \
		"", \
		/* "node service" options */ \
		"", false, false, false,  \
		/* "cluster event" options */ \
		false, "", CLUSTER_EVENT_LIMIT,	\
		/* "cluster cleanup" options */ \
		0, \
		/* following options for internal use */ \
		"/tmp", OM_TEXT, false, false \
}


typedef enum
{
	barman,
	pg_basebackup
} standy_clone_mode;

typedef enum
{
	ACTION_UNKNOWN = -1,
	ACTION_NONE,
	ACTION_START,
	ACTION_STOP,
	ACTION_STOP_WAIT,
	ACTION_RESTART,
	ACTION_RELOAD,
	ACTION_PROMOTE
} t_server_action;


typedef struct ColHeader
{
	char		title[MAXLEN];
	int			max_length;
	int			cur_length;
	bool		display;
} ColHeader;



/* global configuration structures */
extern t_runtime_options runtime_options;
extern t_configuration_options config_file_options;

t_conninfo_param_list source_conninfo;


extern bool config_file_required;
extern char pg_bindir[MAXLEN];

extern t_node_info target_node_info;


extern int	check_server_version(PGconn *conn, char *server_type, bool exit_on_error, char *server_version_string);
extern void check_93_config(void);
extern bool create_repmgr_extension(PGconn *conn);
extern int	test_ssh_connection(char *host, char *remote_user);

extern standy_clone_mode get_standby_clone_mode(void);

extern int copy_remote_files(char *host, char *remote_user, char *remote_path,
				  char *local_path, bool is_directory, int server_version_num);

extern void print_error_list(ItemList *error_list, int log_level);

extern char *make_pg_path(const char *file);

extern void get_superuser_connection(PGconn **conn, PGconn **superuser_conn, PGconn **privileged_conn);

extern void make_remote_repmgr_path(PQExpBufferData *outputbuf, t_node_info *remote_node_record);
extern void make_repmgrd_path(PQExpBufferData *output_buf);


/* display functions */
extern void print_help_header(void);
extern void print_status_header(int cols, ColHeader *headers);

/* server control functions */
extern void get_server_action(t_server_action action, char *script, char *data_dir);
extern bool data_dir_required_for_action(t_server_action action);
extern void get_node_config_directory(char *config_dir_buf);
extern void get_node_data_directory(char *data_dir_buf);
extern void init_node_record(t_node_info *node_record);
extern bool can_use_pg_rewind(PGconn *conn, const char *data_directory, PQExpBufferData *reason);
extern void drop_replication_slot_if_exists(PGconn *conn, int node_id, char *slot_name);

extern bool check_node_can_attach(TimeLineID local_tli, XLogRecPtr local_xlogpos, PGconn *follow_target_conn, t_node_info *follow_target_node_record, bool is_rejoin);
extern void check_shared_library(PGconn *conn);
extern bool is_repmgrd_running(PGconn *conn);

#endif							/* _REPMGR_CLIENT_GLOBAL_H_ */
