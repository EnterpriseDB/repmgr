/*
 * repmgr-client-global.h
 * Copyright (c) 2ndQuadrant, 2010-2017
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
	bool		wal_keep_segments_used;

	/* general configuration options */
	char		config_file[MAXPGPATH];
	bool		dry_run;
	bool		force;
	char		pg_bindir[MAXLEN]; /* overrides setting in repmgr.conf */
	bool		wait;

	/* logging options */
	char		log_level[MAXLEN];  /* overrides setting in repmgr.conf */
	bool		log_to_file;
	bool		terse;
	bool		verbose;

	/* output options */
	bool		csv;

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

	/* "standby clone" options */
	bool		copy_external_config_files;
	int			copy_external_config_files_destination;
	bool		fast_checkpoint;
	bool		rsync_only;
	bool		no_upstream_connection;
	char		recovery_min_apply_delay[MAXLEN];
	char		replication_user[MAXLEN];
	char		upstream_conninfo[MAXLEN];
	int			upstream_node_id;
	bool		use_recovery_conninfo_password;
	char		wal_keep_segments[MAXLEN];
	bool		without_barman;

	/* "standby register" options */
	bool		wait_register_sync;
	int			wait_register_sync_seconds;

	/* "node service" options */
	char		action[MAXLEN];
	bool		check;
	bool		list;

	/* "cluster event" options */
	bool		all;
	char		event[MAXLEN];
	int			limit;

	/* following options for internal use */
	char		config_archive_dir[MAXPGPATH];
}	t_runtime_options;

#define T_RUNTIME_OPTIONS_INITIALIZER { \
		/* configuration metadata */ \
		false, false, false, false,	false, 	\
		/* general configuration options */	\
		"", false, false, "", false,	\
		/* logging options */ \
		"", false, false, false, \
		/* output options */ \
		false, \
		/* database connection options */ \
		"", "", "",	"",				  \
		/* other connection options */ \
		"",	"",				  \
		/* node options */ \
		UNKNOWN_NODE_ID, "", "", \
		/* "standby clone" options */ \
		false, CONFIG_FILE_SAMEPATH, false, false, false, "", "", "", NO_UPSTREAM_NODE, false, "", false, \
		/* "standby register" options */ \
		false, 0, \
		/* "node service" options */ \
		"", false, false, \
		/* "cluster event" options */ \
		false, "", CLUSTER_EVENT_LIMIT,	\
		"/tmp" \
}


typedef enum {
	barman,
	pg_basebackup
}	standy_clone_mode;

typedef enum {
	ACTION_UNKNOWN = -1,
	ACTION_NONE,
	ACTION_START,
	ACTION_STOP,
	ACTION_RESTART,
	ACTION_RELOAD,
	ACTION_PROMOTE
} t_server_action;



/* global configuration structures */
extern t_runtime_options runtime_options;
extern t_configuration_options config_file_options;

t_conninfo_param_list source_conninfo;


extern bool	 config_file_required;
extern char	 pg_bindir[MAXLEN];

extern char	 repmgr_slot_name[MAXLEN];
extern char *repmgr_slot_name_ptr;

extern t_node_info target_node_info;


extern int check_server_version(PGconn *conn, char *server_type, bool exit_on_error, char *server_version_string);
extern bool create_repmgr_extension(PGconn *conn);
extern int test_ssh_connection(char *host, char *remote_user);
extern bool local_command(const char *command, PQExpBufferData *outputbuf);
extern bool check_upstream_config(PGconn *conn, int server_version_num, bool exit_on_error);
extern standy_clone_mode get_standby_clone_mode(void);

extern int  copy_remote_files(char *host, char *remote_user, char *remote_path,
							  char *local_path, bool is_directory, int server_version_num);

extern void print_error_list(ItemList *error_list, int log_level);

extern char *make_pg_path(const char *file);

extern bool create_recovery_file(const char *data_dir, t_conninfo_param_list *recovery_conninfo);

extern void get_superuser_connection(PGconn **conn, PGconn **superuser_conn, PGconn **privileged_conn);

extern bool remote_command(const char *host, const char *user, const char *command, PQExpBufferData *outputbuf);

/* server control functions */
extern void get_server_action(t_server_action action, char *script, char *data_dir);
extern bool data_dir_required_for_action(t_server_action action);
extern void get_node_data_directory(char *data_dir_buf);
#endif
