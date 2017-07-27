/*
 * configfile.h
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 *
 */

#ifndef _REPMGR_CONFIGFILE_H_
#define _REPMGR_CONFIGFILE_H_

#include <getopt_long.h>

#define CONFIG_FILE_NAME	"repmgr.conf"
#define MAXLINELENGTH		4096
extern bool		config_file_found;

typedef enum {
	FAILOVER_MANUAL,
	FAILOVER_AUTOMATIC
}	failover_mode_opt;

typedef struct EventNotificationListCell
{
	struct EventNotificationListCell *next;
	char event_type[MAXLEN];
} EventNotificationListCell;

typedef struct EventNotificationList
{
	EventNotificationListCell *head;
	EventNotificationListCell *tail;
} EventNotificationList;



typedef struct TablespaceListCell
{
	struct TablespaceListCell *next;
	char		old_dir[MAXPGPATH];
	char		new_dir[MAXPGPATH];
} TablespaceListCell;

typedef struct TablespaceList
{
	TablespaceListCell *head;
	TablespaceListCell *tail;
} TablespaceList;


typedef struct
{
	/* node information */
    int			node_id;
	char		node_name[MAXLEN];
	char		conninfo[MAXLEN];
	char		replication_user[NAMEDATALEN];
	char		pg_bindir[MAXLEN];
	int			replication_type;

	/* log settings */
	char		log_level[MAXLEN];
	char		log_facility[MAXLEN];
	char		log_file[MAXLEN];
	int			log_status_interval;

	/* standby clone settings */
	bool		use_replication_slots;
	char		rsync_options[MAXLEN];
	char		ssh_options[MAXLEN];
	char		pg_basebackup_options[MAXLEN];
	char		restore_command[MAXLEN];
	TablespaceList tablespace_mapping;

	/* repmgrd settings */
	failover_mode_opt		failover_mode;
	char		location[MAXLEN];
	int			priority;
	char		promote_command[MAXLEN];
	char		follow_command[MAXLEN];
	int			monitor_interval_secs;
	int			reconnect_attempts;
	int			reconnect_interval;
	bool		monitoring_history;
	int			degraded_monitoring_timeout;
	int			async_query_timeout;
	int			primary_notification_timeout;
	int			primary_follow_timeout;

	/* BDR settings */
	bool		bdr_local_monitoring_only;
	bool		bdr_recovery_timeout;

	/* service settings */
	char		pg_ctl_options[MAXLEN];
	char		service_stop_command[MAXLEN];
	char		service_start_command[MAXLEN];
	char		service_restart_command[MAXLEN];
	char		service_reload_command[MAXLEN];
	char		service_promote_command[MAXLEN];

	/* event notification settings */
	char		event_notification_command[MAXLEN];
	EventNotificationList event_notifications;

	/* barman settings */
	char		barman_host[MAXLEN];
	char		barman_server[MAXLEN];
	char		barman_config[MAXLEN];

	/* undocumented test settings */
	int			promote_delay;
}	t_configuration_options;

/*
 * The following will initialize the structure with a minimal set of options;
 * actual defaults are set in parse_config() before parsing the configuration file
 */

#define T_CONFIGURATION_OPTIONS_INITIALIZER { \
		/* node information */ \
		UNKNOWN_NODE_ID, "", "", "", "", REPLICATION_TYPE_PHYSICAL, \
		/* log settings */ \
		"", "", "", DEFAULT_LOG_STATUS_INTERVAL,	\
		/* standby clone settings */ \
		false, "", "", "", "", { NULL, NULL },	\
		/* repmgrd settings */ \
		FAILOVER_MANUAL, DEFAULT_LOCATION, DEFAULT_PRIORITY, "", "", \
		DEFAULT_STATS_REPORTING_INTERVAL, \
		DEFAULT_RECONNECTION_ATTEMPTS, \
        DEFAULT_RECONNECTION_INTERVAL, \
        false, -1, \
		DEFAULT_ASYNC_QUERY_TIMEOUT, \
		DEFAULT_PRIMARY_NOTIFICATION_TIMEOUT,	\
		DEFAULT_PRIMARY_FOLLOW_TIMEOUT,	\
		/* BDR settings */ \
		false, DEFAULT_BDR_RECOVERY_TIMEOUT, \
		/* service settings */ \
		"", "", "", "", "", "", \
		/* event notification settings */ \
		"", { NULL, NULL }, \
		/* barman settings */ \
		"", "", "",	 \
		/* undocumented test settings */ \
		0 \
 }



typedef struct
{
	char		slot[MAXLEN];
	char		xlog_method[MAXLEN];
	bool		no_slot; /* from PostgreSQL 10 */
} t_basebackup_options;

#define T_BASEBACKUP_OPTIONS_INITIALIZER { "", "", false }



void		set_progname(const char *argv0);
const char *progname(void);

void		load_config(const char *config_file, bool verbose, bool terse, t_configuration_options *options, char *argv0);
void		parse_config(t_configuration_options *options, bool terse);
bool		reload_config(t_configuration_options *orig_options);


int			repmgr_atoi(const char *s,
						const char *config_item,
						ItemList *error_list,
						int minval);


bool parse_pg_basebackup_options(const char *pg_basebackup_options,
								 t_basebackup_options *backup_options,
								 int server_version_num,
								 ItemList *error_list);

/* called by repmgr-client and repmgrd */
void exit_with_cli_errors(ItemList *error_list);
void print_item_list(ItemList *item_list);

#endif /* _REPMGR_CONFIGFILE_H_ */
