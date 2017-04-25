/*
 * config.h
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 *
 */

#ifndef _REPMGR_CONFIG_H_
#define _REPMGR_CONFIG_H_

#define CONFIG_FILE_NAME	"repmgr.conf"
#define MAXLINELENGTH		4096
extern bool		config_file_found;


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
	int			upstream_node_id;
    char		node_name[MAXLEN];
	char		conninfo[MAXLEN];
	char		pg_bindir[MAXLEN];
	int			replication_type;

	/* log settings */
	char		loglevel[MAXLEN];
	char		logfacility[MAXLEN];
	char		logfile[MAXLEN];

	/* standby clone settings */
	bool		use_replication_slots;
	char		rsync_options[MAXLEN];
	char		ssh_options[MAXLEN];
	char		pg_basebackup_options[MAXLEN];
	char		restore_command[MAXLEN];
	TablespaceList tablespace_mapping;

	/* repmgrd settings */
	int			failover_mode;
	int			priority;
	char		promote_command[MAXLEN];
	char		follow_command[MAXLEN];
	int			monitor_interval_secs;
	int			master_response_timeout;
	int			reconnect_attempts;
	int			reconnect_interval;
	int			retry_promote_interval_secs;
	bool		monitoring_history;

	/* witness settings */
	int			witness_repl_nodes_sync_interval_secs;

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

    /* bdr settings */
    int			bdr_monitoring_mode;

	/* barman settings */
	char		barman_server[MAXLEN];
	char		barman_config[MAXLEN];
}	t_configuration_options;

/*
 * The following will initialize the structure with a minimal set of options;
 * actual defaults are set in parse_config() before parsing the configuration file
 */

#define T_CONFIGURATION_OPTIONS_INITIALIZER { \
		/* node information */ \
		UNKNOWN_NODE_ID, NO_UPSTREAM_NODE, "", "", "", REPLICATION_TYPE_PHYSICAL, \
		/* log settings */ \
		"", "", "", \
		/* standby clone settings */ \
		false, "", "", "", "", { NULL, NULL },	\
		/* repmgrd settings */ \
		MANUAL_FAILOVER, DEFAULT_PRIORITY, "", "", 2, 60, 6, 10, 300, false, \
		/* witness settings */ \
		30, \
		/* service settings */ \
		"", "", "", "", "", "", \
		/* event notification settings */ \
		"", { NULL, NULL }, \
		/* bdr settings */ \
		BDR_MONITORING_LOCAL, \
		/* barman settings */ \
		"", "" }



void		set_progname(const char *argv0);
const char *progname(void);

bool		load_config(const char *config_file, bool verbose, t_configuration_options *options, char *argv0);
bool		parse_config(t_configuration_options *options);
bool		reload_config(t_configuration_options *orig_options);


int			repmgr_atoi(const char *s,
						const char *config_item,
						ItemList *error_list,
						int minval);

bool		parse_bool(const char *s,
					   const char *config_item,
					   ItemList *error_list);

#endif
