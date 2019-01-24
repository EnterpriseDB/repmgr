/*
 * configfile.h
 *
 * Copyright (c) 2ndQuadrant, 2010-2019
 *
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

#ifndef _REPMGR_CONFIGFILE_H_
#define _REPMGR_CONFIGFILE_H_

#include <getopt_long.h>

#define CONFIG_FILE_NAME	"repmgr.conf"
#define MAXLINELENGTH		4096
/* magic number for use in t_recovery_conf */
#define TARGET_TIMELINE_LATEST 0

extern bool config_file_found;
extern char config_file_path[MAXPGPATH];

typedef enum
{
	FAILOVER_MANUAL,
	FAILOVER_AUTOMATIC
} failover_mode_opt;

typedef struct EventNotificationListCell
{
	struct EventNotificationListCell *next;
	char		event_type[MAXLEN];
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
	char		data_directory[MAXPGPATH];
	char		config_directory[MAXPGPATH];
	char		pg_bindir[MAXPGPATH];
	char		repmgr_bindir[MAXPGPATH];
	int			replication_type;

	/* log settings */
	char		log_level[MAXLEN];
	char		log_facility[MAXLEN];
	char		log_file[MAXLEN];
	int			log_status_interval;

	/* standby clone settings */
	bool		use_replication_slots;
	char		pg_basebackup_options[MAXLEN];
	char		restore_command[MAXLEN];
	TablespaceList tablespace_mapping;
	char		recovery_min_apply_delay[MAXLEN];
	bool		recovery_min_apply_delay_provided;
	char		archive_cleanup_command[MAXLEN];
	bool		use_primary_conninfo_password;
	char		passfile[MAXPGPATH];

	/* standby promote settings */
	int			promote_check_timeout;
	int			promote_check_interval;

	/* standby follow settings */
	int			primary_follow_timeout;
	int			standby_follow_timeout;

	/* standby switchover settings */
	int			shutdown_check_timeout;
	int			standby_reconnect_timeout;

	/* node rejoin settings */
	int			node_rejoin_timeout;

	/* node check settings */
	int			archive_ready_warning;
	int			archive_ready_critical;
	int			replication_lag_warning;
	int			replication_lag_critical;

	/* witness settings */
	int			witness_sync_interval;

	/* repmgrd settings */
	failover_mode_opt failover;
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
	int			repmgrd_standby_startup_timeout;
	char		repmgrd_pid_file[MAXPGPATH];

	/* BDR settings */
	bool		bdr_local_monitoring_only;
	bool		bdr_recovery_timeout;

	/* service settings */
	char		pg_ctl_options[MAXLEN];
	char		service_start_command[MAXPGPATH];
	char		service_stop_command[MAXPGPATH];
	char		service_restart_command[MAXPGPATH];
	char		service_reload_command[MAXPGPATH];
	char		service_promote_command[MAXPGPATH];

	/* repmgrd service settings */
	char		repmgrd_service_start_command[MAXPGPATH];
	char		repmgrd_service_stop_command[MAXPGPATH];

	/* event notification settings */
	char		event_notification_command[MAXPGPATH];
	char		event_notifications_orig[MAXLEN];
	EventNotificationList event_notifications;

	/* barman settings */
	char		barman_host[MAXLEN];
	char		barman_server[MAXLEN];
	char		barman_config[MAXLEN];

	/* rsync/ssh settings */
	char		rsync_options[MAXLEN];
	char		ssh_options[MAXLEN];

	/* undocumented test settings */
	int			promote_delay;
} t_configuration_options;

/*
 * The following will initialize the structure with a minimal set of options;
 * actual defaults are set in parse_config() before parsing the configuration file
 */

#define T_CONFIGURATION_OPTIONS_INITIALIZER { \
		/* node information */ \
		UNKNOWN_NODE_ID, "", "", "", "", "", "", "", REPLICATION_TYPE_PHYSICAL,	\
		/* log settings */ \
		"", "", "", DEFAULT_LOG_STATUS_INTERVAL,	\
		/* standby clone settings */ \
		false, "", "", { NULL, NULL }, "", false, "", false, "", \
		/* standby promote settings */ \
		DEFAULT_PROMOTE_CHECK_TIMEOUT, DEFAULT_PROMOTE_CHECK_INTERVAL, \
		/* standby follow settings */ \
		DEFAULT_PRIMARY_FOLLOW_TIMEOUT,	\
		DEFAULT_STANDBY_FOLLOW_TIMEOUT,	\
		/* standby switchover settings */ \
		DEFAULT_SHUTDOWN_CHECK_TIMEOUT, \
		DEFAULT_STANDBY_RECONNECT_TIMEOUT, \
		/* node rejoin settings */ \
		DEFAULT_NODE_REJOIN_TIMEOUT, \
		/* node check settings */ \
		DEFAULT_ARCHIVE_READY_WARNING, DEFAULT_ARCHIVE_READY_CRITICAL, \
		DEFAULT_REPLICATION_LAG_WARNING, DEFAULT_REPLICATION_LAG_CRITICAL, \
		/* witness settings */ \
		DEFAULT_WITNESS_SYNC_INTERVAL, \
		/* repmgrd settings */ \
		FAILOVER_MANUAL, DEFAULT_LOCATION, DEFAULT_PRIORITY, "", "", \
		DEFAULT_MONITORING_INTERVAL, \
		DEFAULT_RECONNECTION_ATTEMPTS, \
        DEFAULT_RECONNECTION_INTERVAL, \
        false, -1, \
		DEFAULT_ASYNC_QUERY_TIMEOUT, \
		DEFAULT_PRIMARY_NOTIFICATION_TIMEOUT,	\
		-1, "", \
		/* BDR settings */ \
		false, DEFAULT_BDR_RECOVERY_TIMEOUT, \
		/* service settings */ \
		"", "", "", "", "", "", \
		/* repmgrd service settings */ \
		"", "",  \
		/* event notification settings */ \
		"", "", { NULL, NULL }, \
		/* barman settings */ \
		"", "", "",	 \
		/* rsync/ssh settings */ \
		 "", "", \
		/* undocumented test settings */ \
		0 \
 }



typedef struct
{
	char		slot[MAXLEN];
	char		xlog_method[MAXLEN];
	bool		no_slot;		/* from PostgreSQL 10 */
} t_basebackup_options;

#define T_BASEBACKUP_OPTIONS_INITIALIZER { "", "", false }


typedef enum
{
	RTA_PAUSE,
	RTA_PROMOTE,
	RTA_SHUTDOWN
} RecoveryTargetAction;

/*
 * Struct to hold the contents of a parsed recovery.conf file.
 * We're only really interested in those related to streaming
 * replication (and also "restore_command") but include the
 * others for completeness.
 *
 * NOTE: "recovery_target" not included as it can only have
 * one value, "immediate".
 */
typedef struct
{
	/* archive recovery settings */
	char		restore_command[MAXLEN];
	char		archive_cleanup_command[MAXLEN];
	char		recovery_end_command[MAXLEN];
	/* recovery target settings */
	char		recovery_target_name[MAXLEN];
	char		recovery_target_time[MAXLEN];
	char		recovery_target_xid[MAXLEN];
	bool		recovery_target_inclusive;
	int			recovery_target_timeline;
	RecoveryTargetAction recovery_target_action;	/* default: RTA_PAUSE */
	/* standby server settings */
	bool		standby_mode;
	char		primary_conninfo[MAXLEN];
	char		primary_slot_name[MAXLEN];
	char		trigger_file[MAXLEN];
	char		recovery_min_apply_delay[MAXLEN];
} t_recovery_conf;

#define T_RECOVERY_CONF_INITIALIZER { \
	/* archive recovery settings */ \
	"", "", "", \
	/* recovery target settings */ \
	"", "", "", true, \
	TARGET_TIMELINE_LATEST, \
	RTA_PAUSE, \
	/* standby server settings */ \
	true, \
	"", "", "", "" \
}

#include "dbutils.h"

void		set_progname(const char *argv0);
const char *progname(void);

void		load_config(const char *config_file, bool verbose, bool terse, t_configuration_options *options, char *argv0);
bool		reload_config(t_configuration_options *orig_options, t_server_type server_type);

bool		parse_recovery_conf(const char *data_dir, t_recovery_conf *conf);

bool		parse_bool(const char *s,
					   const char *config_item,
					   ItemList *error_list);

int repmgr_atoi(const char *s,
			const char *config_item,
			ItemList *error_list,
			int minval);

bool parse_pg_basebackup_options(const char *pg_basebackup_options,
							t_basebackup_options *backup_options,
							int server_version_num,
							ItemList *error_list);

int parse_output_to_argv(const char *string, char ***argv_array);
void free_parsed_argv(char ***argv_array);


/* called by repmgr-client and repmgrd */
void		exit_with_cli_errors(ItemList *error_list, const char *repmgr_command);
void		print_item_list(ItemList *item_list);

#endif							/* _REPMGR_CONFIGFILE_H_ */
