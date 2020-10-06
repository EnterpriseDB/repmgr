/*
 * configfile.h
 *
 * Copyright (c) 2ndQuadrant, 2010-2020
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

/*
 * This is defined in src/include/utils.h, however it's not practical
 * to include that from a frontend application.
 */
#define PG_AUTOCONF_FILENAME "postgresql.auto.conf"

extern bool config_file_found;
extern char config_file_path[MAXPGPATH];

typedef enum
{
	FAILOVER_MANUAL,
	FAILOVER_AUTOMATIC
} failover_mode_opt;

typedef enum
{
	CHECK_PING,
	CHECK_QUERY,
	CHECK_CONNECTION
} ConnectionCheckType;

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


typedef enum
{
	CONFIG_BOOL,
	CONFIG_INT,
	CONFIG_STRING,
	CONFIG_FAILOVER_MODE,
	CONFIG_CONNECTION_CHECK_TYPE,
	CONFIG_EVENT_NOTIFICATION_LIST,
	CONFIG_TABLESPACE_MAPPING
} ConfigItemType;


typedef struct ConfigFileSetting
{
	const char	   *name;
	ConfigItemType  type;
    union
    {
        int		   *intptr;
        char	   *strptr;
		bool	   *boolptr;
		failover_mode_opt *failovermodeptr;
		ConnectionCheckType *checktypeptr;
		EventNotificationList *notificationlistptr;
		TablespaceList *tablespacemappingptr;
	} val;
	union {
		int			intdefault;
		const char *strdefault;
		bool		booldefault;
		failover_mode_opt failovermodedefault;
		ConnectionCheckType checktypedefault;
	} defval;
	union {
		int				intminval;
	} minval;
	union {
		int				strmaxlen;
	} maxval;
	struct {
		void (*process_func)(const char *, const char *, char *, ItemList *errors);
		void (*postprocess_func)(const char *, const char *, char *, ItemList *errors);
		bool	   *providedptr;
	} process;
} ConfigFileSetting;

/* Declare the main configfile structure for client applications */
extern ConfigFileSetting config_file_settings[];

typedef struct
{
	/* node information */
	int			node_id;
	char		node_name[NAMEDATALEN];
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
	char		log_file[MAXPGPATH];
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
	bool		standby_follow_restart;

	/* standby switchover settings */
	int			shutdown_check_timeout;
	int			standby_reconnect_timeout;
	int			wal_receive_check_timeout;

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
	bool		standby_disconnect_on_failover;
	int			sibling_nodes_disconnect_timeout;
	ConnectionCheckType connection_check_type;
	bool		primary_visibility_consensus;
	bool		always_promote;
	char		failover_validation_command[MAXPGPATH];
	int			election_rerun_interval;
	int			child_nodes_check_interval;
	int			child_nodes_disconnect_min_count;
	int			child_nodes_connected_min_count;
	bool		child_nodes_connected_include_witness;
	int			child_nodes_disconnect_timeout;
	char		child_nodes_disconnect_command[MAXPGPATH];

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

	/*
	 * undocumented settings
	 *
	 * These settings are for testing or experimential features
	 * and may be changed without notice.
	 */

	/* experimental settings */
	bool		reconnect_loop_sync;

	/* test settings */
	int			promote_delay;
	int			failover_delay;
	char		connection_check_query[MAXLEN];
} t_configuration_options;


/* Declare the main configfile structure for client applications */
extern t_configuration_options config_file_options;

typedef struct
{
	char		slot[MAXLEN];
	char		wal_method[MAXLEN];
	char		waldir[MAXPGPATH];
	bool		no_slot;		/* from PostgreSQL 10 */
} t_basebackup_options;

#define T_BASEBACKUP_OPTIONS_INITIALIZER { "", "", "", false }


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

void		load_config(const char *config_file, bool verbose, bool terse, char *argv0);
bool		reload_config(t_server_type server_type);
void		dump_config(void);

void		parse_configuration_item(ItemList *error_list, ItemList *warning_list, const char *name, const char *value);

bool		parse_recovery_conf(const char *data_dir, t_recovery_conf *conf);

bool		parse_bool(const char *s,
					   const char *config_item,
					   ItemList *error_list);

int repmgr_atoi(const char *s,
			const char *config_item,
			ItemList *error_list,
			int minval);

void parse_time_unit_parameter(const char *name, const char *value, char *dest, ItemList *errors);
void repmgr_canonicalize_path(const char *name, const char *value, char *config_item, ItemList *errors);

bool parse_pg_basebackup_options(const char *pg_basebackup_options,
							t_basebackup_options *backup_options,
							int server_version_num,
							ItemList *error_list);

int parse_output_to_argv(const char *string, char ***argv_array);
void free_parsed_argv(char ***argv_array);
const char *format_failover_mode(failover_mode_opt failover);

/* called by repmgr-client and repmgrd */
void		exit_with_cli_errors(ItemList *error_list, const char *repmgr_command);

void		print_item_list(ItemList *item_list);
const char *print_connection_check_type(ConnectionCheckType type);
char 	   *print_event_notification_list(EventNotificationList *list);
char 	   *print_tablespace_mapping(TablespaceList *tablespacemappingptr);

extern bool modify_auto_conf(const char *data_dir, KeyValueList *items);

extern bool ProcessRepmgrConfigFile(const char *config_file, const char *base_dir, ItemList *error_list, ItemList *warning_list);

extern bool ProcessPostgresConfigFile(const char *config_file, const char *base_dir, KeyValueList *contents, ItemList *error_list, ItemList *warning_list);

#endif							/* _REPMGR_CONFIGFILE_H_ */
