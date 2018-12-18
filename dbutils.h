/*
 * dbutils.h
 *
 * Copyright (c) 2ndQuadrant, 2010-2018
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

#ifndef _REPMGR_DBUTILS_H_
#define _REPMGR_DBUTILS_H_

#include "access/timeline.h"
#include "access/xlogdefs.h"
#include "pqexpbuffer.h"
#include "portability/instr_time.h"

#include "configfile.h"
#include "strutil.h"
#include "voting.h"

#define REPMGR_NODES_COLUMNS "n.node_id, n.type, n.upstream_node_id, n.node_name, n.conninfo, n.repluser, n.slot_name, n.location, n.priority, n.active, n.config_file, '' AS upstream_node_name "
#define BDR2_NODES_COLUMNS "node_sysid, node_timeline, node_dboid, node_name, node_local_dsn, ''"
#define BDR3_NODES_COLUMNS "ns.node_id, 0, 0, ns.node_name, ns.interface_connstr, ns.peer_state_name"


#define ERRBUFF_SIZE 512

typedef enum
{
	UNKNOWN = 0,
	PRIMARY,
	STANDBY,
	WITNESS,
	BDR
} t_server_type;

typedef enum
{
	REPMGR_INSTALLED = 0,
	REPMGR_OLD_VERSION_INSTALLED,
	REPMGR_AVAILABLE,
	REPMGR_UNAVAILABLE,
	REPMGR_UNKNOWN
} ExtensionStatus;

typedef enum
{
	RECTYPE_UNKNOWN = -1,
	RECTYPE_PRIMARY,
	RECTYPE_STANDBY
} RecoveryType;

typedef enum
{
	RECORD_ERROR = -1,
	RECORD_FOUND,
	RECORD_NOT_FOUND
} RecordStatus;

typedef enum
{
	MS_NORMAL = 0,
	MS_DEGRADED = 1
} MonitoringState;

typedef enum
{
	NODE_STATUS_UNKNOWN = -1,
	NODE_STATUS_UP,
	NODE_STATUS_SHUTTING_DOWN,
	NODE_STATUS_DOWN,
	NODE_STATUS_UNCLEAN_SHUTDOWN
} NodeStatus;

typedef enum
{
	CONN_UNKNOWN = -1,
	CONN_OK,
	CONN_BAD,
	CONN_ERROR
} ConnectionStatus;

typedef enum
{
	SLOT_UNKNOWN = -1,
	SLOT_INACTIVE,
	SLOT_ACTIVE
} ReplSlotStatus;

typedef enum
{
	BACKUP_STATE_UNKNOWN = -1,
	BACKUP_STATE_IN_BACKUP,
	BACKUP_STATE_NO_BACKUP
} BackupState;


/*
 * Struct to store extension version information
 */

typedef struct s_extension_versions {
	char		default_version[8];
	int			default_version_num;
	char		installed_version[8];
	int			installed_version_num;
} t_extension_versions;

#define T_EXTENSION_VERSIONS_INITIALIZER { \
	"", \
	UNKNOWN_SERVER_VERSION_NUM, \
	"", \
	UNKNOWN_SERVER_VERSION_NUM \
}

/*
 * Struct to store node information
 */
typedef struct s_node_info
{
	/* contents of "repmgr.nodes" */
	int			node_id;
	int			upstream_node_id;
	t_server_type type;
	char		node_name[MAXLEN];
	char		upstream_node_name[MAXLEN];
	char		conninfo[MAXLEN];
	char		repluser[NAMEDATALEN];
	char		location[MAXLEN];
	int			priority;
	bool		active;
	char		slot_name[MAXLEN];
	char		config_file[MAXPGPATH];
	/* used during failover to track node status */
	XLogRecPtr	last_wal_receive_lsn;
	NodeStatus	node_status;
	RecoveryType recovery_type;
	MonitoringState monitoring_state;
	PGconn	   *conn;
	/* for ad-hoc use e.g. when working with a list of nodes */
	char		details[MAXLEN];
	bool		reachable;
	bool		attached;
	/* various statistics */
	int			max_wal_senders;
	int			attached_wal_receivers;
	int			max_replication_slots;
	int			total_replication_slots;
	int			active_replication_slots;
	int			inactive_replication_slots;
} t_node_info;


#define T_NODE_INFO_INITIALIZER { \
   	/* contents of "repmgr.nodes" */ \
	NODE_NOT_FOUND, \
	NO_UPSTREAM_NODE, \
	UNKNOWN, \
	"", \
	"", \
	"", \
	"", \
	DEFAULT_LOCATION, \
	DEFAULT_PRIORITY, \
	true, \
	"", \
	"", \
	/* used during failover to track node status */ \
	InvalidXLogRecPtr, \
	NODE_STATUS_UNKNOWN, \
	RECTYPE_UNKNOWN,  \
	MS_NORMAL, \
	NULL, \
	/* for ad-hoc use e.g. when working with a list of nodes */ \
	"", true, true \
	/* various statistics */ \
	-1, -1, -1, -1, -1, -1					\
}


/* structs to store a list of repmgr node records */
typedef struct NodeInfoListCell
{
	struct NodeInfoListCell *next;
	t_node_info *node_info;
} NodeInfoListCell;

typedef struct NodeInfoList
{
	NodeInfoListCell *head;
	NodeInfoListCell *tail;
	int			node_count;
} NodeInfoList;

#define T_NODE_INFO_LIST_INITIALIZER { \
	NULL, \
	NULL, \
	0 \
}

typedef struct s_event_info
{
	char	   *node_name;
	char	   *conninfo_str;
	int			node_id;
} t_event_info;

#define T_EVENT_INFO_INITIALIZER { \
	NULL, \
	NULL, \
	UNKNOWN_NODE_ID \
}


/*
 * Struct to store list of conninfo keywords and values
 */
typedef struct
{
	int			size;
	char	  **keywords;
	char	  **values;
} t_conninfo_param_list;

#define T_CONNINFO_PARAM_LIST_INITIALIZER { \
	0, \
	NULL, \
	NULL, \
}

/*
 * Struct to store replication slot information
 */
typedef struct s_replication_slot
{
	char		slot_name[MAXLEN];
	char		slot_type[MAXLEN];
	bool		active;
} t_replication_slot;

#define T_REPLICATION_SLOT_INITIALIZER { "", "", false }


typedef struct s_connection_user
{
	char		username[MAXLEN];
	bool		is_superuser;
} t_connection_user;

#define T_CONNECTION_USER_INITIALIZER { "", false }


/* represents an entry in bdr.bdr_nodes */
typedef struct s_bdr_node_info
{
	char		node_sysid[MAXLEN];
	uint32		node_timeline;
	uint32		node_dboid;
	char		node_name[MAXLEN];
	char		node_local_dsn[MAXLEN];
	char		peer_state_name[MAXLEN];
} t_bdr_node_info;

#define T_BDR_NODE_INFO_INITIALIZER { \
	"", InvalidOid, InvalidOid, \
    "", "", "" \
}


/* structs to store a list of BDR node records */
typedef struct BdrNodeInfoListCell
{
	struct BdrNodeInfoListCell *next;
	t_bdr_node_info *node_info;
} BdrNodeInfoListCell;

typedef struct BdrNodeInfoList
{
	BdrNodeInfoListCell *head;
	BdrNodeInfoListCell *tail;
	int			node_count;
} BdrNodeInfoList;

#define T_BDR_NODE_INFO_LIST_INITIALIZER { \
	NULL, \
	NULL, \
	0 \
}

typedef struct
{
	char		current_timestamp[MAXLEN];
	uint64		last_wal_receive_lsn;
	uint64		last_wal_replay_lsn;
	char		last_xact_replay_timestamp[MAXLEN];
	int			replication_lag_time;
	bool		receiving_streamed_wal;
} ReplInfo;

#define T_REPLINFO_INTIALIZER { \
	"", \
	InvalidXLogRecPtr, \
	InvalidXLogRecPtr, \
	"", \
	0 \
}


typedef struct
{
	char		filepath[MAXPGPATH];
	char		filename[MAXPGPATH];
	bool		in_data_directory;
} t_configfile_info;

#define T_CONFIGFILE_INFO_INITIALIZER { "", "", false }

typedef struct
{
	int			size;
	int			entries;
	t_configfile_info **files;
} t_configfile_list;

#define T_CONFIGFILE_LIST_INITIALIZER { 0, 0, NULL }

typedef struct
{
	uint64		system_identifier;
	TimeLineID	timeline;
	XLogRecPtr	xlogpos;
} t_system_identification;

#define T_SYSTEM_IDENTIFICATION_INITIALIZER { \
    UNKNOWN_SYSTEM_IDENTIFIER, \
    UNKNOWN_TIMELINE_ID, \
	InvalidXLogRecPtr \
}


typedef struct RepmgrdInfo {
	int node_id;
	int pid;
	char pid_text[MAXLEN];
	char pid_file[MAXLEN];
	bool pg_running;
	char pg_running_text[MAXLEN];
	bool running;
	char repmgrd_running[MAXLEN];
	bool paused;
} RepmgrdInfo;


/* macros */

#define is_streaming_replication(x) (x == PRIMARY || x == STANDBY)
#define format_lsn(x) (uint32) (x >> 32), (uint32) x

/* utility functions */

XLogRecPtr	parse_lsn(const char *str);

extern void
wrap_ddl_query(PQExpBufferData *query_buf, int replication_type, const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));
bool		atobool(const char *value);

/* connection functions */
PGconn	   *establish_db_connection(const char *conninfo,
						const bool exit_on_error);
PGconn	   *establish_db_connection_quiet(const char *conninfo);
PGconn	   *establish_db_connection_by_params(t_conninfo_param_list *param_list,
								  const bool exit_on_error);
PGconn	   *establish_primary_db_connection(PGconn *conn,
								const bool exit_on_error);
PGconn	   *get_primary_connection(PGconn *standby_conn, int *primary_id, char *primary_conninfo_out);
PGconn	   *get_primary_connection_quiet(PGconn *standby_conn, int *primary_id, char *primary_conninfo_out);

bool		is_superuser_connection(PGconn *conn, t_connection_user *userinfo);
void		close_connection(PGconn **conn);

/* conninfo manipulation functions */
bool		get_conninfo_value(const char *conninfo, const char *keyword, char *output);
bool		get_conninfo_default_value(const char *param, char *output, int maxlen);
void		initialize_conninfo_params(t_conninfo_param_list *param_list, bool set_defaults);
void		free_conninfo_params(t_conninfo_param_list *param_list);
void		copy_conninfo_params(t_conninfo_param_list *dest_list, t_conninfo_param_list *source_list);
void		conn_to_param_list(PGconn *conn, t_conninfo_param_list *param_list);
void		param_set(t_conninfo_param_list *param_list, const char *param, const char *value);
void		param_set_ine(t_conninfo_param_list *param_list, const char *param, const char *value);
char	   *param_get(t_conninfo_param_list *param_list, const char *param);
bool		parse_conninfo_string(const char *conninfo_str, t_conninfo_param_list *param_list, char **errmsg, bool ignore_local_params);
char	   *param_list_to_string(t_conninfo_param_list *param_list);
bool		has_passfile(void);


/* transaction functions */
bool		begin_transaction(PGconn *conn);
bool		commit_transaction(PGconn *conn);
bool		rollback_transaction(PGconn *conn);

/* GUC manipulation functions */
bool		set_config(PGconn *conn, const char *config_param, const char *config_value);
bool		set_config_bool(PGconn *conn, const char *config_param, bool state);
int		    guc_set(PGconn *conn, const char *parameter, const char *op, const char *value);
int			guc_set_typed(PGconn *conn, const char *parameter, const char *op, const char *value, const char *datatype);
bool		get_pg_setting(PGconn *conn, const char *setting, char *output);

/* server information functions */
bool		get_cluster_size(PGconn *conn, char *size);
int			get_server_version(PGconn *conn, char *server_version_buf);

RecoveryType get_recovery_type(PGconn *conn);
int			get_primary_node_id(PGconn *conn);
int			get_ready_archive_files(PGconn *conn, const char *data_directory);
bool		identify_system(PGconn *repl_conn, t_system_identification *identification);
TimeLineHistoryEntry *get_timeline_history(PGconn *repl_conn, TimeLineID tli);

/* repmgrd shared memory functions */
bool		repmgrd_set_local_node_id(PGconn *conn, int local_node_id);
int			repmgrd_get_local_node_id(PGconn *conn);
BackupState	server_in_exclusive_backup_mode(PGconn *conn);
void		repmgrd_set_pid(PGconn *conn, pid_t repmgrd_pid, const char *pidfile);
pid_t		repmgrd_get_pid(PGconn *conn);
bool		repmgrd_is_running(PGconn *conn);
bool		repmgrd_is_paused(PGconn *conn);
bool		repmgrd_pause(PGconn *conn, bool pause);

/* extension functions */
ExtensionStatus get_repmgr_extension_status(PGconn *conn, t_extension_versions *extversions);

/* node management functions */
void		checkpoint(PGconn *conn);
bool		vacuum_table(PGconn *conn, const char *table);
bool		promote_standby(PGconn *conn, bool wait, int wait_seconds);

/* node record functions */
t_server_type parse_node_type(const char *type);
const char *get_node_type_string(t_server_type type);

RecordStatus get_node_record(PGconn *conn, int node_id, t_node_info *node_info);
RecordStatus get_node_record_with_upstream(PGconn *conn, int node_id, t_node_info *node_info);

RecordStatus get_node_record_by_name(PGconn *conn, const char *node_name, t_node_info *node_info);
t_node_info *get_node_record_pointer(PGconn *conn, int node_id);

bool		get_local_node_record(PGconn *conn, int node_id, t_node_info *node_info);
bool		get_primary_node_record(PGconn *conn, t_node_info *node_info);

bool		get_all_node_records(PGconn *conn, NodeInfoList *node_list);
void		get_downstream_node_records(PGconn *conn, int node_id, NodeInfoList *nodes);
void		get_active_sibling_node_records(PGconn *conn, int node_id, int upstream_node_id, NodeInfoList *node_list);
void		get_node_records_by_priority(PGconn *conn, NodeInfoList *node_list);
bool		get_all_node_records_with_upstream(PGconn *conn, NodeInfoList *node_list);
bool		get_downstream_nodes_with_missing_slot(PGconn *conn, int this_node_id, NodeInfoList *noede_list);

bool		create_node_record(PGconn *conn, char *repmgr_action, t_node_info *node_info);
bool		update_node_record(PGconn *conn, char *repmgr_action, t_node_info *node_info);
bool		delete_node_record(PGconn *conn, int node);
bool		truncate_node_records(PGconn *conn);

bool		update_node_record_set_active(PGconn *conn, int this_node_id, bool active);
bool		update_node_record_set_primary(PGconn *conn, int this_node_id);
bool		update_node_record_set_active_standby(PGconn *conn, int this_node_id);
bool		update_node_record_set_upstream(PGconn *conn, int this_node_id, int new_upstream_node_id);
bool		update_node_record_status(PGconn *conn, int this_node_id, char *type, int upstream_node_id, bool active);
bool		update_node_record_conn_priority(PGconn *conn, t_configuration_options *options);
bool		update_node_record_slot_name(PGconn *primary_conn, int node_id, char *slot_name);

bool		witness_copy_node_records(PGconn *primary_conn, PGconn *witness_conn);

void		clear_node_info_list(NodeInfoList *nodes);

/* PostgreSQL configuration file location functions */
bool		get_datadir_configuration_files(PGconn *conn, KeyValueList *list);
bool		get_configuration_file_locations(PGconn *conn, t_configfile_list *list);
void		config_file_list_init(t_configfile_list *list, int max_size);
void		config_file_list_add(t_configfile_list *list, const char *file, const char *filename, bool in_data_dir);

/* event functions */
bool		create_event_record(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details);
bool		create_event_notification(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details);
bool		create_event_notification_extended(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details, t_event_info *event_info);
PGresult   *get_event_records(PGconn *conn, int node_id, const char *node_name, const char *event, bool all, int limit);

/* replication slot functions */
void		create_slot_name(char *slot_name, int node_id);
bool		create_replication_slot(PGconn *conn, char *slot_name, PQExpBufferData *error_msg);
bool		drop_replication_slot(PGconn *conn, char *slot_name);
RecordStatus get_slot_record(PGconn *conn, char *slot_name, t_replication_slot *record);
int			get_free_replication_slot_count(PGconn *conn);
int			get_inactive_replication_slots(PGconn *conn, KeyValueList *list);

/* tablespace functions */
bool		get_tablespace_name_by_location(PGconn *conn, const char *location, char *name);

/* asynchronous query functions */
bool		cancel_query(PGconn *conn, int timeout);
int			wait_connection_availability(PGconn *conn, long long timeout);

/* node availability functions */
bool		is_server_available(const char *conninfo);
bool		is_server_available_params(t_conninfo_param_list *param_list);
ExecStatusType	connection_ping(PGconn *conn);

/* monitoring functions  */
void
add_monitoring_record(PGconn *primary_conn,
					  PGconn *local_conn,
					  int primary_node_id,
					  int local_node_id,
					  char *monitor_standby_timestamp,
					  XLogRecPtr primary_last_wal_location,
					  XLogRecPtr last_wal_receive_lsn,
					  char *last_xact_replay_timestamp,
					  long long unsigned int replication_lag_bytes,
					  long long unsigned int apply_lag_bytes
);

int			get_number_of_monitoring_records_to_delete(PGconn *primary_conn, int keep_history, int node_id);
bool		delete_monitoring_records(PGconn *primary_conn, int keep_history, int node_id);



/* node voting functions */
void		initialize_voting_term(PGconn *conn);
int			get_current_term(PGconn *conn);
void		increment_current_term(PGconn *conn);
bool		announce_candidature(PGconn *conn, t_node_info *this_node, t_node_info *other_node, int electoral_term);
void		notify_follow_primary(PGconn *conn, int primary_node_id);
bool		get_new_primary(PGconn *conn, int *primary_node_id);
void		reset_voting_status(PGconn *conn);

/* replication status functions */
XLogRecPtr	get_current_wal_lsn(PGconn *conn);
XLogRecPtr	get_last_wal_receive_location(PGconn *conn);
XLogRecPtr	get_current_lsn(PGconn *conn);
bool		get_replication_info(PGconn *conn, ReplInfo *replication_info);
int			get_replication_lag_seconds(PGconn *conn);
void		get_node_replication_stats(PGconn *conn, t_node_info *node_info);
bool		is_downstream_node_attached(PGconn *conn, char *node_name);
void		set_primary_last_seen(PGconn *conn);
int			get_primary_last_seen(PGconn *conn);

/* BDR functions */
int			get_bdr_version_num(void);
void		get_all_bdr_node_records(PGconn *conn, BdrNodeInfoList *node_list);
RecordStatus get_bdr_node_record_by_name(PGconn *conn, const char *node_name, t_bdr_node_info *node_info);
bool		is_bdr_db(PGconn *conn, PQExpBufferData *output);
bool		is_bdr_db_quiet(PGconn *conn);
bool		is_active_bdr_node(PGconn *conn, const char *node_name);
bool		is_bdr_repmgr(PGconn *conn);
char	   *get_default_bdr_replication_set(PGconn *conn);
bool		is_table_in_bdr_replication_set(PGconn *conn, const char *tablename, const char *set);
bool		add_table_to_bdr_replication_set(PGconn *conn, const char *tablename, const char *set);
void		add_extension_tables_to_bdr_replication_set(PGconn *conn);
bool		bdr_node_name_matches(PGconn *conn, const char *node_name, PQExpBufferData *bdr_local_node_name);
ReplSlotStatus get_bdr_node_replication_slot_status(PGconn *conn, const char *node_name);
void		get_bdr_other_node_name(PGconn *conn, int node_id, char *name_buf);

bool		am_bdr_failover_handler(PGconn *conn, int node_id);
void		unset_bdr_failover_handler(PGconn *conn);
bool		bdr_node_has_repmgr_set(PGconn *conn, const char *node_name);
bool		bdr_node_set_repmgr_set(PGconn *conn, const char *node_name);

/* miscellaneous debugging functions */
const char *print_node_status(NodeStatus node_status);
const char *print_pqping_status(PGPing ping_status);

#endif							/* _REPMGR_DBUTILS_H_ */
