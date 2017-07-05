/*
 * dbutils.h
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#ifndef _REPMGR_DBUTILS_H_
#define _REPMGR_DBUTILS_H_

#include "access/xlogdefs.h"
#include "pqexpbuffer.h"

#include "config.h"
#include "strutil.h"
#include "voting.h"

typedef enum {
	UNKNOWN = 0,
	PRIMARY,
	STANDBY,
	WITNESS,
	BDR
} t_server_type;

typedef enum {
	REPMGR_INSTALLED = 0,
	REPMGR_AVAILABLE,
	REPMGR_UNAVAILABLE,
    REPMGR_UNKNOWN
} ExtensionStatus;

typedef enum {
	RECTYPE_UNKNOWN = 0,
	RECTYPE_PRIMARY,
	RECTYPE_STANDBY
} RecoveryType;

typedef enum {
	RECORD_ERROR = -1,
	RECORD_FOUND,
	RECORD_NOT_FOUND
} RecordStatus;




/*
 * Struct to store node information
 */
typedef struct s_node_info
{
	int			  node_id;
	int			  upstream_node_id;
	t_server_type type;
	char		  node_name[MAXLEN];
	char		  conninfo[MAXLEN];
	char		  repluser[NAMEDATALEN];
	char		  location[MAXLEN];
	int			  priority;
	bool		  active;
	char		  slot_name[MAXLEN];
	/* used during failover to track node status */
	bool		  is_ready;
	bool		  is_visible;
	XLogRecPtr	  last_wal_receive_lsn;
	PGconn		 *conn;
}	t_node_info;


#define T_NODE_INFO_INITIALIZER { \
  NODE_NOT_FOUND, \
  NO_UPSTREAM_NODE, \
  UNKNOWN, \
  "", \
  "", \
  "", \
  DEFAULT_LOCATION, \
  DEFAULT_PRIORITY, \
  true, \
  "", \
  false, \
  false, \
  InvalidXLogRecPtr, \
  NULL \
}


/* structs to store a list of node records */
typedef struct NodeInfoListCell
{
	struct NodeInfoListCell *next;
	t_node_info *node_info;
} NodeInfoListCell;

typedef struct NodeInfoList
{
	NodeInfoListCell *head;
	NodeInfoListCell *tail;
	int				  node_count;
} NodeInfoList;

#define T_NODE_INFO_LIST_INITIALIZER { \
	NULL, \
	NULL, \
	0 \
}

typedef struct s_event_info
{
	char		  *node_name;
	char		  *conninfo_str;
}	t_event_info;

#define T_EVENT_INFO_INITIALIZER { \
  NULL, \
  NULL \
}


/*
 * Struct to store list of conninfo keywords and values
 */
typedef struct
{
	int	   size;
	char **keywords;
	char **values;
} t_conninfo_param_list;


/*
 * Struct to store replication slot information
 */
typedef struct s_replication_slot
{
	char slot_name[MAXLEN];
	char slot_type[MAXLEN];
	bool active;
}	t_replication_slot;


typedef struct s_connection_user
{
	char username[MAXLEN];
	bool is_superuser;
}   t_connection_user;

/* utility functions */

XLogRecPtr parse_lsn(const char *str);


/* connection functions */
PGconn *establish_db_connection(const char *conninfo,
								const bool exit_on_error);
PGconn *establish_db_connection_quiet(const char *conninfo);
PGconn *establish_db_connection_as_user(const char *conninfo,
										const char *user,
										const bool exit_on_error);

PGconn *establish_db_connection_by_params(const char *keywords[],
										  const char *values[],
										  const bool exit_on_error);
PGconn *establish_primary_db_connection(PGconn *conn,
										const bool exit_on_error);

PGconn	   *get_primary_connection(PGconn *standby_conn, int *primary_id, char *primary_conninfo_out);
PGconn	   *get_primary_connection_quiet(PGconn *standby_conn, int *primary_id, char *primary_conninfo_out);

bool		is_superuser_connection(PGconn *conn, t_connection_user *userinfo);

/* conninfo manipulation functions */
bool		get_conninfo_value(const char *conninfo, const char *keyword, char *output);

void		initialize_conninfo_params(t_conninfo_param_list *param_list, bool set_defaults);
void		copy_conninfo_params(t_conninfo_param_list *dest_list, t_conninfo_param_list *source_list);
void		conn_to_param_list(PGconn *conn, t_conninfo_param_list *param_list);
void		param_set(t_conninfo_param_list *param_list, const char *param, const char *value);
char	   *param_get(t_conninfo_param_list *param_list, const char *param);
bool		parse_conninfo_string(const char *conninfo_str, t_conninfo_param_list *param_list, char *errmsg, bool ignore_application_name);
char	   *param_list_to_string(t_conninfo_param_list *param_list);

/* transaction functions */
bool		begin_transaction(PGconn *conn);
bool		commit_transaction(PGconn *conn);
bool		rollback_transaction(PGconn *conn);
bool		check_cluster_schema(PGconn *conn);

/* GUC manipulation functions */
bool		set_config(PGconn *conn, const char *config_param,	const char *config_value);
bool		set_config_bool(PGconn *conn, const char *config_param, bool state);
int			guc_set(PGconn *conn, const char *parameter, const char *op,
			const char *value);
int			guc_set_typed(PGconn *conn, const char *parameter, const char *op,
			  const char *value, const char *datatype);
bool		get_pg_setting(PGconn *conn, const char *setting, char *output);

/* server information functions */
bool		get_cluster_size(PGconn *conn, char *size);
int			get_server_version(PGconn *conn, char *server_version);
RecoveryType get_recovery_type(PGconn *conn);
int			get_primary_node_id(PGconn *conn);

/* extension functions */
ExtensionStatus get_repmgr_extension_status(PGconn *conn);

/* result functions */
bool		atobool(const char *value);

/* node record functions */
t_server_type parse_node_type(const char *type);
const char * get_node_type_string(t_server_type type);

RecordStatus get_node_record(PGconn *conn, int node_id, t_node_info *node_info);
RecordStatus get_node_record_by_name(PGconn *conn, const char *node_name, t_node_info *node_info);
bool		get_local_node_record(PGconn *conn, int node_id, t_node_info *node_info);
bool		get_primary_node_record(PGconn *conn, t_node_info *node_info);
void		get_downstream_node_records(PGconn *conn, int node_id, NodeInfoList *nodes);
void		get_active_sibling_node_records(PGconn *conn, int node_id, int upstream_node_id, NodeInfoList *node_list);

bool		create_node_record(PGconn *conn, char *repmgr_action, t_node_info *node_info);
bool		update_node_record(PGconn *conn, char *repmgr_action, t_node_info *node_info);
bool		delete_node_record(PGconn *conn, int node);

bool		update_node_record_set_primary(PGconn *conn, int this_node_id);
bool		update_node_record_set_upstream(PGconn *conn, int this_node_id, int new_upstream_node_id);
bool		update_node_record_status(PGconn *conn, int this_node_id, char *type, int upstream_node_id, bool active);

void		clear_node_info_list(NodeInfoList *nodes);

/* event functions */
bool		create_event_record(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details);
bool		create_event_notification(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details);
bool		create_event_notification_extended(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details, t_event_info *event_info);

/* replication slot functions */
bool		create_replication_slot(PGconn *conn, char *slot_name, int server_version_num, PQExpBufferData *error_msg);
bool		drop_replication_slot(PGconn *conn, char *slot_name);
RecordStatus get_slot_record(PGconn *conn, char *slot_name, t_replication_slot *record);

/* asynchronous query functions */
bool		cancel_query(PGconn *conn, int timeout);
int			wait_connection_availability(PGconn *conn, long long timeout);

/* node availability functions */
bool		is_server_available(const char *conninfo);


/* node voting functions */
NodeVotingStatus get_voting_status(PGconn *conn);
int	request_vote(PGconn *conn, t_node_info *this_node, t_node_info *other_node, int electoral_term);
int set_voting_status_initiated(PGconn *conn);
bool announce_candidature(PGconn *conn, t_node_info *this_node, t_node_info *other_node, int electoral_term);
void notify_follow_primary(PGconn *conn, int primary_node_id);
bool get_new_primary(PGconn *conn, int *primary_node_id);
void reset_voting_status(PGconn *conn);

/* replication status functions */

XLogRecPtr get_last_wal_receive_location(PGconn *conn);

#endif /* dbutils.h */

