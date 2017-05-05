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

#define NODE_RECORD_NOT_FOUND   0
#define NODE_RECORD_QUERY_ERROR -1

typedef enum {
	UNKNOWN = 0,
	MASTER,
	STANDBY,
	WITNESS,
	BDR
} t_server_type;

typedef enum {
	REPMGR_INSTALLED = 0,
	REPMGR_AVAILABLE,
	REPMGR_UNAVAILABLE,
    REPMGR_UNKNOWN
} t_extension_status;

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
	char		  slot_name[MAXLEN];
	int			  priority;
	bool		  active;
	bool		  is_ready;
	bool		  is_visible;
	XLogRecPtr	  xlog_location;
}	t_node_info;


#define T_NODE_INFO_INITIALIZER { \
  NODE_NOT_FOUND, \
  NO_UPSTREAM_NODE, \
  UNKNOWN, \
  "", \
  "", \
  "", \
  DEFAULT_PRIORITY, \
  true, \
  false, \
  false, \
  InvalidXLogRecPtr \
}


typedef struct NodeInfoListCell
{
	struct NodeInfoListCell *next;
	t_node_info *node_info;
} NodeInfoListCell;

typedef struct NodeInfoList
{
	NodeInfoListCell *head;
	NodeInfoListCell *tail;
} NodeInfoList;


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
int			is_standby(PGconn *conn);
PGconn	   *get_master_connection(PGconn *standby_conn, int *master_id, char *master_conninfo_out);
int			get_master_node_id(PGconn *conn);

/* extension functions */
t_extension_status get_repmgr_extension_status(PGconn *conn);

/* result functions */
bool		atobool(const char *value);

/* node record functions */
t_server_type parse_node_type(const char *type);
const char * get_node_type_string(t_server_type type);

int			get_node_record(PGconn *conn, int node_id, t_node_info *node_info);
int			get_node_record_by_name(PGconn *conn, const char *node_name, t_node_info *node_info);

bool		create_node_record(PGconn *conn, char *repmgr_action, t_node_info *node_info);
bool		update_node_record(PGconn *conn, char *repmgr_action, t_node_info *node_info);

/* event record functions */
bool        create_event_record(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details);
bool        create_event_record_extended(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details, t_event_info *event_info);

/* replication slot functions */
bool		create_replication_slot(PGconn *conn, char *slot_name, int server_version_num, PQExpBufferData *error_msg);
bool		drop_replication_slot(PGconn *conn, char *slot_name);
int			get_slot_record(PGconn *conn, char *slot_name, t_replication_slot *record);

/* backup functions */
bool		start_backup(PGconn *conn, char *first_wal_segment, bool fast_checkpoint, int server_version_num);
bool		stop_backup(PGconn *conn, char *last_wal_segment, int server_version_num);

#endif

