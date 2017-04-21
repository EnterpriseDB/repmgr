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


typedef enum {
	UNKNOWN = 0,
	MASTER,
	STANDBY,
	WITNESS,
	BDR
} t_server_type;

/*
 * Struct to store node information
 */
typedef struct s_node_info
{
	int			  node_id;
	int			  upstream_node_id;
	t_server_type type;
	char		  name[MAXLEN];
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
}   t_event_info;

#define T_EVENT_INFO_INITIALIZER { \
  NULL, \
  NULL \
}


/*
 * Struct to store list of conninfo keywords and values
 */
typedef struct
{
	int    size;
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
}   t_replication_slot;


/* connection functions */
PGconn *establish_db_connection(const char *conninfo,
								const bool exit_on_error);

PGconn *establish_db_connection_as_user(const char *conninfo,
										const char *user,
										const bool exit_on_error);

PGconn *establish_db_connection_by_params(const char *keywords[],
										  const char *values[],
										  const bool exit_on_error);


/* conninfo manipulation functions */
void initialize_conninfo_params(t_conninfo_param_list *param_list, bool set_defaults);
void copy_conninfo_params(t_conninfo_param_list *dest_list, t_conninfo_param_list *source_list);
void conn_to_param_list(PGconn *conn, t_conninfo_param_list *param_list);
void param_set(t_conninfo_param_list *param_list, const char *param, const char *value);
char *param_get(t_conninfo_param_list *param_list, const char *param);
bool parse_conninfo_string(const char *conninfo_str, t_conninfo_param_list *param_list, char *errmsg, bool ignore_application_name);

/* GUC manipulation functions */
bool		set_config(PGconn *conn, const char *config_param,  const char *config_value);
bool		set_config_bool(PGconn *conn, const char *config_param, bool state);

/* Server information functions */
int			get_server_version(PGconn *conn, char *server_version);
int			is_standby(PGconn *conn);

#endif

