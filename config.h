/*
 * config.h
 * Copyright (c) 2ndQuadrant, 2010-2016
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
 *
 */

#ifndef _REPMGR_CONFIG_H_
#define _REPMGR_CONFIG_H_

#include "postgres_fe.h"

#include "strutil.h"

#define CONFIG_FILE_NAME	"repmgr.conf"

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
	char		cluster_name[MAXLEN];
	int			node;
	int         upstream_node;
	char		conninfo[MAXLEN];
	char		ssh_hostname[MAXLEN];
	char		barman_server[MAXLEN];
	int			failover;
	int			priority;
	char		node_name[MAXLEN];
	char		promote_command[MAXLEN];
	char		follow_command[MAXLEN];
	char		loglevel[MAXLEN];
	char		logfacility[MAXLEN];
	char		rsync_options[QUERY_STR_LEN];
	char		ssh_options[QUERY_STR_LEN];
	int			master_response_timeout;
	int			reconnect_attempts;
	int			reconnect_interval;
	char		pg_bindir[MAXLEN];
	char		pg_ctl_options[MAXLEN];
	char		pg_basebackup_options[MAXLEN];
        char            pg_restore_command[MAXLEN];
	char		logfile[MAXLEN];
	int			monitor_interval_secs;
	int			retry_promote_interval_secs;
	int			witness_repl_nodes_sync_interval_secs;
	int			use_replication_slots;
	char		event_notification_command[MAXLEN];
	EventNotificationList event_notifications;
	TablespaceList tablespace_mapping;
}	t_configuration_options;

#define T_CONFIGURATION_OPTIONS_INITIALIZER { "", -1, NO_UPSTREAM_NODE, "", "", "", MANUAL_FAILOVER, -1, "", "", "", "", "", "", "", -1, -1, -1, "", "", "", "", "", 0, 0, 0, 0, "", { NULL, NULL }, {NULL, NULL} }

typedef struct ErrorListCell
{
	struct ErrorListCell *next;
	char			     *error_message;
} ErrorListCell;

typedef struct ErrorList
{
	ErrorListCell *head;
	ErrorListCell *tail;
} ErrorList;

typedef struct TablespaceDataListCell
{
	struct TablespaceDataListCell *next;
	char	   *name;
	char	   *oid;
	char	   *location;
	/* optional payload */
	FILE       *f;
} TablespaceDataListCell;

typedef struct TablespaceDataList
{
	TablespaceDataListCell *head;
	TablespaceDataListCell *tail;
} TablespaceDataList;

void set_progname(const char *argv0);
const char * progname(void);

bool		load_config(const char *config_file, bool verbose, t_configuration_options *options, char *argv0);
bool		reload_config(t_configuration_options *orig_options);
bool		parse_config(t_configuration_options *options);
void		parse_line(char *buff, char *name, char *value);
char	   *trim(char *s);
void		error_list_append(ErrorList *error_list, char *error_message);
int			repmgr_atoi(const char *s,
						const char *config_item,
						ErrorList *error_list,
						bool allow_negative);

#endif
