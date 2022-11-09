/*
 * repmgr-action-standby.h
 * Copyright (c) EnterpriseDB Corporation, 2010-2021
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

#ifndef _REPMGR_ACTION_STANDBY_H_
#define _REPMGR_ACTION_STANDBY_H_


typedef struct
{
	int			reachable_sibling_node_count;
	int			reachable_sibling_nodes_with_slot_count;
	int			unreachable_sibling_node_count;
	int			min_required_wal_senders;
	int			min_required_free_slots;
} SiblingNodeStats;

#define T_SIBLING_NODES_STATS_INITIALIZER { \
	0, \
	0, \
	0, \
	0, \
	0 \
}

typedef struct
{
	RepmgrdInfo **repmgrd_info;
	int			repmgrd_running_count;

	bool		dry_run_success;

	/* store list of configuration files on the demotion candidate */
	KeyValueList remote_config_files;

	/* used for handling repmgrd pause/unpause */
	NodeInfoList all_nodes;

	NodeInfoList sibling_nodes;
	SiblingNodeStats sibling_nodes_stats;

	t_event_info event_info;
	int remote_node_id;
	t_node_info remote_node_record;
	t_node_info local_node_record;
	char remote_conninfo[MAXCONNINFO];
	bool	switchover_success;
	RecoveryType recovery_type;
	PGconn	*superuser_conn;

	/* the remote server is the primary to be demoted */
	char		remote_host[MAXLEN];
	int 		remote_repmgr_version;
	PGconn	*remote_conn;
	PGconn	*local_conn;
} t_standby_switchover_rec;

#define T_STANDBY_SWITCHOVER_INITIALIZER { \
	NULL, \
	true, \
	{NULL, NULL}, \
	T_NODE_INFO_LIST_INITIALIZER, \
	T_NODE_INFO_LIST_INITIALIZER, \
	T_SIBLING_NODES_STATS_INITIALIZER, \
	T_EVENT_INFO_INITIALIZER, \
	UNKNOWN_NODE_ID, \
	T_NODE_INFO_INITIALIZER, \
	T_NODE_INFO_INITIALIZER, \
	"", \
	true, \
	RECTYPE_UNKNOWN, \
	NULL, \
	"", \
	UNKNOWN_REPMGR_VERSION_NUM, \
	NULL, \
	NULL \
}

extern void do_standby_clone(void);
extern void do_standby_register(void);
extern void do_standby_unregister(void);
extern void do_standby_promote(void);
extern void do_standby_follow(void);
extern void do_standby_switchover(void);

extern void do_standby_help(void);

extern bool do_standby_follow_internal(PGconn *primary_conn, PGconn *follow_target_conn, t_node_info *follow_target_node_record, PQExpBufferData *output, int general_error_code, int *error_code);



#endif							/* _REPMGR_ACTION_STANDBY_H_ */
