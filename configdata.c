/*
 * configdata.c - contains structs with parsed configuration data
 *
 * Copyright (c) 2ndQuadrant, 2010-2020
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

#include "repmgr.h"
#include "configfile.h"

/*
 * Parsed configuration settings are stored here
 */
t_configuration_options config_file_options;


/*
 * Configuration settings are defined here
 */

struct ConfigFileSetting config_file_settings[] =
{

	/* ================
	 * node information
	 * ================
	 */
	/* node_id */
	{
		"node_id",
		CONFIG_INT,
		{ .intptr = &config_file_options.node_id },
		{ .intdefault = UNKNOWN_NODE_ID },
		{ .intminval = MIN_NODE_ID },
		{},
		{}
	},
	/* node_name */
	{
		"node_name",
		CONFIG_STRING,
		{ .strptr = config_file_options.node_name },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.node_name) },
		{}
	},
	/* conninfo */
	{
		"conninfo",
		CONFIG_STRING,
		{ .strptr = config_file_options.conninfo },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.conninfo) },
		{}
	},
	/* replication_user */
	{
		"replication_user",
		CONFIG_STRING,
		{ .strptr = config_file_options.replication_user },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.replication_user) },
		{}
	},
	/* data_directory */
	{
		"data_directory",
		CONFIG_STRING,
		{ .strptr = config_file_options.data_directory },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.data_directory) },
		{ .postprocess_func = &repmgr_canonicalize_path }
	},
	/* config_directory */
	{
		"config_directory",
		CONFIG_STRING,
		{ .strptr = config_file_options.config_directory },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.config_directory) },
		{ .postprocess_func = &repmgr_canonicalize_path }
	},
	/* pg_bindir */
	{
		"pg_bindir",
		CONFIG_STRING,
		{ .strptr = config_file_options.pg_bindir },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.pg_bindir) },
		{ .postprocess_func = &repmgr_canonicalize_path }
	},
	/* repmgr_bindir */
	{
		"repmgr_bindir",
		CONFIG_STRING,
		{ .strptr = config_file_options.repmgr_bindir },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.repmgr_bindir) },
		{ .postprocess_func = &repmgr_canonicalize_path }
	},
	/* replication_type */
	{
		"replication_type",
		CONFIG_REPLICATION_TYPE,
		{ .replicationtypeptr = &config_file_options.replication_type },
		{ .replicationtypedefault = DEFAULT_REPLICATION_TYPE },
		{},
		{},
		{}
	},

	/* ================
	 * logging settings
	 * ================
	 */

	/*
	 * log_level
	 * NOTE: the default for "log_level" is set in log.c and does not need
	 * to be initialised here
	 */
	{
		"log_level",
		CONFIG_STRING,
		{ .strptr = config_file_options.log_level },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.log_level) },
		{}
	},
	/* log_facility */
	{
		"log_facility",
		CONFIG_STRING,
		{ .strptr = config_file_options.log_facility },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.log_facility) },
		{}
	},
	/* log_file */
	{
		"log_file",
		CONFIG_STRING,
		{ .strptr = config_file_options.log_file },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.log_file) },
		{ .postprocess_func = &repmgr_canonicalize_path }
	},
	/* log_status_interval */
	{
		"log_status_interval",
		CONFIG_INT,
		{ .intptr = &config_file_options.log_status_interval },
		{ .intdefault = DEFAULT_LOG_STATUS_INTERVAL, },
		{ .intminval = 0 },
		{},
		{}
	},
	/* ======================
	 * standby clone settings
	 * ======================
	 */
	/* use_replication_slots */
	{
		"use_replication_slots",
		CONFIG_BOOL,
		{ .boolptr = &config_file_options.use_replication_slots },
		{ .booldefault = DEFAULT_USE_REPLICATION_SLOTS },
		{},
		{},
		{}
	},
	/* pg_basebackup_options */
	{
		"pg_basebackup_options",
		CONFIG_STRING,
		{ .strptr = config_file_options.pg_basebackup_options },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.pg_basebackup_options) },
		{}
	},
	/* restore_command */
	{
		"restore_command",
		CONFIG_STRING,
		{ .strptr = config_file_options.restore_command },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.restore_command) },
		{}
	},
	/* tablespace_mapping */
	{
		"tablespace_mapping",
		CONFIG_TABLESPACE_MAPPING,
		{ .tablespacemappingptr = &config_file_options.tablespace_mapping },
		{},
		{},
		{},
		{}
	},
	/* recovery_min_apply_delay */
	{
		"recovery_min_apply_delay",
		CONFIG_STRING,
		{ .strptr = config_file_options.recovery_min_apply_delay },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.recovery_min_apply_delay) },
        {
			.process_func = &parse_time_unit_parameter,
			.providedptr = &config_file_options.recovery_min_apply_delay_provided
		}
	},

	/* archive_cleanup_command */
	{
		"archive_cleanup_command",
		CONFIG_STRING,
		{ .strptr = config_file_options.archive_cleanup_command },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.archive_cleanup_command) },
		{}
	},

	/* use_primary_conninfo_password */
	{
		"use_primary_conninfo_password",
		CONFIG_BOOL,
		{ .boolptr = &config_file_options.use_primary_conninfo_password },
		{ .booldefault = DEFAULT_USE_PRIMARY_CONNINFO_PASSWORD },
		{},
		{},
		{}
	},
	/* passfile */
	{
		"passfile",
		CONFIG_STRING,
		{ .strptr = config_file_options.passfile },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.passfile) },
		{}
	},

	/* ======================
	 * standby clone settings
	 * ======================
	 */
	/* promote_check_timeout */
	{
		"promote_check_timeout",
		CONFIG_INT,
		{ .intptr = &config_file_options.promote_check_timeout },
		{ .intdefault = DEFAULT_PROMOTE_CHECK_TIMEOUT },
		{ .intminval = 1 },
		{},
		{}
	},
	/* promote_check_interval */
	{
		"promote_check_interval",
		CONFIG_INT,
		{ .intptr = &config_file_options.promote_check_interval },
		{ .intdefault = DEFAULT_PROMOTE_CHECK_INTERVAL },
		{ .intminval = 1 },
		{},
		{}
	},

	/* =======================
	 * standby follow settings
	 * =======================
	 */
	/* primary_follow_timeout */
	{
		"primary_follow_timeout",
		CONFIG_INT,
		{ .intptr = &config_file_options.primary_follow_timeout },
		{ .intdefault = DEFAULT_PRIMARY_FOLLOW_TIMEOUT, },
		{ .intminval = 1 },
		{},
		{}
	},
	/* standby_follow_timeout */
	{
		"standby_follow_timeout",
		CONFIG_INT,
		{ .intptr = &config_file_options.standby_follow_timeout },
		{ .intdefault = DEFAULT_STANDBY_FOLLOW_TIMEOUT },
		{ .intminval = 1 },
		{},
		{}
	},
	/* standby_follow_restart */
	{
		"standby_follow_restart",
		CONFIG_BOOL,
		{ .boolptr = &config_file_options.standby_follow_restart },
		{ .booldefault = DEFAULT_STANDBY_FOLLOW_RESTART },
		{},
		{},
		{}
	},

	/* ===========================
	 * standby switchover settings
	 * ===========================
	 */
	/* shutdown_check_timeout */
	{
		"shutdown_check_timeout",
		CONFIG_INT,
		{ .intptr = &config_file_options.shutdown_check_timeout },
		{ .intdefault = DEFAULT_SHUTDOWN_CHECK_TIMEOUT },
		{ .intminval = 1 },
		{},
		{}
	},
	/* standby_reconnect_timeout */
	{
		"standby_reconnect_timeout",
		CONFIG_INT,
		{ .intptr = &config_file_options.standby_reconnect_timeout },
		{ .intdefault = DEFAULT_STANDBY_RECONNECT_TIMEOUT },
		{ .intminval = 1 },
		{},
		{}
	},
	/* wal_receive_check_timeout */
	{
		"wal_receive_check_timeout",
		CONFIG_INT,
		{ .intptr = &config_file_options.wal_receive_check_timeout },
		{ .intdefault = DEFAULT_WAL_RECEIVE_CHECK_TIMEOUT },
		{ .intminval = 1 },
		{},
		{}
	},

	/* ====================
	 * node rejoin settings
	 * ====================
	 */
	/* node_rejoin_timeout */
	{
		"node_rejoin_timeout",
		CONFIG_INT,
		{ .intptr = &config_file_options.node_rejoin_timeout },
		{ .intdefault = DEFAULT_NODE_REJOIN_TIMEOUT },
		{ .intminval = 1 },
		{},
		{}
	},
	/* ===================
	 * node check settings
	 * ===================
	 */
	/* archive_ready_warning */
	{
		"archive_ready_warning",
		CONFIG_INT,
		{ .intptr = &config_file_options.archive_ready_warning },
		{ .intdefault = DEFAULT_ARCHIVE_READY_WARNING },
		{ .intminval = 1 },
		{},
		{}
	},
	/* archive_ready_critical */
	{
		"archive_ready_critical",
		CONFIG_INT,
		{ .intptr = &config_file_options.archive_ready_critical },
		{ .intdefault = DEFAULT_ARCHIVE_READY_CRITICAL },
		{ .intminval = 1 },
		{},
		{}
	},
	/* replication_lag_warning */
	{
		"replication_lag_warning",
		CONFIG_INT,
		{ .intptr = &config_file_options.replication_lag_warning },
		{ .intdefault = DEFAULT_REPLICATION_LAG_WARNING },
		{ .intminval = 1 },
		{},
		{}
	},
	/* replication_lag_critical */
	{
		"replication_lag_critical",
		CONFIG_INT,
		{ .intptr = &config_file_options.replication_lag_critical },
		{ .intdefault = DEFAULT_REPLICATION_LAG_CRITICAL },
		{ .intminval = 1 },
		{},
		{}
	},

	/* ================
	 * witness settings
	 * ================
	 */
	/* witness_sync_interval */
	{
		"witness_sync_interval",
		CONFIG_INT,
		{ .intptr = &config_file_options.witness_sync_interval },
		{ .intdefault = DEFAULT_WITNESS_SYNC_INTERVAL },
		{ .intminval = 1 },
		{},
		{}
	},

	/* ================
	 * repmgrd settings
	 * ================
	 */
	/* failover */
	{
		"failover",
		CONFIG_FAILOVER_MODE,
		{ .failovermodeptr = &config_file_options.failover },
		{ .failovermodedefault = FAILOVER_MANUAL },
		{},
		{},
		{}
	},
	/* location */
	{
		"location",
		CONFIG_STRING,
		{ .strptr = config_file_options.location },
		{ .strdefault = DEFAULT_LOCATION },
		{},
		{ .strmaxlen = sizeof(config_file_options.location) },
		{}
	},
	/* priority */
	{
		"priority",
		CONFIG_INT,
		{ .intptr = &config_file_options.priority },
		{ .intdefault = DEFAULT_PRIORITY, },
		{ .intminval = 0 },
		{},
		{}
	},
	/* promote_command */
	{
		"promote_command",
		CONFIG_STRING,
		{ .strptr = config_file_options.promote_command },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.promote_command) },
		{}
	},
	/* follow_command */
	{
		"follow_command",
		CONFIG_STRING,
		{ .strptr = config_file_options.follow_command },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.follow_command) },
		{}
	},
	/* monitor_interval_secs */
	{
		"monitor_interval_secs",
		CONFIG_INT,
		{ .intptr = &config_file_options.monitor_interval_secs },
		{ .intdefault = DEFAULT_MONITORING_INTERVAL },
		{ .intminval = 1 },
		{},
		{}
	},
	/* reconnect_attempts */
	{
		"reconnect_attempts",
		CONFIG_INT,
		{ .intptr = &config_file_options.reconnect_attempts },
		{ .intdefault = DEFAULT_RECONNECTION_ATTEMPTS },
		{ .intminval = 0 },
		{},
		{}
	},
	/* reconnect_interval */
	{
		"reconnect_interval",
		CONFIG_INT,
		{ .intptr = &config_file_options.reconnect_interval },
		{ .intdefault = DEFAULT_RECONNECTION_INTERVAL },
		{ .intminval = 0 },
		{},
		{}
	},

	/* monitoring_history */
	{
		"monitoring_history",
		CONFIG_BOOL,
		{ .boolptr = &config_file_options.monitoring_history },
		{ .booldefault = DEFAULT_MONITORING_HISTORY },
		{},
		{},
		{}
	},
	/* degraded_monitoring_timeout */
	{
		"degraded_monitoring_timeout",
		CONFIG_INT,
		{ .intptr = &config_file_options.degraded_monitoring_timeout },
		{ .intdefault = DEFAULT_DEGRADED_MONITORING_TIMEOUT },
		{ .intminval = -1 },
		{},
		{}
	},
	/* async_query_timeout */
	{
		"async_query_timeout",
		CONFIG_INT,
		{ .intptr = &config_file_options.async_query_timeout },
		{ .intdefault = DEFAULT_ASYNC_QUERY_TIMEOUT },
		{ .intminval = 0 },
		{},
		{}
	},
	/* primary_notification_timeout */
	{
		"primary_notification_timeout",
		CONFIG_INT,
		{ .intptr = &config_file_options.primary_notification_timeout },
		{ .intdefault = DEFAULT_PRIMARY_NOTIFICATION_TIMEOUT },
		{ .intminval = 0 },
		{},
		{}
	},
	/* repmgrd_standby_startup_timeout */
	{
		"repmgrd_standby_startup_timeout",
		CONFIG_INT,
		{ .intptr = &config_file_options.repmgrd_standby_startup_timeout },
		{ .intdefault = DEFAULT_REPMGRD_STANDBY_STARTUP_TIMEOUT },
		{ .intminval = 0 },
		{},
		{}
	},
	/* repmgrd_pid_file */
	{
		"repmgrd_pid_file",
		CONFIG_STRING,
		{ .strptr = config_file_options.repmgrd_pid_file },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.repmgrd_pid_file) },
		{ .postprocess_func = &repmgr_canonicalize_path }
	},
	/* standby_disconnect_on_failover */
	{
		"standby_disconnect_on_failover",
		CONFIG_BOOL,
		{ .boolptr = &config_file_options.standby_disconnect_on_failover },
		{ .booldefault = DEFAULT_STANDBY_DISCONNECT_ON_FAILOVER },
		{},
		{},
		{}
	},
	/* sibling_nodes_disconnect_timeout */
	{
		"sibling_nodes_disconnect_timeout",
		CONFIG_INT,
		{ .intptr = &config_file_options.sibling_nodes_disconnect_timeout },
		{ .intdefault = DEFAULT_SIBLING_NODES_DISCONNECT_TIMEOUT },
		{ .intminval = 0 },
		{},
		{}
	},
	/* connection_check_type */
	{
		"connection_check_type",
		CONFIG_CONNECTION_CHECK_TYPE,
		{ .checktypeptr = &config_file_options.connection_check_type },
		{ .checktypedefault = DEFAULT_CONNECTION_CHECK_TYPE },
		{},
		{},
		{}
	},
	/* primary_visibility_consensus */
	{
		"primary_visibility_consensus",
		CONFIG_BOOL,
		{ .boolptr = &config_file_options.primary_visibility_consensus },
		{ .booldefault = DEFAULT_PRIMARY_VISIBILITY_CONSENSUS },
		{},
		{},
		{}
	},
	/* always_promote */
	{
		"always_promote",
		CONFIG_BOOL,
		{ .boolptr = &config_file_options.always_promote },
		{ .booldefault = DEFAULT_ALWAYS_PROMOTE },
		{},
		{},
		{}
	},
	/* failover_validation_command */
	{
		"failover_validation_command",
		CONFIG_STRING,
		{ .strptr = config_file_options.failover_validation_command },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.failover_validation_command) },
		{}
	},
	/* election_rerun_interval */
	{
		"election_rerun_interval",
		CONFIG_INT,
		{ .intptr = &config_file_options.election_rerun_interval },
		{ .intdefault = DEFAULT_ELECTION_RERUN_INTERVAL },
		{ .intminval = 1 },
		{},
		{}
	},
	/* child_nodes_check_interval */
	{
		"child_nodes_check_interval",
		CONFIG_INT,
		{ .intptr = &config_file_options.child_nodes_check_interval },
		{ .intdefault = DEFAULT_CHILD_NODES_CHECK_INTERVAL },
		{ .intminval = 1 },
		{},
		{}
	},
	/* child_nodes_disconnect_min_count */
	{
		"child_nodes_disconnect_min_count",
		CONFIG_INT,
		{ .intptr = &config_file_options.child_nodes_disconnect_min_count },
		{ .intdefault = DEFAULT_CHILD_NODES_DISCONNECT_MIN_COUNT },
		{ .intminval = -1 },
		{},
		{}
	},
	/* child_nodes_connected_min_count */
	{
		"child_nodes_connected_min_count",
		CONFIG_INT,
		{ .intptr = &config_file_options.child_nodes_connected_min_count },
		{ .intdefault = DEFAULT_CHILD_NODES_CONNECTED_MIN_COUNT},
		{ .intminval = -1 },
		{},
		{}
	},
	/* child_nodes_connected_include_witness */
	{
		"child_nodes_connected_include_witness",
		CONFIG_BOOL,
		{ .boolptr = &config_file_options.child_nodes_connected_include_witness },
		{ .booldefault = DEFAULT_CHILD_NODES_CONNECTED_INCLUDE_WITNESS },
		{},
		{},
		{}
	},
	/* child_nodes_disconnect_timeout */
	{
		"child_nodes_disconnect_timeout",
		CONFIG_INT,
		{ .intptr = &config_file_options.child_nodes_disconnect_timeout },
		{ .intdefault = DEFAULT_CHILD_NODES_DISCONNECT_TIMEOUT },
		{ .intminval = 0 },
		{},
		{}
	},
	/* child_nodes_disconnect_command */
	{
		"child_nodes_disconnect_command",
		CONFIG_STRING,
		{ .strptr = config_file_options.child_nodes_disconnect_command },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.child_nodes_disconnect_command) },
		{}
	},
	/* ================
	 * service settings
	 * ================
	 */
	/* pg_ctl_options */
	{
		"pg_ctl_options",
		CONFIG_STRING,
		{ .strptr = config_file_options.pg_ctl_options },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.pg_ctl_options) },
		{}
	},
	/* service_start_command */
	{
		"service_start_command",
		CONFIG_STRING,
		{ .strptr = config_file_options.service_start_command },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.service_start_command) },
		{}
	},
	/* service_stop_command */
	{
		"service_stop_command",
		CONFIG_STRING,
		{ .strptr = config_file_options.service_stop_command },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.service_stop_command) },
		{}
	},
	/* service_restart_command */
	{
		"service_restart_command",
		CONFIG_STRING,
		{ .strptr = config_file_options.service_restart_command },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.service_restart_command) },
		{}
	},
	/* service_reload_command */
	{
		"service_reload_command",
		CONFIG_STRING,
		{ .strptr = config_file_options.service_reload_command },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.service_reload_command) },
		{}
	},
	/* service_promote_command */
	{
		"service_promote_command",
		CONFIG_STRING,
		{ .strptr = config_file_options.service_promote_command },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.service_promote_command) },
		{}
	},

	/* ========================
	 * repmgrd service settings
	 * ========================
	 */
	/* repmgrd_service_start_command */
	{
		"repmgrd_service_start_command",
		CONFIG_STRING,
		{ .strptr = config_file_options.repmgrd_service_start_command },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.repmgrd_service_start_command) },
		{}
	},
	/* repmgrd_service_stop_command */
	{
		"repmgrd_service_stop_command",
		CONFIG_STRING,
		{ .strptr = config_file_options.repmgrd_service_stop_command },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.repmgrd_service_stop_command) },
		{}
	},
	/* ===========================
	 * event notification settings
	 * ===========================
	 */
	/* event_notification_command */
	{
		"event_notification_command",
		CONFIG_STRING,
		{ .strptr = config_file_options.event_notification_command },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.event_notification_command) },
		{}
	},
	{
		"event_notifications",
		CONFIG_EVENT_NOTIFICATION_LIST,
		{ .notificationlistptr = &config_file_options.event_notifications },
		{},
		{},
		{},
		{}
	},
	/* ===============
	 * barman settings
	 * ===============
	 */
	/* barman_host */
	{
		"barman_host",
		CONFIG_STRING,
		{ .strptr = config_file_options.barman_host },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.barman_host) },
		{}
	},
	/* barman_server */
	{
		"barman_server",
		CONFIG_STRING,
		{ .strptr = config_file_options.barman_server },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.barman_server) },
		{}
	},
	/* barman_config */
	{
		"barman_config",
		CONFIG_STRING,
		{ .strptr = config_file_options.barman_config },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.barman_config) },
		{}
	},
	/* ==================
	 * rsync/ssh settings
	 * ==================
	 */
	/* rsync_options */
	{
		"rsync_options",
		CONFIG_STRING,
		{ .strptr = config_file_options.rsync_options },
		{ .strdefault = "" },
		{},
		{ .strmaxlen = sizeof(config_file_options.rsync_options) },
		{}
	},
	/* ssh_options */
	{
		"ssh_options",
		CONFIG_STRING,
		{ .strptr = config_file_options.ssh_options },
		{ .strdefault = DEFAULT_SSH_OPTIONS },
		{},
		{ .strmaxlen = sizeof(config_file_options.ssh_options) },
		{}
	},
	/* ==================================
	 * undocumented experimental settings
	 * ==================================
	 */
	/* reconnect_loop_sync */
	{
		"reconnect_loop_sync",
		CONFIG_BOOL,
		{ .boolptr = &config_file_options.reconnect_loop_sync },
		{ .booldefault = false },
		{},
		{},
		{}
	},
	/* ==========================
	 * undocumented test settings
	 * ==========================
	 */
	/* promote_delay */
	{
		"promote_delay",
		CONFIG_INT,
		{ .intptr = &config_file_options.promote_delay },
		{ .intdefault = 0 },
		{ .intminval = 1 },
		{},
		{}
	},
	/* failover_delay */
	{
		"failover_delay",
		CONFIG_INT,
		{ .intptr = &config_file_options.failover_delay },
		{ .intdefault = 0 },
		{ .intminval = 1 },
		{},
		{}
	},
	{
		"connection_check_query",
		CONFIG_STRING,
		{ .strptr = config_file_options.connection_check_query },
		{ .strdefault = "SELECT 1" },
		{},
		{ .strmaxlen = sizeof(config_file_options.connection_check_query) },
		{}
	},
	/* End-of-list marker */
	{
		NULL, CONFIG_INT, {}, {}, {}, {}, {}
	}
};

