/*
 * repmgrd-physical.c - physical (streaming) replication functionality for repmgrd
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

#include <signal.h>

#include "repmgr.h"
#include "repmgrd.h"
#include "repmgrd-physical.h"

typedef enum
{
	FAILOVER_STATE_UNKNOWN = -1,
	FAILOVER_STATE_NONE,
	FAILOVER_STATE_PROMOTED,
	FAILOVER_STATE_PROMOTION_FAILED,
	FAILOVER_STATE_PRIMARY_REAPPEARED,
	FAILOVER_STATE_LOCAL_NODE_FAILURE,
	FAILOVER_STATE_WAITING_NEW_PRIMARY,
	FAILOVER_STATE_FOLLOW_NEW_PRIMARY,
	FAILOVER_STATE_REQUIRES_MANUAL_FAILOVER,
	FAILOVER_STATE_FOLLOWED_NEW_PRIMARY,
	FAILOVER_STATE_FOLLOWING_ORIGINAL_PRIMARY,
	FAILOVER_STATE_NO_NEW_PRIMARY,
	FAILOVER_STATE_FOLLOW_FAIL,
	FAILOVER_STATE_NODE_NOTIFICATION_ERROR,
	FAILOVER_STATE_ELECTION_RERUN
} FailoverState;


typedef enum
{
	ELECTION_NOT_CANDIDATE = -1,
	ELECTION_WON,
	ELECTION_LOST,
	ELECTION_CANCELLED,
	ELECTION_RERUN
} ElectionResult;

typedef struct election_stats
{
	int visible_nodes;
	int shared_upstream_nodes;
	int all_nodes;
} election_stats;

typedef struct t_child_node_info
{
	int node_id;
	char node_name[NAMEDATALEN];
	t_server_type type;
	NodeAttached attached;
	instr_time detached_time;
	struct t_child_node_info *next;
} t_child_node_info;

typedef struct t_child_node_info_list
{
	t_child_node_info *head;
	t_child_node_info *tail;
	int			node_count;
} t_child_node_info_list;

#define T_CHILD_NODE_INFO_LIST_INITIALIZER { \
	NULL, \
	NULL, \
	0 \
}

static PGconn *upstream_conn = NULL;
static PGconn *primary_conn = NULL;

static FailoverState failover_state = FAILOVER_STATE_UNKNOWN;

static int	primary_node_id = UNKNOWN_NODE_ID;
static t_node_info upstream_node_info = T_NODE_INFO_INITIALIZER;

static instr_time last_monitoring_update;

static bool child_nodes_disconnect_command_executed = false;

static ElectionResult do_election(NodeInfoList *sibling_nodes, int *new_primary_id);
static const char *_print_election_result(ElectionResult result);

static FailoverState promote_self(void);
static void notify_followers(NodeInfoList *standby_nodes, int follow_node_id);

static void check_connection(t_node_info *node_info, PGconn **conn);

static bool check_primary_status(int degraded_monitoring_elapsed);
static void check_primary_child_nodes(t_child_node_info_list *local_child_nodes);

static bool wait_primary_notification(int *new_primary_id);
static FailoverState follow_new_primary(int new_primary_id);
static FailoverState witness_follow_new_primary(int new_primary_id);

static void reset_node_voting_status(void);

static bool do_primary_failover(void);
static bool do_upstream_standby_failover(void);
static bool do_witness_failover(void);

static bool update_monitoring_history(void);

static void handle_sighup(PGconn **conn, t_server_type server_type);

static const char *format_failover_state(FailoverState failover_state);
static ElectionResult execute_failover_validation_command(t_node_info *node_info, election_stats *stats);
static void parse_failover_validation_command(const char *template, t_node_info *node_info, election_stats *stats, PQExpBufferData *out);
static bool check_node_can_follow(PGconn *local_conn, XLogRecPtr local_xlogpos, PGconn *follow_target_conn, t_node_info *follow_target_node_info);
static void check_witness_attached(t_node_info *node_info, bool startup);

static t_child_node_info *append_child_node_record(t_child_node_info_list *nodes, int node_id, const char *node_name, t_server_type type, NodeAttached attached);
static void remove_child_node_record(t_child_node_info_list *nodes, int node_id);
static void clear_child_node_info_list(t_child_node_info_list *nodes);
static void parse_child_nodes_disconnect_command(char *parsed_command, char *template, int reporting_node_id);
static void execute_child_nodes_disconnect_command(NodeInfoList *db_child_node_records, t_child_node_info_list *local_child_nodes);

static int try_primary_reconnect(PGconn **conn, PGconn *local_conn, t_node_info *node_info);

void
handle_sigint_physical(SIGNAL_ARGS)
{
	PGconn *writeable_conn;
	PQExpBufferData event_details;

	initPQExpBuffer(&event_details);

	appendPQExpBuffer(&event_details,
					  _("%s signal received"),
					  postgres_signal_arg == SIGTERM
					  ? "TERM" : "INT");

	log_notice("%s", event_details.data);

	if (local_node_info.type == PRIMARY)
		writeable_conn = local_conn;
	else
		writeable_conn = primary_conn;

	if (PQstatus(writeable_conn) == CONNECTION_OK)
		create_event_notification(writeable_conn,
								  &config_file_options,
								  config_file_options.node_id,
								  "repmgrd_shutdown",
								  true,
								  event_details.data);

	termPQExpBuffer(&event_details);

	terminate(SUCCESS);
}

/* perform some sanity checks on the node's configuration */

void
do_physical_node_check(void)
{
	/*
	 * Check if node record is active - if not, and `failover=automatic`, the
	 * node won't be considered as a promotion candidate; this often happens
	 * when a failed primary is recloned and the node was not re-registered,
	 * giving the impression failover capability is there when it's not. In
	 * this case abort with an error and a hint about registering.
	 *
	 * If `failover=manual`, repmgrd can continue to passively monitor the
	 * node, but we should nevertheless issue a warning and the same hint.
	 */

	if (local_node_info.active == false)
	{
		char	   *hint = "Check that \"repmgr (primary|standby) register\" was executed for this node";

		switch (config_file_options.failover)
		{
			/* "failover" is an enum, all values should be covered here */

			case FAILOVER_AUTOMATIC:
				log_error(_("this node is marked as inactive and cannot be used as a failover target"));
				log_hint(_("%s"), hint);

				create_event_notification(NULL,
										  &config_file_options,
										  config_file_options.node_id,
										  "repmgrd_shutdown",
										  false,
										  "node is inactive and cannot be used as a failover target");

				terminate(ERR_BAD_CONFIG);
				break;

			case FAILOVER_MANUAL:
				log_warning(_("this node is marked as inactive and will be passively monitored only"));
				log_hint(_("%s"), hint);
				break;
		}
	}

	if (config_file_options.failover == FAILOVER_AUTOMATIC)
	{
		/*
		 * Check that "promote_command" and "follow_command" are defined, otherwise repmgrd
		 * won't be able to perform any useful action in a failover situation.
		 */

		bool		required_param_missing = false;

		if (config_file_options.promote_command[0] == '\0')
		{
			log_error(_("\"promote_command\" must be defined in the configuration file"));

			if (config_file_options.service_promote_command[0] != '\0')
			{
				/*
				 * "service_promote_command" is *not* a substitute for "promote_command";
				 * it is intended for use in those systems (e.g. Debian) where there's a service
				 * level promote command (e.g. pg_ctlcluster).
				 *
				 * "promote_command" should either execute "repmgr standby promote" directly, or
				 * a script which executes "repmgr standby promote". This is essential, as the
				 * repmgr metadata is updated by "repmgr standby promote".
				 *
				 * "service_promote_command", if set, will be executed by "repmgr standby promote",
				 * but never by repmgrd.
				 *
				 */
				log_hint(_("\"service_promote_command\" is set, but can only be executed by \"repmgr standby promote\""));
			}

			required_param_missing = true;
		}

		if (config_file_options.follow_command[0] == '\0')
		{
			log_error(_("\"follow_command\" must be defined in the configuration file"));
			required_param_missing = true;
		}

		if (required_param_missing == true)
		{
			log_hint(_("add the missing configuration parameter(s) and start repmgrd again"));
			terminate(ERR_BAD_CONFIG);
		}
	}
}



/*
 * repmgrd running on the primary server
 */
void
monitor_streaming_primary(void)
{
	instr_time	log_status_interval_start;
	instr_time	child_nodes_check_interval_start;
	t_child_node_info_list local_child_nodes = T_CHILD_NODE_INFO_LIST_INITIALIZER;

	reset_node_voting_status();
	repmgrd_set_upstream_node_id(local_conn, NO_UPSTREAM_NODE);

	{
		PQExpBufferData event_details;
		initPQExpBuffer(&event_details);

		appendPQExpBuffer(&event_details,
						  _("monitoring cluster primary \"%s\" (ID: %i)"),
						  local_node_info.node_name,
						  local_node_info.node_id);

		/* Log startup event */
		if (startup_event_logged == false)
		{
			create_event_notification(local_conn,
									  &config_file_options,
									  config_file_options.node_id,
									  "repmgrd_start",
									  true,
									  event_details.data);

			startup_event_logged = true;
		}
		else
		{
			create_event_notification(local_conn,
									  &config_file_options,
									  config_file_options.node_id,
									  "repmgrd_reload",
									  true,
									  event_details.data);
		}

		log_notice("%s", event_details.data);

		termPQExpBuffer(&event_details);
	}

	INSTR_TIME_SET_CURRENT(log_status_interval_start);
	INSTR_TIME_SET_CURRENT(child_nodes_check_interval_start);
	local_node_info.node_status = NODE_STATUS_UP;

	/*
	 * get list of expected and attached nodes
	 */

	{
		NodeInfoList db_child_node_records = T_NODE_INFO_LIST_INITIALIZER;
		bool success = get_child_nodes(local_conn, config_file_options.node_id, &db_child_node_records);

		if (!success)
		{
			log_error(_("unable to retrieve list of child nodes"));
		}
		else
		{
			NodeInfoListCell *cell;

			for (cell = db_child_node_records.head; cell; cell = cell->next)
			{
				/*
				 * At startup, if a node for which a repmgr record exists, is not found
				 * in pg_stat_replication, we can't know whether it has become detached, or
				 * (e.g. during a provisioning operation) is a new node which has not yet
				 * attached. We set the status to "NODE_ATTACHED_UNKNOWN" to stop repmgrd
				 * emitting bogus "node has become detached" alerts.
				 */
				(void) append_child_node_record(&local_child_nodes,
												cell->node_info->node_id,
												cell->node_info->node_name,
												cell->node_info->type,
												cell->node_info->attached == NODE_ATTACHED ? NODE_ATTACHED : NODE_ATTACHED_UNKNOWN);

				/*
				 * witness will not be "attached" in the normal way
				 */
				if (cell->node_info->type == WITNESS)
				{
					check_witness_attached(cell->node_info, true);
				}

				if (cell->node_info->attached == NODE_ATTACHED)
				{
					log_info(_("child node \"%s\" (ID: %i) is attached"),
							 cell->node_info->node_name,
							 cell->node_info->node_id);
				}
				else
				{
					log_info(_("child node \"%s\" (ID: %i) is not yet attached"),
								 cell->node_info->node_name,
							 cell->node_info->node_id);
				}
			}
		}
	}

	while (true)
	{
		/*
		 * TODO: cache node list here, refresh at `node_list_refresh_interval`
		 * also return reason for inavailability so we can log it
		 */

		(void) connection_ping(local_conn);

		check_connection(&local_node_info, &local_conn);

		if (PQstatus(local_conn) != CONNECTION_OK)
		{

			/* local node is down, we were expecting it to be up */
			if (local_node_info.node_status == NODE_STATUS_UP)
			{

				instr_time	local_node_unreachable_start;

				INSTR_TIME_SET_CURRENT(local_node_unreachable_start);

				{
					PQExpBufferData event_details;
					initPQExpBuffer(&event_details);

					appendPQExpBufferStr(&event_details,
										 _("unable to connect to local node"));

					log_warning("%s", event_details.data);


					/*
					 * as we're monitoring the primary, no point in trying to
					 * write the event to the database
					 *
					 * TODO: possibly add pre-action event here
					 */
					create_event_notification(NULL,
											  &config_file_options,
											  config_file_options.node_id,
											  "repmgrd_local_disconnect",
											  true,
											  event_details.data);

					termPQExpBuffer(&event_details);
				}

				local_node_info.node_status = NODE_STATUS_UNKNOWN;

				try_reconnect(&local_conn, &local_node_info);

				if (local_node_info.node_status == NODE_STATUS_UP)
				{
					int			local_node_unreachable_elapsed = calculate_elapsed(local_node_unreachable_start);
					int 		stored_local_node_id = UNKNOWN_NODE_ID;
					PQExpBufferData event_details;

					initPQExpBuffer(&event_details);

					appendPQExpBuffer(&event_details,
									  _("reconnected to local node after %i seconds"),
									  local_node_unreachable_elapsed);
					log_notice("%s", event_details.data);

					create_event_notification(local_conn,
											  &config_file_options,
											  config_file_options.node_id,
											  "repmgrd_local_reconnect",
											  true,
											  event_details.data);
					termPQExpBuffer(&event_details);

					/*
					 * If the local node was restarted, we'll need to reinitialise values
					 * stored in shared memory.
					 */

					stored_local_node_id = repmgrd_get_local_node_id(local_conn);
					if (stored_local_node_id == UNKNOWN_NODE_ID)
					{
						repmgrd_set_local_node_id(local_conn, config_file_options.node_id);
						repmgrd_set_pid(local_conn, getpid(), pid_file);
					}

					/*
					 * check that the local node is still primary, otherwise switch
					 * to standby monitoring
					 */
					if (check_primary_status(NO_DEGRADED_MONITORING_ELAPSED) == false)
						return;

					goto loop;
				}

				monitoring_state = MS_DEGRADED;
				INSTR_TIME_SET_CURRENT(degraded_monitoring_start);
				log_notice(_("unable to connect to local node, falling back to degraded monitoring"));
			}

		}


		if (monitoring_state == MS_DEGRADED)
		{
			int			degraded_monitoring_elapsed = calculate_elapsed(degraded_monitoring_start);

			if (config_file_options.degraded_monitoring_timeout > 0
				&& degraded_monitoring_elapsed > config_file_options.degraded_monitoring_timeout)
			{
				PQExpBufferData event_details;

				initPQExpBuffer(&event_details);

				appendPQExpBuffer(&event_details,
								  _("degraded monitoring timeout (%i seconds) exceeded, terminating"),
								  degraded_monitoring_elapsed);

				log_notice("%s", event_details.data);

				create_event_notification(NULL,
										  &config_file_options,
										  config_file_options.node_id,
										  "repmgrd_shutdown",
										  true,
										  event_details.data);

				termPQExpBuffer(&event_details);
				terminate(ERR_MONITORING_TIMEOUT);
			}

			log_debug("monitoring node in degraded state for %i seconds", degraded_monitoring_elapsed);

			if (is_server_available(local_node_info.conninfo) == true)
			{
				local_conn = establish_db_connection(local_node_info.conninfo, false);

				if (PQstatus(local_conn) != CONNECTION_OK)
				{
					log_warning(_("node appears to be up but no connection could be made"));
					close_connection(&local_conn);
				}
				else
				{
					local_node_info.node_status = NODE_STATUS_UP;

					if (check_primary_status(degraded_monitoring_elapsed) == false)
						return;

					goto loop;
				}
			}


			/*
			 * possibly attempt to find another node from cached list check if
			 * there's a new primary - if so add hook for fencing? loop, if
			 * starts up check status, switch monitoring mode
			 */
		}
		else
		{
			if (config_file_options.child_nodes_check_interval > 0)
			{
				int			child_nodes_check_interval_elapsed = calculate_elapsed(child_nodes_check_interval_start);

				if (child_nodes_check_interval_elapsed >= config_file_options.child_nodes_check_interval)
				{
					INSTR_TIME_SET_CURRENT(child_nodes_check_interval_start);
					check_primary_child_nodes(&local_child_nodes);
				}
			}
		}

loop:

		/* check node is still primary, if not restart monitoring */
		if (check_primary_status(NO_DEGRADED_MONITORING_ELAPSED) == false)
			return;

		/* emit "still alive" log message at regular intervals, if requested */
		if (config_file_options.log_status_interval > 0)
		{
			int			log_status_interval_elapsed = calculate_elapsed(log_status_interval_start);

			if (log_status_interval_elapsed >= config_file_options.log_status_interval)
			{
				log_info(_("monitoring primary node \"%s\" (ID: %i) in %s state"),
						 local_node_info.node_name,
						 local_node_info.node_id,
						 print_monitoring_state(monitoring_state));

				if (monitoring_state == MS_DEGRADED)
				{
					log_detail(_("waiting for the node to become available"));
				}

				INSTR_TIME_SET_CURRENT(log_status_interval_start);
			}
		}

		if (got_SIGHUP)
		{
			handle_sighup(&local_conn, PRIMARY);
		}

		log_verbose(LOG_DEBUG, "sleeping %i seconds (parameter \"monitor_interval_secs\")",
					config_file_options.monitor_interval_secs);

		sleep(config_file_options.monitor_interval_secs);
	}
}


/*
 * If monitoring a primary, it's possible that after an outage of the local node
 * (due to e.g. a switchover), the node has come back as a standby. We therefore
 * need to verify its status and if everything looks OK, restart monitoring in
 * standby mode.
 *
 * Returns "true" to indicate repmgrd should continue monitoring the node as
 * a primary; "false" indicates repmgrd should start monitoring the node as
 * a standby.
 */
bool
check_primary_status(int degraded_monitoring_elapsed)
{
	PGconn *new_primary_conn;
	RecordStatus record_status;
	bool resume_monitoring = true;
	RecoveryType recovery_type = get_recovery_type(local_conn);

	if (recovery_type == RECTYPE_UNKNOWN)
	{
		log_warning(_("unable to determine node recovery status"));
		/* "true" to indicate repmgrd should continue monitoring in degraded state */
		return true;
	}

	/* node is still primary - resume monitoring */
	if (recovery_type == RECTYPE_PRIMARY)
	{
		if (degraded_monitoring_elapsed != NO_DEGRADED_MONITORING_ELAPSED)
		{
			PQExpBufferData event_details;

			monitoring_state = MS_NORMAL;

			initPQExpBuffer(&event_details);
			appendPQExpBuffer(&event_details,
							  _("reconnected to primary node after %i seconds, resuming monitoring"),
							  degraded_monitoring_elapsed);

			create_event_notification(local_conn,
									  &config_file_options,
									  config_file_options.node_id,
									  "repmgrd_local_reconnect",
									  true,
									  event_details.data);

			log_notice("%s", event_details.data);
			termPQExpBuffer(&event_details);
		}

		return true;
	}

	/* the node is now a standby */

	{
		PQExpBufferData event_details;
		initPQExpBuffer(&event_details);

		if (degraded_monitoring_elapsed != NO_DEGRADED_MONITORING_ELAPSED)
		{
			appendPQExpBuffer(&event_details,
							  _("reconnected to node after %i seconds, node is now a standby, switching to standby monitoring"),
							  degraded_monitoring_elapsed);
		}
		else
		{
			appendPQExpBufferStr(&event_details,
								 _("node is now a standby, switching to standby monitoring"));
		}

		log_notice("%s", event_details.data);
		termPQExpBuffer(&event_details);
	}

	primary_node_id = UNKNOWN_NODE_ID;

	new_primary_conn = get_primary_connection_quiet(local_conn, &primary_node_id, NULL);

	if (PQstatus(new_primary_conn) != CONNECTION_OK)
	{
		if (primary_node_id == UNKNOWN_NODE_ID)
		{
			log_warning(_("unable to determine a new primary node"));
		}
		else
		{
			log_warning(_("unable to connect to new primary node %i"), primary_node_id);
			log_detail("\n%s", PQerrorMessage(new_primary_conn));
		}

		close_connection(&new_primary_conn);

		/* "true" to indicate repmgrd should continue monitoring in degraded state */
		return true;
	}

	log_debug("primary node ID is now %i", primary_node_id);

	record_status = get_node_record(new_primary_conn, config_file_options.node_id, &local_node_info);

	/*
	 * If, for whatever reason, the new primary has no record of this node,
	 * we won't be able to perform proper monitoring. In that case
	 * terminate and let the user sort out the situation.
	 */
	if (record_status == RECORD_NOT_FOUND)
	{
		PQExpBufferData event_details;

		initPQExpBuffer(&event_details);

		appendPQExpBuffer(&event_details,
						  _("no metadata record found for this node on current primary %i"),
						  primary_node_id);

		log_error("%s", event_details.data);
		log_hint(_("check that 'repmgr (primary|standby) register' was executed for this node"));

		close_connection(&new_primary_conn);

		create_event_notification(NULL,
								  &config_file_options,
								  config_file_options.node_id,
								  "repmgrd_shutdown",
								  false,
								  event_details.data);
		termPQExpBuffer(&event_details);

		terminate(ERR_BAD_CONFIG);
	}

	log_debug("node %i is registered with type = %s",
			  config_file_options.node_id,
			  get_node_type_string(local_node_info.type));

	/*
	 * node has recovered but metadata not updated - we can do that ourselves,
	 */
	if (local_node_info.type == PRIMARY)
	{
		log_notice(_("node \"%s\" (ID: %i) still registered as primary, setting to standby"),
				   config_file_options.node_name,
				   config_file_options.node_id);

		if (update_node_record_set_active_standby(new_primary_conn, config_file_options.node_id) == false)
		{
			resume_monitoring = false;
		}
		else
		{
			/* refresh our copy of the node record from the primary */
			record_status = get_node_record(new_primary_conn, config_file_options.node_id, &local_node_info);

			/* this is unlikley to happen */
			if (record_status != RECORD_FOUND)
			{
				log_warning(_("unable to retrieve local node record from primary node %i"), primary_node_id);
				resume_monitoring = false;
			}
		}
	}

	if (resume_monitoring == true)
	{
		PQExpBufferData event_details;
		initPQExpBuffer(&event_details);

		if (degraded_monitoring_elapsed != NO_DEGRADED_MONITORING_ELAPSED)
		{
			monitoring_state = MS_NORMAL;

			log_notice(_("former primary has been restored as standby after %i seconds, updating node record and resuming monitoring"),
					   degraded_monitoring_elapsed);

			appendPQExpBuffer(&event_details,
							  _("node restored as standby after %i seconds, monitoring connection to upstream node %i"),
							  degraded_monitoring_elapsed,
							  local_node_info.upstream_node_id);
		}
		else
		{
			if (local_node_info.upstream_node_id == UNKNOWN_NODE_ID)
			{
				/*
				 * If upstream_node_id is not set, it's possible that following a switchover
				 * of some kind (possibly forced in some way), the updated node record has
				 * not yet propagated to the local node. In this case however we can safely
				 * assume we're monitoring the primary.
				 */

				appendPQExpBuffer(&event_details,
								  _("node has become a standby, monitoring connection to primary node %i"),
								  primary_node_id);
			}
			else
			{
				appendPQExpBuffer(&event_details,
								  _("node has become a standby, monitoring connection to upstream node %i"),
								  local_node_info.upstream_node_id);
			}
		}

		create_event_notification(new_primary_conn,
								  &config_file_options,
								  config_file_options.node_id,
								  "repmgrd_standby_reconnect",
								  true,
								  event_details.data);

		termPQExpBuffer(&event_details);

		close_connection(&new_primary_conn);

		/* restart monitoring as standby */
		return false;
	}

	/* continue monitoring as before */
	return true;
}


static void
check_primary_child_nodes(t_child_node_info_list *local_child_nodes)
{
	NodeInfoList db_child_node_records = T_NODE_INFO_LIST_INITIALIZER;
	NodeInfoListCell *cell;
	/* lists for newly attached and missing nodes */
	t_child_node_info_list disconnected_child_nodes = T_CHILD_NODE_INFO_LIST_INITIALIZER;
	t_child_node_info_list reconnected_child_nodes = T_CHILD_NODE_INFO_LIST_INITIALIZER;
	t_child_node_info_list new_child_nodes = T_CHILD_NODE_INFO_LIST_INITIALIZER;

	bool success = get_child_nodes(local_conn, config_file_options.node_id, &db_child_node_records);

	if (!success)
	{
		/* unlikely this will happen, but if it does, we'll try again next time round */
		log_error(_("unable to retrieve list of child nodes"));
		return;
	}

	if (db_child_node_records.node_count == 0)
	{
		/* no registered child nodes - nothing to do */
		return;
	}

	/*
	 * compare DB records with our internal list;
	 * this will tell us about:
	 *  - previously known nodes and their current status
	 *  - newly registered nodes we didn't know about
	 *
	 * We'll need to compare the opposite way to check for nodes
	 * which are in the internal list, but which have now vanished
	 */
	for (cell = db_child_node_records.head; cell; cell = cell->next)
	{
		t_child_node_info *local_child_node_rec;
		bool local_child_node_rec_found = false;


		/*
		 * witness will not be "attached" in the normal way
		 */
		if (cell->node_info->type == WITNESS)
		{
			check_witness_attached(cell->node_info, false);
		}

		log_debug("child node: %i; attached: %s",
				  cell->node_info->node_id,
				  cell->node_info->attached == NODE_ATTACHED ? "yes" : "no");

		for (local_child_node_rec = local_child_nodes->head; local_child_node_rec; local_child_node_rec = local_child_node_rec->next)
		{
			if (local_child_node_rec->node_id == cell->node_info->node_id)
			{
				local_child_node_rec_found = true;
				break;
			}
		}

		if (local_child_node_rec_found == true)
		{
			/* our node record shows node attached, DB record indicates detached */
			if (local_child_node_rec->attached == NODE_ATTACHED && cell->node_info->attached == NODE_DETACHED)
			{
				t_child_node_info *detached_child_node;

				local_child_node_rec->attached = NODE_DETACHED;
				INSTR_TIME_SET_CURRENT(local_child_node_rec->detached_time);

				detached_child_node = append_child_node_record(&disconnected_child_nodes,
															   local_child_node_rec->node_id,
															   local_child_node_rec->node_name,
															   local_child_node_rec->type,
															   NODE_DETACHED);
				detached_child_node->detached_time = local_child_node_rec->detached_time;
			}
			/* our node record shows node detached, DB record indicates attached */
			else if (local_child_node_rec->attached == NODE_DETACHED && cell->node_info->attached == NODE_ATTACHED)
			{
				t_child_node_info *attached_child_node;

				local_child_node_rec->attached = NODE_ATTACHED;

				attached_child_node = append_child_node_record(&reconnected_child_nodes,
															   local_child_node_rec->node_id,
															   local_child_node_rec->node_name,
															   local_child_node_rec->type,
															   NODE_ATTACHED);
				attached_child_node->detached_time = local_child_node_rec->detached_time;
				INSTR_TIME_SET_ZERO(local_child_node_rec->detached_time);
			}
			else if (local_child_node_rec->attached == NODE_ATTACHED_UNKNOWN  && cell->node_info->attached == NODE_ATTACHED)
			{
				local_child_node_rec->attached = NODE_ATTACHED;

				append_child_node_record(&new_child_nodes,
										 local_child_node_rec->node_id,
										 local_child_node_rec->node_name,
										 local_child_node_rec->type,
										 NODE_ATTACHED);
			}
		}
		else
		{
			/* node we didn't know about before */

			NodeAttached attached = cell->node_info->attached;

			/*
			 * node registered but not attached - set state to "UNKNOWN"
			 * to prevent a bogus "reattach" event being generated
			 */
			if (attached == NODE_DETACHED)
				attached = NODE_ATTACHED_UNKNOWN;

			(void) append_child_node_record(local_child_nodes,
											cell->node_info->node_id,
											cell->node_info->node_name,
											cell->node_info->type,
											attached);
			(void) append_child_node_record(&new_child_nodes,
											cell->node_info->node_id,
											cell->node_info->node_name,
											cell->node_info->type,
											attached);
		}
	}

	/*
	 * Check if any nodes in local list are no longer in list returned
	 * from database.
	 */
	{
		t_child_node_info *local_child_node_rec;
		bool db_node_rec_found = false;

		for (local_child_node_rec = local_child_nodes->head; local_child_node_rec; local_child_node_rec = local_child_node_rec->next)
		{
			for (cell = db_child_node_records.head; cell; cell = cell->next)
			{
				if (cell->node_info->node_id == local_child_node_rec->node_id)
				{
					db_node_rec_found = true;
					break;
				}
			}

			if (db_node_rec_found == false)
			{
				log_notice(_("%s node \"%s\" (ID: %i) is no longer connected or registered"),
						   get_node_type_string(local_child_node_rec->type),
						   local_child_node_rec->node_name,
						   local_child_node_rec->node_id);
				remove_child_node_record(local_child_nodes, local_child_node_rec->node_id);
			}
		}
	}

	/* generate "child_node_disconnect" events */
	if (disconnected_child_nodes.node_count > 0)
	{
		t_child_node_info *child_node_rec;
		for (child_node_rec = disconnected_child_nodes.head; child_node_rec; child_node_rec = child_node_rec->next)
		{
			PQExpBufferData event_details;
			initPQExpBuffer(&event_details);
			appendPQExpBuffer(&event_details,
							  _("%s node \"%s\" (ID: %i) has disconnected"),
							  get_node_type_string(child_node_rec->type),
							  child_node_rec->node_name,
							  child_node_rec->node_id);
			log_notice("%s",  event_details.data);

			create_event_notification(local_conn,
									  &config_file_options,
									  local_node_info.node_id,
									  "child_node_disconnect",
									  true,
									  event_details.data);

			termPQExpBuffer(&event_details);
		}
	}

	/* generate "child_node_reconnect" events */
	if (reconnected_child_nodes.node_count > 0)
	{
		t_child_node_info *child_node_rec;
		for (child_node_rec = reconnected_child_nodes.head; child_node_rec; child_node_rec = child_node_rec->next)
		{
			PQExpBufferData event_details;
			initPQExpBuffer(&event_details);
			appendPQExpBuffer(&event_details,
							  _("%s node \"%s\" (ID: %i) has reconnected after %i seconds"),
							  get_node_type_string(child_node_rec->type),
							  child_node_rec->node_name,
							  child_node_rec->node_id,
							  calculate_elapsed( child_node_rec->detached_time ));
			log_notice("%s",  event_details.data);

			create_event_notification(local_conn,
									  &config_file_options,
									  local_node_info.node_id,
									  "child_node_reconnect",
									  true,
									  event_details.data);

			termPQExpBuffer(&event_details);
		}
	}

	/* generate "child_node_new_connect" events */
	if (new_child_nodes.node_count > 0)
	{
		t_child_node_info *child_node_rec;
		for (child_node_rec = new_child_nodes.head; child_node_rec; child_node_rec = child_node_rec->next)
		{
			PQExpBufferData event_details;
			initPQExpBuffer(&event_details);
			appendPQExpBuffer(&event_details,
							  _("new %s \"%s\" (ID: %i) has connected"),
							  get_node_type_string(child_node_rec->type),
							  child_node_rec->node_name,
							  child_node_rec->node_id);
			log_notice("%s",  event_details.data);

			create_event_notification(local_conn,
									  &config_file_options,
									  local_node_info.node_id,
									  "child_node_new_connect",
									  true,
									  event_details.data);

			termPQExpBuffer(&event_details);
		}
	}


	if (config_file_options.child_nodes_disconnect_command[0] != '\0')
	{
		bool repmgrd_paused = repmgrd_is_paused(local_conn);

		if (repmgrd_paused == false)
		{
			/* check criteria for execution, and execute if criteria met */
			execute_child_nodes_disconnect_command(&db_child_node_records, local_child_nodes);
		}
	}

	clear_child_node_info_list(&disconnected_child_nodes);
	clear_child_node_info_list(&reconnected_child_nodes);
	clear_child_node_info_list(&new_child_nodes);

	clear_node_info_list(&db_child_node_records);
}


void
execute_child_nodes_disconnect_command(NodeInfoList *db_child_node_records, t_child_node_info_list *local_child_nodes)
{
	/*
	 * script will only be executed if the number of attached
	 * standbys is lower than this number
	 */
	int min_required_connected_count = 1;
	int connected_count = 0;
	NodeInfoListCell *cell;

	/*
	 * Calculate minimum number of nodes which need to be connected
	 * (if the total falls below that, "child_nodes_disconnect_command"
	 * will be executed)
	 */

	if (config_file_options.child_nodes_connected_min_count > 0)
	{
		min_required_connected_count = config_file_options.child_nodes_connected_min_count;
	}
	else if (config_file_options.child_nodes_disconnect_min_count > 0)
	{
		int child_node_count = db_child_node_records->node_count;

		if (config_file_options.child_nodes_connected_include_witness == false)
		{
			/* reduce total, if witness server in child node list */
			for (cell = db_child_node_records->head; cell; cell = cell->next)
			{
				if (cell->node_info->type == WITNESS)
				{
					child_node_count--;
					break;
				}
			}
		}

		min_required_connected_count =
			(child_node_count - config_file_options.child_nodes_disconnect_min_count)
			+ 1;
	}

	/* calculate number of connected child nodes */
	for (cell = db_child_node_records->head; cell; cell = cell->next)
	{
		/* exclude witness server from total, if necessay */
		if (config_file_options.child_nodes_connected_include_witness == false &&
			cell->node_info->type == WITNESS)
			continue;

		if (cell->node_info->attached == NODE_ATTACHED)
			connected_count ++;
	}

	log_debug("connected: %i; min required: %i",
			  connected_count,
			  min_required_connected_count);

	if (connected_count < min_required_connected_count)
	{
		log_notice(_("%i (of %i) child nodes are connected, but at least %i child nodes required"),
				   connected_count,
				   db_child_node_records->node_count,
				   min_required_connected_count);

		if (child_nodes_disconnect_command_executed == false)
		{
			t_child_node_info *child_node_rec;

			/* set these for informative purposes */
			int most_recently_disconnected_node_id = UNKNOWN_NODE_ID;
			int most_recently_disconnected_elapsed = -1;

			bool most_recent_disconnect_below_threshold = false;
			instr_time  current_time_base;

			INSTR_TIME_SET_CURRENT(current_time_base);

			for (child_node_rec = local_child_nodes->head; child_node_rec; child_node_rec = child_node_rec->next)
			{
				instr_time  current_time = current_time_base;
				int seconds_since_detached;

				/* exclude witness server from calculatin if neccessary */
				if (config_file_options.child_nodes_connected_include_witness == false &&
					child_node_rec->type == WITNESS)
					continue;

				if (child_node_rec->attached != NODE_DETACHED)
					continue;

				INSTR_TIME_SUBTRACT(current_time, child_node_rec->detached_time);
				seconds_since_detached = (int) INSTR_TIME_GET_DOUBLE(current_time);

				if (seconds_since_detached < config_file_options.child_nodes_disconnect_timeout)
				{
					most_recent_disconnect_below_threshold = true;
				}

				if (most_recently_disconnected_node_id == UNKNOWN_NODE_ID)
				{
					most_recently_disconnected_node_id = child_node_rec->node_id;
					most_recently_disconnected_elapsed = seconds_since_detached;
				}
				else if (seconds_since_detached < most_recently_disconnected_elapsed)
				{
					most_recently_disconnected_node_id = child_node_rec->node_id;
					most_recently_disconnected_elapsed = seconds_since_detached;
				}
			}


			if (most_recent_disconnect_below_threshold == false && most_recently_disconnected_node_id != UNKNOWN_NODE_ID)
			{
				char parsed_child_nodes_disconnect_command[MAXPGPATH];
				int child_nodes_disconnect_command_result;
				PQExpBufferData event_details;
				bool success = true;

				parse_child_nodes_disconnect_command(parsed_child_nodes_disconnect_command,
													 config_file_options.child_nodes_disconnect_command,
													 local_node_info.node_id);

				log_info(_("most recently detached child node was %i (ca. %i seconds ago), triggering \"child_nodes_disconnect_command\""),
						 most_recently_disconnected_node_id,
						 most_recently_disconnected_elapsed);

				log_info(_("\"child_nodes_disconnect_command\" is:\n  \"%s\""),
						 parsed_child_nodes_disconnect_command);

				child_nodes_disconnect_command_result = system(parsed_child_nodes_disconnect_command);

				initPQExpBuffer(&event_details);

				if (child_nodes_disconnect_command_result != 0)
				{
					success = false;

					appendPQExpBufferStr(&event_details,
										 _("unable to execute \"child_nodes_disconnect_command\""));

					log_error("%s", event_details.data);
				}
				else
				{
					appendPQExpBufferStr(&event_details,
										 _("\"child_nodes_disconnect_command\" successfully executed"));

					log_info("%s", event_details.data);
				}

				create_event_notification(local_conn,
										  &config_file_options,
										  local_node_info.node_id,
										  "child_nodes_disconnect_command",
										  success,
										  event_details.data);

				termPQExpBuffer(&event_details);

				child_nodes_disconnect_command_executed = true;
			}
			else if (most_recently_disconnected_node_id != UNKNOWN_NODE_ID)
			{
				log_info(_("most recently detached child node was %i (ca. %i seconds ago), not triggering \"child_nodes_disconnect_command\""),
						 most_recently_disconnected_node_id,
						 most_recently_disconnected_elapsed);
				log_detail(_("\"child_nodes_disconnect_timeout\" set to %i seconds"),
						   config_file_options.child_nodes_disconnect_timeout);
			}
			else
			{
				log_info(_("no child nodes have detached since repmgrd startup"));
			}
		}
		else
		{
			log_info(_("\"child_nodes_disconnect_command\" was previously executed, taking no action"));
		}
	}
	else
	{
		/*
		 * "child_nodes_disconnect_command" was executed, but for whatever reason
		 * enough child nodes have returned to clear the threshold; in that case reset
		 * the executed flag so we can execute the command again, if necessary
		 */
		if (child_nodes_disconnect_command_executed == true)
		{
			log_notice(_("%i (of %i) child nodes are now connected, meeting minimum requirement of %i child nodes"),
					   connected_count,
					   db_child_node_records->node_count,
					   min_required_connected_count);
			child_nodes_disconnect_command_executed = false;
		}
	}
}


/*
 * repmgrd running on a standby server
 */
void
monitor_streaming_standby(void)
{
	RecordStatus record_status;
	instr_time	log_status_interval_start;

	MonitoringState local_monitoring_state = MS_NORMAL;
	instr_time	local_degraded_monitoring_start;

	int last_known_upstream_node_id = UNKNOWN_NODE_ID;

	log_debug("monitor_streaming_standby()");

	reset_node_voting_status();

	INSTR_TIME_SET_ZERO(last_monitoring_update);

	/*
	 * If no upstream node id is specified in the metadata, we'll try and
	 * determine the current cluster primary in the assumption we should
	 * connect to that by default.
	 */
	if (local_node_info.upstream_node_id == UNKNOWN_NODE_ID)
	{
		upstream_conn = get_primary_connection(local_conn, &local_node_info.upstream_node_id, NULL);

		/*
		 * Terminate if there doesn't appear to be an active cluster primary.
		 * There could be one or more nodes marked as inactive primaries, and
		 * one of them could actually be a primary, but we can't sensibly
		 * monitor in that state.
		 */
		if (local_node_info.upstream_node_id == NODE_NOT_FOUND)
		{
			log_error(_("unable to determine an active primary for this cluster, terminating"));
			terminate(ERR_BAD_CONFIG);
		}

		log_debug("upstream node ID determined as %i", local_node_info.upstream_node_id);

		(void) get_node_record(upstream_conn, local_node_info.upstream_node_id, &upstream_node_info);
	}
	else
	{
		log_debug("upstream node ID in local node record is %i", local_node_info.upstream_node_id);

		record_status = get_node_record(local_conn, local_node_info.upstream_node_id, &upstream_node_info);

		/*
		 * Terminate if we can't find the record for the node we're supposed to
		 * monitor. This is a "fix-the-config" situation, not a lot else we can
		 * do.
		 */
		if (record_status == RECORD_NOT_FOUND)
		{
			log_error(_("no record found for upstream node (ID: %i), terminating"),
					  local_node_info.upstream_node_id);
			log_hint(_("ensure the upstream node is registered correctly"));

			terminate(ERR_DB_CONN);
		}
		else if (record_status == RECORD_ERROR)
		{
			log_error(_("unable to retrieve record for upstream node (ID: %i), terminating"),
					  local_node_info.upstream_node_id);

			terminate(ERR_DB_CONN);
		}

		log_debug("connecting to upstream node %i: \"%s\"", upstream_node_info.node_id, upstream_node_info.conninfo);

		upstream_conn = establish_db_connection(upstream_node_info.conninfo, false);
	}


	/*
	 * Upstream node must be running at repmgrd startup.
	 *
	 * We could possibly have repmgrd skip to degraded monitoring mode until
	 * it comes up, but there doesn't seem to be much point in doing that.
	 */
	if (PQstatus(upstream_conn) != CONNECTION_OK)
	{
		close_connection(&upstream_conn);
		log_error(_("unable connect to upstream node (ID: %i), terminating"),
				  local_node_info.upstream_node_id);
		log_hint(_("upstream node must be running before repmgrd can start"));

		terminate(ERR_DB_CONN);
	}

	record_status = get_node_record(upstream_conn, local_node_info.node_id, &local_node_info);

	if (upstream_node_info.node_id == local_node_info.node_id)
	{
		close_connection(&upstream_conn);

		return;
	}

	last_known_upstream_node_id = local_node_info.upstream_node_id;

	/*
	 * refresh upstream node record from upstream node, so it's as up-to-date
	 * as possible
	 */
	record_status = get_node_record(upstream_conn, upstream_node_info.node_id, &upstream_node_info);

	if (upstream_node_info.type == STANDBY)
	{
		log_debug("upstream node is standby, connecting to primary");
		/*
		 * Currently cascaded standbys need to be able to connect to the
		 * primary. We could possibly add a limited connection mode for cases
		 * where this isn't possible, but that will complicate things further.
		 */
		primary_conn = establish_primary_db_connection(upstream_conn, false);

		if (PQstatus(primary_conn) != CONNECTION_OK)
		{
			close_connection(&primary_conn);

			log_error(_("unable to connect to primary node"));
			log_hint(_("ensure the primary node is reachable from this node"));

			terminate(ERR_DB_CONN);
		}

		log_verbose(LOG_DEBUG, "connected to primary");
	}
	else
	{
		log_debug("upstream node is primary");
		primary_conn = upstream_conn;
	}

	/*
	 * It's possible monitoring has been restarted after some outage which
	 * resulted in the local node being marked as inactive; if so mark it
	 * as active again.
	 */
	if (local_node_info.active == false)
	{
		if (update_node_record_set_active(primary_conn, local_node_info.node_id, true) == true)
		{
			PQExpBufferData event_details;

			initPQExpBuffer(&event_details);

			local_node_info.active = true;
		}
	}

	if (PQstatus(primary_conn) == CONNECTION_OK)
	{
		primary_node_id = get_primary_node_id(primary_conn);
		log_debug("primary_node_id is %i", primary_node_id);
	}
	else
	{
		primary_node_id = get_primary_node_id(local_conn);
		log_debug("primary_node_id according to local records is %i", primary_node_id);
	}


	/* Log startup event */
	if (startup_event_logged == false)
	{
		PQExpBufferData event_details;

		initPQExpBuffer(&event_details);

		appendPQExpBuffer(&event_details,
						  _("monitoring connection to upstream node \"%s\" (ID: %i)"),
						  upstream_node_info.node_name,
						  upstream_node_info.node_id);

		create_event_notification(primary_conn,
								  &config_file_options,
								  config_file_options.node_id,
								  "repmgrd_start",
								  true,
								  event_details.data);

		startup_event_logged = true;

		log_info("%s", event_details.data);

		termPQExpBuffer(&event_details);
	}

	monitoring_state = MS_NORMAL;
	INSTR_TIME_SET_CURRENT(log_status_interval_start);
	upstream_node_info.node_status = NODE_STATUS_UP;

	while (true)
	{
		bool upstream_check_result;

		log_verbose(LOG_DEBUG, "checking %s", upstream_node_info.conninfo);

		if (upstream_node_info.type == PRIMARY)
		{
			upstream_check_result = check_upstream_connection(&upstream_conn, upstream_node_info.conninfo, &primary_conn);
		}
		else
		{
			upstream_check_result = check_upstream_connection(&upstream_conn, upstream_node_info.conninfo, NULL);
		}

		if (upstream_check_result == true)
		{
			set_upstream_last_seen(local_conn, upstream_node_info.node_id);
		}
		else
		{
			/* upstream node is down, we were expecting it to be up */
			if (upstream_node_info.node_status == NODE_STATUS_UP)
			{
				instr_time	upstream_node_unreachable_start;

				INSTR_TIME_SET_CURRENT(upstream_node_unreachable_start);


				upstream_node_info.node_status = NODE_STATUS_UNKNOWN;

				{
					PQExpBufferData event_details;
					initPQExpBuffer(&event_details);

					appendPQExpBuffer(&event_details,
									  _("unable to connect to upstream node \"%s\" (ID: %i)"),
									  upstream_node_info.node_name, upstream_node_info.node_id);

					/* TODO: possibly add pre-action event here */
					if (upstream_node_info.type == STANDBY)
					{
						create_event_record(primary_conn,
											&config_file_options,
											config_file_options.node_id,
											"repmgrd_upstream_disconnect",
											true,
											event_details.data);
					}
					else
					{
						/* primary connection lost - script notification only */
						create_event_record(NULL,
											&config_file_options,
											config_file_options.node_id,
											"repmgrd_upstream_disconnect",
											true,
											event_details.data);
					}

					log_warning("%s", event_details.data);
					termPQExpBuffer(&event_details);
				}

				/*
				 * if local node is unreachable, make a last-minute attempt to reconnect
				 * before continuing with the failover process
				 */

				if (PQstatus(local_conn) != CONNECTION_OK)
				{
					check_connection(&local_node_info, &local_conn);
				}


				if (upstream_node_info.type == PRIMARY)
				{
					primary_node_id = try_primary_reconnect(&upstream_conn, local_conn, &upstream_node_info);

					/*
					 * We were notified by the the primary during our own reconnection
					 * retry phase, in which case we can leave the failover process early
					 * and connect to the new primary.
					 */
					if (primary_node_id == ELECTION_RERUN_NOTIFICATION)
					{
						if (do_primary_failover() == true)
						{
							primary_node_id = get_primary_node_id(local_conn);
							return;
						}
					}
					if (primary_node_id != UNKNOWN_NODE_ID && primary_node_id != ELECTION_RERUN_NOTIFICATION)
					{
						follow_new_primary(primary_node_id);
						return;
					}
				}
				else
				{
					try_reconnect(&upstream_conn, &upstream_node_info);
				}

				/* Upstream node has recovered - log and continue */
				if (upstream_node_info.node_status == NODE_STATUS_UP)
				{
					int			upstream_node_unreachable_elapsed = calculate_elapsed(upstream_node_unreachable_start);
					PQExpBufferData event_details;

					initPQExpBuffer(&event_details);

					appendPQExpBuffer(&event_details,
									  _("reconnected to upstream node after %i seconds"),
									  upstream_node_unreachable_elapsed);
					log_notice("%s", event_details.data);

					if (upstream_node_info.type == PRIMARY)
					{
						primary_conn = upstream_conn;

						if (get_recovery_type(primary_conn) == RECTYPE_STANDBY)
						{
							ExecStatusType ping_result;

							/*
							 * we're returning at the end of this block and no longer require the
							 * event details buffer
							 */
							termPQExpBuffer(&event_details);

							log_notice(_("current upstream node \"%s\" (ID: %i) is not primary, restarting monitoring"),
									   upstream_node_info.node_name, upstream_node_info.node_id);

							close_connection(&upstream_conn);

							local_node_info.upstream_node_id = UNKNOWN_NODE_ID;

							/* check local connection */
							ping_result = connection_ping(local_conn);

							if (ping_result != PGRES_TUPLES_OK)
							{
								int i;

								close_connection(&local_conn);

								for (i = 0; i < config_file_options.repmgrd_standby_startup_timeout; i++)
								{
									local_conn = establish_db_connection(local_node_info.conninfo, false);

									if (PQstatus(local_conn) == CONNECTION_OK)
										break;

									close_connection(&local_conn);

									log_debug("sleeping 1 second; %i of %i attempts to reconnect to local node",
											  i + 1,
											  config_file_options.repmgrd_standby_startup_timeout);
									sleep(1);
								}
							}

							return;
						}
					}

					create_event_notification(primary_conn,
											  &config_file_options,
											  config_file_options.node_id,
											  "repmgrd_upstream_reconnect",
											  true,
											  event_details.data);
					termPQExpBuffer(&event_details);

					goto loop;
				}


				/* upstream is still down after reconnect attempt(s) */
				if (upstream_node_info.node_status == NODE_STATUS_DOWN)
				{
					bool		failover_done = false;

					if (PQstatus(local_conn) == CONNECTION_OK && repmgrd_is_paused(local_conn))
					{
						log_notice(_("repmgrd on this node is paused"));
						log_detail(_("no failover will be carried out"));
						log_hint(_("execute \"repmgr service unpause\" to resume normal failover mode"));
						monitoring_state = MS_DEGRADED;
						INSTR_TIME_SET_CURRENT(degraded_monitoring_start);
					}
					else
					{
						if (upstream_node_info.type == PRIMARY)
						{
							failover_done = do_primary_failover();
						}
						else if (upstream_node_info.type == STANDBY)
						{

							failover_done = do_upstream_standby_failover();

							if (failover_done == false)
							{
								monitoring_state = MS_DEGRADED;
								INSTR_TIME_SET_CURRENT(degraded_monitoring_start);
							}
						}

						/*
						 * XXX it's possible it will make sense to return in all
						 * cases to restart monitoring
						 */
						if (failover_done == true)
						{
							primary_node_id = get_primary_node_id(local_conn);
							return;
						}
					}
				}
			}
		}

		if (monitoring_state == MS_DEGRADED)
		{
			int			degraded_monitoring_elapsed = calculate_elapsed(degraded_monitoring_start);
			bool		upstream_check_result;

			if (config_file_options.degraded_monitoring_timeout > 0
				&& degraded_monitoring_elapsed > config_file_options.degraded_monitoring_timeout)
			{
				PQExpBufferData event_details;

				initPQExpBuffer(&event_details);

				appendPQExpBuffer(&event_details,
								  _("degraded monitoring timeout (%i seconds) exceeded, terminating"),
								  degraded_monitoring_elapsed);

				log_notice("%s", event_details.data);

				create_event_notification(NULL,
										  &config_file_options,
										  config_file_options.node_id,
										  "repmgrd_shutdown",
										  true,
										  event_details.data);

				termPQExpBuffer(&event_details);
				terminate(ERR_MONITORING_TIMEOUT);
			}

			log_debug("monitoring upstream node %i in degraded state for %i seconds",
					  upstream_node_info.node_id,
					  degraded_monitoring_elapsed);


			if (upstream_node_info.type == PRIMARY)
			{
				upstream_check_result = check_upstream_connection(&upstream_conn, upstream_node_info.conninfo, &primary_conn);
			}
			else
			{
				upstream_check_result = check_upstream_connection(&upstream_conn, upstream_node_info.conninfo, NULL);
			}

			if (upstream_check_result == true)
			{
				if (config_file_options.connection_check_type != CHECK_QUERY)
					upstream_conn = establish_db_connection(upstream_node_info.conninfo, false);

				if (PQstatus(upstream_conn) == CONNECTION_OK)
				{
					PQExpBufferData event_details;

					log_debug("upstream node %i has recovered",
							  upstream_node_info.node_id);

					/* XXX check here if upstream is still primary */
					/*
					 * -> will be a problem if another node was promoted in
					 * the meantime
					 */
					/* and upstream is now former primary */
					/* XXX scan other nodes to see if any has become primary */

					upstream_node_info.node_status = NODE_STATUS_UP;
					monitoring_state = MS_NORMAL;

					if (upstream_node_info.type == PRIMARY)
					{
						primary_conn = upstream_conn;
					}
					else
					{
						if (primary_conn != NULL && PQstatus(primary_conn) != CONNECTION_OK)
						{
							close_connection(&primary_conn);
						}

						if (primary_conn == NULL)
						{
							primary_conn = establish_primary_db_connection(upstream_conn, false);
						}
					}

					initPQExpBuffer(&event_details);

					appendPQExpBuffer(&event_details,
									  _("reconnected to upstream node \"%s\" (ID: %i) after %i seconds, resuming monitoring"),
									  upstream_node_info.node_name,
									  upstream_node_info.node_id,
									  degraded_monitoring_elapsed);

					create_event_notification(primary_conn,
											  &config_file_options,
											  config_file_options.node_id,
											  "repmgrd_upstream_reconnect",
											  true,
											  event_details.data);

					log_notice("%s", event_details.data);
					termPQExpBuffer(&event_details);

					goto loop;
				}
			}
			else
			{
				/*
				 * unable to connect to former primary - check if another node
				 * has been promoted
				 */

				/* local node has been promoted */
				if (get_recovery_type(local_conn) == RECTYPE_PRIMARY)
				{
					log_notice(_("local node is primary, checking local node state"));

					/*
					 * It's possible the promote command timed out, but the promotion itself
					 * succeeded. In this case failover state will be FAILOVER_STATE_PROMOTION_FAILED;
					 * we can update the node record ourselves and resume primary monitoring.
					 */
					if (failover_state == FAILOVER_STATE_PROMOTION_FAILED)
					{
						int			degraded_monitoring_elapsed;
						int			former_upstream_node_id = local_node_info.upstream_node_id;
						NodeInfoList sibling_nodes = T_NODE_INFO_LIST_INITIALIZER;
						PQExpBufferData event_details;

						update_node_record_set_primary(local_conn,  local_node_info.node_id);
						record_status = get_node_record(local_conn, local_node_info.node_id, &local_node_info);

						degraded_monitoring_elapsed = calculate_elapsed(degraded_monitoring_start);

						log_notice(_("resuming monitoring as primary node after %i seconds"),
								   degraded_monitoring_elapsed);

						initPQExpBuffer(&event_details);
						appendPQExpBufferStr(&event_details,
											 _("promotion command failed but promotion completed successfully"));
						create_event_notification(local_conn,
												  &config_file_options,
												  local_node_info.node_id,
												  "repmgrd_failover_promote",
												  true,
												  event_details.data);

						termPQExpBuffer(&event_details);

						/* notify former siblings that they should now follow this node */
						get_active_sibling_node_records(local_conn,
														local_node_info.node_id,
														former_upstream_node_id,
														&sibling_nodes);
						notify_followers(&sibling_nodes, local_node_info.node_id);

						clear_node_info_list(&sibling_nodes);

						/* this will restart monitoring in primary mode */
						monitoring_state = MS_NORMAL;
						return;
					}

					/*
					 * There may be a delay between the node being promoted
					 * and the local record being updated, so if the node
					 * record still shows it as a standby, do nothing, we'll
					 * catch the update during the next loop. (e.g. node was
					 * manually promoted) we'll do nothing, as the repmgr
					 * metadata is now out-of-sync. If it does get fixed,
					 * we'll catch it here on a future iteration.
					 */

					/* refresh own internal node record */
					record_status = refresh_node_record(local_conn, local_node_info.node_id, &local_node_info);

					if (local_node_info.type == PRIMARY)
					{
						int			degraded_monitoring_elapsed = calculate_elapsed(degraded_monitoring_start);

						log_notice(_("resuming monitoring as primary node after %i seconds"),
								   degraded_monitoring_elapsed);

						/* this will restart monitoring in primary mode */
						monitoring_state = MS_NORMAL;
						return;
					}
				}


				if (config_file_options.failover == FAILOVER_AUTOMATIC && repmgrd_is_paused(local_conn) == false)
				{
					NodeInfoList sibling_nodes = T_NODE_INFO_LIST_INITIALIZER;

					get_active_sibling_node_records(local_conn,
													local_node_info.node_id,
													local_node_info.upstream_node_id,
													&sibling_nodes);

					if (sibling_nodes.node_count > 0)
					{
						NodeInfoListCell *cell;
						t_node_info *follow_node_info = NULL;

						log_debug("scanning %i node records to detect new primary...", sibling_nodes.node_count);
						for (cell = sibling_nodes.head; cell; cell = cell->next)
						{
							/* skip local node check, we did that above */
							if (cell->node_info->node_id == local_node_info.node_id)
							{
								continue;
							}

							/* skip witness node - we can't possibly "follow" that */

							if (cell->node_info->type == WITNESS)
							{
								continue;
							}

							cell->node_info->conn = establish_db_connection(cell->node_info->conninfo, false);

							if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
							{
								close_connection(&cell->node_info->conn);
								log_debug("unable to connect to %i ... ", cell->node_info->node_id);
								close_connection(&cell->node_info->conn);
								continue;
							}

							if (get_recovery_type(cell->node_info->conn) == RECTYPE_PRIMARY)
							{
								follow_node_info = cell->node_info;
								close_connection(&cell->node_info->conn);
								break;
							}
							close_connection(&cell->node_info->conn);
						}

						if (follow_node_info != NULL)
						{
							log_info(_("node \"%s\" (node ID: %i) detected as primary"),
									 follow_node_info->node_name,
									 follow_node_info->node_id);
							follow_new_primary(follow_node_info->node_id);
						}
					}

					clear_node_info_list(&sibling_nodes);
				}
			}
		}

loop:

		/* emit "still alive" log message at regular intervals, if requested */
		if (config_file_options.log_status_interval > 0)
		{
			int			log_status_interval_elapsed = calculate_elapsed(log_status_interval_start);

			if (log_status_interval_elapsed >= config_file_options.log_status_interval)
			{
				PQExpBufferData monitoring_summary;

				initPQExpBuffer(&monitoring_summary);

				appendPQExpBuffer(&monitoring_summary,
								  _("node \"%s\" (ID: %i) monitoring upstream node \"%s\" (ID: %i) in %s state"),
								  local_node_info.node_name,
								  local_node_info.node_id,
								  upstream_node_info.node_name,
								  upstream_node_info.node_id,
								  print_monitoring_state(monitoring_state));

				if (config_file_options.failover == FAILOVER_MANUAL)
				{
					appendPQExpBufferStr(&monitoring_summary,
										 _(" (automatic failover disabled)"));
				}

				log_info("%s", monitoring_summary.data);
				termPQExpBuffer(&monitoring_summary);

				if (monitoring_state == MS_DEGRADED && config_file_options.failover == FAILOVER_AUTOMATIC)
				{
					if (PQstatus(local_conn) == CONNECTION_OK && repmgrd_is_paused(local_conn))
					{
						log_detail(_("repmgrd paused by administrator"));
						log_hint(_("execute \"repmgr service unpause\" to resume normal failover mode"));
					}
					else
					{
						log_detail(_("waiting for upstream or another primary to reappear"));
					}
				}

				/*
				 * Add update about monitoring updates.
				 *
				 * Note: with cascaded replication, it's possible we're still able to write
				 * monitoring history to the primary even if the upstream is still reachable.
				 */

				if (PQstatus(primary_conn) == CONNECTION_OK && config_file_options.monitoring_history == true)
				{
					if (INSTR_TIME_IS_ZERO(last_monitoring_update))
					{
						log_detail(_("no monitoring statistics have been written yet"));
					}
					else
					{
						log_detail(_("last monitoring statistics update was %i seconds ago"),
								   calculate_elapsed(last_monitoring_update));
					}
				}

				INSTR_TIME_SET_CURRENT(log_status_interval_start);
			}
		}

		if (PQstatus(primary_conn) == CONNECTION_OK && config_file_options.monitoring_history == true)
		{
			bool success = update_monitoring_history();

			if (success == false && PQstatus(primary_conn) != CONNECTION_OK && upstream_node_info.type == STANDBY)
			{
				primary_conn = establish_primary_db_connection(local_conn, false);

				if (PQstatus(primary_conn) == CONNECTION_OK)
				{
					(void)update_monitoring_history();
				}
			}
		}
		else
		{
			if (config_file_options.monitoring_history == true)
			{
				log_verbose(LOG_WARNING, _("monitoring_history requested but primary connection not available"));
			}

			/*
			 * if monitoring not in use, we'll need to ensure the local connection
			 * handle isn't stale
			 */
			(void) connection_ping(local_conn);
		}

		/*
		 * handle local node failure
		 *
		 * currently we'll just check the connection, and try to reconnect
		 *
		 * TODO: add timeout, after which we run in degraded state
		 */

		check_connection(&local_node_info, &local_conn);

		if (PQstatus(local_conn) != CONNECTION_OK)
		{
			if (local_node_info.active == true)
			{
				bool success = true;
				PQExpBufferData event_details;

				initPQExpBuffer(&event_details);

				local_node_info.active = false;

				appendPQExpBuffer(&event_details,
								  _("unable to connect to local node \"%s\" (ID: %i), marking inactive"),
								  local_node_info.node_name,
								  local_node_info.node_id);
				log_notice("%s", event_details.data);

				if (PQstatus(primary_conn) == CONNECTION_OK)
				{
					if (update_node_record_set_active(primary_conn, local_node_info.node_id, false) == false)
					{
						success = false;
						log_warning(_("unable to mark node \"%s\" (ID: %i) as inactive"),
									  local_node_info.node_name,
									  local_node_info.node_id);
					}
				}

				create_event_notification(primary_conn,
										  &config_file_options,
										  local_node_info.node_id,
										  "standby_failure",
										  success,
										  event_details.data);

				termPQExpBuffer(&event_details);
			}

			if (local_monitoring_state == MS_NORMAL)
			{
				log_info("entering degraded monitoring for the local node");
				local_monitoring_state = MS_DEGRADED;
				INSTR_TIME_SET_CURRENT(local_degraded_monitoring_start);
			}
		}
		else
		{
			int stored_local_node_id = UNKNOWN_NODE_ID;

			if (local_monitoring_state == MS_DEGRADED)
			{
				log_info(_("connection to local node recovered after %i seconds"),
						 calculate_elapsed(local_degraded_monitoring_start));
				local_monitoring_state = MS_NORMAL;

				/*
				 * Check if anything has changed since the local node came back on line;
				 * we may need to restart monitoring.
				 */
				refresh_node_record(local_conn, local_node_info.node_id, &local_node_info);

				if (last_known_upstream_node_id != local_node_info.upstream_node_id)
				{
					log_notice(_("upstream for local node \"%s\" (ID: %i) appears to have changed, restarting monitoring"),
							   local_node_info.node_name,
							   local_node_info.node_id);
					log_detail(_("currently monitoring upstream %i; new upstream is %i"),
							   last_known_upstream_node_id,
							   local_node_info.upstream_node_id);
					close_connection(&upstream_conn);
					return;
				}

				/*
				 *
				 */
				if (local_node_info.type != STANDBY)
				{
					log_notice(_("local node \"%s\" (ID: %i) is no longer a standby, restarting monitoring"),
							   local_node_info.node_name,
							   local_node_info.node_id);
					close_connection(&upstream_conn);
					return;
				}
			}

			/*
			 * If the local node was restarted, we'll need to reinitialise values
			 * stored in shared memory.
			 */
			stored_local_node_id = repmgrd_get_local_node_id(local_conn);

			if (stored_local_node_id == UNKNOWN_NODE_ID)
			{
				repmgrd_set_local_node_id(local_conn, config_file_options.node_id);
				repmgrd_set_pid(local_conn, getpid(), pid_file);
			}

			if (PQstatus(primary_conn) == CONNECTION_OK)
			{
				if (get_recovery_type(primary_conn) == RECTYPE_STANDBY)
				{
					log_notice(_("current upstream node \"%s\" (ID: %i) is not primary, restarting monitoring"),
							   upstream_node_info.node_name, upstream_node_info.node_id);

					close_connection(&primary_conn);

					local_node_info.upstream_node_id = UNKNOWN_NODE_ID;
					return;
				}
			}

			/* we've reconnected to the local node after an outage */
			if (local_node_info.active == false)
			{
				if (PQstatus(primary_conn) == CONNECTION_OK)
				{
					if (update_node_record_set_active(primary_conn, local_node_info.node_id, true) == true)
					{
						PQExpBufferData event_details;

						initPQExpBuffer(&event_details);

						local_node_info.active = true;
						appendPQExpBuffer(&event_details,
										  _("reconnected to local node \"%s\" (ID: %i), marking active"),
										  local_node_info.node_name,
										  local_node_info.node_id);

						log_notice("%s", event_details.data);

						create_event_notification(primary_conn,
												  &config_file_options,
												  local_node_info.node_id,
												  "standby_recovery",
												  true,
												  event_details.data);

						termPQExpBuffer(&event_details);
					}
				}
			}
		}

		if (got_SIGHUP)
		{
			handle_sighup(&local_conn, STANDBY);
		}

		refresh_node_record(local_conn, local_node_info.node_id, &local_node_info);

		if (local_monitoring_state == MS_NORMAL && last_known_upstream_node_id != local_node_info.upstream_node_id)
		{
			/*
			 * It's possible that after a change of upstream, the local node record will not
			 * yet have been updated with the new upstream node ID. Therefore we check the
			 * node record on the upstream, and if that matches "last_known_upstream_node_id",
			 * take that as the correct value.
			 */

			if (monitoring_state == MS_NORMAL)
			{
				t_node_info node_info_on_upstream = T_NODE_INFO_INITIALIZER;
				record_status = get_node_record(primary_conn, config_file_options.node_id, &node_info_on_upstream);

				if (last_known_upstream_node_id == node_info_on_upstream.upstream_node_id)
				{
					local_node_info.upstream_node_id = last_known_upstream_node_id;
				}
			}

			if (last_known_upstream_node_id != local_node_info.upstream_node_id)
			{
				log_notice(_("local node \"%s\" (ID: %i)'s upstream appears to have changed, restarting monitoring"),
						   local_node_info.node_name,
						   local_node_info.node_id);
				log_detail(_("currently monitoring upstream %i; new upstream is %i"),
						   last_known_upstream_node_id,
						   local_node_info.upstream_node_id);
				close_connection(&upstream_conn);
				return;
			}
		}

		log_verbose(LOG_DEBUG, "sleeping %i seconds (parameter \"monitor_interval_secs\")",
					config_file_options.monitor_interval_secs);


		sleep(config_file_options.monitor_interval_secs);
	}
}


void
monitor_streaming_witness(void)
{
	instr_time	log_status_interval_start;
	instr_time	witness_sync_interval_start;

	RecordStatus record_status;

	int primary_node_id = UNKNOWN_NODE_ID;

	reset_node_voting_status();

	log_debug("monitor_streaming_witness()");

	/*
	 * At this point we can't trust the local copy of "repmgr.nodes", as
	 * it may not have been updated. We'll scan the cluster to find the
	 * current primary and refresh the copy from that before proceeding
	 * further.
	 */
	primary_conn = get_primary_connection_quiet(local_conn, &primary_node_id, NULL);

	/*
	 * Primary node should be running at repmgrd startup.
	 *
	 * Otherwise we'll skip to degraded monitoring.
	 */
	if (PQstatus(primary_conn) == CONNECTION_OK)
	{
		PQExpBufferData event_details;

		char *event_type = startup_event_logged == false
			? "repmgrd_start"
			: "repmgrd_upstream_reconnect";

		/* synchronise local copy of "repmgr.nodes", in case it was stale */
		witness_copy_node_records(primary_conn, local_conn);

		/*
		 * refresh upstream node record from primary, so it's as up-to-date
		 * as possible
		 */
		record_status = get_node_record(primary_conn, primary_node_id, &upstream_node_info);

		/*
		 * This is unlikely to happen; if it does emit a warning for diagnostic
		 * purposes and plough on regardless.
		 *
		 * A check for the existence of the record will have already been carried out
		 * in main().
		 */
		if (record_status != RECORD_FOUND)
		{
			log_warning(_("unable to retrieve node record from primary"));
		}

		initPQExpBuffer(&event_details);

		appendPQExpBuffer(&event_details,
						  _("witness monitoring connection to primary node \"%s\" (ID: %i)"),
						  upstream_node_info.node_name,
						  upstream_node_info.node_id);

		log_info("%s", event_details.data);

		create_event_notification(primary_conn,
								  &config_file_options,
								  config_file_options.node_id,
								  event_type,
								  true,
								  event_details.data);

		if (startup_event_logged == false)
			startup_event_logged = true;

		termPQExpBuffer(&event_details);

		monitoring_state = MS_NORMAL;
		INSTR_TIME_SET_CURRENT(log_status_interval_start);
		INSTR_TIME_SET_CURRENT(witness_sync_interval_start);

		upstream_node_info.node_status = NODE_STATUS_UP;
	}
	else
	{
		log_warning(_("unable to connect to primary"));
		log_detail("\n%s", PQerrorMessage(primary_conn));

		/*
		 * Here we're unable to connect to a primary despite having scanned all
		 * known nodes, so we'll grab the record of the node we think is primary
		 * and continue straight to degraded monitoring in the hope a primary
		 * will appear.
		 */

		primary_node_id = get_primary_node_id(local_conn);

		log_notice(_("setting primary_node_id to last known ID %i"), primary_node_id);

		record_status = get_node_record(local_conn, primary_node_id, &upstream_node_info);

		/*
		 * This is unlikely to happen, but if for whatever reason there's
		 * no primary record in the local table, we should just give up
		 */
		if (record_status != RECORD_FOUND)
		{
			log_error(_("unable to retrieve node record for last known primary %i"),
						primary_node_id);
			log_hint(_("execute \"repmgr witness register --force\" to sync the local node records"));
			PQfinish(local_conn);
			terminate(ERR_BAD_CONFIG);
		}

		monitoring_state = MS_DEGRADED;
		INSTR_TIME_SET_CURRENT(degraded_monitoring_start);
		upstream_node_info.node_status = NODE_STATUS_DOWN;
	}

	while (true)
	{
		if (check_upstream_connection(&primary_conn, upstream_node_info.conninfo, NULL) == true)
		{
			set_upstream_last_seen(local_conn, upstream_node_info.node_id);
		}
		else
		{
			if (upstream_node_info.node_status == NODE_STATUS_UP)
			{
				instr_time		upstream_node_unreachable_start;

				INSTR_TIME_SET_CURRENT(upstream_node_unreachable_start);

				upstream_node_info.node_status = NODE_STATUS_UNKNOWN;

				{
					PQExpBufferData event_details;

					initPQExpBuffer(&event_details);

					appendPQExpBuffer(&event_details,
									  _("unable to connect to primary node \"%s\" (ID: %i)"),
									  upstream_node_info.node_name, upstream_node_info.node_id);

					create_event_record(NULL,
										&config_file_options,
										config_file_options.node_id,
										"repmgrd_upstream_disconnect",
										true,
										event_details.data);
					termPQExpBuffer(&event_details);
				}

				try_reconnect(&primary_conn, &upstream_node_info);

				/* Node has recovered - log and continue */
				if (upstream_node_info.node_status == NODE_STATUS_UP)
				{
					int			upstream_node_unreachable_elapsed = calculate_elapsed(upstream_node_unreachable_start);
					PQExpBufferData event_details;

					initPQExpBuffer(&event_details);

					appendPQExpBuffer(&event_details,
									  _("reconnected to upstream node after %i seconds"),
									  upstream_node_unreachable_elapsed);
					log_notice("%s", event_details.data);

					/* check upstream is still primary */
					if (get_recovery_type(primary_conn) != RECTYPE_PRIMARY)
					{
						log_notice(_("current upstream node \"%s\" (ID: %i) is not primary, restarting monitoring"),
								   upstream_node_info.node_name, upstream_node_info.node_id);

						close_connection(&primary_conn);

						termPQExpBuffer(&event_details);
						return;
					}

					create_event_notification(primary_conn,
											  &config_file_options,
											  config_file_options.node_id,
											  "repmgrd_upstream_reconnect",
											  true,
											  event_details.data);
					termPQExpBuffer(&event_details);

					goto loop;
				}

				/* still down after reconnect attempt(s) */
				if (upstream_node_info.node_status == NODE_STATUS_DOWN)
				{
					bool		failover_done = false;


					failover_done = do_witness_failover();

					/*
					 * XXX it's possible it will make sense to return in all
					 * cases to restart monitoring
					 */
					if (failover_done == true)
					{
						primary_node_id = get_primary_node_id(local_conn);
						return;
					}
				}
			}
		}


		if (monitoring_state == MS_DEGRADED)
		{
			int			degraded_monitoring_elapsed = calculate_elapsed(degraded_monitoring_start);

			log_debug("monitoring node %i in degraded state for %i seconds",
					  upstream_node_info.node_id,
					  degraded_monitoring_elapsed);

			if (check_upstream_connection(&primary_conn, upstream_node_info.conninfo, NULL) == true)
			{
				if (config_file_options.connection_check_type != CHECK_QUERY)
					primary_conn = establish_db_connection(upstream_node_info.conninfo, false);

				if (PQstatus(primary_conn) == CONNECTION_OK)
				{
					PQExpBufferData event_details;

					upstream_node_info.node_status = NODE_STATUS_UP;
					monitoring_state = MS_NORMAL;

					initPQExpBuffer(&event_details);

					appendPQExpBuffer(&event_details,
									  _("reconnected to upstream node \"%s\" (ID: %i) after %i seconds, resuming monitoring"),
									  upstream_node_info.node_name,
									  upstream_node_info.node_id,
									  degraded_monitoring_elapsed);

					log_notice("%s", event_details.data);

					/* check upstream is still primary */
					if (get_recovery_type(primary_conn) != RECTYPE_PRIMARY)
					{
						log_notice(_("current upstream node \"%s\" (ID: %i) is not primary, restarting monitoring"),
								   upstream_node_info.node_name,
								   upstream_node_info.node_id);

						close_connection(&primary_conn);

						termPQExpBuffer(&event_details);
						return;
					}

					create_event_notification(primary_conn,
											  &config_file_options,
											  config_file_options.node_id,
											  "repmgrd_upstream_reconnect",
											  true,
											  event_details.data);
					termPQExpBuffer(&event_details);

					goto loop;
				}
			}
			else
			{
				/*
				 * unable to connect to former primary - check if another node
				 * has been promoted
				 */

				NodeInfoList sibling_nodes = T_NODE_INFO_LIST_INITIALIZER;

				get_active_sibling_node_records(local_conn,
												local_node_info.node_id,
												local_node_info.upstream_node_id,
												&sibling_nodes);

				if (sibling_nodes.node_count > 0)
				{
					NodeInfoListCell *cell;
					t_node_info *follow_node_info = NULL;

					log_debug("scanning %i node records to detect new primary...", sibling_nodes.node_count);
					for (cell = sibling_nodes.head; cell; cell = cell->next)
					{
						/* skip local node check, we did that above */
						if (cell->node_info->node_id == local_node_info.node_id)
						{
							continue;
						}

						/* skip node if configured as a witness node - we can't possibly "follow" that */
						if (cell->node_info->type == WITNESS)
						{
							continue;
						}

						cell->node_info->conn = establish_db_connection(cell->node_info->conninfo, false);

						if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
						{
							close_connection(&cell->node_info->conn);
							log_debug("unable to connect to %i ... ", cell->node_info->node_id);
							close_connection(&cell->node_info->conn);
							continue;
						}

						if (get_recovery_type(cell->node_info->conn) == RECTYPE_PRIMARY)
						{
							follow_node_info = cell->node_info;
							close_connection(&cell->node_info->conn);
							break;
						}
						close_connection(&cell->node_info->conn);
					}

					if (follow_node_info != NULL)
					{
						log_info(_("node \"%s\" (node ID: %i) detected as primary"),
								 follow_node_info->node_name,
								 follow_node_info->node_id);
						witness_follow_new_primary(follow_node_info->node_id);
					}
				}

				clear_node_info_list(&sibling_nodes);
			}
		}
loop:

		/*
		 * handle local node failure
		 *
		 * currently we'll just check the connection, and try to reconnect
		 *
		 * TODO: add timeout, after which we run in degraded state
		 */

		(void) connection_ping(local_conn);

		check_connection(&local_node_info, &local_conn);

		if (PQstatus(local_conn) != CONNECTION_OK)
		{
			if (local_node_info.active == true)
			{
				bool success = true;
				PQExpBufferData event_details;

				initPQExpBuffer(&event_details);

				local_node_info.active = false;

				appendPQExpBuffer(&event_details,
								  _("unable to connect to local node \"%s\" (ID: %i), marking inactive"),
								  local_node_info.node_name,
								  local_node_info.node_id);
				log_notice("%s", event_details.data);

				if (PQstatus(primary_conn) == CONNECTION_OK)
				{
					if (update_node_record_set_active(primary_conn, local_node_info.node_id, false) == false)
					{
						success = false;
						log_warning(_("unable to mark node \"%s\" (ID: %i) as inactive"),
									  local_node_info.node_name,
									  local_node_info.node_id);
					}
				}

				create_event_notification(primary_conn,
										  &config_file_options,
										  local_node_info.node_id,
										  "standby_failure",
										  success,
										  event_details.data);

				termPQExpBuffer(&event_details);
			}
		}
		else
		{
			/* we've reconnected to the local node after an outage */
			if (local_node_info.active == false)
			{
				int stored_local_node_id = UNKNOWN_NODE_ID;

				if (PQstatus(primary_conn) == CONNECTION_OK)
				{
					if (update_node_record_set_active(primary_conn, local_node_info.node_id, true) == true)
					{
						PQExpBufferData event_details;

						initPQExpBuffer(&event_details);

						local_node_info.active = true;

						appendPQExpBuffer(&event_details,
										  _("reconnected to local node \"%s\" (ID: %i), marking active"),
										  local_node_info.node_name,
										  local_node_info.node_id);

						log_notice("%s", event_details.data);

						create_event_notification(primary_conn,
												  &config_file_options,
												  local_node_info.node_id,
												  "standby_recovery",
												  true,
												  event_details.data);

						termPQExpBuffer(&event_details);
					}
				}

				/*
				 * If the local node was restarted, we'll need to reinitialise values
				 * stored in shared memory.
				 */

				stored_local_node_id = repmgrd_get_local_node_id(local_conn);
				if (stored_local_node_id == UNKNOWN_NODE_ID)
				{
					repmgrd_set_local_node_id(local_conn, config_file_options.node_id);
					repmgrd_set_pid(local_conn, getpid(), pid_file);
				}
			}
		}


		/*
		 * Refresh repmgr.nodes after "witness_sync_interval" seconds, and check if primary
		 * has changed
		 */

		if (PQstatus(primary_conn) == CONNECTION_OK)
		{
			int witness_sync_interval_elapsed = calculate_elapsed(witness_sync_interval_start);

			if (witness_sync_interval_elapsed >= config_file_options.witness_sync_interval)
			{
				if (get_recovery_type(primary_conn) != RECTYPE_PRIMARY)
				{
					log_notice(_("current upstream node \"%s\" (ID: %i) is not primary, restarting monitoring"),
							   upstream_node_info.node_name, upstream_node_info.node_id);

					close_connection(&primary_conn);

					return;
				}

				log_debug("synchronising witness node records");
				witness_copy_node_records(primary_conn, local_conn);

				INSTR_TIME_SET_CURRENT(witness_sync_interval_start);
			}
			else
			{
				log_debug("seconds since last node record sync: %i (sync interval: %i)",
						  witness_sync_interval_elapsed,
						  config_file_options.witness_sync_interval)
			}
		}

		/* emit "still alive" log message at regular intervals, if requested */
		if (config_file_options.log_status_interval > 0)
		{
			int			log_status_interval_elapsed = calculate_elapsed(log_status_interval_start);

			if (log_status_interval_elapsed >= config_file_options.log_status_interval)
			{
				PQExpBufferData monitoring_summary;

				initPQExpBuffer(&monitoring_summary);

				appendPQExpBuffer(&monitoring_summary,
								  _("witness node \"%s\" (ID: %i) monitoring primary node \"%s\" (ID: %i) in %s state"),
								  local_node_info.node_name,
								  local_node_info.node_id,
								  upstream_node_info.node_name,
								  upstream_node_info.node_id,
								  print_monitoring_state(monitoring_state));

				log_info("%s", monitoring_summary.data);
				termPQExpBuffer(&monitoring_summary);
				if (monitoring_state == MS_DEGRADED && config_file_options.failover == FAILOVER_AUTOMATIC)
				{
					log_detail(_("waiting for current or new primary to reappear"));
				}

				INSTR_TIME_SET_CURRENT(log_status_interval_start);
			}
		}



		if (got_SIGHUP)
		{
			handle_sighup(&local_conn, WITNESS);
		}

		log_verbose(LOG_DEBUG, "sleeping %i seconds (parameter \"monitor_interval_secs\")",
					config_file_options.monitor_interval_secs);

		sleep(config_file_options.monitor_interval_secs);
	}

	return;
}


static bool
do_primary_failover(void)
{
	ElectionResult election_result;
	bool final_result = false;
	NodeInfoList sibling_nodes = T_NODE_INFO_LIST_INITIALIZER;
	int new_primary_id = UNKNOWN_NODE_ID;

	/*
	 * Double-check status of the local connection
	 */
	check_connection(&local_node_info, &local_conn);

	/*
	 * if requested, disable WAL receiver and wait until WAL receivers on all
	 * sibling nodes are disconnected
	 */
	if (config_file_options.standby_disconnect_on_failover == true)
	{
		NodeInfoListCell *cell = NULL;
		NodeInfoList check_sibling_nodes = T_NODE_INFO_LIST_INITIALIZER;
		int i;

		bool sibling_node_wal_receiver_connected = false;

		if (PQserverVersion(local_conn) < 90500)
		{
			log_warning(_("\"standby_disconnect_on_failover\" specified, but not available for this PostgreSQL version"));
			/* TODO: format server version */
			log_detail(_("available from PostgreSQL 9.5, this PostgreSQL version is %i"), PQserverVersion(local_conn));
		}
		else
		{
			disable_wal_receiver(local_conn);

			/*
			 * Loop through all reachable sibling nodes to determine whether
			 * they have disabled their WAL receivers.
			 *
			 * TODO: do_election() also calls get_active_sibling_node_records(),
			 * consolidate calls if feasible
			 *
			 */
			get_active_sibling_node_records(local_conn,
											local_node_info.node_id,
											local_node_info.upstream_node_id,
											&check_sibling_nodes);

			for (i = 0; i < config_file_options.sibling_nodes_disconnect_timeout; i++)
			{
				for (cell = check_sibling_nodes.head; cell; cell = cell->next)
				{
					if (cell->node_info->conn == NULL)
						cell->node_info->conn = establish_db_connection(cell->node_info->conninfo, false);

					if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
					{
						log_warning(_("unable to query WAL receiver PID on node \"%s\" (ID: %i)"),
									cell->node_info->node_name,
									cell->node_info->node_id);
						close_connection(&cell->node_info->conn);
					}
					else
					{
						pid_t sibling_wal_receiver_pid = (pid_t)get_wal_receiver_pid(cell->node_info->conn);

						if (sibling_wal_receiver_pid == UNKNOWN_PID)
						{
							log_warning(_("unable to query WAL receiver PID on node %i"),
										cell->node_info->node_id);
						}
						else if (sibling_wal_receiver_pid > 0)
						{
							log_info(_("WAL receiver PID on node %i is %i"),
									 cell->node_info->node_id,
									 sibling_wal_receiver_pid);
							sibling_node_wal_receiver_connected = true;
						}
					}
				}

				if (sibling_node_wal_receiver_connected == false)
				{
					log_notice(_("WAL receiver disconnected on all sibling nodes"));
					break;
				}

				log_debug("sleeping %i of max %i seconds (\"sibling_nodes_disconnect_timeout\")",
						  i + 1, config_file_options.sibling_nodes_disconnect_timeout);
				sleep(1);
			}

			if (sibling_node_wal_receiver_connected == true)
			{
				/* TODO: prevent any such nodes becoming promotion candidates */
				log_warning(_("WAL receiver still connected on at least one sibling node"));
			}
			else
			{
				log_info(_("WAL receiver disconnected on all %i sibling nodes"),
						 check_sibling_nodes.node_count);
			}

			clear_node_info_list(&check_sibling_nodes);
		}
	}

	/* attempt to initiate voting process */
	election_result = do_election(&sibling_nodes, &new_primary_id);

	/* TODO add pre-event notification here */
	failover_state = FAILOVER_STATE_UNKNOWN;

	log_debug("election result: %s", _print_election_result(election_result));

	/* Reenable WAL receiver, if disabled */
	if (config_file_options.standby_disconnect_on_failover == true)
	{
		/* adjust "wal_retrieve_retry_interval" but don't wait for WAL receiver to start */
		enable_wal_receiver(local_conn, false);
	}

	/* election was cancelled and do_election() did not determine a new primary */
	if (election_result == ELECTION_CANCELLED)
	{
		if (new_primary_id == UNKNOWN_NODE_ID)
		{
			log_notice(_("election cancelled"));
			clear_node_info_list(&sibling_nodes);
			return false;
		}

		log_info(_("follower node intending to follow new primary %i"), new_primary_id);

		failover_state = FAILOVER_STATE_FOLLOW_NEW_PRIMARY;
	}
	else if (election_result == ELECTION_RERUN)
	{
		log_notice(_("promotion candidate election will be rerun"));
		/* notify siblings that they should rerun the election too */
		notify_followers(&sibling_nodes, ELECTION_RERUN_NOTIFICATION);

		failover_state = FAILOVER_STATE_ELECTION_RERUN;
	}
	else if (election_result == ELECTION_WON)
	{
		if (sibling_nodes.node_count > 0)
		{
			log_notice("this node is the winner, will now promote itself and inform other nodes");
		}
		else
		{
			log_notice("this node is the only available candidate and will now promote itself");
		}

		failover_state = promote_self();
	}
	else if (election_result == ELECTION_LOST || election_result == ELECTION_NOT_CANDIDATE)
	{
		/*
		 * if the node couldn't be promoted as it's not in the same location as the primary,
		 * add an explanatory notice
		 */
		if (election_result == ELECTION_NOT_CANDIDATE && strncmp(upstream_node_info.location, local_node_info.location, MAXLEN) != 0)
		{
			log_notice(_("this node's location (\"%s\") is not the primary node location (\"%s\"), so node cannot be promoted"),
					   local_node_info.location,
					   upstream_node_info.location);
		}

		log_info(_("follower node awaiting notification from a candidate node"));

		failover_state = FAILOVER_STATE_WAITING_NEW_PRIMARY;
	}

	/*
	 * node has determined a new primary is already available
	 */
	if (failover_state == FAILOVER_STATE_FOLLOW_NEW_PRIMARY)
	{
		failover_state = follow_new_primary(new_primary_id);
	}

	/*
	 * node has decided it is a follower, so will await notification from the
	 * candidate that it has promoted itself and can be followed
	 */
	else if (failover_state == FAILOVER_STATE_WAITING_NEW_PRIMARY)
	{
		/* TODO: rerun election if new primary doesn't appear after timeout */

		/* either follow, self-promote or time out; either way resume monitoring */
		if (wait_primary_notification(&new_primary_id) == true)
		{
			/* if primary has reappeared, no action needed */
			if (new_primary_id == upstream_node_info.node_id)
			{
				failover_state = FAILOVER_STATE_FOLLOWING_ORIGINAL_PRIMARY;
			}
			/* if new_primary_id is self, promote */
			else if (new_primary_id == local_node_info.node_id)
			{
				log_notice(_("this node is promotion candidate, promoting"));

				failover_state = promote_self();

				get_active_sibling_node_records(local_conn,
												local_node_info.node_id,
												upstream_node_info.node_id,
												&sibling_nodes);

			}
			/* election rerun */
			else if (new_primary_id == ELECTION_RERUN_NOTIFICATION)
			{
				log_notice(_("received notification from promotion candidate to rerun election"));
				failover_state = FAILOVER_STATE_ELECTION_RERUN;
			}
			else if (config_file_options.failover == FAILOVER_MANUAL)
			{
				/* automatic failover disabled */

				t_node_info new_primary = T_NODE_INFO_INITIALIZER;
				RecordStatus record_status = RECORD_NOT_FOUND;
				PGconn	   *new_primary_conn;

				record_status = get_node_record(local_conn, new_primary_id, &new_primary);

				if (record_status != RECORD_FOUND)
				{
					log_error(_("unable to retrieve metadata record for new primary node (ID: %i)"),
							  new_primary_id);
				}
				else
				{
					PQExpBufferData event_details;

					initPQExpBuffer(&event_details);
					appendPQExpBuffer(&event_details,
									  _("node \"%s\" (ID: %i) is in manual failover mode and is now disconnected from streaming replication"),
									  local_node_info.node_name,
									  local_node_info.node_id);

					new_primary_conn = establish_db_connection(new_primary.conninfo, false);

					create_event_notification(new_primary_conn,
											  &config_file_options,
											  local_node_info.node_id,
											  "standby_disconnect_manual",
											  /*
											   * here "true" indicates the action has occurred as expected
											   */
											  true,
											  event_details.data);
					close_connection(&new_primary_conn);
					termPQExpBuffer(&event_details);

				}
				failover_state = FAILOVER_STATE_REQUIRES_MANUAL_FAILOVER;
			}
			else
			{
				failover_state = follow_new_primary(new_primary_id);
			}
		}
		else
		{
			failover_state = FAILOVER_STATE_NO_NEW_PRIMARY;
		}
	}

	log_verbose(LOG_DEBUG, "failover state is %s",
				format_failover_state(failover_state));

	switch (failover_state)
	{
		case FAILOVER_STATE_PROMOTED:
			/* notify former siblings that they should now follow this node */
			notify_followers(&sibling_nodes, local_node_info.node_id);

			/* pass control back down to start_monitoring() */
			log_info(_("switching to primary monitoring mode"));

			failover_state = FAILOVER_STATE_NONE;

			final_result = true;

			break;

		case FAILOVER_STATE_ELECTION_RERUN:

			/* we no longer care about our former siblings */
			clear_node_info_list(&sibling_nodes);

			log_notice(_("rerunning election after %i seconds (\"election_rerun_interval\")"),
					   config_file_options.election_rerun_interval);
			sleep(config_file_options.election_rerun_interval);

			log_info(_("election rerun will now commence"));
			/*
			 * mark the upstream node as "up" so another election is triggered
			 * after we fall back to monitoring
			 */
			upstream_node_info.node_status = NODE_STATUS_UP;
			failover_state = FAILOVER_STATE_NONE;

			final_result = false;
			break;

		case FAILOVER_STATE_PRIMARY_REAPPEARED:

			/*
			 * notify siblings that they should resume following the original
			 * primary
			 */
			notify_followers(&sibling_nodes, upstream_node_info.node_id);

			/* pass control back down to start_monitoring() */

			log_info(_("resuming %s monitoring mode"), get_node_type_string(local_node_info.type));
			log_detail(_("original primary \"%s\" (ID: %i) reappeared"),
					   upstream_node_info.node_name, upstream_node_info.node_id);

			failover_state = FAILOVER_STATE_NONE;

			final_result = true;
			break;

		case FAILOVER_STATE_FOLLOWED_NEW_PRIMARY:
			log_info(_("resuming %s monitoring mode"), get_node_type_string(local_node_info.type));
			log_detail(_("following new primary \"%s\" (ID: %i)"),
					   upstream_node_info.node_name, upstream_node_info.node_id);
			failover_state = FAILOVER_STATE_NONE;

			final_result = true;
			break;

		case FAILOVER_STATE_FOLLOWING_ORIGINAL_PRIMARY:
			log_info(_("resuming %s monitoring mode"), get_node_type_string(local_node_info.type));
			log_detail(_("following original primary \"%s\" (ID: %i)"),
					   upstream_node_info.node_name, upstream_node_info.node_id);
			failover_state = FAILOVER_STATE_NONE;

			final_result = true;
			break;

		case FAILOVER_STATE_PROMOTION_FAILED:
			monitoring_state = MS_DEGRADED;
			INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

			final_result = false;
			break;

		case FAILOVER_STATE_FOLLOW_FAIL:

			/*
			 * for whatever reason we were unable to follow the new primary -
			 * continue monitoring in degraded state
			 */
			monitoring_state = MS_DEGRADED;
			INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

			final_result = false;
			break;

		case FAILOVER_STATE_REQUIRES_MANUAL_FAILOVER:
			log_info(_("automatic failover disabled for this node, manual intervention required"));

			monitoring_state = MS_DEGRADED;
			INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

			final_result = false;
			break;

		case FAILOVER_STATE_NO_NEW_PRIMARY:
		case FAILOVER_STATE_WAITING_NEW_PRIMARY:
			/* pass control back down to start_monitoring() */
			final_result = false;
			break;

		case FAILOVER_STATE_NODE_NOTIFICATION_ERROR:
		case FAILOVER_STATE_LOCAL_NODE_FAILURE:
		case FAILOVER_STATE_UNKNOWN:
		case FAILOVER_STATE_NONE:

			final_result = false;
			break;

		default:	/* should never reach here */
			log_warning(_("unhandled failover state %i"), failover_state);
			break;
	}

	/* we no longer care about our former siblings */
	clear_node_info_list(&sibling_nodes);

	return final_result;
}


static bool
update_monitoring_history(void)
{
	ReplInfo	replication_info;
	XLogRecPtr	primary_last_wal_location = InvalidXLogRecPtr;

	long long unsigned int apply_lag_bytes = 0;
	long long unsigned int replication_lag_bytes = 0;

	/* both local and primary connections must be available */
	if (PQstatus(primary_conn) != CONNECTION_OK)
	{
		log_warning(_("primary connection is not available, unable to update monitoring history"));
		return false;
	}

	if (PQstatus(local_conn) != CONNECTION_OK)
	{
		log_warning(_("local connection is not available, unable to update monitoring history"));
		return false;
	}

	init_replication_info(&replication_info);

	if (get_replication_info(local_conn, STANDBY, &replication_info) == false)
	{
		log_warning(_("unable to retrieve replication status information, unable to update monitoring history"));
		return false;
	}

	/*
	 * This can be the case when a standby is starting up after following
	 * a new primary, or when it has dropped back to archive recovery.
	 * As long as we can connect to the primary, we can still provide lag information.
	 */
	if (replication_info.receiving_streamed_wal == false)
	{
		log_verbose(LOG_WARNING, _("standby %i not connected to streaming replication"),
					local_node_info.node_id);
	}

	primary_last_wal_location = get_primary_current_lsn(primary_conn);

	if (primary_last_wal_location == InvalidXLogRecPtr)
	{
		log_warning(_("unable to retrieve primary's current LSN"));
		return false;
	}

	/* calculate apply lag in bytes */
	if (replication_info.last_wal_receive_lsn >= replication_info.last_wal_replay_lsn)
	{
		apply_lag_bytes = (long long unsigned int) (replication_info.last_wal_receive_lsn - replication_info.last_wal_replay_lsn);
	}
	else
	{
		/* if this happens, it probably indicates archive recovery */
		apply_lag_bytes = 0;
	}

	/* calculate replication lag in bytes */

	if (primary_last_wal_location >= replication_info.last_wal_receive_lsn)
	{
		replication_lag_bytes = (long long unsigned int) (primary_last_wal_location - replication_info.last_wal_receive_lsn);
		log_debug("replication lag in bytes is: %llu", replication_lag_bytes);
	}
	else
	{
		/*
		 * This should never happen, but in case it does set replication lag
		 * to zero
		 */
		log_warning("primary xlog location (%X/%X) is behind the standby receive location (%X/%X)",
					format_lsn(primary_last_wal_location),
					format_lsn(replication_info.last_wal_receive_lsn));
		replication_lag_bytes = 0;
	}

	add_monitoring_record(primary_conn,
						  local_conn,
						  primary_node_id,
						  local_node_info.node_id,
						  replication_info.current_timestamp,
						  primary_last_wal_location,
						  replication_info.last_wal_receive_lsn,
						  replication_info.last_xact_replay_timestamp,
						  replication_lag_bytes,
						  apply_lag_bytes);

	INSTR_TIME_SET_CURRENT(last_monitoring_update);

	log_verbose(LOG_DEBUG, "update_monitoring_history(): monitoring history update sent");

	return true;
}


/*
 * do_upstream_standby_failover()
 *
 * Attach cascaded standby to another node, currently the primary.
 *
 * Note that in contrast to a primary failover, where one of the downstrean
 * standby nodes will become a primary, a cascaded standby failover (where the
 * upstream standby has gone away) is "just" a case of attaching the standby to
 * another node.
 *
 * Currently we will try to attach the node to the cluster primary.
 *
 * TODO: As of repmgr 4.3, "repmgr standby follow" supports attaching a standby to another
 * standby node. We need to provide a selection of reconnection strategies as different
 * behaviour might be desirable in different situations.
 */

static bool
do_upstream_standby_failover(void)
{
	t_node_info primary_node_info = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status = RECORD_NOT_FOUND;
	RecoveryType primary_type = RECTYPE_UNKNOWN;
	int			i, standby_follow_result;
	char		parsed_follow_command[MAXPGPATH] = "";

	close_connection(&upstream_conn);

	/*
	 *
	 */
	if (config_file_options.failover == FAILOVER_MANUAL)
	{
		log_notice(_("this node is not configured for automatic failover"));
		log_detail(_("parameter \"failover\" is set to \"manual\""));
		return false;
	}

	if (get_primary_node_record(local_conn, &primary_node_info) == false)
	{
		log_error(_("unable to retrieve primary node record"));
		return false;
	}

	/*
	 * Verify that we can still talk to the cluster primary, even though the
	 * node's upstream is not available
	 */

	check_connection(&primary_node_info, &primary_conn);

	if (PQstatus(primary_conn) != CONNECTION_OK)
	{
		log_error(_("unable to connect to last known primary \"%s\" (ID: %i)"),
				  primary_node_info.node_name,
				  primary_node_info.node_id);

		close_connection(&primary_conn);
		monitoring_state = MS_DEGRADED;
		INSTR_TIME_SET_CURRENT(degraded_monitoring_start);
		return false;
	}

	primary_type = get_recovery_type(primary_conn);

	if (primary_type != RECTYPE_PRIMARY)
	{
		if (primary_type == RECTYPE_STANDBY)
		{
			log_error(_("last known primary \"%s\" (ID: %i) is in recovery, not following"),
					  primary_node_info.node_name,
					  primary_node_info.node_id);
		}
		else
		{
			log_error(_("unable to determine status of last known primary \"%s\" (ID: %i), not following"),
					  primary_node_info.node_name,
					  primary_node_info.node_id);
		}

		close_connection(&primary_conn);
		monitoring_state = MS_DEGRADED;
		INSTR_TIME_SET_CURRENT(degraded_monitoring_start);
		return false;
	}

	/* Close the connection to this server */
	close_connection(&local_conn);

	log_debug(_("standby follow command is:\n  \"%s\""),
			  config_file_options.follow_command);

	/*
	 * replace %n in "config_file_options.follow_command" with ID of primary
	 * to follow.
	 */
	parse_follow_command(parsed_follow_command, config_file_options.follow_command, primary_node_info.node_id);

	standby_follow_result = system(parsed_follow_command);

	if (standby_follow_result != 0)
	{
		PQExpBufferData event_details;

		initPQExpBuffer(&event_details);

		appendPQExpBuffer(&event_details,
						  _("unable to execute follow command:\n %s"),
						  config_file_options.follow_command);

		log_error("%s", event_details.data);

		/*
		 * It may not possible to write to the event notification table but we
		 * should be able to generate an external notification if required.
		 */
		create_event_notification(primary_conn,
								  &config_file_options,
								  local_node_info.node_id,
								  "repmgrd_failover_follow",
								  false,
								  event_details.data);

		termPQExpBuffer(&event_details);
	}

	/*
	 * It's possible that the standby is still starting up after the "follow_command"
	 * completes, so poll for a while until we get a connection.
	 *
	 * NOTE: we've previously closed the local connection, so even if the follow command
	 * failed for whatever reason and the local node remained up, we can re-open
	 * the local connection.
	 */

	for (i = 0; i < config_file_options.repmgrd_standby_startup_timeout; i++)
	{
		local_conn = establish_db_connection(local_node_info.conninfo, false);

		if (PQstatus(local_conn) == CONNECTION_OK)
			break;

		close_connection(&local_conn);

		log_debug("sleeping 1 second; %i of %i (\"repmgrd_standby_startup_timeout\") attempts to reconnect to local node",
				  i + 1,
				  config_file_options.repmgrd_standby_startup_timeout);
		sleep(1);
	}

	if (PQstatus(local_conn) != CONNECTION_OK)
	{
		log_error(_("unable to reconnect to local node \"%s\" (ID: %i)"),
				  local_node_info.node_name,
				  local_node_info.node_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	/* refresh shared memory settings which will have been zapped by the restart */
	repmgrd_set_local_node_id(local_conn, config_file_options.node_id);
	repmgrd_set_pid(local_conn, getpid(), pid_file);

	/*
	 *
	 */

	if (standby_follow_result != 0)
	{
		monitoring_state = MS_DEGRADED;
		INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	/*
	 * update upstream_node_id to primary node (but only if follow command
	 * was successful)
	 */

	{
		if (update_node_record_set_upstream(primary_conn,
											local_node_info.node_id,
											primary_node_info.node_id) == false)
		{
			PQExpBufferData event_details;

			initPQExpBuffer(&event_details);
			appendPQExpBuffer(&event_details,
							  _("unable to set node \"%s\" (ID: %i)'s new upstream ID to %i"),
							  local_node_info.node_name,
							  local_node_info.node_id,
							  primary_node_info.node_id);

			log_error("%s", event_details.data);

			create_event_notification(NULL,
									  &config_file_options,
									  local_node_info.node_id,
									  "repmgrd_failover_follow",
									  false,
									  event_details.data);

			termPQExpBuffer(&event_details);

			terminate(ERR_BAD_CONFIG);
		}
	}

	/* refresh own internal node record */
	record_status = get_node_record(primary_conn, local_node_info.node_id, &local_node_info);

	/*
	 * highly improbable this will happen, but in case we're unable to
	 * retrieve our node record from the primary, update it ourselves, and
	 * hope for the best
	 */
	if (record_status != RECORD_FOUND)
	{
		local_node_info.upstream_node_id = primary_node_info.node_id;
	}

	{
		PQExpBufferData event_details;

		initPQExpBuffer(&event_details);

		appendPQExpBuffer(&event_details,
						  _("node \"%s\" (ID: %i) is now following primary node \"%s\" (ID: %i)"),
						  local_node_info.node_name,
						  local_node_info.node_id,
						  primary_node_info.node_name,
						  primary_node_info.node_id);

		log_notice("%s", event_details.data);

		create_event_notification(primary_conn,
								  &config_file_options,
								  local_node_info.node_id,
								  "repmgrd_failover_follow",
								  true,
								  event_details.data);

		termPQExpBuffer(&event_details);
	}

	/* keep the primary connection open */

	return true;
}


/*
 * This promotes the local node using the "promote_command" configuration
 * parameter, which must be either "repmgr standby promote" or a script which
 * at some point executes "repmgr standby promote".
 *
 * TODO: make "promote_command" and execute the same code used by
 * "repmgr standby promote".
 */
static FailoverState
promote_self(void)
{
	char	   *promote_command;
	int			r;

	/* Store details of the failed node here */
	t_node_info failed_primary = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status;

	/*
	 * optionally add a delay before promoting the standby; this is mainly
	 * useful for testing (e.g. for reappearance of the original primary) and
	 * is not documented.
	 */
	if (config_file_options.promote_delay > 0)
	{
		log_debug("sleeping %i seconds before promoting standby",
				  config_file_options.promote_delay);
		sleep(config_file_options.promote_delay);
	}

	if (local_node_info.upstream_node_id == UNKNOWN_NODE_ID)
	{
		/*
		 * This is a corner-case situation where the repmgr metadata on the
		 * promotion candidate is outdated and the local node's upstream_node_id
		 * is not set. This is often an indication of potentially serious issues,
		 * such as the local node being very far behind the primary, or not being
		 * attached at all.
		 *
		 * In this case it may be desirable to restore the original primary.
		 * This behaviour can be controlled by the "always_promote" configuration option.
		 */
		if (config_file_options.always_promote == false)
		{
			log_error(_("this node (ID: %i) does not have its upstream_node_id set, not promoting"),
					  local_node_info.node_id);
			log_detail(_("the local node's metadata has not been updated since it became a standby"));
			log_hint(_("set \"always_promote\" to \"true\" to force promotion in this situation"));
			return FAILOVER_STATE_PROMOTION_FAILED;
		}
		else
		{
			log_warning(_("this node (ID: %i) does not have its upstream_node_id set, promoting anyway"),
						local_node_info.node_id);
			log_detail(_("\"always_promote\" is set to \"true\" "));
		}
	}
	else
	{
		record_status = get_node_record(local_conn, local_node_info.upstream_node_id, &failed_primary);

		if (record_status != RECORD_FOUND)
		{
			log_error(_("unable to retrieve metadata record for failed upstream (ID: %i)"),
					  local_node_info.upstream_node_id);
			return FAILOVER_STATE_PROMOTION_FAILED;
		}
	}

	/* the presence of this command has been established already */
	promote_command = config_file_options.promote_command;

	log_info(_("promote_command is:\n  \"%s\""),
			  promote_command);

	if (log_type == REPMGR_STDERR && *config_file_options.log_file)
	{
		fflush(stderr);
	}

	r = system(promote_command);

	log_debug("result of promote_command: %i", WEXITSTATUS(r));

	/* connection should stay up, but check just in case */
	if (PQstatus(local_conn) != CONNECTION_OK)
	{
		log_warning(_("local database connection not available"));
		log_detail("\n%s", PQerrorMessage(local_conn));

		close_connection(&local_conn);

		local_conn = establish_db_connection(local_node_info.conninfo, true);

		/* assume node failed */
		if (PQstatus(local_conn) != CONNECTION_OK)
		{
			log_error(_("unable to reconnect to local node"));
			log_detail("\n%s", PQerrorMessage(local_conn));

			close_connection(&local_conn);

			/* XXX handle this */
			return FAILOVER_STATE_LOCAL_NODE_FAILURE;
		}
	}

	if (WIFEXITED(r) && WEXITSTATUS(r))
	{
		int			primary_node_id = UNKNOWN_NODE_ID;

		log_error(_("promote command failed"));
		log_detail(_("promote command exited with error code %i"), WEXITSTATUS(r));

		log_info(_("checking if original primary node has reappeared"));

		upstream_conn = get_primary_connection(local_conn,
											   &primary_node_id,
											   NULL);

		if (PQstatus(upstream_conn) != CONNECTION_OK)
		{
			close_connection(&upstream_conn);
		}
		else if (primary_node_id == failed_primary.node_id)
		{
			PQExpBufferData event_details;

			log_notice(_("original primary \"%s\" (ID: %i) reappeared before this standby was promoted - no action taken"),
					   failed_primary.node_name,
					   failed_primary.node_id);

			initPQExpBuffer(&event_details);
			appendPQExpBuffer(&event_details,
							  _("original primary \"%s\" (ID: %i) reappeared"),
							  failed_primary.node_name,
							  failed_primary.node_id);

			create_event_notification(upstream_conn,
									  &config_file_options,
									  local_node_info.node_id,
									  "repmgrd_failover_abort",
									  true,
									  event_details.data);

			termPQExpBuffer(&event_details);

			/* XXX handle this! */
			/* -> we'll need to let the other nodes know too.... */
			/* no failover occurred but we'll want to restart connections */

			return FAILOVER_STATE_PRIMARY_REAPPEARED;
		}

		create_event_notification(NULL,
								  &config_file_options,
								  local_node_info.node_id,
								  "repmgrd_promote_error",
								  true,
								  "");

		return FAILOVER_STATE_PROMOTION_FAILED;
	}

	/*
	 * Promotion has succeeded - verify local connection is still available
	 */
	try_reconnect(&local_conn, &local_node_info);

	/* bump the electoral term */
	increment_current_term(local_conn);

	{
		PQExpBufferData event_details;

		/* update own internal node record */
		record_status = get_node_record(local_conn, local_node_info.node_id, &local_node_info);

		/*
		 * XXX here we're assuming the promote command updated metadata
		 */
		initPQExpBuffer(&event_details);

		appendPQExpBuffer(&event_details,
						  _("node \"%s\" (ID: %i) promoted to primary; old primary \"%s\" (ID: %i) marked as failed"),
						  local_node_info.node_name,
						  local_node_info.node_id,
						  failed_primary.node_name,
						  failed_primary.node_id);

		/* local_conn is now the primary connection */
		create_event_notification(local_conn,
								  &config_file_options,
								  local_node_info.node_id,
								  "repmgrd_failover_promote",
								  true,
								  event_details.data);

		termPQExpBuffer(&event_details);
	}

	return FAILOVER_STATE_PROMOTED;
}




/*
 * Notify follower nodes about which node to follow. Normally this
 * will be the current node, however if the original primary reappeared
 * before this node could be promoted, we'll inform the followers they
 * should resume monitoring the original primary.
 */
static void
notify_followers(NodeInfoList *standby_nodes, int follow_node_id)
{
	NodeInfoListCell *cell;

	log_info(_("%i followers to notify"),
			 standby_nodes->node_count);

	for (cell = standby_nodes->head; cell; cell = cell->next)
	{
		log_verbose(LOG_DEBUG, "intending to notify node %i...", cell->node_info->node_id);

		if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
		{
			log_info(_("reconnecting to node \"%s\" (ID: %i)..."),
					 cell->node_info->node_name,
					 cell->node_info->node_id);

			close_connection(&cell->node_info->conn);

			cell->node_info->conn = establish_db_connection(cell->node_info->conninfo, false);
		}

		if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
		{
			log_warning(_("unable to reconnect to \"%s\" (ID: %i)"),
						cell->node_info->node_name,
						cell->node_info->node_id);
			log_detail("\n%s", PQerrorMessage(cell->node_info->conn));

			close_connection(&cell->node_info->conn);
			continue;
		}

		if (follow_node_id == ELECTION_RERUN_NOTIFICATION)
		{
			log_notice(_("notifying node \"%s\" (ID: %i) to rerun promotion candidate selection"),
					   cell->node_info->node_name,
					   cell->node_info->node_id);
		}
		else
		{
			log_notice(_("notifying node \"%s\" (ID: %i) to follow node %i"),
					   cell->node_info->node_name,
					   cell->node_info->node_id,
					   follow_node_id);
		}
		notify_follow_primary(cell->node_info->conn, follow_node_id);
	}
}


static bool
wait_primary_notification(int *new_primary_id)
{
	int			i;

	for (i = 0; i < config_file_options.primary_notification_timeout; i++)
	{
		if (get_new_primary(local_conn, new_primary_id) == true)
		{
			log_debug("new primary is %i; elapsed: %i seconds",
					  *new_primary_id, i);
			return true;
		}

		log_verbose(LOG_DEBUG, "waiting for new primary notification, %i of max %i seconds (\"primary_notification_timeout\")",
					i, config_file_options.primary_notification_timeout);

		sleep(1);
	}

	log_warning(_("no notification received from new primary after %i seconds"),
				config_file_options.primary_notification_timeout);

	monitoring_state = MS_DEGRADED;
	INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

	return false;
}


static FailoverState
follow_new_primary(int new_primary_id)
{
	char		parsed_follow_command[MAXPGPATH] = "";
	int			i, r;

	/* Store details of the failed node here */
	t_node_info failed_primary = T_NODE_INFO_INITIALIZER;
	t_node_info new_primary = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status = RECORD_NOT_FOUND;
	bool		new_primary_ok = false;

	log_verbose(LOG_DEBUG, "follow_new_primary(): new primary id is %i", new_primary_id);

	record_status = get_node_record(local_conn, new_primary_id, &new_primary);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve metadata record for new primary node (ID: %i)"),
				  new_primary_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	log_notice(_("attempting to follow new primary \"%s\" (node ID: %i)"),
				 new_primary.node_name,
				 new_primary_id);

	record_status = get_node_record(local_conn, local_node_info.upstream_node_id, &failed_primary);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve metadata record for failed primary (ID: %i)"),
				  local_node_info.upstream_node_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	/* XXX check if new_primary_id == failed_primary.node_id? */

	if (log_type == REPMGR_STDERR && *config_file_options.log_file)
	{
		fflush(stderr);
	}

	upstream_conn = establish_db_connection(new_primary.conninfo, false);

	if (PQstatus(upstream_conn) == CONNECTION_OK)
	{
		RecoveryType primary_recovery_type = get_recovery_type(upstream_conn);

		if (primary_recovery_type == RECTYPE_PRIMARY)
		{
			new_primary_ok = true;
		}
		else
		{
			new_primary_ok = false;
			log_warning(_("new primary \"%s\" (node ID: %i) is in recovery"),
						new_primary.node_name,
						new_primary_id);
			close_connection(&upstream_conn);
		}
	}

	if (new_primary_ok == false)
	{
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	/*
	 * disconnect from local node, as follow operation will result in a server
	 * restart
	 */

	close_connection(&local_conn);

	/*
	 * replace %n in "config_file_options.follow_command" with ID of primary
	 * to follow.
	 */
	parse_follow_command(parsed_follow_command, config_file_options.follow_command, new_primary_id);

	log_debug(_("standby follow command is:\n  \"%s\""),
			  parsed_follow_command);

	/* execute the follow command */
	r = system(parsed_follow_command);

	if (r != 0)
	{
		PGconn	   *old_primary_conn;

		/*
		 * The "standby follow" command could still fail due to the original primary
		 * reappearing before the candidate could promote itself ("repmgr
		 * standby follow" will refuse to promote another node if the primary
		 * is available). However the new primary will only instruct the other
		 * nodes to follow it after it's successfully promoted itself, so this
		 * case is highly unlikely. A slightly more likely scenario would
		 * be the new primary becoming unavailable just after it's sent notifications
		 * to its follower nodes, and the old primary becoming available again.
		 */
		old_primary_conn = establish_db_connection(failed_primary.conninfo, false);

		if (PQstatus(old_primary_conn) == CONNECTION_OK)
		{
			RecoveryType upstream_recovery_type = get_recovery_type(old_primary_conn);

			if (upstream_recovery_type == RECTYPE_PRIMARY)
			{
				PQExpBufferData event_details;

				initPQExpBuffer(&event_details);
				appendPQExpBufferStr(&event_details,
									 _("original primary reappeared - no action taken"));

				log_notice("%s", event_details.data);

				create_event_notification(old_primary_conn,
										  &config_file_options,
										  local_node_info.node_id,
										  "repmgrd_failover_aborted",
										  true,
										  event_details.data);

				termPQExpBuffer(&event_details);

				close_connection(&old_primary_conn);

				return FAILOVER_STATE_PRIMARY_REAPPEARED;
			}

			log_notice(_("original primary reappeared as standby"));

			close_connection(&old_primary_conn);
		}

		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	/*
	 * refresh local copy of local and primary node records - we get these
	 * directly from the primary to ensure they're the current version
	 */

	record_status = get_node_record(upstream_conn, new_primary_id, &upstream_node_info);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve metadata record found for node %i"),
				  new_primary_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	record_status = get_node_record(upstream_conn, local_node_info.node_id, &local_node_info);
	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve metadata record found for node %i"),
				  local_node_info.node_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	/*
	 * It's possible that the standby is still starting up after the "follow_command"
	 * completes, so poll for a while until we get a connection.
	 */

	for (i = 0; i < config_file_options.repmgrd_standby_startup_timeout; i++)
	{
		local_conn = establish_db_connection(local_node_info.conninfo, false);

		if (PQstatus(local_conn) == CONNECTION_OK)
			break;

		close_connection(&local_conn);

		log_debug("sleeping 1 second; %i of %i attempts to reconnect to local node",
				  i + 1,
				  config_file_options.repmgrd_standby_startup_timeout);
		sleep(1);
	}

	if (local_conn == NULL || PQstatus(local_conn) != CONNECTION_OK)
	{
		log_error(_("unable to reconnect to local node \"%s\" (ID: %i)"),
				  local_node_info.node_name,
				  local_node_info.node_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	/* refresh shared memory settings which will have been zapped by the restart */
	repmgrd_set_local_node_id(local_conn, config_file_options.node_id);
	repmgrd_set_pid(local_conn, getpid(), pid_file);

	{
		PQExpBufferData event_details;

		initPQExpBuffer(&event_details);
		appendPQExpBuffer(&event_details,
						  _("node \"%s\" (ID: %i) now following new upstream node \"%s\" (ID: %i)"),
						  local_node_info.node_name,
						  local_node_info.node_id,
						  upstream_node_info.node_name,
						  upstream_node_info.node_id);

		log_notice("%s", event_details.data);

		create_event_notification(upstream_conn,
								  &config_file_options,
								  local_node_info.node_id,
								  "repmgrd_failover_follow",
								  true,
								  event_details.data);

		termPQExpBuffer(&event_details);
	}

	return FAILOVER_STATE_FOLLOWED_NEW_PRIMARY;
}


static FailoverState
witness_follow_new_primary(int new_primary_id)
{
	t_node_info new_primary = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status = RECORD_NOT_FOUND;
	bool		new_primary_ok = false;

	record_status = get_node_record(local_conn, new_primary_id, &new_primary);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve metadata record for new primary node (ID: %i)"),
				  new_primary_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	/* TODO: check if new_primary_id == failed_primary.node_id? */

	if (log_type == REPMGR_STDERR && *config_file_options.log_file)
	{
		fflush(stderr);
	}

	upstream_conn = establish_db_connection(new_primary.conninfo, false);

	if (PQstatus(upstream_conn) == CONNECTION_OK)
	{
		RecoveryType primary_recovery_type = get_recovery_type(upstream_conn);

		switch (primary_recovery_type)
		{
			case RECTYPE_PRIMARY:
				new_primary_ok = true;
				break;
			case RECTYPE_STANDBY:
				new_primary_ok = false;
				log_warning(_("new primary \"%s\" (node ID: %i) is in recovery"),
							new_primary.node_name,
							new_primary_id);
				break;
			case RECTYPE_UNKNOWN:
				new_primary_ok = false;
				log_warning(_("unable to determine status of new primary"));
				break;
		}
	}

	if (new_primary_ok == false)
	{
		close_connection(&upstream_conn);

		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	/* set new upstream node ID on primary */
	update_node_record_set_upstream(upstream_conn, local_node_info.node_id, new_primary_id);

	witness_copy_node_records(upstream_conn, local_conn);

	/*
	 * refresh local copy of local and primary node records - we get these
	 * directly from the primary to ensure they're the current version
	 */

	record_status = get_node_record(upstream_conn, new_primary_id, &upstream_node_info);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve metadata record found for node %i"),
				  new_primary_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	record_status = get_node_record(upstream_conn, local_node_info.node_id, &local_node_info);
	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve metadata record for node %i"),
				  local_node_info.node_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	{
		PQExpBufferData event_details;

		initPQExpBuffer(&event_details);
		appendPQExpBuffer(&event_details,
						  _("witness node \"%s\" (ID: %i) now following new primary node \"%s\" (ID: %i)"),
						  local_node_info.node_name,
						  local_node_info.node_id,
						  upstream_node_info.node_name,
						  upstream_node_info.node_id);

		log_notice("%s", event_details.data);

		create_event_notification(upstream_conn,
								  &config_file_options,
								  local_node_info.node_id,
								  "repmgrd_failover_follow",
								  true,
								  event_details.data);

		termPQExpBuffer(&event_details);
	}

	return FAILOVER_STATE_FOLLOWED_NEW_PRIMARY;
}


static const char *
_print_election_result(ElectionResult result)
{
	switch (result)
	{
		case ELECTION_NOT_CANDIDATE:
			return "NOT CANDIDATE";

		case ELECTION_WON:
			return "WON";

		case ELECTION_LOST:
			return "LOST";

		case ELECTION_CANCELLED:
			return "CANCELLED";

		case ELECTION_RERUN:
			return "RERUN";
	}

	/* should never reach here */
	return "UNKNOWN";
}


/*
 * Failover decision for nodes attached to the current primary.
 *
 * NB: this function sets "sibling_nodes"; caller (do_primary_failover)
 * expects to be able to read this list
 */
static ElectionResult
do_election(NodeInfoList *sibling_nodes, int *new_primary_id)
{
	int			electoral_term = -1;

	NodeInfoListCell *cell = NULL;

	t_node_info *candidate_node = NULL;
	election_stats stats;

	ReplInfo	local_replication_info;

	/* To collate details of nodes with primary visible for logging purposes */
	PQExpBufferData nodes_with_primary_visible;

	/*
	 * Check if at least one server in the primary's location is visible; if
	 * not we'll assume a network split between this node and the primary
	 * location, and not promote any standby.
	 *
	 * NOTE: this function is only ever called by standbys attached to the
	 * current (unreachable) primary, so "upstream_node_info" will always
	 * contain the primary node record.
	 */
	bool		primary_location_seen = false;


	int			nodes_with_primary_still_visible = 0;

	if (config_file_options.failover_delay > 0)
	{
		log_debug("sleeping %i seconds (\"failover_delay\") before initiating failover",
				  config_file_options.failover_delay);
		sleep(config_file_options.failover_delay);
	}

	/* we're visible */
	stats.visible_nodes = 1;
	stats.shared_upstream_nodes = 0;
	stats.all_nodes = 0;

	electoral_term = get_current_term(local_conn);

	if (electoral_term == -1)
	{
		log_error(_("unable to determine electoral term"));

		return ELECTION_NOT_CANDIDATE;
	}

	log_debug("do_election(): electoral term is %i", electoral_term);

	if (config_file_options.failover == FAILOVER_MANUAL)
	{
		log_notice(_("this node is not configured for automatic failover so will not be considered as promotion candidate, and will not follow the new primary"));
		log_detail(_("\"failover\" is set to \"manual\" in repmgr.conf"));
		log_hint(_("manually execute \"repmgr standby follow\" to have this node follow the new primary"));

		return ELECTION_NOT_CANDIDATE;
	}

	/* node priority is set to zero - don't become a candidate, and lose by default */
	if (local_node_info.priority <= 0)
	{
		log_notice(_("this node's priority is %i so will not be considered as an automatic promotion candidate"),
				   local_node_info.priority);

		return ELECTION_LOST;
	}

	/* get all active nodes attached to upstream, excluding self */
	get_active_sibling_node_records(local_conn,
									local_node_info.node_id,
									upstream_node_info.node_id,
									sibling_nodes);

	log_info(_("%i active sibling nodes registered"), sibling_nodes->node_count);

	stats.shared_upstream_nodes = sibling_nodes->node_count + 1;

	get_all_nodes_count(local_conn, &stats.all_nodes);

	log_info(_("%i total nodes registered"), stats.all_nodes);

	if (strncmp(upstream_node_info.location, local_node_info.location, MAXLEN) != 0)
	{
		log_info(_("primary node \"%s\" (ID: %i) has location \"%s\", this node's location is \"%s\""),
				 upstream_node_info.node_name,
				 upstream_node_info.node_id,
				 upstream_node_info.location,
				 local_node_info.location);
	}
	else
	{
		log_info(_("primary node  \"%s\" (ID: %i) and this node have the same location (\"%s\")"),
				 upstream_node_info.node_name,
				 upstream_node_info.node_id,
				 local_node_info.location);
	}

	local_node_info.last_wal_receive_lsn = InvalidXLogRecPtr;

	/* fast path if no other standbys (or witness) exists - normally win by default */
	if (sibling_nodes->node_count == 0)
	{
		if (strncmp(upstream_node_info.location, local_node_info.location, MAXLEN) == 0)
		{
			if (config_file_options.failover_validation_command[0] != '\0')
			{
				return execute_failover_validation_command(&local_node_info, &stats);
			}

			log_info(_("no other sibling nodes - we win by default"));

			return ELECTION_WON;
		}
		else
		{
			/*
			 * If primary and standby have different locations set, the assumption
			 * is that no action should be taken as we can't tell whether there's
			 * been a network interruption or not.
			 *
			 * Normally a situation with primary and standby in different physical
			 * locations would be handled by leaving the location as "default" and
			 * setting up a witness server in the primary's location.
			 */
			log_debug("no other nodes, but primary and standby locations differ");

			monitoring_state = MS_DEGRADED;
			INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

			return ELECTION_NOT_CANDIDATE;
		}
	}
	else
	{
		/* standby nodes found - check if we're in the primary location before checking theirs */
		if (strncmp(upstream_node_info.location, local_node_info.location, MAXLEN) == 0)
		{
			primary_location_seen = true;
		}
	}

	/* get our lsn */
	if (get_replication_info(local_conn, STANDBY, &local_replication_info) == false)
	{
		log_error(_("unable to retrieve replication information for local node"));
		return ELECTION_LOST;
	}

	/* check if WAL replay on local node is paused */
	if (local_replication_info.wal_replay_paused == true)
	{
		log_debug("WAL replay is paused");
		if (local_replication_info.last_wal_receive_lsn > local_replication_info.last_wal_replay_lsn)
		{
			log_warning(_("WAL replay on this node is paused and WAL is pending replay"));
			log_detail(_("replay paused at %X/%X; last WAL received is %X/%X"),
					   format_lsn(local_replication_info.last_wal_replay_lsn),
					   format_lsn(local_replication_info.last_wal_receive_lsn));
		}

		/* attempt to resume WAL replay - unlikely this will fail, but just in case */
		if (resume_wal_replay(local_conn) == false)
		{
			log_error(_("unable to resume WAL replay"));
			log_detail(_("this node cannot be reliably promoted"));
			return ELECTION_LOST;
		}

		log_notice(_("WAL replay forcibly resumed"));
	}

	local_node_info.last_wal_receive_lsn = local_replication_info.last_wal_receive_lsn;

	log_info(_("local node's last receive lsn: %X/%X"), format_lsn(local_node_info.last_wal_receive_lsn));

	/* pointer to "winning" node, initially self */
	candidate_node = &local_node_info;

	initPQExpBuffer(&nodes_with_primary_visible);

	for (cell = sibling_nodes->head; cell; cell = cell->next)
	{
		ReplInfo	sibling_replication_info;

		log_info(_("checking state of sibling node \"%s\" (ID: %i)"),
				 cell->node_info->node_name,
				 cell->node_info->node_id);

		/* assume the worst case */
		cell->node_info->node_status = NODE_STATUS_UNKNOWN;

		cell->node_info->conn = establish_db_connection(cell->node_info->conninfo, false);

		if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
		{
			close_connection(&cell->node_info->conn);

			continue;
		}

		cell->node_info->node_status = NODE_STATUS_UP;

		stats.visible_nodes++;

		/*
		 * see if the node is in the primary's location (but skip the check if
		 * we've seen a node there already)
		 */
		if (primary_location_seen == false)
		{
			if (strncmp(cell->node_info->location, upstream_node_info.location, MAXLEN) == 0)
			{
				log_debug("node %i in primary location \"%s\"",
						  cell->node_info->node_id,
						  cell->node_info->location);
				primary_location_seen = true;
			}
		}

		/*
		 * check if repmgrd running - skip if not
		 *
		 * TODO: include pid query in replication info query?
		 *
		 * NOTE: from Pg12 we could execute "pg_promote()" from a running repmgrd;
		 * here we'll need to find a way of ensuring only one repmgrd does this
		 */
		if (repmgrd_get_pid(cell->node_info->conn) == UNKNOWN_PID)
		{
			log_warning(_("repmgrd not running on node \"%s\" (ID: %i), skipping"),
						cell->node_info->node_name,
						cell->node_info->node_id);
			continue;
		}

		if (get_replication_info(cell->node_info->conn, cell->node_info->type, &sibling_replication_info) == false)
		{
			log_warning(_("unable to retrieve replication information for node \"%s\" (ID: %i), skipping"),
						cell->node_info->node_name,
						cell->node_info->node_id);
			continue;
		}

		/*
		 * Check if node is not in recovery - it may have been promoted
		 * outside of the failover mechanism, in which case we may be able
		 * to follow it.
		 */

		if (sibling_replication_info.in_recovery == false && cell->node_info->type != WITNESS)
		{
			bool can_follow;

			log_warning(_("node \"%s\" (ID: %i) is not in recovery"),
						cell->node_info->node_name,
						cell->node_info->node_id);

			/*
			 * Node is not in recovery, but still reporting an upstream
			 * node ID; possible it was promoted manually (e.g. with "pg_ctl promote"),
			 * or (less likely) the node's repmgrd has just switched to primary
			 * monitoring node but has not yet unset the upstream node ID in
			 * shared memory. Either way, log this.
			 */
			if (sibling_replication_info.upstream_node_id != UNKNOWN_NODE_ID)
			{
				log_warning(_("node \"%s\" (ID: %i) still reports its upstream is node %i, last seen %i second(s) ago"),
							cell->node_info->node_name,
							cell->node_info->node_id,
							sibling_replication_info.upstream_node_id,
							sibling_replication_info.upstream_last_seen);
			}
			can_follow = check_node_can_follow(local_conn,
											   local_node_info.last_wal_receive_lsn,
											   cell->node_info->conn,
											   cell->node_info);

			if (can_follow == true)
			{
				*new_primary_id = cell->node_info->node_id;
				termPQExpBuffer(&nodes_with_primary_visible);
				return ELECTION_CANCELLED;
			}

			/*
			 * Tricky situation here - we'll assume the node is a rogue primary
			 */
			log_warning(_("not possible to attach to node \"%s\" (ID: %i), ignoring"),
						cell->node_info->node_name,
						cell->node_info->node_id);
			continue;
		}
		else
		{
			log_info(_("node \"%s\" (ID: %i) reports its upstream is node %i, last seen %i second(s) ago"),
					 cell->node_info->node_name,
					 cell->node_info->node_id,
					 sibling_replication_info.upstream_node_id,
					 sibling_replication_info.upstream_last_seen);
		}

		/* check if WAL replay on node is paused */
		if (sibling_replication_info.wal_replay_paused == true)
		{
			/*
			 * Theoretically the repmgrd on the node should have resumed WAL play
			 * at this point.
			 */
			if (sibling_replication_info.last_wal_receive_lsn > sibling_replication_info.last_wal_replay_lsn)
			{
				log_warning(_("WAL replay on node \"%s\" (ID: %i) is paused and WAL is pending replay"),
							cell->node_info->node_name,
							cell->node_info->node_id);
			}
		}

		/*
		 * Check if node has seen primary "recently" - if so, we may have "partial primary visibility".
		 * For now we'll assume the primary is visible if it's been seen less than
		 * monitor_interval_secs * 2 seconds ago. We may need to adjust this, and/or make the value
		 * configurable.
		 */

		if (sibling_replication_info.upstream_last_seen >= 0 && sibling_replication_info.upstream_last_seen < (config_file_options.monitor_interval_secs * 2))
		{
			if (sibling_replication_info.upstream_node_id != upstream_node_info.node_id)
			{
				log_warning(_("assumed sibling node \"%s\" (ID: %i) monitoring different upstream node %i"),
							cell->node_info->node_name,
							cell->node_info->node_id,
							sibling_replication_info.upstream_node_id);

			}
			else
			{
				nodes_with_primary_still_visible++;
				log_notice(_("%s node \"%s\" (ID: %i) last saw primary node %i second(s) ago, considering primary still visible"),
						   get_node_type_string(cell->node_info->type),
						   cell->node_info->node_name,
						   cell->node_info->node_id,
						   sibling_replication_info.upstream_last_seen);
				appendPQExpBuffer(&nodes_with_primary_visible,
								  " - node \"%s\" (ID: %i): %i second(s) ago\n",
								  cell->node_info->node_name,
								  cell->node_info->node_id,
								  sibling_replication_info.upstream_last_seen);
			}
		}
		else
		{
			log_info(_("%s node \"%s\" (ID: %i) last saw primary node %i second(s) ago"),
					 get_node_type_string(cell->node_info->type),
					 cell->node_info->node_name,
					 cell->node_info->node_id,
					 sibling_replication_info.upstream_last_seen);
		}


		/* don't interrogate a witness server */
		if (cell->node_info->type == WITNESS)
		{
			log_debug("node %i is witness, not querying state", cell->node_info->node_id);
			continue;
		}

		/* don't check 0-priority nodes */
		if (cell->node_info->priority <= 0)
		{
			log_info(_("node \"%s\" (ID: %i) has priority of %i, skipping"),
					   cell->node_info->node_name,
					   cell->node_info->node_id,
					   cell->node_info->priority);
			continue;
		}


		/* get node's last receive LSN - if "higher" than current winner, current node is candidate */
		cell->node_info->last_wal_receive_lsn = sibling_replication_info.last_wal_receive_lsn;

		log_info(_("last receive LSN for sibling node \"%s\" (ID: %i) is: %X/%X"),
				 cell->node_info->node_name,
				 cell->node_info->node_id,
				 format_lsn(cell->node_info->last_wal_receive_lsn));

		/* compare LSN */
		if (cell->node_info->last_wal_receive_lsn > candidate_node->last_wal_receive_lsn)
		{
			/* other node is ahead */
			log_info(_("node \"%s\" (ID: %i) is ahead of current candidate \"%s\" (ID: %i)"),
					 cell->node_info->node_name,
					 cell->node_info->node_id,
					 candidate_node->node_name,
					 candidate_node->node_id);

			candidate_node = cell->node_info;
		}
		/* LSN is same - tiebreak on priority, then node_id */
		else if (cell->node_info->last_wal_receive_lsn == candidate_node->last_wal_receive_lsn)
		{
			log_info(_("node \"%s\" (ID: %i) has same LSN as current candidate \"%s\" (ID: %i)"),
					 cell->node_info->node_name,
					 cell->node_info->node_id,
					 candidate_node->node_name,
					 candidate_node->node_id);

			if (cell->node_info->priority > candidate_node->priority)
			{
				log_info(_("node \"%s\" (ID: %i) has higher priority (%i) than current candidate \"%s\" (ID: %i) (%i)"),
						 cell->node_info->node_name,
						 cell->node_info->node_id,
						 cell->node_info->priority,
						 candidate_node->node_name,
						 candidate_node->node_id,
						 candidate_node->priority);

				candidate_node = cell->node_info;
			}
			else if (cell->node_info->priority == candidate_node->priority)
			{
				if (cell->node_info->node_id < candidate_node->node_id)
				{
					log_info(_("node \"%s\" (ID: %i) has same priority but lower node_id than current candidate \"%s\" (ID: %i)"),
							 cell->node_info->node_name,
							 cell->node_info->node_id,
							 candidate_node->node_name,
							 candidate_node->node_id);

					candidate_node = cell->node_info;
				}
			}
			else
			{
				log_info(_("node \"%s\" (ID: %i) has lower priority (%i) than current candidate \"%s\" (ID: %i) (%i)"),
						 cell->node_info->node_name,
						 cell->node_info->node_id,
						 cell->node_info->priority,
						 candidate_node->node_name,
						 candidate_node->node_id,
						 candidate_node->priority);
			}
		}
	}

	if (primary_location_seen == false)
	{
		log_notice(_("no nodes from the primary location \"%s\" visible - assuming network split"),
				   upstream_node_info.location);
		log_detail(_("node will enter degraded monitoring state waiting for reconnect"));

		monitoring_state = MS_DEGRADED;
		INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

		reset_node_voting_status();

		termPQExpBuffer(&nodes_with_primary_visible);

		return ELECTION_CANCELLED;
	}

	if (nodes_with_primary_still_visible > 0)
	{
		log_info(_("%i nodes can see the primary"),
				   nodes_with_primary_still_visible);

		log_detail(_("following nodes can see the primary:\n%s"),
				   nodes_with_primary_visible.data);

		if (config_file_options.primary_visibility_consensus == true)
		{
			log_notice(_("cancelling failover as some nodes can still see the primary"));
			monitoring_state = MS_DEGRADED;
			INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

			reset_node_voting_status();

			termPQExpBuffer(&nodes_with_primary_visible);

			return ELECTION_CANCELLED;
		}
	}

	termPQExpBuffer(&nodes_with_primary_visible);

	log_info(_("visible nodes: %i; total nodes: %i; no nodes have seen the primary within the last %i seconds"),
			 stats.visible_nodes,
			 stats.shared_upstream_nodes,
			 (config_file_options.monitor_interval_secs * 2));

	if (stats.visible_nodes <= (stats.shared_upstream_nodes / 2.0))
	{
		log_notice(_("unable to reach a qualified majority of nodes"));
		log_detail(_("node will enter degraded monitoring state waiting for reconnect"));

		monitoring_state = MS_DEGRADED;
		INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

		reset_node_voting_status();

		return ELECTION_CANCELLED;
	}

	log_notice(_("promotion candidate is \"%s\" (ID: %i)"),
			   candidate_node->node_name,
			   candidate_node->node_id);

	if (candidate_node->node_id == local_node_info.node_id)
	{
		/*
		 * If "failover_validation_command" is set, execute that command
		 * and decide the result based on the command's output
		 */

		if (config_file_options.failover_validation_command[0] != '\0')
		{
			return execute_failover_validation_command(candidate_node, &stats);
		}

		return ELECTION_WON;
	}

	return ELECTION_LOST;
}

/*
 * "failover" for the witness node; the witness has no part in the election
 * other than being reachable, so just needs to await notification from the
 * new primary
 */
static
bool do_witness_failover(void)
{
	int new_primary_id = UNKNOWN_NODE_ID;

	/* TODO add pre-event notification here */
	failover_state = FAILOVER_STATE_UNKNOWN;

	if (wait_primary_notification(&new_primary_id) == true)
	{
		/* if primary has reappeared, no action needed */
		if (new_primary_id == upstream_node_info.node_id)
		{
			failover_state = FAILOVER_STATE_FOLLOWING_ORIGINAL_PRIMARY;
		}
		else
		{
			failover_state = witness_follow_new_primary(new_primary_id);
		}
	}
	else
	{
		failover_state = FAILOVER_STATE_NO_NEW_PRIMARY;
	}


	log_verbose(LOG_DEBUG, "failover state is %s",
				format_failover_state(failover_state));

	switch (failover_state)
	{
		case FAILOVER_STATE_PRIMARY_REAPPEARED:
			/* pass control back down to start_monitoring() */
			log_info(_("resuming %s monitoring mode"),get_node_type_string(local_node_info.type));
			log_detail(_("original primary \"%s\" (ID: %i) reappeared"),
					   upstream_node_info.node_name, upstream_node_info.node_id);

			failover_state = FAILOVER_STATE_NONE;
			return true;


		case FAILOVER_STATE_FOLLOWED_NEW_PRIMARY:
			log_info(_("resuming %s monitoring mode"),get_node_type_string(local_node_info.type));
			log_detail(_("following new primary \"%s\" (ID: %i)"),
					   upstream_node_info.node_name, upstream_node_info.node_id);
			failover_state = FAILOVER_STATE_NONE;

			return true;

		case FAILOVER_STATE_FOLLOWING_ORIGINAL_PRIMARY:
			log_info(_("resuming %s monitoring mode"),get_node_type_string(local_node_info.type));
			log_detail(_("following original primary \"%s\" (ID: %i)"),
					   upstream_node_info.node_name, upstream_node_info.node_id);
			failover_state = FAILOVER_STATE_NONE;

			return true;

		case FAILOVER_STATE_FOLLOW_FAIL:
			/*
			 * for whatever reason we were unable to follow the new primary -
			 * continue monitoring in degraded state
			 */
			monitoring_state = MS_DEGRADED;
			INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

			return false;

		default:
			monitoring_state = MS_DEGRADED;
			INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

			return false;
	}
	/* should never reach here */
	return false;
}


static void
reset_node_voting_status(void)
{
	failover_state = FAILOVER_STATE_NONE;

	if (PQstatus(local_conn) != CONNECTION_OK)
	{
		log_error(_("reset_node_voting_status(): local_conn not set"));
		log_detail("\n%s", PQerrorMessage(local_conn));
		return;
	}
	reset_voting_status(local_conn);
}


static void
check_connection(t_node_info *node_info, PGconn **conn)
{
	if (is_server_available(node_info->conninfo) == false)
	{
		log_warning(_("connection to node \"%s\" (ID: %i) lost"),
					node_info->node_name,
					node_info->node_id);
		log_detail("\n%s", PQerrorMessage(*conn));

		close_connection(conn);
	}

	if (PQstatus(*conn) != CONNECTION_OK)
	{
		log_info(_("attempting to reconnect to node \"%s\" (ID: %i)"),
				 node_info->node_name,
				 node_info->node_id);

		close_connection(conn);

		*conn = establish_db_connection(node_info->conninfo, false);

		if (PQstatus(*conn) != CONNECTION_OK)
		{
			close_connection(conn);

			log_warning(_("reconnection to node \"%s\" (ID: %i) failed"),
						node_info->node_name,
						node_info->node_id);
		}
		else
		{
			int 		stored_local_node_id = UNKNOWN_NODE_ID;

			log_info(_("reconnected to node \"%s\" (ID: %i)"),
					 node_info->node_name,
					 node_info->node_id);

			stored_local_node_id = repmgrd_get_local_node_id(*conn);
			if (stored_local_node_id == UNKNOWN_NODE_ID)
			{
				repmgrd_set_local_node_id(*conn, config_file_options.node_id);
				repmgrd_set_pid(local_conn, getpid(), pid_file);
			}

		}
	}
}


static const char *
format_failover_state(FailoverState failover_state)
{
	switch(failover_state)
	{
		case FAILOVER_STATE_UNKNOWN:
			return "UNKNOWN";
		case FAILOVER_STATE_NONE:
			return "NONE";
		case FAILOVER_STATE_PROMOTED:
			return "PROMOTED";
		case FAILOVER_STATE_PROMOTION_FAILED:
			return "PROMOTION_FAILED";
		case FAILOVER_STATE_PRIMARY_REAPPEARED:
			return "PRIMARY_REAPPEARED";
		case FAILOVER_STATE_LOCAL_NODE_FAILURE:
			return "LOCAL_NODE_FAILURE";
		case FAILOVER_STATE_WAITING_NEW_PRIMARY:
			return "WAITING_NEW_PRIMARY";
		case FAILOVER_STATE_FOLLOW_NEW_PRIMARY:
			return "FOLLOW_NEW_PRIMARY";
		case FAILOVER_STATE_REQUIRES_MANUAL_FAILOVER:
			return "REQUIRES_MANUAL_FAILOVER";
		case FAILOVER_STATE_FOLLOWED_NEW_PRIMARY:
			return "FOLLOWED_NEW_PRIMARY";
		case FAILOVER_STATE_FOLLOWING_ORIGINAL_PRIMARY:
			return "FOLLOWING_ORIGINAL_PRIMARY";
		case FAILOVER_STATE_NO_NEW_PRIMARY:
			return "NO_NEW_PRIMARY";
		case FAILOVER_STATE_FOLLOW_FAIL:
			return "FOLLOW_FAIL";
		case FAILOVER_STATE_NODE_NOTIFICATION_ERROR:
			return "NODE_NOTIFICATION_ERROR";
		case FAILOVER_STATE_ELECTION_RERUN:
			return "ELECTION_RERUN";
	}

	/* should never reach here */
	return "UNKNOWN_FAILOVER_STATE";
}


static void
handle_sighup(PGconn **conn, t_server_type server_type)
{
	log_notice(_("received SIGHUP, reloading configuration"));

	if (reload_config(server_type))
	{
		close_connection(conn);

		*conn = establish_db_connection(config_file_options.conninfo, true);
	}

	if (*config_file_options.log_file)
	{
		FILE	   *fd;

		log_debug("reopening %s", config_file_options.log_file);

		fd = freopen(config_file_options.log_file, "a", stderr);
		if (fd == NULL)
		{
			fprintf(stderr, "error reopening stderr to \"%s\": %s",
					config_file_options.log_file, strerror(errno));
		}
	}

	got_SIGHUP = false;
}

static ElectionResult
execute_failover_validation_command(t_node_info *node_info, election_stats *stats)
{
	PQExpBufferData failover_validation_command;
	PQExpBufferData command_output;
	int return_value = -1;

	initPQExpBuffer(&failover_validation_command);
	initPQExpBuffer(&command_output);

	parse_failover_validation_command(config_file_options.failover_validation_command,
									  node_info,
									  stats,
									  &failover_validation_command);

	log_notice(_("executing \"failover_validation_command\""));
	log_detail("%s", failover_validation_command.data);

	/* we determine success of the command by the value placed into return_value */
	(void) local_command_return_value(failover_validation_command.data,
									  &command_output,
									  &return_value);

	termPQExpBuffer(&failover_validation_command);

	if (command_output.data[0] != '\0')
	{
		log_info("output returned by failover validation command:\n%s", command_output.data);
	}
	else
	{
		log_info(_("no output returned from command"));
	}

	termPQExpBuffer(&command_output);

	if (return_value != 0)
	{
		/* create event here? */
		log_notice(_("failover validation command returned a non-zero value: %i"),
				   return_value);
		return ELECTION_RERUN;
	}

	log_notice(_("failover validation command returned zero"));

	return ELECTION_WON;
}


static void
parse_failover_validation_command(const char *template, t_node_info *node_info, election_stats *stats, PQExpBufferData *out)
{
	const char *src_ptr;

	for (src_ptr = template; *src_ptr; src_ptr++)
	{
		if (*src_ptr == '%')
		{
			switch (src_ptr[1])
			{
				case '%':
					/* %%: replace with % */
					src_ptr++;
					appendPQExpBufferChar(out, *src_ptr);
					break;
				case 'n':
					/* %n: node id */
					src_ptr++;
					appendPQExpBuffer(out, "%i", node_info->node_id);
					break;
				case 'a':
					/* %a: node name */
					src_ptr++;
					appendPQExpBufferStr(out, node_info->node_name);
					break;
				case 'v':
					/* %v: visible nodes count */
					src_ptr++;
					appendPQExpBuffer(out, "%i", stats->visible_nodes);
					break;
				case 'u':
					/* %u: shared upstream nodes count */
					src_ptr++;
					appendPQExpBuffer(out, "%i", stats->shared_upstream_nodes);
					break;
				case 't':
					/* %t: total nodes count */
					src_ptr++;
					appendPQExpBuffer(out, "%i", stats->all_nodes);
					break;

				default:
					/* otherwise treat the % as not special */
					appendPQExpBufferChar(out, *src_ptr);

					break;
			}
		}
		else
		{
			appendPQExpBufferChar(out, *src_ptr);
		}
	}

	return;
}


/*
 * Sanity-check whether the local node can follow the proposed upstream node.
 *
 * Note this function is very similar to check_node_can_attach() in
 * repmgr-client.c, however the later is very focussed on client-side
 * functionality (including log output related to --dry-run, pg_rewind etc.)
 * which we don't want here.
 */
static bool
check_node_can_follow(PGconn *local_conn, XLogRecPtr local_xlogpos, PGconn *follow_target_conn, t_node_info *follow_target_node_info)
{
	PGconn	   *local_repl_conn = NULL;
	t_system_identification local_identification = T_SYSTEM_IDENTIFICATION_INITIALIZER;

	PGconn	   *follow_target_repl_conn = NULL;
	t_system_identification follow_target_identification = T_SYSTEM_IDENTIFICATION_INITIALIZER;
	TimeLineHistoryEntry *follow_target_history = NULL;

	bool can_follow = true;
	bool success;

	local_repl_conn = establish_replication_connection_from_conn(local_conn, local_node_info.repluser);

	if (PQstatus(local_repl_conn) != CONNECTION_OK)
	{
		log_error(_("unable to establish a replication connection to the local node"));
		PQfinish(local_repl_conn);

		return false;
	}

	success = identify_system(local_repl_conn, &local_identification);
	PQfinish(local_repl_conn);

	if (success == false)
	{
		log_error(_("unable to query the local node's system identification"));

		return false;
	}

	/* check replication connection */
	follow_target_repl_conn = establish_replication_connection_from_conn(follow_target_conn,
																		 follow_target_node_info->repluser);
	if (PQstatus(follow_target_repl_conn) != CONNECTION_OK)
	{
		log_error(_("unable to establish a replication connection to the follow target node"));

		PQfinish(follow_target_repl_conn);
		return false;
	}

	/* check system_identifiers match */
	if (identify_system(follow_target_repl_conn, &follow_target_identification) == false)
	{
		log_error(_("unable to query the follow target node's system identification"));

		PQfinish(follow_target_repl_conn);
		return false;
	}

	/*
	 * Check for thing that should never happen, but expect the unexpected anyway.
	 */
	if (follow_target_identification.system_identifier != local_identification.system_identifier)
	{
		log_error(_("this node is not part of the follow target node's replication cluster"));
		log_detail(_("this node's system identifier is %lu, follow target node's system identifier is %lu"),
				   local_identification.system_identifier,
				   follow_target_identification.system_identifier);
		PQfinish(follow_target_repl_conn);
		return false;
	}

	/* check timelines */

	log_verbose(LOG_DEBUG, "local timeline: %i; follow target timeline: %i",
				local_identification.timeline,
				follow_target_identification.timeline);

	/* upstream's timeline is lower than ours - impossible case */
	if (follow_target_identification.timeline < local_identification.timeline)
	{
		log_error(_("this node's timeline is ahead of the follow target node's timeline"));
		log_detail(_("this node's timeline is %i, follow target node's timeline is %i"),
				   local_identification.timeline,
				   follow_target_identification.timeline);
		PQfinish(follow_target_repl_conn);
		return false;
	}

	/* timeline is the same - check relative positions */
	if (follow_target_identification.timeline == local_identification.timeline)
	{
		XLogRecPtr follow_target_xlogpos = get_node_current_lsn(follow_target_conn);

		if (local_xlogpos == InvalidXLogRecPtr || follow_target_xlogpos == InvalidXLogRecPtr)
		{
			log_error(_("unable to compare LSN positions"));
			PQfinish(follow_target_repl_conn);
			return false;
		}

		if (local_xlogpos <= follow_target_xlogpos)
		{
			log_info(_("timelines are same, this server is not ahead"));
			log_detail(_("local node lsn is %X/%X, follow target lsn is %X/%X"),
					   format_lsn(local_xlogpos),
					   format_lsn(follow_target_xlogpos));
		}
		else
		{
			log_error(_("this node is ahead of the follow target"));
			log_detail(_("local node lsn is %X/%X, follow target lsn is %X/%X"),
					   format_lsn(local_xlogpos),
					   format_lsn(follow_target_xlogpos));

			can_follow = false;
		}
	}
	else
	{
		/*
		 * upstream has higher timeline - check where it forked off from this node's timeline
		 */
		follow_target_history = get_timeline_history(follow_target_repl_conn,
													 local_identification.timeline + 1);

		if (follow_target_history == NULL)
		{
			/* get_timeline_history() will emit relevant error messages */
			PQfinish(follow_target_repl_conn);
			return false;
		}

		log_debug("local tli: %i; local_xlogpos: %X/%X; follow_target_history->tli: %i; follow_target_history->end: %X/%X",
				  (int)local_identification.timeline,
				  format_lsn(local_xlogpos),
				  follow_target_history->tli,
				  format_lsn(follow_target_history->end));

		/*
		 * Local node has proceeded beyond the follow target's fork, so we
		 * definitely can't attach.
		 *
		 * This could be the case if the follow target was promoted, but does
		 * not contain all changes which are being replayed to this standby.
		 */
		if (local_xlogpos > follow_target_history->end)
		{
			log_error(_("this node cannot attach to follow target node \"%s\" (ID: %i)"),
					  follow_target_node_info->node_name,
					  follow_target_node_info->node_id);
			can_follow = false;

			log_detail(_("follow target server's timeline %lu forked off current database system timeline %lu before current recovery point %X/%X"),
					   local_identification.system_identifier + 1,
					   local_identification.system_identifier,
					   format_lsn(local_xlogpos));
		}

		if (can_follow == true)
		{
			log_info(_("local node \"%s\" (ID: %i) can attach to follow target node \"%s\" (ID: %i)"),
					 config_file_options.node_name,
					 config_file_options.node_id,
					 follow_target_node_info->node_name,
					 follow_target_node_info->node_id);

			log_detail(_("local node's recovery point: %X/%X; follow target node's fork point: %X/%X"),
					   format_lsn(local_xlogpos),
					   format_lsn(follow_target_history->end));
		}
	}

	PQfinish(follow_target_repl_conn);

	if (follow_target_history)
		pfree(follow_target_history);

	return can_follow;
}


static void
check_witness_attached(t_node_info *node_info, bool startup)
{
	/*
	 * connect and check upstream node id; at this point we don't care if it's
	 * not reachable, only whether we can mark it as attached or not.
	 */
	PGconn *witness_conn = establish_db_connection_quiet(node_info->conninfo);

	if (PQstatus(witness_conn) == CONNECTION_OK)
	{
		int witness_upstream_node_id = repmgrd_get_upstream_node_id(witness_conn);

		log_debug("witness node %i's upstream node ID reported as %i",
				  node_info->node_id,
				  witness_upstream_node_id);

		if (witness_upstream_node_id == local_node_info.node_id)
		{
			node_info->attached = NODE_ATTACHED;
		}
		else
		{
			node_info->attached = NODE_DETACHED;
		}
	}
	else
	{
		node_info->attached = startup == true ? NODE_ATTACHED_UNKNOWN : NODE_DETACHED;
	}

	PQfinish(witness_conn);
}


static t_child_node_info *
append_child_node_record(t_child_node_info_list *nodes, int node_id, const char *node_name, t_server_type type, NodeAttached attached)
{
	t_child_node_info *child_node = pg_malloc0(sizeof(t_child_node_info));

	child_node->node_id = node_id;
	snprintf(child_node->node_name, sizeof(child_node->node_name), "%s", node_name);

	child_node->type = type;
	child_node->attached = attached;

	if (nodes->tail)
		nodes->tail->next = child_node;
	else
		nodes->head = child_node;

	nodes->tail = child_node;
	nodes->node_count++;

	return child_node;
}


static void
remove_child_node_record(t_child_node_info_list *nodes, int node_id)
{
	t_child_node_info *node;
	t_child_node_info *prev_node = NULL;
	t_child_node_info *next_node = NULL;

	node = nodes->head;

	while (node != NULL)
	{
		next_node = node->next;

		if (node->node_id == node_id)
		{
			/* first node */
			if (node == nodes->head)
			{
				nodes->head = next_node;
			}
			/* last node */
			else if (next_node == NULL)
			{
				prev_node->next = NULL;
			}
			else
			{
				prev_node->next = next_node;
			}
			pfree(node);
			nodes->node_count--;
			return;
		}
		else
		{
			prev_node = node;
		}
		node = next_node;
	}
}

static void
clear_child_node_info_list(t_child_node_info_list *nodes)
{
	t_child_node_info *node;
	t_child_node_info *next_node;

	node = nodes->head;

	while (node != NULL)
	{
		next_node = node->next;
		pfree(node);
		node = next_node;
	}

	nodes->head = NULL;
	nodes->tail = NULL;
	nodes->node_count = 0;
}


static void
parse_child_nodes_disconnect_command(char *parsed_command, char *template, int reporting_node_id)
{
	const char *src_ptr = NULL;
	char	   *dst_ptr = NULL;
	char	   *end_ptr = NULL;

	dst_ptr = parsed_command;
	end_ptr = (parsed_command + MAXPGPATH) - 1;
	*end_ptr = '\0';

	for (src_ptr = template; *src_ptr; src_ptr++)
	{
		if (*src_ptr == '%')
		{
			switch (src_ptr[1])
			{
				case '%':
					/* %%: replace with % */
					if (dst_ptr < end_ptr)
					{
						src_ptr++;
						*dst_ptr++ = *src_ptr;
					}
					break;
				case 'p':
					/* %p: node id of the reporting primary */
					src_ptr++;
					snprintf(dst_ptr, end_ptr - dst_ptr, "%i", reporting_node_id);
					dst_ptr += strlen(dst_ptr);
					break;
			}
		}
		else
		{
			if (dst_ptr < end_ptr)
				*dst_ptr++ = *src_ptr;
		}
	}

	*dst_ptr = '\0';

	return;
}


int
try_primary_reconnect(PGconn **conn, PGconn *local_conn, t_node_info *node_info)
{
	t_conninfo_param_list conninfo_params = T_CONNINFO_PARAM_LIST_INITIALIZER;
	int			i;
	int			max_attempts = config_file_options.reconnect_attempts;

	initialize_conninfo_params(&conninfo_params, false);

	/* we assume by now the conninfo string is parseable */
	(void) parse_conninfo_string(node_info->conninfo, &conninfo_params, NULL, false);

	/* set some default values if not explicitly provided */
	param_set_ine(&conninfo_params, "connect_timeout", "2");
	param_set_ine(&conninfo_params, "fallback_application_name", "repmgr");

	for (i = 0; i < max_attempts; i++)
	{
		time_t started_at = time(NULL);
		int up_to;
		bool sleep_now = false;
		bool max_sleep_seconds;

		log_info(_("checking state of node \"%s\" (ID: %i), %i of %i attempts"),
				 node_info->node_name,
				 node_info->node_id,
				 i + 1, max_attempts);

		if (is_server_available_params(&conninfo_params) == true)
		{
			PGconn	   *our_conn;

			log_notice(_("node \"%s\" (ID: %i) has recovered, reconnecting"),
					   node_info->node_name,
					   node_info->node_id);

			/*
			 * Note: we could also handle the case where node is pingable but
			 * connection denied due to connection exhaustion, by falling back to
			 * degraded monitoring (make configurable)
			 */
			our_conn = establish_db_connection_by_params(&conninfo_params, false);

			if (PQstatus(our_conn) == CONNECTION_OK)
			{
				free_conninfo_params(&conninfo_params);

				log_info(_("connection to node \"%s\" (ID: %i) succeeded"),
						 node_info->node_name,
						 node_info->node_id);

				if (PQstatus(*conn) == CONNECTION_BAD)
				{
					log_verbose(LOG_INFO, _("original connection handle returned CONNECTION_BAD, using new connection"));
					close_connection(conn);
					*conn = our_conn;
				}
				else
				{
					ExecStatusType ping_result;

					ping_result = connection_ping(*conn);

					if (ping_result != PGRES_TUPLES_OK)
					{
						log_info(_("original connection no longer available, using new connection"));
						close_connection(conn);
						*conn = our_conn;
					}
					else
					{
						log_info(_("original connection is still available"));

						PQfinish(our_conn);
					}
				}

				node_info->node_status = NODE_STATUS_UP;

				return UNKNOWN_NODE_ID;
			}

			close_connection(&our_conn);
			log_notice(_("unable to reconnect to node \"%s\" (ID: %i)"),
					   node_info->node_name,
					   node_info->node_id);
		}

		/*
		 * Experimental behaviour, see GitHub #662.
		 */
		if (config_file_options.reconnect_loop_sync == true)
		{
			up_to = (time(NULL) - started_at);
			max_sleep_seconds = (up_to == 0)
				? config_file_options.reconnect_interval
				: (up_to % config_file_options.reconnect_interval);
			if (i + 1 <= max_attempts)
				sleep_now = true;
		}
		else
		{
			max_sleep_seconds = config_file_options.reconnect_interval;
			if (i + 1 < max_attempts)
				sleep_now = true;
		}

		if (sleep_now == true)
		{
			int j;
			log_info(_("sleeping up to %i seconds until next reconnection attempt"),
					 max_sleep_seconds);
			for (j = 0; j < max_sleep_seconds; j++)
			{
				int new_primary_node_id;
				if (get_new_primary(local_conn, &new_primary_node_id) == true && new_primary_node_id != UNKNOWN_NODE_ID)
				{
					if (new_primary_node_id == ELECTION_RERUN_NOTIFICATION)
					{
						log_notice(_("received rerun notification"));
					}
					else
					{
						log_notice(_("received notification that new primary is node %i"), new_primary_node_id);
					}

					free_conninfo_params(&conninfo_params);
					return new_primary_node_id;
				}
				sleep(1);
			}
		}
	}

	log_warning(_("unable to reconnect to node \"%s\" (ID: %i) after %i attempts"),
				node_info->node_name,
				node_info->node_id,
				max_attempts);

	node_info->node_status = NODE_STATUS_DOWN;

	free_conninfo_params(&conninfo_params);

	return UNKNOWN_NODE_ID;
}
