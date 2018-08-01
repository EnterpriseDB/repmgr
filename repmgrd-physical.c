/*
 * repmgrd-physical.c - physical (streaming) replication functionality for repmgrd
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
	FAILOVER_STATE_REQUIRES_MANUAL_FAILOVER,
	FAILOVER_STATE_FOLLOWED_NEW_PRIMARY,
	FAILOVER_STATE_FOLLOWING_ORIGINAL_PRIMARY,
	FAILOVER_STATE_NO_NEW_PRIMARY,
	FAILOVER_STATE_FOLLOW_FAIL,
	FAILOVER_STATE_NODE_NOTIFICATION_ERROR
} FailoverState;


typedef enum
{
	ELECTION_NOT_CANDIDATE = -1,
	ELECTION_WON,
	ELECTION_LOST,
	ELECTION_CANCELLED
} ElectionResult;


static PGconn *upstream_conn = NULL;
static PGconn *primary_conn = NULL;

static FailoverState failover_state = FAILOVER_STATE_UNKNOWN;

static int	primary_node_id = UNKNOWN_NODE_ID;
static t_node_info upstream_node_info = T_NODE_INFO_INITIALIZER;
static NodeInfoList sibling_nodes = T_NODE_INFO_LIST_INITIALIZER;


static ElectionResult do_election(void);
static const char *_print_election_result(ElectionResult result);

static FailoverState promote_self(void);
static void notify_followers(NodeInfoList *standby_nodes, int follow_node_id);

static void check_connection(t_node_info *node_info, PGconn **conn);

static bool wait_primary_notification(int *new_primary_id);
static FailoverState follow_new_primary(int new_primary_id);
static FailoverState witness_follow_new_primary(int new_primary_id);

static void reset_node_voting_status(void);

static bool do_primary_failover(void);
static bool do_upstream_standby_failover(void);
static bool do_witness_failover(void);

static void update_monitoring_history(void);

static void handle_sighup(PGconn *conn);

static const char * format_failover_state(FailoverState failover_state);


void
handle_sigint_physical(SIGNAL_ARGS)
{
	PGconn *writeable_conn;
	PQExpBufferData event_details;

	initPQExpBuffer(&event_details);

	appendPQExpBuffer(&event_details,
					  "%s signal received",
					  postgres_signal_arg == SIGTERM
					  ? "TERM" : "INT");

	if (local_node_info.type == PRIMARY)
		writeable_conn = local_conn;
	else
		writeable_conn = primary_conn;

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
				close_connection(&local_conn);

				create_event_notification(NULL,
										  &config_file_options,
										  config_file_options.node_id,
										  "repmgrd_shutdown",
										  false,
										  "node is inactive and cannot be used as a failover target");

				terminate(ERR_BAD_CONFIG);

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
			close_connection(&local_conn);
			exit(ERR_BAD_CONFIG);
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
	PQExpBufferData event_details;

	reset_node_voting_status();

	initPQExpBuffer(&event_details);

	appendPQExpBuffer(&event_details,
					  _("monitoring cluster primary \"%s\" (node ID: %i)"),
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

	INSTR_TIME_SET_CURRENT(log_status_interval_start);
	local_node_info.node_status = NODE_STATUS_UP;

	while (true)
	{
		/*
		 * TODO: cache node list here, refresh at `node_list_refresh_interval`
		 * also return reason for inavailability so we can log it
		 */
		if (is_server_available(local_node_info.conninfo) == false)
		{

			/* local node is down, we were expecting it to be up */
			if (local_node_info.node_status == NODE_STATUS_UP)
			{
				PQExpBufferData event_details;
				instr_time	local_node_unreachable_start;

				INSTR_TIME_SET_CURRENT(local_node_unreachable_start);

				initPQExpBuffer(&event_details);

				appendPQExpBuffer(&event_details,
								  _("unable to connect to local node"));

				log_warning("%s", event_details.data);

				local_node_info.node_status = NODE_STATUS_UNKNOWN;

				close_connection(&local_conn);

				/*
				 * as we're monitoring the primary, no point in trying to
				 * write the event to the database
				 *
				 * XXX possible pre-action event
				 */
				create_event_notification(NULL,
										  &config_file_options,
										  config_file_options.node_id,
										  "repmgrd_local_disconnect",
										  true,
										  event_details.data);

				termPQExpBuffer(&event_details);

				local_conn = try_reconnect(&local_node_info);

				if (local_node_info.node_status == NODE_STATUS_UP)
				{
					int			local_node_unreachable_elapsed = calculate_elapsed(local_node_unreachable_start);

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

					/* check to see if the node has been restored as a standby */
					if (get_recovery_type(local_conn) == RECTYPE_STANDBY)
					{
						PGconn *new_primary_conn;

						initPQExpBuffer(&event_details);

						appendPQExpBuffer(&event_details,
										  _("reconnected to node after %i seconds, node is now a standby, switching to standby monitoring"),
										  degraded_monitoring_elapsed);
						log_notice("%s", event_details.data);
						termPQExpBuffer(&event_details);

						primary_node_id = UNKNOWN_NODE_ID;

						new_primary_conn = get_primary_connection_quiet(local_conn, &primary_node_id, NULL);

						if (PQstatus(new_primary_conn) != CONNECTION_OK)
						{
							close_connection(&new_primary_conn);
							log_warning(_("unable to connect to new primary node %i"), primary_node_id);
						}
						else
						{
							RecordStatus record_status;

							log_debug("primary node id is now %i", primary_node_id);

							record_status = get_node_record(new_primary_conn, config_file_options.node_id, &local_node_info);

							if (record_status == RECORD_FOUND)
							{
								bool resume_monitoring = true;

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
										record_status = get_node_record(new_primary_conn, config_file_options.node_id, &local_node_info);

										if (record_status != RECORD_FOUND)
										{
											resume_monitoring = false;
										}
									}
								}

								if (resume_monitoring == true)
								{
									monitoring_state = MS_NORMAL;
									log_notice(_("former primary has been restored as standby after %i seconds, updating node record and resuming monitoring"),
											   degraded_monitoring_elapsed);

									initPQExpBuffer(&event_details);

									appendPQExpBuffer(&event_details,
													  _("node restored as standby after %i seconds, monitoring connection to upstream node %i"),
													  degraded_monitoring_elapsed,
													  local_node_info.upstream_node_id);

									create_event_notification(new_primary_conn,
															  &config_file_options,
															  config_file_options.node_id,
															  "repmgrd_standby_reconnect",
															  true,
															  event_details.data);


									termPQExpBuffer(&event_details);

									close_connection(&new_primary_conn);

									/* restart monitoring as standby */
									return;
								}
							}
							else if (record_status == RECORD_NOT_FOUND)
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
						}
					}
					else
					{
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
						goto loop;
					}
				}
			}


			/*
			 * possibly attempt to find another node from cached list check if
			 * there's a new primary - if so add hook for fencing? loop, if
			 * starts up check status, switch monitoring mode
			 */
		}
loop:
		/* emit "still alive" log message at regular intervals, if requested */
		if (config_file_options.log_status_interval > 0)
		{
			int			log_status_interval_elapsed = calculate_elapsed(log_status_interval_start);

			if (log_status_interval_elapsed >= config_file_options.log_status_interval)
			{
				log_info(_("monitoring primary node \"%s\" (node ID: %i) in %s state"),
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
			handle_sighup(local_conn);
		}

		log_verbose(LOG_DEBUG, "sleeping %i seconds (parameter \"monitor_interval_secs\")",
					config_file_options.monitor_interval_secs);

		sleep(config_file_options.monitor_interval_secs);
	}
}


void
monitor_streaming_standby(void)
{
	RecordStatus record_status;
	instr_time	log_status_interval_start;
	PQExpBufferData event_details;

	reset_node_voting_status();

	log_debug("monitor_streaming_standby()");

	/*
	 * If no upstream node id is specified in the metadata, we'll try and
	 * determine the current cluster primary in the assumption we should
	 * connect to that by default.
	 */
	if (local_node_info.upstream_node_id == UNKNOWN_NODE_ID)
	{
		local_node_info.upstream_node_id = get_primary_node_id(local_conn);

		/*
		 * Terminate if there doesn't appear to be an active cluster primary.
		 * There could be one or more nodes marked as inactive primaries, and
		 * one of them could actually be a primary, but we can't sensibly
		 * monitor in that state.
		 */
		if (local_node_info.upstream_node_id == NODE_NOT_FOUND)
		{
			log_error(_("unable to determine an active primary for this cluster, terminating"));
			close_connection(&local_conn);
			exit(ERR_BAD_CONFIG);
		}
	}

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
		close_connection(&local_conn);
		exit(ERR_DB_CONN);
	}
	else if (record_status == RECORD_ERROR)
	{
		log_error(_("unable to retrieve record for upstream node (ID: %i), terminating"),
				  local_node_info.upstream_node_id);
		close_connection(&local_conn);
		exit(ERR_DB_CONN);
	}

	log_debug("connecting to upstream node %i: \"%s\"", upstream_node_info.node_id, upstream_node_info.conninfo);

	upstream_conn = establish_db_connection(upstream_node_info.conninfo, false);

	/*
	 * Upstream node must be running at repmgrd startup.
	 *
	 * We could possibly have repmgrd skip to degraded monitoring mode until
	 * it comes up, but there doesn't seem to be much point in doing that.
	 */
	if (PQstatus(upstream_conn) != CONNECTION_OK)
	{
		log_error(_("unable connect to upstream node (ID: %i), terminating"),
				  local_node_info.upstream_node_id);
		log_hint(_("upstream node must be running before repmgrd can start"));

		close_connection(&local_conn);
		exit(ERR_DB_CONN);
	}

	/*
	 * refresh upstream node record from upstream node, so it's as up-to-date
	 * as possible
	 */
	record_status = get_node_record(upstream_conn, upstream_node_info.node_id, &upstream_node_info);

	if (upstream_node_info.type == STANDBY)
	{
		/*
		 * Currently cascaded standbys need to be able to connect to the
		 * primary. We could possibly add a limited connection mode for cases
		 * where this isn't possible.
		 */
		primary_conn = establish_primary_db_connection(upstream_conn, false);

		if (PQstatus(primary_conn) != CONNECTION_OK)
		{
			log_error(_("unable to connect to primary node"));
			log_hint(_("ensure the primary node is reachable from this node"));
			exit(ERR_DB_CONN);
		}

		log_verbose(LOG_DEBUG, "connected to primary");
	}
	else
	{
		primary_conn = upstream_conn;
	}

	primary_node_id = get_primary_node_id(primary_conn);

	/* Log startup event */
	if (startup_event_logged == false)
	{
		PQExpBufferData event_details;

		initPQExpBuffer(&event_details);

		appendPQExpBuffer(&event_details,
						  _("monitoring connection to upstream node \"%s\" (node ID: %i)"),
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
		log_verbose(LOG_DEBUG, "checking %s", upstream_node_info.conninfo);
		if (is_server_available(upstream_node_info.conninfo) == false)
		{
			/* upstream node is down, we were expecting it to be up */
			if (upstream_node_info.node_status == NODE_STATUS_UP)
			{
				instr_time	upstream_node_unreachable_start;

				INSTR_TIME_SET_CURRENT(upstream_node_unreachable_start);

				initPQExpBuffer(&event_details);

				upstream_node_info.node_status = NODE_STATUS_UNKNOWN;

				appendPQExpBuffer(&event_details,
								  _("unable to connect to upstream node \"%s\" (node ID: %i)"),
								  upstream_node_info.node_name, upstream_node_info.node_id);

				/* */
				if (upstream_node_info.type == STANDBY)
				{
					/* XXX possible pre-action event */
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

				close_connection(&upstream_conn);

				/*
				 * if local node is unreachable, make a last-minute attempt to reconnect
				 * before continuing with the failover process
				 */

				if (PQstatus(local_conn) != CONNECTION_OK)
				{
					check_connection(&local_node_info, &local_conn);
				}

				upstream_conn = try_reconnect(&upstream_node_info);

				/* Node has recovered - log and continue */
				if (upstream_node_info.node_status == NODE_STATUS_UP)
				{
					int			upstream_node_unreachable_elapsed = calculate_elapsed(upstream_node_unreachable_start);

					initPQExpBuffer(&event_details);

					appendPQExpBuffer(&event_details,
									  _("reconnected to upstream node after %i seconds"),
									  upstream_node_unreachable_elapsed);
					log_notice("%s", event_details.data);

					create_event_notification(upstream_conn,
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

					if (upstream_node_info.type == PRIMARY)
					{
						failover_done = do_primary_failover();
					}
					else if (upstream_node_info.type == STANDBY)
					{
						failover_done = do_upstream_standby_failover();
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

		if (monitoring_state == MS_DEGRADED)
		{
			int			degraded_monitoring_elapsed = calculate_elapsed(degraded_monitoring_start);

			if (config_file_options.degraded_monitoring_timeout > 0
				&& degraded_monitoring_elapsed > config_file_options.degraded_monitoring_timeout)
			{
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


			log_debug("monitoring node %i in degraded state for %i seconds",
					  upstream_node_info.node_id,
					  degraded_monitoring_elapsed);

			if (is_server_available(upstream_node_info.conninfo) == true)
			{
				upstream_conn = establish_db_connection(upstream_node_info.conninfo, false);

				if (PQstatus(upstream_conn) == CONNECTION_OK)
				{
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
						if (primary_conn == NULL || PQstatus(primary_conn) != CONNECTION_OK)
						{
							primary_conn = establish_primary_db_connection(upstream_conn, false);
						}
					}

					initPQExpBuffer(&event_details);

					appendPQExpBuffer(&event_details,
									  _("reconnected to upstream node %i after %i seconds, resuming monitoring"),
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

				NodeInfoListCell *cell;
				int			follow_node_id = UNKNOWN_NODE_ID;

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

						update_node_record_set_primary(local_conn,  local_node_info.node_id);
						record_status = get_node_record(local_conn, local_node_info.node_id, &local_node_info);

						degraded_monitoring_elapsed = calculate_elapsed(degraded_monitoring_start);

						log_notice(_("resuming monitoring as primary node after %i seconds"),
								   degraded_monitoring_elapsed);

						initPQExpBuffer(&event_details);
						appendPQExpBuffer(&event_details,
										  "promotion command failed but promotion completed successfully");
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
					record_status = get_node_record(local_conn, local_node_info.node_id, &local_node_info);

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


				if (config_file_options.failover == FAILOVER_AUTOMATIC)
				{
					get_active_sibling_node_records(local_conn,
													local_node_info.node_id,
													local_node_info.upstream_node_id,
													&sibling_nodes);

					if (sibling_nodes.node_count > 0)
					{
						log_debug("scanning %i node records to detect new primary...", sibling_nodes.node_count);
						for (cell = sibling_nodes.head; cell; cell = cell->next)
						{
							/* skip local node check, we did that above */
							if (cell->node_info->node_id == local_node_info.node_id)
							{
								continue;
							}

							cell->node_info->conn = establish_db_connection(cell->node_info->conninfo, false);

							if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
							{
								log_debug("unable to connect to %i ... ", cell->node_info->node_id);
								continue;
							}

							if (get_recovery_type(cell->node_info->conn) == RECTYPE_PRIMARY)
							{
								follow_node_id = cell->node_info->node_id;
								close_connection(&cell->node_info->conn);
								break;
							}
							close_connection(&cell->node_info->conn);
						}

						if (follow_node_id != UNKNOWN_NODE_ID)
						{
							follow_new_primary(follow_node_id);
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
								  _("node \"%s\" (node ID: %i) monitoring upstream node \"%s\" (node ID: %i) in %s state"),
								  local_node_info.node_name,
								  local_node_info.node_id,
								  upstream_node_info.node_name,
								  upstream_node_info.node_id,
								  print_monitoring_state(monitoring_state));

				if (config_file_options.failover == FAILOVER_MANUAL)
				{
					appendPQExpBuffer(
									  &monitoring_summary,
									  _(" (automatic failover disabled)"));
				}

				log_info("%s", monitoring_summary.data);
				termPQExpBuffer(&monitoring_summary);
				if (monitoring_state == MS_DEGRADED && config_file_options.failover == FAILOVER_AUTOMATIC)
				{
					log_detail(_("waiting for upstream or another primary to reappear"));
				}

				INSTR_TIME_SET_CURRENT(log_status_interval_start);
			}
		}

		if (PQstatus(primary_conn) == CONNECTION_OK && config_file_options.monitoring_history == true)
		{
			update_monitoring_history();
		}
		else
		{
			connection_ping(local_conn);
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
		}
		else
		{
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

						log_warning("%s", event_details.data)


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
			handle_sighup(local_conn);
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

	PQExpBufferData event_details;
	RecordStatus record_status;

	reset_node_voting_status();

	log_debug("monitor_streaming_witness()");

	if (get_primary_node_record(local_conn, &upstream_node_info) == false)
	{
		PQExpBufferData event_details;

		initPQExpBuffer(&event_details);

		appendPQExpBuffer(&event_details,
						  _("unable to retrieve record for primary node"));

		log_error("%s", event_details.data);
		log_hint(_("execute \"repmgr witness register --force\" to update the witness node "));
		close_connection(&local_conn);

		create_event_notification(NULL,
								  &config_file_options,
								  config_file_options.node_id,
								  "repmgrd_shutdown",
								  false,
								  event_details.data);

		termPQExpBuffer(&event_details);

		terminate(ERR_BAD_CONFIG);
	}

	primary_conn = establish_db_connection(upstream_node_info.conninfo, false);

	/*
	 * Primary node must be running at repmgrd startup.
	 *
	 * We could possibly have repmgrd skip to degraded monitoring mode until
	 * it comes up, but there doesn't seem to be much point in doing that.
	 */
	if (PQstatus(primary_conn) != CONNECTION_OK)
	{
		log_error(_("unable connect to upstream node (ID: %i), terminating"),
				  upstream_node_info.node_id);
		log_hint(_("primary node must be running before repmgrd can start"));

		close_connection(&local_conn);
		exit(ERR_DB_CONN);
	}

	/* synchronise local copy of "repmgr.nodes", in case it was stale */
	witness_copy_node_records(primary_conn, local_conn);

	/*
	 * refresh upstream node record from primary, so it's as up-to-date
	 * as possible
	 */
	record_status = get_node_record(primary_conn, upstream_node_info.node_id, &upstream_node_info);

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


	/* Log startup event */
	if (startup_event_logged == false)
	{
		PQExpBufferData event_details;

		initPQExpBuffer(&event_details);

		appendPQExpBuffer(&event_details,
						  _("witness monitoring connection to primary node \"%s\" (node ID: %i)"),
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
	INSTR_TIME_SET_CURRENT(witness_sync_interval_start);

	upstream_node_info.node_status = NODE_STATUS_UP;

	while (true)
	{
		if (is_server_available(upstream_node_info.conninfo) == false)
		{
			if (upstream_node_info.node_status == NODE_STATUS_UP)
			{
				instr_time	upstream_node_unreachable_start;

				INSTR_TIME_SET_CURRENT(upstream_node_unreachable_start);

				initPQExpBuffer(&event_details);

				upstream_node_info.node_status = NODE_STATUS_UNKNOWN;

				appendPQExpBuffer(&event_details,
								  _("unable to connect to primary node \"%s\" (node ID: %i)"),
								  upstream_node_info.node_name, upstream_node_info.node_id);

				create_event_record(NULL,
									&config_file_options,
									config_file_options.node_id,
									"repmgrd_upstream_disconnect",
									true,
									event_details.data);

				close_connection(&primary_conn);
				primary_conn = try_reconnect(&upstream_node_info);

				/* Node has recovered - log and continue */
				if (upstream_node_info.node_status == NODE_STATUS_UP)
				{
					int			upstream_node_unreachable_elapsed = calculate_elapsed(upstream_node_unreachable_start);

					initPQExpBuffer(&event_details);

					appendPQExpBuffer(&event_details,
									  _("reconnected to upstream node after %i seconds"),
									  upstream_node_unreachable_elapsed);
					log_notice("%s", event_details.data);

					create_event_notification(upstream_conn,
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

			if (is_server_available(upstream_node_info.conninfo) == true)
			{
				primary_conn = establish_db_connection(upstream_node_info.conninfo, false);

				if (PQstatus(primary_conn) == CONNECTION_OK)
				{
					upstream_node_info.node_status = NODE_STATUS_UP;
					monitoring_state = MS_NORMAL;

					initPQExpBuffer(&event_details);

					appendPQExpBuffer(&event_details,
									  _("reconnected to upstream node %i after %i seconds, resuming monitoring"),
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

				NodeInfoListCell *cell;
				int			follow_node_id = UNKNOWN_NODE_ID;

				get_active_sibling_node_records(local_conn,
												local_node_info.node_id,
												local_node_info.upstream_node_id,
												&sibling_nodes);

				if (sibling_nodes.node_count > 0)
				{
					log_debug("scanning %i node records to detect new primary...", sibling_nodes.node_count);
					for (cell = sibling_nodes.head; cell; cell = cell->next)
					{
						/* skip local node check, we did that above */
						if (cell->node_info->node_id == local_node_info.node_id)
						{
							continue;
						}

						cell->node_info->conn = establish_db_connection(cell->node_info->conninfo, false);

						if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
						{
							log_debug("unable to connect to %i ... ", cell->node_info->node_id);
							continue;
						}

						if (get_recovery_type(cell->node_info->conn) == RECTYPE_PRIMARY)
						{
							follow_node_id = cell->node_info->node_id;
							close_connection(&cell->node_info->conn);
							break;
						}
						close_connection(&cell->node_info->conn);
					}

					if (follow_node_id != UNKNOWN_NODE_ID)
					{
						witness_follow_new_primary(follow_node_id);
					}
				}
				clear_node_info_list(&sibling_nodes);
			}
		}
loop:

		/* refresh repmgr.nodes after "witness_sync_interval" seconds */

		{
			int witness_sync_interval_elapsed = calculate_elapsed(witness_sync_interval_start);
			if (witness_sync_interval_elapsed >= config_file_options.witness_sync_interval)
			{
				log_debug("synchronising witness node records");
				witness_copy_node_records(primary_conn, local_conn);
				INSTR_TIME_SET_CURRENT(witness_sync_interval_start);
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
								  _("witness node \"%s\" (node ID: %i) monitoring primary node \"%s\" (node ID: %i) in %s state"),
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
			handle_sighup(local_conn);
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

	/*
	 * Double-check status of the local connection
	 */
	check_connection(&local_node_info, &local_conn);

	/* attempt to initiate voting process */
	election_result = do_election();

	/* TODO add pre-event notification here */
	failover_state = FAILOVER_STATE_UNKNOWN;

	log_debug("election result: %s", _print_election_result(election_result));

	if (election_result == ELECTION_CANCELLED)
	{
		log_notice(_("election cancelled"));
		return false;
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
		log_info(_("follower node awaiting notification from the candidate node"));
		failover_state = FAILOVER_STATE_WAITING_NEW_PRIMARY;
	}

	/*
	 * node has decided it is a follower, so will await notification from the
	 * candidate that it has promoted itself and can be followed
	 */
	if (failover_state == FAILOVER_STATE_WAITING_NEW_PRIMARY)
	{
		int			new_primary_id = UNKNOWN_NODE_ID;

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
									  _("node %i is in manual failover mode and is now disconnected from streaming replication"),
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

			/* we no longer care about our former siblings */
			clear_node_info_list(&sibling_nodes);

			/* pass control back down to start_monitoring() */
			log_info(_("switching to primary monitoring mode"));

			failover_state = FAILOVER_STATE_NONE;
			return true;

		case FAILOVER_STATE_PRIMARY_REAPPEARED:

			/*
			 * notify siblings that they should resume following the original
			 * primary
			 */
			notify_followers(&sibling_nodes, upstream_node_info.node_id);

			/* we no longer care about our former siblings */
			clear_node_info_list(&sibling_nodes);

			/* pass control back down to start_monitoring() */
			log_info(_("resuming standby monitoring mode"));
			log_detail(_("original primary \"%s\" (node ID: %i) reappeared"),
					   upstream_node_info.node_name, upstream_node_info.node_id);

			failover_state = FAILOVER_STATE_NONE;
			return true;


		case FAILOVER_STATE_FOLLOWED_NEW_PRIMARY:
			log_info(_("resuming standby monitoring mode"));
			log_detail(_("following new primary \"%s\" (node id: %i)"),
					   upstream_node_info.node_name, upstream_node_info.node_id);
			failover_state = FAILOVER_STATE_NONE;

			return true;

		case FAILOVER_STATE_FOLLOWING_ORIGINAL_PRIMARY:
			log_info(_("resuming standby monitoring mode"));
			log_detail(_("following original primary \"%s\" (node id: %i)"),
					   upstream_node_info.node_name, upstream_node_info.node_id);
			failover_state = FAILOVER_STATE_NONE;

			return true;

		case FAILOVER_STATE_PROMOTION_FAILED:
			monitoring_state = MS_DEGRADED;
			INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

			return false;

		case FAILOVER_STATE_FOLLOW_FAIL:

			/*
			 * for whatever reason we were unable to follow the new primary -
			 * continue monitoring in degraded state
			 */
			monitoring_state = MS_DEGRADED;
			INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

			return false;

		case FAILOVER_STATE_REQUIRES_MANUAL_FAILOVER:
			log_info(_("automatic failover disabled for this node, manual intervention required"));

			monitoring_state = MS_DEGRADED;
			INSTR_TIME_SET_CURRENT(degraded_monitoring_start);
			return false;

		case FAILOVER_STATE_NO_NEW_PRIMARY:
		case FAILOVER_STATE_WAITING_NEW_PRIMARY:
			/* pass control back down to start_monitoring() */
			return false;

		case FAILOVER_STATE_NODE_NOTIFICATION_ERROR:
		case FAILOVER_STATE_LOCAL_NODE_FAILURE:
		case FAILOVER_STATE_UNKNOWN:
		case FAILOVER_STATE_NONE:
			return false;
	}

	/* should never reach here */
	return false;
}




static void
update_monitoring_history(void)
{
	ReplInfo	replication_info = T_REPLINFO_INTIALIZER;
	XLogRecPtr	primary_last_wal_location = InvalidXLogRecPtr;

	long long unsigned int apply_lag_bytes = 0;
	long long unsigned int replication_lag_bytes = 0;

	/* both local and primary connections must be available */
	if (PQstatus(primary_conn) != CONNECTION_OK || PQstatus(local_conn) != CONNECTION_OK)
		return;

	if (get_replication_info(local_conn, &replication_info) == false)
	{
		log_warning(_("unable to retrieve replication status information"));
		return;
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

	primary_last_wal_location = get_current_wal_lsn(primary_conn);

	if (primary_last_wal_location == InvalidXLogRecPtr)
	{
		log_warning(_("unable to retrieve primary's current LSN"));
		return;
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
	}
	else
	{
		/*
		 * This should never happen, but in case it does set replication lag
		 * to zero
		 */
		log_warning("primary xlog (%X/%X) location appears less than standby receive location (%X/%X)",
					format_lsn(primary_last_wal_location),
					format_lsn(replication_info.last_wal_receive_lsn));
		replication_lag_bytes = 0;
	}

	add_monitoring_record(
						  primary_conn,
						  local_conn,
						  primary_node_id,
						  local_node_info.node_id,
						  replication_info.current_timestamp,
						  primary_last_wal_location,
						  replication_info.last_wal_receive_lsn,
						  replication_info.last_xact_replay_timestamp,
						  replication_lag_bytes,
						  apply_lag_bytes);
}


/*
 * do_upstream_standby_failover()
 *
 * Attach cascaded standby to primary
 *
 * Currently we will try to attach to the cluster primary, as "repmgr
 * standby follow" doesn't support attaching to another node.
 *
 * If this becomes supported, it might be worth providing a selection
 * of reconnection strategies as different behaviour might be desirable
 * in different situations;
 * or maybe the option not to reconnect might be required?
 */

static bool
do_upstream_standby_failover(void)
{
	PQExpBufferData event_details;
	t_node_info primary_node_info = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status = RECORD_NOT_FOUND;
	RecoveryType primary_type = RECTYPE_UNKNOWN;
	int			i, r;
	char		parsed_follow_command[MAXPGPATH] = "";

	close_connection(&upstream_conn);

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
		log_error(_("last known primary\"%s\" (ID: %i) is in recovery, not following"),
				  primary_node_info.node_name,
				  primary_node_info.node_id);

		close_connection(&primary_conn);
		monitoring_state = MS_DEGRADED;
		INSTR_TIME_SET_CURRENT(degraded_monitoring_start);
		return false;
	}

	/* Close the connection to this server */
	close_connection(&local_conn);

	initPQExpBuffer(&event_details);

	log_debug(_("standby follow command is:\n  \"%s\""),
			  config_file_options.follow_command);

	/*
	 * replace %n in "config_file_options.follow_command" with ID of primary
	 * to follow.
	 */
	parse_follow_command(parsed_follow_command, config_file_options.follow_command, primary_node_info.node_id);

	r = system(parsed_follow_command);

	if (r != 0)
	{
		appendPQExpBuffer(&event_details,
						  _("unable to execute follow command:\n %s"),
						  config_file_options.follow_command);

		log_error("%s", event_details.data);

		/*
		 * It may not possible to write to the event notification table but we
		 * should be able to generate an external notification if required.
		 */
		create_event_notification(
								  primary_conn,
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
	 */

	for (i = 0; i < config_file_options.repmgrd_standby_startup_timeout; i++)
	{
		local_conn = establish_db_connection(local_node_info.conninfo, false);

		if (PQstatus(local_conn) == CONNECTION_OK)
			break;

		log_debug("sleeping 1 second; %i of %i attempts to reconnect to local node",
				  i + 1,
				  config_file_options.repmgrd_standby_startup_timeout);
		sleep(1);
	}

	if (PQstatus(local_conn) != CONNECTION_OK)
	{
		log_error(_("unable to reconnect to local node %i"),
				  local_node_info.node_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	/* refresh shared memory settings which will have been zapped by the restart */
	repmgrd_set_local_node_id(local_conn, config_file_options.node_id);

	if (update_node_record_set_upstream(primary_conn,
										local_node_info.node_id,
										primary_node_info.node_id) == false)
	{
		appendPQExpBuffer(&event_details,
						  _("unable to set node %i's new upstream ID to %i"),
						  local_node_info.node_id,
						  primary_node_info.node_id);

		log_error("%s", event_details.data);

		create_event_notification(
								  NULL,
								  &config_file_options,
								  local_node_info.node_id,
								  "repmgrd_failover_follow",
								  false,
								  event_details.data);

		termPQExpBuffer(&event_details);

		terminate(ERR_BAD_CONFIG);
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

	appendPQExpBuffer(&event_details,
					  _("node %i is now following primary node %i"),
					  local_node_info.node_id,
					  primary_node_info.node_id);

	log_notice("%s", event_details.data);

	create_event_notification(
							  primary_conn,
							  &config_file_options,
							  local_node_info.node_id,
							  "repmgrd_failover_follow",
							  true,
							  event_details.data);

	termPQExpBuffer(&event_details);

	/* keep the primary connection open */

	return true;
}


static FailoverState
promote_self(void)
{
	PQExpBufferData event_details;
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

	record_status = get_node_record(local_conn, local_node_info.upstream_node_id, &failed_primary);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve metadata record for failed upstream (ID: %i)"),
				  local_node_info.upstream_node_id);
		return FAILOVER_STATE_PROMOTION_FAILED;
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

	/* connection should stay up, but check just in case */
	if (PQstatus(local_conn) != CONNECTION_OK)
	{
		local_conn = establish_db_connection(local_node_info.conninfo, true);

		/* assume node failed */
		if (PQstatus(local_conn) != CONNECTION_OK)
		{
			log_error(_("unable to reconnect to local node"));
			/* XXX handle this */
			return FAILOVER_STATE_LOCAL_NODE_FAILURE;
		}
	}

	if (r != 0)
	{
		int			primary_node_id;

		upstream_conn = get_primary_connection(local_conn,
											   &primary_node_id, NULL);

		if (PQstatus(upstream_conn) == CONNECTION_OK && primary_node_id == failed_primary.node_id)
		{
			log_notice(_("original primary (id: %i) reappeared before this standby was promoted - no action taken"),
					   failed_primary.node_id);

			initPQExpBuffer(&event_details);
			appendPQExpBuffer(&event_details,
							  _("original primary \"%s\" (node ID: %i) reappeared"),
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


		log_error(_("promote command failed"));
		initPQExpBuffer(&event_details);

		create_event_notification(
								  NULL,
								  &config_file_options,
								  local_node_info.node_id,
								  "repmgrd_promote_error",
								  true,
								  event_details.data);
		termPQExpBuffer(&event_details);

		return FAILOVER_STATE_PROMOTION_FAILED;
	}

	/* bump the electoral term */
	increment_current_term(local_conn);

	initPQExpBuffer(&event_details);

	/* update own internal node record */
	record_status = get_node_record(local_conn, local_node_info.node_id, &local_node_info);

	/*
	 * XXX here we're assuming the promote command updated metadata
	 */
	appendPQExpBuffer(&event_details,
					  _("node %i promoted to primary; old primary %i marked as failed"),
					  local_node_info.node_id,
					  failed_primary.node_id);

	/* local_conn is now the primary connection */
	create_event_notification(local_conn,
							  &config_file_options,
							  local_node_info.node_id,
							  "repmgrd_failover_promote",
							  true,
							  event_details.data);

	termPQExpBuffer(&event_details);

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

	log_verbose(LOG_NOTICE, "%i followers to notify",
				standby_nodes->node_count);

	for (cell = standby_nodes->head; cell; cell = cell->next)
	{
		log_verbose(LOG_DEBUG, "intending to notify node %i... ", cell->node_info->node_id);
		if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
		{
			log_debug("reconnecting to node %i... ", cell->node_info->node_id);

			cell->node_info->conn = establish_db_connection(cell->node_info->conninfo, false);
		}

		if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
		{
			log_debug("unable to reconnect to %i ... ", cell->node_info->node_id);

			continue;
		}

		log_verbose(LOG_NOTICE, "notifying node %i to follow node %i",
					cell->node_info->node_id, follow_node_id);
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

	PQExpBufferData event_details;
	int			i, r;

	/* Store details of the failed node here */
	t_node_info failed_primary = T_NODE_INFO_INITIALIZER;
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
			log_warning(_("new primary is not in recovery"));
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
				initPQExpBuffer(&event_details);
				appendPQExpBuffer(&event_details,
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

		log_debug("sleeping 1 second; %i of %i attempts to reconnect to local node",
				  i + 1,
				  config_file_options.repmgrd_standby_startup_timeout);
		sleep(1);
	}

	if (PQstatus(local_conn) != CONNECTION_OK)
	{
		log_error(_("unable to reconnect to local node %i"),
				  local_node_info.node_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	/* refresh shared memory settings which will have been zapped by the restart */
	repmgrd_set_local_node_id(local_conn, config_file_options.node_id);

	initPQExpBuffer(&event_details);
	appendPQExpBuffer(&event_details,
					  _("node %i now following new upstream node %i"),
					  local_node_info.node_id,
					  upstream_node_info.node_id);

	log_notice("%s", event_details.data);

	create_event_notification(upstream_conn,
							  &config_file_options,
							  local_node_info.node_id,
							  "repmgrd_failover_follow",
							  true,
							  event_details.data);

	termPQExpBuffer(&event_details);

	return FAILOVER_STATE_FOLLOWED_NEW_PRIMARY;
}


static FailoverState
witness_follow_new_primary(int new_primary_id)
{
	PQExpBufferData event_details;

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

		if (primary_recovery_type == RECTYPE_PRIMARY)
		{
			new_primary_ok = true;
		}
		else
		{
			new_primary_ok = false;
			log_warning(_("new primary is not in recovery"));
			close_connection(&upstream_conn);
		}
	}

	if (new_primary_ok == false)
	{
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
		log_error(_("unable to retrieve metadata record found for node %i"),
				  local_node_info.node_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	initPQExpBuffer(&event_details);
	appendPQExpBuffer(&event_details,
					  _("witness node %i now following new primary node %i"),
					  local_node_info.node_id,
					  upstream_node_info.node_id);

	log_notice("%s", event_details.data);

	create_event_notification(
							  upstream_conn,
							  &config_file_options,
							  local_node_info.node_id,
							  "repmgrd_failover_follow",
							  true,
							  event_details.data);

	termPQExpBuffer(&event_details);

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
	}

	/* should never reach here */
	return "UNKNOWN";
}


/*
 * NB: this function sets standby_nodes; caller (do_primary_failover)
 * expects to be able to read this list
 */
static ElectionResult
do_election(void)
{
	int			electoral_term = -1;

	/* we're visible */
	int			visible_nodes = 1;
	int			total_nodes = 0;

	NodeInfoListCell *cell = NULL;

	t_node_info *candidate_node = NULL;

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
									&sibling_nodes);

	total_nodes = sibling_nodes.node_count + 1;

	log_debug("do_election(): primary location is %s", upstream_node_info.location);

	local_node_info.last_wal_receive_lsn = InvalidXLogRecPtr;

	/* fast path if no other standbys (or witness) exists - normally win by default */
	if (sibling_nodes.node_count == 0)
	{
		if (strncmp(upstream_node_info.location, local_node_info.location, MAXLEN) == 0)
		{
			log_debug("no other nodes - we win by default");
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
	local_node_info.last_wal_receive_lsn = get_last_wal_receive_location(local_conn);

	log_debug("our last receive lsn: %X/%X", format_lsn(local_node_info.last_wal_receive_lsn));

	/* pointer to "winning" node, initially self */
	candidate_node = &local_node_info;

	for (cell = sibling_nodes.head; cell; cell = cell->next)
	{
		/* assume the worst case */
		cell->node_info->node_status = NODE_STATUS_UNKNOWN;

		cell->node_info->conn = establish_db_connection(cell->node_info->conninfo, false);

		if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
		{
			continue;
		}

		cell->node_info->node_status = NODE_STATUS_UP;

		visible_nodes++;

		/*
		 * see if the node is in the primary's location (but skip the check if
		 * we've seen a node there already)
		 */
		if (primary_location_seen == false)
		{
			if (strncmp(cell->node_info->location, upstream_node_info.location, MAXLEN) == 0)
			{
				primary_location_seen = true;
			}
		}

		/* don't interrogate a witness server */
		if (cell->node_info->type == WITNESS)
		{
			log_debug("node %i is witness, not querying state", cell->node_info->node_id);
			continue;
		}
		/* XXX don't check 0-priority nodes */

		/* get node's LSN - if "higher" than current winner, current node is candidate */

		cell->node_info->last_wal_receive_lsn = get_last_wal_receive_location(cell->node_info->conn);

		log_verbose(LOG_DEBUG, "node %i's last receive LSN is: %X/%X",
					cell->node_info->node_id,
					format_lsn(cell->node_info->last_wal_receive_lsn));

		/* compare LSN */
		if (cell->node_info->last_wal_receive_lsn > candidate_node->last_wal_receive_lsn)
		{
			/* other node is ahead */
			log_verbose(LOG_DEBUG, "node %i is ahead of current candidate %i",
						cell->node_info->node_id,
						candidate_node->node_id);

			candidate_node = cell->node_info;
		}
		/* LSN is same - tiebreak on priority, then node_id */
		else if (cell->node_info->last_wal_receive_lsn == candidate_node->last_wal_receive_lsn)
		{
			log_verbose(LOG_DEBUG, "node %i has same LSN as current candidate %i",
						cell->node_info->node_id,
						candidate_node->node_id);
			if (cell->node_info->priority > candidate_node->priority)
			{
				log_verbose(LOG_DEBUG, "node %i has higher priority (%i) than current candidate %i (%i)",
							cell->node_info->node_id,
							cell->node_info->priority,
							candidate_node->node_id,
							candidate_node->priority);
				candidate_node = cell->node_info;
			}
			else if (cell->node_info->priority == candidate_node->priority)
			{
				if (cell->node_info->node_id < candidate_node->node_id)
				{
					log_verbose(LOG_DEBUG, "node %i has same priority but lower node_id than current candidate %i",
								cell->node_info->node_id,
								candidate_node->node_id);
					candidate_node = cell->node_info;
				}
			}
			else
			{
				log_verbose(LOG_DEBUG, "node %i has lower priority (%i) than current candidate %i (%i)",
							cell->node_info->node_id,
							cell->node_info->priority,
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

		return ELECTION_CANCELLED;
	}

	log_debug("visible nodes: %i; total nodes: %i",
			  visible_nodes,
			  total_nodes);

	if (visible_nodes <= (total_nodes / 2.0))
	{
		log_notice(_("unable to reach a qualified majority of nodes"));
		log_detail(_("node will enter degraded monitoring state waiting for reconnect"));

		monitoring_state = MS_DEGRADED;
		INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

		reset_node_voting_status();

		return ELECTION_CANCELLED;
	}

	log_debug("promotion candidate is %i", candidate_node->node_id);
	if (candidate_node->node_id == local_node_info.node_id)
		return ELECTION_WON;

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
			log_info(_("resuming witness monitoring mode"));
			log_detail(_("original primary \"%s\" (node ID: %i) reappeared"),
					   upstream_node_info.node_name, upstream_node_info.node_id);

			failover_state = FAILOVER_STATE_NONE;
			return true;


		case FAILOVER_STATE_FOLLOWED_NEW_PRIMARY:
			log_info(_("resuming standby monitoring mode"));
			log_detail(_("following new primary \"%s\" (node id: %i)"),
					   upstream_node_info.node_name, upstream_node_info.node_id);
			failover_state = FAILOVER_STATE_NONE;

			return true;

		case FAILOVER_STATE_FOLLOWING_ORIGINAL_PRIMARY:
			log_info(_("resuming witness monitoring mode"));
			log_detail(_("following original primary \"%s\" (node id: %i)"),
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
		return;
	}
	reset_voting_status(local_conn);
}


static void
check_connection(t_node_info *node_info, PGconn **conn)
{
	if (is_server_available(node_info->conninfo) == false)
	{
		log_warning(_("connection to node %i lost"), node_info->node_id);
	}

	if (PQstatus(*conn) != CONNECTION_OK)
	{
		log_info(_("attempting to reconnect to node \"%s\" (ID: %i)"),
				 node_info->node_name,
				 node_info->node_id);
		*conn = establish_db_connection(node_info->conninfo, false);

		if (PQstatus(*conn) != CONNECTION_OK)
		{
			*conn = NULL;
			log_warning(_("reconnection to node \"%s\" (ID: %i) failed"),
						node_info->node_name,
						node_info->node_id);
		}
		else
		{
			log_info(_("reconnected to node \"%s\" (ID: %i)"),
					 node_info->node_name,
					 node_info->node_id);
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
	}

	/* should never reach here */
	return "UNKNOWN_FAILOVER_STATE";
}


static void
handle_sighup(PGconn *conn)
{
	log_debug("SIGHUP received");

	if (reload_config(&config_file_options))
	{
		close_connection(&conn);
		conn = establish_db_connection(config_file_options.conninfo, true);

		if (*config_file_options.log_file)
		{
			FILE	   *fd;

			fd = freopen(config_file_options.log_file, "a", stderr);
			if (fd == NULL)
			{
				fprintf(stderr, "error reopening stderr to \"%s\": %s",
						config_file_options.log_file, strerror(errno));
			}
		}
	}
	got_SIGHUP = false;
}
