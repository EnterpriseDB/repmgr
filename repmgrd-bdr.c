/*
 * repmgrd-bdr.c - BDR functionality for repmgrd
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
#include "repmgrd-bdr.h"
#include "configfile.h"


static void do_bdr_failover(NodeInfoList *nodes, t_node_info *monitored_node);
static void do_bdr_recovery(NodeInfoList *nodes, t_node_info *monitored_node);


void
do_bdr_node_check(void)
{
	/* nothing to do at the moment */
}

void
handle_sigint_bdr(SIGNAL_ARGS)
{
	PQExpBufferData event_details;

	initPQExpBuffer(&event_details);

	appendPQExpBuffer(&event_details,
					  "%s signal received",
					  postgres_signal_arg == SIGTERM
					  ? "TERM" : "INT");

	create_event_notification(local_conn,
							  &config_file_options,
							  config_file_options.node_id,
							  "repmgrd_shutdown",
							  true,
							  event_details.data);
	termPQExpBuffer(&event_details);

	terminate(SUCCESS);
}


void
monitor_bdr(void)
{
	NodeInfoList nodes = T_NODE_INFO_LIST_INITIALIZER;
	t_bdr_node_info bdr_node_info = T_BDR_NODE_INFO_INITIALIZER;
	RecordStatus record_status;
	NodeInfoListCell *cell;
	PQExpBufferData event_details;
	instr_time	log_status_interval_start;

	/* sanity check local database */
	log_info(_("connecting to local database \"%s\""),
			 config_file_options.conninfo);

	local_conn = establish_db_connection(config_file_options.conninfo, true);

	/*
	 * Local node must be running
	 */
	if (PQstatus(local_conn) != CONNECTION_OK)
	{
		log_error(_("unable connect to local node (ID: %i), terminating"),
				  local_node_info.node_id);
		log_hint(_("local node must be running before repmgrd can start"));
		PQfinish(local_conn);
		exit(ERR_DB_CONN);
	}

	/*
	 * Verify that database is a BDR one TODO: check if supported BDR version?
	 */
	log_info(_("connected to database, checking for BDR"));

	if (!is_bdr_db(local_conn, NULL))
	{
		log_error(_("database is not BDR-enabled"));
		exit(ERR_BAD_CONFIG);
	}

	if (is_table_in_bdr_replication_set(local_conn, "nodes", "repmgr") == false)
	{
		log_error(_("repmgr metadata table 'repmgr.%s' is not in the 'repmgr' replication set"),
				  "nodes");

		/*
		 * TODO: add `repmgr bdr sync` or similar for this situation, and hint
		 * here
		 */

		exit(ERR_BAD_CONFIG);
	}

	record_status = get_bdr_node_record_by_name(local_conn, local_node_info.node_name, &bdr_node_info);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve BDR record for node %s, terminating"),
				  local_node_info.node_name);
		PQfinish(local_conn);
		exit(ERR_BAD_CONFIG);
	}

	if (local_node_info.active == false)
	{
		log_error(_("local node (ID: %i) is marked as inactive in repmgr"),
				  local_node_info.node_id);
		log_hint(_("if the node has been reactivated, run \"repmgr bdr register --force\" and restart repmgrd"));
		PQfinish(local_conn);
		exit(ERR_BAD_CONFIG);
	}

	if (is_active_bdr_node(local_conn, local_node_info.node_name) == false)
	{
		log_error(_("BDR node \"%s\" is not active, terminating"),
				  local_node_info.node_name);
		PQfinish(local_conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Log startup event */
	create_event_record(local_conn,
						&config_file_options,
						config_file_options.node_id,
						"repmgrd_start",
						true,
						NULL);

	/*
	 * retrieve list of all nodes - we'll need these if the DB connection goes
	 * away
	 */
	if (get_all_node_records(local_conn, &nodes) == false)
	{
		/* get_all_node_records() will display the error */
		PQfinish(local_conn);
		exit(ERR_BAD_CONFIG);
	}


	/* we're expecting all (both) nodes to be up */
	for (cell = nodes.head; cell; cell = cell->next)
	{
		cell->node_info->node_status = NODE_STATUS_UP;
	}

	log_info(_("starting continuous BDR node monitoring on node %i"),
			 config_file_options.node_id);

	INSTR_TIME_SET_CURRENT(log_status_interval_start);

	while (true)
	{

		/* monitoring loop */
		log_verbose(LOG_DEBUG, "BDR check loop - checking %i nodes", nodes.node_count);

		for (cell = nodes.head; cell; cell = cell->next)
		{
			if (config_file_options.bdr_local_monitoring_only == true
				&& cell->node_info->node_id != local_node_info.node_id)
			{
				continue;
			}

			if (cell->node_info->node_id == local_node_info.node_id)
			{
				log_debug("checking local node %i in %s state",
						  local_node_info.node_id,
						  print_monitoring_state(cell->node_info->monitoring_state));
			}
			else
			{
				log_debug("checking other node %i in %s state",
						  cell->node_info->node_id,
						  print_monitoring_state(cell->node_info->monitoring_state));
			}


			switch (cell->node_info->monitoring_state)
			{
				case MS_NORMAL:
					{
						if (is_server_available(cell->node_info->conninfo) == false)
						{
							/* node is down, we were expecting it to be up */
							if (cell->node_info->node_status == NODE_STATUS_UP)
							{
								instr_time	node_unreachable_start;

								INSTR_TIME_SET_CURRENT(node_unreachable_start);

								cell->node_info->node_status = NODE_STATUS_DOWN;

								if (cell->node_info->conn != NULL)
								{
									PQfinish(cell->node_info->conn);
									cell->node_info->conn = NULL;
								}

								log_warning(_("unable to connect to node %s (ID %i)"),
											cell->node_info->node_name, cell->node_info->node_id);
								//cell->node_info->conn = try_reconnect(cell->node_info);
								try_reconnect(&cell->node_info->conn, cell->node_info);

								/* node has recovered - log and continue */
								if (cell->node_info->node_status == NODE_STATUS_UP)
								{
									int			node_unreachable_elapsed = calculate_elapsed(node_unreachable_start);

									initPQExpBuffer(&event_details);

									appendPQExpBuffer(&event_details,
													  _("reconnected to node %i after %i seconds"),
													  cell->node_info->node_id,
													  node_unreachable_elapsed);
									log_notice("%s", event_details.data);

									create_event_notification(cell->node_info->conn,
															  &config_file_options,
															  config_file_options.node_id,
															  "bdr_reconnect",
															  true,
															  event_details.data);
									termPQExpBuffer(&event_details);

									goto loop;
								}

								/* still down after reconnect attempt(s) */
								if (cell->node_info->node_status == NODE_STATUS_DOWN)
								{
									do_bdr_failover(&nodes, cell->node_info);
									goto loop;
								}
							}
						}
					}
					break;
				case MS_DEGRADED:
					{
						/* degraded monitoring */
						if (is_server_available(cell->node_info->conninfo) == true)
						{
							do_bdr_recovery(&nodes, cell->node_info);
						}

					}
					break;
			}
		}

loop:

		/* emit "still alive" log message at regular intervals, if requested */
		if (config_file_options.log_status_interval > 0)
		{
			int			log_status_interval_elapsed = calculate_elapsed(log_status_interval_start);
			if (log_status_interval_elapsed >= config_file_options.log_status_interval)
			{
				log_info(_("monitoring BDR replication status on node \"%s\" (ID: %i)"),
						 local_node_info.node_name,
						 local_node_info.node_id);

				for (cell = nodes.head; cell; cell = cell->next)
				{
					if (cell->node_info->monitoring_state == MS_DEGRADED)
					{
						log_detail(_("monitoring node \"%s\" (ID: %i) in degraded mode"),
								   cell->node_info->node_name,
								   cell->node_info->node_id);
					}
				}
				INSTR_TIME_SET_CURRENT(log_status_interval_start);
			}
		}

		if (got_SIGHUP)
		{
			/*
			 * if we can reload, then could need to change local_conn
			 */
			if (reload_config(&config_file_options, BDR))
			{
				PQfinish(local_conn);
				local_conn = establish_db_connection(config_file_options.conninfo, true);
				update_registration(local_conn);
			}

			got_SIGHUP = false;
		}

		/* XXX this looks like it will never be called */
		if (got_SIGHUP)
		{
			log_debug("SIGHUP received");

			if (reload_config(&config_file_options, BDR))
			{
				PQfinish(local_conn);
				local_conn = establish_db_connection(config_file_options.conninfo, true);

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

		log_verbose(LOG_DEBUG, "sleeping %i seconds (\"monitor_interval_secs\")",
					config_file_options.monitor_interval_secs);
		sleep(config_file_options.monitor_interval_secs);
	}

	return;
}

/*
 * do_bdr_failover()
 *
 * Here we attempt to perform a BDR "failover".
 *
 * As there's no equivalent of a physical replication failover,
 * we'll do the following:
 *
 *  - connect to active node
 *  - generate an event log record on that node
 *  - optionally execute `bdr_failover_command`, passing the conninfo string
 *    of that node to the command; this can be used for e.g. reconfiguring
 *    pgbouncer.
 *
 */

void
do_bdr_failover(NodeInfoList *nodes, t_node_info *monitored_node)
{
	PGconn	   *next_node_conn = NULL;
	NodeInfoListCell *cell;
	PQExpBufferData event_details;
	t_event_info event_info = T_EVENT_INFO_INITIALIZER;
	t_node_info target_node = T_NODE_INFO_INITIALIZER;
	t_node_info failed_node = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status;

	/* if one of the two nodes is down, cluster will be in a degraded state */
	monitored_node->monitoring_state = MS_DEGRADED;
	INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

	/* terminate local connection if this is the failed node */
	if (monitored_node->node_id == local_node_info.node_id)
	{
		PQfinish(local_conn);
		local_conn = NULL;
	}


	/* get other node */

	for (cell = nodes->head; cell; cell = cell->next)
	{
		log_debug("do_bdr_failover() %s", cell->node_info->node_name);

		/*
		 * don't attempt to connect to the current monitored node, as that's
		 * the one which has failed
		 */
		if (cell->node_info->node_id == monitored_node->node_id)
			continue;

		/* TODO: reuse local conn if local node is up */
		next_node_conn = establish_db_connection(cell->node_info->conninfo, false);

		if (PQstatus(next_node_conn) == CONNECTION_OK)
		{
			record_status = get_node_record(next_node_conn,
											cell->node_info->node_id,
											&target_node);

			if (record_status == RECORD_FOUND)
			{
				break;
			}
		}

		next_node_conn = NULL;
	}

	/* shouldn't happen, and if it does, it means everything is down */
	if (next_node_conn == NULL)
	{
		log_error(_("no other available node found"));

		/* no other nodes found - continue degraded monitoring */
		return;
	}

	/*
	 * check if the node record for the failed node is still marked as active,
	 * if not it means the other node has done the "failover" already
	 */

	record_status = get_node_record(next_node_conn,
									monitored_node->node_id,
									&failed_node);

	if (record_status == RECORD_FOUND && failed_node.active == false)
	{
		PQfinish(next_node_conn);
		log_notice(_("record for node %i has already been set inactive"),
				   failed_node.node_id);
		return;
	}

	if (am_bdr_failover_handler(next_node_conn, local_node_info.node_id) == false)
	{
		PQfinish(next_node_conn);
		log_notice(_("other node's repmgrd is handling failover"));
		return;
	}


	/* check here that the node hasn't come back up */
	if (is_server_available(monitored_node->conninfo) == true)
	{
		log_notice(_("node %i has reappeared, aborting failover"),
				   monitored_node->node_id);
		monitored_node->monitoring_state = MS_NORMAL;
		PQfinish(next_node_conn);
	}

	log_debug("this node is the failover handler");

	initPQExpBuffer(&event_details);

	event_info.conninfo_str = target_node.conninfo;
	event_info.node_name = target_node.node_name;

	/* update node record on the active node */
	update_node_record_set_active(next_node_conn, monitored_node->node_id, false);

	log_notice(_("setting node record for node %i to inactive"), monitored_node->node_id);

	appendPQExpBuffer(&event_details,
					  _("node \"%s\" (ID: %i) detected as failed; next available node is \"%s\" (ID: %i)"),
					  monitored_node->node_name,
					  monitored_node->node_id,
					  target_node.node_name,
					  target_node.node_id);

	/*
	 * Create an event record
	 *
	 * If we were able to connect to another node, we'll update the event log
	 * there.
	 *
	 * In any case the event notification command will be triggered with the
	 * event "bdr_failover"
	 */


	create_event_notification_extended(next_node_conn,
									   &config_file_options,
									   monitored_node->node_id,
									   "bdr_failover",
									   true,
									   event_details.data,
									   &event_info);

	log_info("%s", event_details.data);

	termPQExpBuffer(&event_details);

	unset_bdr_failover_handler(next_node_conn);

	PQfinish(next_node_conn);


	return;
}

static void
do_bdr_recovery(NodeInfoList *nodes, t_node_info *monitored_node)
{
	PGconn	   *recovered_node_conn;

	PQExpBufferData event_details;
	t_event_info event_info = T_EVENT_INFO_INITIALIZER;
	int			i;
	bool		slot_reactivated = false;
	int			node_recovery_elapsed;

	char		node_name[MAXLEN] = "";

	log_debug("handling recovery for monitored node %i", monitored_node->node_id);

	recovered_node_conn = establish_db_connection(monitored_node->conninfo, false);

	if (PQstatus(recovered_node_conn) != CONNECTION_OK)
	{
		PQfinish(recovered_node_conn);
		return;
	}

	if (PQstatus(local_conn) != CONNECTION_OK)
	{
		log_debug("no local connection - attempting to reconnect ");
		local_conn = establish_db_connection(config_file_options.conninfo, false);
	}

	/*
	 * still unable to connect - the local node is probably down, so we can't
	 * check for reconnection
	 */
	if (PQstatus(local_conn) != CONNECTION_OK)
	{
		local_conn = NULL;
		log_warning(_("unable to reconnect to local node"));

		initPQExpBuffer(&event_details);

		node_recovery_elapsed = calculate_elapsed(degraded_monitoring_start);
		monitored_node->monitoring_state = MS_NORMAL;
		monitored_node->node_status = NODE_STATUS_UP;

		appendPQExpBuffer(
						  &event_details,
						  _("node \"%s\" (ID: %i) has become available after %i seconds"),
						  monitored_node->node_name,
						  monitored_node->node_id,
						  node_recovery_elapsed);

		log_notice("%s", event_details.data);

		termPQExpBuffer(&event_details);

		PQfinish(recovered_node_conn);

		return;
	}

	get_bdr_other_node_name(local_conn, local_node_info.node_id, node_name);

	log_info(_("detected recovery on node %s (ID: %i), checking status"),
			 monitored_node->node_name,
			 monitored_node->node_id);

	for (i = 0; i < config_file_options.bdr_recovery_timeout; i++)
	{
		ReplSlotStatus slot_status;

		log_debug("checking for state of replication slot for node \"%s\"", node_name);

		slot_status = get_bdr_node_replication_slot_status(
														   local_conn,
														   node_name);

		if (slot_status == SLOT_ACTIVE)
		{
			slot_reactivated = true;
			break;
		}

		sleep(1);
	}

	/* mark node as up */
	monitored_node->node_status = NODE_STATUS_UP;

	if (slot_reactivated == false)
	{
		log_warning(_("no active replication slot for node \"%s\" found after %i seconds"),
					node_name,
					config_file_options.bdr_recovery_timeout);
		log_detail(_("this probably means inter-node BDR connections have not been re-established"));
		PQfinish(recovered_node_conn);
		return;
	}

	log_info(_("active replication slot for node \"%s\" found after %i seconds"),
			 node_name,
			 i);

	node_recovery_elapsed = calculate_elapsed(degraded_monitoring_start);
	monitored_node->monitoring_state = MS_NORMAL;


	initPQExpBuffer(&event_details);

	appendPQExpBuffer(&event_details,
					  _("node \"%s\" (ID: %i) has recovered after %i seconds"),
					  monitored_node->node_name,
					  monitored_node->node_id,
					  node_recovery_elapsed);

	log_notice("%s", event_details.data);


	/* other node will generate the event */
	if (monitored_node->node_id == local_node_info.node_id)
	{
		termPQExpBuffer(&event_details);
		PQfinish(recovered_node_conn);

		return;
	}


	/* generate the event on the currently active node only */
	if (monitored_node->node_id != local_node_info.node_id)
	{
		event_info.conninfo_str = monitored_node->conninfo;
		event_info.node_name = monitored_node->node_name;

		create_event_notification_extended(
										   local_conn,
										   &config_file_options,
										   config_file_options.node_id,
										   "bdr_recovery",
										   true,
										   event_details.data,
										   &event_info);
	}


	update_node_record_set_active(local_conn, monitored_node->node_id, true);

	termPQExpBuffer(&event_details);

	PQfinish(recovered_node_conn);

	return;
}
