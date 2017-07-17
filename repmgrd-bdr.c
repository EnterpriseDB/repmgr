/*
 * repmgrd-bdr.c - BDR functionality for repmgrd
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include <signal.h>

#include "repmgr.h"
#include "repmgrd.h"
#include "repmgrd-bdr.h"
#include "configfile.h"


static volatile sig_atomic_t got_SIGHUP = false;

static void do_bdr_failover(NodeInfoList *nodes, t_node_info *monitored_node);
static void do_bdr_recovery(NodeInfoList *nodes, t_node_info *monitored_node);


void
do_bdr_node_check(void)
{
	/* nothing to do at the moment */
}


void
monitor_bdr(void)
{
	NodeInfoList    nodes = T_NODE_INFO_LIST_INITIALIZER;
	t_bdr_node_info bdr_node_info = T_BDR_NODE_INFO_INITIALIZER;
	RecordStatus  record_status;
	NodeInfoListCell *cell;
	PQExpBufferData event_details;

	/* sanity check local database */
	log_info(_("connecting to local database '%s'"),
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
	 * Verify that database is a BDR one
	 * TODO: check if supported BDR version?
	 */
	log_info(_("connected to database, checking for BDR"));

	if (!is_bdr_db(local_conn))
	{
		log_error(_("database is not BDR-enabled"));
		exit(ERR_BAD_CONFIG);
	}

	if (is_table_in_bdr_replication_set(local_conn, "nodes", "repmgr") == false)
	{
		log_error(_("repmgr metadata table 'repmgr.%s' is not in the 'repmgr' replication set"),
				  "nodes");

		/* TODO: add `repmgr bdr sync` or similar for this situation, and hint here */

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

	/* Retrieve record for this node from the local database */
	record_status = get_node_record(local_conn, config_file_options.node_id, &local_node_info);

	/*
	 * Terminate if we can't find the local node record. This is a "fix-the-config"
	 * situation, not a lot else we can do.
	 */
	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve record for local node (ID: %i), terminating"),
				  local_node_info.node_id);
		log_hint(_("check that 'repmgr bdr register' was executed for this node"));
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

	if (is_active_bdr_node(local_conn, local_node_info.node_name))
	{
		log_error(_("BDR node %s is not active, terminating"),
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
	 * retrieve list of all nodes - we'll need these if the DB connection goes away
	 */
	get_all_node_records(local_conn, &nodes);

	/* we're expecting all (both) nodes to be up */
	for (cell = nodes.head; cell; cell = cell->next)
	{
		cell->node_info->node_status = NODE_STATUS_UP;
	}

	log_debug("main_loop_bdr() monitoring local node %i", config_file_options.node_id);

	log_info(_("starting continuous bdr node monitoring"));

	while (true)
	{

		/* monitoring loop */
		log_verbose(LOG_DEBUG, "bdr check loop...");

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

							cell->node_info->conn = try_reconnect(cell->node_info);

							/* Node has recovered - log and continue */
							if (cell->node_info->node_status == NODE_STATUS_UP)
							{
								int		node_unreachable_elapsed = calculate_elapsed(node_unreachable_start);

								initPQExpBuffer(&event_details);

								appendPQExpBuffer(&event_details,
												  _("reconnected to node %i after %i seconds"),
												  cell->node_info->node_id,
												  node_unreachable_elapsed);
								log_notice("%s", event_details.data);

								create_event_notification(cell->node_info->conn,
														  &config_file_options,
														  config_file_options.node_id,
														  "repmgrd_upstream_reconnect",
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
						log_notice(_("monitored node %i has recovered"),  cell->node_info->node_id);
						do_bdr_recovery(&nodes, cell->node_info);
					}

				}
				break;
			}
		}

	loop:

		if (got_SIGHUP)
		{
			/*
			 * if we can reload, then could need to change
			 * local_conn
			 */
			if (reload_config(&config_file_options))
			{
				PQfinish(local_conn);
				local_conn = establish_db_connection(config_file_options.conninfo, true);
				update_registration(local_conn);
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
 *0
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
	t_node_info target_node  = T_NODE_INFO_INITIALIZER;

	monitored_node->monitoring_state = MS_DEGRADED;
	INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

	/* get other node */

	for (cell = nodes->head; cell; cell = cell->next)
	{
		log_debug("do_bdr_failover() %s", cell->node_info->node_name);

		/* don't attempt to connect to the current monitored node, as that's the one which has failed  */
		if (cell->node_info->node_id == monitored_node->node_id)
			continue;

		/* XXX skip inactive node? */
		// reuse local conn if local node is up
		next_node_conn = establish_db_connection(cell->node_info->conninfo, false);

		if (PQstatus(next_node_conn) == CONNECTION_OK)
		{
			RecordStatus record_status = get_node_record(next_node_conn,
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

	if (am_bdr_failover_handler(next_node_conn, local_node_info.node_id) == false)
	{
		PQfinish(next_node_conn);
		log_debug("other node's repmgrd is handling failover");
		return;
	}

	log_debug("this node is the failover handler");

	// check here that the node hasn't come back up...

	log_info(_("connecting to target node %s"), target_node.node_name);

	initPQExpBuffer(&event_details);

	event_info.conninfo_str = target_node.conninfo;
	event_info.node_name = target_node.node_name;

	/* update our own record on the other node */
	update_node_record_set_active(next_node_conn, monitored_node->node_id, false);

	appendPQExpBuffer(&event_details,
					  _("node '%s' (ID: %i) detected as failed; next available node is '%s' (ID: %i)"),
					  monitored_node->node_name,
					  monitored_node->node_id,
					  target_node.node_name,
					  target_node.node_id);




	/*
	 * Create an event record
	 *
	 * If we were able to connect to another node, we'll update the
	 * event log there.
	 *
	 * In any case the event notification command will be triggered
	 * with the event "bdr_failover"
	 */

	create_event_notification_extended(
		next_node_conn,
		&config_file_options,
		monitored_node->node_id,
		"bdr_failover",
		true,
		event_details.data,
		&event_info);

	termPQExpBuffer(&event_details);

	unset_bdr_failover_handler(next_node_conn);

	return;
}

static void
do_bdr_recovery(NodeInfoList *nodes, t_node_info *monitored_node)
{
	PGconn *recovered_node_conn;
	PQExpBufferData event_details;
	t_bdr_node_info bdr_record;
	t_event_info event_info = T_EVENT_INFO_INITIALIZER;
	int i;
	bool node_recovered = false;

	recovered_node_conn = establish_db_connection(monitored_node->conninfo, false);

	if (PQstatus(recovered_node_conn) != CONNECTION_OK)
	{
		PQfinish(recovered_node_conn);
		return;
	}

	if (am_bdr_failover_handler(recovered_node_conn, local_node_info.node_id) == false)
	{
		PQfinish(recovered_node_conn);
		log_debug("other node's repmgrd is handling recovery");
		return;
	}

	// bdr_recovery_timeout
	for (i = 0; i < 30; i++)
	{
		RecordStatus record_status = get_bdr_node_record_by_name(
			recovered_node_conn,
			monitored_node->node_name,
			&bdr_record);

		if (record_status == RECORD_FOUND && bdr_record.node_status == 'r')
		{
			node_recovered = true;
			break;
		}

		sleep(1);
		continue;
	}

	if (node_recovered == false)
	{
		log_warning(_("node did not come up"));
		PQfinish(recovered_node_conn);
		return;
	}


	// XXX check other node is attached to this one so we
	// don't end up monitoring a parted node


	// note elapsed
	initPQExpBuffer(&event_details);
	appendPQExpBuffer(&event_details,
					  _("node '%s' (ID: %i) has recovered"),
					  monitored_node->node_name,
					  monitored_node->node_id);

	monitored_node->monitoring_state = MS_NORMAL;

	if (config_file_options.bdr_active_node_recovery == true)
	{
		event_info.conninfo_str = monitored_node->conninfo;
		event_info.node_name = monitored_node->node_name;

		create_event_notification_extended(
			recovered_node_conn,
			&config_file_options,
			config_file_options.node_id,
			"bdr_recovery",
			true,
			event_details.data,
			&event_info);
	}
	else
	{
		create_event_record(
			recovered_node_conn,
			&config_file_options,
			config_file_options.node_id,
			"bdr_recovery",
			true,
			event_details.data);
	}

	termPQExpBuffer(&event_details);

	unset_bdr_failover_handler(recovered_node_conn);

	PQfinish(recovered_node_conn);

	return;
}
