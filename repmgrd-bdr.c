/*
 * repmgrd-bdr.c - BDR functionality for repmgrd
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include <signal.h>

#include "repmgr.h"
#include "repmgrd.h"
#include "repmgrd-bdr.h"
#include "config.h"


static volatile sig_atomic_t got_SIGHUP = false;

static void do_bdr_failover(NodeInfoList *nodes);


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
	 * retrieve list of nodes - we'll need these if the DB connection goes away,
	 */
	get_all_node_records(local_conn, &nodes);

	log_debug("main_loop_bdr() monitoring local node %i", config_file_options.node_id);

	log_info(_("starting continuous bdr node monitoring"));

	while (true)
	{

		/* monitoring loop */
		log_verbose(LOG_DEBUG, "bdr check loop...");

		switch (monitoring_state)
		{
			case MS_NORMAL:
			{
				if (is_server_available(local_node_info.conninfo) == false)
				{
					// XXX improve
					log_warning("connection problem!");
					do_bdr_failover(&nodes);
				}
				else
				{
					log_verbose(LOG_DEBUG, "sleeping %i seconds (\"monitor_interval_secs\")",
								config_file_options.monitor_interval_secs);
					sleep(config_file_options.monitor_interval_secs);
				}
			}
			case MS_DEGRADED:
			{
				/* degraded monitoring */
				if (is_server_available(local_node_info.conninfo) == true)
				{
					log_notice(_("monitored node %i has recovered"), local_node_info.node_id);
					// do_bdr_recovery()
				}
				else
				{
					log_verbose(LOG_DEBUG, "sleeping %i seconds (\"monitor_interval_secs\")",
								config_file_options.monitor_interval_secs);
					sleep(config_file_options.monitor_interval_secs);
				}
			}
		}

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

			/* reload node list */
			get_all_node_records(local_conn, &nodes);

			got_SIGHUP = false;
		}

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
 *  - attempt to find another node, to set our node record as inactive
 *    (there should be only one other node)
 *  - generate an event log record on that node
 *  - optionally execute `bdr_failover_command`, passing the conninfo string
 *    of that node to the command; this can be used for e.g. reconfiguring
 *    pgbouncer.
 *
 */

void
do_bdr_failover(NodeInfoList *nodes)
{
	PGconn	   *next_node_conn = NULL;
	NodeInfoListCell *cell;
	bool	    failover_success = false;
	PQExpBufferData event_details;
	RecordStatus  record_status;
	t_event_info event_info = T_EVENT_INFO_INITIALIZER;
	t_node_info target_node = T_NODE_INFO_INITIALIZER;

	initPQExpBuffer(&event_details);

	/* get next active node */

	for (cell = nodes->head; cell; cell = cell->next)
	{
		log_debug("do_bdr_failover() %s", cell->node_info->node_name);

		/* don't attempt to connect to the current monitored node, as that's the one which has failed  */
		if (cell->node_info->node_id == local_node_info.node_id)
			continue;

		/* XXX skip inactive node? */
		next_node_conn = establish_db_connection(cell->node_info->conninfo, false);

		if (PQstatus(next_node_conn) == CONNECTION_OK)
		{
			// XXX check if record returned
			record_status = get_node_record(next_node_conn, cell->node_info->node_id, &target_node);

			break;
		}

		next_node_conn = NULL;
	}

	if (next_node_conn == NULL)
	{
		appendPQExpBuffer(&event_details,
						  _("no other available node found"));

		log_error("%s", event_details.data);

		// no other nodes found
		// continue degraded monitoring until node is restored?
	}
	else
	{
		log_info(_("connecting to target node %s"), target_node.node_name);

		failover_success = true;

		event_info.conninfo_str = target_node.conninfo;
		event_info.node_name = target_node.node_name;

		/* update our own record on the other node */
		update_node_record_set_active(next_node_conn, local_node_info.node_id, false);

		appendPQExpBuffer(&event_details,
						  _("node '%s' (ID: %i) detected as failed; next available node is '%s' (ID: %i)"),
						  local_node_info.node_name,
						  local_node_info.node_id,
						  target_node.node_name,
						  target_node.node_id);
	}

	monitoring_state = MS_DEGRADED;
	INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

	// check here that the node hasn't come back up...

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
		config_file_options.node_id,
		"bdr_failover",
		failover_success,
		event_details.data,
		&event_info);

	termPQExpBuffer(&event_details);


	/* local monitoring mode - there's no new node to monitor */
	return;
}
