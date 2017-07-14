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

void
do_bdr_node_check(void)
{
	/* nothing to do at the moment */
}


void
monitor_bdr(void)
{
	NodeInfoList  nodes = T_NODE_INFO_LIST_INITIALIZER;
	PGconn		 *monitoring_conn = NULL;
	t_node_info	 *monitored_node = NULL;
	RecordStatus  record_status;

	bool failover_done = false;

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

	/* Log startup event */

	create_event_record(local_conn,
						&config_file_options,
						config_file_options.node_id,
						"repmgrd_start",
						true,
						NULL);

	/*
	 * retrieve list of nodes - we'll need these if the DB connection goes away,
	 * or if we're monitoring a non-local node
	 */
	get_node_records_by_priority(local_conn, &nodes);

		/* decided which node to monitor */

	if (config_file_options.bdr_monitoring_mode == BDR_MONITORING_LOCAL)
	{
		// if local, reuse local_conn and node info
		//record_status = get_node_record(local_conn, config_file_options.node_id, &monitored_node);
		monitored_node = &local_node_info;

		monitoring_conn = establish_db_connection(monitored_node->conninfo, false);
		log_debug("main_loop_bdr() monitoring local node %i", config_file_options.node_id);
	}
	else
	{
		NodeInfoListCell *cell;

		for (cell = nodes.head; cell; cell = cell->next)
		{
			log_debug("main_loop_bdr() checking node %s %i", cell->node_info->node_name, cell->node_info->priority);

			monitoring_conn = establish_db_connection(cell->node_info->conninfo, false);
			if (PQstatus(monitoring_conn) == CONNECTION_OK)
			{
				log_debug("main_loop_bdr() monitoring node '%s' (ID %i, priority %i)",
						  cell->node_info->node_name, cell->node_info->node_id, cell->node_info->priority);
				/* fetch the record again, as the node list is transient */
				monitored_node = get_node_record_pointer(monitoring_conn, cell->node_info->node_id);

				break;
			}
		}
	}

	// check monitored_node not null!

	while (true)
	{
		/* normal state - connection active */
		if (PQstatus(monitoring_conn) == CONNECTION_OK)
		{
			// XXX detail
			log_info(_("starting continuous bdr node monitoring"));

			/* monitoring loop */
			do
			{
				log_verbose(LOG_DEBUG, "bdr check loop...");

				{
					NodeInfoListCell *cell;

					for (cell = nodes.head; cell; cell = cell->next)
					{
						log_debug("bdr_monitor() %s", cell->node_info->node_name);
					}
				}

				if (is_server_available(monitored_node->conninfo) == false)
				{
					t_node_info  *new_monitored_node;

					// XXX improve
					log_warning("connection problem!");
					new_monitored_node = do_bdr_failover(&nodes, monitored_node);

					if (new_monitored_node != NULL)
					{
						pfree(monitored_node);
						monitored_node = new_monitored_node;
					}
					log_notice(_("monitored_node->node_name is now '%s' \n"), monitored_node->node_name);
				}
				else
				{
					sleep(config_file_options.monitor_interval_secs);
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
					get_node_records_by_priority(local_conn, &nodes);

					got_SIGHUP = false;
				}

			} while (!failover_done);
		}
		/* local connection inactive - periodically try and connect */
		/* TODO: make this an option */
		else
		{

			monitoring_conn = establish_db_connection(monitored_node->conninfo, false);

			if (PQstatus(monitoring_conn) == CONNECTION_OK)
			{
				// XXX event bdr_node_recovered -> if monitored == local node

				if (monitored_node->node_id == config_file_options.node_id)
				{
					log_notice(_("local connection has returned, resuming monitoring"));
				}
				else
				{
					log_notice(_("connection to '%s' has returned, resuming monitoring"), monitored_node->node_name);
				}
			}
			else
			{
				sleep(config_file_options.monitor_interval_secs);
			}


			if (got_SIGHUP)
			{
				/*
				 * if we can reload, then could need to change
				 * local_conn
				 */
				if (reload_config(&config_file_options))
				{
					if (PQstatus(local_conn) == CONNECTION_OK)
					{
						PQfinish(local_conn);
						local_conn = establish_db_connection(config_file_options.conninfo, true);
						update_registration(local_conn);
					}
				}

				/* reload node list */
				if (PQstatus(local_conn) == CONNECTION_OK)
					get_node_records_by_priority(local_conn, &nodes);

				got_SIGHUP = false;
			}
		}

		failover_done = false;
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
 *  - generate an event log record on that node
 *  - optionally execute `bdr_failover_command`, passing the conninfo string
 *    of that node to the command; this can be used for e.g. reconfiguring
 *    pgbouncer.
 *  - if mode is 'BDR_MONITORING_PRIORITY', redirect monitoring to that node.
 *
 */
t_node_info *
do_bdr_failover(NodeInfoList *nodes, t_node_info *monitored_node)
{
	PGconn	   *next_node_conn = NULL;
	NodeInfoListCell *cell;
	bool	    failover_success = false;
	PQExpBufferData event_details;
	t_event_info event_info = T_EVENT_INFO_INITIALIZER;
	t_node_info *new_monitored_node = NULL;

	initPQExpBuffer(&event_details);

	/* get next active priority node */

	for (cell = nodes->head; cell; cell = cell->next)
	{
		log_debug("do_bdr_failover() %s", cell->node_info->node_name);

		/* don't attempt to connect to the current monitored node, as that's the one which has failed  */
		if (cell->node_info->node_id == monitored_node->node_id)
			continue;

		/* XXX skip inactive node? */

		next_node_conn = establish_db_connection(cell->node_info->conninfo, false);

		if (PQstatus(next_node_conn) == CONNECTION_OK)
		{
			// XXX check if record returned
			new_monitored_node = get_node_record_pointer(next_node_conn, cell->node_info->node_id);

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
		log_info(_("connecting to target node %s"), cell->node_info->node_name);

		failover_success = true;

		event_info.conninfo_str = cell->node_info->conninfo;
		event_info.node_name = cell->node_info->node_name;

		/* update our own record on the other node */
		if (monitored_node->node_id == config_file_options.node_id)
		{
			update_node_record_set_active(next_node_conn, monitored_node->node_id, false);
		}

		if (config_file_options.bdr_monitoring_mode == BDR_MONITORING_PRIORITY)
		{
			log_notice(_("monitoring next available node by prioriy: %s (ID %i)"),
					   new_monitored_node->node_name,
					   new_monitored_node->node_id);
		}

		appendPQExpBuffer(&event_details,
						  _("node '%s' (ID: %i) detected as failed; next available node is '%s' (ID: %i)"),
						  monitored_node->node_name,
						  monitored_node->node_id,
						  cell->node_info->node_name,
						  cell->node_info->node_id);
	}

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

	//failover_done = true;

	if (config_file_options.bdr_monitoring_mode == BDR_MONITORING_PRIORITY)
		return new_monitored_node;

	/* local monitoring mode - there's no new node to monitor */
	return NULL;
}
