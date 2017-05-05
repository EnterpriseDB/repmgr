/*
 * repmgr-action-cluster.c
 *
 * Implements master actions for the repmgr command line utility
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include "repmgr.h"

#include "repmgr-client-global.h"
#include "repmgr-action-master.h"


void
do_master_register(void)
{
	PGconn	   *conn = NULL;
	PGconn	   *master_conn = NULL;
	int			current_master_id = UNKNOWN_NODE_ID;
	int			ret;

	t_node_info node_info = T_NODE_INFO_INITIALIZER;
	int			record_found;
	bool		record_created;

	PQExpBufferData	  event_description;

	log_info(_("connecting to master database..."));

	conn = establish_db_connection(config_file_options.conninfo, true);
	log_verbose(LOG_INFO, _("connected to server, checking its state"));

	/* verify that node is running a supported server version */
	check_server_version(conn, "master", true, NULL);

	/* check that node is actually a master */
	ret = is_standby(conn);
	if (ret)
	{
		log_error(_(ret == 1 ? "server is in standby mode and cannot be registered as a master" :
					"connection to node lost!"));

		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	log_verbose(LOG_INFO, _("server is not in recovery"));

	/* create the repmgr extension if it doesn't already exist */
	if (!create_repmgr_extension(conn))
	{
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Ensure there isn't another active master already registered */
	master_conn = get_master_connection(conn, &current_master_id, NULL);

	if (master_conn != NULL)
	{
		if (current_master_id != config_file_options.node_id)
		{
			/* it's impossible to add a second master to a streaming replication cluster */
			log_error(_("there is already an active registered master (node ID: %i) in this cluster"), current_master_id);
			PQfinish(master_conn);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		/* we've probably connected to ourselves */
		PQfinish(master_conn);
	}


	begin_transaction(conn);

	/*
	 * Check if a node with a different ID is registered as master. This shouldn't
	 * happen but could do if an existing master was shut down without being
	 * unregistered.
	*/

	current_master_id = get_master_node_id(conn);
	if (current_master_id != NODE_NOT_FOUND && current_master_id != config_file_options.node_id)
	{
		log_error(_("another node with id %i is already registered as master"), current_master_id);
		// attempt to connect, add info/hint depending if active...
		log_info(_("a streaming replication cluster can have only one master node"));
		rollback_transaction(conn);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Check whether there's an existing record for this node, and
	 * update it if --force set
	 */

	record_found = get_node_record(conn, config_file_options.node_id, &node_info);

	if (record_found)
	{
		if (!runtime_options.force)
		{
			log_error(_("this node is already registered"));
			log_hint(_("use -F/--force to overwrite the existing node record"));
			rollback_transaction(conn);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}
	}
	else
	{
		node_info.node_id = config_file_options.node_id;
	}

	/* if upstream_node_id set, warn that it will be ignored */
	if (config_file_options.upstream_node_id != NO_UPSTREAM_NODE)
	{
		log_warning(_("master node %i is configured with \"upstream_node_id\" set to %i"),
					node_info.node_id,
					config_file_options.upstream_node_id);
		log_detail(_("the value set for \"upstream_node_id\" will be ignored"));
	}
	/* set type to "master", active to "true" and unset upstream_node_id*/
	node_info.type = MASTER;
	node_info.upstream_node_id = NO_UPSTREAM_NODE;
	node_info.active = true;

	/* update node record structure with settings from config file */
	strncpy(node_info.node_name, config_file_options.node_name, MAXLEN);
	strncpy(node_info.conninfo, config_file_options.conninfo, MAXLEN);

	if (repmgr_slot_name_ptr != NULL)
		strncpy(node_info.slot_name, repmgr_slot_name_ptr, MAXLEN);

	node_info.priority = config_file_options.priority;

	initPQExpBuffer(&event_description);
	puts("here");
	if (record_found)
	{
		record_created = update_node_record(conn,
											"master register",
											&node_info);
		if (record_created == true)
		{
			appendPQExpBuffer(&event_description,
							  "existing master record updated");
		}
		else
		{
			appendPQExpBuffer(&event_description,
							  "error encountered while updating master record:\n%s",
							  PQerrorMessage(conn));
		}

	}
	else
	{
		record_created = create_node_record(conn,
											"master register",
											&node_info);
		if (record_created == false)
		{
			appendPQExpBuffer(&event_description,
							  "error encountered while creating master record:\n%s",
							  PQerrorMessage(conn));
		}

	}

	/* Log the event */
	create_event_record(conn,
						&config_file_options,
						config_file_options.node_id,
						"master_register",
						record_created,
						event_description.data);

	if (record_created == false)
	{
		rollback_transaction(conn);
		PQfinish(conn);

		log_notice(_("unable to register master node - see preceding messages"));
		exit(ERR_DB_QUERY);
	}

	commit_transaction(conn);
	PQfinish(conn);

	if (record_found)
	{
		log_notice(_("master node record (id: %i) updated"),
				   config_file_options.node_id);
	}
	else
	{
		log_notice(_("master node record (id: %i) registered"),
				   config_file_options.node_id);
	}

	return;
}
