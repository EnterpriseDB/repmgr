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
	t_recovery_type recovery_type;
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
	recovery_type = get_recovery_type(conn);

	if (recovery_type != RECTYPE_MASTER)
	{
		if (recovery_type == RECTYPE_STANDBY)
		{
			log_error(_("server is in standby mode and cannot be registered as a master"));
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}
		else
		{
			log_error(_("connection to node lost"));
			PQfinish(conn);
			exit(ERR_DB_CONN);
		}
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

void
do_master_unregister(void)
{
	PGconn	    *master_conn = NULL;
	PGconn	    *local_conn = NULL;
	t_node_info  local_node_info = T_NODE_INFO_INITIALIZER;
	bool	     record_found;

	t_node_info *target_node_info_ptr;
	PGconn	    *target_node_conn = NULL;

	/* We must be able to connect to the local node */
	local_conn = establish_db_connection(config_file_options.conninfo, true);

	/* From which we obtain a connection to the master node */
	master_conn = establish_master_db_connection(local_conn, true);

	/* Local connection no longer required */
	PQfinish(local_conn);

	/* Get local node record  */
	record_found = get_node_record(master_conn, config_file_options.node_id, &local_node_info);

	// XXX add function get_local_node_record() which aborts as below
	if (record_found == FALSE)
	{
		log_error(_("unable to retrieve record for local node"));
		log_detail(_("local node id is  %i"), config_file_options.node_id);
		log_hint(_("check this node was correctly registered"));

		exit(ERR_BAD_CONFIG);
	}

	/* Target node is local node? */
	if (target_node_info.node_id == UNKNOWN_NODE_ID
	 || target_node_info.node_id == config_file_options.node_id)
	{
		target_node_info_ptr = &local_node_info;
	}
	/* Target node is explicitly specified, and is not local node */
	else
	{
		target_node_info_ptr = &target_node_info;
	}



	target_node_conn = establish_db_connection_quiet(target_node_info_ptr->conninfo);

	/* If node not reachable, check that the record is for a master node */
	if (PQstatus(target_node_conn) != CONNECTION_OK)
	{
		if (target_node_info_ptr->type != MASTER)
		{
			log_error(_("node %s (id: %i) is not a master, unable to unregister"),
					  target_node_info_ptr->node_name,
					  target_node_info_ptr->node_id);
			if (target_node_info_ptr->type == STANDBY)
			{
				log_hint(_("the node can be unregistered with \"repmgr standby unregister\""));
			}

			PQfinish(master_conn);
			exit(ERR_BAD_CONFIG);
		}
	}
	/* If we can connect to the node, perform some sanity checks on it */
	else
	{
		t_recovery_type recovery_type = get_recovery_type(target_node_conn);

		/* Node appears to be a standby */
		if (recovery_type == RECTYPE_STANDBY)
		{
			/*
			 * If --F/--force not set, hint that it might be appropriate to
			 * register the node as a standby rather than unregister as master
			 */
			if (!runtime_options.force)
			{
				log_error(_("node %s (id: %i) is a standby, unable to unregister"),
						  target_node_info_ptr->node_name,
						  target_node_info_ptr->node_id);
				log_hint(_("the node can be registered as a standby with \"repmgr standby register --force\""));
				log_hint(_("use \"repmgr master unregister --force\" to remove this node's metadata entirely"));

				PQfinish(target_node_conn);
				PQfinish(master_conn);
				exit(ERR_BAD_CONFIG);
			}
		}
		else if (recovery_type == RECTYPE_MASTER)
		{
			t_node_info  master_node_info = T_NODE_INFO_INITIALIZER;
			bool master_record_found;

			master_record_found = get_master_node_record(local_conn, &master_node_info);

			if (master_record_found == false)
			{
				log_error(_("node %s (id: %i) is a master node, but no master node record found"),
						  target_node_info_ptr->node_name,
						  target_node_info_ptr->node_id);
				log_hint(_("register this node as master with \"repmgr master register --force\""));
				PQfinish(target_node_conn);
				PQfinish(master_conn);
				exit(ERR_BAD_CONFIG);
			}
			/* This appears to be the cluster master - cowardly refuse
			 * to delete the record
			 */
			if (master_node_info.node_id == target_node_info_ptr->node_id)
			{
				log_error(_("node %s (id: %i) is the current master node, unable to unregister"),
						  target_node_info_ptr->node_name,
						  target_node_info_ptr->node_id);

				if (master_node_info.active == true)
				{
					log_hint(_("node is marked as inactive, activate with \"repmgr master register --force\""));
				}
				PQfinish(target_node_conn);
				PQfinish(master_conn);
				exit(ERR_BAD_CONFIG);
			}
		}

		/* We don't need the target node connection any more */
		PQfinish(target_node_conn);
	}

	if (target_node_info_ptr->active == true)
	{
		if (!runtime_options.force)
		{
			log_error(_("node %s (id: %i) is marked as active, unable to unregister"),
					  target_node_info_ptr->node_name,
					  target_node_info_ptr->node_id);
			log_hint(_("run \"repmgr master unregister --force\" to unregister this node"));
			PQfinish(master_conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	// check if any records point to this record, detail: each, hint: follow or unregister

	if (runtime_options.dry_run == true)
	{
		log_notice(_("node %s (id: %i) would now be unregistered"),
				   target_node_info_ptr->node_name,
				   target_node_info_ptr->node_id);
		log_hint(_("run the same command without the --dry-run option to unregister this node"));
	}
	else
	{
		bool delete_success = delete_node_record(master_conn,
												 target_node_info_ptr->node_id);

		if (delete_success == false)
		{
			log_error(_("unable to unregister node %s (id: %i)"),
					  target_node_info_ptr->node_name,
					  target_node_info_ptr->node_id);
			PQfinish(master_conn);
			exit(ERR_DB_QUERY);
		}

		log_info(_("node %s (id: %i) was successfully unregistered"),
				 target_node_info_ptr->node_name,
				 target_node_info_ptr->node_id);
	}

	PQfinish(master_conn);
	return;
}
