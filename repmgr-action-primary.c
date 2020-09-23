/*
 * repmgr-action-primary.c
 *
 * Implements primary actions for the repmgr command line utility
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

#include "repmgr-client-global.h"
#include "repmgr-action-primary.h"


/*
 * do_primary_register()
 *
 * Event(s):
 *  - primary_register
 */
void
do_primary_register(void)
{
	PGconn	   *conn = NULL;
	PGconn	   *primary_conn = NULL;
	int			current_primary_id = UNKNOWN_NODE_ID;
	RecoveryType recovery_type = RECTYPE_UNKNOWN;
	t_node_info node_info = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status = RECORD_NOT_FOUND;

	bool		record_created = false;

	PQExpBufferData event_description;

	log_info(_("connecting to primary database..."));

	conn = establish_db_connection(config_file_options.conninfo, true);
	log_verbose(LOG_INFO, _("connected to server, checking its state"));

	/* verify that node is running a supported server version */
	check_server_version(conn, "primary", true, NULL);

	/* check that node is actually a primary */
	recovery_type = get_recovery_type(conn);

	if (recovery_type != RECTYPE_PRIMARY)
	{
		if (recovery_type == RECTYPE_STANDBY)
		{
			log_error(_("server is in standby mode and cannot be registered as a primary"));
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		log_error(_("unable to determine server's recovery type"));
		PQfinish(conn);
		exit(ERR_DB_CONN);
	}

	log_verbose(LOG_INFO, _("server is not in recovery"));

	/*
	 * create the repmgr extension if it doesn't already exist;
	 * note that create_repmgr_extension() will take into account
	 * the --dry-run option
	 */
	if (!create_repmgr_extension(conn))
	{
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * In --dry-run mode we can't proceed any further as the following code
	 * attempts to query the repmgr metadata, which won't exist until the
	 * extension is installed
	 */
	if (runtime_options.dry_run == true)
	{
		PQfinish(conn);
		return;
	}

	initialize_voting_term(conn);

	begin_transaction(conn);

	/*
	 * Check for an active primary node record with a different ID. This
	 * shouldn't happen, but could do if an existing primary was shut down
	 * without being unregistered.
	 */
	current_primary_id = get_primary_node_id(conn);
	if (current_primary_id != NODE_NOT_FOUND && current_primary_id != config_file_options.node_id)
	{
		log_debug("current active primary node ID is %i", current_primary_id);
		primary_conn = establish_primary_db_connection(conn, false);

		if (PQstatus(primary_conn) == CONNECTION_OK)
		{
			if (get_recovery_type(primary_conn) == RECTYPE_PRIMARY)
			{
				log_error(_("there is already an active registered primary (ID: %i) in this cluster"),
						  current_primary_id);
				log_detail(_("a streaming replication cluster can have only one primary node"));

				log_hint(_("ensure this node is shut down before registering a new primary"));
				PQfinish(primary_conn);
				rollback_transaction(conn);
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}

			log_warning(_("node %is is registered as primary but running as a standby"),
						  current_primary_id);
			PQfinish(primary_conn);
		}

		log_notice(_("setting node %i's node record to inactive"),
						  current_primary_id);
		update_node_record_set_active(conn, current_primary_id, false);
	}

	/*
	 * Check whether there's an existing record for this node, and update it
	 * if --force set
	 */

	record_status = get_node_record(conn, config_file_options.node_id, &node_info);

	if (record_status == RECORD_FOUND)
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

	init_node_record(&node_info);

	/* set type to "primary" and unset upstream_node_id */
	node_info.type = PRIMARY;
	node_info.upstream_node_id = NO_UPSTREAM_NODE;

	initPQExpBuffer(&event_description);

	if (record_status == RECORD_FOUND)
	{
		record_created = update_node_record(conn,
											"primary register",
											&node_info);
		if (record_created == true)
		{
			appendPQExpBufferStr(&event_description,
								 "existing primary record updated");
		}
		else
		{
			appendPQExpBuffer(&event_description,
							  "error encountered while updating primary record:\n%s",
							  PQerrorMessage(conn));
		}

	}
	else
	{
		record_created = create_node_record(conn,
											"primary register",
											&node_info);
		if (record_created == false)
		{
			appendPQExpBuffer(&event_description,
							  "error encountered while creating primary record:\n%s",
							  PQerrorMessage(conn));
		}

	}

	if (record_created == false)
	{
		rollback_transaction(conn);
	}
	else
	{
		commit_transaction(conn);
	}

	/* Log the event */
	create_event_notification(
							  conn,
							  &config_file_options,
							  config_file_options.node_id,
							  "primary_register",
							  record_created,
							  event_description.data);

	termPQExpBuffer(&event_description);

	PQfinish(conn);

	if (record_created == false)
	{
		log_notice(_("unable to register primary node - see preceding messages"));
		exit(ERR_DB_QUERY);
	}

	if (record_status == RECORD_FOUND)
	{
		log_notice(_("primary node record (ID: %i) updated"),
				   config_file_options.node_id);
	}
	else
	{
		log_notice(_("primary node record (ID: %i) registered"),
				   config_file_options.node_id);
	}

	return;
}


/*
 * do_primary_unregister()
 *
 * Event(s):
 *  - primary_unregister
 */

void
do_primary_unregister(void)
{
	PGconn	   *primary_conn = NULL;
	PGconn	   *local_conn = NULL;
	t_node_info local_node_info = T_NODE_INFO_INITIALIZER;
	t_node_info primary_node_info = T_NODE_INFO_INITIALIZER;

	t_node_info *target_node_info_ptr = NULL;
	PGconn	   *target_node_conn = NULL;

	NodeInfoList downstream_nodes = T_NODE_INFO_LIST_INITIALIZER;

	/* We must be able to connect to the local node */
	local_conn = establish_db_connection(config_file_options.conninfo, true);

	/* Get local node record */
	get_local_node_record(local_conn, config_file_options.node_id, &local_node_info);

	/*
	 * Obtain a connection to the current primary node - if this isn't
	 * possible, abort as we won't be able to update the "nodes" table anyway.
	 */
	primary_conn = establish_primary_db_connection(local_conn, false);

	if (PQstatus(primary_conn) != CONNECTION_OK)
	{
		log_error(_("unable to connect to primary server"));

		if (get_primary_node_record(local_conn, &primary_node_info) == true)
		{
			log_detail(_("current primary registered as node \"%s\" (ID: %i, conninfo: \"%s\")"),
					   primary_node_info.node_name,
					   primary_node_info.node_id,
					   primary_node_info.conninfo);
		}

		log_hint(_("you may need to promote this standby or ask it to look for a new primary to follow"));
		PQfinish(local_conn);
		exit(ERR_DB_CONN);
	}

	/* Local connection no longer required */
	PQfinish(local_conn);

	if (get_primary_node_record(primary_conn, &primary_node_info) == false)
	{
		log_error(_("unable to retrieve record for primary node"));
		PQfinish(primary_conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Target node is local node? */
	if (target_node_info.node_id == UNKNOWN_NODE_ID)
	{
		target_node_info_ptr = &primary_node_info;
	}
	else if (target_node_info.node_id == config_file_options.node_id)
	{
		target_node_info_ptr = &local_node_info;
	}
	/* Target node is explicitly specified, and is not local node */
	else
	{
		target_node_info_ptr = &target_node_info;
	}

	/*
	 * Sanity-check the target node is not a witness
	 */

	if (target_node_info_ptr->type == WITNESS)
	{
		log_error(_("node \"%s\" (ID: %i) is a witness server, unable to unregister"),
					  target_node_info_ptr->node_name,
					  target_node_info_ptr->node_id);
		if (target_node_info_ptr->type == STANDBY)
		{
			log_hint(_("the node can be unregistered with \"repmgr witness unregister\""));
		}

		PQfinish(primary_conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Check for downstream nodes - if any still defined, we won't be able to
	 * delete the node record due to foreign key constraints.
	 */
	get_downstream_node_records(primary_conn, target_node_info_ptr->node_id, &downstream_nodes);

	if (downstream_nodes.node_count > 0)
	{
		NodeInfoListCell *cell = NULL;
		PQExpBufferData detail;

		if (downstream_nodes.node_count == 1)
		{
			log_error(_("%i other node still has this node as its upstream node"),
					  downstream_nodes.node_count);
		}
		else
		{
			log_error(_("%i other nodes still have this node as their upstream node"),
					  downstream_nodes.node_count);
		}

		log_hint(_("ensure these nodes are following the current primary with \"repmgr standby follow\""));

		initPQExpBuffer(&detail);

		for (cell = downstream_nodes.head; cell; cell = cell->next)
		{
			appendPQExpBuffer(&detail,
							  "  %s (ID: %i)\n",
							  cell->node_info->node_name,
							  cell->node_info->node_id);
		}

		log_detail(_("the affected node(s) are:\n%s"), detail.data);

		termPQExpBuffer(&detail);
		PQfinish(primary_conn);

		exit(ERR_BAD_CONFIG);
	}

	target_node_conn = establish_db_connection_quiet(target_node_info_ptr->conninfo);

	/* If node not reachable, check that the record is for a primary node */
	if (PQstatus(target_node_conn) != CONNECTION_OK)
	{
		if (target_node_info_ptr->type != PRIMARY)
		{
			log_error(_("node \"%s\" (ID: %i) is not a primary, unable to unregister"),
					  target_node_info_ptr->node_name,
					  target_node_info_ptr->node_id);
			if (target_node_info_ptr->type == STANDBY)
			{
				log_hint(_("the node can be unregistered with \"repmgr standby unregister\""));
			}

			PQfinish(primary_conn);
			exit(ERR_BAD_CONFIG);
		}
	}
	/* If we can connect to the node, perform some sanity checks on it */
	else
	{
		bool		can_unregister = true;
		RecoveryType recovery_type = get_recovery_type(target_node_conn);

		/* Node appears to be a standby */
		if (recovery_type == RECTYPE_STANDBY)
		{
			/*
			 * We'll refuse to do anything unless the node record shows it as
			 * a primary
			 */
			if (target_node_info_ptr->type != PRIMARY)
			{
				log_error(_("node \"%s\" (ID: %i) is a %s, unable to unregister"),
						  target_node_info_ptr->node_name,
						  target_node_info_ptr->node_id,
						  get_node_type_string(target_node_info_ptr->type));
				can_unregister = false;
			}

			/*
			 * If --F/--force not set, hint that it might be appropriate to
			 * register the node as a standby rather than unregister as
			 * primary
			 */
			else if (!runtime_options.force)
			{
				log_error(_("node \"%s\" (ID: %i) is running as a standby, unable to unregister"),
						  target_node_info_ptr->node_name,
						  target_node_info_ptr->node_id);
				log_hint(_("the node can be registered as a standby with \"repmgr standby register --force\""));
				log_hint(_("use \"repmgr primary unregister --force\" to remove this node's metadata entirely"));
				can_unregister = false;
			}


			if (can_unregister == false)
			{
				PQfinish(target_node_conn);
				PQfinish(primary_conn);
				exit(ERR_BAD_CONFIG);
			}
		}
		else if (recovery_type == RECTYPE_PRIMARY)
		{
			t_node_info primary_node_info = T_NODE_INFO_INITIALIZER;
			bool		primary_record_found = false;

			primary_record_found = get_primary_node_record(primary_conn, &primary_node_info);

			if (primary_record_found == false)
			{
				log_error(_("node \"%s\" (ID: %i) is a primary node, but no primary node record found"),
						  target_node_info_ptr->node_name,
						  target_node_info_ptr->node_id);
				log_hint(_("register this node as primary with \"repmgr primary register --force\""));
				PQfinish(target_node_conn);
				PQfinish(primary_conn);
				exit(ERR_BAD_CONFIG);
			}

			/*
			 * This appears to be the cluster primary - cowardly refuse to
			 * delete the record, unless --force is supplied.
			 */
			if (primary_node_info.node_id == target_node_info_ptr->node_id && !runtime_options.force)
			{
				log_error(_("node \"%s\" (ID: %i) is the current primary node, unable to unregister"),
						  target_node_info_ptr->node_name,
						  target_node_info_ptr->node_id);

				if (primary_node_info.active == false)
				{
					log_hint(_("node is marked as inactive, activate with \"repmgr primary register --force\""));
				}
				PQfinish(target_node_conn);
				PQfinish(primary_conn);
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
			log_error(_("node \"%s\" (ID: %i) is marked as active, unable to unregister"),
					  target_node_info_ptr->node_name,
					  target_node_info_ptr->node_id);
			log_hint(_("run \"repmgr primary unregister --force\" to unregister this node"));
			PQfinish(primary_conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	if (runtime_options.dry_run == true)
	{
		log_notice(_("node \"%s\" (ID: %i) would now be unregistered"),
				   target_node_info_ptr->node_name,
				   target_node_info_ptr->node_id);
		log_hint(_("run the same command without the --dry-run option to unregister this node"));
	}
	else
	{
		PQExpBufferData event_details;
		bool		delete_success = delete_node_record(primary_conn,
														target_node_info_ptr->node_id);

		if (delete_success == false)
		{
			log_error(_("unable to unregister node \"%s\" (ID: %i)"),
					  target_node_info_ptr->node_name,
					  target_node_info_ptr->node_id);
			PQfinish(primary_conn);
			exit(ERR_DB_QUERY);
		}

		initPQExpBuffer(&event_details);
		appendPQExpBuffer(&event_details,
						  _("node \"%s\" (ID: %i) unregistered"),
						  target_node_info_ptr->node_name,
						  target_node_info_ptr->node_id);

		if (target_node_info_ptr->node_id != config_file_options.node_id)
		{
			appendPQExpBuffer(&event_details,
							  _(" from node \"%s\" (ID: %i)"),
							  config_file_options.node_name,
							  config_file_options.node_id);
		}

		create_event_notification(primary_conn,
								  &config_file_options,
								  config_file_options.node_id,
								  "primary_unregister",
								  true,
								  event_details.data);
		termPQExpBuffer(&event_details);

		log_info(_("node \"%s\" (ID: %i) was successfully unregistered"),
				 target_node_info_ptr->node_name,
				 target_node_info_ptr->node_id);
	}

	PQfinish(primary_conn);
	return;
}



void
do_primary_help(void)
{
	print_help_header();

	printf(_("Usage:\n"));
	printf(_("    %s [OPTIONS] primary register\n"), progname());
	printf(_("    %s [OPTIONS] primary unregister\n"), progname());
	puts("");
	printf(_("  Note: \"%s master ...\" can be used as an alias\n"), progname());
	puts("");

	printf(_("PRIMARY REGISTER\n"));
	puts("");
	printf(_("  \"primary register\" initialises the repmgr cluster and registers the primary node.\n"));
	puts("");
	printf(_("  --dry-run                           check that the prerequisites are met for registering the primary\n" \
			 "                                      (including availability of the repmgr extension)\n"));
	printf(_("  -F, --force                         overwrite an existing node record\n"));
	puts("");

	printf(_("PRIMARY UNREGISTER\n"));
	puts("");
	printf(_("  \"primary unregister\" unregisters an inactive primary node.\n"));
	puts("");
	printf(_("  --dry-run                           check what would happen, but don't actually unregister the primary\n"));
	printf(_("  --node-id                           ID of the inactive primary node to unregister.\n"));
	printf(_("  -F, --force                         force removal of an active record\n"));

	puts("");

	printf(_("%s home page: <%s>\n"), "repmgr", REPMGR_URL);
}
