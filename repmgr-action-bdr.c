/*
 * repmgr-action-standby.c
 *
 * Implements BDR-related actions for the repmgr command line utility
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include "repmgr.h"

#include "repmgr-client-global.h"
#include "repmgr-action-bdr.h"


/*
 * do_bdr_register()
 *
 * As each BDR node is its own master, registering a BDR node
 * will create the repmgr metadata schema if necessary.
 */
void
do_bdr_register(void)
{
	PGconn	   	   *conn = NULL;
    ExtensionStatus extension_status;
	t_node_info		node_info = T_NODE_INFO_INITIALIZER;
	RecordStatus	record_status;
	PQExpBufferData event_details;
	bool	   	    success = true;

	/* sanity-check configuration for BDR-compatability */
	if (config_file_options.replication_type != REPLICATION_TYPE_BDR)
	{
		log_error(_("cannot run BDR REGISTER on a non-BDR node"));
		exit(ERR_BAD_CONFIG);
	}

	conn = establish_db_connection(config_file_options.conninfo, true);

	if (!is_bdr_db(conn))
	{
		/* TODO: name database */
		log_error(_("database is not BDR-enabled"));
		log_hint(_("when using repmgr with BDR, the repmgr schema must be stored in the BDR database"));
		exit(ERR_BAD_CONFIG);
	}

	/* check whether repmgr extension exists, and that any other nodes are BDR */
	extension_status = get_repmgr_extension_status(conn);

	if (extension_status == REPMGR_UNKNOWN)
	{
		log_error(_("unable to determine status of \"repmgr\" extension"));
		PQfinish(conn);
	}


	if (extension_status == REPMGR_UNAVAILABLE)
	{
		log_error(_("\"repmgr\" extension is not available"));
		PQfinish(conn);
	}

	if (extension_status == REPMGR_INSTALLED)
	{
		if (!is_bdr_repmgr(conn))
		{
			log_error(_("repmgr metadatabase contains records for non-BDR nodes"));
			exit(ERR_BAD_CONFIG);
		}

	}
	else
	{
		log_info(_("creating repmgr extension"));

		begin_transaction(conn);

		if (!create_repmgr_extension(conn))
		{
			log_error(_("unable to create repmgr extension - see preceding error message(s); aborting"));
			rollback_transaction(conn);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		commit_transaction(conn);
	}

	/*
	 * before adding the extension tables to the replication set,
	 * if any other BDR nodes exist, populate repmgr.nodes with a copy
	 * of existing entries
	 *
	 * currently we won't copy the contents of any other tables
	 *
	 */
	{
		NodeInfoList local_node_records = T_NODE_INFO_LIST_INITIALIZER;
		get_all_node_records(conn, &local_node_records);

		if (local_node_records.node_count == 0)
		{
			/* XXX get all BDR node records */
			RecordStatus bdr_record_status;
			t_bdr_node_info bdr_init_node_info = T_BDR_NODE_INFO_INITIALIZER;

			bdr_record_status = get_bdr_init_node_record(conn, &bdr_init_node_info);

			if (bdr_record_status != RECORD_FOUND)
			{
				/* XXX don't assume the original node will still be part of the cluster */
				log_error(_("unable to retrieve record for originating node"));
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}

			if (strncmp(node_info.node_name, bdr_init_node_info.node_name, MAXLEN) != 0)
			{
				/* */
				PGconn *init_node_conn;
				NodeInfoList existing_nodes = T_NODE_INFO_LIST_INITIALIZER;
				NodeInfoListCell *cell;

				init_node_conn = establish_db_connection_quiet(bdr_init_node_info.node_local_dsn);

				/* XXX check repmgr schema exists */
				get_all_node_records(init_node_conn, &existing_nodes);

				for (cell = existing_nodes.head; cell; cell = cell->next)
				{
					create_node_record(conn, "bdr register", cell->node_info);
				}
			}
		}
	}

	/* Add the repmgr extension tables to a replication set */
	add_extension_tables_to_bdr_replication_set(conn);

	/* check for a matching BDR node */
	{
		bool node_exists = bdr_node_exists(conn, config_file_options.node_name);

		if (node_exists == false)
		{
			log_error(_("no BDR node with node_name '%s' found"), config_file_options.node_name);
			log_hint(_("'node_name' in repmgr.conf must match 'node_name' in bdr.bdr_nodes\n"));
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	initPQExpBuffer(&event_details);

	begin_transaction(conn);

	/*
	 * we'll check if a record exists (even if the schema was just created),
	 * as there's a faint chance of a race condition
	 */

	record_status = get_node_record(conn, config_file_options.node_id, &node_info);

	/* Update internal node record */

	node_info.type = BDR;
	node_info.node_id = config_file_options.node_id;
	node_info.upstream_node_id = NO_UPSTREAM_NODE;
	node_info.active = true;
	node_info.priority = config_file_options.priority;

	strncpy(node_info.node_name, config_file_options.node_name, MAXLEN);
	strncpy(node_info.location, config_file_options.location, MAXLEN);
	strncpy(node_info.conninfo, config_file_options.conninfo, MAXLEN);

	if (record_status == RECORD_FOUND)
	{
		bool node_updated;
		/*
		 * At this point we will have established there are no non-BDR records,
		 * so no need to verify the node type
		 */
		if (!runtime_options.force)
		{
			log_error(_("this node is already registered"));
			log_hint(_("use -F/--force to overwrite the existing node record"));
			rollback_transaction(conn);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		/*
		 * don't permit changing the node name - this must match the
		 * BDR node name set when the node was registered.
		 */

		if (strncmp(node_info.node_name, config_file_options.node_name, MAXLEN) != 0)
		{
			log_error(_("a record for node %i is already registered with node_name '%s'"),
					  config_file_options.node_id, node_info.node_name);
			log_hint(_("node_name configured in repmgr.conf is '%s'"), config_file_options.node_name);

			rollback_transaction(conn);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		node_updated = update_node_record(conn, "bdr register", &node_info);

		if (node_updated == true)
		{
			appendPQExpBuffer(&event_details, _("node record updated for node '%s' (%i)"),
							  config_file_options.node_name, config_file_options.node_id);
			log_verbose(LOG_NOTICE, "%s\n", event_details.data);
		}
		{
			success = false;
		}

	}
	else
	{
		/* create new node record */
		bool node_created = create_node_record(conn, "bdr register", &node_info);

		if (node_created == true)
		{
			appendPQExpBuffer(&event_details,
							  _("node record created for node '%s' (ID: %i)"),
							  config_file_options.node_name, config_file_options.node_id);
			log_notice("%s", event_details.data);
		}
		else
		{
			success = false;
		}
	}

	if (success == false)
	{
		rollback_transaction(conn);
		PQfinish(conn);
		exit(ERR_DB_QUERY);
	}

	commit_transaction(conn);
	/* Log the event */
	create_event_notification(
		conn,
		&config_file_options,
		config_file_options.node_id,
		"bdr_register",
		true,
		event_details.data);

	termPQExpBuffer(&event_details);

	PQfinish(conn);

	log_notice(_("BDR node %i registered (conninfo: %s)"),
			   config_file_options.node_id, config_file_options.conninfo);

	return;
}


void
do_bdr_unregister(void)
{
	PGconn	   	   *conn;
    ExtensionStatus extension_status;
	int 			target_node_id;
	t_node_info		node_info = T_NODE_INFO_INITIALIZER;
	RecordStatus	record_status;
	bool			node_record_deleted;
	PQExpBufferData event_details;

	/* sanity-check configuration for BDR-compatability */

	if (config_file_options.replication_type != REPLICATION_TYPE_BDR)
	{
		log_error(_("cannot run BDR UNREGISTER on a non-BDR node"));
		exit(ERR_BAD_CONFIG);
	}

	conn = establish_db_connection(config_file_options.conninfo, true);

	if (!is_bdr_db(conn))
	{
		/* TODO: name database */
		log_error(_("database is not BDR-enabled"));
		exit(ERR_BAD_CONFIG);
	}

	extension_status = get_repmgr_extension_status(conn);
	if (extension_status != REPMGR_INSTALLED)
	{
		log_error(_("repmgr is not installed on this database"));
		exit(ERR_BAD_CONFIG);
	}

	if (!is_bdr_repmgr(conn))
	{
		log_error(_("repmgr metadatabase contains records for non-BDR nodes"));
		exit(ERR_BAD_CONFIG);
	}

	initPQExpBuffer(&event_details);
	if (runtime_options.node_id != UNKNOWN_NODE_ID)
		target_node_id = runtime_options.node_id;
	else
		target_node_id = config_file_options.node_id;


	/* Check node exists and is really a BDR node */
	record_status = get_node_record(conn, target_node_id, &node_info);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("no record found for node %i"), target_node_id);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	// BDR node

	begin_transaction(conn);

	log_info(_("unregistering node %i"), target_node_id);

	node_record_deleted = delete_node_record(conn, target_node_id);

	if (node_record_deleted == false)
	{
		appendPQExpBuffer(&event_details,
						  "unable to delete node record for node \"%s\" (ID: %i)",
						  node_info.node_name,
						  target_node_id);
	}
	else
	{
		appendPQExpBuffer(&event_details,
						  "node record deleted for node \"%s\" (ID: %i)",
						  node_info.node_name,
						  target_node_id);
	}
	commit_transaction(conn);

	/* Log the event */
	create_event_notification(
		conn,
		&config_file_options,
		config_file_options.node_id,
		"bdr_unregister",
		true,
		event_details.data);

	PQfinish(conn);

	log_notice(_("bdr node \"%s\" (ID: %i) successfully unregistered"),
			   node_info.node_name, target_node_id);

	termPQExpBuffer(&event_details);

	return;
}
