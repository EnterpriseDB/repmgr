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
		//log_info(_("bdr register: creating database objects inside the %s schema"),
		//			 get_repmgr_schema());

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
	return;
}
