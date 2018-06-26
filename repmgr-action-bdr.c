/*
 * repmgr-action-bdr.c
 *
 * Implements BDR-related actions for the repmgr command line utility
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

#include "repmgr.h"

#include "repmgr-client-global.h"
#include "repmgr-action-bdr.h"


/*
 * do_bdr_register()
 *
 * As each BDR node is its own primary, registering a BDR node
 * will create the repmgr metadata schema if necessary.
 */
void
do_bdr_register(void)
{
	PGconn	   *conn = NULL;
	BdrNodeInfoList bdr_nodes = T_BDR_NODE_INFO_LIST_INITIALIZER;
	ExtensionStatus extension_status = REPMGR_UNKNOWN;
	t_node_info node_info = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status = RECORD_NOT_FOUND;
	PQExpBufferData event_details;
	bool		success = true;
	char	   *dbname = NULL;

	/* sanity-check configuration for BDR-compatability */
	if (config_file_options.replication_type != REPLICATION_TYPE_BDR)
	{
		log_error(_("cannot run BDR REGISTER on a non-BDR node"));
		exit(ERR_BAD_CONFIG);
	}

	dbname = pg_malloc0(MAXLEN);

	if (dbname == NULL)
	{
		log_error(_("unable to allocate memory; terminating."));
		exit(ERR_OUT_OF_MEMORY);
	}

	/* store the database name for future reference */
	get_conninfo_value(config_file_options.conninfo, "dbname", dbname);

	conn = establish_db_connection(config_file_options.conninfo, true);

	if (!is_bdr_db(conn, NULL))
	{
		log_error(_("database \"%s\" is not BDR-enabled"), dbname);
		log_hint(_("when using repmgr with BDR, the repmgr schema must be stored in the BDR database"));
		PQfinish(conn);
		pfree(dbname);
		exit(ERR_BAD_CONFIG);
	}

	/* Check that there are at most 2 BDR nodes */
	get_all_bdr_node_records(conn, &bdr_nodes);

	if (bdr_nodes.node_count == 0)
	{
		log_error(_("database \"%s\" is BDR-enabled but no BDR nodes were found"), dbname);
		PQfinish(conn);
		pfree(dbname);
		exit(ERR_BAD_CONFIG);
	}

	/* BDR 2 implementation is for 2 nodes only */
	if (get_bdr_version_num() < 3 && bdr_nodes.node_count > 2)
	{
		log_error(_("repmgr can only support BDR 2.x clusters with 2 nodes"));
		log_detail(_("this BDR cluster has %i nodes"), bdr_nodes.node_count);
		PQfinish(conn);
		pfree(dbname);
		exit(ERR_BAD_CONFIG);
	}

	/* check for a matching BDR node */
	{
		PQExpBufferData bdr_local_node_name;
		bool		node_match = false;

		initPQExpBuffer(&bdr_local_node_name);
		node_match = bdr_node_name_matches(conn, config_file_options.node_name, &bdr_local_node_name);

		if (node_match == false)
		{
			if (strlen(bdr_local_node_name.data))
			{
				log_error(_("local node BDR node name is \"%s\", expected: \"%s\""),
						  bdr_local_node_name.data,
						  config_file_options.node_name);
				log_hint(_("\"node_name\" in repmgr.conf must match \"node_name\" in bdr.bdr_nodes"));
			}
			else
			{
				log_error(_("local node does not report BDR node name"));
				log_hint(_("ensure this is an active BDR node"));
			}

			PQfinish(conn);
			pfree(dbname);
			termPQExpBuffer(&bdr_local_node_name);
			exit(ERR_BAD_CONFIG);
		}

		termPQExpBuffer(&bdr_local_node_name);
	}

	/* check whether repmgr extension exists, and there are no non-BDR nodes registered */
	extension_status = get_repmgr_extension_status(conn);

	if (extension_status == REPMGR_UNKNOWN)
	{
		log_error(_("unable to determine status of \"repmgr\" extension in database \"%s\""),
				  dbname);
		PQfinish(conn);
		pfree(dbname);
		exit(ERR_BAD_CONFIG);
	}

	if (extension_status == REPMGR_UNAVAILABLE)
	{
		log_error(_("\"repmgr\" extension is not available"));
		PQfinish(conn);
		pfree(dbname);
		exit(ERR_BAD_CONFIG);
	}

	if (extension_status == REPMGR_INSTALLED)
	{
		if (!is_bdr_repmgr(conn))
		{
			log_error(_("repmgr metadatabase contains records for non-BDR nodes"));
			PQfinish(conn);
			pfree(dbname);
			exit(ERR_BAD_CONFIG);
		}
	}
	else
	{
		log_debug("creating repmgr extension in database \"%s\"", dbname);

		begin_transaction(conn);

		if (!create_repmgr_extension(conn))
		{
			log_error(_("unable to create repmgr extension - see preceding error message(s); aborting"));
			rollback_transaction(conn);
			pfree(dbname);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		commit_transaction(conn);
	}

	pfree(dbname);

	if (bdr_node_has_repmgr_set(conn, config_file_options.node_name) == false)
	{
		log_debug("bdr_node_has_repmgr_set() = false");
		bdr_node_set_repmgr_set(conn, config_file_options.node_name);
	}

	/*
	 * before adding the extension tables to the replication set, if any other
	 * BDR nodes exist, populate repmgr.nodes with a copy of existing entries
	 *
	 * currently we won't copy the contents of any other tables
	 *
	 */
	{
		NodeInfoList local_node_records = T_NODE_INFO_LIST_INITIALIZER;

		(void) get_all_node_records(conn, &local_node_records);

		if (local_node_records.node_count == 0)
		{
			BdrNodeInfoList bdr_nodes = T_BDR_NODE_INFO_LIST_INITIALIZER;
			BdrNodeInfoListCell *bdr_cell = NULL;

			get_all_bdr_node_records(conn, &bdr_nodes);

			if (bdr_nodes.node_count == 0)
			{
				log_error(_("unable to retrieve any BDR node records"));
				log_detail("%s", PQerrorMessage(conn));
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}

			for (bdr_cell = bdr_nodes.head; bdr_cell; bdr_cell = bdr_cell->next)
			{
				PGconn	   *bdr_node_conn = NULL;
				NodeInfoList existing_nodes = T_NODE_INFO_LIST_INITIALIZER;
				NodeInfoListCell *cell = NULL;
				ExtensionStatus other_node_extension_status = REPMGR_UNKNOWN;

				/* skip the local node */
				if (strncmp(node_info.node_name, bdr_cell->node_info->node_name, MAXLEN) == 0)
				{
					continue;
				}

				log_debug("connecting to BDR node \"%s\" (conninfo: \"%s\")",
						  bdr_cell->node_info->node_name,
						  bdr_cell->node_info->node_local_dsn);
				bdr_node_conn = establish_db_connection_quiet(bdr_cell->node_info->node_local_dsn);

				if (PQstatus(bdr_node_conn) != CONNECTION_OK)
				{
					continue;
				}

				/* check repmgr schema exists, skip if not */
				other_node_extension_status = get_repmgr_extension_status(bdr_node_conn);

				if (other_node_extension_status != REPMGR_INSTALLED)
				{
					continue;
				}

				(void) get_all_node_records(bdr_node_conn, &existing_nodes);

				for (cell = existing_nodes.head; cell; cell = cell->next)
				{
					log_debug("creating record for node \"%s\" (ID: %i)",
							  cell->node_info->node_name, cell->node_info->node_id);
					create_node_record(conn, "bdr register", cell->node_info);
				}

				PQfinish(bdr_node_conn);
				break;
			}
		}
	}

	/* Add the repmgr extension tables to a replication set */

	if (get_bdr_version_num() < 3)
	{
		add_extension_tables_to_bdr_replication_set(conn);
	}
	else
	{
		/* this is the only table we need to replicate */
		char *replication_set = get_default_bdr_replication_set(conn);

		/*
		 * this probably won't happen, but we need to be sure we're using
		 * the replication set metadata correctly...
		 */
		if (conn == NULL)
		{
			log_error(_("unable to retrieve default BDR replication set"));
			log_hint(_("see preceding messages"));
			log_debug("check query in get_default_bdr_replication_set()");
			exit(ERR_BAD_CONFIG);
		}

		if (is_table_in_bdr_replication_set(conn, "nodes", replication_set) == false)
		{
			add_table_to_bdr_replication_set(conn, "nodes", replication_set);
		}

		pfree(replication_set);
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
		bool		node_updated = false;

		/*
		 * At this point we will have established there are no non-BDR
		 * records, so no need to verify the node type
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
		 * don't permit changing the node name - this must match the BDR node
		 * name set when the node was registered.
		 */

		if (strncmp(node_info.node_name, config_file_options.node_name, MAXLEN) != 0)
		{
			log_error(_("a record for node %i is already registered with node_name \"%s\""),
					  config_file_options.node_id, node_info.node_name);
			log_hint(_("node_name configured in repmgr.conf is \"%s\""), config_file_options.node_name);

			rollback_transaction(conn);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		node_updated = update_node_record(conn, "bdr register", &node_info);

		if (node_updated == true)
		{
			appendPQExpBuffer(&event_details, _("node record updated for node \"%s\" (%i)"),
							  config_file_options.node_name, config_file_options.node_id);
			log_verbose(LOG_NOTICE, "%s", event_details.data);
		}
		else
		{
			success = false;
		}

	}
	else
	{
		/* create new node record */
		bool		node_created = create_node_record(conn, "bdr register", &node_info);

		if (node_created == true)
		{
			appendPQExpBuffer(&event_details,
							  _("node record created for node \"%s\" (ID: %i)"),
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
	PGconn	   *conn = NULL;
	ExtensionStatus extension_status = REPMGR_UNKNOWN;
	int			target_node_id = UNKNOWN_NODE_ID;
	t_node_info node_info = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status = RECORD_NOT_FOUND;
	bool		node_record_deleted = false;
	PQExpBufferData event_details;
	char	   *dbname;

	/* sanity-check configuration for BDR-compatability */

	if (config_file_options.replication_type != REPLICATION_TYPE_BDR)
	{
		log_error(_("cannot run BDR UNREGISTER on a non-BDR node"));
		exit(ERR_BAD_CONFIG);
	}

	dbname = pg_malloc0(MAXLEN);

	if (dbname == NULL)
	{
		log_error(_("unable to allocate memory; terminating."));
		exit(ERR_OUT_OF_MEMORY);
	}

	/* store the database name for future reference */
	get_conninfo_value(config_file_options.conninfo, "dbname", dbname);

	conn = establish_db_connection(config_file_options.conninfo, true);

	if (!is_bdr_db(conn, NULL))
	{
		log_error(_("database \"%s\" is not BDR-enabled"), dbname);
		PQfinish(conn);
		pfree(dbname);
		exit(ERR_BAD_CONFIG);
	}

	extension_status = get_repmgr_extension_status(conn);
	if (extension_status != REPMGR_INSTALLED)
	{
		log_error(_("repmgr is not installed on database \"%s\""), dbname);
		PQfinish(conn);
		pfree(dbname);
		exit(ERR_BAD_CONFIG);
	}

	pfree(dbname);

	if (!is_bdr_repmgr(conn))
	{
		log_error(_("repmgr metadatabase contains records for non-BDR nodes"));
		PQfinish(conn);
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

	begin_transaction(conn);

	log_debug("unregistering node %i", target_node_id);

	node_record_deleted = delete_node_record(conn, target_node_id);

	if (node_record_deleted == false)
	{
		appendPQExpBuffer(&event_details,
						  "unable to delete node record for node \"%s\" (ID: %i)",
						  node_info.node_name,
						  target_node_id);
		rollback_transaction(conn);
	}
	else
	{
		appendPQExpBuffer(&event_details,
						  "node record deleted for node \"%s\" (ID: %i)",
						  node_info.node_name,
						  target_node_id);
		commit_transaction(conn);
	}


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


void
do_bdr_help(void)
{
	print_help_header();

	printf(_("Usage:\n"));
	printf(_("    %s [OPTIONS] bdr register\n"), progname());
	printf(_("    %s [OPTIONS] bdr unregister\n"), progname());
	puts("");

	printf(_("BDR REGISTER\n"));
	puts("");
	printf(_("  \"bdr register\" initialises the repmgr cluster and registers the initial bdr node.\n"));
	puts("");
	printf(_("  -F, --force                         overwrite an existing node record\n"));
	puts("");

	printf(_("BDR UNREGISTER\n"));
	puts("");
	printf(_("  \"bdr unregister\" unregisters an inactive BDR node.\n"));
	puts("");
	printf(_("  --node-id                           ID node to unregister (optional, used when the node to unregister\n" \
			 "                                        is offline)\n"));
	puts("");
}
