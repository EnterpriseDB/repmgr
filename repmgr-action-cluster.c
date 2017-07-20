/*
 * repmgr-action-cluster.c
 *
 * Implements cluster information actions for the repmgr command line utility
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include "repmgr.h"

#include "repmgr-client-global.h"
#include "repmgr-action-cluster.h"

#define SHOW_HEADER_COUNT 6


typedef enum {
	SHOW_ID = 0,
	SHOW_NAME,
	SHOW_ROLE,
	SHOW_STATUS,
	SHOW_UPSTREAM_NAME,
	SHOW_CONNINFO
} ShowHeader;

#define EVENT_HEADER_COUNT 5

typedef enum {
	EV_NODE_ID = 0,
	EV_EVENT,
	EV_SUCCESS,
	EV_TIMESTAMP,
	EV_DETAILS
} EventHeader;



struct ColHeader {
	char  title[MAXLEN];
	int   max_length;
	int   cur_length;
};

struct ColHeader headers_show[SHOW_HEADER_COUNT];
struct ColHeader headers_event[EVENT_HEADER_COUNT];

void
do_cluster_show(void)
{
	PGconn	   *conn;
	NodeInfoList nodes = T_NODE_INFO_LIST_INITIALIZER;
	NodeInfoListCell *cell;
	int			i;

	/* Connect to local database to obtain cluster connection data */
	log_verbose(LOG_INFO, _("connecting to database\n"));

	if (strlen(config_file_options.conninfo))
		conn = establish_db_connection(config_file_options.conninfo, true);
	else
		conn = establish_db_connection_by_params(&source_conninfo, true);

	get_all_node_records_with_upstream(conn, &nodes);

	if (nodes.node_count == 0)
	{
		log_error(_("unable to retrieve any node records"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	strncpy(headers_show[SHOW_ID].title, _("ID"), MAXLEN);
	strncpy(headers_show[SHOW_NAME].title, _("Name"), MAXLEN);
	strncpy(headers_show[SHOW_ROLE].title, _("Role"), MAXLEN);
	strncpy(headers_show[SHOW_STATUS].title, _("Status"), MAXLEN);
	strncpy(headers_show[SHOW_UPSTREAM_NAME].title, _("Upstream"), MAXLEN);
	strncpy(headers_show[SHOW_CONNINFO].title, _("Connection string"), MAXLEN);

	/*
	 * XXX if repmgr is ever localized into non-ASCII locales,
	 * use pg_wcssize() or similar to establish printed column length
	 */

	for (i = 0; i < SHOW_HEADER_COUNT; i++)
	{
		headers_show[i].max_length = strlen(headers_show[i].title);
	}

	for (cell = nodes.head; cell; cell = cell->next)
	{
		PQExpBufferData details;

		cell->node_info->conn = establish_db_connection_quiet(cell->node_info->conninfo);

		if (PQstatus(cell->node_info->conn) == CONNECTION_OK)
		{
			cell->node_info->node_status = NODE_STATUS_UP;

			cell->node_info->recovery_type = get_recovery_type(cell->node_info->conn);
		}
		else
		{
			cell->node_info->node_status = NODE_STATUS_DOWN;
			cell->node_info->recovery_type = RECTYPE_UNKNOWN;
		}

		initPQExpBuffer(&details);

		/*
		 * TODO: count nodes marked as "? unreachable" and add a hint about
		 * the other cluster commands for better determining whether unreachable.
		 */
		switch (cell->node_info->type)
		{
			case PRIMARY:
			{
				/* node is reachable */
				if (cell->node_info->node_status == NODE_STATUS_UP)
				{
					if (cell->node_info->active == true)
					{
						switch (cell->node_info->recovery_type)
						{
							case RECTYPE_PRIMARY:
								appendPQExpBuffer(&details, "* running");
								break;
							case RECTYPE_STANDBY:
								appendPQExpBuffer(&details, "! running as standby");
								break;
							case RECTYPE_UNKNOWN:
								appendPQExpBuffer(&details, "! unknown");
								break;
						}
					}
					else
					{
						if (cell->node_info->recovery_type == RECTYPE_PRIMARY)
							appendPQExpBuffer(&details, "! running");
						else
							appendPQExpBuffer(&details, "! running as standby");
					}
				}
				/* node is unreachable */
				else
				{
					/* node is unreachable but marked active*/
					if (cell->node_info->active == true)
						appendPQExpBuffer(&details, "? unreachable");
					/* node is unreachable and marked as inactive */
					else
						appendPQExpBuffer(&details, "- failed");
				}
			}
			break;
			case STANDBY:
			{
				/* node is reachable */
				if (cell->node_info->node_status == NODE_STATUS_UP)
				{
					if (cell->node_info->active == true)
					{
						switch (cell->node_info->recovery_type)
						{
							case RECTYPE_STANDBY:
								appendPQExpBuffer(&details, "  running");
								break;
							case RECTYPE_PRIMARY:
								appendPQExpBuffer(&details, "! running as primary");
								break;
							case RECTYPE_UNKNOWN:
								appendPQExpBuffer(&details, "! unknown");
								break;
						}
					}
					else
					{
						if (cell->node_info->recovery_type == RECTYPE_STANDBY)
							appendPQExpBuffer(&details, "! running");
						else
							appendPQExpBuffer(&details, "! running as primary");
					}
				}
				/* node is unreachable */
				else
				{
					/* node is unreachable but marked active*/
					if (cell->node_info->active == true)
						appendPQExpBuffer(&details, "? unreachable");
					else
						appendPQExpBuffer(&details, "- failed");
				}
			}
			break;
			case BDR:
			{
				/* node is reachable */
				if (cell->node_info->node_status == NODE_STATUS_UP)
				{
					if (cell->node_info->active == true)
						appendPQExpBuffer(&details, "* running");
					else
						appendPQExpBuffer(&details, "! running");
				}
				/* node is unreachable */
				else
				{
					if (cell->node_info->active == true)
						appendPQExpBuffer(&details, "? unreachable");
					else
						appendPQExpBuffer(&details, "- failed");
				}
			}
			break;
			case UNKNOWN:
			{
				/* this should never happen */
				appendPQExpBuffer(&details, "? unknown node type");
			}
			break;
		}

		strncpy(cell->node_info->details, details.data, MAXLEN);
		termPQExpBuffer(&details);

		PQfinish(cell->node_info->conn);

		headers_show[SHOW_ROLE].cur_length = strlen(get_node_type_string(cell->node_info->type));
		headers_show[SHOW_NAME].cur_length = strlen(cell->node_info->node_name);
		headers_show[SHOW_STATUS].cur_length = strlen(cell->node_info->details);
		headers_show[SHOW_UPSTREAM_NAME].cur_length = strlen(cell->node_info->upstream_node_name);
		headers_show[SHOW_CONNINFO].cur_length = strlen(cell->node_info->conninfo);

		for (i = 0; i < SHOW_HEADER_COUNT; i++)
		{
			if (headers_show[i].cur_length > headers_show[i].max_length)
			{
				headers_show[i].max_length = headers_show[i].cur_length;
			}
		}

	}

	if (! runtime_options.csv)
	{
		for (i = 0; i < SHOW_HEADER_COUNT; i++)
		{
			if (i == 0)
				printf(" ");
			else
				printf(" | ");

			printf("%-*s",
				   headers_show[i].max_length,
				   headers_show[i].title);
		}
		printf("\n");
		printf("-");

		for (i = 0; i < SHOW_HEADER_COUNT; i++)
		{
			int j;
			for (j = 0; j < headers_show[i].max_length; j++)
				printf("-");

			if (i < (SHOW_HEADER_COUNT - 1))
				printf("-+-");
			else
				printf("-");
		}

		printf("\n");
	}

	for (cell = nodes.head; cell; cell = cell->next)
	{
		if (runtime_options.csv)
		{
			int connection_status =	(PQstatus(conn) == CONNECTION_OK) ? 0 : -1;
			int recovery_type = RECTYPE_UNKNOWN;

			/*
			 * here we explicitly convert the RecoveryType to integer values to
			 * avoid implicit dependency on the values in the enum
			 */
			switch (cell->node_info->recovery_type)
			{
				case RECTYPE_UNKNOWN:
					recovery_type = -1;
					break;
				case RECTYPE_PRIMARY:
					recovery_type = 0;
					break;
				case RECTYPE_STANDBY:
					recovery_type = 1;
					break;
			}

			printf("%i,%i,%i\n",
				   cell->node_info->node_id,
				   connection_status,
				   recovery_type);
		}
		else
		{
			printf( " %-*i ",  headers_show[SHOW_ID].max_length, cell->node_info->node_id);
			printf("| %-*s ",  headers_show[SHOW_NAME].max_length, cell->node_info->node_name);
			printf("| %-*s ",  headers_show[SHOW_ROLE].max_length, get_node_type_string(cell->node_info->type));
			printf("| %-*s ",  headers_show[SHOW_STATUS].max_length, cell->node_info->details);
			printf("| %-*s ",  headers_show[SHOW_UPSTREAM_NAME].max_length , cell->node_info->upstream_node_name);
			printf("| %-*s\n", headers_show[SHOW_CONNINFO].max_length, cell->node_info->conninfo);
		}
	}

	PQfinish(conn);
}


/*
 * CLUSTER EVENT
 *
 * Parameters:
 *   --limit[=20]
 *   --all
 *   --node_[id|name]
 *   --event
 */

void
do_cluster_event(void)
{
	PGconn			 *conn;
	PQExpBufferData	  query;
	PQExpBufferData	  where_clause;
	PGresult		 *res;
	int			 	  i;

	conn = establish_db_connection(config_file_options.conninfo, true);

	initPQExpBuffer(&query);
	initPQExpBuffer(&where_clause);

	appendPQExpBuffer(&query,
					  " SELECT node_id, event, successful, \n"
					  "        TO_CHAR(event_timestamp, 'YYYY-MM-DD HH24:MI:SS') AS timestamp, \n"
					  "        details \n"
					  "   FROM repmgr.events");

	if (runtime_options.node_id != UNKNOWN_NODE_ID)
	{
		append_where_clause(&where_clause,
							"node_id=%i", runtime_options.node_id);
	}

	if (runtime_options.event[0] != '\0')
	{
		char *escaped = escape_string(conn, runtime_options.event);

		if (escaped == NULL)
		{
			log_error(_("unable to escape value provided for event"));
		}
		else
		{
			append_where_clause(&where_clause,
								"event='%s'",
								escaped);
			pfree(escaped);
		}
	}

	appendPQExpBuffer(&query, "\n%s\n",
					  where_clause.data);

	appendPQExpBuffer(&query,
					  " ORDER BY event_timestamp DESC");

	if (runtime_options.all == false && runtime_options.limit > 0)
	{
		appendPQExpBuffer(&query, " LIMIT %i",
						  runtime_options.limit);
	}

	log_debug("do_cluster_event():\n%s", query.data);
	res = PQexec(conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to execute event query:\n  %s"),
				  PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		exit(ERR_DB_QUERY);
	}

	if (PQntuples(res) == 0) {
		printf(_("no matching events found\n"));
		PQclear(res);
		PQfinish(conn);
		return;
	}

	strncpy(headers_event[EV_NODE_ID].title, _("Node ID"), MAXLEN);
	strncpy(headers_event[EV_EVENT].title, _("Event"), MAXLEN);
	strncpy(headers_event[EV_SUCCESS].title, _("OK"), MAXLEN);
	strncpy(headers_event[EV_TIMESTAMP].title, _("Timestamp"), MAXLEN);
	strncpy(headers_event[EV_DETAILS].title, _("Details"), MAXLEN);

	for (i = 0; i < EVENT_HEADER_COUNT; i++)
	{
		headers_event[i].max_length = strlen(headers_event[i].title);
	}

	for(i = 0; i < PQntuples(res); i++)
	{
		int j;

		for (j = 0; j < EVENT_HEADER_COUNT; j++)
		{
			headers_event[j].cur_length = strlen(PQgetvalue(res, i, j));
			if(headers_event[j].cur_length > headers_event[j].max_length)
			{
				headers_event[j].max_length = headers_event[j].cur_length;
			}
		}

	}

	for (i = 0; i < EVENT_HEADER_COUNT; i++)
	{
		if (i == 0)
			printf(" ");
		else
			printf(" | ");

		printf("%-*s",
			   headers_event[i].max_length,
			   headers_event[i].title);
	}
	printf("\n");
	printf("-");
	for (i = 0; i < EVENT_HEADER_COUNT; i++)
	{
		int j;
		for (j = 0; j < headers_event[i].max_length; j++)
			printf("-");

		if (i < (EVENT_HEADER_COUNT - 1))
			printf("-+-");
		else
			printf("-");
	}

	printf("\n");

	for(i = 0; i < PQntuples(res); i++)
	{
		int j;

		printf(" ");
		for (j = 0; j < EVENT_HEADER_COUNT; j++)
		{
			printf("%-*s",
				   headers_event[j].max_length,
				   PQgetvalue(res, i, j));

			if (j < (EVENT_HEADER_COUNT - 1))
			printf(" | ");
		}

		printf("\n");
	}

	PQclear(res);

	PQfinish(conn);
}
