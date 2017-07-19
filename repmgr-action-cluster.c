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

#define SHOW_HEADER_COUNT 4

#define ROLE_HEADER 0
#define NAME_HEADER 1
#define UPSTREAM_NAME_HEADER 2
#define CONNINFO_HEADER 3

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

	strncpy(headers_show[ROLE_HEADER].title, _("Role"), MAXLEN);
	strncpy(headers_show[NAME_HEADER].title, _("Name"), MAXLEN);
	strncpy(headers_show[UPSTREAM_NAME_HEADER].title, _("Upstream"), MAXLEN);
	strncpy(headers_show[CONNINFO_HEADER].title, _("Connection string"), MAXLEN);

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
		cell->node_info->conn = establish_db_connection_quiet(cell->node_info->conninfo);

		if (PQstatus(conn) != CONNECTION_OK)
		{
			strcpy(cell->node_info->details, "   FAILED");
		}
		else if (cell->node_info->type == BDR)
		{
			strcpy(cell->node_info->details, "      BDR");
		}
		else
		{
			RecoveryType rec_type = get_recovery_type(cell->node_info->conn);
			switch (rec_type)
			{
				case RECTYPE_PRIMARY:
					strcpy(cell->node_info->details, "* primary");
					break;
				case RECTYPE_STANDBY:
					strcpy(cell->node_info->details, "  standby");
					break;
				case RECTYPE_UNKNOWN:
					strcpy(cell->node_info->details, "  unknown");
					break;
			}
		}

		PQfinish(cell->node_info->conn);

		headers_show[ROLE_HEADER].cur_length = strlen(cell->node_info->details);
		headers_show[NAME_HEADER].cur_length = strlen(cell->node_info->node_name);
		headers_show[UPSTREAM_NAME_HEADER].cur_length = strlen(cell->node_info->upstream_node_name);
		headers_show[CONNINFO_HEADER].cur_length = strlen(cell->node_info->conninfo);

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

			printf("%i,%d\n", cell->node_info->node_id, connection_status);
		}
		else
		{
			printf( " %-*s ",  headers_show[ROLE_HEADER].max_length, cell->node_info->details);
			printf("| %-*s ",  headers_show[NAME_HEADER].max_length, cell->node_info->node_name);
			printf("| %-*s ",  headers_show[UPSTREAM_NAME_HEADER].max_length , cell->node_info->upstream_node_name);
			printf("| %-*s\n", headers_show[CONNINFO_HEADER].max_length, cell->node_info->conninfo);
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
