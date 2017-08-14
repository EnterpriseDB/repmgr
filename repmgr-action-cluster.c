/*
 * repmgr-action-cluster.c
 *
 * Implements cluster information actions for the repmgr command line utility
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include "repmgr.h"
#include "compat.h"
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



static int  build_cluster_matrix(t_node_matrix_rec ***matrix_rec_dest, int *name_length);
static int  build_cluster_crosscheck(t_node_status_cube ***cube_dest, int *name_length);
static void cube_set_node_status(t_node_status_cube **cube, int n, int node_id, int matrix_node_id, int connection_node_id, int connection_status);


void
do_cluster_show(void)
{
	PGconn	   *conn = NULL;
	NodeInfoList nodes = T_NODE_INFO_LIST_INITIALIZER;
	NodeInfoListCell *cell = NULL;
	int			i = 0;

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
	 *
	 * TODO: skip display of "Upstream" for BDR nodes as it will always
	 * be empty
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

	if (runtime_options.output_mode == OM_TEXT)
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
		if (runtime_options.output_mode == OM_CSV)
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
	PGconn			 *conn = NULL;
	PQExpBufferData	  query;
	PQExpBufferData	  where_clause;
	PGresult		 *res;
	int			 	  i = 0;

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
		/* print this message directly, rather than as a log line */
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


void
do_cluster_crosscheck(void)
{
	int			i = 0, n = 0;
	char		c;
	const char *node_header = "Name";
	int			name_length = strlen(node_header);

	t_node_status_cube **cube;

	n = build_cluster_crosscheck(&cube, &name_length);

	printf("%*s | Id ", name_length, node_header);
	for (i = 0; i < n; i++)
		printf("| %2d ", cube[i]->node_id);
	printf("\n");

	for (i = 0; i < name_length; i++)
		printf("-");
	printf("-+----");
	for (i = 0; i < n; i++)
		printf("+----");
	printf("\n");

	for (i = 0; i < n; i++)
	{
		int column_node_ix;

		printf("%*s | %2d ", name_length,
			   cube[i]->node_name,
			   cube[i]->node_id);

		for (column_node_ix = 0; column_node_ix < n; column_node_ix++)
		{
			int max_node_status = -2;
			int node_ix = 0;

			/*
			 * The value of entry (i,j) is equal to the
			 * maximum value of all the (i,j,k). Indeed:
			 *
			 * - if one of the (i,j,k) is 0 (node up), then 0
			 *	 (the node is up);
			 *
			 * - if the (i,j,k) are either -1 (down) or -2
			 *	 (unknown), then -1 (the node is down);
			 *
			 * - if all the (i,j,k) are -2 (unknown), then -2
			 *	 (the node is in an unknown state).
			 */

			for(node_ix = 0; node_ix < n; node_ix ++)
			{
				int node_status = cube[node_ix]->matrix_list_rec[i]->node_status_list[column_node_ix]->node_status;
				if (node_status > max_node_status)
					max_node_status = node_status;
			}

			switch (max_node_status)
			{
				case -2:
					c = '?';
					break;
				case -1:
					c = 'x';
					break;
				case 0:
					c = '*';
					break;
				default:
					exit(ERR_INTERNAL);
			}

			printf("|  %c ", c);
		}

		printf("\n");
	}
}


void
do_cluster_matrix()
{
	int			i = 0, j = 0, n = 0;

	const char *node_header = "Name";
	int			name_length = strlen(node_header);

	t_node_matrix_rec **matrix_rec_list;

	n = build_cluster_matrix(&matrix_rec_list, &name_length);

	if (runtime_options.output_mode == OM_CSV)
	{
		for (i = 0; i < n; i++)
			for (j = 0; j < n; j++)
				printf("%d,%d,%d\n",
					   matrix_rec_list[i]->node_id,
					   matrix_rec_list[i]->node_status_list[j]->node_id,
					   matrix_rec_list[i]->node_status_list[j]->node_status);
	}
	else
	{
		char c;

		printf("%*s | Id ", name_length, node_header);
		for (i = 0; i < n; i++)
			printf("| %2d ", matrix_rec_list[i]->node_id);
		printf("\n");

		for (i = 0; i < name_length; i++)
			printf("-");
		printf("-+----");
		for (i = 0; i < n; i++)
			printf("+----");
		printf("\n");

		for (i = 0; i < n; i++)
		{
			printf("%*s | %2d ", name_length,
				   matrix_rec_list[i]->node_name,
				   matrix_rec_list[i]->node_id);
			for (j = 0; j < n; j++)
			{
				switch (matrix_rec_list[i]->node_status_list[j]->node_status)
				{
				case -2:
					c = '?';
					break;
				case -1:
					c = 'x';
					break;
				case 0:
					c = '*';
					break;
				default:
					exit(ERR_INTERNAL);
				}

				printf("|  %c ", c);
			}
			printf("\n");
		}
	}
}


static void
matrix_set_node_status(t_node_matrix_rec **matrix_rec_list, int n, int node_id, int connection_node_id, int connection_status)
{
	int i, j;

	for (i = 0; i < n; i++)
	{
		if (matrix_rec_list[i]->node_id == node_id)
		{
			for (j = 0; j < n; j++)
			{
				if (matrix_rec_list[i]->node_status_list[j]->node_id == connection_node_id)
				{
					matrix_rec_list[i]->node_status_list[j]->node_status = connection_status;
					break;
				}
			}
			break;
		}
	}
}


static int
build_cluster_matrix(t_node_matrix_rec ***matrix_rec_dest, int *name_length)
{
	PGconn	    *conn = NULL;
	int			 i = 0, j = 0;
	int			 local_node_id = UNKNOWN_NODE_ID;
	NodeInfoList nodes = T_NODE_INFO_LIST_INITIALIZER;
	NodeInfoListCell *cell = NULL;

	PQExpBufferData command;
	PQExpBufferData command_output;

	t_node_matrix_rec **matrix_rec_list;

	/* obtain node list from the database */
	log_info(_("connecting to database"));

	if (strlen(config_file_options.conninfo))
	{
		conn = establish_db_connection(config_file_options.conninfo, true);
		local_node_id = config_file_options.node_id;
	}
	else
	{
		conn = establish_db_connection_by_params(&source_conninfo, true);
		local_node_id = runtime_options.node_id;
	}

	get_all_node_records(conn, &nodes);

	PQfinish(conn);

	if (nodes.node_count == 0)
	{
		log_error(_("unable to retrieve any node records"));
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Allocate an empty matrix record list
	 *
	 * -2 == NULL  ?
	 * -1 == Error x
	 *  0 == OK    *
	 */

	matrix_rec_list = (t_node_matrix_rec **) pg_malloc0(sizeof(t_node_matrix_rec) * nodes.node_count);

	i = 0;

	/* Initialise matrix structure for each node */
	for (cell = nodes.head; cell; cell = cell->next)
	{
		int name_length_cur;
		NodeInfoListCell *cell_j;

		matrix_rec_list[i] = (t_node_matrix_rec *) pg_malloc0(sizeof(t_node_matrix_rec));

		matrix_rec_list[i]->node_id = cell->node_info->node_id;
		strncpy(matrix_rec_list[i]->node_name, cell->node_info->node_name, MAXLEN);

		/*
		 * Find the maximum length of a node name
		 */
		name_length_cur	= strlen(matrix_rec_list[i]->node_name);
		if (name_length_cur > *name_length)
			*name_length = name_length_cur;

		matrix_rec_list[i]->node_status_list = (t_node_status_rec **) pg_malloc0(sizeof(t_node_status_rec) * nodes.node_count);

		j = 0;

		for (cell_j = nodes.head; cell_j; cell_j = cell_j->next)
		{
			matrix_rec_list[i]->node_status_list[j] = (t_node_status_rec *) pg_malloc0(sizeof(t_node_status_rec));
			matrix_rec_list[i]->node_status_list[j]->node_id = cell_j->node_info->node_id;
			matrix_rec_list[i]->node_status_list[j]->node_status = -2;  /* default unknown */

			j++;
		}

		i++;
	}

	/* Fetch `repmgr cluster show --csv` output for each node */
	i = 0;

	for (cell = nodes.head; cell; cell = cell->next)
	{
		int connection_status = 0;
		t_conninfo_param_list remote_conninfo;
		char *host = NULL, *p = NULL;
		int connection_node_id = cell->node_info->node_id;
		int			x, y;

		initialize_conninfo_params(&remote_conninfo, false);
		parse_conninfo_string(cell->node_info->conninfo,
							  &remote_conninfo,
							  NULL,
							  false);

		host = param_get(&remote_conninfo, "host");

		conn = establish_db_connection(cell->node_info->conninfo, false);

		connection_status =
			(PQstatus(conn) == CONNECTION_OK) ? 0 : -1;


		matrix_set_node_status(matrix_rec_list,
							   nodes.node_count,
							   local_node_id,
							   connection_node_id,
							   connection_status);


		if (connection_status)
			continue;

		/* We don't need to issue `cluster show --csv` for the local node */
		if (connection_node_id == local_node_id)
			continue;

		initPQExpBuffer(&command);

		/*
		 * We'll pass cluster name and database connection string to the remote
		 * repmgr - those are the only values it needs to work, and saves us
		 * making assumptions about the location of repmgr.conf
		 */
		appendPQExpBuffer(&command,
						  "\"%s -d '%s' ",
						  make_pg_path(progname()),
						  cell->node_info->conninfo);


		if (strlen(pg_bindir))
		{
			appendPQExpBuffer(&command,
							  "--pg_bindir=");
			appendShellString(&command,
							  pg_bindir);
			appendPQExpBuffer(&command,
							  " ");
		}

		appendPQExpBuffer(&command,
						  " cluster show --csv\"");

		log_verbose(LOG_DEBUG, "build_cluster_matrix(): executing:\n  %s", command.data);

		initPQExpBuffer(&command_output);

		(void)remote_command(
			host,
			runtime_options.remote_user,
			command.data,
			&command_output);

		p = command_output.data;

		termPQExpBuffer(&command);

		for (j = 0; j < nodes.node_count; j++)
		{
			if (sscanf(p, "%d,%d", &x, &y) != 2)
			{
				fprintf(stderr, _("cannot parse --csv output: %s\n"), p);
				PQfinish(conn);
				exit(ERR_INTERNAL);
			}

			matrix_set_node_status(matrix_rec_list,
								   nodes.node_count,
								   connection_node_id,
								   x,
								   (y == -1) ? -1 : 0 );

			while (*p && (*p != '\n'))
				p++;
			if (*p == '\n')
				p++;
		}

		PQfinish(conn);
	}

	*matrix_rec_dest = matrix_rec_list;

	return nodes.node_count;
}


static int
build_cluster_crosscheck(t_node_status_cube ***dest_cube, int *name_length)
{
	PGconn	   *conn = NULL;
	int			h, i, j;
	NodeInfoList nodes = T_NODE_INFO_LIST_INITIALIZER;
	NodeInfoListCell *cell = NULL;

	t_node_status_cube **cube;

	/* We need to connect to get the list of nodes */
	log_info(_("connecting to database\n"));

	if (strlen(config_file_options.conninfo))
		conn = establish_db_connection(config_file_options.conninfo, true);
	else
		conn = establish_db_connection_by_params(&source_conninfo, true);

	get_all_node_records(conn, &nodes);

	PQfinish(conn);

	if (nodes.node_count == 0)
	{
		log_error(_("unable to retrieve any node records"));
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Allocate an empty cube matrix structure
	 *
	 * -2 == NULL
	 * -1 == Error
	 *	0 == OK
	 */

	cube = (t_node_status_cube **) pg_malloc(sizeof(t_node_status_cube *) * nodes.node_count);

	h = 0;

	for (cell = nodes.head; cell; cell = cell->next)
	{
		int name_length_cur = 0;
		NodeInfoListCell *cell_i = NULL;

		cube[h] = (t_node_status_cube *) pg_malloc(sizeof(t_node_status_cube));
		cube[h]->node_id = cell->node_info->node_id;
		strncpy(cube[h]->node_name, cell->node_info->node_name, MAXLEN);

		/*
		 * Find the maximum length of a node name
		 */
		name_length_cur	= strlen(cube[h]->node_name);
		if (name_length_cur > *name_length)
			*name_length = name_length_cur;

		cube[h]->matrix_list_rec = (t_node_matrix_rec **) pg_malloc(sizeof(t_node_matrix_rec) * nodes.node_count);

		i = 0;
		for (cell_i = nodes.head; cell_i; cell_i = cell_i->next)
		{
			NodeInfoListCell *cell_j;

			cube[h]->matrix_list_rec[i] = (t_node_matrix_rec *) pg_malloc0(sizeof(t_node_matrix_rec));
			cube[h]->matrix_list_rec[i]->node_id = cell_i->node_info->node_id;

			/* we don't need the name here */
			cube[h]->matrix_list_rec[i]->node_name[0] = '\0';

			cube[h]->matrix_list_rec[i]->node_status_list = (t_node_status_rec **) pg_malloc0(sizeof(t_node_status_rec) * nodes.node_count);

			j = 0;

			for (cell_j = nodes.head; cell_j; cell_j = cell_j->next)
			{
				cube[h]->matrix_list_rec[i]->node_status_list[j] = (t_node_status_rec *) pg_malloc0(sizeof(t_node_status_rec));
				cube[h]->matrix_list_rec[i]->node_status_list[j]->node_id = cell_j->node_info->node_id;
				cube[h]->matrix_list_rec[i]->node_status_list[j]->node_status = -2;  /* default unknown */

				j++;
			}

			i++;
		}

		h++;
	}


	/*
	 * Build the connection cube
	 */
	i = 0;

	for (cell = nodes.head; cell; cell = cell->next)
	{
		int remote_node_id = UNKNOWN_NODE_ID;
		PQExpBufferData command;
		PQExpBufferData command_output;

		char	   *p = NULL;

		remote_node_id = cell->node_info->node_id;

		initPQExpBuffer(&command);

		appendPQExpBuffer(&command,
						  "%s -d '%s' --node-id=%i ",
						  make_pg_path(progname()),
						  cell->node_info->conninfo,
						  remote_node_id);

		if (strlen(pg_bindir))
		{
			appendPQExpBuffer(&command,
							  "--pg_bindir=");
			appendShellString(&command,
							  pg_bindir);
			appendPQExpBuffer(&command,
							  " ");
		}

		appendPQExpBuffer(&command,
						  "cluster matrix --csv 2>/dev/null");

		initPQExpBuffer(&command_output);

		/* fix to work with --node-id */
		if (cube[i]->node_id == config_file_options.node_id)
		{
			(void)local_command(
				command.data,
				&command_output);
		}
		else
		{
			t_conninfo_param_list remote_conninfo;
			char *host = NULL;
			PQExpBufferData quoted_command;

			initPQExpBuffer(&quoted_command);
			appendPQExpBuffer(&quoted_command,
							  "\"%s\"",
							  command.data);

			initialize_conninfo_params(&remote_conninfo, false);
			parse_conninfo_string(cell->node_info->conninfo,
								  &remote_conninfo,
								  NULL,
								  false);

			host = param_get(&remote_conninfo, "host");

			log_verbose(LOG_DEBUG, "build_cluster_crosscheck(): executing\n  %s", quoted_command.data);

			(void)remote_command(
				host,
				runtime_options.remote_user,
				quoted_command.data,
				&command_output);

			termPQExpBuffer(&quoted_command);
		}

		p = command_output.data;

		if(!strlen(command_output.data))
		{
			continue;
		}

		for (j = 0; j < (nodes.node_count * nodes.node_count); j++)
		{
			int matrix_rec_node_id;
			int node_status_node_id;
			int node_status;

			if (sscanf(p, "%d,%d,%d", &matrix_rec_node_id, &node_status_node_id, &node_status) != 3)
			{
				fprintf(stderr, _("cannot parse --csv output: %s\n"), p);
				exit(ERR_INTERNAL);
			}

			cube_set_node_status(cube,
								 nodes.node_count,
								 remote_node_id,
								 matrix_rec_node_id,
								 node_status_node_id,
								 node_status);

			while (*p && (*p != '\n'))
				p++;
			if (*p == '\n')
				p++;
		}

		i++;
	}

	*dest_cube = cube;
	return nodes.node_count;
}


static void
cube_set_node_status(t_node_status_cube **cube, int n, int execute_node_id, int matrix_node_id, int connection_node_id, int connection_status)
{
	int h, i, j;


	for (h = 0; h < n; h++)
	{
		if (cube[h]->node_id == execute_node_id)
		{
			for (i = 0; i < n; i++)
			{
				if (cube[h]->matrix_list_rec[i]->node_id == matrix_node_id)
				{
					for (j = 0; j < n; j++)
					{
						if (cube[h]->matrix_list_rec[i]->node_status_list[j]->node_id == connection_node_id)
						{
							cube[h]->matrix_list_rec[i]->node_status_list[j]->node_status = connection_status;
							break;
						}
					}
					break;
				}
			}
		}
	}
}
