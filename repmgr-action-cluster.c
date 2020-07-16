/*
 * repmgr-action-cluster.c
 *
 * Implements cluster information actions for the repmgr command line utility
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
#include "compat.h"
#include "repmgr-client-global.h"
#include "repmgr-action-cluster.h"

#define SHOW_HEADER_COUNT 9

typedef enum
{
	SHOW_ID = 0,
	SHOW_NAME,
	SHOW_ROLE,
	SHOW_STATUS,
	SHOW_UPSTREAM_NAME,
	SHOW_LOCATION,
	SHOW_PRIORITY,
	SHOW_TIMELINE_ID,
	SHOW_CONNINFO
}			ShowHeader;

#define EVENT_HEADER_COUNT 6

typedef enum
{
	EV_NODE_ID = 0,
	EV_NODE_NAME,
	EV_EVENT,
	EV_SUCCESS,
	EV_TIMESTAMP,
	EV_DETAILS
}			EventHeader;


struct ColHeader headers_show[SHOW_HEADER_COUNT];
struct ColHeader headers_event[EVENT_HEADER_COUNT];

static int	build_cluster_matrix(t_node_matrix_rec ***matrix_rec_dest, ItemList *warnings, int *error_code);
static int	build_cluster_crosscheck(t_node_status_cube ***cube_dest, ItemList *warnings, int *error_code);
static void cube_set_node_status(t_node_status_cube **cube, int n, int node_id, int matrix_node_id, int connection_node_id, int connection_status);

/*
 * CLUSTER SHOW
 *
 * Parameters:
 *   --compact
 *   --csv
 *   --terse
 *   --verbose
 */
void
do_cluster_show(void)
{
	PGconn	   *conn = NULL;
	NodeInfoList nodes = T_NODE_INFO_LIST_INITIALIZER;
	NodeInfoListCell *cell = NULL;
	int			i = 0;
	ItemList	warnings = {NULL, NULL};
	bool		success = false;
	bool		error_found = false;
	bool		connection_error_found = false;

	/* Connect to local database to obtain cluster connection data */
	log_verbose(LOG_INFO, _("connecting to database"));

	if (strlen(config_file_options.conninfo))
		conn = establish_db_connection(config_file_options.conninfo, true);
	else
		conn = establish_db_connection_by_params(&source_conninfo, true);

	success = get_all_node_records_with_upstream(conn, &nodes);

	if (success == false)
	{
		/* get_all_node_records_with_upstream() will print error message */
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	if (nodes.node_count == 0)
	{
		log_error(_("no node records were found"));
		log_hint(_("ensure at least one node is registered"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Initialize column headers  */
	strncpy(headers_show[SHOW_ID].title, _("ID"), MAXLEN);
	strncpy(headers_show[SHOW_NAME].title, _("Name"), MAXLEN);
	strncpy(headers_show[SHOW_ROLE].title, _("Role"), MAXLEN);
	strncpy(headers_show[SHOW_STATUS].title, _("Status"), MAXLEN);
	strncpy(headers_show[SHOW_UPSTREAM_NAME].title, _("Upstream"), MAXLEN);
	strncpy(headers_show[SHOW_LOCATION].title, _("Location"), MAXLEN);

	if (runtime_options.compact == true)
	{
		strncpy(headers_show[SHOW_PRIORITY].title, _("Prio."), MAXLEN);
		strncpy(headers_show[SHOW_TIMELINE_ID].title, _("TLI"), MAXLEN);
	}
	else
	{
		strncpy(headers_show[SHOW_PRIORITY].title, _("Priority"), MAXLEN);
		strncpy(headers_show[SHOW_TIMELINE_ID].title, _("Timeline"), MAXLEN);
	}

	strncpy(headers_show[SHOW_CONNINFO].title, _("Connection string"), MAXLEN);

	/*
	 * NOTE: if repmgr is ever localized into non-ASCII locales, use
	 * pg_wcssize() or similar to establish printed column length
	 */

	for (i = 0; i < SHOW_HEADER_COUNT; i++)
	{
		headers_show[i].display = true;

		/* Don't display timeline on pre-9.6 clusters  */
		if (i == SHOW_TIMELINE_ID)
		{
			if (PQserverVersion(conn) < 90600)
			{
				headers_show[i].display = false;
			}
		}

		/* if --compact provided, don't display conninfo */
		if (runtime_options.compact == true)
		{
			if (i == SHOW_CONNINFO)
			{
				headers_show[i].display = false;
			}
		}


		if (headers_show[i].display == true)
		{
			headers_show[i].max_length = strlen(headers_show[i].title);
		}
	}

	/*
	 * TODO: count nodes marked as "? unreachable" and add a hint about
	 * the other cluster commands for better determining whether
	 * unreachable.
	 */

	for (cell = nodes.head; cell; cell = cell->next)
	{
		PQExpBufferData node_status;
		PQExpBufferData upstream;
		PQExpBufferData buf;

		cell->node_info->replication_info = palloc0(sizeof(ReplInfo));
		if (cell->node_info->replication_info == NULL)
		{
			log_error(_("unable to allocate memory"));
			exit(ERR_INTERNAL);
		}

		init_replication_info(cell->node_info->replication_info);

		cell->node_info->conn = establish_db_connection_quiet(cell->node_info->conninfo);

		if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
		{
			connection_error_found = true;

			if (runtime_options.verbose)
			{
				char		error[MAXLEN];

				strncpy(error, PQerrorMessage(cell->node_info->conn), MAXLEN);
				item_list_append_format(&warnings,
										"when attempting to connect to node \"%s\" (ID: %i), following error encountered :\n\"%s\"",
										cell->node_info->node_name, cell->node_info->node_id, trim(error));
			}
			else
			{
				item_list_append_format(&warnings,
										"unable to connect to node \"%s\" (ID: %i)",
										cell->node_info->node_name, cell->node_info->node_id);
			}
		}
		else
		{
			/* NOP on pre-9.6 servers */
			cell->node_info->replication_info->timeline_id = get_node_timeline(cell->node_info->conn,
																			   cell->node_info->replication_info->timeline_id_str);
		}

		initPQExpBuffer(&node_status);
		initPQExpBuffer(&upstream);

		if (format_node_status(cell->node_info, &node_status, &upstream, &warnings) == true)
			error_found = true;

		snprintf(cell->node_info->details, sizeof(cell->node_info->details),
				 "%s", node_status.data);
		snprintf(cell->node_info->upstream_node_name, sizeof(cell->node_info->upstream_node_name),
				 "%s", upstream.data);

		termPQExpBuffer(&node_status);
		termPQExpBuffer(&upstream);

		PQfinish(cell->node_info->conn);
		cell->node_info->conn = NULL;

		initPQExpBuffer(&buf);
		appendPQExpBuffer(&buf, "%i", cell->node_info->node_id);
		headers_show[SHOW_ID].cur_length = strlen(buf.data);
		termPQExpBuffer(&buf);

		headers_show[SHOW_ROLE].cur_length = strlen(get_node_type_string(cell->node_info->type));
		headers_show[SHOW_NAME].cur_length = strlen(cell->node_info->node_name);
		headers_show[SHOW_STATUS].cur_length = strlen(cell->node_info->details);

		headers_show[SHOW_UPSTREAM_NAME].cur_length = strlen(cell->node_info->upstream_node_name);

		initPQExpBuffer(&buf);
		appendPQExpBuffer(&buf, "%i", cell->node_info->priority);
		headers_show[SHOW_PRIORITY].cur_length = strlen(buf.data);
		termPQExpBuffer(&buf);

		headers_show[SHOW_LOCATION].cur_length = strlen(cell->node_info->location);

		/* Format timeline ID */
		if (cell->node_info->type == WITNESS)
		{
			/* The witness node's timeline ID is irrelevant */
			strncpy(cell->node_info->replication_info->timeline_id_str, _("n/a"), MAXLEN);
		}
		headers_show[SHOW_TIMELINE_ID].cur_length = strlen(cell->node_info->replication_info->timeline_id_str);

		headers_show[SHOW_CONNINFO].cur_length = strlen(cell->node_info->conninfo);

		for (i = 0; i < SHOW_HEADER_COUNT; i++)
		{
			if (runtime_options.compact == true)
			{
				if (headers_show[i].display == false)
					continue;
			}

			if (headers_show[i].cur_length > headers_show[i].max_length)
			{
				headers_show[i].max_length = headers_show[i].cur_length;
			}
		}

	}

	/* Print column header row (text mode only) */
	if (runtime_options.output_mode == OM_TEXT)
	{
		print_status_header(SHOW_HEADER_COUNT, headers_show);
	}

	for (cell = nodes.head; cell; cell = cell->next)
	{
		if (runtime_options.output_mode == OM_CSV)
		{
			int			connection_status = (cell->node_info->node_status == NODE_STATUS_UP) ? 0 : -1;
			int			recovery_type = RECTYPE_UNKNOWN;

			/*
			 * here we explicitly convert the RecoveryType to integer values
			 * to avoid implicit dependency on the values in the enum
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
			printf(" %-*i ", headers_show[SHOW_ID].max_length, cell->node_info->node_id);
			printf("| %-*s ", headers_show[SHOW_NAME].max_length, cell->node_info->node_name);
			printf("| %-*s ", headers_show[SHOW_ROLE].max_length, get_node_type_string(cell->node_info->type));
			printf("| %-*s ", headers_show[SHOW_STATUS].max_length, cell->node_info->details);
			printf("| %-*s ", headers_show[SHOW_UPSTREAM_NAME].max_length, cell->node_info->upstream_node_name);
			printf("| %-*s ", headers_show[SHOW_LOCATION].max_length, cell->node_info->location);
			printf("| %-*i ", headers_show[SHOW_PRIORITY].max_length, cell->node_info->priority);

			if (headers_show[SHOW_TIMELINE_ID].display == true)
			{
				printf("| %-*s ", headers_show[SHOW_TIMELINE_ID].max_length, cell->node_info->replication_info->timeline_id_str);
			}

			if (headers_show[SHOW_CONNINFO].display == true)
			{
				printf("| %-*s", headers_show[SHOW_CONNINFO].max_length, cell->node_info->conninfo);
			}

			puts("");
		}
	}

	clear_node_info_list(&nodes);
	PQfinish(conn);

	/* emit any warnings */
	if (warnings.head != NULL && runtime_options.terse == false && runtime_options.output_mode != OM_CSV)
	{

		ItemListCell *cell = NULL;
		PQExpBufferData warning;

		initPQExpBuffer(&warning);

		appendPQExpBufferStr(&warning,
							 _("following issues were detected\n"));
		for (cell = warnings.head; cell; cell = cell->next)
		{
			appendPQExpBuffer(&warning,
							  _("  - %s\n"), cell->string);
		}

		puts("");
		log_warning("%s", warning.data);

		termPQExpBuffer(&warning);

		if (runtime_options.verbose == false && connection_error_found == true)
		{
			log_hint(_("execute with --verbose option to see connection error messages"));
		}
	}

	/*
	 * If warnings were noted, even if they're not displayed (e.g. in --csv node),
	 * that means something's not right so we need to emit a non-zero exit code.
	 */
	if (warnings.head != NULL)
	{
		error_found = true;
	}

	if (error_found == true)
	{
		exit(ERR_NODE_STATUS);
	}
}


/*
 * CLUSTER EVENT
 *
 * Parameters:
 *   --limit[=20]
 *   --all
 *   --node-[id|name]
 *   --event
 *   --csv
 *   --compact
 */

void
do_cluster_event(void)
{
	PGconn	   *conn = NULL;
	PGresult   *res;
	int			i = 0;
	int			column_count = EVENT_HEADER_COUNT;

	conn = establish_db_connection(config_file_options.conninfo, true);

	res = get_event_records(conn,
							runtime_options.node_id,
							runtime_options.node_name,
							runtime_options.event,
							runtime_options.all,
							runtime_options.limit);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to execute event query:\n  %s"),
				  PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		exit(ERR_DB_QUERY);
	}

	if (PQntuples(res) == 0)
	{
		/* print this message directly, rather than as a log line */
		printf(_("no matching events found\n"));
		PQclear(res);
		PQfinish(conn);
		return;
	}

	strncpy(headers_event[EV_NODE_ID].title, _("Node ID"), MAXLEN);
	strncpy(headers_event[EV_NODE_NAME].title, _("Name"), MAXLEN);
	strncpy(headers_event[EV_EVENT].title, _("Event"), MAXLEN);
	strncpy(headers_event[EV_SUCCESS].title, _("OK"), MAXLEN);
	strncpy(headers_event[EV_TIMESTAMP].title, _("Timestamp"), MAXLEN);
	strncpy(headers_event[EV_DETAILS].title, _("Details"), MAXLEN);

	/*
	 * If --compact or --csv provided, simply omit the "Details" column.
	 * In --csv mode we'd need to quote/escape the contents "Details" column,
	 * which is doable but which will remain a TODO for now.
	 */
	if (runtime_options.compact == true || runtime_options.output_mode == OM_CSV)
		column_count --;

	for (i = 0; i < column_count; i++)
	{
		headers_event[i].max_length = strlen(headers_event[i].title);
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		int			j;

		for (j = 0; j < column_count; j++)
		{
			headers_event[j].cur_length = strlen(PQgetvalue(res, i, j));
			if (headers_event[j].cur_length > headers_event[j].max_length)
			{
				headers_event[j].max_length = headers_event[j].cur_length;
			}
		}

	}

	if (runtime_options.output_mode == OM_TEXT)
	{
		for (i = 0; i < column_count; i++)
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
		for (i = 0; i < column_count; i++)
		{
			int			j;

			for (j = 0; j < headers_event[i].max_length; j++)
				printf("-");

			if (i < (column_count - 1))
				printf("-+-");
			else
				printf("-");
		}

		printf("\n");
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		int			j;

		if (runtime_options.output_mode == OM_CSV)
		{
			for (j = 0; j < column_count; j++)
			{
				printf("%s", PQgetvalue(res, i, j));
				if ((j + 1) < column_count)
				{
					printf(",");
				}
			}
		}
		else
		{
			printf(" ");
			for (j = 0; j < column_count; j++)
			{
				printf("%-*s",
					   headers_event[j].max_length,
					   PQgetvalue(res, i, j));

				if (j < (column_count - 1))
					printf(" | ");
			}
		}

		printf("\n");
	}

	PQclear(res);

	PQfinish(conn);

	if (runtime_options.output_mode == OM_TEXT)
		puts("");
}


void
do_cluster_crosscheck(void)
{
	int			i = 0,
				n = 0;

	t_node_status_cube **cube;

	bool		connection_error_found = false;
	int			error_code = SUCCESS;
	ItemList	warnings = {NULL, NULL};

	n = build_cluster_crosscheck(&cube, &warnings, &error_code);

	if (runtime_options.output_mode == OM_CSV)
	{
		for (i = 0; i < n; i++)
		{
			int j;
			for (j = 0; j < n; j++)
			{
				int			max_node_status = -2;
				int			node_ix = 0;

				for (node_ix = 0; node_ix < n; node_ix++)
				{
					int			node_status = cube[node_ix]->matrix_list_rec[i]->node_status_list[j]->node_status;

					if (node_status > max_node_status)
						max_node_status = node_status;
				}
				printf("%i,%i,%i\n",
					   cube[i]->node_id,
					   cube[j]->node_id,
					   max_node_status);

				if (max_node_status == -1)
				{
					connection_error_found = true;
				}
			}

		}
	}
	else
	{
		/* output header contains node name, node ID and one column for each node in the cluster */
		struct ColHeader *headers_crosscheck = NULL;

		int header_count = n + 2;
		int header_id = 2;

		headers_crosscheck = palloc0(sizeof(ColHeader) * header_count);

		/* Initialize column headers  */
		strncpy(headers_crosscheck[0].title, _("Name"), MAXLEN);
		strncpy(headers_crosscheck[1].title, _("ID"), MAXLEN);

		for (i = 0; i < n; i++)
		{
			maxlen_snprintf(headers_crosscheck[header_id].title, "%i", cube[i]->node_id);
			header_id++;
		}

		/* Initialize column max values */
		for (i = 0; i < header_count; i++)
		{
			headers_crosscheck[i].display = true;
			headers_crosscheck[i].max_length = strlen(headers_crosscheck[i].title);
			headers_crosscheck[i].cur_length = headers_crosscheck[i].max_length;

			/* We can derive the maximum node ID length for the ID column from
			 * the generated matrix node ID headers
			 */
			if (i >= 2 && headers_crosscheck[i].max_length > headers_crosscheck[1].max_length)
				headers_crosscheck[1].max_length = headers_crosscheck[i].max_length;
		}

		for (i = 0; i < n; i++)
		{
			if (strlen(cube[i]->node_name) > headers_crosscheck[0].max_length)
			{
				headers_crosscheck[0].max_length = strlen(cube[i]->node_name);
			}
		}

		print_status_header(header_count, headers_crosscheck);

		for (i = 0; i < n; i++)
		{
			int			column_node_ix;

			printf(" %-*s | %-*i ",
				   headers_crosscheck[0].max_length,
				   cube[i]->node_name,
				   headers_crosscheck[1].max_length,
				   cube[i]->node_id);

			for (column_node_ix = 0; column_node_ix < n; column_node_ix++)
			{
				int			max_node_status = -2;
				int			node_ix = 0;

				char		c;

				/*
				 * The value of entry (i,j) is equal to the maximum value of all
				 * the (i,j,k). Indeed:
				 *
				 * - if one of the (i,j,k) is 0 (node up), then 0 (the node is
				 * up);
				 *
				 * - if the (i,j,k) are either -1 (down) or -2 (unknown), then -1
				 * (the node is down);
				 *
				 * - if all the (i,j,k) are -2 (unknown), then -2 (the node is in
				 * an unknown state).
				 */

				for (node_ix = 0; node_ix < n; node_ix++)
				{
					int			node_status = cube[node_ix]->matrix_list_rec[i]->node_status_list[column_node_ix]->node_status;

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
						connection_error_found = true;
						break;
					case 0:
						c = '*';
						break;
					default:
						log_error("unexpected node status value %i", max_node_status);
						exit(ERR_INTERNAL);
				}

				printf("| %-*c ", headers_crosscheck[column_node_ix + 2].max_length, c);
			}

			printf("\n");
		}

		pfree(headers_crosscheck);

		if (warnings.head != NULL && runtime_options.terse == false)
		{
			log_warning(_("following problems detected:"));
			print_item_list(&warnings);
		}

	}

	/* clean up allocated cube array */
	{
		int			h,
					j;

		for (h = 0; h < n; h++)
		{
			for (i = 0; i < n; i++)
			{
				for (j = 0; j < n; j++)
				{
					free(cube[h]->matrix_list_rec[i]->node_status_list[j]);
				}
				free(cube[h]->matrix_list_rec[i]->node_status_list);
				free(cube[h]->matrix_list_rec[i]);
			}

			free(cube[h]->matrix_list_rec);
			free(cube[h]);
		}

		free(cube);
	}

	/* errors detected by build_cluster_crosscheck() have priority */
	if (connection_error_found == true)
	{
		error_code = ERR_NODE_STATUS;
	}

	exit(error_code);

}


/*
 * CLUSTER MATRIX
 *
 * Parameters:
 *   --csv
 */
void
do_cluster_matrix()
{
	int			i = 0,
				j = 0,
				n = 0;

	t_node_matrix_rec **matrix_rec_list;

	bool		connection_error_found = false;
	int			error_code = SUCCESS;
	ItemList	warnings = {NULL, NULL};

	n = build_cluster_matrix(&matrix_rec_list, &warnings, &error_code);

	if (runtime_options.output_mode == OM_CSV)
	{
		for (i = 0; i < n; i++)
		{
			for (j = 0; j < n; j++)
			{
				printf("%d,%d,%d\n",
					   matrix_rec_list[i]->node_id,
					   matrix_rec_list[i]->node_status_list[j]->node_id,
					   matrix_rec_list[i]->node_status_list[j]->node_status);

				if (matrix_rec_list[i]->node_status_list[j]->node_status == -2
					|| matrix_rec_list[i]->node_status_list[j]->node_status == -1)
				{
					connection_error_found = true;
				}
			}
		}
	}
	else
	{
		/* output header contains node name, node ID and one column for each node in the cluster */
		struct ColHeader *headers_matrix = NULL;

		int header_count = n + 2;
		int header_id = 2;

		headers_matrix = palloc0(sizeof(ColHeader) * header_count);

		/* Initialize column headers  */
		strncpy(headers_matrix[0].title, _("Name"), MAXLEN);
		strncpy(headers_matrix[1].title, _("ID"), MAXLEN);

		for (i = 0; i < n; i++)
		{
			maxlen_snprintf(headers_matrix[header_id].title, "%i", matrix_rec_list[i]->node_id);
			header_id++;
		}

		/* Initialize column max values */
		for (i = 0; i < header_count; i++)
		{
			headers_matrix[i].display = true;
			headers_matrix[i].max_length = strlen(headers_matrix[i].title);
			headers_matrix[i].cur_length = headers_matrix[i].max_length;

			/* We can derive the maximum node ID length for the ID column from
			 * the generated matrix node ID headers
			 */
			if (i >= 2 && headers_matrix[i].max_length > headers_matrix[1].max_length)
				headers_matrix[1].max_length = headers_matrix[i].max_length;
		}


		for (i = 0; i < n; i++)
		{
			if (strlen(matrix_rec_list[i]->node_name) > headers_matrix[0].max_length)
			{
				headers_matrix[0].max_length = strlen(matrix_rec_list[i]->node_name);
			}
		}

		print_status_header(header_count, headers_matrix);

		for (i = 0; i < n; i++)
		{
			printf(" %-*s | %-*i ",
				   headers_matrix[0].max_length,
				   matrix_rec_list[i]->node_name,
				   headers_matrix[1].max_length,
				   matrix_rec_list[i]->node_id);
			for (j = 0; j < n; j++)
			{
				char		c;

				switch (matrix_rec_list[i]->node_status_list[j]->node_status)
				{
					case -2:
						c = '?';
						break;
					case -1:
						c = 'x';
						connection_error_found = true;
						break;
					case 0:
						c = '*';
						break;
					default:
						log_error("unexpected node status value %i", matrix_rec_list[i]->node_status_list[j]->node_status);
						exit(ERR_INTERNAL);
				}

				printf("| %-*c ", headers_matrix[j + 2].max_length, c);
			}
			printf("\n");
		}

		pfree(headers_matrix);

		if (warnings.head != NULL && runtime_options.terse == false)
		{
			log_warning(_("following problems detected:"));
			print_item_list(&warnings);
		}

	}

	for (i = 0; i < n; i++)
	{
		for (j = 0; j < n; j++)
		{
			free(matrix_rec_list[i]->node_status_list[j]);
		}
		free(matrix_rec_list[i]->node_status_list);
		free(matrix_rec_list[i]);
	}

	free(matrix_rec_list);

	/* actual database connection errors have priority */
	if (connection_error_found == true)
	{
		error_code = ERR_NODE_STATUS;
	}

	exit(error_code);
}


static void
matrix_set_node_status(t_node_matrix_rec **matrix_rec_list, int n, int node_id, int connection_node_id, int connection_status)
{
	int			i,
				j;

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
build_cluster_matrix(t_node_matrix_rec ***matrix_rec_dest, ItemList *warnings, int *error_code)
{
	PGconn	   *conn = NULL;
	int			i = 0,
				j = 0;
	int			local_node_id = UNKNOWN_NODE_ID;
	int			node_count = 0;
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

	if (get_all_node_records(conn, &nodes) == false)
	{
		/* get_all_node_records() will display the error */
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	PQfinish(conn);
	conn = NULL;

	if (nodes.node_count == 0)
	{
		log_error(_("unable to retrieve any node records"));
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Allocate an empty matrix record list
	 *
	 * -2 == NULL  ? -1 == Error x 0 == OK
	 */

	matrix_rec_list = (t_node_matrix_rec **) pg_malloc0(sizeof(t_node_matrix_rec) * nodes.node_count);

	i = 0;

	/* Initialise matrix structure for each node */
	for (cell = nodes.head; cell; cell = cell->next)
	{
		NodeInfoListCell *cell_j;

		matrix_rec_list[i] = (t_node_matrix_rec *) pg_malloc0(sizeof(t_node_matrix_rec));

		matrix_rec_list[i]->node_id = cell->node_info->node_id;
		strncpy(matrix_rec_list[i]->node_name,
				cell->node_info->node_name,
				sizeof(matrix_rec_list[i]->node_name));

		matrix_rec_list[i]->node_status_list = (t_node_status_rec **) pg_malloc0(sizeof(t_node_status_rec) * nodes.node_count);

		j = 0;

		for (cell_j = nodes.head; cell_j; cell_j = cell_j->next)
		{
			matrix_rec_list[i]->node_status_list[j] = (t_node_status_rec *) pg_malloc0(sizeof(t_node_status_rec));
			matrix_rec_list[i]->node_status_list[j]->node_id = cell_j->node_info->node_id;
			matrix_rec_list[i]->node_status_list[j]->node_status = -2;	/* default unknown */

			j++;
		}

		i++;
	}

	/* Fetch `repmgr cluster show --csv` output for each node */
	i = 0;

	for (cell = nodes.head; cell; cell = cell->next)
	{
		int			connection_status = 0;
		t_conninfo_param_list remote_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;
		char	   *host = NULL,
				   *p = NULL;
		int			connection_node_id = cell->node_info->node_id;
		int			x,
					y;
		PGconn	   *node_conn = NULL;

		initialize_conninfo_params(&remote_conninfo, false);
		parse_conninfo_string(cell->node_info->conninfo,
							  &remote_conninfo,
							  NULL,
							  false);

		host = param_get(&remote_conninfo, "host");

		node_conn = establish_db_connection_quiet(cell->node_info->conninfo);

		connection_status =
			(PQstatus(node_conn) == CONNECTION_OK) ? 0 : -1;


		matrix_set_node_status(matrix_rec_list,
							   nodes.node_count,
							   local_node_id,
							   connection_node_id,
							   connection_status);


		if (connection_status)
		{
			free_conninfo_params(&remote_conninfo);
			PQfinish(node_conn);
			node_conn = NULL;
			continue;
		}

		/* We don't need to issue `cluster show --csv` for the local node */
		if (connection_node_id == local_node_id)
		{
			free_conninfo_params(&remote_conninfo);
			PQfinish(node_conn);
			node_conn = NULL;
			continue;
		}

		initPQExpBuffer(&command);

		/*
		 * We'll pass cluster name and database connection string to the
		 * remote repmgr - those are the only values it needs to work, and
		 * saves us making assumptions about the location of repmgr.conf
		 */
		appendPQExpBufferChar(&command, '"');

		make_remote_repmgr_path(&command, cell->node_info);

		appendPQExpBufferStr(&command,
							 " cluster show --csv --terse");

		/*
		 * Usually we'll want NOTICE as the log level, but if the user
		 * explicitly provided one with --log-level, that will be passed
		 * in the remote repmgr invocation.
		 */
		if (runtime_options.log_level[0] == '\0')
		{
			appendPQExpBufferStr(&command,
								 " -L NOTICE");
		}
		appendPQExpBufferChar(&command, '"');

		log_verbose(LOG_DEBUG, "build_cluster_matrix(): executing:\n  %s", command.data);

		initPQExpBuffer(&command_output);

		(void) remote_command(host,
							  runtime_options.remote_user,
							  command.data,
							  config_file_options.ssh_options,
							  &command_output);

		p = command_output.data;

		termPQExpBuffer(&command);

		/* no output returned - probably SSH error */
		if (p[0] == '\0' || p[0] == '\n')
		{
			item_list_append_format(warnings,
									"node %i inaccessible via SSH",
									connection_node_id);
			*error_code = ERR_BAD_SSH;
		}
		else
		{
			for (j = 0; j < nodes.node_count; j++)
			{
				if (sscanf(p, "%d,%d", &x, &y) != 2)
				{
					matrix_set_node_status(matrix_rec_list,
										   nodes.node_count,
										   connection_node_id,
										   x,
										   -2);

					item_list_append_format(warnings,
											"unable to parse --csv output for node %i; output returned was:\n\"%s\"",
											connection_node_id, p);
					*error_code = ERR_INTERNAL;
				}
				else
				{
					matrix_set_node_status(matrix_rec_list,
										   nodes.node_count,
										   connection_node_id,
										   x,
										   (y == -1) ? -1 : 0);
				}

				while (*p && (*p != '\n'))
					p++;
				if (*p == '\n')
					p++;
			}
		}

		termPQExpBuffer(&command_output);
		PQfinish(node_conn);
		free_conninfo_params(&remote_conninfo);
	}

	*matrix_rec_dest = matrix_rec_list;

	node_count = nodes.node_count;
	clear_node_info_list(&nodes);

	return node_count;
}


static int
build_cluster_crosscheck(t_node_status_cube ***dest_cube, ItemList *warnings, int *error_code)
{
	PGconn	   *conn = NULL;
	int			h,
				i,
				j;
	NodeInfoList nodes = T_NODE_INFO_LIST_INITIALIZER;
	NodeInfoListCell *cell = NULL;

	t_node_status_cube **cube;

	int			node_count = 0;

	/* We need to connect to get the list of nodes */
	log_info(_("connecting to database"));

	if (strlen(config_file_options.conninfo))
		conn = establish_db_connection(config_file_options.conninfo, true);
	else
		conn = establish_db_connection_by_params(&source_conninfo, true);

	if (get_all_node_records(conn, &nodes) == false)
	{
		/* get_all_node_records() will display the error */
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	PQfinish(conn);
	conn = NULL;

	if (nodes.node_count == 0)
	{
		log_error(_("unable to retrieve any node records"));
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Allocate an empty cube matrix structure
	 *
	 * -2 == NULL -1 == Error 0 == OK
	 */

	cube = (t_node_status_cube **) pg_malloc(sizeof(t_node_status_cube *) * nodes.node_count);

	h = 0;

	for (cell = nodes.head; cell; cell = cell->next)
	{
		NodeInfoListCell *cell_i = NULL;

		cube[h] = (t_node_status_cube *) pg_malloc(sizeof(t_node_status_cube));
		cube[h]->node_id = cell->node_info->node_id;
		strncpy(cube[h]->node_name, cell->node_info->node_name, sizeof(cube[h]->node_name));

		cube[h]->matrix_list_rec = (t_node_matrix_rec **) pg_malloc(sizeof(t_node_matrix_rec) * nodes.node_count);

		i = 0;
		for (cell_i = nodes.head; cell_i; cell_i = cell_i->next)
		{
			NodeInfoListCell *cell_j;

			cube[h]->matrix_list_rec[i] = (t_node_matrix_rec *) pg_malloc0(sizeof(t_node_matrix_rec));
			cube[h]->matrix_list_rec[i]->node_id = cell_i->node_info->node_id;

			/* we don't need the name here */
			cube[h]->matrix_list_rec[i]->node_name[0] = '\0';

			cube[h]->matrix_list_rec[i]->node_status_list = (t_node_status_rec **) pg_malloc0(sizeof(t_node_status_rec *) * nodes.node_count);

			j = 0;

			for (cell_j = nodes.head; cell_j; cell_j = cell_j->next)
			{
				cube[h]->matrix_list_rec[i]->node_status_list[j] = (t_node_status_rec *) pg_malloc0(sizeof(t_node_status_rec));
				cube[h]->matrix_list_rec[i]->node_status_list[j]->node_id = cell_j->node_info->node_id;
				cube[h]->matrix_list_rec[i]->node_status_list[j]->node_status = -2; /* default unknown */

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
		int			remote_node_id = UNKNOWN_NODE_ID;
		PQExpBufferData command;
		PQExpBufferData command_output;

		char	   *p = NULL;

		remote_node_id = cell->node_info->node_id;

		initPQExpBuffer(&command);

		make_remote_repmgr_path(&command, cell->node_info);

		appendPQExpBufferStr(&command,
							 " cluster matrix --csv --terse");

		/*
		 * Usually we'll want NOTICE as the log level, but if the user
		 * explicitly provided one with --log-level, that will be passed
		 * in the remote repmgr invocation.
		 */
		if (runtime_options.log_level[0] == '\0')
		{
			appendPQExpBufferStr(&command,
								 " -L NOTICE");
		}

		initPQExpBuffer(&command_output);

		if (cube[i]->node_id == config_file_options.node_id)
		{
			(void) local_command_simple(command.data,
										&command_output);
		}
		else
		{
			t_conninfo_param_list remote_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;
			char	   *host = NULL;
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

			(void) remote_command(host,
								  runtime_options.remote_user,
								  quoted_command.data,
								  config_file_options.ssh_options,
								  &command_output);

			free_conninfo_params(&remote_conninfo);
			termPQExpBuffer(&quoted_command);
		}

		termPQExpBuffer(&command);

		p = command_output.data;

		if (p[0] == '\0' || p[0] == '\n')
		{
			item_list_append_format(warnings,
									"node %i inaccessible via SSH",
									remote_node_id);
			termPQExpBuffer(&command_output);
			*error_code = ERR_BAD_SSH;
			continue;
		}

		for (j = 0; j < (nodes.node_count * nodes.node_count); j++)
		{
			int			matrix_rec_node_id;
			int			node_status_node_id;
			int			node_status;

			if (sscanf(p, "%d,%d,%d", &matrix_rec_node_id, &node_status_node_id, &node_status) != 3)
			{
				cube_set_node_status(cube,
									 nodes.node_count,
									 remote_node_id,
									 matrix_rec_node_id,
									 node_status_node_id,
									 -2);
				*error_code = ERR_INTERNAL;
			}
			else
			{
				cube_set_node_status(cube,
									 nodes.node_count,
									 remote_node_id,
									 matrix_rec_node_id,
									 node_status_node_id,
									 node_status);
			}

			while (*p && (*p != '\n'))
				p++;
			if (*p == '\n')
				p++;
		}
		termPQExpBuffer(&command_output);

		i++;
	}

	*dest_cube = cube;

	node_count = nodes.node_count;

	clear_node_info_list(&nodes);

	return node_count;
}


static void
cube_set_node_status(t_node_status_cube **cube, int n, int execute_node_id, int matrix_node_id, int connection_node_id, int connection_status)
{
	int			h,
				i,
				j;


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


void
do_cluster_cleanup(void)
{
	PGconn	   *conn = NULL;
	PGconn	   *primary_conn = NULL;
	int			entries_to_delete = 0;
	PQExpBufferData event_details;

	conn = establish_db_connection(config_file_options.conninfo, true);

	/* check if there is a primary in this cluster */
	log_info(_("connecting to primary server"));
	primary_conn = establish_primary_db_connection(conn, true);

	PQfinish(conn);

	log_debug(_("number of days of monitoring history to retain: %i"), runtime_options.keep_history);

	entries_to_delete = get_number_of_monitoring_records_to_delete(primary_conn,
																   runtime_options.keep_history,
																   runtime_options.node_id);

	if (entries_to_delete < 0)
	{
		log_error(_("unable to query number of monitoring records to clean up"));
		PQfinish(primary_conn);
		exit(ERR_DB_QUERY);
	}
	else if (entries_to_delete == 0)
	{
		log_info(_("no monitoring records to delete"));
		PQfinish(primary_conn);
		return;
	}

	log_debug("at least %i monitoring records for deletion",
			  entries_to_delete);

	initPQExpBuffer(&event_details);

	if (delete_monitoring_records(primary_conn, runtime_options.keep_history, runtime_options.node_id) == false)
	{
		appendPQExpBufferStr(&event_details,
						  _("unable to delete monitoring records"));

		log_error("%s", event_details.data);
		log_detail("%s", PQerrorMessage(primary_conn));

		create_event_notification(primary_conn,
								  &config_file_options,
								  config_file_options.node_id,
								  "cluster_cleanup",
								  false,
								  event_details.data);

		PQfinish(primary_conn);
		exit(ERR_DB_QUERY);
	}

	if (vacuum_table(primary_conn, "repmgr.monitoring_history") == false)
	{
		/* annoying if this fails, but not fatal */
		log_warning(_("unable to vacuum table \"repmgr.monitoring_history\""));
		log_detail("%s", PQerrorMessage(primary_conn));
	}
	else
	{
		log_info(_("vacuum of table \"repmgr.monitoring_history\" completed"));
	}

	if (runtime_options.keep_history == 0)
	{
		appendPQExpBufferStr(&event_details,
						  _("all monitoring records deleted"));
	}
	else
	{
		appendPQExpBufferStr(&event_details,
						  _("monitoring records deleted"));
	}

	if (runtime_options.node_id != UNKNOWN_NODE_ID)
		appendPQExpBuffer(&event_details,
						  _(" for node %i"),
						  runtime_options.node_id);

	if (runtime_options.keep_history > 0)
		appendPQExpBuffer(&event_details,
						  _("; records newer than %i day(s) retained"),
						  runtime_options.keep_history);

	create_event_notification(primary_conn,
							  &config_file_options,
							  config_file_options.node_id,
							  "cluster_cleanup",
							  true,
							  event_details.data);

	log_notice("%s", event_details.data);

	termPQExpBuffer(&event_details);
	PQfinish(primary_conn);


	return;
}


void
do_cluster_help(void)
{
	print_help_header();

	printf(_("Usage:\n"));
	printf(_("    %s [OPTIONS] cluster show\n"), progname());
	printf(_("    %s [OPTIONS] cluster matrix\n"), progname());
	printf(_("    %s [OPTIONS] cluster crosscheck\n"), progname());
	printf(_("    %s [OPTIONS] cluster event\n"), progname());
	printf(_("    %s [OPTIONS] cluster cleanup\n"), progname());
	puts("");

	printf(_("CLUSTER SHOW\n"));
	puts("");
	printf(_("  \"cluster show\" displays a list showing the status of each node in the cluster.\n"));
	puts("");
	printf(_("  Configuration file or database connection required.\n"));
	puts("");
	printf(_("    --csv                     emit output as CSV (with a subset of fields)\n"));
	printf(_("    --compact                 display only a subset of fields\n"));
	puts("");

	printf(_("CLUSTER MATRIX\n"));
	puts("");
	printf(_("  \"cluster matrix\" displays a matrix showing connectivity between nodes, seen from this node.\n"));
	puts("");
	printf(_("  Configuration file or database connection required.\n"));
	puts("");
	printf(_("    --csv                     emit output as CSV\n"));
	puts("");

	printf(_("CLUSTER CROSSCHECK\n"));
	puts("");
	printf(_("  \"cluster crosscheck\" displays a matrix showing connectivity between nodes, seen from all nodes.\n"));
	puts("");
	printf(_("  Configuration file or database connection required.\n"));
	puts("");
	printf(_("    --csv                     emit output as CSV\n"));
	puts("");


	printf(_("CLUSTER EVENT\n"));
	puts("");
	printf(_("  \"cluster event\" lists recent events logged in the \"repmgr.events\" table.\n"));
	puts("");
	printf(_("    --limit                   maximum number of events to display (default: %i)\n"), CLUSTER_EVENT_LIMIT);
	printf(_("    --all                     display all events (overrides --limit)\n"));
	printf(_("    --event                   filter specific event\n"));
	printf(_("    --node-id                 restrict entries to node with this ID\n"));
	printf(_("    --node-name               restrict entries to node with this name\n"));
	printf(_("    --compact                 omit \"Details\" column"));
	printf(_("    --csv                     emit output as CSV\n"));
	puts("");

	printf(_("CLUSTER CLEANUP\n"));
	puts("");
	printf(_("  \"cluster cleanup\" purges records from the \"repmgr.monitoring_history\" table.\n"));
	puts("");
	printf(_("    -k, --keep-history=VALUE  retain indicated number of days of history (default: 0)\n"));
	puts("");

	printf(_("%s home page: <%s>\n"), "repmgr", REPMGR_URL);
}
