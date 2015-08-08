/*
 * dbutils.h
 * Copyright (c) 2ndQuadrant, 2010-2015
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
 *
 */

#ifndef _REPMGR_DBUTILS_H_
#define _REPMGR_DBUTILS_H_

#include "config.h"
#include "strutil.h"



PGconn *establish_db_connection(const char *conninfo,
						const bool exit_on_error);
PGconn *establish_db_connection_by_params(const char *keywords[],
								  const char *values[],
								  const bool exit_on_error);
bool		check_cluster_schema(PGconn *conn);
int			is_standby(PGconn *conn);
bool		is_pgup(PGconn *conn, int timeout);
int			get_master_node_id(PGconn *conn, char *cluster);
int			get_server_version(PGconn *conn, char *server_version);
bool		get_cluster_size(PGconn *conn, char *size);
bool		get_pg_setting(PGconn *conn, const char *setting, char *output);

int			guc_set(PGconn *conn, const char *parameter, const char *op,
			const char *value);
int			guc_set_typed(PGconn *conn, const char *parameter, const char *op,
			  const char *value, const char *datatype);

PGconn     *get_upstream_connection(PGconn *standby_conn, char *cluster,
									int node_id,
									int *upstream_node_id_ptr,
									char *upstream_conninfo_out);
PGconn	   *get_master_connection(PGconn *standby_conn, char *cluster,
					  int *master_id, char *master_conninfo_out);

int			wait_connection_availability(PGconn *conn, long long timeout);
bool		cancel_query(PGconn *conn, int timeout);
char       *get_repmgr_schema(void);
char       *get_repmgr_schema_quoted(PGconn *conn);
bool		create_replication_slot(PGconn *conn, char *slot_name);

bool		start_backup(PGconn *conn, char *first_wal_segment, bool fast_checkpoint);
bool		stop_backup(PGconn *conn, char *last_wal_segment);
bool		set_config_bool(PGconn *conn, const char *config_param, bool state);
bool		copy_configuration(PGconn *masterconn, PGconn *witnessconn, char *cluster_name);
bool		create_node_record(PGconn *conn, char *action, int node, char *type, int upstream_node, char *cluster_name, char *node_name, char *conninfo, int priority, char *slot_name);
bool		delete_node_record(PGconn *conn, int node, char *action);
bool        create_event_record(PGconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details);
bool        update_node_record_set_upstream(PGconn *conn, char *cluster_name, int this_node_id, int new_upstream_node_id);

#endif
