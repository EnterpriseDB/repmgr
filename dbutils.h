/*
 * dbutils.h
 * Copyright (c) 2ndQuadrant, 2010
 *
 */

PGconn *establishDBConnection(const char *conninfo, const bool exit_on_error);
bool	is_standby(PGconn *conn);
char   *pg_version(PGconn *conn);
bool	guc_setted(PGconn *conn, const char *parameter, const char *op,
				   const char *value);
const char	 *get_cluster_size(PGconn *conn);
PGconn * getMasterConnection(PGconn *standby_conn, int id, char *cluster,
							 int *master_id);
