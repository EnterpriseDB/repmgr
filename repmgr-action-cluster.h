/*
 * repmgr-action-cluster.h
 * Copyright (c) EnterpriseDB Corporation, 2010-2021
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

#ifndef _REPMGR_ACTION_CLUSTER_H_
#define _REPMGR_ACTION_CLUSTER_H_



typedef struct
{
	int			node_id;
	int			node_status;
} t_node_status_rec;

typedef struct
{
	int			node_id;
	char		node_name[NAMEDATALEN];
	t_node_status_rec **node_status_list;
} t_node_matrix_rec;

typedef struct
{
	int			node_id;
	char		node_name[NAMEDATALEN];
	t_node_matrix_rec **matrix_list_rec;
} t_node_status_cube;



extern void do_cluster_show(void);
extern void do_cluster_event(void);
extern void do_cluster_crosscheck(void);
extern void do_cluster_matrix(void);
extern void do_cluster_cleanup(void);

extern void do_cluster_help(void);

#endif
