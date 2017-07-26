/*
 * repmgr-action-cluster.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#ifndef _REPMGR_ACTION_CLUSTER_H_
#define _REPMGR_ACTION_CLUSTER_H_



typedef struct
{
	int node_id;
	int node_status;
} t_node_status_rec;

typedef struct
{
	int node_id;
	char node_name[MAXLEN];
	t_node_status_rec **node_status_list;
} t_node_matrix_rec;

typedef struct
{
	int node_id;
	char node_name[MAXLEN];
	t_node_matrix_rec **matrix_list_rec;
} t_node_status_cube;



extern void do_cluster_show(void);
extern void do_cluster_event(void);
extern void do_cluster_crosscheck(void);
extern void do_cluster_matrix(void);


#endif
