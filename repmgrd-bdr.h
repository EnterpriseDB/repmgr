/*
 * repmgrd-bdr.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#ifndef _REPMGRD_BDR_H_
#define _REPMGRD_BDR_H_

extern void do_bdr_node_check(void);
extern void monitor_bdr(void);
extern t_node_info *do_bdr_failover(NodeInfoList *nodes, t_node_info *monitored_node);

#endif /* _REPMGRD_BDR_H_ */
