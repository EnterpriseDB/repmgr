/*
 * repmgrd-physical.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#ifndef _REPMGRD_PHYSICAL_H_
#define _REPMGRD_PHYSICAL_H_

void do_physical_node_check(void);

void monitor_streaming_primary(void);
void monitor_streaming_standby(void);
void close_connections_physical(void);

#endif /* _REPMGRD_PHYSICAL_H_ */
