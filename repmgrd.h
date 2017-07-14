/*
 * repmgrd.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#ifndef _REPMGRD_H_
#define _REPMGRD_H_

#include <time.h>
#include "portability/instr_time.h"

typedef enum {
	NODE_STATUS_UNKNOWN = -1,
	NODE_STATUS_UP,
	NODE_STATUS_DOWN
} NodeStatus;

typedef enum {
	MS_NORMAL = 0,
	MS_DEGRADED = 1
} MonitoringState;

extern MonitoringState monitoring_state;
extern instr_time	degraded_monitoring_start;

extern t_configuration_options config_file_options;
extern t_node_info local_node_info;
extern PGconn	   *local_conn;
extern bool			startup_event_logged;

PGconn *try_reconnect(const char *conninfo, NodeStatus *node_status);

int calculate_elapsed(instr_time start_time);
void update_registration(PGconn *conn);
void terminate(int retval);
#endif /* _REPMGRD_H_ */
