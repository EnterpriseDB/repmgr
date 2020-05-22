/*
 * repmgrd.h
 * Copyright (c) 2ndQuadrant, 2010-2020
 */


#ifndef _REPMGRD_H_
#define _REPMGRD_H_

#include <time.h>
#include "portability/instr_time.h"

#define OPT_NO_PID_FILE                  1000
#define OPT_DAEMONIZE                    1001

extern volatile sig_atomic_t got_SIGHUP;
extern MonitoringState monitoring_state;
extern instr_time degraded_monitoring_start;

extern t_node_info local_node_info;
extern PGconn *local_conn;
extern bool startup_event_logged;
extern char pid_file[MAXPGPATH];

bool		check_upstream_connection(PGconn **conn, const char *conninfo, PGconn **paired_conn);
void		try_reconnect(PGconn **conn, t_node_info *node_info);

int			calculate_elapsed(instr_time start_time);
const char *print_monitoring_state(MonitoringState monitoring_state);

void		update_registration(PGconn *conn);
void		terminate(int retval);

#endif							/* _REPMGRD_H_ */
