/*
 * repmgr-action-standby.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#ifndef _REPMGR_ACTION_STANDBY_H_
#define _REPMGR_ACTION_STANDBY_H_

extern void do_standby_clone(void);
extern void do_standby_register(void);
extern void do_standby_unregister(void);
extern void do_standby_promote(void);
extern void do_standby_follow(void);
extern void do_standby_switchover(void);

extern void do_standby_help(void);

extern bool do_standby_follow_internal(PGconn *primary_conn, t_node_info *primary_node_record, PQExpBufferData *output);



#endif /* _REPMGR_ACTION_STANDBY_H_ */
