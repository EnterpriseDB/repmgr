/*
 * repmgr-action-standby.h
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

#ifndef _REPMGR_ACTION_STANDBY_H_
#define _REPMGR_ACTION_STANDBY_H_

extern void do_standby_clone(void);
extern void do_standby_register(void);
extern void do_standby_unregister(void);
extern void do_standby_promote(void);
extern void do_standby_follow(void);
extern void do_standby_switchover(void);

extern void do_standby_help(void);

extern bool do_standby_follow_internal(PGconn *primary_conn, PGconn *follow_target_conn, t_node_info *follow_target_node_record, PQExpBufferData *output, int general_error_code, int *error_code);

void check_pg_backupapi_standby_clone_options(void);

#endif							/* _REPMGR_ACTION_STANDBY_H_ */
