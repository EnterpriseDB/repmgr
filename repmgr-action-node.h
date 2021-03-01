/*
 * repmgr-action-node.h
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

#ifndef _REPMGR_ACTION_NODE_H_
#define _REPMGR_ACTION_NODE_H_

extern void do_node_status(void);
extern void do_node_check(void);

extern void do_node_rejoin(void);
extern void do_node_service(void);
extern void do_node_control(void);

extern void do_node_help(void);

#endif							/* _REPMGR_ACTION_NODE_H_ */
