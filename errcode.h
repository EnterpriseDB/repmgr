/*
 * errcode.h
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

#ifndef _ERRCODE_H_
#define _ERRCODE_H_

/* Exit return codes */

#define SUCCESS 0
#define ERR_BAD_CONFIG 1
#define ERR_BAD_RSYNC 2
#define ERR_BAD_PIDFILE 3
#define ERR_NO_RESTART 4
#define ERR_LOCAL_COMMAND 5
#define ERR_DB_CONN 6
#define ERR_DB_QUERY 7
#define ERR_PROMOTION_FAIL 8
#define ERR_MONITORING_TIMEOUT 9
#define ERR_STR_OVERFLOW 10
#define ERR_FAILOVER_FAIL 11
#define ERR_BAD_SSH 12
#define ERR_SYS_FAILURE 13
#define ERR_BAD_BASEBACKUP 14
#define ERR_INTERNAL 15
#define ERR_MONITORING_FAIL 16
#define ERR_BAD_BACKUP_LABEL 17
#define ERR_SWITCHOVER_FAIL 18
#define ERR_BARMAN 19
#define ERR_REGISTRATION_SYNC 20
#define ERR_OUT_OF_MEMORY 21
#define ERR_SWITCHOVER_INCOMPLETE 22
#define ERR_FOLLOW_FAIL 23
#define ERR_REJOIN_FAIL 24
#define ERR_NODE_STATUS 25
#define ERR_REPMGRD_PAUSE 26
#define ERR_REPMGRD_SERVICE 27
#define ERR_PGBACKUPAPI_SERVICE 28

#endif							/* _ERRCODE_H_ */
