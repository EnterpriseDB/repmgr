/*
 * errcode.h
 * Copyright (C) 2ndQuadrant, 2010-2015
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
 *
 */

#ifndef _ERRCODE_H_
#define _ERRCODE_H_

/* Exit return code */

#define SUCCESS 0
#define ERR_BAD_CONFIG 1
#define ERR_BAD_RSYNC 2
#define ERR_STOP_BACKUP 3
#define ERR_NO_RESTART 4
#define ERR_NEEDS_XLOG 5
#define ERR_DB_CON 6
#define ERR_DB_QUERY 7
#define ERR_PROMOTED 8
#define ERR_BAD_PASSWORD 9
#define ERR_STR_OVERFLOW 10
#define ERR_FAILOVER_FAIL 11
#define ERR_BAD_SSH 12
#define ERR_SYS_FAILURE 13

#endif   /* _ERRCODE_H_ */
