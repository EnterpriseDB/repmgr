/*
 * compat.h
 * Copyright (c) EnterpriseDB Corporation, 2010-2021
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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

#ifndef _COMPAT_H_
#define _COMPAT_H_

extern void appendConnStrVal(PQExpBuffer buf, const char *str);

extern void appendShellString(PQExpBuffer buf, const char *str);

extern void appendRemoteShellString(PQExpBuffer buf, const char *str);

#endif
