/*
 * sysutils.h
 * Copyright (c) 2ndQuadrant, 2010-2020
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

#ifndef _SYSUTILS_H_
#define _SYSUTILS_H_

extern bool local_command(const char *command, PQExpBufferData *outputbuf);
extern bool local_command_return_value(const char *command, PQExpBufferData *outputbuf, int *return_value);
extern bool local_command_simple(const char *command, PQExpBufferData *outputbuf);

extern bool remote_command(const char *host, const char *user, const char *command, const char *ssh_options, PQExpBufferData *outputbuf);
extern void make_remote_command(const char *host, const char *user, const char *command, const char *ssh_options, PQExpBufferData *ssh_command);

extern pid_t disable_wal_receiver(PGconn *conn);
extern pid_t enable_wal_receiver(PGconn *conn, bool wait_startup);

#endif							/* _SYSUTILS_H_ */
