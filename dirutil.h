/*
 * dirutil.h
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

#ifndef _DIRUTIL_H_
#define _DIRUTIL_H_

typedef enum
{
	DIR_ERROR = -1,
	DIR_NOENT,
	DIR_EMPTY,
	DIR_NOT_EMPTY
} DataDirState;

typedef enum
{
	PG_DIR_ERROR = -1,
	PG_DIR_NOT_RUNNING,
	PG_DIR_RUNNING
} PgDirState;

extern int	mkdir_p(char *path, mode_t omode);
extern bool set_dir_permissions(const char *path, int server_version_num);

extern DataDirState	check_dir(const char *path);
extern bool create_dir(const char *path);
extern bool is_pg_dir(const char *path);
extern PgDirState is_pg_running(const char *path);
extern bool create_pg_dir(const char *path, bool force);
extern int rmdir_recursive(const char *path);

#endif
