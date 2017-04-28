/*
 * dirutil.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 *
 */

#ifndef _DIRUTIL_H_
#define _DIRUTIL_H_

extern int		mkdir_p(char *path, mode_t omode);
extern bool		set_dir_permissions(char *path);

extern int		check_dir(char *path);
extern bool		create_dir(char *path);
extern bool		is_pg_dir(char *path);
extern bool		create_pg_dir(char *path, bool force);
extern bool		create_witness_pg_dir(char *path, bool force);

#endif
