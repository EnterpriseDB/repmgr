/*
 * strutil.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 *
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

#ifndef _STRUTIL_H_
#define _STRUTIL_H_

#include <stdlib.h>
#include "pqexpbuffer.h"
#include "errcode.h"


#define QUERY_STR_LEN	8192
#define MAXLEN			1024
#define MAXLINELENGTH	4096
#define MAXVERSIONSTR	16
#define MAXCONNINFO		1024

/* Why? http://stackoverflow.com/a/5459929/398670 */
#define STR(x) CppAsString(x)

#define MAXLEN_STR STR(MAXLEN)

extern int
xsnprintf(char *str, size_t size, const char *format,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));

extern int
sqlquery_snprintf(char *str, const char *format,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

extern int
maxlen_snprintf(char *str, const char *format,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

extern char *
escape_recovery_conf_value(const char *src);
#endif   /* _STRUTIL_H_ */
