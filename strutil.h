/*
 * strutil.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#ifndef _STRUTIL_H_
#define _STRUTIL_H_

#define MAXLEN			1024
#define MAX_QUERY_LEN	8192

/* same as defined in src/include/replication/walreceiver.h */
#define MAXCONNINFO		1024

/* Why? http://stackoverflow.com/a/5459929/398670 */
#define STR(x) CppAsString(x)

#define MAXLEN_STR STR(MAXLEN)

extern int
sqlquery_snprintf(char *str, const char *format,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

extern int
maxlen_snprintf(char *str, const char *format,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

extern char *
escape_recovery_conf_value(const char *src);

#endif	 /* _STRUTIL_H_ */
