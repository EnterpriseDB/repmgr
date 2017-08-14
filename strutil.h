/*
 * strutil.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#ifndef _STRUTIL_H_
#define _STRUTIL_H_

#include <pqexpbuffer.h>

#define MAXLEN			1024
#define MAX_QUERY_LEN	8192
#define MAXVERSIONSTR	16

/* same as defined in src/include/replication/walreceiver.h */
#define MAXCONNINFO		1024

#define STR(x) CppAsString(x)

#define MAXLEN_STR STR(MAXLEN)


typedef enum {
	CHECK_STATUS_OK = 0,
	CHECK_STATUS_WARNING,
	CHECK_STATUS_CRITICAL,
	CHECK_STATUS_UNKNOWN
} CheckStatus;

typedef enum {
	OM_NOT_SET = -1,
	OM_TEXT,
	OM_CSV,
	OM_NAGIOS,
	OM_OPTFORMAT
} OutputMode;

typedef struct ItemListCell
{
	struct ItemListCell *next;
	char			    *string;
} ItemListCell;

typedef struct ItemList
{
	ItemListCell *head;
	ItemListCell *tail;
} ItemList;

typedef struct KeyValueListCell
{
	struct KeyValueListCell *next;
	char				    *key;
	char				    *value;
	OutputMode			     output_mode;
} KeyValueListCell;

typedef struct KeyValueList
{
	KeyValueListCell *head;
	KeyValueListCell *tail;
} KeyValueList;


typedef struct CheckStatusListCell
{
	struct CheckStatusListCell *next;
	char				       *item;
	CheckStatus				    status;
	char				       *details;
} CheckStatusListCell;

typedef struct CheckStatusList
{
	CheckStatusListCell *head;
	CheckStatusListCell *tail;
} CheckStatusList;



extern int
maxlen_snprintf(char *str, const char *format,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

extern int
maxpath_snprintf(char *str, const char *format,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

extern void
item_list_append(ItemList *item_list, const char *message);

extern void
item_list_append_format(ItemList *item_list, const char *format, ...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

extern void
item_list_free(ItemList *item_list);

extern void
key_value_list_set(KeyValueList *item_list, const char *key, const char *value);

extern void
key_value_list_set_format(KeyValueList *item_list, const char *key, const char *value, ...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));

extern void
key_value_list_set_output_mode(KeyValueList *item_list, const char *key, OutputMode mode);

extern const char *
key_value_list_get(KeyValueList *item_list, const char *key);

extern void
key_value_list_free(KeyValueList *item_list);

extern void
check_status_list_set(CheckStatusList *list, const char *item, CheckStatus status, const char *details);

extern void
check_status_list_set_format(CheckStatusList *list, const char *item, CheckStatus status, const char *details, ...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 4, 5)));

extern void
check_status_list_free(CheckStatusList *list);

extern const char * output_check_status(CheckStatus status);

extern char *
escape_recovery_conf_value(const char *src);

extern char *
escape_string(PGconn *conn, const char *string);

extern void
append_where_clause(PQExpBufferData *where_clause, const char *clause, ...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

extern char *
string_skip_prefix(const char *prefix, char *string);

extern char
*string_remove_trailing_newlines(char *string);

extern char	*trim(char *s);


#endif	 /* _STRUTIL_H_ */
