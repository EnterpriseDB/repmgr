/*
 * strutil.c
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include "repmgr.h"
#include "log.h"
#include "strutil.h"

static int
xvsnprintf(char *str, size_t size, const char *format, va_list ap)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 0)));

static int
xvsnprintf(char *str, size_t size, const char *format, va_list ap)
{
	int			retval;

	retval = vsnprintf(str, size, format, ap);

	if (retval >= (int) size)
	{
		log_error(_("buffer of specified size not large enough to format entire string '%s'"),
				  str);
		exit(ERR_STR_OVERFLOW);
	}

	return retval;
}


int
maxlen_snprintf(char *str, const char *format,...)
{
	va_list		arglist;
	int			retval;

	va_start(arglist, format);
	retval = xvsnprintf(str, MAXLEN, format, arglist);
	va_end(arglist);

	return retval;
}


int
maxpath_snprintf(char *str, const char *format,...)
{
	va_list		arglist;
	int			retval;

	va_start(arglist, format);
	retval = xvsnprintf(str, MAXPGPATH, format, arglist);
	va_end(arglist);

	return retval;
}


void
append_where_clause(PQExpBufferData *where_clause, const char *format, ...)
{
	va_list		arglist;
	char		stringbuf[MAXLEN];

	va_start(arglist, format);
	(void) xvsnprintf(stringbuf, MAXLEN, format, arglist);
	va_end(arglist);

	if(where_clause->data[0] == '\0')
	{
		appendPQExpBuffer(where_clause,
						  " WHERE ");
	}
	else
	{
		appendPQExpBuffer(where_clause,
						  " AND ");
	}

	appendPQExpBuffer(where_clause,
					  "%s", stringbuf);

}


void
item_list_append(ItemList *item_list, const char *message)
{
	item_list_append_format(item_list, "%s", message);
}


void
item_list_append_format(ItemList *item_list, const char *format, ...)
{
	ItemListCell *cell;
	va_list		  arglist;

	cell = (ItemListCell *) pg_malloc0(sizeof(ItemListCell));

	if (cell == NULL)
	{
		log_error(_("unable to allocate memory; terminating."));
		exit(ERR_BAD_CONFIG);
	}

	cell->string = pg_malloc0(MAXLEN);

	va_start(arglist, format);

	(void) xvsnprintf(cell->string, MAXLEN, format, arglist);
	va_end(arglist);


	if (item_list->tail)
		item_list->tail->next = cell;
	else
		item_list->head = cell;

	item_list->tail = cell;
}


void
item_list_free(ItemList *item_list)
{
	ItemListCell *cell = NULL;
	ItemListCell *next_cell = NULL;

	cell = item_list->head;

	while (cell != NULL)
	{
		next_cell = cell->next;
		pfree(cell->string);
		pfree(cell);
		cell = next_cell;
	}
}


void
key_value_list_set(KeyValueList *item_list, const char *key, const char *value)
{
	key_value_list_set_format(item_list, key, "%s", value);
	return;
}

void
key_value_list_set_format(KeyValueList *item_list, const char *key, const char *value, ...)
{
	KeyValueListCell *cell = NULL;
	va_list			  arglist;
	int			  	  keylen = 0;

	cell = (KeyValueListCell *) pg_malloc0(sizeof(KeyValueListCell));

	if (cell == NULL)
	{
		log_error(_("unable to allocate memory; terminating."));
		exit(ERR_BAD_CONFIG);
	}

	keylen = strlen(key);

	cell->key = pg_malloc0(keylen + 1);
	cell->value = pg_malloc0(MAXLEN);
	cell->output_mode = OM_NOT_SET;

	strncpy(cell->key, key, keylen);

	va_start(arglist, value);
	(void) xvsnprintf(cell->value, MAXLEN, value, arglist);
	va_end(arglist);


	if (item_list->tail)
		item_list->tail->next = cell;
	else
		item_list->head = cell;

	item_list->tail = cell;

	return;
}


void
key_value_list_set_output_mode (KeyValueList *item_list, const char *key, OutputMode mode)
{
	KeyValueListCell *cell = NULL;

	for (cell = item_list->head; cell; cell = cell->next)
	{
		if (strncmp(key, cell->key, MAXLEN) == 0)
			cell->output_mode = mode;
	}
}

const char *
key_value_list_get(KeyValueList *item_list, const char *key)
{
	return NULL;
}


void
key_value_list_free(KeyValueList *item_list)
{
	KeyValueListCell *cell;
	KeyValueListCell *next_cell;

	cell = item_list->head;

	while (cell != NULL)
	{
		next_cell = cell->next;
		pfree(cell->key);
		pfree(cell->value);
		pfree(cell);
		cell = next_cell;
	}
}


void
check_status_list_set(CheckStatusList *list, const char *item, CheckStatus status, const char *details)
{
	check_status_list_set_format(list, item, status, "%s", details);
}


void
check_status_list_set_format(CheckStatusList *list, const char *item, CheckStatus status, const char *details, ...)
{
	CheckStatusListCell *cell;
	va_list			  arglist;
	int			  	  itemlen;

	cell = (CheckStatusListCell *) pg_malloc0(sizeof(CheckStatusListCell));

	if (cell == NULL)
	{
		log_error(_("unable to allocate memory; terminating."));
		exit(ERR_BAD_CONFIG);
	}

	itemlen = strlen(item);

	cell->item = pg_malloc0(itemlen + 1);
	cell->details = pg_malloc0(MAXLEN);
	cell->status = status;

	strncpy(cell->item, item, itemlen);

	va_start(arglist, details);
	(void) xvsnprintf(cell->details, MAXLEN, details, arglist);
	va_end(arglist);


	if (list->tail)
		list->tail->next = cell;
	else
		list->head = cell;

	list->tail = cell;

	return;

}


void
check_status_list_free(CheckStatusList *list)
{
	CheckStatusListCell *cell = NULL;
	CheckStatusListCell *next_cell = NULL;

	cell = list->head;

	while (cell != NULL)
	{
		next_cell = cell->next;
		pfree(cell->item);
		pfree(cell->details);
		pfree(cell);
		cell = next_cell;
	}
}



const char *
output_check_status(CheckStatus status)
{
	switch (status)
	{
		case CHECK_STATUS_OK:
			return "OK";
		case CHECK_STATUS_WARNING:
			return "WARNING";
		case CHECK_STATUS_CRITICAL:
			return "CRITICAL";
		case CHECK_STATUS_UNKNOWN:
			return "UNKNOWN";
	}

	return "UNKNOWN";

}




/*
 * Escape a string for use as a parameter in recovery.conf
 * Caller must free returned value
 */
char *
escape_recovery_conf_value(const char *src)
{
	char	   *result = escape_single_quotes_ascii(src);

	if (!result)
	{
		fprintf(stderr, _("%s: out of memory\n"), progname());
		exit(ERR_INTERNAL);
	}
	return result;
}


char *
escape_string(PGconn *conn, const char *string)
{
	char		*escaped_string;
	int			error;

	escaped_string = pg_malloc0(MAXLEN);

	(void) PQescapeStringConn(conn, escaped_string, string, MAXLEN, &error);

	if (error)
	{
		pfree(escaped_string);
		return NULL;
	}

	return escaped_string;
}


char *
string_skip_prefix(const char *prefix, char *string)
{
	int n;

	n = strlen(prefix);

	if (strncmp(prefix, string, n))
		return NULL;
	else
		return string + n;
}


char *
string_remove_trailing_newlines(char *string)
{
	int n;

	n = strlen(string) - 1;

	while (n >= 0 && string[n] == '\n')
		string[n] = 0;

	return string;
}


char *
trim(char *s)
{
	/* Initialize start, end pointers */
	char	   *s1 = s,
			   *s2 = &s[strlen(s) - 1];

	/* If string is empty, no action needed */
	if (s2 < s1)
		return s;

	/* Trim and delimit right side */
	while ((isspace(*s2)) && (s2 >= s1))
		--s2;
	*(s2 + 1) = '\0';

	/* Trim left side */
	while ((isspace(*s1)) && (s1 < s2))
		++s1;

	/* Copy finished string */
	memmove(s, s1, s2 - s1);
	s[s2 - s1 + 1] = '\0';

	return s;
}
