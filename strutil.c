/*
 * strutil.c
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>



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
	va_list		arglist;

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
