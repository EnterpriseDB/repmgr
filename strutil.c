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
		log_error(_("buffer of size not large enough to format entire string '%s'"),
				  str);
		exit(ERR_STR_OVERFLOW);
	}

	return retval;
}


int
sqlquery_snprintf(char *str, const char *format,...)
{
	va_list		arglist;
	int			retval;

	va_start(arglist, format);
	retval = xvsnprintf(str, MAX_QUERY_LEN, format, arglist);
	va_end(arglist);

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
