/*
 * strutil.c
 *
 * Copyright (c) Heroku, 2010
 *
 */

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include "strutil.h"

static int xvsnprintf(char *str, size_t size, const char *format, va_list ap);


static int
xvsnprintf(char *str, size_t size, const char *format, va_list ap)
{
	int retval;

	retval = vsnprintf(str, size, format, ap);

	if (retval >= size)
	{
		fprintf(stderr, "Buffer not large enough to format entire string\n");
		exit(255);
	}

	return retval;
}


int
xsnprintf(char *str, size_t size, const char *format, ...)
{
	va_list arglist;
	int retval;

	va_start(arglist, format);
	retval = xvsnprintf(str, size, format, arglist);
	va_end(arglist);

	return retval;
}


int
sqlquery_snprintf(char *str, const char *format, ...)
{
	va_list		arglist;
	int			retval;

	va_start(arglist, format);
	retval = xvsnprintf(str, QUERY_STR_LEN, format, arglist);
	va_end(arglist);

	return retval;
}


int maxlen_snprintf(char *str, const char *format, ...)
{
	va_list		arglist;
	int			retval;

	va_start(arglist, format);
	retval = xvsnprintf(str, MAXLEN, format, arglist);
	va_end(arglist);

	return retval;
}
