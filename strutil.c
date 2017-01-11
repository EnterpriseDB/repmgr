/*
 * strutil.c
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
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
		log_err(_("Buffer of size not large enough to format entire string '%s'\n"),
				str);
		exit(ERR_STR_OVERFLOW);
	}

	return retval;
}


int
xsnprintf(char *str, size_t size, const char *format,...)
{
	va_list		arglist;
	int			retval;

	va_start(arglist, format);
	retval = xvsnprintf(str, size, format, arglist);
	va_end(arglist);

	return retval;
}


int
sqlquery_snprintf(char *str, const char *format,...)
{
	va_list		arglist;
	int			retval;

	va_start(arglist, format);
	retval = xvsnprintf(str, QUERY_STR_LEN, format, arglist);
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
