/*
 * strutil.c
 *
 * Copyright (c) 2ndQuadrant, 2010-2020
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

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include "repmgr.h"
#include "log.h"
#include "strutil.h"

static int
xvsnprintf(char *str, size_t size, const char *format, va_list ap)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 0)));

static void
_key_value_list_set(KeyValueList *item_list, bool replace, const char *key, const char *value);

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
append_where_clause(PQExpBufferData *where_clause, const char *format,...)
{
	va_list		arglist;
	char		stringbuf[MAXLEN];

	va_start(arglist, format);
	(void) xvsnprintf(stringbuf, MAXLEN, format, arglist);
	va_end(arglist);

	if (where_clause->data[0] == '\0')
	{
		appendPQExpBufferStr(where_clause,
							 " WHERE ");
	}
	else
	{
		appendPQExpBufferStr(where_clause,
							 " AND ");
	}

	appendPQExpBufferStr(where_clause,
						 stringbuf);

}


void
item_list_append(ItemList *item_list, const char *message)
{
	item_list_append_format(item_list, "%s", message);
}


void
item_list_append_format(ItemList *item_list, const char *format,...)
{
	ItemListCell *cell;
	va_list		arglist;

	if (item_list == NULL)
		return;

	cell = (ItemListCell *) pg_malloc0(sizeof(ItemListCell));

	if (cell == NULL)
	{
		log_error(_("unable to allocate memory; terminating."));
		exit(ERR_OUT_OF_MEMORY);
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
	_key_value_list_set(item_list, false, key, value);
	return;
}

void
key_value_list_replace_or_set(KeyValueList *item_list, const char *key, const char *value)
{
	_key_value_list_set(item_list, true, key, value);
	return;
}

void
key_value_list_set_format(KeyValueList *item_list, const char *key, const char *value, ...)
{
	va_list		arglist;
	char formatted_value[MAXLEN];

	va_start(arglist, value);
	(void) xvsnprintf(formatted_value, MAXLEN, value, arglist);
	va_end(arglist);

	return _key_value_list_set(item_list, false, key, formatted_value);
}

static void
_key_value_list_set(KeyValueList *item_list, bool replace, const char *key, const char *value)
{
	KeyValueListCell *cell = NULL;
	int			keylen = 0;
	int			vallen = 0;

	if (replace == true)
	{
		KeyValueListCell *prev_cell = NULL;
		KeyValueListCell *next_cell = NULL;


		for (cell = item_list->head; cell; cell = next_cell)
		{
			next_cell = cell->next;

			if (strcmp(cell->key, key) == 0)
			{
				if (item_list->head == cell)
					item_list->head = cell->next;

				if (prev_cell)
				{
					prev_cell->next = cell->next;

					if (item_list->tail == cell)
						item_list->tail = prev_cell;
				}
				else if (item_list->tail == cell)
				{
					item_list->tail = NULL;
				}

				pfree(cell->key);
				pfree(cell->value);
				pfree(cell);
			}
			else
			{
				prev_cell = cell;
			}
		}
	}

	cell = (KeyValueListCell *) pg_malloc0(sizeof(KeyValueListCell));

	if (cell == NULL)
	{
		log_error(_("unable to allocate memory; terminating."));
		exit(ERR_BAD_CONFIG);
	}

	keylen = strlen(key);
	vallen = strlen(value);

	cell->key = pg_malloc0(keylen + 1);
	cell->value = pg_malloc0(vallen + 1);
	cell->output_mode = OM_NOT_SET;

	strncpy(cell->key, key, keylen);
	strncpy(cell->value, value, vallen);

	if (item_list->tail)
		item_list->tail->next = cell;
	else
		item_list->head = cell;

	item_list->tail = cell;

	return;
}


void
key_value_list_set_output_mode(KeyValueList *item_list, const char *key, OutputMode mode)
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
check_status_list_set_format(CheckStatusList *list, const char *item, CheckStatus status, const char *details,...)
{
	CheckStatusListCell *cell;
	va_list		arglist;
	int			itemlen;

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
	char	   *escaped_string;
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


/*
 * simple function to escape double quotes only
 */
void
escape_double_quotes(char *string, PQExpBufferData *out)
{
	char *ptr;

	for (ptr = string; *ptr; ptr++)
	{
		if (*ptr == '"')
		{
			if ( (ptr == string) || (ptr > string && *(ptr - 1) != '\\'))
			{
				appendPQExpBufferChar(out, '\\');
			}
		}
		appendPQExpBufferChar(out, *ptr);
	}

	return;
}


char *
string_skip_prefix(const char *prefix, char *string)
{
	int			n;

	n = strlen(prefix);

	if (strncmp(prefix, string, n))
		return NULL;
	else
		return string + n;
}


char *
string_remove_trailing_newlines(char *string)
{
	int			n;

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

	/* String is all whitespace - no need for further processing */
	if (s2 + 1 == s1)
		return s;

	/* Trim left side */
	while ((isspace(*s1)) && (s1 < s2))
		++s1;

	/* Copy finished string */
	memmove(s, s1, (s2 - s1) + 1);
	s[s2 - s1 + 1] = '\0';

	return s;
}


void
parse_follow_command(char *parsed_command, char *template, int node_id)
{
	const char *src_ptr = NULL;
	char	   *dst_ptr = NULL;
	char	   *end_ptr = NULL;

	dst_ptr = parsed_command;
	end_ptr = parsed_command + MAXPGPATH - 1;
	*end_ptr = '\0';

	for (src_ptr = template; *src_ptr; src_ptr++)
	{
		if (*src_ptr == '%')
		{
			switch (src_ptr[1])
			{
				case '%':
					/* %%: replace with % */
					if (dst_ptr < end_ptr)
					{
						src_ptr++;
						*dst_ptr++ = *src_ptr;
					}
					break;
				case 'n':
					/* %n: node id */
					src_ptr++;
					snprintf(dst_ptr, end_ptr - dst_ptr, "%i", node_id);
					dst_ptr += strlen(dst_ptr);
					break;
				default:
					/* otherwise treat the % as not special */
					if (dst_ptr < end_ptr)
						*dst_ptr++ = *src_ptr;
					break;
			}
		}
		else
		{
			if (dst_ptr < end_ptr)
				*dst_ptr++ = *src_ptr;
		}
	}

	*dst_ptr = '\0';

	return;
}


const char *
format_bool(bool value)
{
	return value == true ? "true" : "false";
}
