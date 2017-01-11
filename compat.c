/*
 *
 * compat.c
 *	  Provides a couple of useful string utility functions adapted
 *	  from the backend code, which are not publicly exposed. They're
 *	  unlikely to change but it would be worth keeping an eye on them
 *	  for any fixes/improvements
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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

#include "repmgr.h"
#include "compat.h"

/*
 * Append the given string to the buffer, with suitable quoting for passing
 * the string as a value, in a keyword/pair value in a libpq connection
 * string
 *
 * This function is adapted from src/fe_utils/string_utils.c (before 9.6
 * located in: src/bin/pg_dump/dumputils.c)
 */
void
appendConnStrVal(PQExpBuffer buf, const char *str)
{
	const char *s;
	bool		needquotes;

	/*
	 * If the string is one or more plain ASCII characters, no need to quote
	 * it. This is quite conservative, but better safe than sorry.
	 */
	needquotes = true;
	for (s = str; *s; s++)
	{
		if (!((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') ||
			  (*s >= '0' && *s <= '9') || *s == '_' || *s == '.'))
		{
			needquotes = true;
			break;
		}
		needquotes = false;
	}

	if (needquotes)
	{
		appendPQExpBufferChar(buf, '\'');
		while (*str)
		{
			/* ' and \ must be escaped by to \' and \\ */
			if (*str == '\'' || *str == '\\')
				appendPQExpBufferChar(buf, '\\');

			appendPQExpBufferChar(buf, *str);
			str++;
		}
		appendPQExpBufferChar(buf, '\'');
	}
	else
		appendPQExpBufferStr(buf, str);
}

/*
 * Adapted from: src/fe_utils/string_utils.c
 */
void
appendShellString(PQExpBuffer buf, const char *str)
{
	const char *p;

	appendPQExpBufferChar(buf, '\'');
	for (p = str; *p; p++)
	{
		if (*p == '\n' || *p == '\r')
		{
			fprintf(stderr,
					_("shell command argument contains a newline or carriage return: \"%s\"\n"),
					str);
			exit(ERR_BAD_CONFIG);
		}

		if (*p == '\'')
			appendPQExpBufferStr(buf, "'\"'\"'");
		else
			appendPQExpBufferChar(buf, *p);
	}

	appendPQExpBufferChar(buf, '\'');
}

