/*
 * log.c - Logging methods
 * Copyright (C) 2ndQuadrant, 2010
 *
 * This module is a set of methods for logging (currently only syslog)
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

#include <stdlib.h>

#ifdef HAVE_SYSLOG
#include <syslog.h>
#include <stdarg.h>
#endif

#include "log.h"

#define DEFAULT_IDENT "repmgr"
#ifdef HAVE_SYSLOG
#define DEFAULT_SYSLOG_FACILITY LOG_LOCAL0
#endif

static int detect_log_level(const char* level);
static int detect_log_facility(const char* facility);

int log_type = REPMGR_STDERR;
int log_level = LOG_NOTICE;

bool logger_init(const char* ident, const char* level, const char* facility)
{

	int l;
	int f;

#ifdef HAVE_SYSLOG
	int syslog_facility = DEFAULT_SYSLOG_FACILITY;
#endif
printf("Logger init: detect stuff: %s, %s\n", level, facility);

	if (!ident) {
		ident = DEFAULT_IDENT;
	}

	if (level) {
		l = detect_log_level(level);
printf("Logger level: %d\n", l);

		if (l > 0)
			log_level = l;
		else
			stderr_log_warning(_("Cannot detect log level %s (use any of DEBUG, INFO, NOTICE, WARNING, ERR, ALERT, CRIT or EMERG)"), level);
	}

	if (facility) {
		f = detect_log_facility(facility);
printf("Logger facility: %d\n", f);
		if (f == 0) {
			/* No syslog requested, just stderr */
			stderr_log_notice(_("Use stderr for logging"));
			return true;
		}
		else if (f == -1) {
			stderr_log_warning(_("Cannot detect log facility %s (use any of LOCAL0, LOCAL1, ..., LOCAL7, USER or STDERR)"), facility);
		}
#ifdef HAVE_SYSLOG
		else {
			syslog_facility = f;
		}
#endif
	}

#ifdef HAVE_SYSLOG

	setlogmask (LOG_UPTO (log_level));
	openlog (ident, LOG_CONS | LOG_PID | LOG_NDELAY, syslog_facility);

	log_type = REPMGR_SYSLOG;
	stderr_log_notice(_("Setup syslog (level: %s, facility: %s)"), level, facility);

#endif

	return true;

}

bool logger_shutdown(void)
{

#ifdef HAVE_SYSLOG
	if (log_type == REPMGR_SYSLOG)
		closelog();
#endif

	return true;
}

int detect_log_level(const char* level)
{
	if (!strcmp(level, "DEBUG"))
		return LOG_DEBUG;
	if (!strcmp(level, "INFO"))
		return LOG_INFO;
	if (!strcmp(level, "NOTICE"))
		return LOG_NOTICE;
	if (!strcmp(level, "WARNING"))
		return LOG_WARNING;
	if (!strcmp(level, "ERR"))
		return LOG_ERR;
	if (!strcmp(level, "ALERT"))
		return LOG_ALERT;
	if (!strcmp(level, "CRIT"))
		return LOG_CRIT;
	if (!strcmp(level, "EMERG"))
		return LOG_EMERG;

	return 0;
}

int detect_log_facility(const char* facility)
{
	int local = 0;
	if (!strncmp(facility, "LOCAL", 5) && strlen(facility) == 6) {

		local = atoi (&facility[5]);

		if (local == 0)
			return LOG_LOCAL0;
		if (local == 1)
			return LOG_LOCAL1;
		if (local == 2)
			return LOG_LOCAL2;
		if (local == 3)
			return LOG_LOCAL3;
		if (local == 4)
			return LOG_LOCAL4;
		if (local == 5)
			return LOG_LOCAL5;
		if (local == 6)
			return LOG_LOCAL6;
		if (local == 7)
			return LOG_LOCAL7;

	}
	else if (!strcmp(facility, "USER")) {
		return LOG_USER;
	}
	else if (!strcmp(facility, "STDERR")) {
		return 0;
	}

	return -1;
}
