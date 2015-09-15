/*
 * log.c - Logging methods
 * Copyright (C) 2ndQuadrant, 2010-2015
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
#endif

#include <stdarg.h>
#include <time.h>

#include "log.h"

#define DEFAULT_IDENT "repmgr"
#ifdef HAVE_SYSLOG
#define DEFAULT_SYSLOG_FACILITY LOG_LOCAL0
#endif

/* #define REPMGR_DEBUG */

void
stderr_log_with_level(const char *level_name, int level, const char *fmt, ...)
{
	time_t		t;
	struct tm  *tm;
	char		buff[100];
	va_list		ap;

	if (log_level >= level)
	{
		time(&t);
		tm = localtime(&t);
		strftime(buff, 100, "[%Y-%m-%d %H:%M:%S]", tm);
		fprintf(stderr, "%s [%s] ", buff, level_name);

		va_start(ap, fmt);
		vfprintf(stderr, fmt, ap);
		va_end(ap);

		fflush(stderr);
	}
}


static int	detect_log_level(const char *level);
static int	detect_log_facility(const char *facility);

int			log_type = REPMGR_STDERR;
int			log_level = LOG_NOTICE;

bool
logger_init(t_configuration_options * opts, const char *ident, const char *level, const char *facility)
{
	int			l;
	int			f;

#ifdef HAVE_SYSLOG
	int			syslog_facility = DEFAULT_SYSLOG_FACILITY;
#endif

#ifdef REPMGR_DEBUG
	printf("Logger initialisation (Level: %s, Facility: %s)\n", level, facility);
#endif

	if (!ident)
	{
		ident = DEFAULT_IDENT;
	}

	if (level && *level)
	{
		l = detect_log_level(level);
#ifdef REPMGR_DEBUG
		printf("Assigned level for logger: %d\n", l);
#endif

		if (l > 0)
			log_level = l;
		else
			stderr_log_warning(_("Cannot detect log level %s (use any of DEBUG, INFO, NOTICE, WARNING, ERR, ALERT, CRIT or EMERG)\n"), level);
	}

	if (facility && *facility)
	{

		f = detect_log_facility(facility);
#ifdef REPMGR_DEBUG
		printf("Assigned facility for logger: %d\n", f);
#endif

		if (f == 0)
		{
			/* No syslog requested, just stderr */
#ifdef REPMGR_DEBUG
			printf(_("Use stderr for logging\n"));
#endif
		}
		else if (f == -1)
		{
			stderr_log_warning(_("Cannot detect log facility %s (use any of LOCAL0, LOCAL1, ..., LOCAL7, USER or STDERR)\n"), facility);
		}
#ifdef HAVE_SYSLOG
		else
		{
			syslog_facility = f;
			log_type = REPMGR_SYSLOG;
		}
#endif
	}

#ifdef HAVE_SYSLOG

	if (log_type == REPMGR_SYSLOG)
	{
		setlogmask(LOG_UPTO(log_level));
		openlog(ident, LOG_CONS | LOG_PID | LOG_NDELAY, syslog_facility);

		stderr_log_notice(_("Setup syslog (level: %s, facility: %s)\n"), level, facility);
	}
#endif

	if (*opts->logfile)
	{
		FILE	   *fd;

		/* Check if we can write to the specified file before redirecting
		 * stderr - if freopen() fails, stderr output will vanish into
		 * the ether and the user won't know what's going on.
		 */

		fd = fopen(opts->logfile, "a");
		if (fd == NULL)
		{
			stderr_log_err(_("Unable to open specified logfile '%s' for writing: %s\n"), opts->logfile, strerror(errno));
			stderr_log_err(_("Terminating\n"));
			exit(ERR_BAD_CONFIG);
		}
		fclose(fd);

		stderr_log_notice(_("Redirecting logging output to '%s'\n"), opts->logfile);
		fd = freopen(opts->logfile, "a", stderr);

		/* It's possible freopen() may still fail due to e.g. a race condition;
		   as it's not feasible to restore stderr after a failed freopen(),
		   we'll write to stdout as a last resort.
		 */
		if (fd == NULL)
		{
			printf(_("Unable to open specified logfile %s for writing: %s\n"), opts->logfile, strerror(errno));
			printf(_("Terminating\n"));
			exit(ERR_BAD_CONFIG);
		}
	}

	return true;

}

bool
logger_shutdown(void)
{
#ifdef HAVE_SYSLOG
	if (log_type == REPMGR_SYSLOG)
		closelog();
#endif

	return true;
}

/*
 * Set a minimum logging level.  Intended for command line verbosity
 * options, which might increase requested logging over what's specified
 * in the regular configuration file.
 */
void
logger_min_verbose(int minimum)
{
	if (log_level < minimum)
		log_level = minimum;
}

int
detect_log_level(const char *level)
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

int
detect_log_facility(const char *facility)
{
	int			local = 0;

	if (!strncmp(facility, "LOCAL", 5) && strlen(facility) == 6)
	{

		local = atoi(&facility[5]);

		switch (local)
		{
			case 0:
				return LOG_LOCAL0;
				break;
			case 1:
				return LOG_LOCAL1;
				break;
			case 2:
				return LOG_LOCAL2;
				break;
			case 3:
				return LOG_LOCAL3;
				break;
			case 4:
				return LOG_LOCAL4;
				break;
			case 5:
				return LOG_LOCAL5;
				break;
			case 6:
				return LOG_LOCAL6;
				break;
			case 7:
				return LOG_LOCAL7;
				break;
		}

	}
	else if (!strcmp(facility, "USER"))
	{
		return LOG_USER;
	}
	else if (!strcmp(facility, "STDERR"))
	{
		return 0;
	}

	return -1;
}
