/*
 * log.c - Logging methods
 * Copyright (c) EnterpriseDB Corporation, 2010-2021
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

static int	detect_log_facility(const char *facility);
static void
_stderr_log_with_level(const char *level_name, int level, const char *fmt, va_list ap)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 0)));

int			log_type = REPMGR_STDERR;
int			log_level = LOG_INFO;
int			last_log_level = LOG_INFO;
int			verbose_logging = false;
int			terse_logging = false;

/*
 * Global variable to be set by the main application to ensure any log output
 * emitted before logger_init is called, is output in the correct format
 */
int			logger_output_mode = OM_DAEMON;

extern void
stderr_log_with_level(const char *level_name, int level, const char *fmt,...)
{
	va_list		arglist;

	va_start(arglist, fmt);
	_stderr_log_with_level(level_name, level, fmt, arglist);
	va_end(arglist);
}

static void
_stderr_log_with_level(const char *level_name, int level, const char *fmt, va_list ap)
{
	char		buf[100];

	/*
	 * Store the requested level so that if there's a subsequent log_hint() or
	 * log_detail(), we can suppress that if --terse was specified,
	 */
	last_log_level = level;

	if (log_level >= level)
	{

		/* Format log line prefix with timestamp if in daemon mode */
		if (logger_output_mode == OM_DAEMON)
		{
			time_t		t;
			struct tm  *tm;

			time(&t);
			tm = localtime(&t);
			strftime(buf, sizeof(buf), "[%Y-%m-%d %H:%M:%S]", tm);
			fprintf(stderr, "%s [%s] ", buf, level_name);
		}
		else
		{
			fprintf(stderr, "%s: ", level_name);
		}

		vfprintf(stderr, fmt, ap);
		fprintf(stderr, "\n");
		fflush(stderr);
	}
}

void
log_hint(const char *fmt,...)
{
	va_list		ap;

	if (terse_logging == false)
	{
		va_start(ap, fmt);
		_stderr_log_with_level("HINT", last_log_level, fmt, ap);
		va_end(ap);
	}
}


void
log_detail(const char *fmt,...)
{
	va_list		ap;

	if (terse_logging == false)
	{
		va_start(ap, fmt);
		_stderr_log_with_level("DETAIL", last_log_level, fmt, ap);
		va_end(ap);
	}
}


void
log_verbose(int level, const char *fmt,...)
{
	va_list		ap;

	va_start(ap, fmt);

	if (verbose_logging == true)
	{
		switch (level)
		{
			case LOG_EMERG:
				_stderr_log_with_level("EMERG", level, fmt, ap);
				break;
			case LOG_ALERT:
				_stderr_log_with_level("ALERT", level, fmt, ap);
				break;
			case LOG_CRIT:
				_stderr_log_with_level("CRIT", level, fmt, ap);
				break;
			case LOG_ERROR:
				_stderr_log_with_level("ERROR", level, fmt, ap);
				break;
			case LOG_WARNING:
				_stderr_log_with_level("WARNING", level, fmt, ap);
				break;
			case LOG_NOTICE:
				_stderr_log_with_level("NOTICE", level, fmt, ap);
				break;
			case LOG_INFO:
				_stderr_log_with_level("INFO", level, fmt, ap);
				break;
			case LOG_DEBUG:
				_stderr_log_with_level("DEBUG", level, fmt, ap);
				break;
		}
	}

	va_end(ap);
}


bool
logger_init(t_configuration_options *opts, const char *ident)
{
	char	   *level = opts->log_level;
	char	   *facility = opts->log_facility;

	int			l;
	int			f;

#ifdef HAVE_SYSLOG
	int			syslog_facility = DEFAULT_SYSLOG_FACILITY;
#endif

#ifdef REPMGR_DEBUG
	printf("logger initialisation (Level: %s, Facility: %s)\n", level, facility);
#endif

	if (!ident)
	{
		ident = DEFAULT_IDENT;
	}

	if (level && *level)
	{
		l = detect_log_level(level);
#ifdef REPMGR_DEBUG
		printf("assigned level for logger: %d\n", l);
#endif

		if (l >= 0)
			log_level = l;
		else
			stderr_log_warning(_("invalid log level \"%s\" (available values: DEBUG, INFO, NOTICE, WARNING, ERR, ALERT, CRIT or EMERG)\n"), level);
	}

	/*
	 * STDERR only logging requested - finish here without setting up any
	 * further logging facility.
	 */
	if (logger_output_mode == OM_COMMAND_LINE)
		return true;

	if (facility && *facility)
	{

		f = detect_log_facility(facility);
#ifdef REPMGR_DEBUG
		printf("assigned facility for logger: %d\n", f);
#endif

		if (f == 0)
		{
			/* No syslog requested, just stderr */
#ifdef REPMGR_DEBUG
			printf(_("using stderr for logging\n"));
#endif
		}
		else if (f == -1)
		{
			stderr_log_warning(_("cannot detect log facility %s (use any of LOCAL0, LOCAL1, ..., LOCAL7, USER or STDERR)\n"), facility);
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

		stderr_log_notice(_("setup syslog (level: %s, facility: %s)\n"), level, facility);
	}
#endif

	if (*opts->log_file)
	{
		FILE	   *fd;

		/*
		 * Check if we can write to the specified file before redirecting
		 * stderr - if freopen() fails, stderr output will vanish into the
		 * ether and the user won't know what's going on.
		 */

		fd = fopen(opts->log_file, "a");
		if (fd == NULL)
		{
			stderr_log_error(_("unable to open specified log file \"%s\" for writing: %s\n"),
							 opts->log_file, strerror(errno));
			stderr_log_error(_("Terminating\n"));
			exit(ERR_BAD_CONFIG);
		}
		fclose(fd);

		stderr_log_notice(_("redirecting logging output to \"%s\"\n"), opts->log_file);
		fd = freopen(opts->log_file, "a", stderr);

		/*
		 * It's possible freopen() may still fail due to e.g. a race
		 * condition; as it's not feasible to restore stderr after a failed
		 * freopen(), we'll write to stdout as a last resort.
		 */
		if (fd == NULL)
		{
			printf(_("unable to open specified log file %s for writing: %s\n"), opts->log_file, strerror(errno));
			printf(_("terminating\n"));
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
 * Indicate whether extra-verbose logging is required. This will
 * generate a lot of output, particularly debug logging, and should
 * not be permanently enabled in production.
 *
 * NOTE: in previous repmgr versions, this option forced the log
 * level to INFO.
 */
void
logger_set_verbose(void)
{
	verbose_logging = true;
}


/*
 * Indicate whether some non-critical log messages can be omitted.
 * Currently this includes warnings about irrelevant command line
 * options and hints.
 */

void
logger_set_terse(void)
{
	terse_logging = true;
}


void
logger_set_level(int new_log_level)
{
	log_level = new_log_level;
}


void
logger_set_min_level(int min_log_level)
{
	if (min_log_level > log_level)
		log_level = min_log_level;
}


int
detect_log_level(const char *level)
{
	if (!strcasecmp(level, "DEBUG"))
		return LOG_DEBUG;
	if (!strcasecmp(level, "INFO"))
		return LOG_INFO;
	if (!strcasecmp(level, "NOTICE"))
		return LOG_NOTICE;
	if (!strcasecmp(level, "WARNING"))
		return LOG_WARNING;
	if (!strcasecmp(level, "ERROR"))
		return LOG_ERROR;
	if (!strcasecmp(level, "ALERT"))
		return LOG_ALERT;
	if (!strcasecmp(level, "CRIT"))
		return LOG_CRIT;
	if (!strcasecmp(level, "EMERG"))
		return LOG_EMERG;

	return -1;
}

static int
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
