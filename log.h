/*
 * log.h
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

#ifndef _REPMGR_LOG_H_
#define _REPMGR_LOG_H_

#include "repmgr.h"

#define REPMGR_SYSLOG 1
#define REPMGR_STDERR 2

#define OM_COMMAND_LINE 1
#define OM_DAEMON       2

extern void
stderr_log_with_level(const char *level_name, int level, const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));

/* Standard error logging */
#define stderr_log_debug(...) stderr_log_with_level("DEBUG", LOG_DEBUG, __VA_ARGS__)
#define stderr_log_info(...)  stderr_log_with_level("INFO", LOG_INFO, __VA_ARGS__)
#define stderr_log_notice(...) stderr_log_with_level("NOTICE", LOG_NOTICE, __VA_ARGS__)
#define stderr_log_warning(...) stderr_log_with_level("WARNING", LOG_WARNING, __VA_ARGS__)
#define stderr_log_err(...) stderr_log_with_level("ERROR", LOG_ERR, __VA_ARGS__)
#define stderr_log_crit(...) stderr_log_with_level("CRITICAL", LOG_CRIT, __VA_ARGS__)
#define stderr_log_alert(...) stderr_log_with_level("ALERT", LOG_ALERT, __VA_ARGS__)
#define stderr_log_emerg(...) stderr_log_with_level("EMERGENCY", LOG_EMERG, __VA_ARGS__)

#ifdef HAVE_SYSLOG

#include <syslog.h>

#define log_debug(...) \
	if (log_type == REPMGR_SYSLOG) \
		syslog(LOG_DEBUG, __VA_ARGS__); \
	else \
		stderr_log_debug(__VA_ARGS__);

#define log_info(...) \
	{ \
		if (log_type == REPMGR_SYSLOG) syslog(LOG_INFO, __VA_ARGS__); \
		else stderr_log_info(__VA_ARGS__); \
	}

#define log_notice(...) \
	{ \
		if (log_type == REPMGR_SYSLOG) syslog(LOG_NOTICE, __VA_ARGS__); \
		else stderr_log_notice(__VA_ARGS__); \
	}

#define log_warning(...) \
	{ \
		if (log_type == REPMGR_SYSLOG) syslog(LOG_WARNING, __VA_ARGS__); \
		else stderr_log_warning(__VA_ARGS__); \
	}

#define log_err(...) \
	{ \
		if (log_type == REPMGR_SYSLOG) syslog(LOG_ERR, __VA_ARGS__); \
		else stderr_log_err(__VA_ARGS__); \
	}

#define log_crit(...) \
	{ \
		if (log_type == REPMGR_SYSLOG) syslog(LOG_CRIT, __VA_ARGS__); \
		else stderr_log_crit(__VA_ARGS__); \
	}

#define log_alert(...) \
	{ \
		if (log_type == REPMGR_SYSLOG) syslog(LOG_ALERT, __VA_ARGS__); \
		else stderr_log_alert(__VA_ARGS__); \
	}

#define log_emerg(...) \
	{ \
		if (log_type == REPMGR_SYSLOG) syslog(LOG_ALERT, __VA_ARGS__); \
		else stderr_log_alert(__VA_ARGS__); \
	}
#else

#define LOG_EMERG	0			/* system is unusable */
#define LOG_ALERT	1			/* action must be taken immediately */
#define LOG_CRIT	2			/* critical conditions */
#define LOG_ERR		3			/* error conditions */
#define LOG_WARNING 4			/* warning conditions */
#define LOG_NOTICE	5			/* normal but significant condition */
#define LOG_INFO	6			/* informational */
#define LOG_DEBUG	7			/* debug-level messages */

#define log_debug(...) stderr_log_debug(__VA_ARGS__)
#define log_info(...) stderr_log_info(__VA_ARGS__)
#define log_notice(...) stderr_log_notice(__VA_ARGS__)
#define log_warning(...) stderr_log_warning(__VA_ARGS__)
#define log_err(...) stderr_log_err(__VA_ARGS__)
#define log_crit(...) stderr_log_crit(__VA_ARGS__)
#define log_alert(...) stderr_log_alert(__VA_ARGS__)
#define log_emerg(...) stderr_log_emerg(__VA_ARGS__)
#endif


int			detect_log_level(const char *level);

/* Logger initialisation and shutdown */

bool		logger_init(t_configuration_options * opts, const char *ident);

bool		logger_shutdown(void);

void		logger_set_verbose(void);
void		logger_set_terse(void);

void		log_detail(const char *fmt, ...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));
void		log_hint(const char *fmt, ...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));
void		log_verbose(int level, const char *fmt, ...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

extern int	log_type;
extern int	log_level;
extern int	verbose_logging;
extern int	terse_logging;
extern int	logger_output_mode;

#endif /* _REPMGR_LOG_H_ */
