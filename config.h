/*
 * config.h
 * Copyright (c) 2ndQuadrant, 2010-2015
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

#ifndef _REPMGR_CONFIG_H_
#define _REPMGR_CONFIG_H_

#include "repmgr.h"
#include "strutil.h"

typedef struct
{
	char		cluster_name[MAXLEN];
	int			node;
	char		conninfo[MAXLEN];
	int			failover;
	int			priority;
	char		node_name[MAXLEN];
	char		promote_command[MAXLEN];
	char		follow_command[MAXLEN];
	char		gateway[MAXLEN];
	char		loglevel[MAXLEN];
	char		logfacility[MAXLEN];
	char		rsync_options[QUERY_STR_LEN];
	char		ssh_options[QUERY_STR_LEN];
	int			master_response_timeout;
	int			reconnect_attempts;
	int			reconnect_intvl;
	char		pg_bindir[MAXLEN];
	char		pgctl_options[MAXLEN];
	char		logfile[MAXLEN];
	int			monitor_interval_secs;
	int			retry_promote_interval_secs;
}	t_configuration_options;

#define T_CONFIGURATION_OPTIONS_INITIALIZER { "", -1, "", MANUAL_FAILOVER, -1, "", "", "", "", "", "", "", "", -1, -1, -1, "", "", "", 0, 0 }

void		parse_config(const char *config_file, t_configuration_options * options);
void		parse_line(char *buff, char *name, char *value);
char	   *trim(char *s);
bool		reload_config(char *config_file, t_configuration_options * orig_options);

#endif
