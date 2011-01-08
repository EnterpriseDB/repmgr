/*
 * config.h
 * Copyright (c) 2ndQuadrant, 2010
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

typedef struct
{
    char cluster_name[MAXLEN];
    int node;
    char conninfo[MAXLEN];
    char rsync_options[QUERY_STR_LEN];
} repmgr_config;

void parse_config(const char *config_file, repmgr_config *config);
void parse_line(char *buff, char *name, char *value);
char *trim(char *s);
