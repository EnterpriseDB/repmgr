/*
 * config.c - parse repmgr.conf and other configuration-related functionality
 *
 * Copyright (c) 2ndQuadrant, 2010-2019
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

#include <sys/stat.h>			/* for stat() */

#include "repmgr.h"
#include "configfile.h"
#include "log.h"

const static char *_progname = NULL;
char		config_file_path[MAXPGPATH] = "";
static bool config_file_provided = false;
bool		config_file_found = false;

static void parse_config(t_configuration_options *options, bool terse);
static void _parse_config(t_configuration_options *options, ItemList *error_list, ItemList *warning_list);

static void _parse_line(char *buf, char *name, char *value);
static void parse_event_notifications_list(t_configuration_options *options, const char *arg);
static void clear_event_notification_list(t_configuration_options *options);

static void parse_time_unit_parameter(const char *name, const char *value, char *dest, ItemList *errors);

static void tablespace_list_append(t_configuration_options *options, const char *arg);


static void exit_with_config_file_errors(ItemList *config_errors, ItemList *config_warnings, bool terse);


void
set_progname(const char *argv0)
{
	_progname = get_progname(argv0);
}

const char *
progname(void)
{
	return _progname;
}

void
load_config(const char *config_file, bool verbose, bool terse, t_configuration_options *options, char *argv0)
{
	struct stat stat_config;

	/*
	 * If a configuration file was provided, check it exists, otherwise emit
	 * an error and terminate. We assume that if a user explicitly provides a
	 * configuration file, they'll want to make sure it's used and not fall
	 * back to any of the defaults.
	 */
	if (config_file != NULL && config_file[0] != '\0')
	{
		strncpy(config_file_path, config_file, MAXPGPATH);
		canonicalize_path(config_file_path);

		/* relative path supplied - convert to absolute path */
		if (config_file_path[0] != '/')
		{
			PQExpBufferData fullpath;
			char *pwd = NULL;

			initPQExpBuffer(&fullpath);

			/*
			 * we'll attempt to use $PWD to derive the effective path; getcwd()
			 * will likely resolve symlinks, which may result in a path which
			 * isn't permanent (e.g. if filesystem mountpoints change).
			 */
			pwd = getenv("PWD");

			if (pwd != NULL)
			{
				appendPQExpBufferStr(&fullpath, pwd);
			}
			else
			{
				/* $PWD not available - fall back to getcwd() */
				char cwd[MAXPGPATH] = "";

				if (getcwd(cwd, MAXPGPATH) == NULL)
				{
					log_error(_("unable to execute getcwd()"));
					log_detail("%s", strerror(errno));

					termPQExpBuffer(&fullpath);
					exit(ERR_BAD_CONFIG);
				}

				appendPQExpBufferStr(&fullpath, cwd);
			}

			appendPQExpBuffer(&fullpath,
							  "/%s", config_file_path);

			log_debug("relative configuration file converted to:\n  \"%s\"",
					  fullpath.data);

			strncpy(config_file_path, fullpath.data, MAXPGPATH);

			termPQExpBuffer(&fullpath);

			canonicalize_path(config_file_path);
		}


		if (stat(config_file_path, &stat_config) != 0)
		{
			log_error(_("provided configuration file \"%s\" not found"),
					  config_file);
			log_detail("%s", strerror(errno));
			exit(ERR_BAD_CONFIG);
		}


		if (verbose == true)
		{
			log_notice(_("using provided configuration file \"%s\""), config_file);
		}

		config_file_provided = true;
		config_file_found = true;
	}


	/*-----------
	 * If no configuration file was provided, attempt to find a default file
	 * in this order:
	 *  - location provided by packager
	 *  - current directory
	 *  - /etc/repmgr.conf
	 *  - default sysconfdir
	 *
	 * here we just check for the existence of the file; parse_config() will
	 * handle read errors etc.
	 *
	 *-----------
	 */
	if (config_file_provided == false)
	{
		/* packagers: if feasible, patch configuration file path into "package_conf_file" */
		char		package_conf_file[MAXPGPATH] = "";
		char		my_exec_path[MAXPGPATH] = "";
		char		sysconf_etc_path[MAXPGPATH] = "";

		/* 1. location provided by packager */
		if (package_conf_file[0] != '\0')
		{
			if (verbose == true)
				fprintf(stdout, _("INFO: checking for package configuration file \"%s\"\n"), package_conf_file);

			if (stat(package_conf_file, &stat_config) == 0)
			{
				strncpy(config_file_path, package_conf_file, MAXPGPATH);
				config_file_found = true;
				goto end_search;
			}
		}

		/* 2 "./repmgr.conf" */
		log_verbose(LOG_INFO, _("looking for configuration file in current directory\n"));

		maxpath_snprintf(config_file_path, "./%s", CONFIG_FILE_NAME);
		canonicalize_path(config_file_path);

		if (stat(config_file_path, &stat_config) == 0)
		{
			config_file_found = true;
			goto end_search;
		}

		/* 3. "/etc/repmgr.conf" */
		if (verbose == true)
			fprintf(stdout, _("INFO: looking for configuration file in /etc\n"));

		maxpath_snprintf(config_file_path, "/etc/%s", CONFIG_FILE_NAME);
		if (stat(config_file_path, &stat_config) == 0)
		{
			config_file_found = true;
			goto end_search;
		}

		/* 4. default sysconfdir */
		if (find_my_exec(argv0, my_exec_path) < 0)
		{
			fprintf(stderr, _("ERROR: %s: could not find own program executable\n"), argv0);
			exit(EXIT_FAILURE);
		}

		get_etc_path(my_exec_path, sysconf_etc_path);

		if (verbose == true)
			fprintf(stdout, _("INFO: looking for configuration file in \"%s\"\n"), sysconf_etc_path);

		maxpath_snprintf(config_file_path, "%s/%s", sysconf_etc_path, CONFIG_FILE_NAME);
		if (stat(config_file_path, &stat_config) == 0)
		{
			config_file_found = true;
			goto end_search;
		}

end_search:
		if (verbose == true)
		{
			if (config_file_found == true)
			{
				fprintf(stdout, _("INFO: configuration file found at: \"%s\"\n"), config_file_path);
			}
			else
			{
				fprintf(stdout, _("INFO: no configuration file provided or found\n"));
			}
		}
	}

	parse_config(options, terse);

	return;
}


static void
parse_config(t_configuration_options *options, bool terse)
{
	/* Collate configuration file errors here for friendlier reporting */
	static ItemList config_errors = {NULL, NULL};
	static ItemList config_warnings = {NULL, NULL};

	_parse_config(options, &config_errors, &config_warnings);

	/* errors found - exit after printing details, and any warnings */
	if (config_errors.head != NULL)
	{
		exit_with_config_file_errors(&config_errors, &config_warnings, terse);
	}

	if (terse == false && config_warnings.head != NULL)
	{
		log_warning(_("the following problems were found in the configuration file:"));

		print_item_list(&config_warnings);
	}

	return;
}


static void
_parse_config(t_configuration_options *options, ItemList *error_list, ItemList *warning_list)
{
	FILE	   *fp;
	char	   *s = NULL,
				buf[MAXLINELENGTH] = "";
	char		name[MAXLEN] = "";
	char		value[MAXLEN] = "";

	bool		node_id_found = false;

	/* Initialize configuration options with sensible defaults */

	/*-----------------
	 * node information
	 *-----------------
	 */
	options->node_id = UNKNOWN_NODE_ID;
	memset(options->node_name, 0, sizeof(options->node_name));
	memset(options->conninfo, 0, sizeof(options->conninfo));
	memset(options->data_directory, 0, sizeof(options->data_directory));
	memset(options->config_directory, 0, sizeof(options->data_directory));
	memset(options->pg_bindir, 0, sizeof(options->pg_bindir));
	memset(options->repmgr_bindir, 0, sizeof(options->repmgr_bindir));
	options->replication_type = REPLICATION_TYPE_PHYSICAL;

	/*-------------
	 * log settings
	 *
	 * note: the default for "log_level" is set in log.c and does not need
	 * to be initialised here
	 *-------------
	 */
	memset(options->log_facility, 0, sizeof(options->log_facility));
	memset(options->log_file, 0, sizeof(options->log_file));
	options->log_status_interval = DEFAULT_LOG_STATUS_INTERVAL;

	/*-----------------------
	 * standby clone settings
	 *------------------------
	 */
	options->use_replication_slots = false;
	memset(options->replication_user, 0, sizeof(options->replication_user));
	memset(options->pg_basebackup_options, 0, sizeof(options->pg_basebackup_options));
	memset(options->restore_command, 0, sizeof(options->restore_command));
	options->tablespace_mapping.head = NULL;
	options->tablespace_mapping.tail = NULL;
	memset(options->recovery_min_apply_delay, 0, sizeof(options->recovery_min_apply_delay));
	options->recovery_min_apply_delay_provided = false;
	memset(options->archive_cleanup_command, 0, sizeof(options->archive_cleanup_command));
	options->use_primary_conninfo_password = false;
	memset(options->passfile, 0, sizeof(options->passfile));

	/*-------------------------
	 * standby promote settings
	 *-------------------------
	 */
	options->promote_check_timeout = DEFAULT_PROMOTE_CHECK_TIMEOUT;
	options->promote_check_interval = DEFAULT_PROMOTE_CHECK_INTERVAL;

	/*------------------------
	 * standby follow settings
	 *------------------------
	 */
	options->primary_follow_timeout = DEFAULT_PRIMARY_FOLLOW_TIMEOUT;
	options->standby_follow_timeout = DEFAULT_STANDBY_FOLLOW_TIMEOUT;

	/*------------------------
	 * standby switchover settings
	 *------------------------
	 */
	options->shutdown_check_timeout = DEFAULT_SHUTDOWN_CHECK_TIMEOUT;
	options->standby_reconnect_timeout = DEFAULT_STANDBY_RECONNECT_TIMEOUT;

	/*-----------------
	 * repmgrd settings
	 *-----------------
	 */
	options->failover = FAILOVER_MANUAL;
	options->priority = DEFAULT_PRIORITY;
	memset(options->location, 0, sizeof(options->location));
	strncpy(options->location, DEFAULT_LOCATION, MAXLEN);
	memset(options->promote_command, 0, sizeof(options->promote_command));
	memset(options->follow_command, 0, sizeof(options->follow_command));
	options->monitor_interval_secs = DEFAULT_MONITORING_INTERVAL;
	/* default to 6 reconnection attempts at intervals of 10 seconds */
	options->reconnect_attempts = DEFAULT_RECONNECTION_ATTEMPTS;
	options->reconnect_interval = DEFAULT_RECONNECTION_INTERVAL;
	options->monitoring_history = false;	/* new in 4.0, replaces
											 * --monitoring-history */
	options->degraded_monitoring_timeout = -1;
	options->async_query_timeout = DEFAULT_ASYNC_QUERY_TIMEOUT;
	options->primary_notification_timeout = DEFAULT_PRIMARY_NOTIFICATION_TIMEOUT;
	options->repmgrd_standby_startup_timeout = -1; /* defaults to "standby_reconnect_timeout" if not set */
	memset(options->repmgrd_pid_file, 0, sizeof(options->repmgrd_pid_file));

	/*-------------
	 * witness settings
	 *-------------
	 */
	options->witness_sync_interval = DEFAULT_WITNESS_SYNC_INTERVAL;

	/*-------------
	 * BDR settings
	 *-------------
	 */
	options->bdr_local_monitoring_only = false;
	options->bdr_recovery_timeout = DEFAULT_BDR_RECOVERY_TIMEOUT;

	/*-----------------
	 * service settings
	 *-----------------
	 */
	memset(options->pg_ctl_options, 0, sizeof(options->pg_ctl_options));
	memset(options->service_stop_command, 0, sizeof(options->service_stop_command));
	memset(options->service_start_command, 0, sizeof(options->service_start_command));
	memset(options->service_restart_command, 0, sizeof(options->service_restart_command));
	memset(options->service_reload_command, 0, sizeof(options->service_reload_command));
	memset(options->service_promote_command, 0, sizeof(options->service_promote_command));

	/*----------------------------
	 * event notification settings
	 *----------------------------
	 */
	memset(options->event_notification_command, 0, sizeof(options->event_notification_command));
	options->event_notifications.head = NULL;
	options->event_notifications.tail = NULL;

	/*----------------
	 * barman settings
	 * ---------------
	 */
	memset(options->barman_host, 0, sizeof(options->barman_host));
	memset(options->barman_server, 0, sizeof(options->barman_server));
	memset(options->barman_config, 0, sizeof(options->barman_config));

	/*-------------------
	 * rsync/ssh settings
	 * ------------------
	 */
	memset(options->rsync_options, 0, sizeof(options->rsync_options));
	memset(options->ssh_options, 0, sizeof(options->ssh_options));
	strncpy(options->ssh_options, "-q -o ConnectTimeout=10", sizeof(options->ssh_options));

	/*---------------------------
	 * undocumented test settings
	 *---------------------------
	 */
	options->promote_delay = 0;

	/*
	 * If no configuration file available (user didn't specify and none found
	 * in the default locations), return with default values
	 */
	if (config_file_found == false)
	{
		log_verbose(LOG_NOTICE,
					_("no configuration file provided and no default file found - "
					  "continuing with default values"));
		return;
	}

	fp = fopen(config_file_path, "r");

	/*
	 * A configuration file has been found, either provided by the user or
	 * found in one of the default locations. If we can't open it, fail with
	 * an error.
	 */
	if (fp == NULL)
	{
		if (config_file_provided)
		{
			log_error(_("unable to open provided configuration file \"%s\"; terminating"),
					  config_file_path);
		}
		else
		{
			log_error(_("unable to open default configuration file	\"%s\"; terminating"),
					  config_file_path);
		}

		exit(ERR_BAD_CONFIG);
	}

	/* Read file */
	while ((s = fgets(buf, sizeof buf, fp)) != NULL)
	{
		bool		known_parameter = true;

		/* Parse name/value pair from line */
		_parse_line(buf, name, value);

		/* Skip blank lines */
		if (!strlen(name))
			continue;

		/* Skip comments */
		if (name[0] == '#')
			continue;

		/* Copy into correct entry in parameters struct */
		if (strcmp(name, "node_id") == 0)
		{
			options->node_id = repmgr_atoi(value, name, error_list, 1);
			node_id_found = true;
		}
		else if (strcmp(name, "node_name") == 0)
			strncpy(options->node_name, value, MAXLEN);
		else if (strcmp(name, "conninfo") == 0)
			strncpy(options->conninfo, value, MAXLEN);
		else if (strcmp(name, "data_directory") == 0)
			strncpy(options->data_directory, value, MAXPGPATH);
		else if (strcmp(name, "config_directory") == 0)
			strncpy(options->config_directory, value, MAXPGPATH);

		else if (strcmp(name, "replication_user") == 0)
		{
			if (strlen(value) < NAMEDATALEN)
				strncpy(options->replication_user, value, NAMEDATALEN);
			else
				item_list_append(error_list,
								 _("value for \"replication_user\" must contain fewer than " STR(NAMEDATALEN) " characters"));
		}
		else if (strcmp(name, "pg_bindir") == 0)
			strncpy(options->pg_bindir, value, MAXPGPATH);
		else if (strcmp(name, "repmgr_bindir") == 0)
			strncpy(options->repmgr_bindir, value, MAXPGPATH);

		else if (strcmp(name, "replication_type") == 0)
		{
			if (strcmp(value, "physical") == 0)
				options->replication_type = REPLICATION_TYPE_PHYSICAL;
			else if (strcmp(value, "bdr") == 0)
				options->replication_type = REPLICATION_TYPE_BDR;
			else
				item_list_append(error_list, _("value for \"replication_type\" must be \"physical\" or \"bdr\""));
		}

		/* log settings */
		else if (strcmp(name, "log_file") == 0)
			strncpy(options->log_file, value, MAXLEN);
		else if (strcmp(name, "log_level") == 0)
			strncpy(options->log_level, value, MAXLEN);
		else if (strcmp(name, "log_facility") == 0)
			strncpy(options->log_facility, value, MAXLEN);
		else if (strcmp(name, "log_status_interval") == 0)
			options->log_status_interval = repmgr_atoi(value, name, error_list, 0);

		/* standby clone settings */
		else if (strcmp(name, "use_replication_slots") == 0)
			options->use_replication_slots = parse_bool(value, name, error_list);
		else if (strcmp(name, "pg_basebackup_options") == 0)
			strncpy(options->pg_basebackup_options, value, MAXLEN);
		else if (strcmp(name, "tablespace_mapping") == 0)
			tablespace_list_append(options, value);
		else if (strcmp(name, "restore_command") == 0)
			strncpy(options->restore_command, value, MAXLEN);
		else if (strcmp(name, "recovery_min_apply_delay") == 0)
		{
			parse_time_unit_parameter(name, value, options->recovery_min_apply_delay, error_list);
			options->recovery_min_apply_delay_provided = true;
		}
		else if (strcmp(name, "archive_cleanup_command") == 0)
			strncpy(options->archive_cleanup_command, value, MAXLEN);
		else if (strcmp(name, "use_primary_conninfo_password") == 0)
			options->use_primary_conninfo_password = parse_bool(value, name, error_list);
		else if (strcmp(name, "passfile") == 0)
			strncpy(options->passfile, value, sizeof(options->passfile));

		/* standby promote settings */
		else if (strcmp(name, "promote_check_timeout") == 0)
			options->promote_check_timeout = repmgr_atoi(value, name, error_list, 1);

		else if (strcmp(name, "promote_check_interval") == 0)
			options->promote_check_interval = repmgr_atoi(value, name, error_list, 1);

		/* standby follow settings */
		else if (strcmp(name, "primary_follow_timeout") == 0)
			options->primary_follow_timeout = repmgr_atoi(value, name, error_list, 0);
		else if (strcmp(name, "standby_follow_timeout") == 0)
			options->standby_follow_timeout = repmgr_atoi(value, name, error_list, 0);

		/* standby switchover settings */
		else if (strcmp(name, "shutdown_check_timeout") == 0)
			options->shutdown_check_timeout = repmgr_atoi(value, name, error_list, 0);
		else if (strcmp(name, "standby_reconnect_timeout") == 0)
			options->standby_reconnect_timeout = repmgr_atoi(value, name, error_list, 0);

		/* node rejoin settings */
		else if (strcmp(name, "node_rejoin_timeout") == 0)
			options->node_rejoin_timeout = repmgr_atoi(value, name, error_list, 0);

		/* node check settings */
		else if (strcmp(name, "archive_ready_warning") == 0)
			options->archive_ready_warning = repmgr_atoi(value, name, error_list, 1);
		else if (strcmp(name, "archive_ready_critical") == 0)
			options->archive_ready_critical = repmgr_atoi(value, name, error_list, 1);
		else if (strcmp(name, "replication_lag_warning") == 0)
			options->replication_lag_warning = repmgr_atoi(value, name, error_list, 1);
		else if (strcmp(name, "replication_lag_critical") == 0)
			options->replication_lag_critical = repmgr_atoi(value, name, error_list, 1);

		/* repmgrd settings */
		else if (strcmp(name, "failover") == 0)
		{
			if (strcmp(value, "manual") == 0)
			{
				options->failover = FAILOVER_MANUAL;
			}
			else if (strcmp(value, "automatic") == 0)
			{
				options->failover = FAILOVER_AUTOMATIC;
			}
			else
			{
				item_list_append(error_list,
								 _("value for \"failover\" must be \"automatic\" or \"manual\"\n"));
			}
		}
		else if (strcmp(name, "priority") == 0)
			options->priority = repmgr_atoi(value, name, error_list, 0);
		else if (strcmp(name, "location") == 0)
			strncpy(options->location, value, MAXLEN);
		else if (strcmp(name, "promote_command") == 0)
			strncpy(options->promote_command, value, MAXLEN);
		else if (strcmp(name, "follow_command") == 0)
			strncpy(options->follow_command, value, MAXLEN);
		else if (strcmp(name, "reconnect_attempts") == 0)
			options->reconnect_attempts = repmgr_atoi(value, name, error_list, 0);
		else if (strcmp(name, "reconnect_interval") == 0)
			options->reconnect_interval = repmgr_atoi(value, name, error_list, 0);
		else if (strcmp(name, "monitor_interval_secs") == 0)
			options->monitor_interval_secs = repmgr_atoi(value, name, error_list, 1);
		else if (strcmp(name, "monitoring_history") == 0)
			options->monitoring_history = parse_bool(value, name, error_list);
		else if (strcmp(name, "degraded_monitoring_timeout") == 0)
			options->degraded_monitoring_timeout = repmgr_atoi(value, name, error_list, -1);
		else if (strcmp(name, "async_query_timeout") == 0)
			options->async_query_timeout = repmgr_atoi(value, name, error_list, 0);
		else if (strcmp(name, "primary_notification_timeout") == 0)
			options->primary_notification_timeout = repmgr_atoi(value, name, error_list, 0);
		else if (strcmp(name, "repmgrd_standby_startup_timeout") == 0)
			options->repmgrd_standby_startup_timeout = repmgr_atoi(value, name, error_list, 0);
		else if (strcmp(name, "repmgrd_pid_file") == 0)
			strncpy(options->repmgrd_pid_file, value, MAXPGPATH);

		/* witness settings */
		else if (strcmp(name, "witness_sync_interval") == 0)
			options->witness_sync_interval = repmgr_atoi(value, name, error_list, 1);

		/* BDR settings */
		else if (strcmp(name, "bdr_local_monitoring_only") == 0)
			options->bdr_local_monitoring_only = parse_bool(value, name, error_list);
		else if (strcmp(name, "bdr_recovery_timeout") == 0)
			options->bdr_recovery_timeout = repmgr_atoi(value, name, error_list, 0);

		/* service settings */
		else if (strcmp(name, "pg_ctl_options") == 0)
			strncpy(options->pg_ctl_options, value, MAXLEN);
		else if (strcmp(name, "service_stop_command") == 0)
			strncpy(options->service_stop_command, value, MAXLEN);
		else if (strcmp(name, "service_start_command") == 0)
			strncpy(options->service_start_command, value, MAXLEN);
		else if (strcmp(name, "service_restart_command") == 0)
			strncpy(options->service_restart_command, value, MAXLEN);
		else if (strcmp(name, "service_reload_command") == 0)
			strncpy(options->service_reload_command, value, MAXLEN);
		else if (strcmp(name, "service_promote_command") == 0)
			strncpy(options->service_promote_command, value, MAXLEN);

		/* event notification settings */
		else if (strcmp(name, "event_notification_command") == 0)
			strncpy(options->event_notification_command, value, MAXLEN);
		else if (strcmp(name, "event_notifications") == 0)
		{
			/* store unparsed value for comparison when reloading config */
			strncpy(options->event_notifications_orig, value, MAXLEN);
			parse_event_notifications_list(options, value);
		}

		/* barman settings */
		else if (strcmp(name, "barman_host") == 0)
			strncpy(options->barman_host, value, MAXLEN);
		else if (strcmp(name, "barman_server") == 0)
			strncpy(options->barman_server, value, MAXLEN);
		else if (strcmp(name, "barman_config") == 0)
			strncpy(options->barman_config, value, MAXLEN);

		/* rsync/ssh settings */
		else if (strcmp(name, "rsync_options") == 0)
			strncpy(options->rsync_options, value, MAXLEN);
		else if (strcmp(name, "ssh_options") == 0)
			strncpy(options->ssh_options, value, MAXLEN);

		/* undocumented settings for testing */
		else if (strcmp(name, "promote_delay") == 0)
			options->promote_delay = repmgr_atoi(value, name, error_list, 1);

		/*
		 * Following parameters have been deprecated or renamed from 3.x -
		 * issue a warning
		 */
		else if (strcmp(name, "cluster") == 0)
		{
			item_list_append(warning_list,
							 _("parameter \"cluster\" is deprecated and will be ignored"));
			known_parameter = false;
		}
		else if (strcmp(name, "node") == 0)
		{
			item_list_append(warning_list,
							 _("parameter \"node\" has been renamed to \"node_id\""));
			known_parameter = false;
		}
		else if (strcmp(name, "upstream_node") == 0)
		{
			item_list_append(warning_list,
							 _("parameter \"upstream_node\" has been removed; use \"--upstream-node-id\" when cloning a standby"));
			known_parameter = false;
		}
		else if (strcmp(name, "loglevel") == 0)
		{
			item_list_append(warning_list,
							 _("parameter \"loglevel\" has been renamed to \"log_level\""));
			known_parameter = false;
		}
		else if (strcmp(name, "logfacility") == 0)
		{
			item_list_append(warning_list,
							 _("parameter \"logfacility\" has been renamed to \"log_facility\""));
			known_parameter = false;
		}
		else if (strcmp(name, "logfile") == 0)
		{
			item_list_append(warning_list,
							 _("parameter \"logfile\" has been renamed to \"log_file\""));
			known_parameter = false;
		}
		else if (strcmp(name, "master_reponse_timeout") == 0)
		{
			item_list_append(warning_list,
							 _("parameter \"master_reponse_timeout\" has been removed; use \"async_query_timeout\" instead"));
			known_parameter = false;
		}
		else if (strcmp(name, "retry_promote_interval_secs") == 0)
		{
			item_list_append(warning_list,
							 _("parameter \"retry_promote_interval_secs\" has been removed; use \"primary_notification_timeout\" instead"));
			known_parameter = false;
		}
		else
		{
			known_parameter = false;
			log_warning(_("%s/%s: unknown name/value pair provided; ignoring"), name, value);
		}

		/*
		 * Raise an error if a known parameter is provided with an empty
		 * value. Currently there's no reason why empty parameters are needed;
		 * if we want to accept those, we'd need to add stricter default
		 * checking, as currently e.g. an empty `node_id` value will be converted
		 * to '0'.
		 */
		if (known_parameter == true && !strlen(value))
		{
			char		error_message_buf[MAXLEN] = "";

			maxlen_snprintf(error_message_buf,
							_("\"%s\": no value provided"),
							name);

			item_list_append(error_list, error_message_buf);
		}
	}

	fclose(fp);

	/* check required parameters */
	if (node_id_found == false)
	{
		item_list_append(error_list, _("\"node_id\": required parameter was not found"));
	}

	if (!strlen(options->node_name))
	{
		item_list_append(error_list, _("\"node_name\": required parameter was not found"));
	}

	if (!strlen(options->data_directory))
	{
		item_list_append(error_list, _("\"data_directory\": required parameter was not found"));
	}

	if (!strlen(options->conninfo))
	{
		item_list_append(error_list, _("\"conninfo\": required parameter was not found"));
	}
	else
	{
		/*
		 * Sanity check the provided conninfo string
		 *
		 * NOTE: PQconninfoParse() verifies the string format and checks for
		 * valid options but does not sanity check values
		 */

		PQconninfoOption *conninfo_options = NULL;
		char	   *conninfo_errmsg = NULL;

		conninfo_options = PQconninfoParse(options->conninfo, &conninfo_errmsg);
		if (conninfo_options == NULL)
		{
			char		error_message_buf[MAXLEN] = "";

			snprintf(error_message_buf,
					 MAXLEN,
					 _("\"conninfo\": %s	(provided: \"%s\")"),
					 conninfo_errmsg,
					 options->conninfo);

			item_list_append(error_list, error_message_buf);
		}

		PQconninfoFree(conninfo_options);
	}

	/* set values for parameters which default to other parameters */

	/*
	 * From 4.1, "repmgrd_standby_startup_timeout" replaces "standby_reconnect_timeout"
	 * in repmgrd; fall back to "standby_reconnect_timeout" if no value explicitly provided
	 */
	if (options->repmgrd_standby_startup_timeout == -1)
	{
		options->repmgrd_standby_startup_timeout = options->standby_reconnect_timeout;
	}

	/* add warning about changed "barman_" parameter meanings */
	if ((options->barman_host[0] == '\0' && options->barman_server[0] != '\0') ||
		(options->barman_host[0] != '\0' && options->barman_server[0] == '\0'))
	{
		item_list_append(error_list,
						 _("use \"barman_host\" for the hostname of the Barman server"));
		item_list_append(error_list,
						 _("use \"barman_server\" for the name of the [server] section in the Barman configuration file"));

	}

	/* other sanity checks */

	if (options->archive_ready_warning >= options->archive_ready_critical)
	{
		item_list_append(error_list,
						 _("\"archive_ready_critical\" must be greater than  \"archive_ready_warning\""));
	}

	if (options->replication_lag_warning >= options->replication_lag_critical)
	{
		item_list_append(error_list,
						 _("\"replication_lag_critical\" must be greater than  \"replication_lag_warning\""));
	}

	if (options->standby_reconnect_timeout < options->node_rejoin_timeout)
	{
		item_list_append(error_list,
						 _("\"standby_reconnect_timeout\" must be equal to or greater than \"node_rejoin_timeout\""));
	}
}




bool
parse_recovery_conf(const char *data_dir, t_recovery_conf *conf)
{
	char		recovery_conf_path[MAXPGPATH] = "";
	FILE	   *fp;
	char	   *s = NULL,
				buf[MAXLINELENGTH] = "";
	char		name[MAXLEN] = "";
	char		value[MAXLEN] = "";

	snprintf(recovery_conf_path, MAXPGPATH,
			 "%s/%s",
			 data_dir,
			 RECOVERY_COMMAND_FILE);

	fp = fopen(recovery_conf_path, "r");

	if (fp == NULL)
	{
		return false;
	}

	/* Read file */
	while ((s = fgets(buf, sizeof buf, fp)) != NULL)
	{

		/* Parse name/value pair from line */
		_parse_line(buf, name, value);

		/* Skip blank lines */
		if (!strlen(name))
			continue;

		/* Skip comments */
		if (name[0] == '#')
			continue;

		/* archive recovery settings */
		if (strcmp(name, "restore_command") == 0)
			strncpy(conf->restore_command, value, MAXLEN);
		else if (strcmp(name, "archive_cleanup_command") == 0)
			strncpy(conf->archive_cleanup_command, value, MAXLEN);
		else if (strcmp(name, "recovery_end_command") == 0)
			strncpy(conf->recovery_end_command, value, MAXLEN);
		/* recovery target settings */
		else if (strcmp(name, "recovery_target_name") == 0)
			strncpy(conf->recovery_target_name, value, MAXLEN);
		else if (strcmp(name, "recovery_target_time") == 0)
			strncpy(conf->recovery_target_time, value, MAXLEN);
		else if (strcmp(name, "recovery_target_xid") == 0)
			strncpy(conf->recovery_target_xid, value, MAXLEN);
		else if (strcmp(name, "recovery_target_inclusive") == 0)
			conf->recovery_target_inclusive = parse_bool(value, NULL, NULL);
		else if (strcmp(name, "recovery_target_timeline") == 0)
		{
			if (strncmp(value, "latest", MAXLEN) == 0)
			{
				conf->recovery_target_timeline = TARGET_TIMELINE_LATEST;
			}
			else
			{
				conf->recovery_target_timeline = atoi(value);
			}
		}
		else if (strcmp(name, "recovery_target_action") == 0)
		{
			if (strcmp(value, "pause") == 0)
				conf->recovery_target_action = RTA_PAUSE;
			else if (strcmp(value, "promote") == 0)
				conf->recovery_target_action = RTA_PROMOTE;
			else if (strcmp(value, "shutdown") == 0)
				conf->recovery_target_action = RTA_SHUTDOWN;
		}

		/* standby server settings */

		else if (strcmp(name, "standby_mode") == 0)
			conf->standby_mode = parse_bool(value, NULL, NULL);
		else if (strcmp(name, "primary_conninfo") == 0)
			strncpy(conf->primary_conninfo, value, MAXLEN);
		else if (strcmp(name, "primary_slot_name") == 0)
			strncpy(conf->trigger_file, value, MAXLEN);
		else if (strcmp(name, "trigger_file") == 0)
			strncpy(conf->trigger_file, value, MAXLEN);
		else if (strcmp(name, "recovery_min_apply_delay") == 0)
			parse_time_unit_parameter(name, value, conf->recovery_min_apply_delay, NULL);

	}
	fclose(fp);

	return true;
}


void
_parse_line(char *buf, char *name, char *value)
{
	int			i = 0;
	int			j = 0;

	/*
	 * Extract parameter name, if present
	 */
	for (; i < MAXLEN; ++i)
	{
		if (buf[i] == '=')
			break;

		switch (buf[i])
		{
				/* Ignore whitespace */
			case ' ':
			case '\n':
			case '\r':
			case '\t':
				continue;
			default:
				name[j++] = buf[i];
		}
	}
	name[j] = '\0';

	/*
	 * Ignore any whitespace following the '=' sign
	 */
	for (; i < MAXLEN; ++i)
	{
		if (buf[i + 1] == ' ')
			continue;
		if (buf[i + 1] == '\t')
			continue;

		break;
	}

	/*
	 * Extract parameter value
	 */
	j = 0;
	for (++i; i < MAXLEN; ++i)
		if (buf[i] == '\'')
			continue;
		else if (buf[i] == '#')
			break;
		else if (buf[i] != '\n')
			value[j++] = buf[i];
		else
			break;
	value[j] = '\0';
	trim(value);
}


static void
parse_time_unit_parameter(const char *name, const char *value, char *dest, ItemList *errors)
{
	char	   *ptr = NULL;
	int			targ = strtol(value, &ptr, 10);

	if (targ < 0)
	{
		if (errors != NULL)
		{
			item_list_append_format(errors,
									_("invalid value provided for \"%s\""),
									name);
		}
		return;
	}

	if (ptr && *ptr)
	{
		if (strcmp(ptr, "ms") != 0 && strcmp(ptr, "s") != 0 &&
			strcmp(ptr, "min") != 0 && strcmp(ptr, "h") != 0 &&
			strcmp(ptr, "d") != 0)
		{
			if (errors != NULL)
			{
				item_list_append_format(
										errors,
										_("value provided for \"%s\" must be one of ms/s/min/h/d"),
										name);
				return;
			}
		}
	}

	strncpy(dest, value, MAXLEN);
}

/*
 * reload_config()
 *
 * This is only called by repmgrd after receiving a SIGHUP or when a monitoring
 * loop is started up; it therefore only needs to reload options required
 * by repmgrd, which are as follows:
 *
 * changeable options:
 * - async_query_timeout
 * - bdr_local_monitoring_only
 * - bdr_recovery_timeout
 * - conninfo
 * - degraded_monitoring_timeout
 * - event_notification_command
 * - event_notifications
 * - failover
 * - follow_command
 * - log_facility
 * - log_file
 * - log_level
 * - log_status_interval
 * - monitor_interval_secs
 * - monitoring_history
 * - promote_command
 * - promote_delay
 * - reconnect_attempts
 * - reconnect_interval
 * - repmgrd_standby_startup_timeout
 * - retry_promote_interval_secs
 *
 * non-changeable options (repmgrd references these from the "repmgr.nodes"
 * table, not the configuration file)
 *
 * - node_id
 * - node_name
 * - data_directory
 * - location
 * - priority
 * - replication_type
 *
 * extract with something like:
 *	 grep config_file_options\\. repmgrd*.c | perl -n -e '/config_file_options\.([\w_]+)/ && print qq|$1\n|;' | sort | uniq

 */
bool
reload_config(t_configuration_options *orig_options, t_server_type server_type)
{
	PGconn	   *conn;
	t_configuration_options new_options = T_CONFIGURATION_OPTIONS_INITIALIZER;
	bool		config_changed = false;
	bool		log_config_changed = false;

	static ItemList config_errors = {NULL, NULL};
	static ItemList config_warnings = {NULL, NULL};

	PQExpBufferData errors;

	log_info(_("reloading configuration file"));

	_parse_config(&new_options, &config_errors, &config_warnings);


	if (server_type == PRIMARY || server_type == STANDBY)
	{
		if (new_options.promote_command[0] == '\0')
		{
			item_list_append(&config_errors, _("\"promote_command\": required parameter was not found"));
		}

		if (new_options.follow_command[0] == '\0')
		{
			item_list_append(&config_errors, _("\"follow_command\": required parameter was not found"));
		}
	}

	if (config_errors.head != NULL)
	{
		ItemListCell *cell = NULL;

		log_warning(_("unable to parse new configuration, retaining current configuration"));

		initPQExpBuffer(&errors);

		appendPQExpBufferStr(&errors,
							 "following errors were detected:\n");

		for (cell = config_errors.head; cell; cell = cell->next)
		{
			appendPQExpBuffer(&errors,
							  "  %s\n", cell->string);
		}

		log_detail("%s", errors.data);
		termPQExpBuffer(&errors);
		return false;
	}



	/* The following options cannot be changed */

	if (new_options.node_id != orig_options->node_id)
	{
		log_warning(_("\"node_id\" cannot be changed, retaining current configuration"));
		return false;
	}

	if (strncmp(new_options.node_name, orig_options->node_name, MAXLEN) != 0)
	{
		log_warning(_("\"node_name\" cannot be changed, keeping current configuration"));
		return false;
	}


	/*
	 * No configuration problems detected - copy any changed values
	 *
	 * NB: keep these in the same order as in configfile.h to make it easier
	 * to manage them
	 */


	/* async_query_timeout */
	if (orig_options->async_query_timeout != new_options.async_query_timeout)
	{
		orig_options->async_query_timeout = new_options.async_query_timeout;

		log_info(_("\"async_query_timeout\" is now \"%i\""), new_options.async_query_timeout);

		config_changed = true;
	}

	/* bdr_local_monitoring_only */
	if (orig_options->bdr_local_monitoring_only != new_options.bdr_local_monitoring_only)
	{
		orig_options->bdr_local_monitoring_only = new_options.bdr_local_monitoring_only;
		log_info(_("\"bdr_local_monitoring_only\" is now \"%s\""), new_options.bdr_local_monitoring_only == true ? "TRUE" : "FALSE");

		config_changed = true;
	}

	/* bdr_recovery_timeout */
	if (orig_options->bdr_recovery_timeout != new_options.bdr_recovery_timeout)
	{
		orig_options->bdr_recovery_timeout = new_options.bdr_recovery_timeout;
		log_info(_("\"bdr_recovery_timeout\" is now \"%i\""), new_options.bdr_recovery_timeout);

		config_changed = true;
	}

	/* conninfo */
	if (strncmp(orig_options->conninfo, new_options.conninfo, MAXLEN) != 0)
	{
		/* Test conninfo string works */
		conn = establish_db_connection(new_options.conninfo, false);
		if (!conn || (PQstatus(conn) != CONNECTION_OK))
		{
			log_warning(_("\"conninfo\" string is not valid, retaining current configuration"));
		}
		else
		{
			strncpy(orig_options->conninfo, new_options.conninfo, MAXLEN);
			log_info(_("\"conninfo\" is now \"%s\""), new_options.conninfo);

		}
		PQfinish(conn);
	}

	/* degraded_monitoring_timeout */
	if (orig_options->degraded_monitoring_timeout != new_options.degraded_monitoring_timeout)
	{
		orig_options->degraded_monitoring_timeout = new_options.degraded_monitoring_timeout;
		log_info(_("\"degraded_monitoring_timeout\" is now \"%i\""), new_options.degraded_monitoring_timeout);

		config_changed = true;
	}

	/* event_notification_command */
	if (strncmp(orig_options->event_notification_command, new_options.event_notification_command, MAXLEN) != 0)
	{
		strncpy(orig_options->event_notification_command, new_options.event_notification_command, MAXLEN);
		log_info(_("\"event_notification_command\" is now \"%s\""), new_options.event_notification_command);

		config_changed = true;
	}

	/* event_notifications */
	if (strncmp(orig_options->event_notifications_orig, new_options.event_notifications_orig, MAXLEN) != 0)
	{
		strncpy(orig_options->event_notifications_orig, new_options.event_notifications_orig, MAXLEN);
		log_info(_("\"event_notifications\" is now \"%s\""), new_options.event_notifications_orig);

		clear_event_notification_list(orig_options);
		orig_options->event_notifications = new_options.event_notifications;

		config_changed = true;
	}

	/* failover */
	if (orig_options->failover != new_options.failover)
	{
		orig_options->failover = new_options.failover;
		log_info(_("\"failover\" is now \"%s\""), new_options.failover == true ? "TRUE" : "FALSE");
		config_changed = true;
	}

	/* follow_command */
	if (strncmp(orig_options->follow_command, new_options.follow_command, MAXLEN) != 0)
	{
		strncpy(orig_options->follow_command, new_options.follow_command, MAXLEN);
		log_info(_("\"follow_command\" is now \"%s\""), new_options.follow_command);

		config_changed = true;
	}

	/* monitor_interval_secs */
	if (orig_options->monitor_interval_secs != new_options.monitor_interval_secs)
	{
		orig_options->monitor_interval_secs = new_options.monitor_interval_secs;
		log_info(_("\"monitor_interval_secs\" is now \"%i\""), new_options.monitor_interval_secs);

		config_changed = true;
	}

	/* monitoring_history */
	if (orig_options->monitoring_history != new_options.monitoring_history)
	{
		orig_options->monitoring_history = new_options.monitoring_history;
		log_info(_("\"monitoring_history\" is now \"%s\""), new_options.monitoring_history == true ? "TRUE" : "FALSE");

		config_changed = true;
	}

	/* primary_notification_timeout */
	if (orig_options->primary_notification_timeout != new_options.primary_notification_timeout)
	{
		orig_options->primary_notification_timeout = new_options.primary_notification_timeout;
		log_info(_("\"primary_notification_timeout\" is now \"%i\""), new_options.primary_notification_timeout);

		config_changed = true;
	}


	/* promote_command */
	if (strncmp(orig_options->promote_command, new_options.promote_command, MAXLEN) != 0)
	{
		strncpy(orig_options->promote_command, new_options.promote_command, MAXLEN);
		log_info(_("\"promote_command\" is now \"%s\""), new_options.promote_command);

		config_changed = true;
	}

	/* promote_delay (for testing use only; not documented */
	if (orig_options->promote_delay != new_options.promote_delay)
	{
		orig_options->promote_delay = new_options.promote_delay;
		log_info(_("\"promote_delay\" is now \"%i\""), new_options.promote_delay);

		config_changed = true;
	}

	/* reconnect_attempts */
	if (orig_options->reconnect_attempts != new_options.reconnect_attempts)
	{
		orig_options->reconnect_attempts = new_options.reconnect_attempts;
		log_info(_("\"reconnect_attempts\" is now \"%i\""), new_options.reconnect_attempts);

		config_changed = true;
	}

	/* reconnect_interval */
	if (orig_options->reconnect_interval != new_options.reconnect_interval)
	{
		orig_options->reconnect_interval = new_options.reconnect_interval;
		log_info(_("\"reconnect_interval\" is now \"%i\""), new_options.reconnect_interval);

		config_changed = true;
	}

	/* repmgrd_standby_startup_timeout */
	if (orig_options->repmgrd_standby_startup_timeout != new_options.repmgrd_standby_startup_timeout)
	{
		orig_options->repmgrd_standby_startup_timeout = new_options.repmgrd_standby_startup_timeout;
		log_info(_("\"repmgrd_standby_startup_timeout\" is now \"%i\""), new_options.repmgrd_standby_startup_timeout);

		config_changed = true;
	}

	/*
	 * Handle changes to logging configuration
	 */

	/* log_facility */
	if (strncmp(orig_options->log_facility, new_options.log_facility, MAXLEN) != 0)
	{
		strncpy(orig_options->log_facility, new_options.log_facility, MAXLEN);
		log_info(_("\"log_facility\" is now \"%s\""), new_options.log_facility);

		log_config_changed = true;
	}

	/* log_file */
	if (strncmp(orig_options->log_file, new_options.log_file, MAXLEN) != 0)
	{
		strncpy(orig_options->log_file, new_options.log_file, MAXLEN);
		log_info(_("\"log_file\" is now \"%s\""), new_options.log_file);

		log_config_changed = true;
	}


	/* log_level */
	if (strncmp(orig_options->log_level, new_options.log_level, MAXLEN) != 0)
	{
		strncpy(orig_options->log_level, new_options.log_level, MAXLEN);
		log_info(_("\"log_level\" is now \"%s\""), new_options.log_level);

		log_config_changed = true;
	}

	/* log_status_interval */
	if (orig_options->log_status_interval != new_options.log_status_interval)
	{
		orig_options->log_status_interval = new_options.log_status_interval;
		log_info(_("\"log_status_interval\" is now \"%i\""), new_options.log_status_interval);

		config_changed = true;
	}


	if (log_config_changed == true)
	{
		log_notice(_("restarting logging with changed parameters"));
		logger_shutdown();
		logger_init(orig_options, progname());
		log_notice(_("configuration file reloaded with changed parameters"));
	}

	if (config_changed == true)
	{
		log_info(_("configuration has changed"));
	}

	/*
	 * neither logging nor other configuration has changed
	 */
	if (log_config_changed == false && config_changed == false)
	{
		log_info(_("configuration has not changed"));
	}

	return config_changed;
}


static void
exit_with_config_file_errors(ItemList *config_errors, ItemList *config_warnings, bool terse)
{
	log_error(_("following errors were found in the configuration file:"));

	print_item_list(config_errors);
	item_list_free(config_errors);

	if (terse == false && config_warnings->head != NULL)
	{
		puts("");
		log_warning(_("the following problems were also found in the configuration file:"));

		print_item_list(config_warnings);
		item_list_free(config_warnings);
	}

	if (config_file_provided == false)
		log_detail(_("configuration file is: \"%s\""), config_file_path);

	exit(ERR_BAD_CONFIG);
}


void
exit_with_cli_errors(ItemList *error_list, const char *repmgr_command)
{
	fprintf(stderr, _("The following command line errors were encountered:\n"));

	print_item_list(error_list);

	if (repmgr_command != NULL)
	{
		fprintf(stderr, _("Try \"%s --help\" or \"%s %s --help\" for more information.\n"),
				progname(),
				progname(),
				repmgr_command);
	}
	else
	{
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname());
	}

	exit(ERR_BAD_CONFIG);
}

void
print_item_list(ItemList *item_list)
{
	ItemListCell *cell = NULL;

	for (cell = item_list->head; cell; cell = cell->next)
	{
		fprintf(stderr, "  %s\n", cell->string);
	}
}




/*
 * Convert provided string to an integer using strtol;
 * on error, if a callback is provided, pass the error message to that,
 * otherwise exit
 */
int
repmgr_atoi(const char *value, const char *config_item, ItemList *error_list, int minval)
{
	char	   *endptr = NULL;
	long		longval = 0;
	PQExpBufferData errors;

	initPQExpBuffer(&errors);

	/*
	 * It's possible that some versions of strtol() don't treat an empty
	 * string as an error.
	 */
	if (*value == '\0')
	{
		/* don't log here - empty values will be caught later */
		return 0;
	}
	else
	{
		errno = 0;
		longval = strtol(value, &endptr, 10);

		if (value == endptr || errno)
		{
			appendPQExpBuffer(&errors,
							  _("\"%s\": invalid value (provided: \"%s\")"),
							  config_item, value);
		}
		else if ((int32) longval < longval)
		{
			appendPQExpBuffer(&errors,
							  _("\"%s\": must be a positive signed 32 bit integer, i.e. 2147483647 or less (provided: \"%s\")"),
							  config_item,
							  value);
		}
		else if ((int32) longval < minval)
			/* Disallow negative values for most parameters */
		{
			appendPQExpBuffer(&errors,
							  _("\"%s\": must be %i or greater (provided: \"%s\")"),
							  config_item,
							  minval,
							  value);
		}
	}

	/* Error message buffer is set */
	if (errors.data[0] != '\0')
	{
		if (error_list == NULL)
		{
			log_error("%s", errors.data);
			termPQExpBuffer(&errors);
			exit(ERR_BAD_CONFIG);
		}

		item_list_append(error_list, errors.data);
	}

	termPQExpBuffer(&errors);
	return (int32) longval;
}


/*
 * Interpret a parameter value as a boolean. Currently accepts:
 *
 * - true/false
 * - 1/0
 * - on/off
 * - yes/no

 * Returns 'false' if unable to determine the booleanness of the value
 * and adds an entry to the error list, which will result in the program
 * erroring out before it proceeds to do anything.
 *
 * TODO: accept "any unambiguous prefix of one of these" as per postgresql.conf:
 *
 *   https://www.postgresql.org/docs/current/config-setting.html
 */
bool
parse_bool(const char *s, const char *config_item, ItemList *error_list)
{
	PQExpBufferData errors;

	if (s == NULL)
		return true;

	if (strcasecmp(s, "0") == 0)
		return false;

	if (strcasecmp(s, "1") == 0)
		return true;

	if (strcasecmp(s, "false") == 0)
		return false;

	if (strcasecmp(s, "true") == 0)
		return true;

	if (strcasecmp(s, "off") == 0)
		return false;

	if (strcasecmp(s, "on") == 0)
		return true;

	if (strcasecmp(s, "no") == 0)
		return false;

	if (strcasecmp(s, "yes") == 0)
		return true;

	if (error_list != NULL)
	{
		initPQExpBuffer(&errors);

		appendPQExpBuffer(&errors,
						  "\"%s\": unable to interpret \"%s\" as a boolean value",
						  config_item, s);
		item_list_append(error_list, errors.data);
		termPQExpBuffer(&errors);
	}

	return false;
}

/*
 * Split argument into old_dir and new_dir and append to tablespace mapping
 * list.
 *
 * Adapted from pg_basebackup.c
 */
static void
tablespace_list_append(t_configuration_options *options, const char *arg)
{
	TablespaceListCell *cell = NULL;
	char	   *dst = NULL;
	char	   *dst_ptr = NULL;
	const char *arg_ptr = NULL;

	cell = (TablespaceListCell *) pg_malloc0(sizeof(TablespaceListCell));
	if (cell == NULL)
	{
		log_error(_("unable to allocate memory; terminating"));
		exit(ERR_BAD_CONFIG);
	}

	dst_ptr = dst = cell->old_dir;
	for (arg_ptr = arg; *arg_ptr; arg_ptr++)
	{
		if (dst_ptr - dst >= MAXPGPATH)
		{
			log_error(_("directory name too long"));
			exit(ERR_BAD_CONFIG);
		}

		if (*arg_ptr == '\\' && *(arg_ptr + 1) == '=')
			;					/* skip backslash escaping = */
		else if (*arg_ptr == '=' && (arg_ptr == arg || *(arg_ptr - 1) != '\\'))
		{
			if (*cell->new_dir)
			{
				log_error(_("multiple \"=\" signs in tablespace mapping"));
				exit(ERR_BAD_CONFIG);
			}
			else
			{
				dst = dst_ptr = cell->new_dir;
			}
		}
		else
			*dst_ptr++ = *arg_ptr;
	}

	if (!*cell->old_dir || !*cell->new_dir)
	{
		log_error(_("invalid tablespace mapping format \"%s\", must be \"OLDDIR=NEWDIR\""),
				  arg);
		exit(ERR_BAD_CONFIG);
	}

	canonicalize_path(cell->old_dir);
	canonicalize_path(cell->new_dir);

	if (options->tablespace_mapping.tail)
		options->tablespace_mapping.tail->next = cell;
	else
		options->tablespace_mapping.head = cell;

	options->tablespace_mapping.tail = cell;
}



/*
 * parse_event_notifications_list()
 *
 *
 */

static void
parse_event_notifications_list(t_configuration_options *options, const char *arg)
{
	const char *arg_ptr = NULL;
	char		event_type_buf[MAXLEN] = "";
	char	   *dst_ptr = event_type_buf;

	for (arg_ptr = arg; arg_ptr <= (arg + strlen(arg)); arg_ptr++)
	{
		/* ignore whitespace */
		if (*arg_ptr == ' ' || *arg_ptr == '\t')
		{
			continue;
		}

		/*
		 * comma (or end-of-string) should mark the end of an event type -
		 * just as long as there was something preceding it
		 */
		if ((*arg_ptr == ',' || *arg_ptr == '\0') && event_type_buf[0] != '\0')
		{
			EventNotificationListCell *cell;

			cell = (EventNotificationListCell *) pg_malloc0(sizeof(EventNotificationListCell));

			if (cell == NULL)
			{
				log_error(_("unable to allocate memory; terminating"));
				exit(ERR_BAD_CONFIG);
			}

			strncpy(cell->event_type, event_type_buf, MAXLEN);

			if (options->event_notifications.tail)
			{
				options->event_notifications.tail->next = cell;
			}
			else
			{
				options->event_notifications.head = cell;
			}

			options->event_notifications.tail = cell;

			memset(event_type_buf, 0, MAXLEN);
			dst_ptr = event_type_buf;
		}
		/* ignore duplicated commas */
		else if (*arg_ptr == ',')
		{
			continue;
		}
		else
		{
			*dst_ptr++ = *arg_ptr;
		}
	}
}


static void
clear_event_notification_list(t_configuration_options *options)
{
	if (options->event_notifications.head != NULL)
	{
		EventNotificationListCell *cell;
		EventNotificationListCell *next_cell;

		cell = options->event_notifications.head;

		while (cell != NULL)
		{
			next_cell = cell->next;
			pfree(cell);
			cell = next_cell;
		}
	}
}


int
parse_output_to_argv(const char *string, char ***argv_array)
{
	int			options_len = 0;
	char	   *options_string = NULL;
	char	   *options_string_ptr = NULL;
	int			c = 1,
	   			argc_item = 1;
	char	   *argv_item = NULL;
	char	  **local_argv_array = NULL;
	ItemListCell *cell;

	/*
	 * Add parsed options to this list, then copy to an array to pass to
	 * getopt
	 */
	ItemList option_argv = {NULL, NULL};

	options_len = strlen(string) + 1;
	options_string = pg_malloc0(options_len);
	options_string_ptr = options_string;

	/* Copy the string before operating on it with strtok() */
	strncpy(options_string, string, options_len);

	/* Extract arguments into a list and keep a count of the total */
	while ((argv_item = strtok(options_string_ptr, " ")) != NULL)
	{
		item_list_append(&option_argv, trim(argv_item));

		argc_item++;

		if (options_string_ptr != NULL)
			options_string_ptr = NULL;
	}

	pfree(options_string);

	/*
	 * Array of argument values to pass to getopt_long - this will need to
	 * include an empty string as the first value (normally this would be the
	 * program name)
	 */
	local_argv_array = pg_malloc0(sizeof(char *) * (argc_item + 2));

	/* Insert a blank dummy program name at the start of the array */
	local_argv_array[0] = pg_malloc0(1);

	/*
	 * Copy the previously extracted arguments from our list to the array
	 */
	for (cell = option_argv.head; cell; cell = cell->next)
	{
		int			argv_len = strlen(cell->string) + 1;

		local_argv_array[c] = (char *)pg_malloc0(argv_len);

		strncpy(local_argv_array[c], cell->string, argv_len);

		c++;
	}

	local_argv_array[c] = NULL;

	item_list_free(&option_argv);

	*argv_array = local_argv_array;

	return argc_item;
}


void
free_parsed_argv(char ***argv_array)
{
	char	  **local_argv_array = *argv_array;
	int			i = 0;

	while (local_argv_array[i] != NULL)
	{
		pfree((char *)local_argv_array[i]);
		i++;
	}

	pfree((char **)local_argv_array);
	*argv_array = NULL;
}





bool
parse_pg_basebackup_options(const char *pg_basebackup_options, t_basebackup_options *backup_options, int server_version_num, ItemList *error_list)
{
	bool		backup_options_ok = true;

	int			c = 0,
				argc_item = 0;

	char	  **argv_array = NULL;

	int			optindex = 0;

	struct option *long_options = NULL;


	/* We're only interested in these options */
	static struct option long_options_9[] =
	{
		{"slot", required_argument, NULL, 'S'},
		{"xlog-method", required_argument, NULL, 'X'},
		{NULL, 0, NULL, 0}
	};

	/*
	 * From PostgreSQL 10, --xlog-method is renamed --wal-method and there's
	 * also --no-slot, which we'll want to consider.
	 */
	static struct option long_options_10[] =
	{
		{"slot", required_argument, NULL, 'S'},
		{"wal-method", required_argument, NULL, 'X'},
		{"no-slot", no_argument, NULL, 1},
		{NULL, 0, NULL, 0}
	};

	/* Don't attempt to tokenise an empty string */
	if (!strlen(pg_basebackup_options))
		return backup_options_ok;

	if (server_version_num >= 100000)
		long_options = long_options_10;
	else
		long_options = long_options_9;

	argc_item = parse_output_to_argv(pg_basebackup_options, &argv_array);

	/* Reset getopt's optind variable */
	optind = 0;

	/* Prevent getopt from emitting errors */
	opterr = 0;

	while ((c = getopt_long(argc_item, argv_array, "S:X:", long_options,
							&optindex)) != -1)
	{
		switch (c)
		{
			case 'S':
				strncpy(backup_options->slot, optarg, MAXLEN);
				break;
			case 'X':
				strncpy(backup_options->xlog_method, optarg, MAXLEN);
				break;
			case 1:
				backup_options->no_slot = true;
				break;
			case '?':
				if (server_version_num >= 100000 && optopt == 1)
				{
					if (error_list != NULL)
					{
						item_list_append(error_list, "invalid use of --no-slot");
					}
					backup_options_ok = false;
				}
				break;
		}
	}

	if (backup_options->no_slot == true && backup_options->slot[0] != '\0')
	{
		if (error_list != NULL)
		{
			item_list_append(error_list, "--no-slot cannot be used with -S/--slot");
		}
		backup_options_ok = false;
	}

	free_parsed_argv(&argv_array);

	return backup_options_ok;
}
