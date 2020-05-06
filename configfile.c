/*
 * configfile.c - parse repmgr.conf and other configuration-related functionality
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

#include <sys/stat.h>			/* for stat() */

#include "repmgr.h"
#include "configfile.h"
#include "log.h"

#include <utils/elog.h>

#if (PG_ACTUAL_VERSION_NUM >= 100000)
#include <storage/fd.h>			/* for durable_rename() */
#endif

const static char *_progname = NULL;
char		config_file_path[MAXPGPATH] = "";
static bool config_file_provided = false;
bool		config_file_found = false;
t_configuration_options config_file_options;


t_configuration_options config_file_options = T_CONFIGURATION_OPTIONS_INITIALIZER;

struct ConfigFileOption config_file_options2[] =
{

	/* ================
	 * node information
	 * ================
	 */
	/* node_id */
	{
		"node_id",
		CONFIG_INT,
		{ .intptr = &config_file_options.node_id },
		{ .intdefault = UNKNOWN_NODE_ID },
		MIN_NODE_ID,
		-1
	},
	/* node_name */
	{
		"node_name",
		CONFIG_STRING,
		{ .strptr = config_file_options.node_name },
		{ .strdefault = NULL },
		-1,
		sizeof(config_file_options.node_name)
	},

	/* ======================
	 * standby clone settings
	 * ======================
	 */
	/* use_replication_slots */
	{
		"use_replication_slots",
		CONFIG_BOOL,
		{ .boolptr = &config_file_options.use_replication_slots },
		{ .booldefault = false },
		-1,
		-1
	},
	/* ================
	 * repmgrd settings
	 * ================
	 */
	/* failover */
	{
		"failover",
		CONFIG_FAILOVER_MODE,
		{ .failovermodeptr = &config_file_options.failover },
		{ .failovermodedefault = FAILOVER_MANUAL },
		-1,
		-1
	},
	/* connection_check_type */
	{
		"connection_check_type",
		CONFIG_CONNECTION_CHECK_TYPE,
		{ .checktypeptr = &config_file_options.connection_check_type },
		{ .checktypedefault = CHECK_PING },
		-1,
		-1
	},
	/* ===========================
	 * event notification settings
	 * ===========================
	 */
	{
		"event_notifications",
		CONFIG_EVENT_NOTIFICATION_LIST,
		{ .notificationlistptr = &config_file_options.event_notifications },
		{ .notificationlistdefault = NULL },
		-1,
		-1
	},
	/* End-of-list marker */
	{
		NULL, CONFIG_INT, {}, {}, -1, -1
	}
};


static void parse_config(bool terse);
static void parse_config2(bool terse);

static void _parse_config(t_configuration_options *options, ItemList *error_list, ItemList *warning_list);
static void _parse_config2(ItemList *error_list, ItemList *warning_list);

static void _parse_line(char *buf, char *name, char *value);
static void parse_event_notifications_list(EventNotificationList *event_notifications, const char *arg);
static void clear_event_notification_list(EventNotificationList *event_notifications);

static void parse_time_unit_parameter(const char *name, const char *value, char *dest, ItemList *errors);

static void copy_config_file_options(t_configuration_options *original, t_configuration_options *copy);

static void tablespace_list_append(t_configuration_options *options, const char *arg);
static void tablespace_list_copy(t_configuration_options *original, t_configuration_options *copy);
static void tablespace_list_free(t_configuration_options *options);

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

	parse_config(terse);

	return;
}

void
load_config2(const char *config_file, bool verbose, bool terse, char *argv0)
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

	parse_config2(terse);

	return;
}


static void
parse_config(bool terse)
{
	/* Collate configuration file errors here for friendlier reporting */
	static ItemList config_errors = {NULL, NULL};
	static ItemList config_warnings = {NULL, NULL};

	_parse_config(&config_file_options, &config_errors, &config_warnings);

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
parse_config2(bool terse)
{
	/* Collate configuration file errors here for friendlier reporting */
	static ItemList config_errors = {NULL, NULL};
	static ItemList config_warnings = {NULL, NULL};

	_parse_config2(&config_errors, &config_warnings);

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
_parse_config2(ItemList *error_list, ItemList *warning_list)
{
	FILE	   *fp;
	char		base_directory[MAXPGPATH];


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

	/*
	 * A configuration file has been found, either provided by the user or
	 * found in one of the default locations. Sanity check whether we
	 * can open it, and fail with an error about the nature of the file
	 * (provided or default) if not. We do this here rather than having
	 * to teach the configuration file parser the difference.
	 */

	fp = fopen(config_file_path, "r");

	if (fp == NULL)
	{
		if (config_file_provided)
		{
			log_error(_("unable to open provided configuration file \"%s\"; terminating"),
					  config_file_path);
		}
		else
		{
			log_error(_("unable to open default configuration file \"%s\"; terminating"),
					  config_file_path);
		}

		exit(ERR_BAD_CONFIG);
	}

	fclose(fp);

	strncpy(base_directory, config_file_path, MAXPGPATH);
	canonicalize_path(base_directory);
	get_parent_directory(base_directory);

	// XXX fail here if processing issue found
	(void) ProcessRepmgrConfigFile2(config_file_path, base_directory, error_list, warning_list);

	/* check required parameters */
	// ...

	{
		ConfigFileOption *option = &config_file_options2[0];
		int i = 0;

		do {
			switch (option->type)
			{
				case CONFIG_INT:
					printf(" %s: %i\n", option->name, *option->val.intptr);
					break;
				case CONFIG_STRING:
					printf(" %s: %s\n", option->name, option->val.strptr);
					break;
				case CONFIG_BOOL:
					printf(" %s: %s\n", option->name, format_bool(*option->val.boolptr));
					break;
				case CONFIG_FAILOVER_MODE:
					printf(" %s: %s\n", option->name, format_failover_mode(*option->val.failovermodeptr));
					break;
				case CONFIG_CONNECTION_CHECK_TYPE:
					printf(" %s: %s\n", option->name, print_connection_check_type(*option->val.checktypeptr));
					break;
				case CONFIG_EVENT_NOTIFICATION_LIST:
				{
					char *list_str = print_event_notification_list(option->val.notificationlistptr);
					printf(" %s: %s\n", option->name, list_str);
					pfree(list_str);
					break;
				}
			}
			i++;
			option = &config_file_options2[i];
		} while (option->name != NULL);
	}
}


void
parse_configuration_item2(ItemList *error_list, ItemList *warning_list, const char *name, const char *value)
{
	ConfigFileOption *option = &config_file_options2[0];
	int i = 0;

	do {
		if (strcmp(name, option->name) == 0)
		{
			//printf("%s = '%s'\n", name, value);

			switch (option->type)
			{
				case CONFIG_INT:
					*(int *)option->val.intptr = repmgr_atoi(value, name, error_list, option->minval);
					break;
				case CONFIG_STRING:
					strncpy((char *)option->val.strptr, value, option->strmaxlen);
					break;
				case CONFIG_BOOL:
					*(bool *)option->val.boolptr = parse_bool(value, name, error_list);
					break;
				case CONFIG_FAILOVER_MODE:
				{
					if (strcmp(value, "manual") == 0)
					{
						*(failover_mode_opt *)option->val.failovermodeptr = FAILOVER_MANUAL;
					}
					else if (strcmp(value, "automatic") == 0)
					{
						*(failover_mode_opt *)option->val.failovermodeptr = FAILOVER_AUTOMATIC;
					}
					else
					{
						item_list_append_format(error_list,
												_("value for \"%s\" must be \"automatic\" or \"manual\"\n"),
												name);
					}
					break;
				}
				case CONFIG_CONNECTION_CHECK_TYPE:
				{
					if (strcasecmp(value, "ping") == 0)
					{
						*(ConnectionCheckType *)option->val.checktypeptr = CHECK_PING;
					}
					else if (strcasecmp(value, "connection") == 0)
					{
						*(ConnectionCheckType *)option->val.checktypeptr = CHECK_CONNECTION;
					}
					else if (strcasecmp(value, "query") == 0)
					{
						*(ConnectionCheckType *)option->val.checktypeptr = CHECK_QUERY;
					}
					else
					{
						item_list_append_format(error_list,
												_("value for \"%s\" must be \"ping\", \"connection\" or \"query\"\n"),
												name);
					}
					break;
				}
				case CONFIG_EVENT_NOTIFICATION_LIST:
				{
					parse_event_notifications_list((EventNotificationList *)&option->val.notificationlistptr, value);
				}
				default:
					log_error("encountered unknown configuration type %i when processing \"%s\"",
							  (int)option->type,
							  option->name);
			}

		}
		i++;
		option = &config_file_options2[i];
	} while (option->name);


	/* If we reach here, the configuration item is either deprecated or unknown */
	if (strcmp(name, "cluster") == 0)
	{
		item_list_append(warning_list,
						 _("parameter \"cluster\" is deprecated and will be ignored"));
	}
	else if (strcmp(name, "node") == 0)
	{
		item_list_append(warning_list,
						 _("parameter \"node\" has been renamed to \"node_id\""));
	}
	else if (strcmp(name, "upstream_node") == 0)
	{
		item_list_append(warning_list,
						 _("parameter \"upstream_node\" has been removed; use \"--upstream-node-id\" when cloning a standby"));
	}
	else if (strcmp(name, "loglevel") == 0)
	{
		item_list_append(warning_list,
						 _("parameter \"loglevel\" has been renamed to \"log_level\""));
	}
	else if (strcmp(name, "logfacility") == 0)
	{
		item_list_append(warning_list,
						 _("parameter \"logfacility\" has been renamed to \"log_facility\""));
	}
	else if (strcmp(name, "logfile") == 0)
	{
		item_list_append(warning_list,
						 _("parameter \"logfile\" has been renamed to \"log_file\""));
	}
	else if (strcmp(name, "master_reponse_timeout") == 0)
	{
		item_list_append(warning_list,
						 _("parameter \"master_reponse_timeout\" has been removed; use \"async_query_timeout\" instead"));
	}
	else if (strcmp(name, "retry_promote_interval_secs") == 0)
	{
		item_list_append(warning_list,
						 _("parameter \"retry_promote_interval_secs\" has been removed; use \"primary_notification_timeout\" instead"));
	}
	else
	{
		// why not just append to the warning list?
		//log_warning(_("%s/%s: unknown name/value pair provided; ignoring"), name, value);
	}

}

/*
 * This creates a parsed representation of the configuration file in a location provided
 * by the caller.
 */
static void
_parse_config(t_configuration_options *options, ItemList *error_list, ItemList *warning_list)
{
	FILE	   *fp;
	char		base_directory[MAXPGPATH];

	/*
	 * Clear lists pointing to allocated memory
	 */

	clear_event_notification_list(&options->event_notifications);
	tablespace_list_free(options);

	/* Initialize configuration options with sensible defaults */

	/*-----------------
	 * node information
	 *-----------------
	 */
	options->node_id = UNKNOWN_NODE_ID;
	memset(options->node_name, 0, sizeof(options->node_name));
	memset(options->conninfo, 0, sizeof(options->conninfo));
	memset(options->replication_user, 0, sizeof(options->replication_user));
	memset(options->data_directory, 0, sizeof(options->data_directory));
	memset(options->config_directory, 0, sizeof(options->data_directory));
	memset(options->pg_bindir, 0, sizeof(options->pg_bindir));
	memset(options->repmgr_bindir, 0, sizeof(options->repmgr_bindir));
	options->replication_type = REPLICATION_TYPE_PHYSICAL;

	/*-------------
	 * log settings
	 *
	 * NOTE: the default for "log_level" is set in log.c and does not need
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
	options->wal_receive_check_timeout = DEFAULT_WAL_RECEIVE_CHECK_TIMEOUT;

	/*------------------------
	 * node rejoin settings
	 *------------------------
	 */

	options->node_rejoin_timeout = DEFAULT_NODE_REJOIN_TIMEOUT;

	/*------------------------
	 * node check settings
	 *------------------------
	 */
	options->archive_ready_warning = DEFAULT_ARCHIVE_READY_WARNING;
	options->archive_ready_critical = DEFAULT_ARCHIVE_READY_CRITICAL;
	options->replication_lag_warning = 	DEFAULT_REPLICATION_LAG_WARNING;
	options->replication_lag_critical = DEFAULT_REPLICATION_LAG_CRITICAL;

	/*-------------
	 * witness settings
	 *-------------
	 */
	options->witness_sync_interval = DEFAULT_WITNESS_SYNC_INTERVAL;

	/*-----------------
	 * repmgrd settings
	 *-----------------
	 */
	options->failover = FAILOVER_MANUAL;
	memset(options->location, 0, sizeof(options->location));
	strncpy(options->location, DEFAULT_LOCATION, sizeof(options->location));
	options->priority = DEFAULT_PRIORITY;
	memset(options->promote_command, 0, sizeof(options->promote_command));
	memset(options->follow_command, 0, sizeof(options->follow_command));
	options->monitor_interval_secs = DEFAULT_MONITORING_INTERVAL;
	/* default to 6 reconnection attempts at intervals of 10 seconds */
	options->reconnect_attempts = DEFAULT_RECONNECTION_ATTEMPTS;
	options->reconnect_interval = DEFAULT_RECONNECTION_INTERVAL;
	options->monitoring_history = false;
	options->degraded_monitoring_timeout = -1;
	options->async_query_timeout = DEFAULT_ASYNC_QUERY_TIMEOUT;
	options->primary_notification_timeout = DEFAULT_PRIMARY_NOTIFICATION_TIMEOUT;
	options->repmgrd_standby_startup_timeout = -1; /* defaults to "standby_reconnect_timeout" if not set */
	memset(options->repmgrd_pid_file, 0, sizeof(options->repmgrd_pid_file));
	options->standby_disconnect_on_failover = false;
	options->sibling_nodes_disconnect_timeout = DEFAULT_SIBLING_NODES_DISCONNECT_TIMEOUT;
	options->connection_check_type = CHECK_PING;
	options->primary_visibility_consensus = false;
	memset(options->failover_validation_command, 0, sizeof(options->failover_validation_command));
	options->election_rerun_interval = DEFAULT_ELECTION_RERUN_INTERVAL;

	options->child_nodes_check_interval = DEFAULT_CHILD_NODES_CHECK_INTERVAL;
	options->child_nodes_disconnect_min_count = DEFAULT_CHILD_NODES_DISCONNECT_MIN_COUNT;
	options->child_nodes_connected_min_count = DEFAULT_CHILD_NODES_CONNECTED_MIN_COUNT;
	options->child_nodes_connected_include_witness = DEFAULT_CHILD_NODES_CONNECTED_INCLUDE_WITNESS;
	options->child_nodes_disconnect_timeout = DEFAULT_CHILD_NODES_DISCONNECT_TIMEOUT;
	memset(options->child_nodes_disconnect_command, 0, sizeof(options->child_nodes_disconnect_command));

	/*-------------------------
	 * service command settings
	 *-------------------------
	 */
	memset(options->pg_ctl_options, 0, sizeof(options->pg_ctl_options));
	memset(options->service_start_command, 0, sizeof(options->service_start_command));
	memset(options->service_stop_command, 0, sizeof(options->service_stop_command));
	memset(options->service_restart_command, 0, sizeof(options->service_restart_command));
	memset(options->service_reload_command, 0, sizeof(options->service_reload_command));
	memset(options->service_promote_command, 0, sizeof(options->service_promote_command));

	/*---------------------------------
	 * repmgrd service command settings
	 *---------------------------------
	 */
	memset(options->repmgrd_service_start_command, 0, sizeof(options->repmgrd_service_start_command));
	memset(options->repmgrd_service_stop_command, 0, sizeof(options->repmgrd_service_stop_command));

	/*----------------------------
	 * event notification settings
	 *----------------------------
	 */
	memset(options->event_notification_command, 0, sizeof(options->event_notification_command));
	memset(options->event_notifications_orig, 0, sizeof(options->event_notifications_orig));
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


	/*
	 * A configuration file has been found, either provided by the user or
	 * found in one of the default locations. Sanity check whether we
	 * can open it, and fail with an error about the nature of the file
	 * (provided or default) if not. We do this here rather than having
	 * to teach the configuration file parser the difference.
	 */

	fp = fopen(config_file_path, "r");

	if (fp == NULL)
	{
		if (config_file_provided)
		{
			log_error(_("unable to open provided configuration file \"%s\"; terminating"),
					  config_file_path);
		}
		else
		{
			log_error(_("unable to open default configuration file \"%s\"; terminating"),
					  config_file_path);
		}

		exit(ERR_BAD_CONFIG);
	}

	fclose(fp);


	strncpy(base_directory, config_file_path, MAXPGPATH);
	canonicalize_path(base_directory);
	get_parent_directory(base_directory);

	// XXX fail here if processing issue found
	(void) ProcessRepmgrConfigFile(config_file_path, base_directory, options, error_list, warning_list);


	/* check required parameters */
	if (options->node_id == UNKNOWN_NODE_ID)
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
		 * Basic sanity check of provided conninfo string; this will catch any
		 * invalid parameters (but not values).
		 */
		char	   *conninfo_errmsg = NULL;

		if (validate_conninfo_string(options->conninfo, &conninfo_errmsg) == false)
		{
			PQExpBufferData error_message_buf;
			initPQExpBuffer(&error_message_buf);

			appendPQExpBuffer(&error_message_buf,
							  _("\"conninfo\": %s	(provided: \"%s\")"),
							  conninfo_errmsg,
							  options->conninfo);

			item_list_append(error_list, error_message_buf.data);
			termPQExpBuffer(&error_message_buf);
		}
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


void
parse_configuration_item(t_configuration_options *options, ItemList *error_list, ItemList *warning_list, const char *name, const char *value)
{
	if (strcmp(name, "node_id") == 0)
	{
		options->node_id = repmgr_atoi(value, name, error_list, MIN_NODE_ID);
	}
	else if (strcmp(name, "node_name") == 0)
	{
		if (strlen(value) < sizeof(options->node_name))
			strncpy(options->node_name, value, sizeof(options->node_name));
		else
			item_list_append_format(error_list,
									_("value for \"node_name\" must contain fewer than %lu characters"),
									sizeof(options->node_name));
	}
	else if (strcmp(name, "conninfo") == 0)
	{
		strncpy(options->conninfo, value, MAXLEN);
	}
	else if (strcmp(name, "data_directory") == 0)
	{
		strncpy(options->data_directory, value, MAXPGPATH);
		canonicalize_path(options->data_directory);
	}
	else if (strcmp(name, "config_directory") == 0)
	{
		strncpy(options->config_directory, value, MAXPGPATH);
		canonicalize_path(options->config_directory);
	}
	else if (strcmp(name, "replication_user") == 0)
	{
		if (strlen(value) < sizeof(options->replication_user))
			strncpy(options->replication_user, value, sizeof(options->replication_user));
		else
			item_list_append_format(error_list,
									_("value for \"replication_user\" must contain fewer than %lu characters"),
									sizeof(options->replication_user));
	}
	else if (strcmp(name, "pg_bindir") == 0)
		strncpy(options->pg_bindir, value, MAXPGPATH);
	else if (strcmp(name, "repmgr_bindir") == 0)
		strncpy(options->repmgr_bindir, value, MAXPGPATH);

	else if (strcmp(name, "replication_type") == 0)
	{
		if (strcmp(value, "physical") == 0)
			options->replication_type = REPLICATION_TYPE_PHYSICAL;
		else
			item_list_append(error_list, _("value for \"replication_type\" must be \"physical\""));
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
	else if (strcmp(name, "wal_receive_check_timeout") == 0)
		options->wal_receive_check_timeout = repmgr_atoi(value, name, error_list, 0);

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
		strncpy(options->location, value, sizeof(options->location));
	else if (strcmp(name, "promote_command") == 0)
		strncpy(options->promote_command, value, sizeof(options->promote_command));
	else if (strcmp(name, "follow_command") == 0)
		strncpy(options->follow_command, value, sizeof(options->follow_command));
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
	else if (strcmp(name, "standby_disconnect_on_failover") == 0)
		options->standby_disconnect_on_failover = parse_bool(value, name, error_list);
	else if (strcmp(name, "sibling_nodes_disconnect_timeout") == 0)
		options->sibling_nodes_disconnect_timeout = repmgr_atoi(value, name, error_list, 0);
	else if (strcmp(name, "connection_check_type") == 0)
	{
		if (strcasecmp(value, "ping") == 0)
		{
			options->connection_check_type = CHECK_PING;
		}
		else if (strcasecmp(value, "connection") == 0)
		{
			options->connection_check_type = CHECK_CONNECTION;
		}
		else if (strcasecmp(value, "query") == 0)
		{
			options->connection_check_type = CHECK_QUERY;
		}
		else
		{
			item_list_append(error_list,
							 _("value for \"connection_check_type\" must be \"ping\", \"connection\" or \"query\"\n"));
		}
	}
	else if (strcmp(name, "primary_visibility_consensus") == 0)
		options->primary_visibility_consensus = parse_bool(value, name, error_list);
	else if (strcmp(name, "failover_validation_command") == 0)
		strncpy(options->failover_validation_command, value, sizeof(options->failover_validation_command));
	else if (strcmp(name, "election_rerun_interval") == 0)
		options->election_rerun_interval = repmgr_atoi(value, name, error_list, 0);
	else if (strcmp(name, "child_nodes_check_interval") == 0)
		options->child_nodes_check_interval = repmgr_atoi(value, name, error_list, 1);
	else if (strcmp(name, "child_nodes_disconnect_command") == 0)
		snprintf(options->child_nodes_disconnect_command, sizeof(options->child_nodes_disconnect_command), "%s", value);
	else if (strcmp(name, "child_nodes_disconnect_min_count") == 0)
		options->child_nodes_disconnect_min_count = repmgr_atoi(value, name, error_list, -1);
	else if (strcmp(name, "child_nodes_connected_min_count") == 0)
		options->child_nodes_connected_min_count = repmgr_atoi(value, name, error_list, -1);
	else if (strcmp(name, "child_nodes_connected_include_witness") == 0)
		options->child_nodes_connected_include_witness = parse_bool(value, name, error_list);
	else if (strcmp(name, "child_nodes_disconnect_timeout") == 0)
		options->child_nodes_disconnect_timeout = repmgr_atoi(value, name, error_list, 0);

	/* witness settings */
	else if (strcmp(name, "witness_sync_interval") == 0)
		options->witness_sync_interval = repmgr_atoi(value, name, error_list, 1);

	/* service settings */
	else if (strcmp(name, "pg_ctl_options") == 0)
		strncpy(options->pg_ctl_options, value, sizeof(options->pg_ctl_options));
	else if (strcmp(name, "service_start_command") == 0)
		strncpy(options->service_start_command, value, sizeof(options->service_start_command));
	else if (strcmp(name, "service_stop_command") == 0)
		strncpy(options->service_stop_command, value, sizeof(options->service_stop_command));
	else if (strcmp(name, "service_restart_command") == 0)
		strncpy(options->service_restart_command, value, sizeof(options->service_restart_command));
	else if (strcmp(name, "service_reload_command") == 0)
		strncpy(options->service_reload_command, value, sizeof(options->service_reload_command));
	else if (strcmp(name, "service_promote_command") == 0)
		strncpy(options->service_promote_command, value, sizeof(options->service_promote_command));

	/* repmgrd service settings */
	else if (strcmp(name, "repmgrd_service_start_command") == 0)
		strncpy(options->repmgrd_service_start_command, value, sizeof(options->repmgrd_service_start_command));
	else if (strcmp(name, "repmgrd_service_stop_command") == 0)
		strncpy(options->repmgrd_service_stop_command, value, sizeof(options->repmgrd_service_stop_command));


	/* event notification settings */
	else if (strcmp(name, "event_notification_command") == 0)
		strncpy(options->event_notification_command, value, sizeof(options->event_notification_command));
	else if (strcmp(name, "event_notifications") == 0)
	{
		/* store unparsed value for comparison when reloading config */
		strncpy(options->event_notifications_orig, value, sizeof(options->event_notifications_orig));
		parse_event_notifications_list(&options->event_notifications, value);
	}

	/* barman settings */
	else if (strcmp(name, "barman_host") == 0)
		strncpy(options->barman_host, value, sizeof(options->barman_host));
	else if (strcmp(name, "barman_server") == 0)
		strncpy(options->barman_server, value, sizeof(options->barman_server));
	else if (strcmp(name, "barman_config") == 0)
		strncpy(options->barman_config, value, sizeof(options->barman_config));

	/* rsync/ssh settings */
	else if (strcmp(name, "rsync_options") == 0)
		strncpy(options->rsync_options, value, sizeof(options->rsync_options));
	else if (strcmp(name, "ssh_options") == 0)
		strncpy(options->ssh_options, value, sizeof(options->ssh_options));

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
	}
	else if (strcmp(name, "node") == 0)
	{
		item_list_append(warning_list,
						 _("parameter \"node\" has been renamed to \"node_id\""));
	}
	else if (strcmp(name, "upstream_node") == 0)
	{
		item_list_append(warning_list,
						 _("parameter \"upstream_node\" has been removed; use \"--upstream-node-id\" when cloning a standby"));
	}
	else if (strcmp(name, "loglevel") == 0)
	{
		item_list_append(warning_list,
						 _("parameter \"loglevel\" has been renamed to \"log_level\""));
	}
	else if (strcmp(name, "logfacility") == 0)
	{
		item_list_append(warning_list,
						 _("parameter \"logfacility\" has been renamed to \"log_facility\""));
	}
	else if (strcmp(name, "logfile") == 0)
	{
		item_list_append(warning_list,
						 _("parameter \"logfile\" has been renamed to \"log_file\""));
	}
	else if (strcmp(name, "master_reponse_timeout") == 0)
	{
		item_list_append(warning_list,
						 _("parameter \"master_reponse_timeout\" has been removed; use \"async_query_timeout\" instead"));
	}
	else if (strcmp(name, "retry_promote_interval_secs") == 0)
	{
		item_list_append(warning_list,
						 _("parameter \"retry_promote_interval_secs\" has been removed; use \"primary_notification_timeout\" instead"));
	}
	else
	{
		log_warning(_("%s/%s: unknown name/value pair provided; ignoring"), name, value);
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
 * changeable options (keep the list in "doc/repmgrd-configuration.xml" in sync
 * with these):
 *
 * - async_query_timeout
 * - child_nodes_check_interval
 * - child_nodes_connected_min_count
 * - child_nodes_connected_include_witness
 * - child_nodes_disconnect_command
 * - child_nodes_disconnect_min_count
 * - child_nodes_disconnect_timeout
 * - connection_check_type
 * - conninfo
 * - degraded_monitoring_timeout
 * - event_notification_command
 * - event_notifications
 * - failover
 * - failover_validation_command
 * - follow_command
 * - log_facility
 * - log_file
 * - log_level
 * - log_status_interval
 * - monitor_interval_secs
 * - monitoring_history
 * - primary_notification_timeout
 * - primary_visibility_consensus
 * - promote_command
 * - reconnect_attempts
 * - reconnect_interval
 * - repmgrd_standby_startup_timeout
 * - retry_promote_interval_secs
 * - sibling_nodes_disconnect_timeout
 * - standby_disconnect_on_failover
 *
 *
 * Not publicly documented:
 * - promote_delay
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
 *
 * Returns "true" if the configuration was successfully changed, otherwise "false".
 */
bool
reload_config(t_server_type server_type)
{
	bool		log_config_changed = false;

	ItemList config_errors = {NULL, NULL};
	ItemList config_warnings = {NULL, NULL};
	ItemList config_changes = {NULL, NULL};

	t_configuration_options orig_config_file_options;

	copy_config_file_options(&config_file_options, &orig_config_file_options);

	log_info(_("reloading configuration file"));
	log_detail(_("using file \"%s\""), config_file_path);

	/*
	 * _parse_config() will sanity-check the provided values and put any
	 * errors/warnings in the provided lists; no need to add further sanity
	 * checks here. We do still need to check for repmgrd-specific
	 * requirements.
	 */
	_parse_config(&config_file_options, &config_errors, &config_warnings);

	if (config_file_options.failover == FAILOVER_AUTOMATIC
		&& (server_type == PRIMARY || server_type == STANDBY))
	{
		if (config_file_options.promote_command[0] == '\0')
		{
			item_list_append(&config_errors, _("\"promote_command\": required parameter was not found"));
		}

		if (config_file_options.follow_command[0] == '\0')
		{
			item_list_append(&config_errors, _("\"follow_command\": required parameter was not found"));
		}
	}


	/* The following options cannot be changed */

	if (config_file_options.node_id != orig_config_file_options.node_id)
	{
		item_list_append_format(&config_errors,
								_("\"node_id\" cannot be changed, retaining current configuration %i %i"),
								config_file_options.node_id,
								orig_config_file_options.node_id);
	}

	if (strncmp(config_file_options.node_name, orig_config_file_options.node_name, sizeof(config_file_options.node_name)) != 0)
	{
		item_list_append(&config_errors,
						 _("\"node_name\" cannot be changed, keeping current configuration"));
	}


	/*
	 * conninfo
	 *
	 * _parse_config() will already have sanity-checked the string; we do that here
	 * again so we can avoid trying to connect with a known bad string
	 */
	if (strncmp(config_file_options.conninfo, orig_config_file_options.conninfo, sizeof(config_file_options.conninfo)) != 0 && validate_conninfo_string(config_file_options.conninfo, NULL))
	{
		PGconn	   *conn;

		/* Test conninfo string works */
		conn = establish_db_connection(config_file_options.conninfo, false);

		if (!conn || (PQstatus(conn) != CONNECTION_OK))
		{
			item_list_append_format(&config_errors,
									_("provided \"conninfo\" string \"%s\" is not valid"),
									config_file_options.conninfo);
		}
		else
		{
			item_list_append_format(&config_changes,
									_("\"conninfo\" changed from \"%s\" to \"%s\""),
									orig_config_file_options.conninfo,
									config_file_options.conninfo);
		}

		PQfinish(conn);
	}


	/*
	 * If any issues encountered, raise an error and roll back to the original
	 * configuration
	 */
	if (config_errors.head != NULL)
	{
		ItemListCell *cell = NULL;
		PQExpBufferData errors;

		log_error(_("one or more errors encountered while parsing the configuration file"));

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

		log_notice(_("the current configuration has been retained unchanged"));

		copy_config_file_options(&orig_config_file_options, &config_file_options);

		return false;
	}


	/*
	 * No configuration problems detected - log any changed values.
	 *
	 * NB: keep these in the same order as in configfile.h to make it easier
	 * to manage them
	 */


	/* async_query_timeout */
	if (config_file_options.async_query_timeout != orig_config_file_options.async_query_timeout)
	{
		item_list_append_format(&config_changes,
								_("\"async_query_timeout\" changed from \"%i\" to \"%i\""),
								orig_config_file_options.async_query_timeout,
								config_file_options.async_query_timeout);
	}

	/* child_nodes_check_interval */
	if (config_file_options.child_nodes_check_interval != orig_config_file_options.child_nodes_check_interval)
	{
		item_list_append_format(&config_changes,
								_("\"child_nodes_check_interval\" changed from \"%i\" to \"%i\""),
								orig_config_file_options.child_nodes_check_interval,
								config_file_options.child_nodes_check_interval);
	}

	/* child_nodes_disconnect_command */
	if (strncmp(config_file_options.child_nodes_disconnect_command, orig_config_file_options.child_nodes_disconnect_command, sizeof(config_file_options.child_nodes_disconnect_command)) != 0)
	{
		item_list_append_format(&config_changes,
								_("\"child_nodes_disconnect_command\" changed from \"%s\" to \"%s\""),
								orig_config_file_options.child_nodes_disconnect_command,
								config_file_options.child_nodes_disconnect_command);
	}

	/* child_nodes_disconnect_min_count */
	if (config_file_options.child_nodes_disconnect_min_count != orig_config_file_options.child_nodes_disconnect_min_count)
	{
		item_list_append_format(&config_changes,
								_("\"child_nodes_disconnect_min_count\" changed from \"%i\" to \"%i\""),
								orig_config_file_options.child_nodes_disconnect_min_count,
								config_file_options.child_nodes_disconnect_min_count);
	}

	/* child_nodes_connected_min_count */
	if (config_file_options.child_nodes_connected_min_count != orig_config_file_options.child_nodes_connected_min_count)
	{
		item_list_append_format(&config_changes,
								_("\"child_nodes_connected_min_count\" changed from \"%i\" to \"%i\""),
								orig_config_file_options.child_nodes_connected_min_count,
								config_file_options.child_nodes_connected_min_count);
	}

	/* child_nodes_connected_include_witness */
	if (config_file_options.child_nodes_connected_include_witness != orig_config_file_options.child_nodes_connected_include_witness)
	{
		item_list_append_format(&config_changes,
								_("\"child_nodes_connected_include_witness\" changed from \"%s\" to \"%s\""),
								format_bool(orig_config_file_options.child_nodes_connected_include_witness),
								format_bool(config_file_options.child_nodes_connected_include_witness));
	}

	/* child_nodes_disconnect_timeout */
	if (config_file_options.child_nodes_disconnect_timeout != orig_config_file_options.child_nodes_disconnect_timeout)
	{
		item_list_append_format(&config_changes,
								_("\"child_nodes_disconnect_timeout\" changed from \"%i\" to \"%i\""),
								orig_config_file_options.child_nodes_disconnect_timeout,
								config_file_options.child_nodes_disconnect_timeout);
	}


	/* degraded_monitoring_timeout */
	if (config_file_options.degraded_monitoring_timeout != orig_config_file_options.degraded_monitoring_timeout)
	{
		item_list_append_format(&config_changes,
								_("\"degraded_monitoring_timeout\" changed from \"%i\" to \"%i\""),
								orig_config_file_options.degraded_monitoring_timeout,
								config_file_options.degraded_monitoring_timeout);
	}

	/* event_notification_command */
	if (strncmp(config_file_options.event_notification_command, orig_config_file_options.event_notification_command, sizeof(config_file_options.event_notification_command)) != 0)
	{
		item_list_append_format(&config_changes,
								_("\"event_notification_command\" changed from \"%s\" to \"%s\""),
								orig_config_file_options.event_notification_command,
								config_file_options.event_notification_command);
	}

	/* event_notifications */
	if (strncmp(config_file_options.event_notifications_orig, orig_config_file_options.event_notifications_orig, sizeof(config_file_options.event_notifications_orig)) != 0)
	{
		item_list_append_format(&config_changes,
								_("\"event_notifications\" changed from \"%s\" to \"%s\""),
								orig_config_file_options.event_notifications_orig,
								config_file_options.event_notifications_orig);
	}

	/* failover */
	if (config_file_options.failover != orig_config_file_options.failover)
	{
		item_list_append_format(&config_changes,
								_("\"failover\" changed from \"%s\" to \"%s\""),
								format_failover_mode(orig_config_file_options.failover),
								format_failover_mode(config_file_options.failover));
	}

	/* follow_command */
	if (strncmp(config_file_options.follow_command, orig_config_file_options.follow_command, sizeof(config_file_options.follow_command)) != 0)
	{
		item_list_append_format(&config_changes,
								_("\"follow_command\" changed from \"%s\" to \"%s\""),
								orig_config_file_options.follow_command,
								config_file_options.follow_command);
	}

	/* monitor_interval_secs */
	if (config_file_options.monitor_interval_secs != orig_config_file_options.monitor_interval_secs)
	{
		item_list_append_format(&config_changes,
								_("\"monitor_interval_secs\" changed from \"%i\" to \"%i\""),
								orig_config_file_options.monitor_interval_secs,
								config_file_options.monitor_interval_secs);
	}

	/* monitoring_history */
	if (config_file_options.monitoring_history != orig_config_file_options.monitoring_history)
	{
		item_list_append_format(&config_changes,
								_("\"monitoring_history\" changed from \"%s\" to \"%s\""),
								format_bool(orig_config_file_options.monitoring_history),
								format_bool(config_file_options.monitoring_history));
	}

	/* primary_notification_timeout */
	if (config_file_options.primary_notification_timeout != orig_config_file_options.primary_notification_timeout)
	{
		item_list_append_format(&config_changes,
								_("\"primary_notification_timeout\" changed from \"%i\" to \"%i\""),
								orig_config_file_options.primary_notification_timeout,
								config_file_options.primary_notification_timeout);
	}

	/* promote_command */
	if (strncmp(config_file_options.promote_command, orig_config_file_options.promote_command, sizeof(config_file_options.promote_command)) != 0)
	{
		item_list_append_format(&config_changes,
								_("\"promote_command\" changed from \"%s\" to \"%s\""),
								orig_config_file_options.promote_command,
								config_file_options.promote_command);
	}

	/* promote_delay (for testing use only; not documented */
	if (config_file_options.promote_delay != orig_config_file_options.promote_delay)
	{
		item_list_append_format(&config_changes,
								_("\"promote_delay\" changed from \"%i\" to \"%i\""),
								orig_config_file_options.promote_delay,
								config_file_options.promote_delay);
	}

	/* reconnect_attempts */
	if (config_file_options.reconnect_attempts != orig_config_file_options.reconnect_attempts)
	{
		item_list_append_format(&config_changes,
								_("\"reconnect_attempts\" changed from \"%i\" to \"%i\""),
								orig_config_file_options.reconnect_attempts,
								config_file_options.reconnect_attempts);
	}

	/* reconnect_interval */
	if (config_file_options.reconnect_interval != orig_config_file_options.reconnect_interval)
	{
		item_list_append_format(&config_changes,
								_("\"reconnect_interval\" changed from \"%i\" to \"%i\""),
								orig_config_file_options.reconnect_interval,
								config_file_options.reconnect_interval);
	}

	/* repmgrd_standby_startup_timeout */
	if (config_file_options.repmgrd_standby_startup_timeout != orig_config_file_options.repmgrd_standby_startup_timeout)
	{
		item_list_append_format(&config_changes,
								_("\"repmgrd_standby_startup_timeout\" changed from \"%i\" to \"%i\""),
								orig_config_file_options.repmgrd_standby_startup_timeout,
								config_file_options.repmgrd_standby_startup_timeout);
	}

	/* standby_disconnect_on_failover */
	if (config_file_options.standby_disconnect_on_failover != orig_config_file_options.standby_disconnect_on_failover)
	{
		item_list_append_format(&config_changes,
								_("\"standby_disconnect_on_failover\" changed from \"%s\" to \"%s\""),
								format_bool(orig_config_file_options.standby_disconnect_on_failover),
								format_bool(config_file_options.standby_disconnect_on_failover));
	}

	/* sibling_nodes_disconnect_timeout */
	if (config_file_options.sibling_nodes_disconnect_timeout != orig_config_file_options.sibling_nodes_disconnect_timeout)
	{
		item_list_append_format(&config_changes,
								_("\"sibling_nodes_disconnect_timeout\" changed from \"%i\" to \"%i\""),
								orig_config_file_options.sibling_nodes_disconnect_timeout,
								config_file_options.sibling_nodes_disconnect_timeout);
	}

	/* connection_check_type */
	if (config_file_options.connection_check_type != orig_config_file_options.connection_check_type)
	{
		item_list_append_format(&config_changes,
								_("\"connection_check_type\" changed from \"%s\" to \"%s\""),
								print_connection_check_type(orig_config_file_options.connection_check_type),
								print_connection_check_type(config_file_options.connection_check_type));
	}

	/* primary_visibility_consensus */
	if (config_file_options.primary_visibility_consensus != orig_config_file_options.primary_visibility_consensus)
	{
		item_list_append_format(&config_changes,
								_("\"primary_visibility_consensus\" changed from \"%s\" to \"%s\""),
								format_bool(orig_config_file_options.primary_visibility_consensus),
								format_bool(config_file_options.primary_visibility_consensus));
	}

	/* failover_validation_command */
	if (strncmp(config_file_options.failover_validation_command, orig_config_file_options.failover_validation_command, sizeof(config_file_options.failover_validation_command)) != 0)
	{
		item_list_append_format(&config_changes,
								_("\"failover_validation_command\" changed from \"%s\" to \"%s\""),
								orig_config_file_options.failover_validation_command,
								config_file_options.failover_validation_command);
	}

	/*
	 * Handle changes to logging configuration
	 */

	/* log_facility */
	if (strncmp(config_file_options.log_facility, orig_config_file_options.log_facility, sizeof(config_file_options.log_facility)) != 0)
	{
		item_list_append_format(&config_changes,
								_("\"log_facility\" changed from \"%s\" to \"%s\""),
								orig_config_file_options.log_facility,
								config_file_options.log_facility);
	}

	/* log_file */
	if (strncmp(config_file_options.log_file, orig_config_file_options.log_file, sizeof(config_file_options.log_file)) != 0)
	{
		item_list_append_format(&config_changes,
								_("\"log_file\" changed from \"%s\" to \"%s\""),
								orig_config_file_options.log_file,
								config_file_options.log_file);
	}


	/* log_level */
	if (strncmp(config_file_options.log_level, orig_config_file_options.log_level, sizeof(config_file_options.log_level)) != 0)
	{
		item_list_append_format(&config_changes,
								_("\"log_level\" changed from \"%s\" to \"%s\""),
								orig_config_file_options.log_level,
								config_file_options.log_level);
	}

	/* log_status_interval */
	if (config_file_options.log_status_interval != orig_config_file_options.log_status_interval)
	{
		item_list_append_format(&config_changes,
								_("\"log_status_interval\" changed from \"%i\" to \"%i\""),
								orig_config_file_options.log_status_interval,
								config_file_options.log_status_interval);
	}


	if (log_config_changed == true)
	{
		log_notice(_("restarting logging with changed parameters"));
		logger_shutdown();
		logger_init(&config_file_options, progname());
		log_notice(_("configuration file reloaded with changed parameters"));
	}

	if (config_changes.head != NULL)
	{
		ItemListCell *cell = NULL;
		PQExpBufferData detail;

		log_notice(_("configuration was successfully changed"));

		initPQExpBuffer(&detail);

		appendPQExpBufferStr(&detail,
							 _("following configuration items were changed:\n"));
		for (cell = config_changes.head; cell; cell = cell->next)
		{
			appendPQExpBuffer(&detail,
							  "  %s\n", cell->string);
		}

		log_detail("%s", detail.data);

		termPQExpBuffer(&detail);
	}
	else
	{
		log_info(_("configuration has not changed"));
	}

	/*
	 * parse_configuration_item() (called from _parse_config()) will add warnings
	 * about any deprecated configuration parameters; we'll dump these here as a reminder.
	 */
	if (config_warnings.head != NULL)
	{
		ItemListCell *cell = NULL;
		PQExpBufferData detail;

		log_warning(_("configuration file contains deprecated parameters"));

		initPQExpBuffer(&detail);

		appendPQExpBufferStr(&detail,
							 _("following parameters are deprecated:\n"));
		for (cell = config_warnings.head; cell; cell = cell->next)
		{
			appendPQExpBuffer(&detail,
							  "  %s\n", cell->string);
		}

		log_detail("%s", detail.data);

		termPQExpBuffer(&detail);
	}

	return config_changes.head == NULL ? false : true;
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
 * Copy a configuration file struct
 */

void
copy_config_file_options(t_configuration_options *original, t_configuration_options *copy)
{
	memcpy(copy, original, (int)sizeof(t_configuration_options));

	/* Copy structures which point to allocated memory */

	if (original->event_notifications.head != NULL)
	{
		/* For the event notifications, we can just reparse the string */
		parse_event_notifications_list(&copy->event_notifications, original->event_notifications_orig);
	}

	if (original->tablespace_mapping.head != NULL)
	{
		/*
		 * We allow multiple instances of "tablespace_mapping" in the configuration file
		 * which are appended to the list as they're encountered.
		 */
		tablespace_list_copy(original, copy);
	}
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


static void
tablespace_list_copy(t_configuration_options *original, t_configuration_options *copy)
{
	TablespaceListCell *orig_cell = original->tablespace_mapping.head;

	for (orig_cell = config_file_options.tablespace_mapping.head; orig_cell; orig_cell = orig_cell->next)
	{
		TablespaceListCell *copy_cell = (TablespaceListCell *) pg_malloc0(sizeof(TablespaceListCell));

		strncpy(copy_cell->old_dir, orig_cell->old_dir, sizeof(orig_cell->old_dir));
		strncpy(copy_cell->new_dir, orig_cell->new_dir, sizeof(orig_cell->new_dir));

		if (copy->tablespace_mapping.tail)
			copy->tablespace_mapping.tail->next = copy_cell;
		else
			copy->tablespace_mapping.head = copy_cell;

		copy->tablespace_mapping.tail = copy_cell;
	}
}


static void
tablespace_list_free(t_configuration_options *options)
{
	TablespaceListCell *cell = NULL;
	TablespaceListCell *next_cell = NULL;

	cell = options->tablespace_mapping.head;

	while (cell != NULL)
	{
		next_cell = cell->next;
		pfree(cell);
		cell = next_cell;
	}

	options->tablespace_mapping.head = NULL;
	options->tablespace_mapping.tail = NULL;
}


bool
modify_auto_conf(const char *data_dir, KeyValueList *items)
{
	PQExpBufferData auto_conf;
	PQExpBufferData auto_conf_tmp;
	PQExpBufferData auto_conf_contents;

	FILE	   *fp;
	mode_t		um;
	struct stat auto_conf_st;

	KeyValueList config = {NULL, NULL};
	KeyValueListCell *cell = NULL;

	bool	   success = true;

	initPQExpBuffer(&auto_conf);
	appendPQExpBuffer(&auto_conf, "%s/%s",
					  data_dir, PG_AUTOCONF_FILENAME);

	// XXX do we need this?
	fp = fopen(auto_conf.data, "r");

	if (fp == NULL)
	{
		fprintf(stderr, "unable to open \"%s\": %s\n",
				auto_conf.data,
				strerror(errno));
		termPQExpBuffer(&auto_conf);
		return false;
	}
	fclose(fp);

	success = ProcessPostgresConfigFile(auto_conf.data, NULL, &config, NULL, NULL);

	if (success == false)
	{
		fprintf(stderr, "unable to process \"%s\"\n",
				auto_conf.data);
		termPQExpBuffer(&auto_conf);
		return false;
	}

	/*
	 * Append requested items to items extracted from the existing file.
	 */
	for (cell = items->head; cell; cell = cell->next)
	{
		key_value_list_replace_or_set(&config,
									  cell->key,
									  cell->value);
	}

	initPQExpBuffer(&auto_conf_tmp);
	appendPQExpBuffer(&auto_conf_tmp, "%s.tmp",
					  auto_conf.data);

	initPQExpBuffer(&auto_conf_contents);

	/*
	 * Keep this in sync with src/backend/utils/misc/guc.c:write_auto_conf_file()
	 */
	appendPQExpBufferStr(&auto_conf_contents,
						 "# Do not edit this file manually!\n"
						 "# It will be overwritten by the ALTER SYSTEM command.\n");

	for (cell = config.head; cell; cell = cell->next)
	{
		appendPQExpBuffer(&auto_conf_contents,
						  "%s = '%s'\n",
						  cell->key, cell->value);
	}

	stat(auto_conf.data, &auto_conf_st);

	/*
	 * Set umask so the temporary file is created in the same mode as the original
	 * postgresql.auto.conf file.
	 */
	um = umask(~(auto_conf_st.st_mode));
	fp = fopen(auto_conf_tmp.data, "w");
	umask(um);

	if (fp == NULL)
	{
		fprintf(stderr, "unable to open \"%s\": %s\n",
				auto_conf_tmp.data,
				strerror(errno));
	}
	else
	{
		if (fwrite(auto_conf_contents.data, strlen(auto_conf_contents.data), 1, fp) != 1)
		{
			fclose(fp);
		}
		else
		{
			fclose(fp);

			/*
			 * Note: durable_rename() is not exposed to frontend code before Pg 10.
			 * We only really need to be modifying postgresql.auto.conf from Pg 12,
			 * but provide backwards compatibitilty for Pg 9.6 and earlier for the
			 * (unlikely) event that a repmgr built against one of those versions
			 * is being used against Pg 12 and later.
			 */

#if (PG_ACTUAL_VERSION_NUM >= 100000)
			if (durable_rename(auto_conf_tmp.data, auto_conf.data, LOG) != 0)
			{
				success = false;
			}
#else
			if (rename(auto_conf_tmp.data, auto_conf.data) < 0)
			{
				success = false;
			}
#endif
		}
	}

	termPQExpBuffer(&auto_conf);
	termPQExpBuffer(&auto_conf_tmp);
	termPQExpBuffer(&auto_conf_contents);

	key_value_list_free(&config);

	return success;
}


/*
 * parse_event_notifications_list()
 *
 *
 */

static void
parse_event_notifications_list(EventNotificationList *event_notifications, const char *arg)
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

			if (event_notifications->tail)
			{
				event_notifications->tail->next = cell;
			}
			else
			{
				event_notifications->head = cell;
			}

			event_notifications->tail = cell;

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
clear_event_notification_list(EventNotificationList *event_notifications)
{
	if (event_notifications->head != NULL)
	{
		EventNotificationListCell *cell;
		EventNotificationListCell *next_cell;

		cell = event_notifications->head;

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


	/*
	 * We're only interested in these options.
	 */

	static struct option long_options_10[] =
	{
		{"slot", required_argument, NULL, 'S'},
		{"wal-method", required_argument, NULL, 'X'},
		{"no-slot", no_argument, NULL, 1},
		{NULL, 0, NULL, 0}
	};

	/*
	 * Pre-PostgreSQL 10 options
	 */
	static struct option long_options_legacy[] =
	{
		{"slot", required_argument, NULL, 'S'},
		{"xlog-method", required_argument, NULL, 'X'},
		{NULL, 0, NULL, 0}
	};


	/* Don't attempt to tokenise an empty string */
	if (!strlen(pg_basebackup_options))
		return backup_options_ok;

	if (server_version_num >= 100000)
		long_options = long_options_10;
	else
		long_options = long_options_legacy;

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
				strncpy(backup_options->wal_method, optarg, MAXLEN);
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


const char *
print_connection_check_type(ConnectionCheckType type)
{
	switch (type)
	{
		case CHECK_PING:
			return "ping";
		case CHECK_QUERY:
			return "query";
		case CHECK_CONNECTION:
			return "connection";
	}

	/* should never reach here */
	return "UNKNOWN";
}



char *
print_event_notification_list(EventNotificationList *list)
{
	PQExpBufferData buf;
	char *ptr;
	EventNotificationListCell *cell;

	initPQExpBuffer(&buf);
	cell = list->head;

	while (cell != NULL)
	{
		appendPQExpBufferStr(&buf, cell->event_type);

		if (cell->next)
			appendPQExpBufferStr(&buf, ", ");

		cell = cell->next;
	}

	ptr = palloc0(strlen(buf.data) + 1);
	strncpy(ptr, buf.data, strlen(buf.data));

	termPQExpBuffer(&buf);

	return ptr;
}



const char *
format_failover_mode(failover_mode_opt failover)
{
	switch (failover)
	{
		case FAILOVER_MANUAL:
			return "manual";
		case FAILOVER_AUTOMATIC:
			return "automatic";
		default:
			return "unknown failover mode";
	}
}
