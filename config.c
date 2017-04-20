/*
 * config.c - parse repmgr.conf and other configuration-related functionality
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include <sys/stat.h>			/* for stat() */

#include "repmgr.h"
#include "config.h"
#include "log.h"

const static char *_progname = NULL;
static char config_file_path[MAXPGPATH];
static bool config_file_provided = false;
bool config_file_found = false;

static void	_parse_config(t_configuration_options *options, ItemList *error_list);
static void	exit_with_errors(ItemList *config_errors);


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

bool
load_config(const char *config_file, bool verbose, t_configuration_options *options, char *argv0)
{
	struct stat stat_config;

	/*
	 * If a configuration file was provided, check it exists, otherwise
	 * emit an error and terminate. We assume that if a user explicitly
	 * provides a configuration file, they'll want to make sure it's
	 * used and not fall back to any of the defaults.
	 */
	if (config_file[0])
	{
		strncpy(config_file_path, config_file, MAXPGPATH);
		canonicalize_path(config_file_path);

		if (stat(config_file_path, &stat_config) != 0)
		{
			log_error(_("provided configuration file \"%s\" not found: %s"),
					config_file,
					strerror(errno)
				);
			exit(ERR_BAD_CONFIG);
		}

		if (verbose == true)
		{
			log_notice(_("using configuration file \"%s\""), config_file);
		}

		config_file_provided = true;
		config_file_found = true;
	}


	/*
	 * If no configuration file was provided, attempt to find a default file
	 * in this order:
	 *	- current directory
	 *	- /etc/repmgr.conf
	 *	- default sysconfdir
	 *
	 * here we just check for the existence of the file; parse_config()
	 * will handle read errors etc.
	 *
	 * XXX modify this section so package maintainers can provide a patch
	 *     specifying location of a distribution-specific configuration file
	 */
	if (config_file_provided == false)
	{
		char		my_exec_path[MAXPGPATH];
		char		sysconf_etc_path[MAXPGPATH];

		/* 1. "./repmgr.conf" */
		if (verbose == true)
		{
			log_notice(_("looking for configuration file in current directory"));
		}

		snprintf(config_file_path, MAXPGPATH, "./%s", CONFIG_FILE_NAME);
		canonicalize_path(config_file_path);

		if (stat(config_file_path, &stat_config) == 0)
		{
			config_file_found = true;
			goto end_search;
		}

		/* 2. "/etc/repmgr.conf" */
		if (verbose == true)
		{
			log_notice(_("looking for configuration file in /etc"));
		}

		snprintf(config_file_path, MAXPGPATH, "/etc/%s", CONFIG_FILE_NAME);
		if (stat(config_file_path, &stat_config) == 0)
		{
			config_file_found = true;
			goto end_search;
		}

		/* 3. default sysconfdir */
		if (find_my_exec(argv0, my_exec_path) < 0)
		{
			log_error(_("%s: could not find own program executable"), argv0);
			exit(EXIT_FAILURE);
		}

		get_etc_path(my_exec_path, sysconf_etc_path);

		if (verbose == true)
		{
			log_notice(_("looking for configuration file in %s"), sysconf_etc_path);
		}

		snprintf(config_file_path, MAXPGPATH, "%s/%s", sysconf_etc_path, CONFIG_FILE_NAME);
		if (stat(config_file_path, &stat_config) == 0)
		{
			config_file_found = true;
			goto end_search;
		}

	end_search:
		if (config_file_found == true)
		{
			if (verbose == true)
			{
				log_notice(_("configuration file found at: %s"), config_file_path);
			}
		}
		else
		{
			if (verbose == true)
			{
				log_notice(_("no configuration file provided or found"));
			}
		}
	}

	return parse_config(options);
}

bool
parse_config(t_configuration_options *options)
{
	/* Collate configuration file errors here for friendlier reporting */
	static ItemList config_errors = { NULL, NULL };

	_parse_config(options, &config_errors);

	if (config_errors.head != NULL)
	{
		exit_with_errors(&config_errors);
	}

	return true;
}

static void
_parse_config(t_configuration_options *options, ItemList *error_list)
{
	FILE	   *fp;
	char	   *s,
				buf[MAXLINELENGTH];
	char		name[MAXLEN];
	char		value[MAXLEN];

	/* For sanity-checking provided conninfo string */
	PQconninfoOption *conninfo_options;
	char	   *conninfo_errmsg = NULL;

	bool		node_id_found = false;

	/* Initialize configuration options with sensible defaults */

	/* node information */
	options->node_id = UNKNOWN_NODE_ID;
	options->upstream_node_id = NO_UPSTREAM_NODE;
	memset(options->node_name, 0, sizeof(options->node_name));
	memset(options->conninfo, 0, sizeof(options->conninfo));
	memset(options->pg_bindir, 0, sizeof(options->pg_bindir));

	/*
	 * log settings
	 *
	 * note: the default for "loglevel" is set in log.c and does not need
	 * to be initialised here
	 */
	memset(options->logfacility, 0, sizeof(options->logfacility));
	memset(options->logfile, 0, sizeof(options->logfile));

	/* standby clone settings
	 * ----------------------- */
	options->use_replication_slots = false;
	memset(options->rsync_options, 0, sizeof(options->rsync_options));
	memset(options->ssh_options, 0, sizeof(options->ssh_options));
	memset(options->pg_basebackup_options, 0, sizeof(options->pg_basebackup_options));
	memset(options->restore_command, 0, sizeof(options->restore_command));
	options->tablespace_mapping.head = NULL;
	options->tablespace_mapping.tail = NULL;

	/* repmgrd settings
	 * ---------------- */
	options->failover_mode = MANUAL_FAILOVER;
	options->priority = DEFAULT_PRIORITY;
	memset(options->promote_command, 0, sizeof(options->promote_command));
	memset(options->follow_command, 0, sizeof(options->follow_command));
	options->monitor_interval_secs = 2;
	options->master_response_timeout = 60;
	/* default to 6 reconnection attempts at intervals of 10 seconds */
	options->reconnect_attempts = 6;
	options->reconnect_interval = 10;
	options->retry_promote_interval_secs = 300;
	options->monitoring_history = false;  /* new in 4.0, replaces --monitoring-history */

	/* witness settings
	 * ---------------- */

	/* default to resyncing repl_nodes table every 30 seconds on the witness server */
	options->witness_repl_nodes_sync_interval_secs = 30;

	/* service settings
	 * ---------------- */
	memset(options->pg_ctl_options, 0, sizeof(options->pg_ctl_options));
	memset(options->service_stop_command, 0, sizeof(options->service_stop_command));
	memset(options->service_start_command, 0, sizeof(options->service_start_command));
	memset(options->service_restart_command, 0, sizeof(options->service_restart_command));
	memset(options->service_reload_command, 0, sizeof(options->service_reload_command));
	memset(options->service_promote_command, 0, sizeof(options->service_promote_command));

	/* event notification settings
	 * --------------------------- */
	memset(options->event_notification_command, 0, sizeof(options->event_notification_command));
	options->event_notifications.head = NULL;
	options->event_notifications.tail = NULL;

	/* barman settings */
	memset(options->barman_server, 0, sizeof(options->barman_server));
	memset(options->barman_config, 0, sizeof(options->barman_config));
}


bool
reload_config(t_configuration_options *orig_options)
{
    return true;
}


static void
exit_with_errors(ItemList *config_errors)
{
}

void
item_list_append(ItemList *item_list, char *error_message)
{
	ItemListCell *cell;

	cell = (ItemListCell *) pg_malloc0(sizeof(ItemListCell));

	if (cell == NULL)
	{
		//log_err(_("unable to allocate memory; terminating.\n"));
		exit(ERR_BAD_CONFIG);
	}

	cell->string = pg_malloc0(MAXLEN);
	strncpy(cell->string, error_message, MAXLEN);

	if (item_list->tail)
	{
		item_list->tail->next = cell;
	}
	else
	{
		item_list->head = cell;
	}

	item_list->tail = cell;
}
