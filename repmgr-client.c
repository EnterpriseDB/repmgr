/*
 * repmgr-client.c - Command interpreter for the repmgr package
 *
 * Copyright (c) 2ndQuadrant, 2010-2020
 *
 * This module is a command-line utility to easily setup a cluster of
 * hot standby servers for an HA environment
 *
 * Commands implemented are:
 *
 * [ PRIMARY | MASTER ] REGISTER
 * [ PRIMARY | MASTER ] UNREGISTER
 *
 * STANDBY CLONE
 * STANDBY REGISTER
 * STANDBY UNREGISTER
 * STANDBY PROMOTE
 * STANDBY FOLLOW
 * STANDBY SWITCHOVER
 *
 * CLUSTER SHOW
 * CLUSTER EVENT
 * CLUSTER CROSSCHECK
 * CLUSTER MATRIX
 * CLUSTER CLEANUP
 *
 * NODE STATUS
 * NODE CHECK
 * NODE REJOIN
 * NODE SERVICE
 * NODE CONTROL
 *
 * SERVICE STATUS
 * SERVICE PAUSE
 * SERVICE UNPAUSE
 *
 * DAEMON START
 * DAEMON STOP
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

#include <unistd.h>
#include <sys/stat.h>
#include <signal.h>


#include "repmgr.h"
#include "compat.h"
#include "controldata.h"
#include "repmgr-client.h"
#include "repmgr-client-global.h"
#include "repmgr-action-primary.h"
#include "repmgr-action-standby.h"
#include "repmgr-action-witness.h"
#include "repmgr-action-node.h"
#include "repmgr-action-cluster.h"
#include "repmgr-action-service.h"
#include "repmgr-action-daemon.h"

#include <storage/fd.h>			/* for PG_TEMP_FILE_PREFIX */

/* globally available variables *
 * ============================ */

t_runtime_options runtime_options = T_RUNTIME_OPTIONS_INITIALIZER;


/* conninfo params for the node we're operating on */
t_conninfo_param_list source_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;

bool		config_file_required = true;
char		pg_bindir[MAXPGPATH] = "";

/*
 * if --node-id/--node-name provided, place that node's record here
 * for later use
 */
t_node_info target_node_info = T_NODE_INFO_INITIALIZER;

/* used by create_replication_slot() */
static t_user_type ReplicationSlotUser = USER_TYPE_UNKNOWN;

/* Collate command line errors and warnings here for friendlier reporting */
static ItemList cli_errors = {NULL, NULL};
static ItemList cli_warnings = {NULL, NULL};

static void _determine_replication_slot_user(PGconn *conn,
											 t_node_info *upstream_node_record,
											 char **replication_user);

int
main(int argc, char **argv)
{
	t_conninfo_param_list default_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;

	int			optindex = 0;
	int			c;

	char	   *repmgr_command = NULL;
	char	   *repmgr_action = NULL;
	bool		valid_repmgr_command_found = true;
	int			action = NO_ACTION;
	char	   *dummy_action = "";

	bool		help_option = false;
	bool		option_error_found = false;

	set_progname(argv[0]);

	/*
	 * Tell the logger we're a command-line program - this will ensure any
	 * output logged before the logger is initialized will be formatted
	 * correctly. Can be overriden with "--log-to-file".
	 */
	logger_output_mode = OM_COMMAND_LINE;

	/*
	 * Initialize and pre-populate conninfo parameters; these will be
	 * overwritten if matching command line parameters are provided.
	 *
	 * Only some actions will need these, but we need to do this before the
	 * command line is parsed.
	 *
	 * Note: PQconndefaults() does not provide a default value for "dbname",
	 * but if none is provided will default to "username" when the connection
	 * is made. We won't set "dbname" here if no default available, as that
	 * would break the libpq behaviour if non-default username is provided.
	 */
	initialize_conninfo_params(&default_conninfo, true);

	for (c = 0; c < default_conninfo.size && default_conninfo.keywords[c]; c++)
	{
		if (strcmp(default_conninfo.keywords[c], "host") == 0 &&
			(default_conninfo.values[c] != NULL))
		{
			strncpy(runtime_options.host, default_conninfo.values[c], MAXLEN);
		}
		else if (strcmp(default_conninfo.keywords[c], "hostaddr") == 0 &&
				 (default_conninfo.values[c] != NULL))
		{
			strncpy(runtime_options.host, default_conninfo.values[c], MAXLEN);
		}
		else if (strcmp(default_conninfo.keywords[c], "port") == 0 &&
				 (default_conninfo.values[c] != NULL))
		{
			strncpy(runtime_options.port, default_conninfo.values[c], MAXLEN);
		}
		else if (strcmp(default_conninfo.keywords[c], "dbname") == 0 &&
				 (default_conninfo.values[c] != NULL))
		{
			strncpy(runtime_options.dbname, default_conninfo.values[c], MAXLEN);
		}
		else if (strcmp(default_conninfo.keywords[c], "user") == 0 &&
				 (default_conninfo.values[c] != NULL))
		{
			strncpy(runtime_options.username, default_conninfo.values[c], MAXLEN);
		}
	}
	free_conninfo_params(&default_conninfo);

	initialize_conninfo_params(&source_conninfo, false);

	/* set default user for -R/--remote-user */
	{
		struct passwd *pw = getpwuid(geteuid());

		if (pw == NULL)
		{
			fprintf(stderr, _("could not get current user name: %s\n"), strerror(errno));
			exit(ERR_BAD_CONFIG);
		}

		strncpy(runtime_options.username, pw->pw_name, MAXLEN);
	}

	/* Make getopt emit errors */
	opterr = 1;

	while ((c = getopt_long(argc, argv, "?Vb:f:FwWd:h:p:U:R:S:D:ck:L:qtvC:", long_options,
							&optindex)) != -1)
	{
		/*
		 * NOTE: some integer parameters (e.g. -p/--port) are stored
		 * internally as strings. We use repmgr_atoi() to check these but
		 * discard the returned integer; repmgr_atoi() will append the error
		 * message to the provided list.
		 */
		switch (c)
		{
				/*
				 * Options which cause repmgr to exit in this block; these are
				 * the only ones which can be executed as root user
				 */
			case OPT_HELP:		/* --help */
				help_option = true;
				break;

				/* -V/--version */
			case 'V':

				/*
				 * in contrast to repmgr3 and earlier, we only display the
				 * repmgr version as it's not specific to a particular
				 * PostgreSQL version
				 */
				printf("%s %s\n", progname(), REPMGR_VERSION);
				exit(SUCCESS);

				/* --version-number */
			case OPT_VERSION_NUMBER:
				printf("%i\n", REPMGR_VERSION_NUM);
				exit(SUCCESS);

				/*------------------------------
				 * general configuration options
				 *------------------------------
				 */

				/* -b/--pg_bindir */
			case 'b':
				strncpy(runtime_options.pg_bindir, optarg, MAXLEN);
				break;

				/* -f/--config-file */
			case 'f':
				strncpy(runtime_options.config_file, optarg, MAXLEN);
				break;

				/* --dry-run */
			case OPT_DRY_RUN:
				runtime_options.dry_run = true;
				break;

				/* -F/--force */
			case 'F':
				runtime_options.force = true;
				break;

				/* --replication-user (primary/standby register only) */
			case OPT_REPLICATION_USER:
				strncpy(runtime_options.replication_user, optarg, MAXLEN);
				break;

				/* -w/--wait */
			case 'w':
				runtime_options.wait_provided = true;
				if (optarg != NULL)
				{
					runtime_options.wait = repmgr_atoi(optarg, "--wait", &cli_errors, 0);
				}
				break;

				/* -W/--no-wait */
			case 'W':
				runtime_options.no_wait = true;
				break;

				/* --compact */
			case OPT_COMPACT:
				runtime_options.compact = true;
				break;

				/* --detail */
			case OPT_DETAIL:
				runtime_options.detail = true;
				break;

				/* --dump-config */
			case OPT_DUMP_CONFIG:
				runtime_options.dump_config = true;
				break;

				/*----------------------------
				 * database connection options
				 *----------------------------
				 */

				/*
				 * These are the standard database connection options; with
				 * the exception of -d/--dbname (which could be a conninfo
				 * string) we'll also set these values in "source_conninfo"
				 * (overwriting preset values from environment variables).
				 */
				/* -d/--dbname */
			case 'd':
				strncpy(runtime_options.dbname, optarg, MAXLEN);

				/*
				 * dbname will be set in source_conninfo later after checking
				 * if it's a conninfo string
				 */
				runtime_options.connection_param_provided = true;
				break;

				/* -h/--host */
			case 'h':
				strncpy(runtime_options.host, optarg, MAXLEN);
				param_set(&source_conninfo, "host", optarg);
				runtime_options.connection_param_provided = true;
				runtime_options.host_param_provided = true;
				break;

			case 'p':
				/*
				 * minimum TCP port number is 1; in practice PostgreSQL
				 * won't be running on a privileged port, but we don't want
				 * to be concerned with that level of checking
				 */
				(void) repmgr_atoi(optarg, "-p/--port", &cli_errors, 1);
				param_set(&source_conninfo, "port", optarg);
				strncpy(runtime_options.port,
						optarg,
						MAXLEN);
				runtime_options.connection_param_provided = true;
				break;

				/* -U/--user */
			case 'U':
				strncpy(runtime_options.username, optarg, MAXLEN);
				param_set(&source_conninfo, "user", optarg);
				runtime_options.connection_param_provided = true;
				break;

				/*-------------------------
				 * other connection options
				 *-------------------------
				 */

				/* -R/--remote_user */
			case 'R':
				strncpy(runtime_options.remote_user, optarg, MAXLEN);
				break;

				/* -S/--superuser */
			case 'S':
				strncpy(runtime_options.superuser, optarg, MAXLEN);
				break;

				/*-------------
				 * node options
				 *-------------
				 */

				/* -D/--pgdata/--data-dir */
			case 'D':
				strncpy(runtime_options.data_dir, optarg, MAXPGPATH);
				break;

				/* --node-id */
			case OPT_NODE_ID:
				runtime_options.node_id = repmgr_atoi(optarg, "--node-id", &cli_errors, MIN_NODE_ID);
				break;

				/* --node-name */
			case OPT_NODE_NAME:
			{
				if (strlen(optarg) < sizeof(runtime_options.node_name))
					strncpy(runtime_options.node_name, optarg, sizeof(runtime_options.node_name));
				else
					item_list_append_format(&cli_errors,
											_("value for \"--node-name\" must contain fewer than %lu characters"),
											sizeof(runtime_options.node_name));
				break;
			}
				/* --remote-node-id */
			case OPT_REMOTE_NODE_ID:
				runtime_options.remote_node_id = repmgr_atoi(optarg, "--remote-node-id", &cli_errors, MIN_NODE_ID);
				break;

				/*
				 * standby options * ---------------
				 */

				/* --upstream-node-id */
			case OPT_UPSTREAM_NODE_ID:
				runtime_options.upstream_node_id = repmgr_atoi(optarg, "--upstream-node-id", &cli_errors, MIN_NODE_ID);
				break;

				/*------------------------
				 * "standby clone" options
				 *------------------------
				 */

				/* -c/--fast-checkpoint */
			case 'c':
				runtime_options.fast_checkpoint = true;
				break;

				/* --copy-external-config-files(=[samepath|pgdata]) */
			case OPT_COPY_EXTERNAL_CONFIG_FILES:
				runtime_options.copy_external_config_files = true;
				if (optarg != NULL)
				{
					if (strcmp(optarg, "samepath") == 0)
					{
						runtime_options.copy_external_config_files_destination = CONFIG_FILE_SAMEPATH;
					}
					/* allow "data_directory" as synonym for "pgdata" */
					else if (strcmp(optarg, "pgdata") == 0 || strcmp(optarg, "data_directory") == 0)
					{
						runtime_options.copy_external_config_files_destination = CONFIG_FILE_PGDATA;
					}
					else
					{
						item_list_append(&cli_errors,
										 _("value provided for \"--copy-external-config-files\" must be \"samepath\" or \"pgdata\""));
					}
				}
				break;

				/* --no-upstream-connection */
			case OPT_NO_UPSTREAM_CONNECTION:
				runtime_options.no_upstream_connection = true;
				break;


			case OPT_UPSTREAM_CONNINFO:
				strncpy(runtime_options.upstream_conninfo, optarg, MAXLEN);
				break;

			case OPT_WITHOUT_BARMAN:
				runtime_options.without_barman = true;
				break;

			case OPT_REPLICATION_CONF_ONLY:
				runtime_options.replication_conf_only = true;
				break;

				/* --verify-backup */
			case OPT_VERIFY_BACKUP:
				runtime_options.verify_backup = true;
				break;

				/*---------------------------
				 * "standby register" options
				 *---------------------------
				 */

			case OPT_WAIT_START:
				runtime_options.wait_start = repmgr_atoi(optarg, "--wait-start", &cli_errors, 0);
				break;

			case OPT_WAIT_SYNC:
				runtime_options.wait_register_sync = true;
				if (optarg != NULL)
				{
					runtime_options.wait_register_sync_seconds = repmgr_atoi(optarg, "--wait-sync", &cli_errors, 0);
				}
				break;

				/*-----------------------------
				 * "standby switchover" options
				 *-----------------------------
				 */

			case OPT_ALWAYS_PROMOTE:
				runtime_options.always_promote = true;
				break;

			case OPT_FORCE_REWIND:
				runtime_options.force_rewind_used = true;

				if (optarg != NULL)
				{
					strncpy(runtime_options.force_rewind_path, optarg, MAXPGPATH);
				}

				break;

			case OPT_SIBLINGS_FOLLOW:
				runtime_options.siblings_follow = true;
				break;

			case OPT_REPMGRD_NO_PAUSE:
				runtime_options.repmgrd_no_pause = true;
				break;

			case OPT_REPMGRD_FORCE_UNPAUSE:
				runtime_options.repmgrd_force_unpause = true;
				break;

				/*----------------------
				 * "node status" options
				 *----------------------
				 */

			case OPT_IS_SHUTDOWN_CLEANLY:
				runtime_options.is_shutdown_cleanly = true;
				break;

				/*---------------------
				 * "node check" options
				 *--------------------
				 */
			case OPT_ARCHIVE_READY:
				runtime_options.archive_ready = true;
				break;

			case OPT_DOWNSTREAM:
				runtime_options.downstream = true;
				break;

			case OPT_UPSTREAM:
				runtime_options.upstream = true;
				break;

			case OPT_REPLICATION_LAG:
				runtime_options.replication_lag = true;
				break;

			case OPT_ROLE:
				runtime_options.role = true;
				break;

			case OPT_SLOTS:
				runtime_options.slots = true;
				break;

			case OPT_MISSING_SLOTS:
				runtime_options.missing_slots = true;
				break;

			case OPT_HAS_PASSFILE:
				runtime_options.has_passfile = true;
				break;

			case OPT_REPL_CONN:
				runtime_options.replication_connection = true;
				break;

			case OPT_DATA_DIRECTORY_CONFIG:
				runtime_options.data_directory_config = true;
				break;

			case OPT_REPLICATION_CONFIG_OWNER:
				runtime_options.replication_config_owner = true;
				break;

			case OPT_DB_CONNECTION:
				runtime_options.db_connection = true;
				break;

				/*--------------------
				 * "node rejoin" options
				 *--------------------
				 */
			case OPT_CONFIG_FILES:
				strncpy(runtime_options.config_files, optarg, MAXLEN);
				break;

			case OPT_CONFIG_ARCHIVE_DIR:
				/* TODO: check this is an absolute path */
				strncpy(runtime_options.config_archive_dir, optarg, MAXPGPATH);
				break;

				/*-----------------------
				 * "node service" options
				 *-----------------------
				 */

				/* --action (repmgr node service --action) */
			case OPT_ACTION:
				strncpy(runtime_options.action, optarg, MAXLEN);
				break;

			case OPT_LIST_ACTIONS:
				runtime_options.list_actions = true;
				break;

			case OPT_CHECKPOINT:
				runtime_options.checkpoint = true;
				break;

				/*------------------------
				 * "cluster event" options
				 *------------------------
				 */

			case OPT_EVENT:
				strncpy(runtime_options.event, optarg, MAXLEN);
				break;

			case OPT_LIMIT:
				runtime_options.limit = repmgr_atoi(optarg, "--limit", &cli_errors, 1);
				runtime_options.limit_provided = true;
				break;

			case OPT_ALL:
				runtime_options.all = true;
				break;

				/*------------------------
				 * "cluster cleanup" options
				 *------------------------
				 */

				/* -k/--keep-history */
			case 'k':
				runtime_options.keep_history = repmgr_atoi(optarg, "-k/--keep-history", &cli_errors, 0);
				break;

				/*----------------
				 * logging options
				 *----------------
				 */

				/* -L/--log-level */
			case 'L':
				{
					int			detected_log_level = detect_log_level(optarg);

					if (detected_log_level != -1)
					{
						strncpy(runtime_options.log_level, optarg, MAXLEN);
					}
					else
					{
						PQExpBufferData invalid_log_level;

						initPQExpBuffer(&invalid_log_level);
						appendPQExpBuffer(&invalid_log_level, _("invalid log level \"%s\" provided"), optarg);
						item_list_append(&cli_errors, invalid_log_level.data);
						termPQExpBuffer(&invalid_log_level);
					}
					break;
				}

				/* --log-to-file */
			case OPT_LOG_TO_FILE:
				runtime_options.log_to_file = true;
				logger_output_mode = OM_DAEMON;
				break;


				/* --quiet */
			case 'q':
				runtime_options.quiet = true;
				break;

				/* --terse */
			case 't':
				runtime_options.terse = true;
				break;

				/* --verbose */
			case 'v':
				runtime_options.verbose = true;
				break;


				/*---------------
				 * output options
				 *---------------
				 */
			case OPT_CSV:
				runtime_options.csv = true;
				break;

			case OPT_NAGIOS:
				runtime_options.nagios = true;
				break;

			case OPT_OPTFORMAT:
				runtime_options.optformat = true;
				break;

				/*---------------------------------
				 * undocumented options for testing
				 *----------------------------------
				 */

			case OPT_DISABLE_WAL_RECEIVER:
				runtime_options.disable_wal_receiver = true;
				break;

			case OPT_ENABLE_WAL_RECEIVER:
				runtime_options.enable_wal_receiver = true;
				break;

				/*-----------------------------
				 * options deprecated since 4.0
				 *-----------------------------
				 */
			case OPT_CHECK_UPSTREAM_CONFIG:
				item_list_append(&cli_warnings,
								 _("--check-upstream-config is deprecated; use --dry-run instead"));
				break;

				/* -C/--remote-config-file */
			case 'C':
				item_list_append(&cli_warnings,
								 _("--remote-config-file is no longer required"));
				break;

			case ':':   /* missing option argument */
				option_error_found = true;
				break;
			case '?':
				/* Actual help option given? */
				if (strcmp(argv[optind - 1], "-?") == 0)
				{
					help_option = true;
				}
				else
				{
					option_error_found = true;
				}
				break;
			default:    /* invalid option */
				option_error_found = true;
				break;
		}
	}


	/*
	 * If -d/--dbname appears to be a conninfo string, validate by attempting
	 * to parse it (and if successful, store the parsed parameters)
	 */
	if (runtime_options.dbname[0])
	{
		if (strncmp(runtime_options.dbname, "postgresql://", 13) == 0 ||
			strncmp(runtime_options.dbname, "postgres://", 11) == 0 ||
			strchr(runtime_options.dbname, '=') != NULL)
		{
			char	   *errmsg = NULL;
			PQconninfoOption *opts;

			runtime_options.conninfo_provided = true;

			opts = PQconninfoParse(runtime_options.dbname, &errmsg);

			if (opts == NULL)
			{
				PQExpBufferData conninfo_error;

				initPQExpBuffer(&conninfo_error);
				appendPQExpBuffer(&conninfo_error, _("error parsing conninfo:\n%s"), errmsg);
				item_list_append(&cli_errors, conninfo_error.data);

				termPQExpBuffer(&conninfo_error);
				pfree(errmsg);
			}
			else
			{
				/*
				 * Store any parameters provided in the conninfo string in our
				 * internal array; also overwrite any options set in
				 * runtime_options.(host|port|username), as the conninfo
				 * settings take priority
				 */
				PQconninfoOption *opt;

				for (opt = opts; opt->keyword != NULL; opt++)
				{
					if (opt->val != NULL && opt->val[0] != '\0')
					{
						param_set(&source_conninfo, opt->keyword, opt->val);
					}

					if (strcmp(opt->keyword, "host") == 0 &&
						(opt->val != NULL && opt->val[0] != '\0'))
					{
						strncpy(runtime_options.host, opt->val, MAXLEN);
						runtime_options.host_param_provided = true;
					}
					if (strcmp(opt->keyword, "hostaddr") == 0 &&
						(opt->val != NULL && opt->val[0] != '\0'))
					{
						strncpy(runtime_options.host, opt->val, MAXLEN);
						runtime_options.host_param_provided = true;
					}
					else if (strcmp(opt->keyword, "port") == 0 &&
							 (opt->val != NULL && opt->val[0] != '\0'))
					{
						strncpy(runtime_options.port, opt->val, MAXLEN);
					}
					else if (strcmp(opt->keyword, "user") == 0 &&
							 (opt->val != NULL && opt->val[0] != '\0'))
					{
						strncpy(runtime_options.username, opt->val, MAXLEN);
					}
				}

				PQconninfoFree(opts);
			}
		}
		else
		{
			param_set(&source_conninfo, "dbname", runtime_options.dbname);
		}
	}

	/*
	 * Disallow further running as root to prevent directory ownership
	 * problems. We check this here to give the root user a chance to execute
	 * --help/--version options.
	 */
	if (geteuid() == 0 && help_option == false)
	{
		fprintf(stderr,
				_("%s: cannot be run as root\n"
				  "Please log in (using, e.g., \"su\") as the "
				  "(unprivileged) user that owns "
				  "the data directory.\n"
				  ),
				progname());
		free_conninfo_params(&source_conninfo);
		exit(ERR_BAD_CONFIG);
	}

	/* Exit here already if errors in command line options found */
	if (cli_errors.head != NULL)
	{
		free_conninfo_params(&source_conninfo);
		exit_with_cli_errors(&cli_errors, NULL);
	}

	/*----------
	 * Determine the node type and action; following are valid:
	 *
	 *   { PRIMARY | MASTER } REGISTER |
	 *   STANDBY { REGISTER | UNREGISTER | CLONE [node] | PROMOTE | FOLLOW [node] | SWITCHOVER } |
	 *   WITNESS { CREATE | REGISTER | UNREGISTER }
	 *   NODE { STATUS | CHECK | REJOIN | SERVICE } |
	 *   CLUSTER { CROSSCHECK | MATRIX | SHOW | EVENT | CLEANUP }
	 *   SERVICE { STATUS | PAUSE | UNPAUSE | START | STOP }
	 *
	 * [node] is an optional hostname, provided instead of the -h/--host
	 * option
	 * ---------
	 */
	if (optind < argc)
	{
		repmgr_command = argv[optind++];
	}

	if (optind < argc)
	{
		repmgr_action = argv[optind++];
	}
	else
	{
		repmgr_action = dummy_action;
	}

	if (repmgr_command != NULL)
	{
		if (strcasecmp(repmgr_command, "PRIMARY") == 0 || strcasecmp(repmgr_command, "MASTER") == 0)
		{
			if (help_option == true)
			{
				do_primary_help();
				exit(SUCCESS);
			}

			if (strcasecmp(repmgr_action, "REGISTER") == 0)
				action = PRIMARY_REGISTER;
			else if (strcasecmp(repmgr_action, "UNREGISTER") == 0)
				action = PRIMARY_UNREGISTER;
			/* allow "primary check"/"primary status" as aliases for "node check"/"node status" */
			else if (strcasecmp(repmgr_action, "CHECK") == 0)
				action = NODE_CHECK;
			else if (strcasecmp(repmgr_action, "STATUS") == 0)
				action = NODE_STATUS;
		}

		else if (strcasecmp(repmgr_command, "STANDBY") == 0)
		{
			if (help_option == true)
			{
				do_standby_help();
				exit(SUCCESS);
			}

			if (strcasecmp(repmgr_action, "CLONE") == 0)
				action = STANDBY_CLONE;
			else if (strcasecmp(repmgr_action, "REGISTER") == 0)
				action = STANDBY_REGISTER;
			else if (strcasecmp(repmgr_action, "UNREGISTER") == 0)
				action = STANDBY_UNREGISTER;
			else if (strcasecmp(repmgr_action, "PROMOTE") == 0)
				action = STANDBY_PROMOTE;
			else if (strcasecmp(repmgr_action, "FOLLOW") == 0)
				action = STANDBY_FOLLOW;
			else if (strcasecmp(repmgr_action, "SWITCHOVER") == 0)
				action = STANDBY_SWITCHOVER;
			/* allow "standby check"/"standby status" as aliases for "node check"/"node status" */
			else if (strcasecmp(repmgr_action, "CHECK") == 0)
				action = NODE_CHECK;
			else if (strcasecmp(repmgr_action, "STATUS") == 0)
				action = NODE_STATUS;
		}

		else if (strcasecmp(repmgr_command, "WITNESS") == 0)
		{
			if (help_option == true)
			{
				do_witness_help();
				exit(SUCCESS);
			}
			else if (strcasecmp(repmgr_action, "REGISTER") == 0)
				action = WITNESS_REGISTER;
			else if (strcasecmp(repmgr_action, "UNREGISTER") == 0)
				action = WITNESS_UNREGISTER;
		}

		else if (strcasecmp(repmgr_command, "NODE") == 0)
		{
			if (help_option == true)
			{
				do_node_help();
				exit(SUCCESS);
			}

			if (strcasecmp(repmgr_action, "CHECK") == 0)
				action = NODE_CHECK;
			else if (strcasecmp(repmgr_action, "STATUS") == 0)
				action = NODE_STATUS;
			else if (strcasecmp(repmgr_action, "REJOIN") == 0)
				action = NODE_REJOIN;
			else if (strcasecmp(repmgr_action, "SERVICE") == 0)
				action = NODE_SERVICE;
			else if (strcasecmp(repmgr_action, "CONTROL") == 0)
				action = NODE_CONTROL;
		}

		else if (strcasecmp(repmgr_command, "CLUSTER") == 0)
		{
			if (help_option == true)
			{
				do_cluster_help();
				exit(SUCCESS);
			}

			if (strcasecmp(repmgr_action, "SHOW") == 0)
				action = CLUSTER_SHOW;
			else if (strcasecmp(repmgr_action, "EVENT") == 0)
				action = CLUSTER_EVENT;
			/* allow "CLUSTER EVENTS" as synonym for "CLUSTER EVENT" */
			else if (strcasecmp(repmgr_action, "EVENTS") == 0)
				action = CLUSTER_EVENT;
			else if (strcasecmp(repmgr_action, "CROSSCHECK") == 0)
				action = CLUSTER_CROSSCHECK;
			else if (strcasecmp(repmgr_action, "MATRIX") == 0)
				action = CLUSTER_MATRIX;
			else if (strcasecmp(repmgr_action, "CLEANUP") == 0)
				action = CLUSTER_CLEANUP;
		}
		else if (strcasecmp(repmgr_command, "SERVICE") == 0)
		{
			if (help_option == true)
			{
				do_service_help();
				exit(SUCCESS);
			}

			if (strcasecmp(repmgr_action, "STATUS") == 0)
				action = SERVICE_STATUS;
			else if (strcasecmp(repmgr_action, "PAUSE") == 0)
				action = SERVICE_PAUSE;
			else if (strcasecmp(repmgr_action, "UNPAUSE") == 0)
				action = SERVICE_UNPAUSE;

		}
		else if (strcasecmp(repmgr_command, "DAEMON") == 0)
		{
			if (help_option == true)
			{
				do_daemon_help();
				exit(SUCCESS);
			}

			if (strcasecmp(repmgr_action, "START") == 0)
				action = DAEMON_START;
			else if (strcasecmp(repmgr_action, "STOP") == 0)
				action = DAEMON_STOP;

			/* allow "daemon" as an alias for "service" for repmgr 4.x compatibility */
			if (strcasecmp(repmgr_action, "STATUS") == 0)
				action = SERVICE_STATUS;
			else if (strcasecmp(repmgr_action, "PAUSE") == 0)
				action = SERVICE_PAUSE;
			else if (strcasecmp(repmgr_action, "UNPAUSE") == 0)
				action = SERVICE_UNPAUSE;
		}
		else
		{
			valid_repmgr_command_found = false;
		}
	}

	if (help_option == true)
	{
		do_help();
		exit(SUCCESS);
	}

	if (action == NO_ACTION)
	{
		PQExpBufferData command_error;

		initPQExpBuffer(&command_error);

		if (repmgr_command == NULL)
		{
			appendPQExpBuffer(&command_error,
							  _("no repmgr command provided"));
		}
		else if (valid_repmgr_command_found == false && repmgr_action[0] == '\0')
		{
			appendPQExpBuffer(&command_error,
							  _("unknown repmgr command '%s'"),
							  repmgr_command);
		}
		else if (repmgr_action[0] == '\0')
		{
			appendPQExpBuffer(&command_error,
							  _("no action provided for command '%s'"),
							  repmgr_command);
		}
		else
		{
			appendPQExpBuffer(&command_error,
							  _("unknown repmgr action '%s %s'"),
							  repmgr_command,
							  repmgr_action);
		}

		item_list_append(&cli_errors, command_error.data);
	}

	/*
	 * STANDBY CLONE historically accepts the upstream hostname as an
	 * additional argument
	 */
	if (action == STANDBY_CLONE)
	{
		if (optind < argc)
		{
			if (runtime_options.host_param_provided == true)
			{
				PQExpBufferData additional_host_arg;

				initPQExpBuffer(&additional_host_arg);
				appendPQExpBuffer(&additional_host_arg,
								  _("host name provided both with %s and as an extra parameter"),
								  runtime_options.conninfo_provided == true ? "host=" : "-h/--host");
				item_list_append(&cli_errors, additional_host_arg.data);
			}
			else
			{
				strncpy(runtime_options.host, argv[optind++], MAXLEN);
				param_set(&source_conninfo, "host", runtime_options.host);
				runtime_options.host_param_provided = true;
			}
		}
	}

	if (optind < argc)
	{
		PQExpBufferData too_many_args;

		initPQExpBuffer(&too_many_args);
		appendPQExpBuffer(&too_many_args, _("too many command-line arguments (first extra is \"%s\")"), argv[optind]);
		item_list_append(&cli_errors, too_many_args.data);
	}


	/*
	 * The configuration file is not required for some actions (e.g. 'standby
	 * clone'), however if available we'll parse it anyway for options like
	 * 'log_level', 'use_replication_slots' etc.
	 */
	load_config(runtime_options.config_file,
				runtime_options.verbose,
				runtime_options.terse,
				argv[0]);


	/*
	 * Handle options which must be executed without a repmgr command
	 */
	if (runtime_options.dump_config == true)
	{
		if (repmgr_command != NULL)
		{
			fprintf(stderr,
					_("--dump-config cannot be used in combination with a repmgr command"));
			exit(ERR_BAD_CONFIG);
		}
		dump_config();
		exit(SUCCESS);
	}



	check_cli_parameters(action);

	/*
	 * Sanity checks for command line parameters completed by now; any further
	 * errors will be runtime ones
	 */
	if (cli_errors.head != NULL)
	{
		free_conninfo_params(&source_conninfo);

		exit_with_cli_errors(&cli_errors, valid_repmgr_command_found == true ? repmgr_command : NULL);
	}

	/* no errors detected by repmgr, but getopt might have */
	if (option_error_found == true)
	{
		if (valid_repmgr_command_found == true)
		{
			printf(_("Try \"%s --help\" or \"%s %s --help\" for more information.\n"),
				   progname(),
				   progname(),
				   repmgr_command);
		}
		else
		{
			printf(_("Try \"repmgr --help\" for more information.\n"));
		}

		free_conninfo_params(&source_conninfo);
		exit(ERR_BAD_CONFIG);
	}


	/*
	 * Print any warnings about inappropriate command line options, unless
	 * -t/--terse set
	 */
	if (cli_warnings.head != NULL && runtime_options.terse == false)
	{
		log_warning(_("following problems with command line parameters detected:"));
		print_item_list(&cli_warnings);
	}

	/*
	 * post-processing following command line parameter checks
	 * =======================================================
	 */

	if (runtime_options.csv == true)
	{
		runtime_options.output_mode = OM_CSV;
	}
	else if (runtime_options.nagios == true)
	{
		runtime_options.output_mode = OM_NAGIOS;
	}
	else if (runtime_options.optformat == true)
	{
		runtime_options.output_mode = OM_OPTFORMAT;
	}

	/*
	 * Check for configuration file items which can be overriden by runtime
	 * options
	 * =====================================================================
	 */

	/*
	 * Command-line parameter -L/--log-level overrides any setting in config
	 * file
	 */
	if (*runtime_options.log_level != '\0')
	{
		strncpy(config_file_options.log_level, runtime_options.log_level, MAXLEN);
	}

	/*
	 * Initialise pg_bindir - command line parameter will override any setting
	 * in the configuration file
	 */
	if (!strlen(runtime_options.pg_bindir))
	{
		strncpy(runtime_options.pg_bindir, config_file_options.pg_bindir, MAXLEN);
	}

	/* Add trailing slash */
	if (strlen(runtime_options.pg_bindir))
	{
		int			len = strlen(runtime_options.pg_bindir);

		if (runtime_options.pg_bindir[len - 1] != '/')
		{
			maxlen_snprintf(pg_bindir, "%s/", runtime_options.pg_bindir);
		}
		else
		{
			strncpy(pg_bindir, runtime_options.pg_bindir, MAXLEN);
		}
	}

	/*
	 * Initialize the logger. We've previously requested STDERR logging only
	 * to ensure the repmgr command doesn't have its output diverted to a
	 * logging facility (which usually doesn't make sense for a command line
	 * program).
	 *
	 * If required (e.g. when calling repmgr from repmgrd), this behaviour can
	 * be overridden with "--log-to-file".
	 */

	logger_init(&config_file_options, progname());

	if (runtime_options.verbose)
		logger_set_verbose();

	if (runtime_options.terse)
		logger_set_terse();

	/*
	 * If --dry-run specified, ensure log_level is at least LOG_INFO, regardless
	 * of what's in the configuration file or -L/--log-level parameter, otherwise
	 * some or output might not be displayed.
	 */
	if (runtime_options.dry_run == true)
	{
		logger_set_min_level(LOG_INFO);
	}

	/*
	 * If -q/--quiet supplied, suppress any non-ERROR log output.
	 * This overrides everything else; we'll leave it up to the user to deal with the
	 * consequences of e.g. running --dry-run together with -q/--quiet.
	 */
	if (runtime_options.quiet == true)
	{
		logger_set_level(LOG_ERROR);
	}

	/*
	 * Node configuration information is not needed for all actions, with
	 * STANDBY CLONE being the main exception.
	 */
	if (config_file_required)
	{
		/*
		 * if a configuration file was provided, the configuration file parser
		 * will already have errored out if no valid node_id found
		 */
		if (config_file_options.node_id == NODE_NOT_FOUND)
		{
			free_conninfo_params(&source_conninfo);

			log_error(_("no node information was found - please supply a configuration file"));
			exit(ERR_BAD_CONFIG);
		}
	}

	/*
	 * If a node was specified (by --node-id or --node-name), check it exists
	 * (and pre-populate a record for later use).
	 *
	 * At this point check_cli_parameters() will already have determined if
	 * provision of these is valid for the action, otherwise it unsets them.
	 *
	 * We need to check this much later than other command line parameters as
	 * we need to wait until the configuration file is parsed and we can
	 * obtain the conninfo string.
	 */

	if (runtime_options.node_id != UNKNOWN_NODE_ID || runtime_options.node_name[0] != '\0')
	{
		PGconn	   *conn = NULL;
		RecordStatus record_status = RECORD_NOT_FOUND;

		log_verbose(LOG_DEBUG, "connecting to local node to retrieve record for node specified with --node-id or --node-name");

		if (strlen(config_file_options.conninfo))
			conn = establish_db_connection(config_file_options.conninfo, true);
		else
			conn = establish_db_connection_by_params(&source_conninfo, true);

		if (runtime_options.node_id != UNKNOWN_NODE_ID)
		{
			record_status = get_node_record(conn, runtime_options.node_id, &target_node_info);

			if (record_status != RECORD_FOUND)
			{
				log_error(_("node %i (specified with --node-id) not found"),
						  runtime_options.node_id);
				PQfinish(conn);
				free_conninfo_params(&source_conninfo);

				exit(ERR_BAD_CONFIG);
			}
		}
		else if (runtime_options.node_name[0] != '\0')
		{
			char	   *escaped = escape_string(conn, runtime_options.node_name);

			if (escaped == NULL)
			{
				log_error(_("unable to escape value provided for --node-name"));
				PQfinish(conn);
				free_conninfo_params(&source_conninfo);

				exit(ERR_BAD_CONFIG);
			}

			record_status = get_node_record_by_name(conn, escaped, &target_node_info);

			pfree(escaped);
			if (record_status != RECORD_FOUND)
			{
				log_error(_("node \"%s\" (specified with --node-name) not found"),
						  runtime_options.node_name);
				PQfinish(conn);
				free_conninfo_params(&source_conninfo);

				exit(ERR_BAD_CONFIG);
			}
		}

		PQfinish(conn);
	}


	switch (action)
	{
			/* PRIMARY */
		case PRIMARY_REGISTER:
			do_primary_register();
			break;
		case PRIMARY_UNREGISTER:
			do_primary_unregister();
			break;

			/* STANDBY */
		case STANDBY_CLONE:
			do_standby_clone();
			break;
		case STANDBY_REGISTER:
			do_standby_register();
			break;
		case STANDBY_UNREGISTER:
			do_standby_unregister();
			break;
		case STANDBY_PROMOTE:
			do_standby_promote();
			break;
		case STANDBY_FOLLOW:
			do_standby_follow();
			break;
		case STANDBY_SWITCHOVER:
			do_standby_switchover();
			break;

			/* WITNESS */
		case WITNESS_REGISTER:
			do_witness_register();
			break;
		case WITNESS_UNREGISTER:
			do_witness_unregister();
			break;

			/* NODE */
		case NODE_STATUS:
			do_node_status();
			break;
		case NODE_CHECK:
			do_node_check();
			break;
		case NODE_REJOIN:
			do_node_rejoin();
			break;
		case NODE_SERVICE:
			do_node_service();
			break;
		case NODE_CONTROL:
			do_node_control();
			break;

			/* CLUSTER */
		case CLUSTER_SHOW:
			do_cluster_show();
			break;
		case CLUSTER_EVENT:
			do_cluster_event();
			break;
		case CLUSTER_CROSSCHECK:
			do_cluster_crosscheck();
			break;
		case CLUSTER_MATRIX:
			do_cluster_matrix();
			break;
		case CLUSTER_CLEANUP:
			do_cluster_cleanup();
			break;

			/* SERVICE */
		case SERVICE_STATUS:
			do_service_status();
			break;
		case SERVICE_PAUSE:
			do_service_pause();
			break;
		case SERVICE_UNPAUSE:
			do_service_unpause();
			break;

			/* DAEMON */
		case DAEMON_START:
			do_daemon_start();
			break;
		case DAEMON_STOP:
			do_daemon_stop();
			break;

		default:
			/* An action will have been determined by this point  */
			break;
	}

	free_conninfo_params(&source_conninfo);

	return SUCCESS;
}



/*
 * Check for useless or conflicting parameters, and also whether a
 * configuration file is required.
 *
 * Messages will be added to the command line warning and error lists
 * as appropriate.
 */

static void
check_cli_parameters(const int action)
{
	/*
	 * ========================================================================
	 * check all parameters required for an action are provided, and warn
	 * about ineffective actions
	 * ========================================================================
	 */
	switch (action)
	{
		case PRIMARY_REGISTER:
			/* no required parameters */
			break;
		case STANDBY_CLONE:
			{
				standy_clone_mode mode = get_standby_clone_mode();

				config_file_required = false;

				if (mode == barman)
				{
					if (runtime_options.copy_external_config_files)
					{
						item_list_append(&cli_warnings,
										 _("--copy-external-config-files ineffective in Barman mode"));
					}

					if (runtime_options.fast_checkpoint)
					{
						item_list_append(&cli_warnings,
										 _("-c/--fast-checkpoint has no effect in Barman mode"));
					}


				}
				else
				{
					if (!runtime_options.host_param_provided)
					{
						item_list_append_format(&cli_errors,
												_("host name for the source node must be provided with -h/--host when executing %s"),
												action_name(action));
					}

					if (!runtime_options.connection_param_provided)
					{
						item_list_append_format(&cli_errors,
												_("database connection parameters for the source node must be provided when executing %s"),
												action_name(action));
					}

					/*
					 * If -D/--pgdata was provided, but config_file_options.pgdata
					 * is set, warn that -D/--pgdata will be ignored.
					 */
					if (runtime_options.data_dir[0] && config_file_options.data_directory[0])
					{
						item_list_append(&cli_warnings,
										 _("-D/--pgdata will be ignored if a repmgr configuration file is provided"));

					}

					if (*runtime_options.upstream_conninfo)
					{
						if (*runtime_options.replication_user)
						{
							item_list_append(&cli_warnings,
											 _("--replication-user ineffective when specifying --upstream-conninfo"));
						}
					}

					if (runtime_options.no_upstream_connection == true)
					{
						item_list_append(&cli_warnings,
										 _("--no-upstream-connection only effective in Barman mode"));
					}
				}

				if (strlen(config_file_options.config_directory))
				{
					if (runtime_options.copy_external_config_files == false)
					{
						item_list_append(&cli_warnings,
										 _("\"config_directory\" set in repmgr.conf, but --copy-external-config-files not provided"));
					}
				}
			}
			break;

		case STANDBY_FOLLOW:
			{
				/*
				 * if `repmgr standby follow` executed with host params,
				 * ensure data directory was provided
				 */
			}
			break;
		case WITNESS_REGISTER:
			{
				if (!runtime_options.host_param_provided)
				{
					item_list_append_format(&cli_errors,
											_("host name for the source node must be provided with -h/--host when executing %s"),
											action_name(action));
				}
			}
			break;
		case NODE_CHECK:
			if (runtime_options.has_passfile == true)
			{
				config_file_required = false;
			}
			break;
		case NODE_STATUS:
			if (runtime_options.node_id != UNKNOWN_NODE_ID)
			{
				item_list_append(
								 &cli_warnings,
								 "--node-id will be ignored; \"repmgr node status\" can only be executed on the local node");
			}
			if (runtime_options.node_name[0] != '\0')
			{
				item_list_append(
								 &cli_warnings,
								 "--node-name will be ignored; \"repmgr node status\" can only be executed on the local node");
			}
			break;
		case NODE_REJOIN:
			if (runtime_options.connection_param_provided == false)
			{
				item_list_append(
								 &cli_errors,
								 "database connection parameters for an available node must be provided when executing NODE REJOIN");
			}
			break;
		case CLUSTER_SHOW:
		case CLUSTER_MATRIX:
		case CLUSTER_CROSSCHECK:
			if (runtime_options.connection_param_provided)
				config_file_required = false;
			break;
		case CLUSTER_EVENT:
			/* no required parameters */
			break;

	}

	/*
	 * ========================================================================
	 * warn if parameters provided for an action where they're not relevant
	 * ========================================================================
	 */

	/* --host etc. */
	if (runtime_options.connection_param_provided)
	{
		switch (action)
		{
			case STANDBY_CLONE:
			case STANDBY_FOLLOW:
			case STANDBY_REGISTER:
			case WITNESS_REGISTER:
			case WITNESS_UNREGISTER:
			case CLUSTER_SHOW:
			case CLUSTER_MATRIX:
			case CLUSTER_CROSSCHECK:
			case NODE_REJOIN:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("database connection parameters not required when executing %s"),
										action_name(action));
		}
	}

	/* -D/--pgdata */
	if (runtime_options.data_dir[0])
	{
		switch (action)
		{
			case STANDBY_CLONE:
			case STANDBY_FOLLOW:
			case NODE_SERVICE:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("-D/--pgdata not required when executing %s"),
										action_name(action));
		}
	}

	/*
	 * --node-id
	 *
	 * NOTE: overrides --node-name, if present
	 */
	if (runtime_options.node_id != UNKNOWN_NODE_ID)
	{
		switch (action)
		{
			case PRIMARY_UNREGISTER:
			case STANDBY_UNREGISTER:
			case WITNESS_UNREGISTER:
			case CLUSTER_CLEANUP:
			case CLUSTER_EVENT:
			case CLUSTER_MATRIX:
			case CLUSTER_CROSSCHECK:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--node-id not required when executing %s"),
										action_name(action));
				runtime_options.node_id = UNKNOWN_NODE_ID;
		}
	}

	if (runtime_options.node_name[0])
	{
		switch (action)
		{
			case STANDBY_UNREGISTER:
			case CLUSTER_EVENT:
				if (runtime_options.node_id != UNKNOWN_NODE_ID)
				{
					item_list_append(&cli_warnings,
									 _("--node-id provided, ignoring --node-name"));
					memset(runtime_options.node_name, 0, sizeof(runtime_options.node_name));
				}
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--node-name not required when executing %s"),
										action_name(action));
				memset(runtime_options.node_name, 0, sizeof(runtime_options.node_name));
		}
	}

	if (runtime_options.upstream_node_id != UNKNOWN_NODE_ID)
	{
		switch (action)
		{
			case STANDBY_CLONE:
			case STANDBY_REGISTER:
			case STANDBY_FOLLOW:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--upstream-node-id will be ignored when executing %s"),
										action_name(action));
		}
	}

	if (runtime_options.replication_user[0])
	{
		switch (action)
		{
			case PRIMARY_REGISTER:
			case STANDBY_REGISTER:
			case STANDBY_CLONE:
				break;

			case STANDBY_FOLLOW:
				item_list_append_format(&cli_warnings,
										_("--replication-user ignored when executing %s"),
										action_name(action));
				break;

			default:
				item_list_append_format(&cli_warnings,
										_("--replication-user not required when executing %s"),
										action_name(action));
		}
	}

	if (runtime_options.superuser[0])
	{
		switch (action)
		{
			case STANDBY_CLONE:
			case STANDBY_SWITCHOVER:
			case NODE_CHECK:
			case NODE_SERVICE:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--superuser ignored when executing %s"),
										action_name(action));
		}
	}


	if (runtime_options.replication_conf_only == true)
	{
		switch (action)
		{
			case STANDBY_CLONE:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--create-recovery-conf will be ignored when executing %s"),
										action_name(action));
		}
	}

	if (runtime_options.event[0])
	{
		switch (action)
		{
			case CLUSTER_EVENT:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--event not required when executing %s"),
										action_name(action));
		}
	}

	if (runtime_options.limit_provided)
	{
		switch (action)
		{
			case CLUSTER_EVENT:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--limit not required when executing %s"),
										action_name(action));
		}
	}

	if (runtime_options.all)
	{
		switch (action)
		{
			case CLUSTER_EVENT:
				if (runtime_options.limit_provided == true)
				{
					runtime_options.all = false;
					item_list_append(&cli_warnings,
									 _("--limit provided, ignoring --all"));
				}
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--all not required when executing %s"),
										action_name(action));
		}
	}

	/* --wait/--no-wait */

	if (runtime_options.wait_provided == true && runtime_options.no_wait == true)
	{
		item_list_append_format(&cli_errors,
								_("both --wait and --no-wait options provided"));
	}
	else
	{
		if (runtime_options.wait_provided)
		{
			switch (action)
			{
				case DAEMON_START:
				case DAEMON_STOP:
				case STANDBY_FOLLOW:
					break;
				default:
					item_list_append_format(&cli_warnings,
											_("--wait will be ignored when executing %s"),
											action_name(action));
			}
		}
		else if (runtime_options.no_wait)
		{
			switch (action)
			{
				case DAEMON_START:
				case DAEMON_STOP:
				case NODE_REJOIN:
					break;
				default:
					item_list_append_format(&cli_warnings,
											_("--no-wait will be ignored when executing %s"),
											action_name(action));
			}
		}
	}

	/* repmgr node service --action */
	if (runtime_options.action[0] != '\0')
	{
		switch (action)
		{
			case NODE_SERVICE:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--action will be ignored when executing %s"),
										action_name(action));
		}
	}

	/* repmgr node status --is-shutdown-cleanly */
	if (runtime_options.is_shutdown_cleanly == true)
	{
		switch (action)
		{
			case NODE_STATUS:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--is-shutdown-cleanly will be ignored when executing %s"),
										action_name(action));
		}
	}

	if (runtime_options.always_promote == true)
	{
		switch (action)
		{
			case STANDBY_SWITCHOVER:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--always-promote will be ignored when executing %s"),
										action_name(action));
		}
	}

	if (runtime_options.force_rewind_used == true)
	{
		switch (action)
		{
			case STANDBY_SWITCHOVER:
			case NODE_REJOIN:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--force-rewind will be ignored when executing %s"),
										action_name(action));
		}
	}

	if (runtime_options.repmgrd_no_pause == true)
	{
		switch (action)
		{
			case STANDBY_SWITCHOVER:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--repmgrd-no-pause will be ignored when executing %s"),
										action_name(action));
		}
	}

	if (runtime_options.repmgrd_force_unpause == true)
	{
		switch (action)
		{
			case STANDBY_SWITCHOVER:
				if (runtime_options.repmgrd_no_pause == true)
					item_list_append(&cli_errors,
									 _("--repmgrd-force-unpause and --repmgrd-no-pause cannot be used together"));
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--repmgrd-force-unpause will be ignored when executing %s"),
										action_name(action));
		}
	}

	if (runtime_options.config_files[0] != '\0')
	{
		switch (action)
		{
			case NODE_REJOIN:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--config-files will be ignored when executing %s"),
										action_name(action));
		}
	}

	if (runtime_options.dry_run == true)
	{
		switch (action)
		{
			case PRIMARY_REGISTER:
			case PRIMARY_UNREGISTER:
			case STANDBY_CLONE:
			case STANDBY_REGISTER:
			case STANDBY_FOLLOW:
			case STANDBY_SWITCHOVER:
			case STANDBY_PROMOTE:
			case WITNESS_REGISTER:
			case WITNESS_UNREGISTER:
			case NODE_REJOIN:
			case NODE_SERVICE:
			case SERVICE_PAUSE:
			case SERVICE_UNPAUSE:
			case SERVICE_STATUS:
			case DAEMON_START:
			case DAEMON_STOP:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--dry-run is not effective when executing %s"),
										action_name(action));
		}
	}

	/* check only one of --csv, --nagios and --optformat  used */
	{
		int			used_options = 0;

		if (runtime_options.csv == true)
			used_options++;

		if (runtime_options.nagios == true)
			used_options++;

		if (runtime_options.optformat == true)
			used_options++;

		if (used_options > 1)
		{
			/* TODO: list which options were used */
			item_list_append(&cli_errors,
							 "only one of --csv, --nagios and --optformat can be used");
		}
	}

	/* --compact */
	if (runtime_options.compact == true)
	{
		switch (action)
		{
			case CLUSTER_SHOW:
			case CLUSTER_EVENT:
			case SERVICE_STATUS:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--compact is not effective when executing %s"),
										action_name(action));
		}
	}

	/* --detail */
	if (runtime_options.detail == true)
	{
		switch (action)
		{
			case SERVICE_STATUS:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--detail is not effective when executing %s"),
										action_name(action));
		}
	}

	/* --siblings-follow */
	if (runtime_options.siblings_follow == true)
	{
		switch (action)
		{
			case STANDBY_PROMOTE:
			case STANDBY_SWITCHOVER:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("----siblings-follow is not effective when executing %s"),
										action_name(action));
		}
	}

	/* --disable-wal-receiver / --enable-wal-receiver */
	if (runtime_options.disable_wal_receiver == true || runtime_options.enable_wal_receiver == true)
	{
		switch (action)
		{
			case NODE_CONTROL:
			{
				if (runtime_options.disable_wal_receiver == true && runtime_options.enable_wal_receiver == true)
				{
						item_list_append(&cli_errors,
										 _("provide either --disable-wal-receiver or --enable-wal-receiver"));
				}
			}
				break;
			default:
					item_list_append_format(&cli_warnings,
											_("--disable-wal-receiver / --enable-wal-receiver not effective when executing %s"),
											action_name(action));
		}
	}

}


/*
 * Generate formatted node status output for display by "cluster show" and
 * "service status".
 */
bool
format_node_status(t_node_info *node_info, PQExpBufferData *node_status, PQExpBufferData *upstream, ItemList *warnings)
{
	bool error_found = false;
	t_node_info remote_node_rec = T_NODE_INFO_INITIALIZER;
	RecordStatus remote_node_rec_found = RECORD_NOT_FOUND;

	if (PQstatus(node_info->conn) == CONNECTION_OK)
	{
		node_info->node_status = NODE_STATUS_UP;
		node_info->recovery_type = get_recovery_type(node_info->conn);
		/* get node's copy of its record so we can see what it thinks its status is */
		remote_node_rec_found = get_node_record_with_upstream(node_info->conn, node_info->node_id, &remote_node_rec);
	}
	else
	{
		/* check if node is reachable, but just not letting us in */
		if (is_server_available_quiet(node_info->conninfo))
			node_info->node_status = NODE_STATUS_REJECTED;
		else
			node_info->node_status = NODE_STATUS_DOWN;

		node_info->recovery_type = RECTYPE_UNKNOWN;
	}

	/* format node status info */
	switch (node_info->type)
	{
		case PRIMARY:
		{
			/* node is reachable */
			if (node_info->node_status == NODE_STATUS_UP)
			{
				if (node_info->active == true)
				{
					switch (node_info->recovery_type)
					{
						case RECTYPE_PRIMARY:
							appendPQExpBufferStr(node_status, "* running");
							break;
						case RECTYPE_STANDBY:
							appendPQExpBufferStr(node_status, "! running as standby");
							item_list_append_format(warnings,
													"node \"%s\" (ID: %i) is registered as primary but running as standby",
													node_info->node_name, node_info->node_id);
							break;
						case RECTYPE_UNKNOWN:
							appendPQExpBufferStr(node_status, "! unknown");
							item_list_append_format(warnings,
													"node \"%s\" (ID: %i) has unknown replication status",
													node_info->node_name, node_info->node_id);
							break;
					}
				}
				else
				{
					if (node_info->recovery_type == RECTYPE_PRIMARY)
					{
						appendPQExpBufferStr(node_status, "! running");
						item_list_append_format(warnings,
												"node \"%s\" (ID: %i) is running but the repmgr node record is inactive",
												node_info->node_name, node_info->node_id);
					}
					else
					{
						appendPQExpBufferStr(node_status, "! running as standby");
						item_list_append_format(warnings,
												"node \"%s\" (ID: %i) is registered as an inactive primary but running as standby",
												node_info->node_name, node_info->node_id);
					}
				}
			}
			/* node is up but cannot connect */
			else if (node_info->node_status == NODE_STATUS_REJECTED)
			{
				if (node_info->active == true)
				{
					appendPQExpBufferStr(node_status, "? running");
				}
				else
				{
					appendPQExpBufferStr(node_status, "! running");
					error_found = true;
				}
			}
			/* node is unreachable */
			else
			{
				/* node is unreachable but marked active */
				if (node_info->active == true)
				{
					appendPQExpBufferStr(node_status, "? unreachable");
					item_list_append_format(warnings,
											"node \"%s\" (ID: %i) is registered as an active primary but is unreachable",
											node_info->node_name, node_info->node_id);
				}
				/* node is unreachable and marked as inactive */
				else
				{
					appendPQExpBufferStr(node_status, "- failed");
					error_found = true;
				}
			}
		}
		break;
		case STANDBY:
		{
			/* node is reachable */
			if (node_info->node_status == NODE_STATUS_UP)
			{
				if (node_info->active == true)
				{
					switch (node_info->recovery_type)
					{
						case RECTYPE_STANDBY:
							appendPQExpBufferStr(node_status, "  running");
							break;
						case RECTYPE_PRIMARY:
							appendPQExpBufferStr(node_status, "! running as primary");
							item_list_append_format(warnings,
													"node \"%s\" (ID: %i) is registered as standby but running as primary",
													node_info->node_name, node_info->node_id);
							break;
						case RECTYPE_UNKNOWN:
							appendPQExpBufferStr(node_status, "! unknown");
							item_list_append_format(
								warnings,
								"node \"%s\" (ID: %i) has unknown replication status",
								node_info->node_name, node_info->node_id);
							break;
					}
				}
				else
				{
					if (node_info->recovery_type == RECTYPE_STANDBY)
					{
						appendPQExpBufferStr(node_status, "! running");
						item_list_append_format(warnings,
												"node \"%s\" (ID: %i) is running but the repmgr node record is inactive",
												node_info->node_name, node_info->node_id);
					}
					else
					{
						appendPQExpBufferStr(node_status, "! running as primary");
						item_list_append_format(warnings,
												"node \"%s\" (ID: %i) is running as primary but the repmgr node record is inactive",
												node_info->node_name, node_info->node_id);
					}
				}

				/* warn about issue with paused WAL replay */
				if (is_wal_replay_paused(node_info->conn, true))
				{
					item_list_append_format(warnings,
											_("WAL replay is paused on node \"%s\" (ID: %i) with WAL replay pending; this node cannot be manually promoted until WAL replay is resumed"),
											node_info->node_name, node_info->node_id);
				}
			}
			/* node is up but cannot connect */
			else if (node_info->node_status == NODE_STATUS_REJECTED)
			{
				if (node_info->active == true)
				{
					appendPQExpBufferStr(node_status, "? running");
				}
				else
				{
					appendPQExpBufferStr(node_status, "! running");
					error_found = true;
				}
			}
			/* node is unreachable */
			else
			{
				/* node is unreachable but marked active */
				if (node_info->active == true)
				{
					appendPQExpBufferStr(node_status, "? unreachable");
					item_list_append_format(warnings,
											"node \"%s\" (ID: %i) is registered as an active standby but is unreachable",
											node_info->node_name, node_info->node_id);
				}
				else
				{
					appendPQExpBufferStr(node_status, "- failed");
					error_found = true;
				}
			}
		}

		break;
		case WITNESS:
		{
			/* node is reachable */
			if (node_info->node_status == NODE_STATUS_UP)
			{
				if (node_info->active == true)
				{
					appendPQExpBufferStr(node_status, "* running");
				}
				else
				{
					appendPQExpBufferStr(node_status, "! running");
					error_found = true;
				}
			}
			/* node is up but cannot connect */
			else if (node_info->node_status == NODE_STATUS_REJECTED)
			{
				if (node_info->active == true)
				{
					appendPQExpBufferStr(node_status, "? rejected");
				}
				else
				{
					appendPQExpBufferStr(node_status, "! failed");
					error_found = true;
				}
			}
			/* node is unreachable */
			else
			{
				if (node_info->active == true)
				{
					appendPQExpBufferStr(node_status, "? unreachable");
				}
				else
				{
					appendPQExpBufferStr(node_status, "- failed");
					error_found = true;
				}
			}
		}
		break;
		case UNKNOWN:
		{
			/* this should never happen */
			appendPQExpBufferStr(node_status, "? unknown node type");
			error_found = true;
		}
		break;
	}

	/* format node upstream info */

	if (remote_node_rec_found == RECORD_NOT_FOUND)
	{
		/*
		 * Unable to retrieve the node's copy of its own record - copy the
		 * name from our own copy of the record
		 */
		appendPQExpBuffer(upstream,
						  "? %s",
						  node_info->upstream_node_name);
	}
	else if (remote_node_rec.type == WITNESS)
	{
		/* no upstream - unlikely to happen */
		if (remote_node_rec.upstream_node_id == NO_UPSTREAM_NODE)
		{
			appendPQExpBufferStr(upstream, "! ");
			item_list_append_format(warnings,
									"node \"%s\" (ID: %i) is a witness but reports it has no upstream node",
									node_info->node_name,
									node_info->node_id);
		}
		/* mismatch between reported upstream and upstream in local node's metadata */
		else if (node_info->upstream_node_id != remote_node_rec.upstream_node_id)
		{
			appendPQExpBufferStr(upstream, "! ");

			if (node_info->upstream_node_id != remote_node_rec.upstream_node_id)
			{
				item_list_append_format(warnings,
										"node \"%s\" (ID: %i) reports a different upstream (reported: \"%s\", expected \"%s\")",
										node_info->node_name,
										node_info->node_id,
										remote_node_rec.upstream_node_name,
										node_info->upstream_node_name);
			}
		}
		else
		{
			t_node_info upstream_node_rec = T_NODE_INFO_INITIALIZER;
			RecordStatus upstream_node_rec_found = get_node_record(node_info->conn,
																   node_info->upstream_node_id,
																   &upstream_node_rec);

			if (upstream_node_rec_found != RECORD_FOUND)
			{
				appendPQExpBufferStr(upstream, "? ");
				item_list_append_format(warnings,
										"unable to find record for upstream node ID %i",
										node_info->upstream_node_id);

			}
			else
			{
				PGconn *upstream_conn = establish_db_connection_quiet(upstream_node_rec.conninfo);

				if (PQstatus(upstream_conn) != CONNECTION_OK)
				{
					appendPQExpBufferStr(upstream, "? ");
					item_list_append_format(warnings,
											"unable to connect to node \"%s\" (ID: %i)'s upstream node \"%s\" (ID: %i)",
											node_info->node_name,
											node_info->node_id,
											upstream_node_rec.node_name,
											upstream_node_rec.node_id);
				}

				PQfinish(upstream_conn);
			}
		}

		appendPQExpBufferStr(upstream,
							 remote_node_rec.upstream_node_name);

	}
	else if (remote_node_rec.type == STANDBY)
	{
		if (node_info->upstream_node_id != NO_UPSTREAM_NODE && node_info->upstream_node_id == remote_node_rec.upstream_node_id)
		{
			/*
			 * expected and reported upstreams match - check if node is actually
			 * connected to the upstream
			 */
			NodeAttached attached_to_upstream = NODE_ATTACHED_UNKNOWN;
			char *replication_state = NULL;
			t_node_info upstream_node_rec = T_NODE_INFO_INITIALIZER;
			RecordStatus upstream_node_rec_found = get_node_record(node_info->conn,
																   node_info->upstream_node_id,
																   &upstream_node_rec);

			if (upstream_node_rec_found != RECORD_FOUND)
			{
				item_list_append_format(warnings,
										"unable to find record for upstream node ID %i",
										node_info->upstream_node_id);

			}
			else
			{
				PGconn *upstream_conn = establish_db_connection_quiet(upstream_node_rec.conninfo);

				if (PQstatus(upstream_conn) != CONNECTION_OK)
				{
					item_list_append_format(warnings,
											"unable to connect to node \"%s\" (ID: %i)'s upstream node \"%s\" (ID: %i)",
											node_info->node_name,
											node_info->node_id,
											upstream_node_rec.node_name,
											upstream_node_rec.node_id);
				}
				else
				{
					attached_to_upstream = is_downstream_node_attached(upstream_conn, node_info->node_name, &replication_state);
				}

				PQfinish(upstream_conn);
			}

			if (attached_to_upstream == NODE_ATTACHED_UNKNOWN)
			{
				appendPQExpBufferStr(upstream, "? ");
				item_list_append_format(warnings,
										"unable to determine if node \"%s\" (ID: %i) is attached to its upstream node \"%s\" (ID: %i)",
										node_info->node_name,
										node_info->node_id,
										upstream_node_rec.node_name,
										upstream_node_rec.node_id);
			}
			if (attached_to_upstream == NODE_NOT_ATTACHED)
			{
				appendPQExpBufferStr(upstream, "? ");
				item_list_append_format(warnings,
										"node \"%s\" (ID: %i) attached to its upstream node \"%s\" (ID: %i) in state \"%s\"",
										node_info->node_name,
										node_info->node_id,
										upstream_node_rec.node_name,
										upstream_node_rec.node_id,
										replication_state);
			}

			else if (attached_to_upstream == NODE_DETACHED)
			{
				appendPQExpBufferStr(upstream, "! ");
				item_list_append_format(warnings,
										"node \"%s\" (ID: %i) is not attached to its upstream node \"%s\" (ID: %i)",
										node_info->node_name,
										node_info->node_id,
										upstream_node_rec.node_name,
										upstream_node_rec.node_id);
			}
			appendPQExpBufferStr(upstream,
								 node_info->upstream_node_name);

		}
		else
		{
			if (node_info->upstream_node_id != NO_UPSTREAM_NODE && remote_node_rec.upstream_node_id == NO_UPSTREAM_NODE)
			{
				appendPQExpBufferChar(upstream, '!');
				item_list_append_format(warnings,
										"node \"%s\" (ID: %i) reports it has no upstream (expected: \"%s\")",
										node_info->node_name,
										node_info->node_id,
										node_info->upstream_node_name);
			}
			else if (node_info->upstream_node_id != NO_UPSTREAM_NODE && remote_node_rec.upstream_node_id != NO_UPSTREAM_NODE)

			{
				appendPQExpBuffer(upstream,
								  "! %s", remote_node_rec.upstream_node_name);
				item_list_append_format(warnings,
										"node \"%s\" (ID: %i) reports a different upstream (reported: \"%s\", expected \"%s\")",
										node_info->node_name,
										node_info->node_id,
										remote_node_rec.upstream_node_name,
										node_info->upstream_node_name);
			}
		}
	}

	return error_found;
}


static const char *
action_name(const int action)
{
	switch (action)
	{
		case PRIMARY_REGISTER:
			return "PRIMARY REGISTER";
		case PRIMARY_UNREGISTER:
			return "PRIMARY UNREGISTER";

		case STANDBY_CLONE:
			return "STANDBY CLONE";
		case STANDBY_REGISTER:
			return "STANDBY REGISTER";
		case STANDBY_UNREGISTER:
			return "STANDBY UNREGISTER";
		case STANDBY_PROMOTE:
			return "STANDBY PROMOTE";
		case STANDBY_FOLLOW:
			return "STANDBY FOLLOW";
		case STANDBY_SWITCHOVER:
			return "STANDBY SWITCHOVER";

		case WITNESS_REGISTER:
			return "WITNESS REGISTER";
		case WITNESS_UNREGISTER:
			return "WITNESS UNREGISTER";

		case NODE_STATUS:
			return "NODE STATUS";
		case NODE_CHECK:
			return "NODE CHECK";
		case NODE_REJOIN:
			return "NODE REJOIN";
		case NODE_SERVICE:
			return "NODE SERVICE";
		case NODE_CONTROL:
			return "NODE CONTROL";

		case CLUSTER_SHOW:
			return "CLUSTER SHOW";
		case CLUSTER_CLEANUP:
			return "CLUSTER CLEANUP";
		case CLUSTER_EVENT:
			return "CLUSTER EVENT";
		case CLUSTER_MATRIX:
			return "CLUSTER MATRIX";
		case CLUSTER_CROSSCHECK:
			return "CLUSTER CROSSCHECK";

		case SERVICE_STATUS:
			return "SERVICE STATUS";
		case SERVICE_PAUSE:
			return "SERVICE PAUSE";
		case SERVICE_UNPAUSE:
			return "SERVICE UNPAUSE";

		case DAEMON_START:
			return "DAEMON START";
		case DAEMON_STOP:
			return "DAEMON STOP";
	}

	return "UNKNOWN ACTION";
}


void
print_error_list(ItemList *error_list, int log_level)
{
	ItemListCell *cell = NULL;

	for (cell = error_list->head; cell; cell = cell->next)
	{
		switch (log_level)
		{
				/* Currently we only need errors and warnings */
			case LOG_ERROR:
				log_error("%s", cell->string);
				break;
			case LOG_WARNING:
				log_warning("%s", cell->string);
				break;
		}
	}
}


void
print_status_header(int cols, ColHeader *headers)
{
	int i, di;
	int max_cols = 0;


	/* count how many columns we actually need to display */
	for (i = 0; i < cols; i++)
	{
		if (headers[i].display == true)
			max_cols ++;
	}

	for (i = 0; i < cols; i++)
	{
		if (headers[i].display == false)
			continue;

		if (i == 0)
			printf(" ");
		else
			printf(" | ");

		printf("%-*s",
			   headers[i].max_length,
			   headers[i].title);
	}


	printf("\n");
	printf("-");

	di = 0;
	for (i = 0; i < cols; i++)
	{
		int			j;

		if (headers[i].display == false)
			continue;

		for (j = 0; j < headers[i].max_length; j++)
			printf("-");

		if (di < (max_cols - 1))
			printf("-+-");
		else
			printf("-");
		di++;
	}

	printf("\n");
}


void
print_help_header(void)
{
	printf(_("%s: replication management tool for PostgreSQL\n"), progname());
	puts("");

	/* add a big friendly warning if root is executing "repmgr --help" */
	if (geteuid() == 0)
	{
		printf(_("	**************************************************\n"));
		printf(_("	*** repmgr must be executed by a non-superuser ***\n"));
		printf(_("	**************************************************\n"));
		puts("");
	}
}

static void
do_help(void)
{
	print_help_header();

	printf(_("Usage:\n"));
	printf(_("    %s [OPTIONS] primary {register|unregister}\n"), progname());
	printf(_("    %s [OPTIONS] standby {register|unregister|clone|promote|follow|switchover}\n"), progname());
	printf(_("    %s [OPTIONS] node    {status|check|rejoin|service}\n"), progname());
	printf(_("    %s [OPTIONS] cluster {show|event|matrix|crosscheck|cleanup}\n"), progname());
	printf(_("    %s [OPTIONS] witness {register|unregister}\n"), progname());
	printf(_("    %s [OPTIONS] service {status|pause|unpause}\n"), progname());
	printf(_("    %s [OPTIONS] daemon  {start|stop}\n"), progname());

	puts("");

	printf(_("  Execute \"%s {primary|standby|node|cluster|witness|service} --help\" to see command-specific options\n"), progname());

	puts("");

	printf(_("General options:\n"));
	printf(_("  -?, --help                          show this help, then exit\n"));
	printf(_("  -V, --version                       output version information, then exit\n"));
	printf(_("  --version-number                    output version number, then exit\n"));
	puts("");

	printf(_("General configuration options:\n"));
	printf(_("  -b, --pg_bindir=PATH                path to PostgreSQL binaries (optional)\n"));
	printf(_("  -f, --config-file=PATH              path to the repmgr configuration file\n"));
	printf(_("  -F, --force                         force potentially dangerous operations to happen\n"));
	puts("");

	printf(_("Database connection options:\n"));
	printf(_("  -d, --dbname=DBNAME                 database to connect to (default: "));
	if (runtime_options.dbname[0] != '\0')
		printf(_("\"%s\")\n"), runtime_options.dbname);
	else
		printf(_("\"%s\")\n"), runtime_options.username);

	printf(_("  -h, --host=HOSTNAME                 database server host"));
	if (runtime_options.host[0] != '\0')
		printf(_(" (default: \"%s\")"), runtime_options.host);
	printf(_("\n"));

	printf(_("  -p, --port=PORT                     database server port (default: \"%s\")\n"), runtime_options.port);
	printf(_("  -U, --username=USERNAME             database user name to connect as (default: \"%s\")\n"), runtime_options.username);

	puts("");

	printf(_("Node-specific options:\n"));
	printf(_("  -D, --pgdata=DIR                    location of the node's data directory \n"));
	printf(_("  --node-id                           specify a node by id (only available for some operations)\n"));
	printf(_("  --node-name                         specify a node by name (only available for some operations)\n"));

	puts("");

	printf(_("Logging options:\n"));
	printf(_("  --dry-run                           show what would happen for action, but don't execute it\n"));
	printf(_("  -L, --log-level                     set log level (overrides configuration file; default: NOTICE)\n"));
	printf(_("  --log-to-file                       log to file (or logging facility) defined in repmgr.conf\n"));
	printf(_("  -q, --quiet                         suppress all log output apart from errors\n"));
	printf(_("  -t, --terse                         don't display detail, hints and other non-critical output\n"));
	printf(_("  -v, --verbose                       display additional log output (useful for debugging)\n"));

	puts("");

	printf(_("%s home page: <%s>\n"), "repmgr", REPMGR_URL);
}


/*
 * Create the repmgr extension, and grant access for the repmgr
 * user if not a superuser.
 *
 * Note:
 *   This is one of two places where superuser rights are required.
 *   We should also consider possible scenarious where a non-superuser
 *   has sufficient privileges to install the extension.
 */

bool
create_repmgr_extension(PGconn *conn)
{
	PQExpBufferData query;
	PGresult   *res;

	ExtensionStatus extension_status = REPMGR_UNKNOWN;

	t_connection_user userinfo = T_CONNECTION_USER_INITIALIZER;
	bool		is_superuser = false;
	PGconn	   *superuser_conn = NULL;
	PGconn	   *schema_create_conn = NULL;
	t_extension_versions extversions = T_EXTENSION_VERSIONS_INITIALIZER;

	extension_status = get_repmgr_extension_status(conn, &extversions);

	switch (extension_status)
	{
		case REPMGR_UNKNOWN:
			log_error(_("unable to determine status of \"repmgr\" extension"));
			return false;

		case REPMGR_UNAVAILABLE:
			log_error(_("\"repmgr\" extension is not available"));
			return false;

		case REPMGR_OLD_VERSION_INSTALLED:
			log_error(_("an older version of the \"repmgr\" extension is installed"));
			log_detail(_("version %s is installed but newer version %s is available"),
					   extversions.installed_version,
					   extversions.default_version);
			log_hint(_("update the installed extension version by executing \"ALTER EXTENSION repmgr UPDATE\""));
			return false;

		case REPMGR_INSTALLED:
			log_info(_("\"repmgr\" extension is already installed"));
			return true;

		case REPMGR_AVAILABLE:
			if (runtime_options.dry_run == true)
			{
				log_notice(_("would now attempt to install extension \"repmgr\""));
			}
			else
			{
				log_notice(_("attempting to install extension \"repmgr\""));
			}
			break;
	}

	/* 3. Attempt to get a superuser connection */

	is_superuser = is_superuser_connection(conn, &userinfo);

	get_superuser_connection(&conn, &superuser_conn, &schema_create_conn);

	if (runtime_options.dry_run == true)
		return true;

	/* 4. Create extension */

	res = PQexec(schema_create_conn, "CREATE EXTENSION repmgr");

	if ((PQresultStatus(res) != PGRES_COMMAND_OK && PQresultStatus(res) != PGRES_TUPLES_OK))
	{
		log_error(_("unable to create \"repmgr\" extension:\n  %s"),
				  PQerrorMessage(schema_create_conn));
		log_hint(_("check that the provided user has sufficient privileges for CREATE EXTENSION"));

		PQclear(res);
		if (superuser_conn != NULL)
			PQfinish(superuser_conn);
		return false;
	}

	PQclear(res);

	/* 5. If not superuser, grant usage */
	if (is_superuser == false)
	{
		initPQExpBuffer(&query);

		appendPQExpBuffer(&query,
						  "GRANT USAGE ON SCHEMA repmgr TO %s",
						  userinfo.username);

		res = PQexec(schema_create_conn, query.data);
		termPQExpBuffer(&query);

		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			log_error(_("unable to grant usage on \"repmgr\" extension to %s:\n  %s"),
					  userinfo.username,
					  PQerrorMessage(schema_create_conn));
			PQclear(res);

			if (superuser_conn != 0)
				PQfinish(superuser_conn);

			return false;
		}

		initPQExpBuffer(&query);

		appendPQExpBuffer(&query,
						  "GRANT ALL ON ALL TABLES IN SCHEMA repmgr TO %s",
						  userinfo.username);

		res = PQexec(schema_create_conn, query.data);
		termPQExpBuffer(&query);

		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			log_error(_("unable to grant permission on tables on \"repmgr\" extension to %s:\n  %s"),
					  userinfo.username,
					  PQerrorMessage(schema_create_conn));
			PQclear(res);

			if (superuser_conn != NULL)
				PQfinish(superuser_conn);

			return false;
		}
	}

	if (superuser_conn != NULL)
		PQfinish(superuser_conn);

	log_notice(_("\"repmgr\" extension successfully installed"));

	create_event_notification(conn,
							  &config_file_options,
							  config_file_options.node_id,
							  "cluster_created",
							  true,
							  NULL);

	return true;
}


/**
 * check_server_version()
 *
 * Verify that the server is MIN_SUPPORTED_VERSION_NUM or later
 *
 * PGconn *conn:
 *	 the connection to check
 *
 * char *server_type:
 *	 either "primary" or "standby"; used to format error message
 *
 * bool exit_on_error:
 *	 exit if reported server version is too low; optional to enable some callers
 *	 to perform additional cleanup
 *
 * char *server_version_string
 *	 passed to get_server_version(), which will place the human-readable
 *	 server version string there (e.g. "9.4.0")
 */
int
check_server_version(PGconn *conn, char *server_type, bool exit_on_error, char *server_version_string)
{
	char		version_string[MAXVERSIONSTR] = "";
	int			conn_server_version_num = get_server_version(conn, version_string);

	/* Copy the version string, if the caller wants it */
	if (server_version_string != NULL)
		strncpy(server_version_string, version_string, MAXVERSIONSTR);

	if (conn_server_version_num < MIN_SUPPORTED_VERSION_NUM)
	{
		if (conn_server_version_num > 0)
		{
			log_error(_("%s requires %s to be PostgreSQL %s or later"),
					  progname(),
					  server_type,
					  MIN_SUPPORTED_VERSION);
			log_detail(_("%s server version is %s"),
					   server_type,
					   version_string);
		}

		if (exit_on_error == true)
		{
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		return UNKNOWN_SERVER_VERSION_NUM;
	}

	/*
	 * If it's clear a particular repmgr feature branch won't be able to support
	 * PostgreSQL from a particular PostgreSQL release onwards (e.g. 4.4 with PostgreSQL
	 * 12 and later due to recovery.conf removal), set MAX_UNSUPPORTED_VERSION and
	 * MAX_UNSUPPORTED_VERSION_NUM in "repmgr.h" to define the first PostgreSQL
	 * version which can't be suppored.
	 */
#ifdef MAX_UNSUPPORTED_VERSION_NUM
	if (conn_server_version_num >= MAX_UNSUPPORTED_VERSION_NUM)
	{
		if (conn_server_version_num > 0)
		{
			log_error(_("%s %s does not support PostgreSQL %s or later"),
					  progname(),
					  REPMGR_VERSION,
					  MAX_UNSUPPORTED_VERSION);
			log_detail(_("%s server version is %s"),
					   server_type,
					   version_string);
			log_hint(_("For details of supported versions see: https://repmgr.org/docs/current/install-requirements.html#INSTALL-COMPATIBILITY-MATRIX"));
		}

		if (exit_on_error == true)
		{
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		return UNKNOWN_SERVER_VERSION_NUM;
	}
#endif

	return conn_server_version_num;
}


int
test_ssh_connection(char *host, char *remote_user)
{
	char		script[MAXLEN] = "";
	int			r = 1,
				i;

	/*
	 * On some OS, true is located in a different place than in Linux we have
	 * to try them all until all alternatives are gone or we found `true'
	 * because the target OS may differ from the source OS
	 */
	const char *bin_true_paths[] = {
		"/bin/true",
		"/usr/bin/true",
		NULL
	};

	for (i = 0; bin_true_paths[i] && r != 0; ++i)
	{
		if (!remote_user[0])
			maxlen_snprintf(script, "ssh -o Batchmode=yes %s %s %s 2>/dev/null",
							config_file_options.ssh_options, host, bin_true_paths[i]);
		else
			maxlen_snprintf(script, "ssh -o Batchmode=yes %s %s -l %s %s 2>/dev/null",
							config_file_options.ssh_options, host, remote_user,
							bin_true_paths[i]);

		log_verbose(LOG_DEBUG, _("test_ssh_connection(): executing %s"), script);
		r = system(script);
	}

	if (r != 0)
		log_warning(_("unable to connect to remote host \"%s\" via SSH"), host);

	return r;
}




/*
 * get_superuser_connection()
 *
 * Check if provided connection "conn" is a superuser connection, if not attempt to
 * make a superuser connection "superuser_conn" with the provided --superuser parameter.
 *
 * "privileged_conn" is set to whichever connection is the superuser connection.
 */
void
get_superuser_connection(PGconn **conn, PGconn **superuser_conn, PGconn **privileged_conn)
{
	t_connection_user userinfo = T_CONNECTION_USER_INITIALIZER;
	t_conninfo_param_list conninfo_params = T_CONNINFO_PARAM_LIST_INITIALIZER;
	bool		is_superuser = false;

	/* this should never happen */
	if (PQstatus(*conn) != CONNECTION_OK)
	{
		log_error(_("no database connection available"));
		log_detail("\n%s", PQerrorMessage(*conn));
		exit(ERR_INTERNAL);
	}

	is_superuser = is_superuser_connection(*conn, &userinfo);

	if (is_superuser == true)
	{
		*privileged_conn = *conn;

		return;
	}

	if (runtime_options.superuser[0] == '\0')
	{
		log_error(_("\"%s\" is not a superuser and no superuser name supplied"), userinfo.username);
		log_hint(_("supply a valid superuser name with -S/--superuser"));
		PQfinish(*conn);
		exit(ERR_BAD_CONFIG);
	}

	initialize_conninfo_params(&conninfo_params, false);
	conn_to_param_list(*conn, &conninfo_params);
	param_set(&conninfo_params, "user", runtime_options.superuser);

	*superuser_conn = establish_db_connection_by_params(&conninfo_params, false);

	if (PQstatus(*superuser_conn) != CONNECTION_OK)
	{
		log_error(_("unable to establish superuser connection as \"%s\""),
				  runtime_options.superuser);

		PQfinish(*conn);
		exit(ERR_BAD_CONFIG);
	}

	/* check provided superuser really is superuser */
	if (!is_superuser_connection(*superuser_conn, NULL))
	{
		log_error(_("\"%s\" is not a superuser"), runtime_options.superuser);
		PQfinish(*superuser_conn);
		PQfinish(*conn);
		exit(ERR_BAD_CONFIG);
	}

	log_debug("established superuser connection as \"%s\"", runtime_options.superuser);

	*privileged_conn = *superuser_conn;
	return;
}


standy_clone_mode
get_standby_clone_mode(void)
{
	standy_clone_mode mode;

	if (*config_file_options.barman_host != '\0' && runtime_options.without_barman == false)
		mode = barman;
	else
		mode = pg_basebackup;

	return mode;
}


void
make_pg_path(PQExpBufferData *buf, const char *file)
{
	appendPQExpBuffer(buf, "%s%s",
					  pg_bindir, file);
}


int
copy_remote_files(char *host, char *remote_user, char *remote_path,
				  char *local_path, bool is_directory, int server_version_num)
{
	PQExpBufferData rsync_flags;
	char		script[MAXLEN] = "";
	char		host_string[MAXLEN] = "";
	int			r = 0;

	initPQExpBuffer(&rsync_flags);

	if (*config_file_options.rsync_options == '\0')
	{
		appendPQExpBufferStr(&rsync_flags,
							 "--archive --checksum --compress --progress --rsh=ssh");
	}
	else
	{
		appendPQExpBufferStr(&rsync_flags,
							 config_file_options.rsync_options);
	}

	if (runtime_options.force)
	{
		appendPQExpBufferStr(&rsync_flags,
							 " --delete --checksum");
	}

	if (!remote_user[0])
	{
		maxlen_snprintf(host_string, "%s", host);
	}
	else
	{
		maxlen_snprintf(host_string, "%s@%s", remote_user, host);
	}

	/*
	 * When copying the main PGDATA directory, certain files and contents of
	 * certain directories need to be excluded.
	 *
	 * See function 'sendDir()' in 'src/backend/replication/basebackup.c' -
	 * we're basically simulating what pg_basebackup does, but with rsync
	 * rather than the BASEBACKUP replication protocol command.
	 *
	 * *However* currently we'll always copy the contents of the 'pg_replslot'
	 * directory and delete later if appropriate.
	 */
	if (is_directory)
	{
		/* Files which we don't want */
		appendPQExpBufferStr(&rsync_flags,
							 " --exclude=postmaster.pid --exclude=postmaster.opts --exclude=global/pg_control");

		appendPQExpBufferStr(&rsync_flags,
							 " --exclude=recovery.conf --exclude=recovery.done");

		/*
		 * Ideally we'd use PG_AUTOCONF_FILENAME from utils/guc.h, but
		 * that has too many dependencies for a mere client program.
		 */
		appendPQExpBuffer(&rsync_flags, " --exclude=%s.tmp",
						  PG_AUTOCONF_FILENAME);

		/* Temporary files which we don't want, if they exist */
		appendPQExpBuffer(&rsync_flags, " --exclude=%s*",
						  PG_TEMP_FILE_PREFIX);

		/* Directories which we don't want */

		if (server_version_num >= 100000)
		{
			appendPQExpBufferStr(&rsync_flags,
								 " --exclude=pg_wal/* --exclude=log/*");
		}
		else
		{
			appendPQExpBufferStr(&rsync_flags,
								 " --exclude=pg_xlog/* --exclude=pg_log/*");
		}

		appendPQExpBufferStr(&rsync_flags,
							 " --exclude=pg_stat_tmp/*");

		maxlen_snprintf(script, "rsync %s %s:%s/* %s",
						rsync_flags.data, host_string, remote_path, local_path);
	}
	else
	{
		maxlen_snprintf(script, "rsync %s %s:%s %s",
						rsync_flags.data, host_string, remote_path, local_path);
	}

	termPQExpBuffer(&rsync_flags);

	log_info(_("rsync command line:\n  %s"), script);

	r = system(script);

	log_debug("copy_remote_files(): r = %i; WIFEXITED: %i; WEXITSTATUS: %i", r, WIFEXITED(r), WEXITSTATUS(r));

	/* exit code 24 indicates vanished files, which isn't a problem for us */
	if (WIFEXITED(r) && WEXITSTATUS(r) && WEXITSTATUS(r) != 24)
		log_verbose(LOG_WARNING, "copy_remote_files(): rsync returned unexpected exit status %i", WEXITSTATUS(r));

	return r;
}




void
make_remote_repmgr_path(PQExpBufferData *output_buf, t_node_info *remote_node_record)
{
	if (config_file_options.repmgr_bindir[0] != '\0')
	{
		int			len = strlen(config_file_options.repmgr_bindir);

		appendPQExpBufferStr(output_buf,
							 config_file_options.repmgr_bindir);

		/* Add trailing slash */
		if (config_file_options.repmgr_bindir[len - 1] != '/')
		{
			appendPQExpBufferChar(output_buf, '/');
		}
	}
	else if (pg_bindir[0] != '\0')
	{
		appendPQExpBufferStr(output_buf,
							 pg_bindir);
	}

	appendPQExpBuffer(output_buf,
					  "%s -f %s ",
					  progname(),
					  remote_node_record->config_file);

	/*
	 * If --log-level was explicitly supplied, pass that through
	 * to the remote repmgr client too.
	 */
	if (runtime_options.log_level[0] != '\0')
	{
		appendPQExpBuffer(output_buf,
						  " -L %s ",
						  runtime_options.log_level);
	}

}


void
make_repmgrd_path(PQExpBufferData *output_buf)
{
	if (config_file_options.repmgr_bindir[0] != '\0')
	{
		int			len = strlen(config_file_options.repmgr_bindir);

		appendPQExpBufferStr(output_buf,
							 config_file_options.repmgr_bindir);

		/* Add trailing slash */
		if (config_file_options.repmgr_bindir[len - 1] != '/')
		{
			appendPQExpBufferChar(output_buf, '/');
		}
	}
	else if (pg_bindir[0] != '\0')
	{
		appendPQExpBufferStr(output_buf,
							 pg_bindir);
	}

	appendPQExpBuffer(output_buf,
					  "repmgrd -f %s ",
					  config_file_path);
}


/* ======================== */
/* server control functions */
/* ======================== */

void
get_server_action(t_server_action action, char *script, char *data_dir)
{
	PQExpBufferData command;

	if (data_dir == NULL || data_dir[0] == '\0')
		data_dir = "(none provided)";

	switch (action)
	{
		case ACTION_NONE:
			script[0] = '\0';
			return;

		case ACTION_START:
			{
				if (config_file_options.service_start_command[0] != '\0')
				{
					maxlen_snprintf(script, "%s",
									config_file_options.service_start_command);
				}
				else
				{
					initPQExpBuffer(&command);

					make_pg_path(&command, "pg_ctl");

					appendPQExpBuffer(&command,
									  " %s -w -D ",
									  config_file_options.pg_ctl_options);

					appendShellString(&command,
									  data_dir);

					appendPQExpBuffer(&command,
									  " start");

					strncpy(script, command.data, MAXLEN);

					termPQExpBuffer(&command);
				}

				return;
			}

		case ACTION_STOP:
		case ACTION_STOP_WAIT:
			{
				if (config_file_options.service_stop_command[0] != '\0')
				{
					maxlen_snprintf(script, "%s",
									config_file_options.service_stop_command);
				}
				else
				{
					initPQExpBuffer(&command);
					make_pg_path(&command, "pg_ctl");

					appendPQExpBuffer(&command,
									  " %s -D ",
									  config_file_options.pg_ctl_options);

					appendShellString(&command,
									  data_dir);

					if (action == ACTION_STOP_WAIT)
						appendPQExpBuffer(&command,
										  " -w");
					else
						appendPQExpBuffer(&command,
										  " -W");

					appendPQExpBuffer(&command,
									  " -m fast stop");

					strncpy(script, command.data, MAXLEN);

					termPQExpBuffer(&command);
				}
				return;
			}

		case ACTION_RESTART:
			{
				if (config_file_options.service_restart_command[0] != '\0')
				{
					maxlen_snprintf(script, "%s",
									config_file_options.service_restart_command);
				}
				else
				{
					initPQExpBuffer(&command);

					make_pg_path(&command, "pg_ctl");

					appendPQExpBuffer(&command,
									  " %s -w -D ",
									  config_file_options.pg_ctl_options);

					appendShellString(&command,
									  data_dir);

					appendPQExpBuffer(&command,
									  " restart");

					strncpy(script, command.data, MAXLEN);

					termPQExpBuffer(&command);
				}
				return;
			}

		case ACTION_RELOAD:
			{
				if (config_file_options.service_reload_command[0] != '\0')
				{
					maxlen_snprintf(script, "%s",
									config_file_options.service_reload_command);
				}
				else
				{
					initPQExpBuffer(&command);

					make_pg_path(&command, "pg_ctl");

					appendPQExpBuffer(&command,
									  " %s -w -D ",
									  config_file_options.pg_ctl_options);

					appendShellString(&command,
									  data_dir);

					appendPQExpBuffer(&command,
									  " reload");

					strncpy(script, command.data, MAXLEN);

					termPQExpBuffer(&command);

				}
				return;
			}

		case ACTION_PROMOTE:
			{
				if (config_file_options.service_promote_command[0] != '\0')
				{
					maxlen_snprintf(script, "%s",
									config_file_options.service_promote_command);
				}
				else
				{
					initPQExpBuffer(&command);

					make_pg_path(&command, "pg_ctl");

					appendPQExpBuffer(&command,
									  " %s -w -D ",
									  config_file_options.pg_ctl_options);

					appendShellString(&command,
									  data_dir);

					appendPQExpBuffer(&command,
									  " promote");

					strncpy(script, command.data, MAXLEN);

					termPQExpBuffer(&command);
				}
				return;
			}

		default:
			return;
	}

	return;
}


bool
data_dir_required_for_action(t_server_action action)
{
	switch (action)
	{
		case ACTION_NONE:
			return false;

		case ACTION_START:
			if (config_file_options.service_start_command[0] != '\0')
			{
				return false;
			}
			return true;

		case ACTION_STOP:
		case ACTION_STOP_WAIT:
			if (config_file_options.service_stop_command[0] != '\0')
			{
				return false;
			}
			return true;

		case ACTION_RESTART:
			if (config_file_options.service_restart_command[0] != '\0')
			{
				return false;
			}
			return true;

		case ACTION_RELOAD:
			if (config_file_options.service_reload_command[0] != '\0')
			{
				return false;
			}
			return true;

		case ACTION_PROMOTE:
			if (config_file_options.service_promote_command[0] != '\0')
			{
				return false;
			}
			return true;

		default:
			return false;
	}

	return false;
}


/*
 * Copy the location of the configuration file directory into the
 * provided buffer; if "config_directory" provided, use that, otherwise
 * default to the data directory.
 *
 * This is primarily intended for use with "pg_ctl" (which itself shouldn't
 * be used outside of development environments).
 */
void
get_node_config_directory(char *config_dir_buf)
{
	if (config_file_options.config_directory[0] != '\0')
	{
		strncpy(config_dir_buf, config_file_options.config_directory, MAXPGPATH);
		return;
	}

	if (config_file_options.data_directory[0] != '\0')
	{
		strncpy(config_dir_buf, config_file_options.data_directory, MAXPGPATH);
		return;
	}

	return;
}


void
get_node_data_directory(char *data_dir_buf)
{
	/*
	 * the configuration file setting has priority, and will always be set
	 * when a configuration file was provided
	 */
	if (config_file_options.data_directory[0] != '\0')
	{
		strncpy(data_dir_buf, config_file_options.data_directory, MAXPGPATH);
		return;
	}

	if (runtime_options.data_dir[0] != '\0')
	{
		strncpy(data_dir_buf, runtime_options.data_dir, MAXPGPATH);
		return;
	}

	return;
}


/*
 * initialise a node record from the provided configuration
 * parameters
 */
void
init_node_record(t_node_info *node_record)
{
	node_record->node_id = config_file_options.node_id;
	node_record->upstream_node_id = runtime_options.upstream_node_id;
	node_record->priority = config_file_options.priority;
	node_record->active = true;

	if (config_file_options.location[0] != '\0')
		strncpy(node_record->location, config_file_options.location, MAXLEN);
	else
		strncpy(node_record->location, "default", MAXLEN);


	strncpy(node_record->node_name, config_file_options.node_name, sizeof(node_record->node_name));
	strncpy(node_record->conninfo, config_file_options.conninfo, MAXLEN);
	strncpy(node_record->config_file, config_file_path, MAXPGPATH);

	if (config_file_options.replication_user[0] != '\0')
	{
		/* replication user explicitly provided in configuration file */
		strncpy(node_record->repluser, config_file_options.replication_user, NAMEDATALEN);
	}
	else
	{
		/* use the "user" value from "conninfo" */
		char		repluser[MAXLEN] = "";

		(void) get_conninfo_value(config_file_options.conninfo, "user", repluser);
		strncpy(node_record->repluser, repluser, NAMEDATALEN);
	}

	if (config_file_options.use_replication_slots == true)
	{
		create_slot_name(node_record->slot_name, config_file_options.node_id);
	}
}


bool
can_use_pg_rewind(PGconn *conn, const char *data_directory, PQExpBufferData *reason)
{
	bool		can_use = true;

	/* "full_page_writes" must be on in any case */
	if (guc_set(conn, "full_page_writes", "=", "off"))
	{
		appendPQExpBuffer(reason,
						  _("\"full_page_writes\" must be set to \"on\""));

		can_use = false;
	}

	/*
	 * "wal_log_hints" off - are data checksums available? Note: we're
	 * checking the local pg_control file here as the value will be the same
	 * throughout the cluster and saves a round-trip to the demotion
	 * candidate.
	 */
	if (guc_set(conn, "wal_log_hints", "=", "on") == false)
	{
		int			data_checksum_version = get_data_checksum_version(data_directory);

		if (data_checksum_version == UNKNOWN_DATA_CHECKSUM_VERSION)
		{
			if (can_use == false)
				appendPQExpBuffer(reason, "; ");

			appendPQExpBuffer(reason,
							  _("\"wal_log_hints\" is set to \"off\" but unable to determine data checksum version"));
			can_use = false;
		}
		else if (data_checksum_version == 0)
		{
			if (can_use == false)
				appendPQExpBuffer(reason, "; ");

			appendPQExpBuffer(reason,
							  _("\"wal_log_hints\" is set to \"off\" and data checksums are disabled"));

			can_use = false;
		}
	}

	return can_use;
}


void
make_standby_signal_path(char *buf)
{
	snprintf(buf, MAXPGPATH,
			 "%s/%s",
			 config_file_options.data_directory,
			 STANDBY_SIGNAL_FILE);
}

/*
 * create standby.signal (PostgreSQL 12 and later)
 */
bool
write_standby_signal(void)
{
	char	    standby_signal_file_path[MAXPGPATH] = "";
	FILE	   *file;
	mode_t		um;

	make_standby_signal_path(standby_signal_file_path);

	/* Set umask to 0600 */
	um = umask((~(S_IRUSR | S_IWUSR)) & (S_IRWXG | S_IRWXO));
	file = fopen(standby_signal_file_path, "w");
	umask(um);

	if (file == NULL)
	{
		log_error(_("unable to create %s file at \"%s\""),
				  STANDBY_SIGNAL_FILE,
				  standby_signal_file_path);
		log_detail("%s", strerror(errno));

		return false;
	}

	if (fputs("# created by repmgr\n", file) == EOF)
	{
		log_error(_("unable to write to %s file at \"%s\""),
				  STANDBY_SIGNAL_FILE,
				  standby_signal_file_path);
		fclose(file);

		return false;
	}

	fclose(file);

	return true;
}


/*
 * NOTE:
 *  - the provided connection should be for the normal repmgr user
 *  - if upstream_node_record is not NULL, its "repluser" entry, if
 *    set, will be used as the fallback replication user
 */
bool
create_replication_slot(PGconn *conn, char *slot_name, t_node_info *upstream_node_record, PQExpBufferData *error_msg)
{
	PGconn *slot_conn = NULL;
	bool use_replication_protocol = false;
	bool success = true;
	char *replication_user = NULL;

	_determine_replication_slot_user(conn, upstream_node_record, &replication_user);
	/*
	 * If called in --dry-run context, if the replication slot user is not the
	 * repmgr user, attempt to validate the connection.
	 */
	if (runtime_options.dry_run == true)
	{
		switch (ReplicationSlotUser)
		{
			case USER_TYPE_UNKNOWN:
				log_error("unable to determine user for replication slot creation");
				return false;
			case  REPMGR_USER:
				log_info(_("replication slots will be created by user \"%s\""),
						 PQuser(conn));
				return true;

			case REPLICATION_USER_NODE:
			case REPLICATION_USER_OPT:
			{
				PGconn *repl_conn = duplicate_connection(conn,
														 replication_user,
														 true);
				if (repl_conn == NULL || PQstatus(repl_conn) != CONNECTION_OK)
				{
					log_error(_("unable to create replication connection as user \"%s\""),
							  replication_user);
					log_detail("%s", PQerrorMessage(repl_conn));

					PQfinish(repl_conn);
					return false;
				}
				log_info(_("replication slots will be created by replication user \"%s\""),
						 replication_user);
				PQfinish(repl_conn);
				return true;
			}
			case SUPERUSER:
			{
				PGconn *superuser_conn = duplicate_connection(conn,
															  runtime_options.superuser,
															  false);
				if (superuser_conn == NULL || PQstatus(superuser_conn )!= CONNECTION_OK)
				{
					log_error(_("unable to create superuser connection as user \"%s\""),
							  runtime_options.superuser);
					log_detail("%s", PQerrorMessage(superuser_conn));

					PQfinish(superuser_conn);

					return false;
				}

				log_info(_("replication slots will be created by superuser \"%s\""),
						 runtime_options.superuser);
				PQfinish(superuser_conn);
			}
		}

	}

	/*
	 * If we can't create a replication slot with the connection provided to
	 * the function, create an connection with appropriate permissions.
	 */
	switch (ReplicationSlotUser)
	{
		case USER_TYPE_UNKNOWN:
			log_error("unable to determine user for replication slot creation");
			return false;
		case  REPMGR_USER:
			slot_conn = conn;
			log_info(_("creating replication slot as user \"%s\""),
					 PQuser(conn));
			break;

		case REPLICATION_USER_NODE:
		case REPLICATION_USER_OPT:
		{
			slot_conn = duplicate_connection(conn,
											 replication_user,
											 true);
			if (slot_conn == NULL || PQstatus(slot_conn) != CONNECTION_OK)
			{
				log_error(_("unable to create replication connection as user \"%s\""),
						  runtime_options.replication_user);
				log_detail("%s", PQerrorMessage(slot_conn));

				PQfinish(slot_conn);
					return false;
			}
			use_replication_protocol = true;
			log_info(_("creating replication slot as replication user \"%s\""),
					 replication_user);
		}
			break;

		case SUPERUSER:
		{
			slot_conn = duplicate_connection(conn,
											 runtime_options.superuser,
											 false);
			if (slot_conn == NULL || PQstatus(slot_conn )!= CONNECTION_OK)
			{
				log_error(_("unable to create super connection as user \"%s\""),
						  runtime_options.superuser);
				log_detail("%s", PQerrorMessage(slot_conn));

				PQfinish(slot_conn);

				return false;
			}
			log_info(_("creating replication slot as superuser \"%s\""),
					 runtime_options.superuser);
		}
			break;
	}

	if (use_replication_protocol == true)
	{
		success = create_replication_slot_replprot(conn, slot_conn, slot_name, error_msg);
	}
	else
	{
		success = create_replication_slot_sql(slot_conn, slot_name, error_msg);
	}


	if (slot_conn != conn)
		PQfinish(slot_conn);

	return success;
}


bool
drop_replication_slot_if_exists(PGconn *conn, int node_id, char *slot_name)
{
	t_node_info node_record = T_NODE_INFO_INITIALIZER;
	t_replication_slot slot_info = T_REPLICATION_SLOT_INITIALIZER;
	RecordStatus record_status;

	char *replication_user = NULL;
	bool success = true;

	if (node_id != UNKNOWN_NODE_ID)
	{
		record_status = get_node_record(conn, node_id, &node_record);
	}

	_determine_replication_slot_user(conn, &node_record, &replication_user);

	record_status = get_slot_record(conn, slot_name, &slot_info);

	log_verbose(LOG_DEBUG, "attempting to delete slot \"%s\" on node %i",
				slot_name, node_id);

	if (record_status != RECORD_FOUND)
	{
		/* this is not a bad good thing */
		log_verbose(LOG_INFO,
					_("slot \"%s\" does not exist on node %i, nothing to remove"),
					slot_name, node_id);
		return true;
	}

	if (slot_info.active == false)
	{
		if (drop_replication_slot_sql(conn, slot_name) == true)
		{
			log_notice(_("replication slot \"%s\" deleted on node %i"), slot_name, node_id);
		}
		else
		{
			log_error(_("unable to delete replication slot \"%s\" on node %i"), slot_name, node_id);
			success = false;
		}
	}

	/*
	 * If an active replication slot exists, call Houston as we have a
	 * problem.
	 */
	else
	{
		log_warning(_("replication slot \"%s\" is still active on node %i"), slot_name, node_id);
		success = false;
	}

	return success;
}


static void
_determine_replication_slot_user(PGconn *conn, t_node_info *upstream_node_record, char **replication_user)
{
	/*
	 * If not previously done, work out which user will be responsible
	 * for creating replication slots.
	 */
	if (ReplicationSlotUser == USER_TYPE_UNKNOWN)
	{
		/*
		 * Is the repmgr user a superuser?
		 */
		if (is_superuser_connection(conn, NULL))
		{
			ReplicationSlotUser = REPMGR_USER;
		}
		/*
		 * Does the repmgr user have the REPLICATION role?
		 * Note we don't care here whether the repmgr user can actually
		 * make a replication connection, we're just confirming that the
		 * connection we have has the appropriate permissions.
		 */
		else if (is_replication_role(conn, NULL))
		{
			ReplicationSlotUser = REPMGR_USER;
		}
		/*
		 * Is a superuser provided with --superuser?
		 * We'll check later whether we can make a connection as that user.
		 */
		else if (runtime_options.superuser[0] != '\0')
		{
			ReplicationSlotUser = SUPERUSER;
		}
		/*
		 * Is a replication user provided with --replication-user?
		 * We'll check later whether we can make a replication connection as that user.
		 * Overrides any replication user defined in the upstream node record.
		 */
		else if (runtime_options.replication_user[0] != '\0')
		{
			ReplicationSlotUser = REPLICATION_USER_OPT;
			*replication_user = runtime_options.replication_user;
		}
		/*
		 * Is the upstream's node record provided, and does it have a different
		 * replication user?
		 * We'll check later whether we can make a replication connection as that user.
		 */
		else if (upstream_node_record != NULL && upstream_node_record->node_id != UNKNOWN_NODE_ID
			 && strncmp(upstream_node_record->repluser, PQuser(conn), NAMEDATALEN) != 0)
		{
			ReplicationSlotUser = REPLICATION_USER_NODE;
			*replication_user = upstream_node_record->repluser;
		}
	}
}


bool
check_replication_slots_available(int node_id, PGconn* conn)
{
	int max_replication_slots = UNKNOWN_VALUE;
	int free_slots = get_free_replication_slot_count(conn, &max_replication_slots);

	if (free_slots < 0)
	{
		log_error(_("unable to determine number of free replication slots on node %i"),
				  node_id);
		return false;
	}

	if (free_slots == 0)
	{
		log_error(_("no free replication slots available on node %i"),
				  node_id);
		log_hint(_("consider increasing \"max_replication_slots\" (current value: %i)"),
				 max_replication_slots);
		return false;
	}
	else if (runtime_options.dry_run == true)
	{
		log_info(_("replication slots in use, %i free slots on node %i"),
				 node_id,
				 free_slots);
	}

	return true;
}


/*
 * Check whether the specified standby has joined to its upstream.
 *
 * This is used by "standby switchover" and "node rejoin" to check
 * the success of a node rejoin operation.
 *
 * IMPORTANT: the timeout settings will be taken from the node where the check
 * is performed, which might not be the standby itself.
 */
standy_join_status
check_standby_join(PGconn *upstream_conn, t_node_info *upstream_node_record, t_node_info *standby_node_record)
 {
	 int i;
	 bool available = false;

	 for (i = 0; i < config_file_options.standby_reconnect_timeout; i++)
	 {
		 if (is_server_available(config_file_options.conninfo))
		 {
			 log_verbose(LOG_INFO, _("node \"%s\" (ID: %i) is pingable"),
						 standby_node_record->node_name,
						 standby_node_record->node_id);
			 available = true;
			 break;
		 }

		 if (i % 5 == 0)
		 {
			 log_verbose(LOG_INFO, _("waiting for node \"%s\" (ID: %i) to respond to pings; %i of max %i attempts (parameter \"node_rejoin_timeout\")"),
						 standby_node_record->node_name,
						 standby_node_record->node_id,
						 i + 1,
						 config_file_options.node_rejoin_timeout);
		 }
		 else
		 {
			 log_debug("sleeping 1 second waiting for node \"%s\" (ID: %i) to respond to pings; %i of max %i attempts",
					   standby_node_record->node_name,
					   standby_node_record->node_id,
					   i + 1,
					   config_file_options.node_rejoin_timeout);
		 }

		 sleep(1);
	 }

	 /* node did not become available */
	 if (available == false)
	 {
		 return JOIN_FAIL_NO_PING;
	 }

	 for (; i < config_file_options.node_rejoin_timeout; i++)
	 {
		 char *node_state = NULL;
		 NodeAttached node_attached = is_downstream_node_attached(upstream_conn,
																  standby_node_record->node_name,
																  &node_state);
		 if (node_attached == NODE_ATTACHED)
		 {
			 log_verbose(LOG_INFO, _("node \"%s\" (ID: %i) has attached to its upstream node"),
						 standby_node_record->node_name,
						 standby_node_record->node_id);
			 return JOIN_SUCCESS;
		 }

		 if (i % 5 == 0)
		 {
			 log_info(_("waiting for node \"%s\" (ID: %i) to connect to new primary; %i of max %i attempts (parameter \"node_rejoin_timeout\")"),
					  standby_node_record->node_name,
					  standby_node_record->node_id,
					  i + 1,
					  config_file_options.node_rejoin_timeout);

			 if (node_attached == NODE_NOT_ATTACHED)
			 {
				 log_detail(_("node \"%s\" (ID: %i) is currrently attached to its upstream node in state \"%s\""),
							upstream_node_record->node_name,
							standby_node_record->node_id,
							node_state);
			 }
			 else
			 {
				 log_detail(_("checking for record in node \"%s\"'s \"pg_stat_replication\" table where \"application_name\" is \"%s\""),
							upstream_node_record->node_name,
							standby_node_record->node_name);
			 }
		 }
		 else
		 {
			 log_debug("sleeping 1 second waiting for node  \"%s\" (ID: %i) to connect to new primary; %i of max %i attempts",
					   standby_node_record->node_name,
					   standby_node_record->node_id,
					   i + 1,
					   config_file_options.node_rejoin_timeout);
		 }

		 sleep(1);
	 }

	 return JOIN_FAIL_NO_REPLICATION;
}


/*
 * Here we'll perform some timeline sanity checks to ensure the follow target
 * can actually be followed or rejoined.
 *
 * See also comment for check_node_can_follow() in repmgrd-physical.c .
 */
bool
check_node_can_attach(TimeLineID local_tli, XLogRecPtr local_xlogpos, PGconn *follow_target_conn, t_node_info *follow_target_node_record, bool is_rejoin)
{
	uint64		local_system_identifier = UNKNOWN_SYSTEM_IDENTIFIER;
	PGconn	   *follow_target_repl_conn = NULL;
	t_system_identification follow_target_identification = T_SYSTEM_IDENTIFICATION_INITIALIZER;
	bool success = true;

	const char *action = is_rejoin == true ? "rejoin" : "follow";

	/* check replication connection */
	follow_target_repl_conn = establish_replication_connection_from_conn(follow_target_conn,
																		 follow_target_node_record->repluser);

	if (PQstatus(follow_target_repl_conn) != CONNECTION_OK)
	{
		log_error(_("unable to establish a replication connection to the %s target node"), action);
		return false;
	}
	else if (runtime_options.dry_run == true)
	{
		log_info(_("replication connection to the %s target node was successful"), action);
	}

	/* check system_identifiers match */
	if (identify_system(follow_target_repl_conn, &follow_target_identification) == false)
	{
		log_error(_("unable to query the %s target node's system identification"), action);

		PQfinish(follow_target_repl_conn);
		return false;
	}

	local_system_identifier = get_system_identifier(config_file_options.data_directory);

	/*
	 * Check for things that should never happen, but expect the unexpected anyway.
	 */

	if (local_system_identifier == UNKNOWN_SYSTEM_IDENTIFIER)
	{
		/*
		 * We don't return immediately here so subsequent checks can be
		 * made, but indicate the node will not be able to rejoin.
		 */
		success = false;
		if (runtime_options.dry_run == true)
		{
			log_warning(_("unable to retrieve system identifier from pg_control"));
		}
		else
		{
			log_error(_("unable to retrieve system identifier from pg_control, aborting"));
		}
	}
	else if (follow_target_identification.system_identifier != local_system_identifier)
	{
		/*
		 * It's never going to be possible to rejoin a node from another cluster,
		 * so no need to bother with further checks.
		 */
		log_error(_("this node is not part of the %s target node's replication cluster"), action);
		log_detail(_("this node's system identifier is %lu, %s target node's system identifier is %lu"),
				   local_system_identifier,
				   action,
				   follow_target_identification.system_identifier);
		PQfinish(follow_target_repl_conn);
		return false;
	}
	else if (runtime_options.dry_run == true)
	{
		log_info(_("local and %s target system identifiers match"), action);
		log_detail(_("system identifier is %lu"), local_system_identifier);
	}

	/* check timelines */

	log_verbose(LOG_DEBUG, "local timeline: %i; %s target timeline: %i",
				local_tli,
				action,
				follow_target_identification.timeline);

	/*
	 * The upstream's timeline is lower than ours - we cannot follow, and rejoin
	 * requires PostgreSQL 9.6 and later.
	 */
	if (follow_target_identification.timeline < local_tli)
	{
		/*
		 * "repmgr standby follow" is impossible in this case
		 */
		if (is_rejoin == false)
		{
			log_error(_("this node's timeline is ahead of the %s target node's timeline"), action);
			log_detail(_("this node's timeline is %i, %s target node's timeline is %i"),
					   local_tli,
					   action,
					   follow_target_identification.timeline);

			if (PQserverVersion(follow_target_conn) >= 90600)
			{
				log_hint(_("use \"repmgr node rejoin --force-rewind\" to reattach this node"));
			}

			PQfinish(follow_target_repl_conn);
			return false;
		}

		/*
		 * pg_rewind can only rejoin to a lower timeline from PostgreSQL 9.6
		 */
		if (PQserverVersion(follow_target_conn) < 90600)
		{
			log_error(_("this node's timeline is ahead of the %s target node's timeline"), action);
			log_detail(_("this node's timeline is %i, %s target node's timeline is %i"),
					   local_tli,
					   action,
					   follow_target_identification.timeline);

			if (runtime_options.force_rewind_used == true)
			{
				log_hint(_("pg_rewind can only be used to rejoin to a node with a lower timeline from PostgreSQL 9.6"));
			}

			PQfinish(follow_target_repl_conn);
			return false;
		}

		if (runtime_options.force_rewind_used == false)
		{
			log_notice(_("pg_rewind execution required for this node to attach to rejoin target node %i"),
					   follow_target_node_record->node_id);
			log_hint(_("provide --force-rewind"));
			PQfinish(follow_target_repl_conn);
			return false;
		}
	}

	/* timelines are the same - check relative positions */
	else if (follow_target_identification.timeline == local_tli)
	{
		XLogRecPtr follow_target_xlogpos = get_node_current_lsn(follow_target_conn);

		if (local_xlogpos == InvalidXLogRecPtr || follow_target_xlogpos == InvalidXLogRecPtr)
		{
			log_error(_("unable to compare LSN positions"));
			PQfinish(follow_target_repl_conn);
			return false;
		}

		if (local_xlogpos <= follow_target_xlogpos)
		{
			log_info(_("timelines are same, this server is not ahead"));
			log_detail(_("local node lsn is %X/%X, %s target lsn is %X/%X"),
					   format_lsn(local_xlogpos),
					   action,
					   format_lsn(follow_target_xlogpos));
		}
		else
		{
			/*
			 * Unable to follow or join to a node we're ahead of, if we're on the
			 * same timeline. Also, pg_rewind does not detect this situation,
			 * as there is no definitive fork point.
			 *
			 * Note that Pg will still happily attach to the upstream in state "streaming"
			 * for a while but then detach with an endless stream of
			 * "record with incorrect prev-link" errors.
			 */
			log_error(_("this node ahead of the %s target on the same timeline (%i)"), action, local_tli);
			log_detail(_("local node lsn is %X/%X, %s target lsn is %X/%X"),
					   format_lsn(local_xlogpos),
					   action,
					   format_lsn(follow_target_xlogpos));

			if (is_rejoin == true)
			{
				log_hint(_("the --force-rewind option is ineffective in this case"));
			}

			success = false;
		}
	}
	else
	{
		/*
		 * upstream has higher timeline - check where it forked off from this node's timeline
		 */
		TimeLineHistoryEntry *follow_target_history = get_timeline_history(follow_target_repl_conn,
																		   local_tli + 1);

		if (follow_target_history == NULL)
		{
			/* get_timeline_history() will emit relevant error messages */
			PQfinish(follow_target_repl_conn);
			return false;
		}

		log_debug("local tli: %i; local_xlogpos: %X/%X; follow_target_history->tli: %i; follow_target_history->end: %X/%X",
				  local_tli,
				  format_lsn(local_xlogpos),
				  follow_target_history->tli,
				  format_lsn(follow_target_history->end));

		/*
		 * Local node has proceeded beyond the follow target's fork, so we
		 * definitely can't attach.
		 *
		 * This could be the case if the follow target was promoted, but does
		 * not contain all changes which are being replayed to this standby.
		 */
		if (local_xlogpos > follow_target_history->end)
		{
			if (is_rejoin == true && runtime_options.force_rewind_used == true)
			{
				log_notice(_("pg_rewind execution required for this node to attach to rejoin target node %i"),
						   follow_target_node_record->node_id);
			}
			else
			{
				log_error(_("this node cannot attach to %s target node %i"),
						  action,
						  follow_target_node_record->node_id);
				success = false;
			}

			log_detail(_("%s target server's timeline %i forked off current database system timeline %i before current recovery point %X/%X"),
					   action,
					   local_tli + 1,
					   local_tli,
					   format_lsn(local_xlogpos));

			if (is_rejoin == true && runtime_options.force_rewind_used == false)
			{
				log_hint(_("use --force-rewind to execute pg_rewind"));
			}
		}

		if (success == true)
		{
			if (is_rejoin == false || (is_rejoin == true && runtime_options.force_rewind_used == false))
			{
				log_info(_("local node %i can attach to %s target node %i"),
						 config_file_options.node_id,
						 action,
						 follow_target_node_record->node_id);

				log_detail(_("local node's recovery point: %X/%X; %s target node's fork point: %X/%X"),
						   format_lsn(local_xlogpos),
						   action,
						   format_lsn(follow_target_history->end));
			}
		}

		pfree(follow_target_history);
	}

	PQfinish(follow_target_repl_conn);

	return success;
}


/*
 * Check that the replication configuration file is owned by the user who
 * owns the data directory.
 */
extern bool
check_replication_config_owner(int pg_version, const char *data_directory, PQExpBufferData *error_msg, PQExpBufferData *detail_msg)
{
	PQExpBufferData replication_config_file;
	struct stat     dirstat;
	struct stat     confstat;

	if (stat(data_directory, &dirstat))
	{
		if (error_msg != NULL)
		{
			appendPQExpBuffer(error_msg,
							  "unable to check ownership of data directory \"%s\"",
							  data_directory);
			appendPQExpBufferStr(detail_msg,
								 strerror(errno));
		}
		return false;
	}

	initPQExpBuffer(&replication_config_file);

	appendPQExpBuffer(&replication_config_file,
					  "%s/%s",
					  config_file_options.data_directory,
					  pg_version >= 120000 ? PG_AUTOCONF_FILENAME : RECOVERY_COMMAND_FILE);

	stat(replication_config_file.data, &confstat);

	if (confstat.st_uid == dirstat.st_uid)
	{
		termPQExpBuffer(&replication_config_file);
		return true;
	}

	if (error_msg != NULL)
	{
		char conf_owner[MAXLEN];
		char dir_owner[MAXLEN];
		struct passwd *pw;

		pw = getpwuid(confstat.st_uid);
		if (!pw)
		{
			maxlen_snprintf(conf_owner,
							"(unknown user %i)",
							confstat.st_uid);
		}
		else
		{
			strncpy(conf_owner, pw->pw_name, MAXLEN);
		}

		pw = getpwuid(dirstat.st_uid);

		if (!pw)
		{
			maxlen_snprintf(conf_owner,
							"(unknown user %i)",
							dirstat.st_uid);
		}
		else
		{
			strncpy(dir_owner, pw->pw_name, MAXLEN);
		}

		appendPQExpBuffer(error_msg,
						  "ownership error for file \"%s\"",
						  replication_config_file.data);
		appendPQExpBuffer(detail_msg,
						  "file owner is \"%s\", data directory owner is \"%s\"",
						  conf_owner,
						  dir_owner);
	}

	termPQExpBuffer(&replication_config_file);

	return false;
}


/*
 * Simple check to see if "shared_preload_libraries" includes "repmgr".
 * Parsing "shared_preload_libraries" is non-trivial, as it's potentially
 * a comma-separated list, and worse may not be readable by the repmgr
 * user.
 *
 * Instead, we check if a function which should return a value returns
 * NULL; this indicates the shared library is not installed.
 */
void
check_shared_library(PGconn *conn)
{
	bool ok = repmgrd_check_local_node_id(conn);

	if (ok == true)
		return;

	log_error(_("repmgrd not configured for this node"));
	log_hint(_("ensure \"shared_preload_libraries\" includes \"repmgr\" and restart PostgreSQL"));
	PQfinish(conn);
	exit(ERR_BAD_CONFIG);
}


bool
is_repmgrd_running(PGconn *conn)
{
	pid_t		pid;
	bool		is_running = false;

	pid = repmgrd_get_pid(conn);

	if (pid != UNKNOWN_PID)
	{
		if (kill(pid, 0) != -1)
		{
			is_running = true;
		}
	}

	return is_running;
}


/**
 * Parse the string returned by "repmgr --version", e.g. "repmgr 4.1.2",
 * and return it as a version integer (e.g. 40102).
 *
 * This is required for backwards compatibility as versions prior to
 * 4.3 do not have the --version-number option.
 */
int
parse_repmgr_version(const char *version_string)
{
	int series, major, minor;
	int version_integer = UNKNOWN_REPMGR_VERSION_NUM;
	PQExpBufferData sscanf_string;

	initPQExpBuffer(&sscanf_string);

	appendPQExpBuffer(&sscanf_string, "%s ",
					  progname());
	appendPQExpBufferStr(&sscanf_string, "%i.%i.%i");

	if (sscanf(version_string, sscanf_string.data, &series, &major, &minor) == 3)
	{
		version_integer = (series * 10000) + (major * 100) + minor;
	}
	else
	{
		resetPQExpBuffer(&sscanf_string);
		appendPQExpBuffer(&sscanf_string, "%s ",
						  progname());
		appendPQExpBufferStr(&sscanf_string, "%i.%i");

		if (sscanf(version_string, "repmgr %i.%i", &series, &major) == 2)
		{
			version_integer = (series * 10000) + (major * 100);
		}
	}

	return version_integer;
}
