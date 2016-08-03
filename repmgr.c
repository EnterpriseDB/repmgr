/*
 * repmgr.c - Command interpreter for the repmgr package
 *
 * Copyright (C) 2ndQuadrant, 2010-2016
 *
 * This module is a command-line utility to easily setup a cluster of
 * hot standby servers for an HA environment
 *
 * Commands implemented are:
 *
 * [ MASTER | PRIMARY ] REGISTER
 *
 * STANDBY REGISTER
 * STANDBY UNREGISTER
 * STANDBY CLONE
 * STANDBY FOLLOW
 * STANDBY PROMOTE
 * STANDBY SWITCHOVER
 *
 * WITNESS CREATE
 * WITNESS REGISTER
 * WITNESS UNREGISTER
 *
 * CLUSTER SHOW
 * CLUSTER CLEANUP
 *
 * For internal use:
 * STANDBY ARCHIVE-CONFIG
 * STANDBY RESTORE-CONFIG
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

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "storage/fd.h"         /* for PG_TEMP_FILE_PREFIX */
#include "pqexpbuffer.h"

#include "log.h"
#include "config.h"
#include "check_dir.h"
#include "strutil.h"
#include "version.h"

#ifndef RECOVERY_COMMAND_FILE
#define RECOVERY_COMMAND_FILE "recovery.conf"
#endif

#ifndef TABLESPACE_MAP
#define TABLESPACE_MAP "tablespace_map"
#endif

#define WITNESS_DEFAULT_PORT "5499" /* If this value is ever changed, remember
									 * to update comments and documentation */

#define NO_ACTION			0		/* Dummy default action */
#define MASTER_REGISTER		1
#define STANDBY_REGISTER	2
#define STANDBY_UNREGISTER	3
#define STANDBY_CLONE		4
#define STANDBY_PROMOTE		5
#define STANDBY_FOLLOW		6
#define STANDBY_SWITCHOVER  7
#define STANDBY_ARCHIVE_CONFIG 8
#define STANDBY_RESTORE_CONFIG 9
#define WITNESS_CREATE		   10
#define WITNESS_REGISTER       11
#define WITNESS_UNREGISTER     12
#define CLUSTER_SHOW		   13
#define CLUSTER_CLEANUP		   14


static int	test_ssh_connection(char *host, char *remote_user);
static int  copy_remote_files(char *host, char *remote_user, char *remote_path,
							  char *local_path, bool is_directory, int server_version_num);
static int  run_basebackup(const char *data_dir, int server_version);
static void check_parameters_for_action(const int action);
static bool create_schema(PGconn *conn);
static bool create_recovery_file(const char *data_dir, PGconn *primary_conn);
static void write_primary_conninfo(char *line, PGconn *primary_conn);
static bool write_recovery_file_line(FILE *recovery_file, char *recovery_file_path, char *line);
static void check_master_standby_version_match(PGconn *conn, PGconn *master_conn);
static int	check_server_version(PGconn *conn, char *server_type, bool exit_on_error, char *server_version_string);
static bool check_upstream_config(PGconn *conn, int server_version_num, bool exit_on_error);
static bool update_node_record_set_master(PGconn *conn, int this_node_id);

static char *make_pg_path(char *file);

static void do_master_register(void);

static void do_standby_register(void);
static void do_standby_unregister(void);
static void do_standby_clone(void);
static void do_standby_promote(void);
static void do_standby_follow(void);
static void do_standby_switchover(void);
static void do_standby_archive_config(void);
static void do_standby_restore_config(void);

static void do_witness_create(void);
static void do_witness_register(PGconn *masterconn);
static void do_witness_unregister(void);

static void do_cluster_show(void);
static void do_cluster_cleanup(void);
static void do_check_upstream_config(void);
static void do_help(void);

static void exit_with_errors(void);
static void print_error_list(ItemList *error_list, int log_level);

static bool remote_command(const char *host, const char *user, const char *command, PQExpBufferData *outputbuf);
static void format_db_cli_params(const char *conninfo, char *output);
static bool copy_file(const char *old_filename, const char *new_filename);

static bool read_backup_label(const char *local_data_directory, struct BackupLabel *out_backup_label);

static void param_set(const char *param, const char *value);

static void parse_pg_basebackup_options(const char *pg_basebackup_options, t_basebackup_options *backup_options);

/* Global variables */
static PQconninfoOption *opts = NULL;

static int   param_count = 0;
static char  **param_keywords;
static char  **param_values;

static bool	config_file_required = true;

/* Initialization of runtime options */
t_runtime_options runtime_options = T_RUNTIME_OPTIONS_INITIALIZER;
t_configuration_options options = T_CONFIGURATION_OPTIONS_INITIALIZER;

static bool  wal_keep_segments_used = false;
static bool  conninfo_provided = false;
static bool  connection_param_provided = false;
static bool  host_param_provided = false;
static bool  pg_rewind_supplied = false;

static char *server_mode = NULL;
static char *server_cmd = NULL;

static char  pg_bindir[MAXLEN] = "";
static char  repmgr_slot_name[MAXLEN] = "";
static char *repmgr_slot_name_ptr = NULL;
static char  path_buf[MAXLEN] = "";

/* Collate command line errors and warnings here for friendlier reporting */
ItemList	cli_errors = { NULL, NULL };
ItemList	cli_warnings = { NULL, NULL };

static	struct BackupLabel backup_label;

int
main(int argc, char **argv)
{
	static struct option long_options[] =
	{
		{"dbname", required_argument, NULL, 'd'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"username", required_argument, NULL, 'U'},
		{"superuser", required_argument, NULL, 'S'},
		{"data-dir", required_argument, NULL, 'D'},
		/* alias for -D/--data-dir, following pg_ctl usage */
		{"pgdata", required_argument, NULL, 'D'},
		{"config-file", required_argument, NULL, 'f'},
		{"remote-user", required_argument, NULL, 'R'},
		{"wal-keep-segments", required_argument, NULL, 'w'},
		{"keep-history", required_argument, NULL, 'k'},
		{"force", no_argument, NULL, 'F'},
		{"wait", no_argument, NULL, 'W'},
		{"verbose", no_argument, NULL, 'v'},
		{"pg_bindir", required_argument, NULL, 'b'},
		{"rsync-only", no_argument, NULL, 'r'},
		{"fast-checkpoint", no_argument, NULL, 'c'},
		{"log-level", required_argument, NULL, 'L'},
		{"terse", required_argument, NULL, 't'},
		{"mode", required_argument, NULL, 'm'},
		{"remote-config-file", required_argument, NULL, 'C'},
		{"help", no_argument, NULL, OPT_HELP},
		{"check-upstream-config", no_argument, NULL, OPT_CHECK_UPSTREAM_CONFIG},
		{"recovery-min-apply-delay", required_argument, NULL, OPT_RECOVERY_MIN_APPLY_DELAY},
		{"ignore-external-config-files", no_argument, NULL, OPT_IGNORE_EXTERNAL_CONFIG_FILES},
		{"config-archive-dir", required_argument, NULL, OPT_CONFIG_ARCHIVE_DIR},
		{"pg_rewind", optional_argument, NULL, OPT_PG_REWIND},
		{"pwprompt", optional_argument, NULL, OPT_PWPROMPT},
		{"csv", no_argument, NULL, OPT_CSV},
		{"node", required_argument, NULL, OPT_NODE},
		{"version", no_argument, NULL, 'V'},
		{NULL, 0, NULL, 0}
	};

	int			optindex;
	int			c, targ;
	int			action = NO_ACTION;
	bool 		check_upstream_config = false;
	bool 		config_file_parsed = false;
	char 	   *ptr = NULL;

	PQconninfoOption *defs = NULL;
	PQconninfoOption *def;

	set_progname(argv[0]);

	/* Disallow running as root to prevent directory ownership problems */
	if (geteuid() == 0)
	{
		fprintf(stderr,
				_("%s: cannot be run as root\n"
				  "Please log in (using, e.g., \"su\") as the "
				  "(unprivileged) user that owns\n"
				  "the data directory.\n"
				),
				progname());
		exit(1);
	}

	param_count = 0;
	defs = PQconndefaults();

	/* Count maximum number of parameters */
	for (def = defs; def->keyword; def++)
		param_count ++;

	/* Initialize our internal parameter list */
	param_keywords = pg_malloc0(sizeof(char *) * (param_count + 1));
	param_values = pg_malloc0(sizeof(char *) * (param_count + 1));

	for (c = 0; c <= param_count; c++)
	{
		param_keywords[c] = NULL;
		param_values[c] = NULL;
	}

	/*
	 * Pre-set any defaults, which can be overwritten if matching
	 * command line parameters are provided
	 */

	for (def = defs; def->keyword; def++)
	{
		if (def->val != NULL && def->val[0] != '\0')
		{
			param_set(def->keyword, def->val);
		}

		if (strcmp(def->keyword, "host") == 0 &&
			(def->val != NULL && def->val[0] != '\0'))
		{
			strncpy(runtime_options.host, def->val, MAXLEN);
		}
		else if (strcmp(def->keyword, "hostaddr") == 0 &&
			(def->val != NULL && def->val[0] != '\0'))
		{
			strncpy(runtime_options.host, def->val, MAXLEN);
		}
		else if (strcmp(def->keyword, "port") == 0 &&
				 (def->val != NULL && def->val[0] != '\0'))
		{
			strncpy(runtime_options.masterport, def->val, MAXLEN);
		}
		else if (strcmp(def->keyword, "dbname") == 0 &&
				 (def->val != NULL && def->val[0] != '\0'))
		{
			strncpy(runtime_options.dbname, def->val, MAXLEN);
		}
		else if (strcmp(def->keyword, "user") == 0 &&
				 (def->val != NULL && def->val[0] != '\0'))
		{
			strncpy(runtime_options.username, def->val, MAXLEN);
		}
	}

	PQconninfoFree(defs);

	/* set default user for -R/--remote-user */

	{
		struct passwd *pw = NULL;

		pw = getpwuid(geteuid());
		if (pw == NULL)
		{
			fprintf(stderr, _("could not get current user name: %s\n"), strerror(errno));
			exit(ERR_BAD_CONFIG);
		}

		strncpy(runtime_options.username, pw->pw_name, MAXLEN);
	}

	/*
	 * Though libpq will default to the username as dbname, PQconndefaults()
	 * doesn't return this
	 */
	if (runtime_options.dbname[0] == '\0')
	{
		strncpy(runtime_options.dbname, runtime_options.username, MAXLEN);
	}

	/* Prevent getopt_long() from printing an error message */
	opterr = 0;

	while ((c = getopt_long(argc, argv, "?Vd:h:p:U:S:D:f:R:w:k:FWIvb:rcL:tm:C:", long_options,
							&optindex)) != -1)
	{
		/*
		 * NOTE: some integer parameters (e.g. -p/--port) are stored internally
		 * as strings. We use repmgr_atoi() to check these but discard the
		 * returned integer; repmgr_atoi() will append the error message to the
		 * provided list.
		 */
		switch (c)
		{
			case '?':
				/* Actual help option given */
				if (strcmp(argv[optind - 1], "-?") == 0)
				{
					do_help();
					exit(SUCCESS);
				}
				/* unknown option reported by getopt */
				else
					goto unknown_option;
				break;
			case OPT_HELP:
				do_help();
				exit(SUCCESS);
			case 'V':
				printf("%s %s (PostgreSQL %s)\n", progname(), REPMGR_VERSION, PG_VERSION);
				exit(SUCCESS);
			case 'd':
				strncpy(runtime_options.dbname, optarg, MAXLEN);
				/* we'll set the dbname parameter below  if we detect it's not a conninfo string */
				connection_param_provided = true;
				break;
			case 'h':
				strncpy(runtime_options.host, optarg, MAXLEN);
				param_set("host", optarg);
				connection_param_provided = true;
				host_param_provided = true;
				break;
			case 'p':
				repmgr_atoi(optarg, "-p/--port", &cli_errors, false);
				param_set("port", optarg);
				strncpy(runtime_options.masterport,
						optarg,
						MAXLEN);
				connection_param_provided = true;
				break;
			case 'U':
				strncpy(runtime_options.username, optarg, MAXLEN);
				param_set("user", optarg);
				connection_param_provided = true;
				break;
			case 'S':
				strncpy(runtime_options.superuser, optarg, MAXLEN);
				break;
			case 'D':
				strncpy(runtime_options.dest_dir, optarg, MAXPGPATH);
				break;
			case 'f':
				strncpy(runtime_options.config_file, optarg, MAXLEN);
				break;
			case 'R':
				strncpy(runtime_options.remote_user, optarg, MAXLEN);
				break;
			case 'w':
				repmgr_atoi(optarg, "-w/--wal-keep-segments", &cli_errors, false);
				strncpy(runtime_options.wal_keep_segments,
						optarg,
						MAXLEN);
				wal_keep_segments_used = true;
				break;
			case 'k':
				runtime_options.keep_history = repmgr_atoi(optarg, "-k/--keep-history", &cli_errors, false);
				break;
			case 'F':
				runtime_options.force = true;
				break;
			case 'W':
				runtime_options.wait_for_master = true;
				break;
			case 'I':
				runtime_options.ignore_rsync_warn = true;
				break;
			case 'v':
				runtime_options.verbose = true;
				break;
			case 'b':
				strncpy(runtime_options.pg_bindir, optarg, MAXLEN);
				break;
			case 'r':
				runtime_options.rsync_only = true;
				break;
			case 'c':
				runtime_options.fast_checkpoint = true;
				break;
			case 'L':
			{
				int detected_log_level = detect_log_level(optarg);
				if (detected_log_level != -1)
				{
					strncpy(runtime_options.loglevel, optarg, MAXLEN);
				}
				else
				{
					PQExpBufferData invalid_log_level;
					initPQExpBuffer(&invalid_log_level);
					appendPQExpBuffer(&invalid_log_level, _("Invalid log level \"%s\" provided"), optarg);
					item_list_append(&cli_errors, invalid_log_level.data);
					termPQExpBuffer(&invalid_log_level);
				}
				break;
			}
			case 't':
				runtime_options.terse = true;
				break;
			case 'm':
			{
				if (strcmp(optarg, "smart") == 0 ||
					strcmp(optarg, "fast") == 0 ||
					strcmp(optarg, "immediate") == 0
					)
				{
					strncpy(runtime_options.pg_ctl_mode, optarg, MAXLEN);
				}
				else
				{
					PQExpBufferData invalid_mode;
					initPQExpBuffer(&invalid_mode);
					appendPQExpBuffer(&invalid_mode, _("Invalid pg_ctl shutdown mode \"%s\" provided"), optarg);
					item_list_append(&cli_errors, invalid_mode.data);
					termPQExpBuffer(&invalid_mode);
				}
			}
			break;
			case 'C':
				strncpy(runtime_options.remote_config_file, optarg, MAXLEN);
				break;
			case OPT_CHECK_UPSTREAM_CONFIG:
				check_upstream_config = true;
				break;
			case OPT_RECOVERY_MIN_APPLY_DELAY:
				targ = strtol(optarg, &ptr, 10);

				if (targ < 1)
				{
					item_list_append(&cli_errors, _("Invalid value provided for '--recovery-min-apply-delay'"));
					break;
				}
				if (ptr && *ptr)
				{
					if (strcmp(ptr, "ms") != 0 && strcmp(ptr, "s") != 0 &&
					   strcmp(ptr, "min") != 0 && strcmp(ptr, "h") != 0 &&
					   strcmp(ptr, "d") != 0)
					{
						item_list_append(&cli_errors, _("Value provided for '--recovery-min-apply-delay' must be one of ms/s/min/h/d"));
						break;
					}
				}

				strncpy(runtime_options.recovery_min_apply_delay, optarg, MAXLEN);
				break;
			case OPT_IGNORE_EXTERNAL_CONFIG_FILES:
				runtime_options.ignore_external_config_files = true;
				break;
			case OPT_CONFIG_ARCHIVE_DIR:
				strncpy(runtime_options.config_archive_dir, optarg, MAXLEN);
				break;
			case OPT_PG_REWIND:
				if (optarg != NULL)
				{
					strncpy(runtime_options.pg_rewind, optarg, MAXPGPATH);
				}
				pg_rewind_supplied = true;
				break;
			case OPT_PWPROMPT:
				runtime_options.witness_pwprompt = true;
				break;
			case OPT_CSV:
				runtime_options.csv_mode = true;
				break;
			case OPT_NODE:
				runtime_options.node = repmgr_atoi(optarg, "--node", &cli_errors, false);
				break;

			default:
		unknown_option:
			{
				PQExpBufferData unknown_option;
				initPQExpBuffer(&unknown_option);
				appendPQExpBuffer(&unknown_option, _("Unknown option '%s'"), argv[optind - 1]);

				item_list_append(&cli_errors, unknown_option.data);
			}
		}
	}

	/*
	 * If -d/--dbname appears to be a conninfo string, validate by attempting
	 * to parse it (and if successful, store the parsed parameters)
	 */
	if (runtime_options.dbname)
	{
		if(strncmp(runtime_options.dbname, "postgresql://", 13) == 0 ||
		   strncmp(runtime_options.dbname, "postgres://", 11) == 0 ||
		   strchr(runtime_options.dbname, '=') != NULL)
		{
			char	   *errmsg = NULL;

			conninfo_provided = true;

			opts = PQconninfoParse(runtime_options.dbname, &errmsg);

			if (opts == NULL)
			{
				PQExpBufferData conninfo_error;
				initPQExpBuffer(&conninfo_error);
				appendPQExpBuffer(&conninfo_error, _("error parsing conninfo:\n%s"), errmsg);
				item_list_append(&cli_errors, conninfo_error.data);

				termPQExpBuffer(&conninfo_error);
				free(errmsg);
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
						param_set(opt->keyword, opt->val);
					}

					if (strcmp(opt->keyword, "host") == 0 &&
						(opt->val != NULL && opt->val[0] != '\0'))
					{
						strncpy(runtime_options.host, opt->val, MAXLEN);
						host_param_provided = true;
					}
					if (strcmp(opt->keyword, "hostaddr") == 0 &&
						(opt->val != NULL && opt->val[0] != '\0'))
					{
						strncpy(runtime_options.host, opt->val, MAXLEN);
						host_param_provided = true;
					}
					else if (strcmp(opt->keyword, "port") == 0 &&
							 (opt->val != NULL && opt->val[0] != '\0'))
					{
						strncpy(runtime_options.masterport, opt->val, MAXLEN);
					}
					else if (strcmp(opt->keyword, "user") == 0 &&
							 (opt->val != NULL && opt->val[0] != '\0'))
					{
						strncpy(runtime_options.username, opt->val, MAXLEN);
					}
				}
			}
		}
		else
		{
			param_set("dbname", runtime_options.dbname);
		}
	}


	/* Exit here already if errors in command line options found */
	if (cli_errors.head != NULL)
	{
		exit_with_errors();
	}

	if (check_upstream_config == true)
	{
		do_check_upstream_config();
		exit(SUCCESS);
	}

	/*
	 * Now we need to obtain the action, this comes in one of these forms:
	 *   { MASTER | PRIMARY } REGISTER |
	 *   STANDBY {REGISTER | UNREGISTER | CLONE [node] | PROMOTE | FOLLOW [node] | SWITCHOVER | REWIND} |
	 *   WITNESS { CREATE | REGISTER | UNREGISTER } |
	 *   CLUSTER {SHOW | CLEANUP}
	 *
	 * the node part is optional, if we receive it then we shouldn't have
	 * received a -h option
	 */
	if (optind < argc)
	{
		server_mode = argv[optind++];
		if (strcasecmp(server_mode, "STANDBY") != 0 &&
			strcasecmp(server_mode, "MASTER") != 0 &&
			/* allow PRIMARY as synonym for MASTER */
			strcasecmp(server_mode, "PRIMARY") != 0 &&
			strcasecmp(server_mode, "WITNESS") != 0 &&
			strcasecmp(server_mode, "CLUSTER") != 0)
		{
			PQExpBufferData unknown_mode;
			initPQExpBuffer(&unknown_mode);
			appendPQExpBuffer(&unknown_mode, _("Unknown server mode '%s'"), server_mode);
			item_list_append(&cli_errors, unknown_mode.data);
		}
	}

	if (optind < argc)
	{
		server_cmd = argv[optind++];
		/* check posibilities for all server modes */
		if (strcasecmp(server_mode, "MASTER") == 0 || strcasecmp(server_mode, "PRIMARY") == 0 )
		{
			if (strcasecmp(server_cmd, "REGISTER") == 0)
				action = MASTER_REGISTER;
		}
		else if (strcasecmp(server_mode, "STANDBY") == 0)
		{
			if (strcasecmp(server_cmd, "REGISTER") == 0)
				action = STANDBY_REGISTER;
			if (strcasecmp(server_cmd, "UNREGISTER") == 0)
				action = STANDBY_UNREGISTER;
			else if (strcasecmp(server_cmd, "CLONE") == 0)
				action = STANDBY_CLONE;
			else if (strcasecmp(server_cmd, "PROMOTE") == 0)
				action = STANDBY_PROMOTE;
			else if (strcasecmp(server_cmd, "FOLLOW") == 0)
				action = STANDBY_FOLLOW;
			else if (strcasecmp(server_cmd, "SWITCHOVER") == 0)
				action = STANDBY_SWITCHOVER;
			else if (strcasecmp(server_cmd, "ARCHIVE-CONFIG") == 0)
				action = STANDBY_ARCHIVE_CONFIG;
			else if (strcasecmp(server_cmd, "RESTORE-CONFIG") == 0)
				action = STANDBY_RESTORE_CONFIG;
		}
		else if (strcasecmp(server_mode, "CLUSTER") == 0)
		{
			if (strcasecmp(server_cmd, "SHOW") == 0)
				action = CLUSTER_SHOW;
			else if (strcasecmp(server_cmd, "CLEANUP") == 0)
				action = CLUSTER_CLEANUP;
		}
		else if (strcasecmp(server_mode, "WITNESS") == 0)
		{
			if (strcasecmp(server_cmd, "CREATE") == 0)
				action = WITNESS_CREATE;
			else if (strcasecmp(server_cmd, "REGISTER") == 0)
				action = WITNESS_REGISTER;
			else if (strcasecmp(server_cmd, "UNREGISTER") == 0)
				action = WITNESS_UNREGISTER;
		}
	}

	if (action == NO_ACTION) {
		if (server_cmd == NULL)
		{
			item_list_append(&cli_errors, "No server command provided");
		}
		else
		{
			PQExpBufferData unknown_action;
			initPQExpBuffer(&unknown_action);
			appendPQExpBuffer(&unknown_action, _("Unknown server command '%s'"), server_cmd);
			item_list_append(&cli_errors, unknown_action.data);
		}
	}

	/* STANDBY CLONE historically accepts the upstream hostname as an additional argument */
	if (action == STANDBY_CLONE)
	{
		if (optind < argc)
		{
			if (runtime_options.host[0])
			{
				PQExpBufferData additional_host_arg;
				initPQExpBuffer(&additional_host_arg);
				appendPQExpBuffer(&additional_host_arg,
								  _("Conflicting parameters:  you can't use %s while providing a node separately."),
								  conninfo_provided == true ? "host=" : "-h/--host");
				item_list_append(&cli_errors, additional_host_arg.data);
			}
			else
			{
				strncpy(runtime_options.host, argv[optind++], MAXLEN);
				param_set("host", runtime_options.host);
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

	check_parameters_for_action(action);

	/*
	 * Sanity checks for command line parameters completed by now;
	 * any further errors will be runtime ones
	 */
	if (cli_errors.head != NULL)
	{
		exit_with_errors();
	}

	if (cli_warnings.head != NULL && runtime_options.terse == false)
	{
		print_error_list(&cli_warnings, LOG_WARNING);
	}

	/*
	 * The configuration file is not required for some actions (e.g. 'standby clone'),
	 * however if available we'll parse it anyway for options like 'log_level',
	 * 'use_replication_slots' etc.
	 */
	config_file_parsed = load_config(runtime_options.config_file,
									 runtime_options.verbose,
									 &options,
									 argv[0]);

	/* Some configuration file items can be overriden by command line options */
	/* Command-line parameter -L/--log-level overrides any setting in config file*/
	if (*runtime_options.loglevel != '\0')
	{
		strncpy(options.loglevel, runtime_options.loglevel, MAXLEN);
	}

	/*
	 * Initialise pg_bindir - command line parameter will override
	 * any setting in the configuration file
	 */
	if (!strlen(runtime_options.pg_bindir))
	{
		strncpy(runtime_options.pg_bindir, options.pg_bindir, MAXLEN);
	}

	/* Add trailing slash */
	if (strlen(runtime_options.pg_bindir))
	{
		int len = strlen(runtime_options.pg_bindir);
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
	 * Initialize the logger.  If verbose command line parameter was input,
	 * make sure that the log level is at least INFO.  This is mainly useful
	 * for STANDBY CLONE.  That doesn't require a configuration file where a
	 * logging level might be specified at, but it often requires detailed
	 * logging to troubleshoot problems.
	 */

	logger_init(&options, progname());

	if (runtime_options.verbose)
		logger_set_verbose();

	if (runtime_options.terse)
		logger_set_terse();


	/*
	 * Node configuration information is not needed for all actions, with
	 * STANDBY CLONE being the main exception.
	 */
	if (config_file_required)
	{
		if (options.node == NODE_NOT_FOUND)
		{
			if (config_file_parsed == true)
			{
				log_err(_("No node information was found. "
						  "Check the configuration file.\n"));
			}
			else
			{
				log_err(_("No node information was found. "
						  "Please supply a configuration file.\n"));
			}
			exit(ERR_BAD_CONFIG);
		}
	}


	/*
	 * If `use_replication_slots` set in the configuration file
	 * and command line parameter `--wal-keep-segments` was used,
	 * emit a warning as to the latter's redundancy. Note that
	 * the version check for 9.4 or later is done in check_upstream_config()
	 */

	if (options.use_replication_slots && wal_keep_segments_used)
	{
		log_warning(_("-w/--wal-keep-segments has no effect when replication slots in use\n"));
	}

	/* Initialise the repmgr schema name */
	maxlen_snprintf(repmgr_schema, "%s%s", DEFAULT_REPMGR_SCHEMA_PREFIX,
			 options.cluster_name);

	/*
	 * Initialise slot name, if required (9.4 and later)
	 *
	 * NOTE: the slot name will be defined for each record, including
	 * the master; the `slot_name` column in `repl_nodes` defines
	 * the name of the slot, but does not imply a slot has been created.
	 * The version check for 9.4 or later  is done in check_upstream_config()
	 */
	if (options.use_replication_slots)
	{
		maxlen_snprintf(repmgr_slot_name, "repmgr_slot_%i", options.node);
		repmgr_slot_name_ptr = repmgr_slot_name;
		log_verbose(LOG_DEBUG, "slot name initialised as: %s\n", repmgr_slot_name);
	}

	switch (action)
	{
		case MASTER_REGISTER:
			do_master_register();
			break;
		case STANDBY_REGISTER:
			do_standby_register();
			break;
		case STANDBY_UNREGISTER:
			do_standby_unregister();
			break;
		case STANDBY_CLONE:
			do_standby_clone();
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
		case STANDBY_ARCHIVE_CONFIG:
			do_standby_archive_config();
			break;
		case STANDBY_RESTORE_CONFIG:
			do_standby_restore_config();
			break;
		case WITNESS_CREATE:
			do_witness_create();
			break;
		case WITNESS_REGISTER:
			do_witness_register(NULL);
			break;
		case WITNESS_UNREGISTER:
			do_witness_unregister();
			break;
		case CLUSTER_SHOW:
			do_cluster_show();
			break;
		case CLUSTER_CLEANUP:
			do_cluster_cleanup();
			break;
		default:
			/* An action will have been determined by this point  */
			break;
	}


	logger_shutdown();

	return 0;
}

static void
do_cluster_show(void)
{
	PGconn	   *conn;
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];
	char		node_role[MAXLEN];
	int			i;
	char		name_header[MAXLEN];
	char		upstream_header[MAXLEN];
	int			name_length,
				upstream_length,
				conninfo_length = 0;

	/* Connect to local database to obtain cluster connection data */
	log_info(_("connecting to database\n"));
	conn = establish_db_connection(options.conninfo, true);

	sqlquery_snprintf(sqlquery,
					  "SELECT conninfo, type, name, upstream_node_name, id"
					  "  FROM %s.repl_show_nodes",
					  get_repmgr_schema_quoted(conn));

	log_verbose(LOG_DEBUG, "do_cluster_show(): \n%s\n",sqlquery );

	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Unable to retrieve node information from the database\n%s\n"),
				PQerrorMessage(conn));
		log_hint(_("Please check that all nodes have been registered\n"));

		PQclear(res);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}
	PQfinish(conn);

	/* Format header nicely */

	strncpy(name_header, _("Name"), MAXLEN);
	strncpy(upstream_header, _("Upstream"), MAXLEN);

	/*
	 * XXX if repmgr is ever localized into non-ASCII locales,
	 * use pg_wcssize() or similar to establish printed column length
	 */
	name_length = strlen(name_header);
	upstream_length = strlen(upstream_header);

	for (i = 0; i < PQntuples(res); i++)
	{
		int conninfo_length_cur, name_length_cur, upstream_length_cur;

		conninfo_length_cur = strlen(PQgetvalue(res, i, 0));
		if (conninfo_length_cur > conninfo_length)
			conninfo_length = conninfo_length_cur;

		name_length_cur	= strlen(PQgetvalue(res, i, 2));
		if (name_length_cur > name_length)
			name_length = name_length_cur;

		upstream_length_cur = strlen(PQgetvalue(res, i, 3));
		if (upstream_length_cur > upstream_length)
			upstream_length = upstream_length_cur;
	}

	if (! runtime_options.csv_mode)
	{
		printf("Role      | %-*s | %-*s | Connection String\n", name_length, name_header, upstream_length, upstream_header);
		printf("----------+-");

		for (i = 0; i < name_length; i++)
			printf("-");

		printf("-|-");
		for (i = 0; i < upstream_length; i++)
			printf("-");

		printf("-|-");
		for (i = 0; i < conninfo_length; i++)
			printf("-");

		printf("\n");
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		conn = establish_db_connection_quiet(PQgetvalue(res, i, 0));

		if (PQstatus(conn) != CONNECTION_OK)
			strcpy(node_role, "  FAILED");
		else if (strcmp(PQgetvalue(res, i, 1), "witness") == 0)
			strcpy(node_role, "  witness");
		else if (is_standby(conn))
			strcpy(node_role, "  standby");
		else
			strcpy(node_role, "* master");

		if (runtime_options.csv_mode)
		{
			int connection_status =
				(PQstatus(conn) == CONNECTION_OK) ?
				(is_standby(conn) ? 1 : 0) : -1;
			printf("%s,%d\n", PQgetvalue(res, i, 4), connection_status);
		}
		else
		{
			printf("%-10s", node_role);
			printf("| %-*s ", name_length, PQgetvalue(res, i, 2));
			printf("| %-*s ", upstream_length, PQgetvalue(res, i, 3));
			printf("| %s\n", PQgetvalue(res, i, 0));
		}
		PQfinish(conn);
	}

	PQclear(res);
}

static void
do_cluster_cleanup(void)
{
	PGconn	   *conn = NULL;
	PGconn	   *master_conn = NULL;
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];
	int			entries_to_delete = 0;

	/* We need to connect to check configuration */
	log_info(_("connecting to database\n"));
	conn = establish_db_connection(options.conninfo, true);

	/* check if there is a master in this cluster */
	log_info(_("connecting to master database\n"));
	master_conn = get_master_connection(conn, options.cluster_name,
										NULL, NULL);
	if (!master_conn)
	{
		log_err(_("cluster cleanup: cannot connect to master\n"));
		PQfinish(conn);
		exit(ERR_DB_CON);
	}
	PQfinish(conn);

	log_debug(_("Number of days of monitoring history to retain: %i\n"), runtime_options.keep_history);

	sqlquery_snprintf(sqlquery,
					  "SELECT COUNT(*) "
					  "  FROM %s.repl_monitor "
					  " WHERE age(now(), last_monitor_time) >= '%d days'::interval ",
					  get_repmgr_schema_quoted(master_conn),
					  runtime_options.keep_history);

	res = PQexec(master_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("cluster cleanup: unable to query number of monitoring records to clean up:\n%s\n"),
				PQerrorMessage(master_conn));
		PQclear(res);
		PQfinish(master_conn);
		exit(ERR_DB_QUERY);
	}

	entries_to_delete = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);

	if (entries_to_delete == 0)
	{
		log_info(_("cluster cleanup: no monitoring records to delete\n"));
		PQfinish(master_conn);
		return;
	}

	log_debug(_("cluster cleanup: at least %i monitoring records to delete\n"), entries_to_delete);

	if (runtime_options.keep_history > 0)
	{
		sqlquery_snprintf(sqlquery,
						  "DELETE FROM %s.repl_monitor "
						  " WHERE age(now(), last_monitor_time) >= '%d days'::interval ",
						  get_repmgr_schema_quoted(master_conn),
						  runtime_options.keep_history);
	}
	else
	{
		sqlquery_snprintf(sqlquery,
						  "TRUNCATE TABLE %s.repl_monitor",
						  get_repmgr_schema_quoted(master_conn));
	}

	res = PQexec(master_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("cluster cleanup: unable to delete monitoring records\n%s\n"),
				PQerrorMessage(master_conn));
		PQclear(res);
		PQfinish(master_conn);
		exit(ERR_DB_QUERY);
	}
	PQclear(res);

	/*
	 * Let's VACUUM the table to avoid autovacuum to be launched in an
	 * unexpected hour
	 */
	sqlquery_snprintf(sqlquery, "VACUUM %s.repl_monitor", get_repmgr_schema_quoted(master_conn));
	res = PQexec(master_conn, sqlquery);

	/* XXX There is any need to check this VACUUM happens without problems? */

	PQclear(res);
	PQfinish(master_conn);

	if (runtime_options.keep_history > 0)
	{
		log_info(_("cluster cleanup: monitoring records older than %i day(s) deleted\n"), runtime_options.keep_history);
	}
	else
	{
		log_info(_("cluster cleanup: all monitoring records deleted\n"));
	}
}


static void
do_master_register(void)
{
	PGconn	   *conn;
	PGconn	   *master_conn;

	bool		schema_exists = false;
	int			ret;

	int		    primary_node_id = UNKNOWN_NODE_ID;

	bool		record_created;

	conn = establish_db_connection(options.conninfo, true);

	/* Verify that master is a supported server version */
	log_info(_("connecting to master database\n"));
	check_server_version(conn, "master", true, NULL);

	/* Check we are a master */
	log_verbose(LOG_INFO, _("connected to master, checking its state\n"));
	ret = is_standby(conn);

	if (ret)
	{
		log_err(_(ret == 1 ? "server is in standby mode and cannot be registered as a master\n" :
				  "connection to node lost!\n"));

		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Create schema and associated database objects, if it does not exist */
	schema_exists = check_cluster_schema(conn);

	if (!schema_exists)
	{
		log_info(_("master register: creating database objects inside the %s schema\n"),
				 get_repmgr_schema());

		begin_transaction(conn);

		if (!create_schema(conn))
		{
			log_err(_("Unable to create repmgr schema - see preceding error message(s); aborting\n"));
			rollback_transaction(conn);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		commit_transaction(conn);
	}

	/* Ensure there isn't any other master already registered */
	master_conn = get_master_connection(conn,
										options.cluster_name, NULL, NULL);

	if (master_conn != NULL && !runtime_options.force)
	{
		PQfinish(master_conn);
		log_err(_("there is a master already in cluster %s\n"),
					options.cluster_name);
		exit(ERR_BAD_CONFIG);
	}

	PQfinish(master_conn);

	begin_transaction(conn);

	/*
	 * Check if a node with a different ID is registered as primary. This shouldn't
	 * happen but could do if an existing master was shut down without being
	 * unregistered.
	*/

	primary_node_id = get_master_node_id(conn, options.cluster_name);
	if (primary_node_id != NODE_NOT_FOUND && primary_node_id != options.node)
	{
		log_err(_("another node with id %i is already registered as master\n"), primary_node_id);
		rollback_transaction(conn);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Delete any existing record for this node if --force set */
	if (runtime_options.force)
	{
		bool node_record_deleted;
		t_node_info node_info = T_NODE_INFO_INITIALIZER;

		begin_transaction(conn);

		if (get_node_record(conn, options.cluster_name, options.node, &node_info))
		{
			log_notice(_("deleting existing master record with id %i\n"), options.node);

			node_record_deleted = delete_node_record(conn,
													 options.node,
													 "master register");
			if (node_record_deleted == false)
			{
				rollback_transaction(conn);
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}
		}
	}

	/* Now register the master */
	record_created = create_node_record(conn,
										"master register",
										options.node,
										"master",
										NO_UPSTREAM_NODE,
										options.cluster_name,
										options.node_name,
										options.conninfo,
										options.priority,
										repmgr_slot_name_ptr,
										true);

	if (record_created == false)
	{
		rollback_transaction(conn);
		PQfinish(conn);
		exit(ERR_DB_QUERY);
	}

	commit_transaction(conn);

	/* Log the event */
	create_event_record(conn,
						&options,
						options.node,
						"master_register",
						true,
						NULL);

	PQfinish(conn);

	log_notice(_("master node correctly registered for cluster %s with id %d (conninfo: %s)\n"),
			   options.cluster_name, options.node, options.conninfo);
	return;
}


static void
do_standby_register(void)
{
	PGconn	   *conn;
	PGconn	   *master_conn;
	int			ret;

	bool		record_created;
	t_node_info node_record = T_NODE_INFO_INITIALIZER;
	int			node_result;

	log_info(_("connecting to standby database\n"));
	conn = establish_db_connection(options.conninfo, true);

	/* Check we are a standby */
	ret = is_standby(conn);
	if (ret == 0 || ret == -1)
	{
		log_err(_(ret == 0 ? "this node should be a standby (%s)\n" :
				"connection to node (%s) lost\n"), options.conninfo);

		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Check if there is a schema for this cluster */
	if (check_cluster_schema(conn) == false)
	{
		/* schema doesn't exist */
		log_err(_("schema '%s' doesn't exist.\n"), get_repmgr_schema());
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* check if there is a master in this cluster */
	log_info(_("connecting to master database\n"));
	master_conn = get_master_connection(conn, options.cluster_name,
										NULL, NULL);
	if (!master_conn)
	{
		log_err(_("a master must be defined before configuring a standby\n"));
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Verify that standby and master are supported and compatible server
	 * versions
	 */
	check_master_standby_version_match(conn, master_conn);

	/* Now register the standby */
	log_info(_("registering the standby\n"));
	if (runtime_options.force)
	{
		bool node_record_deleted = delete_node_record(master_conn,
													  options.node,
													  "standby register");

		if (node_record_deleted == false)
		{
			PQfinish(master_conn);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/*
	 * Check that an active node with the same node_name doesn't exist already
	 */

	node_result = get_node_record_by_name(master_conn,
										  options.cluster_name,
										  options.node_name,
										  &node_record);

	if (node_result)
	{
		if (node_record.active == true)
		{
			log_err(_("Node %i exists already with node_name \"%s\"\n"),
					  node_record.node_id,
					  options.node_name);
			PQfinish(master_conn);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	record_created = create_node_record(master_conn,
										"standby register",
										options.node,
										"standby",
										options.upstream_node,
										options.cluster_name,
										options.node_name,
										options.conninfo,
										options.priority,
										repmgr_slot_name_ptr,
										true);

	if (record_created == false)
	{
		if (!runtime_options.force)
		{
			log_hint(_("use option -F/--force to overwrite an existing node record\n"));
		}

		/* XXX log registration failure? */
		PQfinish(master_conn);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Log the event */
	create_event_record(master_conn,
						&options,
						options.node,
						"standby_register",
						true,
						NULL);

	PQfinish(master_conn);
	PQfinish(conn);

	log_info(_("standby registration complete\n"));
	log_notice(_("standby node correctly registered for cluster %s with id %d (conninfo: %s)\n"),
			   options.cluster_name, options.node, options.conninfo);
	return;
}


static void
do_standby_unregister(void)
{
	PGconn	   *conn;
	PGconn	   *master_conn;

	int 		target_node_id;
	t_node_info node_info = T_NODE_INFO_INITIALIZER;

	bool		node_record_deleted;

	log_info(_("connecting to database\n"));
	conn = establish_db_connection(options.conninfo, true);

	/* Check we are a standby */
/*	ret = is_standby(conn);
	if (ret == 0 || ret == -1)
	{
		log_err(_(ret == 0 ? "this node should be a standby (%s)\n" :
				"connection to node (%s) lost\n"), options.conninfo);

		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
		}*/

	/* Check if there is a schema for this cluster */
	if (check_cluster_schema(conn) == false)
	{
		/* schema doesn't exist */
		log_err(_("schema '%s' doesn't exist.\n"), get_repmgr_schema());
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* check if there is a master in this cluster */
	log_info(_("connecting to master database\n"));
	master_conn = get_master_connection(conn, options.cluster_name,
										NULL, NULL);
	if (!master_conn)
	{
		log_err(_("a master must be defined before unregistering a standby\n"));
		exit(ERR_BAD_CONFIG);
	}

	if (runtime_options.node)
		target_node_id = runtime_options.node;
	else
		target_node_id = options.node;

	/* Check node exists and is really a standby */

	if (!get_node_record(master_conn, options.cluster_name, target_node_id, &node_info))
	{
		log_err(_("No record found for node %i\n"), target_node_id);
		exit(ERR_BAD_CONFIG);
	}

	if (node_info.type != STANDBY)
	{
		log_err(_("Node %i is not a standby server\n"), target_node_id);
		exit(ERR_BAD_CONFIG);
	}

	/* Now unregister the standby */
	log_info(_("unregistering the standby\n"));
	node_record_deleted = delete_node_record(master_conn,
										     target_node_id,
											 "standby unregister");

	if (node_record_deleted == false)
	{
		PQfinish(master_conn);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Log the event */
	create_event_record(master_conn,
						&options,
						target_node_id,
						"standby_unregister",
						true,
						NULL);

	PQfinish(master_conn);
	PQfinish(conn);

	log_info(_("standby unregistration complete\n"));
	log_notice(_("standby node correctly unregistered for cluster %s with id %d (conninfo: %s)\n"),
			   options.cluster_name, target_node_id, options.conninfo);
	return;
}


static void
do_standby_clone(void)
{
	PGconn	   *primary_conn = NULL;
	PGconn	   *upstream_conn;
	PGresult   *res;

	char		sqlquery[QUERY_STR_LEN];

	int			server_version_num;

	char		cluster_size[MAXLEN];

	int			r = 0,
				retval = SUCCESS;

	int			i;
	bool		pg_start_backup_executed = false;
	bool		target_directory_provided = false;
	bool		external_config_file_copy_required = false;

	char		master_data_directory[MAXPGPATH];
	char		local_data_directory[MAXPGPATH];

	char		master_config_file[MAXPGPATH] = "";
	char		local_config_file[MAXPGPATH] = "";
	bool		config_file_outside_pgdata = false;

	char		master_hba_file[MAXPGPATH] = "";
	char		local_hba_file[MAXPGPATH] = "";
	bool		hba_file_outside_pgdata = false;

	char		master_ident_file[MAXPGPATH] = "";
	char		local_ident_file[MAXPGPATH] = "";
	bool		ident_file_outside_pgdata = false;

	char		master_control_file[MAXPGPATH] = "";
	char		local_control_file[MAXPGPATH] = "";

	char	   *first_wal_segment = NULL;
	char	   *last_wal_segment = NULL;

	PQExpBufferData event_details;


	/*
	 * If dest_dir (-D/--pgdata) was provided, this will become the new data
	 * directory (otherwise repmgr will default to the same directory as on the
	 * source host)
	 */
	if (runtime_options.dest_dir[0])
	{
		target_directory_provided = true;
		log_notice(_("destination directory '%s' provided\n"),
				   runtime_options.dest_dir);
	}

	param_set("application_name", options.node_name);

	/* Connect to check configuration */
	log_info(_("connecting to upstream node\n"));
	upstream_conn = establish_db_connection_by_params((const char**)param_keywords, (const char**)param_values, true);

	/* Verify that upstream node is a supported server version */
	log_verbose(LOG_INFO, _("connected to upstream node, checking its state\n"));
	server_version_num = check_server_version(upstream_conn, "master", true, NULL);

	check_upstream_config(upstream_conn, server_version_num, true);

	if (get_cluster_size(upstream_conn, cluster_size) == false)
		exit(ERR_DB_QUERY);

	log_info(_("Successfully connected to upstream node. Current installation size is %s\n"),
			 cluster_size);

	/*
	 * If the upstream node is a standby, try to connect to the primary too so we
	 * can write an event record
	 */
	if (is_standby(upstream_conn))
	{
		if (strlen(options.cluster_name))
		{
			primary_conn = get_master_connection(upstream_conn, options.cluster_name,
												 NULL, NULL);
		}
	}
	else
	{
		primary_conn = upstream_conn;
	}

	/*
	 * If --recovery-min-apply-delay was passed, check that
	 * we're connected to PostgreSQL 9.4 or later
	 */

	if (*runtime_options.recovery_min_apply_delay)
	{
		if (server_version_num < 90400)
		{
			log_err(_("PostgreSQL 9.4 or greater required for --recovery-min-apply-delay\n"));
			PQfinish(upstream_conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/*
	 * Check that tablespaces named in any `tablespace_mapping` configuration
	 * file parameters exist.
	 *
	 * pg_basebackup doesn't verify mappings, so any errors will not be caught.
	 * We'll do that here as a value-added service.
	 *
	 * -T/--tablespace-mapping is not available as a pg_basebackup option for
	 * PostgreSQL 9.3 - we can only handle that with rsync, so if `--rsync-only`
	 * not set, fail with an error
	 */

	if (options.tablespace_mapping.head != NULL)
	{
		TablespaceListCell *cell;

		if (server_version_num < 90400 && !runtime_options.rsync_only)
		{
			log_err(_("in PostgreSQL 9.3, tablespace mapping can only be used in conjunction with --rsync-only\n"));
			PQfinish(upstream_conn);
			exit(ERR_BAD_CONFIG);
		}

		for (cell = options.tablespace_mapping.head; cell; cell = cell->next)
		{
			sqlquery_snprintf(sqlquery,
							  "SELECT spcname "
							  "  FROM pg_tablespace "
							  " WHERE pg_tablespace_location(oid) = '%s'",
							  cell->old_dir);
			res = PQexec(upstream_conn, sqlquery);
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				log_err(_("unable to execute tablespace query: %s\n"), PQerrorMessage(upstream_conn));
				PQclear(res);
				PQfinish(upstream_conn);
				exit(ERR_BAD_CONFIG);
			}

			if (PQntuples(res) == 0)
			{
				log_err(_("no tablespace matching path '%s' found\n"), cell->old_dir);
				PQclear(res);
				PQfinish(upstream_conn);
				exit(ERR_BAD_CONFIG);
			}
		}
	}

	/*
	 * Obtain data directory and configuration file locations
	 * We'll check to see whether the configuration files are in the data
	 * directory - if not we'll have to copy them via SSH
	 *
	 * XXX: if configuration files are symlinks to targets outside the data
	 * directory, they won't be copied by pg_basebackup, but we can't tell
	 * this from the below query; we'll probably need to add a check for their
	 * presence and if missing force copy by SSH
	 */
	sqlquery_snprintf(sqlquery,
					  "  WITH dd AS ( "
					  "    SELECT setting "
					  "      FROM pg_settings "
					  "     WHERE name = 'data_directory' "
					  "  ) "
					  "    SELECT ps.name, ps.setting, "
					  "           ps.setting ~ ('^' || dd.setting) AS in_data_dir "
					  "      FROM dd, pg_settings ps "
					  "     WHERE ps.name IN ('data_directory', 'config_file', 'hba_file', 'ident_file') "
					  "  ORDER BY 1 ");

	log_debug(_("standby clone: %s\n"), sqlquery);
	res = PQexec(upstream_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("can't get info about data directory and configuration files: %s\n"),
				PQerrorMessage(upstream_conn));
		PQclear(res);
		PQfinish(upstream_conn);
		exit(ERR_BAD_CONFIG);
	}

	/* We need all 4 parameters, and they can be retrieved only by superusers */
	if (PQntuples(res) != 4)
	{
		log_err("STANDBY CLONE should be run by a SUPERUSER\n");
		PQclear(res);
		PQfinish(upstream_conn);
		exit(ERR_BAD_CONFIG);
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		if (strcmp(PQgetvalue(res, i, 0), "data_directory") == 0)
		{
			strncpy(master_data_directory, PQgetvalue(res, i, 1), MAXPGPATH);
		}
		else if (strcmp(PQgetvalue(res, i, 0), "config_file") == 0)
		{
			if (strcmp(PQgetvalue(res, i, 2), "f") == 0)
			{
				config_file_outside_pgdata = true;
				external_config_file_copy_required = true;
				strncpy(master_config_file, PQgetvalue(res, i, 1), MAXPGPATH);
			}
		}
		else if (strcmp(PQgetvalue(res, i, 0), "hba_file") == 0)
		{
			if (strcmp(PQgetvalue(res, i, 2), "f") == 0)
			{
				hba_file_outside_pgdata  = true;
				external_config_file_copy_required = true;
				strncpy(master_hba_file, PQgetvalue(res, i, 1), MAXPGPATH);
			}
		}
		else if (strcmp(PQgetvalue(res, i, 0), "ident_file") == 0)
		{
			if (strcmp(PQgetvalue(res, i, 2), "f") == 0)
			{
				ident_file_outside_pgdata = true;
				external_config_file_copy_required = true;
				strncpy(master_ident_file, PQgetvalue(res, i, 1), MAXPGPATH);
			}
		}
		else
			log_warning(_("unknown parameter: %s\n"), PQgetvalue(res, i, 0));
	}

	PQclear(res);

	/*
	 * target directory (-D/--pgdata) provided - use that as new data directory
	 * (useful when executing backup on local machine only or creating the backup
	 * in a different local directory when backup source is a remote host)
	 */
	if (target_directory_provided)
	{
		strncpy(local_data_directory, runtime_options.dest_dir, MAXPGPATH);
		strncpy(local_config_file, runtime_options.dest_dir, MAXPGPATH);
		strncpy(local_hba_file, runtime_options.dest_dir, MAXPGPATH);
		strncpy(local_ident_file, runtime_options.dest_dir, MAXPGPATH);
	}
	/*
	 * Otherwise use the same data directory as on the remote host
	 */
	else
	{
		strncpy(local_data_directory, master_data_directory, MAXPGPATH);
		strncpy(local_config_file, master_config_file, MAXPGPATH);
		strncpy(local_hba_file, master_hba_file, MAXPGPATH);
		strncpy(local_ident_file, master_ident_file, MAXPGPATH);

		log_notice(_("setting data directory to: %s\n"), local_data_directory);
		log_hint(_("use -D/--data-dir to explicitly specify a data directory\n"));
	}

	/*
	 * When using rsync only, we need to check the SSH connection early
	 */
	if (runtime_options.rsync_only)
	{
		r = test_ssh_connection(runtime_options.host, runtime_options.remote_user);
		if (r != 0)
		{
			log_err(_("aborting, remote host %s is not reachable.\n"),
					runtime_options.host);
			retval = ERR_BAD_SSH;
			goto stop_backup;
		}
	}

	/* Check the local data directory can be used */

	if (!create_pg_dir(local_data_directory, runtime_options.force))
	{
		log_err(_("unable to use directory %s ...\n"),
				local_data_directory);
		log_hint(_("use -F/--force option to force this directory to be overwritten\n"));
		r = ERR_BAD_CONFIG;
		retval = ERR_BAD_CONFIG;
		goto stop_backup;
	}

	/*
	 * If replication slots requested, create appropriate slot on
	 * the primary; this must be done before pg_start_backup() is
	 * issued, either by us or by pg_basebackup.
	 */
	if (options.use_replication_slots)
	{
		if (create_replication_slot(upstream_conn, repmgr_slot_name, server_version_num) == false)
		{
			PQfinish(upstream_conn);
			exit(ERR_DB_QUERY);
		}
	}

	if (runtime_options.rsync_only)
	{
		log_notice(_("starting backup (using rsync)...\n"));
	}
	else
	{
		log_notice(_("starting backup (using pg_basebackup)...\n"));
		if (runtime_options.fast_checkpoint == false)
			log_hint(_("this may take some time; consider using the -c/--fast-checkpoint option\n"));
	}

	if (runtime_options.rsync_only)
	{
		PQExpBufferData tablespace_map;
		bool		tablespace_map_rewrite = false;

		/* For 9.5 and greater, create our own tablespace_map file */
		if (server_version_num >= 90500)
		{
			initPQExpBuffer(&tablespace_map);
		}

		/*
		 * From 9.1 default is to wait for a sync standby to ack, avoid that by
		 * turning off sync rep for this session
		 */
		if (set_config_bool(upstream_conn, "synchronous_commit", false) == false)
		{
			r = ERR_BAD_CONFIG;
			retval = ERR_BAD_CONFIG;
			goto stop_backup;
		}

		if (start_backup(upstream_conn, first_wal_segment, runtime_options.fast_checkpoint) == false)
		{
			r = ERR_BAD_BASEBACKUP;
			retval = ERR_BAD_BASEBACKUP;
			goto stop_backup;
		}

		/*
		 * Note that we've successfully executed pg_start_backup(),
		 * so we know whether or not to execute pg_stop_backup() after
		 * the 'stop_backup' label
		 */
		pg_start_backup_executed = true;

		/*
		 * 1. copy data directory, omitting directories which should not be
		 *    copied, or for which copying would serve no purpose.
		 *
		 * 2. copy pg_control file
		 */


		/* Copy the data directory */
		log_info(_("standby clone: master data directory '%s'\n"),
				 master_data_directory);
		r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
							  master_data_directory, local_data_directory,
							  true, server_version_num);
		/*
		  Exit code 0 means no error, but we want to ignore exit code 24 as well
		  as rsync returns that code on "Partial transfer due to vanished source files".
		  It's quite common for this to happen on the data directory, particularly
		  with long running rsync on a busy server.
		*/
		if (!WIFEXITED(r) && WEXITSTATUS(r) != 24)
		{
			log_warning(_("standby clone: failed copying master data directory '%s'\n"),
						master_data_directory);
			goto stop_backup;
		}

		/* Read backup label copied from primary */
		if (read_backup_label(local_data_directory, &backup_label) == false)
		{
			r = retval = ERR_BAD_BACKUP_LABEL;
			goto stop_backup;
		}

		/* Copy tablespaces and, if required, remap to a new location */

		sqlquery_snprintf(sqlquery,
						  " SELECT oid, pg_tablespace_location(oid) AS spclocation "
						  "   FROM pg_tablespace "
						  "  WHERE spcname NOT IN ('pg_default', 'pg_global')");

		res = PQexec(upstream_conn, sqlquery);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			log_err(_("unable to execute tablespace query: %s\n"),
					PQerrorMessage(upstream_conn));

			PQclear(res);

			r = retval = ERR_DB_QUERY;
			goto stop_backup;
		}

		for (i = 0; i < PQntuples(res); i++)
		{
			bool mapping_found = false;
			PQExpBufferData tblspc_dir_src;
			PQExpBufferData tblspc_dir_dst;
			PQExpBufferData tblspc_oid;
			TablespaceListCell *cell;

			initPQExpBuffer(&tblspc_dir_src);
			initPQExpBuffer(&tblspc_dir_dst);
			initPQExpBuffer(&tblspc_oid);


			appendPQExpBuffer(&tblspc_oid, "%s", PQgetvalue(res, i, 0));
			appendPQExpBuffer(&tblspc_dir_src, "%s", PQgetvalue(res, i, 1));

			/* Check if tablespace path matches one of the provided tablespace mappings */
			if (options.tablespace_mapping.head != NULL)
			{
				for (cell = options.tablespace_mapping.head; cell; cell = cell->next)
				{
					if (strcmp(tblspc_dir_src.data, cell->old_dir) == 0)
					{
						mapping_found = true;
						break;
					}
				}
			}

			if (mapping_found == true)
			{
				appendPQExpBuffer(&tblspc_dir_dst, "%s", cell->new_dir);
				log_debug(_("mapping source tablespace '%s' (OID %s) to '%s'\n"),
						  tblspc_dir_src.data, tblspc_oid.data, tblspc_dir_dst.data);
			}
			else
			{
				appendPQExpBuffer(&tblspc_dir_dst, "%s",  tblspc_dir_src.data);
			}


			/* Copy tablespace directory */
			r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
								  tblspc_dir_src.data, tblspc_dir_dst.data,
								  true, server_version_num);

			/*
			  Exit code 0 means no error, but we want to ignore exit code 24 as well
			  as rsync returns that code on "Partial transfer due to vanished source files".
			  It's quite common for this to happen on the data directory, particularly
			  with long running rsync on a busy server.
			*/
			if (!WIFEXITED(r) && WEXITSTATUS(r) != 24)
			{
			       log_warning(_("standby clone: failed copying tablespace directory '%s'\n"),
					            tblspc_dir_src.data);
			       goto stop_backup;
			}

			/*
			 * If a valid mapping was provide for this tablespace, arrange for it to
			 * be remapped
			 * (if no tablespace mappings was provided, the link will be copied as-is
			 * by pg_basebackup or rsync and no action is required)
			 */
			if (mapping_found == true)
			{
				/* 9.5 and later - append to the tablespace_map file */
				if (server_version_num >= 90500)
				{
					tablespace_map_rewrite = true;
					appendPQExpBuffer(&tablespace_map,
									  "%s %s\n",
									  tblspc_oid.data,
									  tblspc_dir_dst.data);
				}
				/* Pre-9.5, we have to manipulate the symlinks in pg_tblspc/ ourselves */
				else
				{
					PQExpBufferData tblspc_symlink;

					initPQExpBuffer(&tblspc_symlink);
					appendPQExpBuffer(&tblspc_symlink, "%s/pg_tblspc/%s",
									  local_data_directory,
									  tblspc_oid.data);

					if (unlink(tblspc_symlink.data) < 0 && errno != ENOENT)
					{
						log_err(_("unable to remove tablespace symlink %s\n"), tblspc_symlink.data);

						PQclear(res);

						r = retval = ERR_BAD_BASEBACKUP;
						goto stop_backup;
					}

					if (symlink(tblspc_dir_dst.data, tblspc_symlink.data) < 0)
					{
						log_err(_("unable to create tablespace symlink from %s to %s\n"), tblspc_symlink.data, tblspc_dir_dst.data);

						PQclear(res);

						r = retval = ERR_BAD_BASEBACKUP;
						goto stop_backup;
					}
				}
			}
		}

		PQclear(res);

		/*
		 * For 9.5 and later, if tablespace remapping was requested, we'll need
		 * to rewrite the tablespace map file ourselves.
		 * The tablespace map file is read on startup and any links created by
		 * the backend; we could do this ourselves like for pre-9.5 servers, but
		 * it's better to rely on functionality the backend provides.
		 */
		if (server_version_num >= 90500 && tablespace_map_rewrite == true)
		{
			PQExpBufferData tablespace_map_filename;
			FILE	   *tablespace_map_file;
			initPQExpBuffer(&tablespace_map_filename);
			appendPQExpBuffer(&tablespace_map_filename, "%s/%s",
							  local_data_directory,
							  TABLESPACE_MAP);

			/* Unlink any existing file (it should be there, but we don't care if it isn't) */
			if (unlink(tablespace_map_filename.data) < 0 && errno != ENOENT)
			{
				log_err(_("unable to remove tablespace_map file %s: %s\n"),
						tablespace_map_filename.data,
						strerror(errno));

				r = retval = ERR_BAD_BASEBACKUP;
				goto stop_backup;
			}

			tablespace_map_file = fopen(tablespace_map_filename.data, "w");
			if (tablespace_map_file == NULL)
			{
				log_err(_("unable to create tablespace_map file '%s'\n"), tablespace_map_filename.data);

				r = retval = ERR_BAD_BASEBACKUP;
				goto stop_backup;
			}

			if (fputs(tablespace_map.data, tablespace_map_file) == EOF)
			{
				log_err(_("unable to write to tablespace_map file '%s'\n"), tablespace_map_filename.data);

				r = retval = ERR_BAD_BASEBACKUP;
				goto stop_backup;
			}

			fclose(tablespace_map_file);
		}
	}
	else
	{
		r = run_basebackup(local_data_directory, server_version_num);
		if (r != 0)
		{
			log_warning(_("standby clone: base backup failed\n"));

			retval = ERR_BAD_BASEBACKUP;
			goto stop_backup;
		}
	}

	/*
	 * If configuration files were not inside the data directory, we'll need to
	 * copy them via SSH (unless `--ignore-external-config-files` was provided)
	 *
	 * TODO: add option to place these files in the same location on the
	 * standby server as on the primary?
	 */

	if (external_config_file_copy_required && !runtime_options.ignore_external_config_files)
	{
		log_notice(_("copying configuration files from master\n"));
		r = test_ssh_connection(runtime_options.host, runtime_options.remote_user);
		if (r != 0)
		{
			log_err(_("aborting, remote host %s is not reachable.\n"),
					runtime_options.host);
			retval = ERR_BAD_SSH;
			goto stop_backup;
		}

		if (config_file_outside_pgdata)
		{
			log_info(_("standby clone: master config file '%s'\n"), master_config_file);
			r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
								  master_config_file, local_config_file, false, server_version_num);
			if (r != 0)
			{
				log_err(_("standby clone: failed copying master config file '%s'\n"),
						master_config_file);
				retval = ERR_BAD_SSH;
				goto stop_backup;
			}
		}

		if (hba_file_outside_pgdata)
		{
			log_info(_("standby clone: master hba file '%s'\n"), master_hba_file);
			r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
								  master_hba_file, local_hba_file, false, server_version_num);
			if (r != 0)
			{
				log_err(_("standby clone: failed copying master hba file '%s'\n"),
						master_hba_file);
				retval = ERR_BAD_SSH;
				goto stop_backup;
			}
		}

		if (ident_file_outside_pgdata)
		{
			log_info(_("standby clone: master ident file '%s'\n"), master_ident_file);
			r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
								  master_ident_file, local_ident_file, false, server_version_num);
			if (r != 0)
			{
				log_err(_("standby clone: failed copying master ident file '%s'\n"),
						master_ident_file);
				retval = ERR_BAD_SSH;
				goto stop_backup;
			}
		}
	}

	/*
	 * When using rsync, copy pg_control file last, emulating the base backup
	 * protocol.
	 */
	if (runtime_options.rsync_only)
	{
		maxlen_snprintf(local_control_file, "%s/global", local_data_directory);

		log_info(_("standby clone: local control file '%s'\n"),
				 local_control_file);

		if (!create_dir(local_control_file))
		{
			log_err(_("couldn't create directory %s ...\n"),
					local_control_file);
			goto stop_backup;
		}

		maxlen_snprintf(master_control_file, "%s/global/pg_control",
						master_data_directory);
		log_info(_("standby clone: master control file '%s'\n"),
				 master_control_file);
		r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
							  master_control_file, local_control_file,
							  false, server_version_num);
		if (r != 0)
		{
			log_warning(_("standby clone: failed copying master control file '%s'\n"),
						master_control_file);
			retval = ERR_BAD_SSH;
			goto stop_backup;
		}
	}

stop_backup:

	if (runtime_options.rsync_only && pg_start_backup_executed)
	{
		log_notice(_("notifying master about backup completion...\n"));
		if (stop_backup(upstream_conn, last_wal_segment) == false)
		{
			r = ERR_BAD_BASEBACKUP;
			retval = ERR_BAD_BASEBACKUP;
		}
	}

	/* If the backup failed then exit */
	if (r != 0)
	{
		/* If a replication slot was previously created, drop it */
		if (options.use_replication_slots)
		{
			drop_replication_slot(upstream_conn, repmgr_slot_name);
		}

		log_err(_("unable to take a base backup of the master server\n"));
		log_warning(_("destination directory (%s) may need to be cleaned up manually\n"),
				local_data_directory);

		PQfinish(upstream_conn);
		exit(retval);
	}

	/*
	 * Clean up any $PGDATA subdirectories which may contain
	 * files which won't be removed by rsync and which could
	 * be stale or are otherwise not required
	 */
	if (runtime_options.rsync_only)
	{
		char	label_path[MAXPGPATH];
		char	dirpath[MAXLEN] = "";

		if (runtime_options.force)
		{
			/*
			 * Remove any existing WAL from the target directory, since
			 * rsync's --exclude option doesn't do it.
			 */

			maxlen_snprintf(dirpath, "%s/pg_xlog/", local_data_directory);

			if (!rmtree(dirpath, false))
			{
				log_err(_("unable to empty local WAL directory %s\n"),
						dirpath);
				exit(ERR_BAD_RSYNC);
			}
		}

		/*
		 * Remove any existing replication slot directories from previous use
		 * of this data directory; this matches the behaviour of a fresh
		 * pg_basebackup, which would usually result in an empty pg_replslot
		 * directory.
		 *
		 * If the backup label contains a nonzero
		 * 'MIN FAILOVER SLOT LSN' entry we retain the slots and let
		 * the server clean them up instead, matching pg_basebackup's
		 * behaviour when failover slots are enabled.
		 *
		 * NOTE: watch out for any changes in the replication
		 * slot directory name (as of 9.4: "pg_replslot") and
		 * functionality of replication slots
		 */
		if (server_version_num >= 90400 &&
			backup_label.min_failover_slot_lsn == InvalidXLogRecPtr)
		{
			maxlen_snprintf(dirpath, "%s/pg_replslot/",
							local_data_directory);

			log_debug("deleting pg_replslot directory contents\n");

			if (!rmtree(dirpath, false))
			{
				log_err(_("unable to empty replication slot directory %s\n"),
						dirpath);
				exit(ERR_BAD_RSYNC);
			}
		}

		/* delete the backup label file copied from the primary */
		maxlen_snprintf(label_path, "%s/backup_label", local_data_directory);
		if (0 && unlink(label_path) < 0 && errno != ENOENT)
		{
			log_warning(_("unable to delete backup label file %s\n"), label_path);
		}
	}

	/* Finally, write the recovery.conf file */
	create_recovery_file(local_data_directory, upstream_conn);

	if (runtime_options.rsync_only)
	{
		log_notice(_("standby clone (using rsync) complete\n"));
	}
	else
	{
		log_notice(_("standby clone (using pg_basebackup) complete\n"));
	}

	/*
	 * XXX It might be nice to provide an options to have repmgr start
	 * the PostgreSQL server automatically (e.g. with a custom pg_ctl
	 * command)
	 */

	log_notice(_("you can now start your PostgreSQL server\n"));
	if (target_directory_provided)
	{
		log_hint(_("for example : pg_ctl -D %s start\n"),
				   local_data_directory);
	}
	else
	{
		log_hint(_("for example : /etc/init.d/postgresql start\n"));
	}


	/*
	 * XXX forgetting to (re) register the standby is a frequent cause
	 * of error; we should consider having repmgr automatically
	 * register the standby, either by default with an option
	 * "--no-register", or an option "--register".
	 *
	 * Note that "repmgr standby register" requires the standby to
	 * be running - if not, and we just update the node record,
	 * we'd have an incorrect representation of the replication cluster.
	 * Best combined with an automatic start of the server (see note
	 * above)
	 */

	/*
	 * XXX detect whether a record exists for this node already, and
	 * add a hint about using the -F/--force.
	 */

	log_hint(_("After starting the server, you need to register this standby with \"repmgr standby register\"\n"));

	/* Log the event - if we can connect to the primary */

	if (primary_conn != NULL)
	{
		initPQExpBuffer(&event_details);

		/* Add details about relevant runtime options used */
		appendPQExpBuffer(&event_details,
						  _("Cloned from host '%s', port %s"),
						  runtime_options.host,
						  runtime_options.masterport);

		appendPQExpBuffer(&event_details,
						  _("; backup method: %s"),
						  runtime_options.rsync_only ? "rsync" : "pg_basebackup");

		appendPQExpBuffer(&event_details,
						  _("; --force: %s"),
						  runtime_options.force ? "Y" : "N");

		create_event_record(primary_conn,
							&options,
							options.node,
							"standby_clone",
							true,
							event_details.data);
	}

	PQfinish(upstream_conn);
	exit(retval);
}

static bool
parse_lsn(XLogRecPtr *ptr, const char *str)
{
	uint32 high, low;

	if (sscanf(str, "%x/%x", &high, &low) != 2)
		return false;

	*ptr = (((XLogRecPtr)high) << 32) + (XLogRecPtr)low;

	return true;
}

static XLogRecPtr
parse_label_lsn(const char *label_key, const char *label_value)
{
	XLogRecPtr ptr;

	if (!parse_lsn(&ptr, label_value))
	{
		log_err(_("Couldn't parse backup label entry \"%s: %s\" as lsn"),
				label_key, label_value);
		return InvalidXLogRecPtr;
	}

	return ptr;
}

/*======================================
 * Read entries of interest from the backup label.
 *
 * Sample backup label (with failover slots):
 *
 *		START WAL LOCATION: 0/6000028 (file 000000010000000000000006)
 *		CHECKPOINT LOCATION: 0/6000060
 *		BACKUP METHOD: streamed
 *		BACKUP FROM: master
 *		START TIME: 2016-03-30 12:18:12 AWST
 *		LABEL: pg_basebackup base backup
 *		MIN FAILOVER SLOT LSN: 0/5000000
 *
 *======================================
 */
static bool
read_backup_label(const char *local_data_directory, struct BackupLabel *out_backup_label)
{
	char label_path[MAXPGPATH];
	FILE *label_file;
	int  nmatches = 0;

	char line[MAXLEN];

	out_backup_label->start_wal_location = InvalidXLogRecPtr;
	out_backup_label->start_wal_file[0] = '\0';
	out_backup_label->checkpoint_location = InvalidXLogRecPtr;
	out_backup_label->backup_from[0] = '\0';
	out_backup_label->backup_method[0] = '\0';
	out_backup_label->start_time[0] = '\0';
	out_backup_label->label[0] = '\0';
	out_backup_label->min_failover_slot_lsn = InvalidXLogRecPtr;

	maxlen_snprintf(label_path, "%s/backup_label", local_data_directory);

	label_file = fopen(label_path, "r");
	if (label_file == NULL)
	{
		log_err(_("read_backup_label: could not open backup label file %s: %s"),
				label_path, strerror(errno));
		return false;
	}

	log_info(_("read_backup_label: parsing backup label file '%s'\n"),
			 label_path);

	while(fgets(line, sizeof line, label_file) != NULL)
	{
		char label_key[MAXLEN];
		char label_value[MAXLEN];
		char newline;

		nmatches = sscanf(line, "%" MAXLEN_STR "[^:]: %" MAXLEN_STR "[^\n]%c",
						  label_key, label_value, &newline);

		if (nmatches != 3)
			break;

		if (newline != '\n')
		{
			log_err(_("read_backup_label: line too long in backup label file. Line begins \"%s: %s\""),
					label_key, label_value);
			return false;
		}

		log_debug("standby clone: got backup label entry \"%s: %s\"\n",
				label_key, label_value);

		if (strcmp(label_key, "START WAL LOCATION") == 0)
		{
			char start_wal_location[MAXLEN];
			char wal_filename[MAXLEN];

			nmatches = sscanf(label_value, "%" MAXLEN_STR "s (file %" MAXLEN_STR "[^)]", start_wal_location, wal_filename);

			if (nmatches != 2)
			{
				log_err(_("read_backup_label: unable to parse \"START WAL LOCATION\" in backup label\n"));
				return false;
			}

			out_backup_label->start_wal_location =
				parse_label_lsn(&label_key[0], start_wal_location);

			if (out_backup_label->start_wal_location == InvalidXLogRecPtr)
				return false;

			(void) strncpy(out_backup_label->start_wal_file, wal_filename, MAXLEN);
			out_backup_label->start_wal_file[MAXLEN-1] = '\0';
		}
		else if (strcmp(label_key, "CHECKPOINT LOCATION") == 0)
		{
			out_backup_label->checkpoint_location =
				parse_label_lsn(&label_key[0], &label_value[0]);

			if (out_backup_label->checkpoint_location == InvalidXLogRecPtr)
				return false;
		}
		else if (strcmp(label_key, "BACKUP METHOD") == 0)
		{
			(void) strncpy(out_backup_label->backup_method, label_value, MAXLEN);
			out_backup_label->backup_method[MAXLEN-1] = '\0';
		}
		else if (strcmp(label_key, "BACKUP FROM") == 0)
		{
			(void) strncpy(out_backup_label->backup_from, label_value, MAXLEN);
			out_backup_label->backup_from[MAXLEN-1] = '\0';
		}
		else if (strcmp(label_key, "START TIME") == 0)
		{
			(void) strncpy(out_backup_label->start_time, label_value, MAXLEN);
			out_backup_label->start_time[MAXLEN-1] = '\0';
		}
		else if (strcmp(label_key, "LABEL") == 0)
		{
			(void) strncpy(out_backup_label->label, label_value, MAXLEN);
			out_backup_label->label[MAXLEN-1] = '\0';
		}
		else if (strcmp(label_key, "MIN FAILOVER SLOT LSN") == 0)
		{
			out_backup_label->min_failover_slot_lsn =
				parse_label_lsn(&label_key[0], &label_value[0]);

			if (out_backup_label->min_failover_slot_lsn == InvalidXLogRecPtr)
				return false;
		}
		else
		{
			log_info("read_backup_label: ignored unrecognised backup label entry \"%s: %s\"",
					label_key, label_value);
		}
	}

	(void) fclose(label_file);

	log_debug(_("read_backup_label: label is %s; start wal file is %s\n"), out_backup_label->label, out_backup_label->start_wal_file);

	return true;
}

static void
do_standby_promote(void)
{
	PGconn	   *conn;

	char		script[MAXLEN];

	PGconn	   *old_master_conn;

	int			r,
				retval;
	char		data_dir[MAXLEN];

	int			i,
				promote_check_timeout  = 60,
				promote_check_interval = 2;
	bool		promote_success = false;
	bool        success;
	PQExpBufferData details;

	/* We need to connect to check configuration */
	log_info(_("connecting to standby database\n"));
	conn = establish_db_connection(options.conninfo, true);

	/* Verify that standby is a supported server version */
	log_verbose(LOG_INFO, _("connected to standby, checking its state\n"));

	check_server_version(conn, "standby", true, NULL);

	/* Check we are in a standby node */
	retval = is_standby(conn);
	if (retval == 0 || retval == -1)
	{
		log_err(_(retval == 0 ? "this command should be executed on a standby node\n" :
				  "connection to node lost!\n"));

		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* we also need to check if there isn't any master already */
	old_master_conn = get_master_connection(conn,
											options.cluster_name, NULL, NULL);
	if (old_master_conn != NULL)
	{
		log_err(_("this cluster already has an active master server\n"));
		PQfinish(old_master_conn);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	log_notice(_("promoting standby\n"));

	/* Get the data directory */
	success = get_pg_setting(conn, "data_directory", data_dir);
	PQfinish(conn);

	if (success == false)
	{
		log_err(_("unable to determine data directory\n"));
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Promote standby to master.
	 *
	 * `pg_ctl promote` returns immediately and has no -w option, so we
	 * can't be sure when or if the promotion completes.
	 * For now we'll poll the server until the default timeout (60 seconds)
	 */
	maxlen_snprintf(script, "%s -D %s promote",
					make_pg_path("pg_ctl"), data_dir);
	log_notice(_("promoting server using '%s'\n"),
			   script);

	r = system(script);
	if (r != 0)
	{
		log_err(_("unable to promote server from standby to master\n"));
		exit(ERR_NO_RESTART);
	}

	/* reconnect to check we got promoted */

	log_info(_("reconnecting to promoted server\n"));
	conn = establish_db_connection(options.conninfo, true);

	for(i = 0; i < promote_check_timeout; i += promote_check_interval)
	{
		retval = is_standby(conn);
		if (!retval)
		{
			promote_success = true;
			break;
		}
		sleep(promote_check_interval);
	}

	if (promote_success == false)
	{
		log_err(_(retval == 1 ?
			  "STANDBY PROMOTE failed, this is still a standby node.\n" :
				  "connection to node lost!\n"));
		exit(ERR_FAILOVER_FAIL);
	}


	/* update node information to reflect new status */
	if (update_node_record_set_master(conn, options.node) == false)
	{
		initPQExpBuffer(&details);
		appendPQExpBuffer(&details,
						  _("unable to update node record for node %i"),
						  options.node);

		log_err("%s\n", details.data);

		create_event_record(NULL,
							&options,
							options.node,
							"standby_promote",
							false,
							details.data);

		exit(ERR_DB_QUERY);
	}


	initPQExpBuffer(&details);
	appendPQExpBuffer(&details,
					  "node %i was successfully promoted to master",
					  options.node);

	log_notice(_("STANDBY PROMOTE successful\n"));

	/* Log the event */
	create_event_record(conn,
						&options,
						options.node,
						"standby_promote",
						true,
						details.data);

	PQfinish(conn);

	return;
}


/*
 * Follow a new primary.
 *
 * This function has two "modes":
 *  1) no primary info provided - determine primary from standby metadata
 *  2) primary info provided - use that info to connect to the primary.
 *
 * (2) is mainly for when a node has been stopped as part of a switchover
 * and needs to be started with recovery.conf correctly configured.
 */

static void
do_standby_follow(void)
{
	PGconn	   *conn;

	char		script[MAXLEN];
	char		master_conninfo[MAXLEN];
	PGconn	   *master_conn;
	int			master_id = 0;

	int			r,
				retval;
	char		data_dir[MAXPGPATH];

	bool        success;

	log_debug("do_standby_follow()\n");

	/*
	 * If -h/--host wasn't provided, attempt to connect to standby
	 * to determine primary, and carry out some other checks while we're
	 * at it.
	 */
	if (host_param_provided == false)
	{
		/* We need to connect to check configuration */
		log_info(_("connecting to standby database\n"));
		conn = establish_db_connection(options.conninfo, true);
		log_verbose(LOG_INFO, _("connected to standby, checking its state\n"));

		/* Check we are in a standby node */
		retval = is_standby(conn);
		if (retval == 0 || retval == -1)
		{
			log_err(_(retval == 0 ? "this command should be executed on a standby node\n" :
					  "connection to node lost!\n"));

			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		/* Get the data directory full path */
		success = get_pg_setting(conn, "data_directory", data_dir);

		if (success == false)
		{
			log_err(_("unable to determine data directory\n"));
			exit(ERR_BAD_CONFIG);
		}

		/*
		 * we also need to check if there is any master in the cluster or wait for
		 * one to appear if we have set the wait option
		 */
		log_info(_("discovering new master...\n"));

		do
		{
			if (!is_pgup(conn, options.master_response_timeout))
			{
				conn = establish_db_connection(options.conninfo, true);
			}

			master_conn = get_master_connection(conn,
												options.cluster_name, &master_id, (char *) &master_conninfo);
		}
		while (master_conn == NULL && runtime_options.wait_for_master);

		if (master_conn == NULL)
		{
			log_err(_("unable to determine new master node\n"));
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		/*
		 * Verify that standby and master are supported and compatible server
		 * versions
		 */
		check_master_standby_version_match(conn, master_conn);

		PQfinish(conn);
	}
	/* primary server info explictly provided - attempt to connect to that */
	else
	{
		master_conn = establish_db_connection_by_params((const char**)param_keywords, (const char**)param_values, true);

		master_id = get_master_node_id(master_conn, options.cluster_name);

		strncpy(data_dir, runtime_options.dest_dir, MAXPGPATH);
	}


	/* Check we are going to point to a master */
	retval = is_standby(master_conn);
	if (retval)
	{
		log_err(_(retval == 1 ? "the node to follow should be a master\n" :
				  "connection to node lost!\n"));

		PQfinish(master_conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * If 9.4 or later, and replication slots in use, we'll need to create a
	 * slot on the new master
	 */

	if (options.use_replication_slots)
	{
 		int	server_version_num = get_server_version(master_conn, NULL);

		if (create_replication_slot(master_conn, repmgr_slot_name, server_version_num) == false)
		{
			PQExpBufferData event_details;
			initPQExpBuffer(&event_details);

			appendPQExpBuffer(&event_details,
							  _("Unable to create slot '%s' on the master node: %s"),
							  repmgr_slot_name,
							  PQerrorMessage(master_conn));

			log_err("%s\n", event_details.data);

			create_event_record(master_conn,
								&options,
								options.node,
								"standby_follow",
								false,
								event_details.data);

			PQfinish(master_conn);
			exit(ERR_DB_QUERY);
		}
	}

	/* XXX add more detail! */
	log_info(_("changing standby's master\n"));

	/* write the recovery.conf file */
	if (!create_recovery_file(data_dir, master_conn))
		exit(ERR_BAD_CONFIG);

	/* Finally, restart the service */
	if (*options.restart_command)
	{
		maxlen_snprintf(script, "%s", options.restart_command);
	}
	else
	{
		maxlen_snprintf(script, "%s %s -w -D %s -m fast restart",
				        make_pg_path("pg_ctl"), options.pg_ctl_options, data_dir);
	}
	log_notice(_("restarting server using '%s'\n"),
			   script);

	r = system(script);
	if (r != 0)
	{
		log_err(_("unable to restart server\n"));
		exit(ERR_NO_RESTART);
	}

	/*
	 * It's possible this node was an inactive primary - update the
	 * relevant fields to ensure it's marked as an active standby
	 */
	if (update_node_record_status(master_conn,
								  options.cluster_name,
								  options.node,
								  "standby",
								  master_id,
								  true) == false)
	{
		log_err(_("unable to update upstream node\n"));
		PQfinish(master_conn);

		exit(ERR_BAD_CONFIG);
	}

	/* XXX add event record - possible move from repmgrd? */
	PQfinish(master_conn);

	return;
}


/*
 * Perform a switchover by:
 *  - stopping current primary node
 *  - promoting this standby node to primary
 *  - forcing previous primary node to follow this node
 *
 * Caveats:
 *  - repmgrd must not be running, otherwise it may
 *    attempt a failover
 *    (TODO: find some way of notifying repmgrd of planned
 *     activity like this)
 *  - currently only set up for two-node operation; any other
 *    standbys will probably become downstream cascaded standbys
 *    of the old primary once it's restarted
 *  - as we're executing repmgr remotely (on the old primary),
 *    we'll need the location of its configuration file; this
 *    can be provided explicitly with -C/--remote-config-file,
 *    otherwise repmgr will look in default locations on the
 *    remote server
 *
 * TODO:
 *  - make connection test timeouts/intervals configurable (see below)
 *  - add command line option --remote_pg_bindir or similar to
 *    optionally handle cases where the remote pg_bindir is different
 */

static void
do_standby_switchover(void)
{
	PGconn	   *local_conn;
	PGconn	   *remote_conn;
	int			server_version_num;
	bool		use_pg_rewind;

	/* the remote server is the primary to be demoted */
	char	    remote_conninfo[MAXCONNINFO] = "";
	char	    remote_host[MAXLEN];
	char        remote_data_directory[MAXLEN];
	int         remote_node_id;
	char        remote_node_replication_state[MAXLEN] = "";
	char        remote_archive_config_dir[MAXLEN];
	char	    remote_pg_rewind[MAXLEN];
	int			i,
				r = 0;

	char	    command[MAXLEN];
	PQExpBufferData command_output;

	char	    repmgr_db_cli_params[MAXLEN] = "";
	int	        query_result;
	t_node_info remote_node_record = T_NODE_INFO_INITIALIZER;
	bool		connection_success,
				shutdown_success;

	/*
	 * SANITY CHECKS
	 *
	 * We'll be doing a bunch of operations on the remote server (primary
	 * to be demoted) - careful checks needed before proceding.
	 */

	log_notice(_("switching current node %i to master server and demoting current master to standby...\n"), options.node);

	local_conn = establish_db_connection(options.conninfo, true);

	/* Check that this is a standby */

	if (!is_standby(local_conn))
	{
		log_err(_("switchover must be executed from the standby node to be promoted\n"));
		PQfinish(local_conn);

		exit(ERR_SWITCHOVER_FAIL);
	}

	server_version_num = check_server_version(local_conn, "master", true, NULL);

	/*
	 * Add a friendly notice if --pg_rewind supplied for 9.5 and later - we'll
	 * be ignoring it anyway
	 */
	if (pg_rewind_supplied == true && server_version_num >= 90500)
	{
		log_notice(_("--pg_rewind not required for PostgreSQL 9.5 and later\n"));
	}

	/*
	 * TODO: check that standby's upstream node is the primary
	 * (it's probably not feasible to switch over to a cascaded standby)
	 */

	/* Check that primary is available */
	remote_conn = get_master_connection(local_conn, options.cluster_name, &remote_node_id, remote_conninfo);

	if (remote_conn == NULL)
	{
		log_err(_("unable to connect to current master node\n"));
		log_hint(_("check that the cluster is correctly configured and this standby is registered\n"));
		PQfinish(local_conn);
		exit(ERR_DB_CON);
	}

	/* Get the remote's node record */
	query_result = get_node_record(remote_conn, options.cluster_name, remote_node_id, &remote_node_record);

	if (query_result < 1)
	{
		log_err(_("unable to retrieve node record for node %i\n"), remote_node_id);

		PQfinish(local_conn);

		exit(ERR_DB_QUERY);
	}

	log_debug("remote node name is \"%s\"\n", remote_node_record.name);

	/*
	 * Check that we can connect by SSH to the remote (current primary) server,
	 * and read its data directory
	 *
	 * TODO: check we can read contents of PG_VERSION??
	 * -> assuming the remote user/directory is set up correctly,
	 * we should only be able to see the file as the PostgreSQL
	 * user, so it should be readable anyway
	 */
	get_conninfo_value(remote_conninfo, "host", remote_host);

	r = test_ssh_connection(remote_host, runtime_options.remote_user);

	if (r != 0)
	{
		log_err(_("unable to connect via ssh to host %s, user %s\n"), remote_host, runtime_options.remote_user);
	}

	if (get_pg_setting(remote_conn, "data_directory", remote_data_directory) == false)
	{
		log_err(_("unable to retrieve master's data directory location\n"));
		PQfinish(remote_conn);
		PQfinish(local_conn);
		exit(ERR_DB_CON);
	}

	log_debug("master's data directory is: %s\n", remote_data_directory);

	maxlen_snprintf(command,
					"ls %s/PG_VERSION >/dev/null 2>&1 && echo 1 || echo 0",
					remote_data_directory);
	initPQExpBuffer(&command_output);

	(void)remote_command(
		remote_host,
		runtime_options.remote_user,
		command,
		&command_output);

	if (*command_output.data == '1')
	{
		log_verbose(LOG_DEBUG, "PG_VERSION found in %s\n", remote_data_directory);
	}
	else if (*command_output.data == '0')
	{
		log_err(_("%s is not a PostgreSQL data directory or is not accessible to user %s\n"), remote_data_directory, runtime_options.remote_user);
		PQfinish(remote_conn);
		PQfinish(local_conn);
		exit(ERR_BAD_CONFIG);
	}
	else
	{
		log_err(_("Unexpected output from remote command:\n%s\n"), command_output.data);
		PQfinish(remote_conn);
		PQfinish(local_conn);
		exit(ERR_BAD_CONFIG);
	}

	termPQExpBuffer(&command_output);


	if (server_version_num >= 90500)
	{
		/* 9.5 and later have pg_rewind built-in - always use that */
		use_pg_rewind = true;
		maxlen_snprintf(remote_pg_rewind,
						"%s",
						make_pg_path("pg_rewind"));
	}
	else
	{
		/* 9.3/9.4 - user can use separately-compiled pg_rewind */
		if (pg_rewind_supplied == true)
		{
			use_pg_rewind = true;

			/* User has specified pg_rewind path */
			if (strlen(runtime_options.pg_rewind))
			{
				maxlen_snprintf(remote_pg_rewind,
								"%s",
								runtime_options.pg_rewind);
			}
			/* No path supplied - assume in normal bindir */
			else
			{
				maxlen_snprintf(remote_pg_rewind,
								"%s",
								make_pg_path("pg_rewind"));
			}
		}
		else
		{
			use_pg_rewind = false;
		}
	}

	/* Sanity checks so we're sure pg_rewind can be used */
	if (use_pg_rewind == true)
	{
		bool wal_log_hints = false;

		/* check pg_rewind actually exists on remote */

		maxlen_snprintf(command,
						"ls -1 %s >/dev/null 2>&1 && echo 1 || echo 0",
						remote_pg_rewind);

		initPQExpBuffer(&command_output);

		(void)remote_command(
			remote_host,
			runtime_options.remote_user,
			command,
			&command_output);

		if (*command_output.data == '0')
		{
			log_err(_("unable to find pg_rewind on the remote server\n"));
			log_err(_("expected location is: %s\n"), remote_pg_rewind);
			exit(ERR_BAD_CONFIG);
		}

		/* check that server is appropriately configured */

		/*
		 * "full_page_writes" must be enabled in any case
		 */

		if (guc_set(remote_conn, "full_page_writes", "=", "off"))
		{
			log_err(_("\"full_page_writes\" must be set to \"on\""));
			exit(ERR_BAD_CONFIG);
		}

		/*
		 * Check whether wal_log_hints is on - if so we're fine and don't need
		 * to check for checksums
		 */

		wal_log_hints = guc_set(remote_conn, "wal_log_hints", "=", "on");

		if (wal_log_hints == false)
		{
			char local_data_directory[MAXLEN];
			int  data_checksum_version;

			/*
			 * check the *local* server's control data for the date checksum
			 * version - much easier than doing it on the remote server
			 */

			if (get_pg_setting(local_conn, "data_directory", local_data_directory) == false)
			{
				log_err(_("unable to retrieve standby's data directory location\n"));
				PQfinish(remote_conn);
				PQfinish(local_conn);
				exit(ERR_DB_CON);
			}

			data_checksum_version = get_data_checksum_version(local_data_directory);

			if (data_checksum_version == 0)
			{
				log_err(_("pg_rewind cannot be used - data checksums are not enabled for this cluster and \"wal_log_hints\" is \"off\"\n"));
				exit(ERR_BAD_CONFIG);
			}
		}
	}

	PQfinish(local_conn);
	PQfinish(remote_conn);

	/* Determine the remote's configuration file location */

	/* Remote configuration file provided - check it exists */
	if (runtime_options.remote_config_file[0])
	{
		log_verbose(LOG_INFO, _("looking for file \"%s\" on remote server \"%s\"\n"),
					runtime_options.remote_config_file,
					remote_host);

		maxlen_snprintf(command,
						"ls -1 %s >/dev/null 2>&1 && echo 1 || echo 0",
						runtime_options.remote_config_file);

		initPQExpBuffer(&command_output);

		(void)remote_command(
			remote_host,
			runtime_options.remote_user,
			command,
			&command_output);

		if (*command_output.data == '0')
		{
			log_err(_("unable to find the specified repmgr configuration file on remote server\n"));
			exit(ERR_BAD_CONFIG);
		}

		termPQExpBuffer(&command_output);

		log_verbose(LOG_INFO, _("remote configuration file \"%s\" found on remote server\n"),
					runtime_options.remote_config_file);

		termPQExpBuffer(&command_output);
	}
	/*
	 * No remote configuration file provided - check some default locations:
	 *  - path of configuration file for this repmgr
	 *  - /etc/repmgr.conf
	 */
	else
	{
		int		    i;
		bool		config_file_found = false;

		const char *config_paths[] = {
			runtime_options.config_file,
			"/etc/repmgr.conf",
			NULL
		};

		log_verbose(LOG_INFO, _("no remote configuration file provided - checking default locations\n"));

		for(i = 0; config_paths[i] && config_file_found == false; ++i)
		{
			log_verbose(LOG_INFO, _("checking \"%s\"\n"), config_paths[i]);

			maxlen_snprintf(command,
							"ls -1 %s >/dev/null 2>&1 && echo 1 || echo 0",
							config_paths[i]);

			initPQExpBuffer(&command_output);

			(void)remote_command(
				remote_host,
				runtime_options.remote_user,
				command,
				&command_output);

			if (*command_output.data == '1')
			{
				strncpy(runtime_options.remote_config_file, config_paths[i], MAXLEN);
				log_verbose(LOG_INFO, _("configuration file \"%s\" found on remote server\n"),
							runtime_options.remote_config_file);
				config_file_found = true;
			}

			termPQExpBuffer(&command_output);
		}

		if (config_file_found == false)
		{
			log_err(_("no remote configuration file supplied or found in a default location - terminating\n"));
			log_hint(_("specify the remote configuration file with -C/--remote-config-file\n"));
			exit(ERR_BAD_CONFIG);
		}
	}



	/*
	 * Sanity checks completed - prepare for the switchover
	 */

	/*
	 * When using pg_rewind (the preferable option, and default from 9.5
	 * onwards), we need to archive any configuration files in the remote
	 * server's data directory as they'll be overwritten by pg_rewind
	 *
	 * Possible todo item: enable the archive location to be specified
	 * by the user
	 */
	if (use_pg_rewind == true)
	{
		maxlen_snprintf(remote_archive_config_dir,
						"/tmp/repmgr-%s-archive",
						remote_node_record.name);

		log_verbose(LOG_DEBUG, "remote_archive_config_dir: %s\n", remote_archive_config_dir);

		maxlen_snprintf(command,
						"%s standby archive-config -f %s --config-archive-dir=%s",
						make_pg_path("repmgr"),
						runtime_options.remote_config_file,
						remote_archive_config_dir);

		log_debug("Executing:\n%s\n", command);

		initPQExpBuffer(&command_output);

		(void)remote_command(
			remote_host,
			runtime_options.remote_user,
			command,
			&command_output);

		termPQExpBuffer(&command_output);
	}

	/*
	 * Stop the remote primary
	 *
	 * We'll issue the pg_ctl command but not force it not to wait; we'll check
	 * the connection from here - and error out if no shutdown is detected
	 * after a certain time.
	 *
	 * XXX currently we assume the same Postgres binary path on the primary
	 * as configured on the local standby; we may need to add a command
	 * line option to provide an explicit path (--remote-pg-bindir)?
	 */

	/*
	 * TODO
	 * - notify repmgrd instances that this is a controlled
	 *   event so they don't initiate failover
	 * - optional "immediate" shutdown?
	 *    -> use -F/--force?
	 */

	if (*options.stop_command)
	{
		maxlen_snprintf(command, "%s", options.stop_command);
	}
	else
	{
	        maxlen_snprintf(command,
					"%s -D %s -m %s -W stop >/dev/null 2>&1 && echo 1 || echo 0",
					make_pg_path("pg_ctl"),
					remote_data_directory,
					runtime_options.pg_ctl_mode);
	}
	initPQExpBuffer(&command_output);

	// XXX handle failure

	(void)remote_command(
		remote_host,
		runtime_options.remote_user,
		command,
		&command_output);

	termPQExpBuffer(&command_output);

	shutdown_success = false;

	/* loop for timeout waiting for current primary to stop */

	for (i = 0; i < options.reconnect_attempts; i++)
	{
		/* Check whether primary is available */

		PGPing ping_res = PQping(remote_conninfo);

		/* database server could not be contacted */
		if (ping_res == PQPING_NO_RESPONSE)
		{
			bool command_success;

			/*
			 * directly access the server and check that the
			 * pidfile has gone away so we can be sure the server is actually
			 * shut down and the PQPING_NO_RESPONSE is not due to other issues
			 * such as coincidental network failure.
			 */
			initPQExpBuffer(&command_output);

			maxlen_snprintf(command,
					"ls %s/postmaster.pid >/dev/null 2>&1 && echo 1 || echo 0",
					remote_data_directory);

			command_success = remote_command(
				remote_host,
				runtime_options.remote_user,
				command,
				&command_output);

			if (command_success == true && *command_output.data == '0')
			{
				shutdown_success = true;

				log_notice(_("current master has been stopped\n"));

				termPQExpBuffer(&command_output);

				break;
			}

			termPQExpBuffer(&command_output);
		}

		/* XXX make configurable? */
		sleep(options.reconnect_interval);
		i++;
	}

	if (shutdown_success == false)
	{
		log_err(_("master server did not shut down\n"));
		log_hint(_("check the master server status before performing any further actions"));
		exit(ERR_SWITCHOVER_FAIL);
	}

	/* promote this standby */

	do_standby_promote();

	/*
	 * TODO: optionally have any other downstream nodes from old primary
	 * follow new primary? Currently they'll just latch onto the old
	 * primary as cascaded standbys.
	 */

	/* restore old primary */

	/* TODO: additional check old primary is shut down */

	if (use_pg_rewind == true)
	{
		PQExpBufferData recovery_done_remove;

		/* Execute pg_rewind */
		maxlen_snprintf(command,
						"%s -D %s --source-server=\\'%s\\'",
						remote_pg_rewind,
						remote_data_directory,
						options.conninfo);

		log_notice("Executing pg_rewind on old master server\n");
		log_debug("pg_rewind command is:\n%s\n", command);

		initPQExpBuffer(&command_output);

		// XXX handle failure

		(void)remote_command(
			remote_host,
			runtime_options.remote_user,
			command,
			&command_output);

		termPQExpBuffer(&command_output);

		/* Restore any previously archived config files */
		maxlen_snprintf(command,
						"%s standby restore-config -D %s --config-archive-dir=%s",
						make_pg_path("repmgr"),
						remote_data_directory,
						remote_archive_config_dir);

		initPQExpBuffer(&command_output);

		// XXX handle failure

		(void)remote_command(
			remote_host,
			runtime_options.remote_user,
			command,
			&command_output);

		termPQExpBuffer(&command_output);

		/* remove any recovery.done file copied in by pg_rewind */

		initPQExpBuffer(&recovery_done_remove);

		appendPQExpBuffer(&recovery_done_remove,
						  "test -e %s/recovery.done && rm -f %s/recovery.done",
						  remote_data_directory,
						  remote_data_directory);
		initPQExpBuffer(&command_output);

		// XXX handle failure

		(void)remote_command(
			remote_host,
			runtime_options.remote_user,
			recovery_done_remove.data,
			&command_output);

		termPQExpBuffer(&command_output);
		termPQExpBuffer(&recovery_done_remove);



	}
	else
	{
		/*
		 * For 9.3/9.4, if pg_rewind is not available on the remote server,
		 * we'll need to force a reclone of the standby sing rsync - this may
		 * take some time on larger databases, so use with care!
		 *
		 * Note that following this clone we'll be using `repmgr standby follow`
		 * to start the server - that will mean recovery.conf will be created
		 * for a second time, but as this is a workaround for the absence
		 * of pg_rewind. It's preferable to have `repmgr standby follow` start
		 * the remote database as it can access the remote config file
		 * directly.
		 */

		format_db_cli_params(options.conninfo, repmgr_db_cli_params);
		maxlen_snprintf(command,
						"%s -D %s -f %s %s --rsync-only --force --ignore-external-config-files standby clone",
						make_pg_path("repmgr"),
						remote_data_directory,
						runtime_options.remote_config_file,
						repmgr_db_cli_params
			);

		log_debug("Executing:\n%s\n", command);

		initPQExpBuffer(&command_output);

		(void)remote_command(
			remote_host,
			runtime_options.remote_user,
			command,
			&command_output);

		termPQExpBuffer(&command_output);
	}

	/*
	 * Execute `repmgr standby follow` to create recovery.conf and start
	 * the remote server
	 */
	format_db_cli_params(options.conninfo, repmgr_db_cli_params);
	maxlen_snprintf(command,
					"%s -D %s -f %s %s standby follow",
					make_pg_path("repmgr"),
					remote_data_directory,
					runtime_options.remote_config_file,
					repmgr_db_cli_params
		);

	log_debug("Executing:\n%s\n", command);

	initPQExpBuffer(&command_output);

	(void)remote_command(
		remote_host,
		runtime_options.remote_user,
		command,
		&command_output);

	termPQExpBuffer(&command_output);

	/* verify that new standby is connected and replicating */

	connection_success = false;

	for(i = 0; i < options.reconnect_attempts; i++)
	{
		/* Check whether primary is available */
		remote_conn = test_db_connection(remote_conninfo);

		if (PQstatus(remote_conn) == CONNECTION_OK)
		{
			log_debug("connected to new standby (old master)\n");
			if (is_standby(remote_conn) == 0)
			{
				log_err(_("new standby (old master) is not a standby\n"));
				exit(ERR_SWITCHOVER_FAIL);
			}
			connection_success = true;
			break;
		}
		PQfinish(remote_conn);

		sleep(options.reconnect_interval);
		i++;
	}

	if (connection_success == false)
	{
		log_err(_("unable to connect to new standby (old master)\n"));
		exit(ERR_SWITCHOVER_FAIL);
	}

	log_debug("new standby is in recovery\n");

	/* Check for entry in pg_stat_replication */

	local_conn = establish_db_connection(options.conninfo, true);

	query_result = get_node_replication_state(local_conn, remote_node_record.name, remote_node_replication_state);

	if (query_result == -1)
	{
		log_err(_("unable to retrieve replication status for node %i\n"), remote_node_id);
		PQfinish(local_conn);

		exit(ERR_SWITCHOVER_FAIL);
	}

	if (query_result == 0)
	{
		log_err(_("node %i not replicating\n"), remote_node_id);
	}
	else
	{
		/* XXX we should poll for a while in case the node takes time to connect to the primary */
		if (strcmp(remote_node_replication_state, "streaming") == 0 ||
			strcmp(remote_node_replication_state, "catchup")  == 0)
		{
			log_verbose(LOG_NOTICE, _("node %i is replicating in state \"%s\"\n"), remote_node_id, remote_node_replication_state);
		}
		else
		{
			/*
			 * Other possible replication states are:
			 *  - startup
			 *  - backup
			 *  - UNKNOWN
			 */
			log_err(_("node %i has unexpected replication state \"%s\"\n"),
					remote_node_id, remote_node_replication_state);
			PQfinish(local_conn);
			exit(ERR_SWITCHOVER_FAIL);
		}
	}

	/*
	 * If replication slots are in use, and an inactive one for this node
	 * (a former standby) exists on the remote node (a former primary),
	 * drop it.
	 */

	if (options.use_replication_slots)
	{
		t_node_info local_node_record  = T_NODE_INFO_INITIALIZER;

		query_result = get_node_record(local_conn, options.cluster_name, options.node, &local_node_record);

		remote_conn = establish_db_connection(remote_conninfo, false);

		if (PQstatus(remote_conn) != CONNECTION_OK)
		{
			log_warning(_("unable to connect to former master to clean up replication slots \n"));
		}
		else
		{
			t_replication_slot  slot_info;
			int					query_res;

			query_res = get_slot_record(remote_conn, local_node_record.slot_name, &slot_info);

			if (query_res)
			{
				if (slot_info.active == false)
				{
					if (drop_replication_slot(remote_conn, local_node_record.slot_name) == true)
					{
						log_notice(_("replication slot \"%s\" deleted on former master\n"), local_node_record.slot_name);
					}
					else
					{
						log_err(_("unable to delete replication slot \"%s\" on former master\n"), local_node_record.slot_name);
					}
				}
				/* if active replication slot exists, call Houston as we have a problem */
				else
				{
					log_err(_("replication slot \"%s\" is still active on former master\n"), local_node_record.slot_name);
				}
			}
		}

		PQfinish(remote_conn);
	}

	/* TODO: verify this node's record was updated correctly */

	PQfinish(local_conn);

	log_notice(_("switchover was successful\n"));
	return;
}


/*
 * Intended mainly for "internal" use by `standby switchover`, which
 * calls this on the target server to archive any configuration files
 * in the data directory, which may be overwritten by an operation
 * like pg_rewind
 */
static void
do_standby_archive_config(void)
{
	PGconn	   *local_conn = NULL;
	char		sqlquery[QUERY_STR_LEN];
	PGresult   *res;
	int			i, copied_count = 0;

	if (mkdir(runtime_options.config_archive_dir, S_IRWXU) != 0 && errno != EEXIST)
	{
		log_err(_("unable to create temporary directory\n"));
		exit(ERR_BAD_CONFIG);
	}

	// XXX check if directory is directory and we own it
	// XXX delete any files in dir in case it existed already

	local_conn = establish_db_connection(options.conninfo, true);

	/*
	 * Detect which config files are actually inside the data directory;
	 * this query will include any settings from included files too
	 */
	sqlquery_snprintf(sqlquery,
					  "WITH files AS ( "
					  "  WITH dd AS ( "
					  "    SELECT setting "
					  "     FROM pg_settings "
					  "    WHERE name = 'data_directory') "
					  " SELECT distinct(sourcefile) AS config_file"
					  "   FROM dd, pg_settings ps "
					  "  WHERE ps.sourcefile IS NOT NULL "
					  "    AND ps.sourcefile ~ ('^' || dd.setting) "
					  "     UNION "
					  "  SELECT ps.setting  AS config_file"
					  "    FROM dd, pg_settings ps "
					  "   WHERE ps.name IN ( 'config_file', 'hba_file', 'ident_file') "
					  "     AND ps.setting ~ ('^' || dd.setting) "
					  ") "
					  "  SELECT config_file, "
					  "         regexp_replace(config_file, '^.*\\/','') AS filename "
					  "    FROM files "
					  "ORDER BY config_file");

	log_verbose(LOG_DEBUG, "do_standby_archive_config(): %s\n", sqlquery);

	res = PQexec(local_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("unable to query config file locations\n"));
		PQclear(res);
		PQfinish(local_conn);
		exit(ERR_DB_QUERY);
	}

	/* Copy any configuration files to the specified directory */
	for (i = 0; i < PQntuples(res); i++)
	{
		PQExpBufferData buf;

		initPQExpBuffer(&buf);
		appendPQExpBuffer(&buf, "%s/%s",
						  runtime_options.config_archive_dir, PQgetvalue(res, i, 1));

		log_verbose(LOG_DEBUG, "Copying %s to %s/\n", PQgetvalue(res, i, 0), buf.data);
		/* XXX check result */
		copy_file(PQgetvalue(res, i, 0), buf.data);

		termPQExpBuffer(&buf);

		copied_count++;
	}

	PQclear(res);

	PQfinish(local_conn);

	log_notice(_("%i files copied to %s\n"), copied_count, runtime_options.config_archive_dir);
}

/*
 * Intended mainly for "internal" use by `standby switchover`, which
 * calls this on the target server to restore any configuration files
 * to the data directory, which may have been overwritten by an operation
 * like pg_rewind
 *
 * Not designed to be called if the instance is running, but does
 * not currently check.
 *
 * Requires -D/--data-dir and --config_archive_dir
 *
 * Removes --config_archive_dir after successful copy
 */
static void
do_standby_restore_config(void)
{
	DIR			  *arcdir;
	struct dirent *arcdir_ent;
	int			   copied_count = 0;
	bool		   copy_ok = true;

	arcdir = opendir(runtime_options.config_archive_dir);
	if (arcdir == NULL)
	{
		log_err(_("Unable to open directory '%s'\n"), runtime_options.config_archive_dir);
		exit(ERR_BAD_CONFIG);
	}

	while ((arcdir_ent = readdir(arcdir)) != NULL) {
		struct stat statbuf;
		char arcdir_ent_path[MAXPGPATH];
		PQExpBufferData src_file;
		PQExpBufferData dst_file;

		snprintf(arcdir_ent_path, MAXPGPATH,
				 "%s/%s",
				 runtime_options.config_archive_dir,
				 arcdir_ent->d_name);

		if (stat(arcdir_ent_path, &statbuf) == 0 && !S_ISREG(statbuf.st_mode))
		{
			continue;
		}
		initPQExpBuffer(&src_file);
		initPQExpBuffer(&dst_file);

		appendPQExpBuffer(&src_file, "%s/%s",
						  runtime_options.config_archive_dir, arcdir_ent->d_name);

		appendPQExpBuffer(&dst_file, "%s/%s",
						  runtime_options.dest_dir, arcdir_ent->d_name);

		log_verbose(LOG_DEBUG, "Copying %s to %s\n", src_file.data, dst_file.data);

		/* XXX check result */

		if (copy_file(src_file.data, dst_file.data) == false)
		{
			copy_ok = false;
			log_warning(_("Unable to copy %s from %s\n"), arcdir_ent->d_name, runtime_options.config_archive_dir);
		}
		else
		{
			unlink(src_file.data);
			copied_count++;
		}

		termPQExpBuffer(&src_file);
		termPQExpBuffer(&dst_file);
	}

	closedir(arcdir);


	if (copy_ok == false)
	{
		log_err(_("Unable to copy all files from %s\n"), runtime_options.config_archive_dir);
		exit(ERR_BAD_CONFIG);
	}

	log_notice(_("%i files copied to %s\n"), copied_count, runtime_options.dest_dir);

	/*
	 * Finally, delete directory - it should be empty unless it's been interfered
	 * with for some reason, in which case manual attention is required
	 */

	if (rmdir(runtime_options.config_archive_dir) != 0 && errno != EEXIST)
	{
		log_err(_("Unable to delete %s\n"), runtime_options.config_archive_dir);
		exit(ERR_BAD_CONFIG);
	}

	log_verbose(LOG_NOTICE, "Directory %s deleted\n", runtime_options.config_archive_dir);

	return;
}


static void
do_witness_create(void)
{
	PGconn	   *masterconn;

	char		script[MAXLEN];
	char		buf[MAXLEN];
	FILE	   *pg_conf = NULL;

	int			r = 0,
				retval;

	char		master_hba_file[MAXLEN];
	bool        success;

	char		witness_port[MAXLEN];
	char		repmgr_user[MAXLEN];
	char		repmgr_db[MAXLEN];

	/*
	 * Extract the repmgr user and database names from the conninfo string
	 * provided in repmgr.conf
	 */
	get_conninfo_value(options.conninfo, "user", repmgr_user);
	get_conninfo_value(options.conninfo, "dbname", repmgr_db);

	param_set("user", repmgr_user);
	param_set("dbname", repmgr_db);

	/* We need to connect to check configuration and copy it */
	masterconn = establish_db_connection_by_params((const char**)param_keywords, (const char**)param_values, false);

	if (PQstatus(masterconn) != CONNECTION_OK)
	{
		/* No event logging possible here as we can't connect to the master */
		log_err(_("unable to connect to master\n"));
		exit(ERR_DB_CON);
	}

	/* Verify that master is a supported server version */
	check_server_version(masterconn, "master", true, NULL);

	/* Check we are connecting to a primary node */
	retval = is_standby(masterconn);
	if (retval)
	{
		PQExpBufferData errmsg;
		initPQExpBuffer(&errmsg);
		appendPQExpBuffer(&errmsg,
						  "%s",
						  _(retval == 1 ?
							"provided upstream node is not a master" :
							"connection to upstream node lost"));

		log_err("%s\n", errmsg.data);

		create_event_record(masterconn,
							&options,
							options.node,
							"witness_create",
							false,
							errmsg.data);
		PQfinish(masterconn);
		exit(ERR_BAD_CONFIG);
	}

	log_info(_("successfully connected to master.\n"));

	r = test_ssh_connection(runtime_options.host, runtime_options.remote_user);
	if (r != 0)
	{
		PQExpBufferData errmsg;
		initPQExpBuffer(&errmsg);
		appendPQExpBuffer(&errmsg,
						  _("unable to connect to remote host '%s' via SSH"),
						  runtime_options.host);
		log_err("%s\n", errmsg.data);

		create_event_record(masterconn,
							&options,
							options.node,
							"witness_create",
							false,
							errmsg.data);
		PQfinish(masterconn);
		exit(ERR_BAD_SSH);
	}

	/* Check this directory could be used as a PGDATA dir */
	if (!create_witness_pg_dir(runtime_options.dest_dir, runtime_options.force))
	{
		PQExpBufferData errmsg;
		initPQExpBuffer(&errmsg);
		appendPQExpBuffer(&errmsg,
						  _("unable to create witness server data directory (\"%s\")"),
						  runtime_options.host);
		log_err("%s\n", errmsg.data);
		create_event_record(masterconn,
							&options,
							options.node,
							"witness_create",
							false,
							errmsg.data);
		exit(ERR_BAD_CONFIG);
	}


	/*
	 * To create a witness server we need to: 1) initialize the cluster 2)
	 * register the witness in repl_nodes 3) copy configuration from master
	 */

	/* Create the cluster for witness */
	if (!runtime_options.superuser[0])
		strncpy(runtime_options.superuser, "postgres", MAXLEN);

	maxlen_snprintf(script, "%s %s -D %s init -o \"%s-U %s\"",
				   make_pg_path("pg_ctl"),
				   options.pg_ctl_options, runtime_options.dest_dir,
				   runtime_options.witness_pwprompt ? "-W " : "",
				   runtime_options.superuser);
	log_info(_("initializing cluster for witness: %s.\n"), script);

	r = system(script);
	if (r != 0)
	{
		char *errmsg = _("unable to initialize cluster for witness server");
		log_err("%s\n", errmsg);
		create_event_record(masterconn,
							&options,
							options.node,
							"witness_create",
							false,
							errmsg);
		PQfinish(masterconn);
		exit(ERR_BAD_CONFIG);
	}


	xsnprintf(buf, sizeof(buf), "%s/postgresql.conf", runtime_options.dest_dir);
	pg_conf = fopen(buf, "a");
	if (pg_conf == NULL)
	{
		PQExpBufferData errmsg;
		initPQExpBuffer(&errmsg);
		appendPQExpBuffer(&errmsg,
						  _("unable to open \"%s\" to add additional configuration items: %s\n"),
						  buf,
						  strerror(errno));
		log_err("%s\n", errmsg.data);

		create_event_record(masterconn,
							&options,
							options.node,
							"witness_create",
							false,
							errmsg.data);

		PQfinish(masterconn);
		exit(ERR_BAD_CONFIG);
	}

	xsnprintf(buf, sizeof(buf), "\n#Configuration added by %s\n", progname());
	fputs(buf, pg_conf);

	/*
	 * Attempt to extract a port number from the provided conninfo string.
	 */

	get_conninfo_value(options.conninfo, "port", witness_port);

	/*
	 * If not specified by the user, the default port for the witness server
	 * is 5499; this is intended to support running the witness server as
	 * a separate instance on a normal node server, rather than on its own
	 * dedicated server.
	 */
	if (!witness_port[0])
		strncpy(witness_port, WITNESS_DEFAULT_PORT, MAXLEN);

	xsnprintf(buf, sizeof(buf), "port = %s\n", witness_port);
	fputs(buf, pg_conf);

	xsnprintf(buf, sizeof(buf), "shared_preload_libraries = 'repmgr_funcs'\n");
	fputs(buf, pg_conf);

	xsnprintf(buf, sizeof(buf), "listen_addresses = '*'\n");
	fputs(buf, pg_conf);

	fclose(pg_conf);


	/* start new instance */
	if (*options.start_command)
	{
		maxlen_snprintf(script, "%s", options.start_command);
	}
	else
	{
		maxlen_snprintf(script, "%s %s -w -D %s start",
				        make_pg_path("pg_ctl"),
				        options.pg_ctl_options, runtime_options.dest_dir);
	}
	log_info(_("starting witness server: %s\n"), script);
	r = system(script);
	if (r != 0)
	{
		char *errmsg = _("unable to start witness server");
		log_err("%s\n", errmsg);

		create_event_record(masterconn,
							&options,
							options.node,
							"witness_create",
							false,
							errmsg);

		PQfinish(masterconn);
		exit(ERR_BAD_CONFIG);
	}


	/* check if we need to create a user */
	if (strcmp(repmgr_user, "postgres") != 0)
	{
		/* create required user; needs to be superuser to create untrusted
		 * language function in C */
		maxlen_snprintf(script, "%s -p %s --superuser --login %s-U %s %s",
						make_pg_path("createuser"),
						witness_port,
						runtime_options.witness_pwprompt ? "-P " : "",
						runtime_options.superuser,
						repmgr_user);
		log_info(_("creating user for witness db: %s.\n"), script);

		r = system(script);
		if (r != 0)
		{
			char *errmsg = _("unable to create user for witness server");
			log_err("%s\n", errmsg);

			create_event_record(masterconn,
								&options,
								options.node,
								"witness_create",
								false,
								errmsg);
			PQfinish(masterconn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/* check if we need to create a database */
	if (runtime_options.dbname[0] && strcmp(runtime_options.dbname,"postgres") != 0 && witness_port[0])
	{
		/* create required db */
		maxlen_snprintf(script, "%s -p %s -U %s --owner=%s %s",
						make_pg_path("createdb"),
						witness_port,
						runtime_options.superuser,
						repmgr_user,
						repmgr_db);
		log_info("creating database for witness db: %s.\n", script);

		r = system(script);
		if (r != 0)
		{
			char *errmsg = _("Unable to create database for witness server");
			log_err("%s\n", errmsg);

			create_event_record(masterconn,
								&options,
								options.node,
								"witness_create",
								false,
								errmsg);

			PQfinish(masterconn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/* Get the pg_hba.conf full path */
	success = get_pg_setting(masterconn, "hba_file", master_hba_file);

	if (success == false)
	{
		char *errmsg = _("Unable to retrieve location of pg_hba.conf");
		log_err("%s\n", errmsg);

		create_event_record(masterconn,
							&options,
							options.node,
							"witness_create",
							false,
							errmsg);

		exit(ERR_DB_QUERY);
	}

	r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
						  master_hba_file, runtime_options.dest_dir, false, -1);
	if (r != 0)
	{
		char *errmsg = _("Unable to copy pg_hba.conf from master");
		log_err("%s\n", errmsg);

		create_event_record(masterconn,
							&options,
							options.node,
							"witness_create",
							false,
							errmsg);

		PQfinish(masterconn);
		exit(ERR_BAD_CONFIG);
	}

	/* reload witness server to activate the copied pg_hba.conf */
	maxlen_snprintf(script, "%s %s -w -D %s reload",
					make_pg_path("pg_ctl"),
					options.pg_ctl_options, runtime_options.dest_dir);
	log_info(_("reloading witness server configuration: %s"), script);
	r = system(script);
	if (r != 0)
	{
		char *errmsg = _("unable to reload witness server");
		log_err("%s\n", errmsg);

		create_event_record(masterconn,
							&options,
							options.node,
							"witness_create",
							false,
							errmsg);

		PQfinish(masterconn);
		exit(ERR_BAD_CONFIG);
	}

	/* Let do_witness_register() handle the rest */
	do_witness_register(masterconn);
}


static void
do_witness_register(PGconn *masterconn)
{
	PGconn	   *witnessconn;
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];

	char		repmgr_user[MAXLEN];
	char		repmgr_db[MAXLEN];

	bool		record_created;

	/*
	 * Extract the repmgr user and database names from the conninfo string
	 * provided in repmgr.conf
	 */
	get_conninfo_value(options.conninfo, "user", repmgr_user);
	get_conninfo_value(options.conninfo, "dbname", repmgr_db);

	param_set("user", repmgr_user);
	param_set("dbname", repmgr_db);

	/* masterconn will only be set when called from do_witness_create() */
	if (masterconn == NULL)
	{
		masterconn = establish_db_connection_by_params((const char**)param_keywords, (const char**)param_values, false);

		if (PQstatus(masterconn) != CONNECTION_OK)
		{
			/* No event logging possible here as we can't connect to the master */
			log_err(_("unable to connect to master\n"));
			exit(ERR_DB_CON);
		}
	}

	/* establish a connection to the witness, and create the schema */
	witnessconn = establish_db_connection(options.conninfo, false);

	if (PQstatus(witnessconn) != CONNECTION_OK)
	{
		create_event_record(masterconn,
							&options,
							options.node,
							"witness_create",
							false,
							_("Unable to connect to witness server"));
		PQfinish(masterconn);
		exit(ERR_BAD_CONFIG);
	}

	log_info(_("starting copy of configuration from master...\n"));

	begin_transaction(witnessconn);

	if (!create_schema(witnessconn))
	{
		rollback_transaction(witnessconn);
		create_event_record(masterconn,
							&options,
							options.node,
							"witness_create",
							false,
							_("Unable to create schema on witness"));
		PQfinish(masterconn);
		PQfinish(witnessconn);
		exit(ERR_BAD_CONFIG);
	}

	commit_transaction(witnessconn);

	/*
	 * Register new witness server on the primary
	 * Do this as late as possible to avoid having to delete
	 * the record if the server creation fails
	 */

	if (runtime_options.force)
	{
		bool node_record_deleted = delete_node_record(masterconn,
													  options.node,
													  "witness create");

		if (node_record_deleted == false)
		{
			PQfinish(masterconn);
			exit(ERR_BAD_CONFIG);
		}
	}

	record_created = create_node_record(masterconn,
										"witness create",
										options.node,
										"witness",
										NO_UPSTREAM_NODE,
										options.cluster_name,
										options.node_name,
										options.conninfo,
										options.priority,
										NULL,
										true);

	if (record_created == false)
	{
		create_event_record(masterconn,
							&options,
							options.node,
							"witness_create",
							false,
							"Unable to create witness node record on master");

		PQfinish(masterconn);
		exit(ERR_DB_QUERY);
	}


	/* copy configuration from master, only repl_nodes is needed */
	if (!witness_copy_node_records(masterconn, witnessconn, options.cluster_name))
	{
		create_event_record(masterconn,
							&options,
							options.node,
							"witness_create",
							false,
							_("Unable to copy configuration from master"));

		/*
		 * delete previously created witness node record
		 * XXX maybe set inactive?
		 */
		delete_node_record(masterconn,
						   options.node,
						   "witness create");

		PQfinish(masterconn);
		PQfinish(witnessconn);
		exit(ERR_BAD_CONFIG);
	}

	/* drop superuser powers if needed */
	if (strcmp(repmgr_user, "postgres") != 0)
	{
		sqlquery_snprintf(sqlquery, "ALTER ROLE %s NOSUPERUSER", repmgr_user);
		log_info(_("revoking superuser status on user %s: %s.\n"),
				   repmgr_user, sqlquery);

		log_debug(_("witness create: %s\n"), sqlquery);
		res = PQexec(witnessconn, sqlquery);
		if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			log_err(_("Unable to alter user privileges for user %s: %s\n"),
					repmgr_user,
					PQerrorMessage(witnessconn));
			PQfinish(masterconn);
			PQfinish(witnessconn);
			exit(ERR_DB_QUERY);
		}
	}

	/* Finished with the witness server */

	PQfinish(witnessconn);

	/* Log the event */
	create_event_record(masterconn,
						&options,
						options.node,
						"witness_create",
						true,
						NULL);

	PQfinish(masterconn);

	log_notice(_("configuration has been successfully copied to the witness\n"));
}

static void
do_witness_unregister(void)
{
	PGconn	   *conn;
	PGconn	   *master_conn;

	int 		target_node_id;
	t_node_info node_info = T_NODE_INFO_INITIALIZER;

	bool		node_record_deleted;


	log_info(_("connecting to witness database\n"));
	conn = establish_db_connection(options.conninfo, true);

	/* Check if there is a schema for this cluster */
	if (check_cluster_schema(conn) == false)
	{
		/* schema doesn't exist */
		log_err(_("schema '%s' doesn't exist.\n"), get_repmgr_schema());
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* check if there is a master in this cluster */
	log_info(_("connecting to master server\n"));
	master_conn = get_master_connection(conn, options.cluster_name,
										NULL, NULL);
	if (!master_conn)
	{
		log_err(_("Unable to connect to master server\n"));
		exit(ERR_BAD_CONFIG);
	}

	if (runtime_options.node)
		target_node_id = runtime_options.node;
	else
		target_node_id = options.node;

	/* Check node exists and is really a witness */

	if (!get_node_record(master_conn, options.cluster_name, target_node_id, &node_info))
	{
		log_err(_("No record found for node %i\n"), target_node_id);
		exit(ERR_BAD_CONFIG);
	}

	if (node_info.type != WITNESS)
	{
		log_err(_("Node %i is not a witness server\n"), target_node_id);
		exit(ERR_BAD_CONFIG);
	}

	log_info(_("unregistering the witness server\n"));
	node_record_deleted = delete_node_record(master_conn,
										     target_node_id,
											 "witness unregister");

	if (node_record_deleted == false)
	{
		PQfinish(master_conn);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Log the event */
	create_event_record(master_conn,
						&options,
						target_node_id,
						"witness_unregister",
						true,
						NULL);

	PQfinish(master_conn);
	PQfinish(conn);

	log_info(_("witness unregistration complete\n"));
	log_notice(_("witness node correctly unregistered for cluster %s with id %d (conninfo: %s)\n"),
			   options.cluster_name, target_node_id, options.conninfo);

	return;
}


static void
do_help(void)
{
	printf(_("%s: replication management tool for PostgreSQL\n"), progname());
	printf(_("\n"));
	printf(_("Usage:\n"));
	printf(_("  %s [OPTIONS] master  register\n"), progname());
	printf(_("  %s [OPTIONS] standby {register|unregister|clone|promote|follow|switchover}\n"),
		   progname());
	printf(_("  %s [OPTIONS] witness {create|unregister}\n"), progname());
	printf(_("  %s [OPTIONS] cluster {show|cleanup}\n"), progname());
	printf(_("\n"));
	printf(_("General options:\n"));
	printf(_("  -?, --help                          show this help, then exit\n"));
	printf(_("  -V, --version                       output version information, then exit\n"));
	printf(_("\n"));
	printf(_("Logging options:\n"));
	printf(_("  -L, --log-level                     set log level (overrides configuration file; default: NOTICE)\n"));
	printf(_("  -v, --verbose                       display additional log output (useful for debugging)\n"));
	printf(_("  -t, --terse                         don't display hints and other non-critical output\n"));
	printf(_("\n"));
	printf(_("Connection options:\n"));
	printf(_("  -d, --dbname=DBNAME                 database to connect to (default: \"%s\")\n"), runtime_options.dbname);
	printf(_("  -h, --host=HOSTNAME                 database server host"));
	if (runtime_options.host[0] != '\0')
		printf(_(" (default: \"%s\")"), runtime_options.host);
	printf(_("\n"));
	printf(_("  -p, --port=PORT                     database server port (default: \"%s\")\n"), runtime_options.masterport);
	printf(_("  -U, --username=USERNAME             database user name to connect as (default: \"%s\")\n"), runtime_options.username);
	printf(_("\n"));
	printf(_("General configuration options:\n"));
	printf(_("  -b, --pg_bindir=PATH                path to PostgreSQL binaries (optional)\n"));
	printf(_("  -D, --data-dir=DIR                  local directory where the files will be\n" \
			 "                                      copied to\n"));
	printf(_("  -f, --config-file=PATH              path to the configuration file\n"));
	printf(_("  -R, --remote-user=USERNAME          database server username for rsync (default: \"%s\")\n"), runtime_options.username);
	printf(_("  -F, --force                         force potentially dangerous operations to happen\n"));
	printf(_("  --check-upstream-config             verify upstream server configuration\n"));
	printf(_("\n"));
	printf(_("Command-specific configuration options:\n"));
	printf(_("  -c, --fast-checkpoint               (standby clone) force fast checkpoint\n"));
	printf(_("  -r, --rsync-only                    (standby clone) use only rsync, not pg_basebackup\n"));
	printf(_("  --recovery-min-apply-delay=VALUE    (standby clone, follow) set recovery_min_apply_delay\n" \
			 "                                        in recovery.conf (PostgreSQL 9.4 and later)\n"));
	printf(_("  --ignore-external-config-files      (standby clone) don't copy configuration files located\n" \
			 "                                        outside the data directory when cloning a standby\n"));
	printf(_("  -w, --wal-keep-segments=VALUE       (standby clone) minimum value for the GUC\n" \
			 "                                        wal_keep_segments (default: %s)\n"), DEFAULT_WAL_KEEP_SEGMENTS);
	printf(_("  -W, --wait                          (standby follow) wait for a master to appear\n"));
	printf(_("  -m, --mode                          (standby switchover) shutdown mode (\"fast\" - default, \"smart\" or \"immediate\")\n"));
	printf(_("  -C, --remote-config-file            (standby switchover) path to the configuration file on\n" \
			 "                                        the current master\n"));
	printf(_("  --pg_rewind[=VALUE]                 (standby switchover) 9.3/9.4 only - use pg_rewind if available,\n" \
			 "                                        optionally providing a path to the binary\n"));
	printf(_("  -k, --keep-history=VALUE            (cluster cleanup) retain indicated number of days of history (default: 0)\n"));
	printf(_("  --csv                               (cluster show) output in CSV mode (0 = master, 1 = standby, -1 = down)\n"));
/*	printf(_("  --initdb-no-pwprompt                (witness server) no superuser password prompt during initdb\n"));*/
	printf(_("  -P, --pwprompt                      (witness server) prompt for password when creating users\n"));
	printf(_("  -S, --superuser=USERNAME            (witness server) superuser username for witness database\n" \
			 "                                        (default: postgres)\n"));
	printf(_("\n"));
	printf(_("%s performs the following node management tasks:\n"), progname());
	printf(_("\n"));
	printf(_("COMMANDS:\n"));
	printf(_(" master  register      - registers the master in a cluster\n"));
	printf(_(" standby clone [node]  - creates a new standby\n"));
	printf(_(" standby register      - registers a standby in a cluster\n"));
	printf(_(" standby unregister    - unregisters a standby\n"));
	printf(_(" standby promote       - promotes a specific standby to master\n"));
	printf(_(" standby follow        - makes standby follow a new master\n"));
	printf(_(" standby switchover    - switch this standby with the current master\n"));
	printf(_(" witness create        - creates a new witness server\n"));
	printf(_(" witness unregister    - unregisters a witness server\n"));
	printf(_(" cluster show          - displays information about cluster nodes\n"));
	printf(_(" cluster cleanup       - prunes or truncates monitoring history\n" \
			 "                         (monitoring history creation requires repmgrd\n" \
			 "                         with --monitoring-history option)\n"));
}


/*
 * Creates a recovery file for a standby.
 */
static bool
create_recovery_file(const char *data_dir, PGconn *primary_conn)
{
	FILE	   *recovery_file;
	char		recovery_file_path[MAXLEN];
	char		line[MAXLEN];

	maxlen_snprintf(recovery_file_path, "%s/%s", data_dir, RECOVERY_COMMAND_FILE);

	recovery_file = fopen(recovery_file_path, "w");
	if (recovery_file == NULL)
	{
		log_err(_("unable to create recovery.conf file at '%s'\n"), recovery_file_path);
		return false;
	}

	log_debug(_("create_recovery_file(): creating '%s'...\n"), recovery_file_path);

	/* standby_mode = 'on' */
	maxlen_snprintf(line, "standby_mode = 'on'\n");

	if (write_recovery_file_line(recovery_file, recovery_file_path, line) == false)
		return false;

	log_debug(_("recovery.conf: %s"), line);

	/* primary_conninfo = '...' */
	write_primary_conninfo(line, primary_conn);

	if (write_recovery_file_line(recovery_file, recovery_file_path, line) == false)
		return false;

	log_debug(_("recovery.conf: %s"), line);

	/* recovery_target_timeline = 'latest' */
	maxlen_snprintf(line, "recovery_target_timeline = 'latest'\n");

	if (write_recovery_file_line(recovery_file, recovery_file_path, line) == false)
		return false;

	log_debug(_("recovery.conf: %s"), line);

	/* recovery_min_apply_delay = ... (optional) */
	if (*runtime_options.recovery_min_apply_delay)
	{
		maxlen_snprintf(line, "recovery_min_apply_delay = %s\n",
						runtime_options.recovery_min_apply_delay);
		if (write_recovery_file_line(recovery_file, recovery_file_path, line) == false)
			return false;

		log_debug(_("recovery.conf: %s"), line);
	}

	/* primary_slot_name = '...' (optional, for 9.4 and later) */
	if (options.use_replication_slots)
	{
		maxlen_snprintf(line, "primary_slot_name = %s\n",
						repmgr_slot_name);
		if (write_recovery_file_line(recovery_file, recovery_file_path, line) == false)
			return false;

		log_debug(_("recovery.conf: %s"), line);
	}

	/* If restore_command is set, we use it as restore_command in recovery.conf */
	if (strcmp(options.restore_command, "") != 0)
	{
		maxlen_snprintf(line, "restore_command = '%s'\n",
						options.restore_command);
		if (write_recovery_file_line(recovery_file, recovery_file_path, line) == false)
		        return false;
		log_debug(_("recovery.conf: %s"), line);
	}
	fclose(recovery_file);

	return true;
}


static bool
write_recovery_file_line(FILE *recovery_file, char *recovery_file_path, char *line)
{
	if (fputs(line, recovery_file) == EOF)
	{
		log_err(_("unable to write to recovery file at '%s'\n"), recovery_file_path);
		fclose(recovery_file);
		return false;
	}

	return true;
}


static int
test_ssh_connection(char *host, char *remote_user)
{
	char		script[MAXLEN];
	int			r = 1, i;

	/* On some OS, true is located in a different place than in Linux
	 * we have to try them all until all alternatives are gone or we
	 * found `true' because the target OS may differ from the source
	 * OS
	 */
	const char *truebin_paths[] = {
		"/bin/true",
		"/usr/bin/true",
		NULL
	};

	/* Check if we have ssh connectivity to host before trying to rsync */
	for(i = 0; truebin_paths[i] && r != 0; ++i)
	{
		if (!remote_user[0])
			maxlen_snprintf(script, "ssh -o Batchmode=yes %s %s %s 2>/dev/null",
							options.ssh_options, host, truebin_paths[i]);
		else
			maxlen_snprintf(script, "ssh -o Batchmode=yes %s %s -l %s %s 2>/dev/null",
							options.ssh_options, host, remote_user,
							truebin_paths[i]);

		log_verbose(LOG_DEBUG, _("test_ssh_connection(): executing %s\n"), script);
		r = system(script);
	}

	if (r != 0)
		log_info(_("unable to connect to remote host (%s)\n"), host);
	return r;
}


static int
copy_remote_files(char *host, char *remote_user, char *remote_path,
				  char *local_path, bool is_directory, int server_version_num)
{
	PQExpBufferData 	rsync_flags;
	char		script[MAXLEN];
	char		host_string[MAXLEN];
	int			r;

	initPQExpBuffer(&rsync_flags);

	if (*options.rsync_options == '\0')
	{
		appendPQExpBuffer(&rsync_flags, "%s",
						  "--archive --checksum --compress --progress --rsh=ssh");
	}
	else
	{
		appendPQExpBuffer(&rsync_flags, "%s",
						  options.rsync_options);
	}

	if (runtime_options.force)
	{
		appendPQExpBuffer(&rsync_flags, "%s",
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
	 * When copying the main PGDATA directory, certain files and contents
	 * of certain directories need to be excluded.
	 *
	 * See function 'sendDir()' in 'src/backend/replication/basebackup.c' -
	 * we're basically simulating what pg_basebackup does, but with rsync rather
	 * than the BASEBACKUP replication protocol command.
	 *
	 * *However* currently we'll always copy the contents of the 'pg_replslot'
	 * directory and delete later if appropriate.
	 */
	if (is_directory)
	{
		/* Files which we don't want */
		appendPQExpBuffer(&rsync_flags, "%s",
						  " --exclude=postmaster.pid --exclude=postmaster.opts --exclude=global/pg_control");

		appendPQExpBuffer(&rsync_flags, "%s",
						  " --exclude=recovery.conf --exclude=recovery.done");

		if (server_version_num >= 90400)
		{
			/*
			 * Ideally we'd use PG_AUTOCONF_FILENAME from utils/guc.h, but
			 * that has too many dependencies for a mere client program.
			 */
			appendPQExpBuffer(&rsync_flags, "%s",
							  " --exclude=postgresql.auto.conf.tmp");
		}

		/* Temporary files which we don't want, if they exist */
		appendPQExpBuffer(&rsync_flags, " --exclude=%s*",
						  PG_TEMP_FILE_PREFIX);

		/* Directories which we don't want */
		appendPQExpBuffer(&rsync_flags, "%s",
						  " --exclude=pg_xlog/* --exclude=pg_log/* --exclude=pg_stat_tmp/*");

		maxlen_snprintf(script, "rsync %s %s:%s/* %s",
						rsync_flags.data, host_string, remote_path, local_path);
	}
	else
	{
		maxlen_snprintf(script, "rsync %s %s:%s %s",
						rsync_flags.data, host_string, remote_path, local_path);
	}

	log_info(_("rsync command line: '%s'\n"), script);

	r = system(script);

	if (r != 0)
		log_err(_("unable to rsync from remote host (%s:%s)\n"),
				host_string, remote_path);

	return r;
}


static int
run_basebackup(const char *data_dir, int server_version)
{
	char				  script[MAXLEN];
	int					  r = 0;
	PQExpBufferData 	  params;
	TablespaceListCell   *cell;
	t_basebackup_options  backup_options = T_BASEBACKUP_OPTIONS_INITIALIZER;

	/*
	 * Parse the pg_basebackup_options provided in repmgr.conf - we'll want
	 * to check later whether certain options were set by the user
	 */
	parse_pg_basebackup_options(options.pg_basebackup_options, &backup_options);

	/* Create pg_basebackup command line options */

	initPQExpBuffer(&params);

	appendPQExpBuffer(&params, " -D %s", data_dir);

	/*
	 * conninfo string provided - pass it to pg_basebackup as the -d option
	 * (pg_basebackup doesn't require or want a database name, but for
	 * consistency with other applications accepts a conninfo string
	 * under -d/--dbname)
	 */
	if (conninfo_provided == true)
	{
		appendPQExpBuffer(&params, " -d '%s'", runtime_options.dbname);
	}

	/*
	 * Connection parameters not passed to repmgr as conninfo string - provide
	 * them individually to pg_basebackup (-d/--dbname not required)
	 */
	else
	{
		if (strlen(runtime_options.host))
		{
			appendPQExpBuffer(&params, " -h %s", runtime_options.host);
		}

		if (strlen(runtime_options.masterport))
		{
			appendPQExpBuffer(&params, " -p %s", runtime_options.masterport);
		}

		if (strlen(runtime_options.username))
		{
			appendPQExpBuffer(&params, " -U %s", runtime_options.username);
		}
	}

	if (runtime_options.fast_checkpoint) {
		appendPQExpBuffer(&params, " -c fast");
	}

	if (options.tablespace_mapping.head != NULL)
	{
		for (cell = options.tablespace_mapping.head; cell; cell = cell->next)
		{
			appendPQExpBuffer(&params, " -T %s=%s", cell->old_dir, cell->new_dir);
		}
	}

	/*
	 * To ensure we have all the WALs needed during basebackup execution we stream
	 * them as the backup is taking place.
	 *
	 * From 9.6, if replication slots are in use, we'll have previously
	 * created a slot with reserved LSN, and will stream from that slot to avoid
	 * WAL buildup on the master using the -S/--slot, which requires -X/--xlog-method=stream
	 */
	if (!strlen(backup_options.xlog_method))
	{
		appendPQExpBuffer(&params, " -X stream");
	}

	/*
	 * From 9.6, pg_basebackup accepts -S/--slot, which forces WAL streaming to use
	 * the specified replication slot. If replication slot usage is specified, the
	 * slot will already have been created.
	 *
	 * NOTE: currently there's no way of disabling the --slot option while using
	 *   --xlog-method=stream - it's hard to imagine a use case for this, so no
	 *   provision has been made for doing it.
	 *
	 * NOTE:
	 *   It's possible to set 'pg_basebackup_options' with an invalid combination
	 *   of values for --xlog-method and --slot - we're not checking that, just that
	 *   we're not overriding any user-supplied values
	 */
	if (server_version >= 90600 && options.use_replication_slots)
	{
		bool slot_add = true;

		/*
		 * Check whether 'pg_basebackup_options' in repmgr.conf has the --slot option set,
		 * or if --xlog-method is set to a value other than "stream" (in which case we can't
		 * use --slot).
		 */
		if(strlen(backup_options.slot) || strcmp(backup_options.xlog_method, "stream") != 0) {
			slot_add = false;
		}

		if (slot_add == true)
		{
			appendPQExpBuffer(&params, " -S %s", repmgr_slot_name_ptr);
		}
	}

	maxlen_snprintf(script,
					"%s -l \"repmgr base backup\" %s %s",
					make_pg_path("pg_basebackup"),
					params.data,
					options.pg_basebackup_options);

	termPQExpBuffer(&params);

	log_info(_("executing: '%s'\n"), script);

	/*
	 * As of 9.4, pg_basebackup only ever returns 0 or 1
	 */

	r = system(script);

	return r;
}


/*
 * Check for useless or conflicting parameters, and also whether a
 * configuration file is required.
 */
static void
check_parameters_for_action(const int action)
{
	switch (action)
	{
		case MASTER_REGISTER:

			/*
			 * To register a master we only need the repmgr.conf all other
			 * parameters are at least useless and could be confusing so
			 * reject them
			 */
			if (connection_param_provided)
			{
				item_list_append(&cli_warnings, _("master connection parameters not required when executing MASTER REGISTER"));
			}
			if (runtime_options.dest_dir[0])
			{
				item_list_append(&cli_warnings, _("destination directory not required when executing MASTER REGISTER"));
			}
			break;

		case STANDBY_REGISTER:

			/*
			 * To register a standby we only need the repmgr.conf we don't
			 * need connection parameters to the master because we can detect
			 * the master in repl_nodes
			 */
			if (connection_param_provided)
			{
				item_list_append(&cli_warnings, _("master connection parameters not required when executing STANDBY REGISTER"));
			}
			if (runtime_options.dest_dir[0])
			{
				item_list_append(&cli_warnings, _("destination directory not required when executing STANDBY REGISTER"));
			}
			break;

		case STANDBY_UNREGISTER:

			/*
			 * To unregister a standby we only need the repmgr.conf we don't
			 * need connection parameters to the master because we can detect
			 * the master in repl_nodes
			 */
			if (connection_param_provided)
			{
				item_list_append(&cli_warnings, _("master connection parameters not required when executing STANDBY UNREGISTER"));
			}
			if (runtime_options.dest_dir[0])
			{
				item_list_append(&cli_warnings, _("destination directory not required when executing STANDBY UNREGISTER"));
			}
			break;

		case STANDBY_PROMOTE:

			/*
			 * To promote a standby we only need the repmgr.conf we don't want
			 * connection parameters to the master because we will try to
			 * detect the master in repl_nodes if we can't find it then the
			 * promote action will be cancelled
			 */
			if (connection_param_provided)
			{
				item_list_append(&cli_warnings, _("master connection parameters not required when executing STANDBY PROMOTE"));
			}
			if (runtime_options.dest_dir[0])
			{
				item_list_append(&cli_warnings, _("destination directory not required when executing STANDBY PROMOTE"));
			}
			break;

		case STANDBY_FOLLOW:

			/*
			 * To make a standby follow a master we only need the repmgr.conf
			 * we don't want connection parameters to the new master because
			 * we will try to detect the master in repl_nodes; if we can't find
			 * it then the follow action will be cancelled
			 */

			if (runtime_options.host[0] || runtime_options.dest_dir[0])
			{
				if (!runtime_options.host[0])
				{
					item_list_append(&cli_errors, _("master hostname (-h/--host) required when executing STANDBY FOLLOW with -D/--data-dir option"));
				}

				if (host_param_provided && !runtime_options.dest_dir[0])
				{
					item_list_append(&cli_errors, _("local data directory (-D/--data-dir) required when executing STANDBY FOLLOW with -h/--host option"));
				}
			}
			break;

		case STANDBY_CLONE:

			/*
			 * Explicitly require connection information for standby clone -
			 * this will be written into `recovery.conf` so it's important to
			 * specify it explicitly
			 */

			if (strcmp(runtime_options.host, "") == 0)
			{
				item_list_append(&cli_errors, _("master hostname (-h/--host) required when executing STANDBY CLONE"));
			}

			if (runtime_options.fast_checkpoint && runtime_options.rsync_only)
			{
				item_list_append(&cli_warnings, _("-c/--fast-checkpoint has no effect when using -r/--rsync-only"));
			}
			config_file_required = false;
			break;
		case STANDBY_SWITCHOVER:
			/* allow all parameters to be supplied */
			break;

		case STANDBY_ARCHIVE_CONFIG:
			if (strcmp(runtime_options.config_archive_dir, "") == 0)
			{
				item_list_append(&cli_errors, _("--config-archive-dir required when executing STANDBY ARCHIVE_CONFIG"));
			}
			break;

		case STANDBY_RESTORE_CONFIG:
			if (strcmp(runtime_options.config_archive_dir, "") == 0)
			{
				item_list_append(&cli_errors, _("--config-archive-dir required when executing STANDBY RESTORE_CONFIG"));
			}

			if (strcmp(runtime_options.dest_dir, "") == 0)
			{
				item_list_append(&cli_errors, _("-D/--data-dir required when executing STANDBY RESTORE_CONFIG"));
			}

			config_file_required = false;
			break;

		case WITNESS_CREATE:
			/* Require data directory */
			if (strcmp(runtime_options.dest_dir, "") == 0)
			{
				item_list_append(&cli_errors, _("-D/--data-dir required when executing WITNESS CREATE"));
			}
			/* allow all parameters to be supplied */
			break;

		case CLUSTER_SHOW:
			/* allow all parameters to be supplied */
			break;

		case CLUSTER_CLEANUP:
			/* allow all parameters to be supplied */
			break;
	}

	/* Warn about parameters which apply to STANDBY CLONE only */
	if (action != STANDBY_CLONE)
	{
		if (runtime_options.fast_checkpoint)
		{
			item_list_append(&cli_warnings, _("-c/--fast-checkpoint can only be used when executing STANDBY CLONE"));
		}

		if (runtime_options.ignore_external_config_files)
		{
			item_list_append(&cli_warnings, _("--ignore-external-config-files can only be used when executing STANDBY CLONE"));
		}

		if (*runtime_options.recovery_min_apply_delay)
		{
			item_list_append(&cli_warnings, _("--recovery-min-apply-delay can only be used when executing STANDBY CLONE"));
		}

		if (runtime_options.rsync_only)
		{
			item_list_append(&cli_warnings, _("-r/--rsync-only can only be used when executing STANDBY CLONE"));
		}

		if (wal_keep_segments_used)
		{
			item_list_append(&cli_warnings, _("-w/--wal-keep-segments can only be used when executing STANDBY CLONE"));
		}
	}

    /* Warn about parameters which apply to STANDBY SWITCHOVER only */
	if (action != STANDBY_SWITCHOVER)
	{
		if (pg_rewind_supplied == true)
		{
			item_list_append(&cli_warnings, _("--pg_rewind can only be used when executing STANDBY SWITCHOVER"));
		}
	}

	if (action != WITNESS_UNREGISTER)
	{
		if (runtime_options.node)
		{
			item_list_append(&cli_warnings, _("--node can only be supplied when executing WITNESS UNREGISTER"));
		}
	}

    /* Warn about parameters which apply to CLUSTER SHOW only */
	if (action != CLUSTER_SHOW)
	{
		if (runtime_options.csv_mode)
		{
			item_list_append(&cli_warnings, _("--csv can only be used when executing CLUSTER SHOW"));
		}
	}


	return;
}


/* The caller should wrap this function in a transaction */
static bool
create_schema(PGconn *conn)
{
	char		sqlquery[QUERY_STR_LEN];
	PGresult   *res;

	/* create schema */
	sqlquery_snprintf(sqlquery, "CREATE SCHEMA %s", get_repmgr_schema_quoted(conn));
	log_debug(_("master register: %s\n"), sqlquery);
	res = PQexec(conn, sqlquery);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("unable to create the schema %s: %s\n"),
				get_repmgr_schema(), PQerrorMessage(conn));

		if (res != NULL)
			PQclear(res);

		return false;
	}
	PQclear(res);


	/* create functions */

	/*
	 * to avoid confusion of the time_lag field and provide a consistent UI we
	 * use these functions for providing the latest update timestamp
	 */
	sqlquery_snprintf(sqlquery,
					  "CREATE FUNCTION %s.repmgr_update_last_updated() "
					  "  RETURNS TIMESTAMP WITH TIME ZONE "
					  "  AS '$libdir/repmgr_funcs', 'repmgr_update_last_updated' "
					  "  LANGUAGE C STRICT ",
					  get_repmgr_schema_quoted(conn));

	res = PQexec(conn, sqlquery);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("unable to create the function repmgr_update_last_updated: %s\n"),
				PQerrorMessage(conn));

		if (res != NULL)
			PQclear(res);

		return false;
	}
	PQclear(res);


	sqlquery_snprintf(sqlquery,
					  "CREATE FUNCTION %s.repmgr_get_last_updated() "
					  "  RETURNS TIMESTAMP WITH TIME ZONE "
					  "  AS '$libdir/repmgr_funcs', 'repmgr_get_last_updated' "
					  "  LANGUAGE C STRICT ",
					  get_repmgr_schema_quoted(conn));

	res = PQexec(conn, sqlquery);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("unable to create the function repmgr_get_last_updated: %s\n"),
				PQerrorMessage(conn));

		if (res != NULL)
			PQclear(res);

		return false;
	}
	PQclear(res);


	/* Create tables */

	/* CREATE TABLE repl_nodes */
	sqlquery_snprintf(sqlquery,
					  "CREATE TABLE %s.repl_nodes (     "
					  "  id               INTEGER PRIMARY KEY, "
					  "  type             TEXT    NOT NULL CHECK (type IN('master','standby','witness')), "
					  "  upstream_node_id INTEGER NULL REFERENCES %s.repl_nodes (id) DEFERRABLE, "
					  "  cluster          TEXT    NOT NULL, "
					  "  name             TEXT    NOT NULL, "
					  "  conninfo         TEXT    NOT NULL, "
					  "  slot_name        TEXT    NULL, "
					  "  priority         INTEGER NOT NULL, "
					  "  active           BOOLEAN NOT NULL DEFAULT TRUE )",
					  get_repmgr_schema_quoted(conn),
					  get_repmgr_schema_quoted(conn));

	log_debug(_("master register: %s\n"), sqlquery);
	res = PQexec(conn, sqlquery);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("unable to create table '%s.repl_nodes': %s\n"),
				get_repmgr_schema_quoted(conn), PQerrorMessage(conn));

		if (res != NULL)
			PQclear(res);

		return false;
	}
	PQclear(res);

	/* CREATE TABLE repl_monitor */

	sqlquery_snprintf(sqlquery,
					  "CREATE TABLE %s.repl_monitor ( "
					  "  primary_node                   INTEGER NOT NULL, "
					  "  standby_node                   INTEGER NOT NULL, "
					  "  last_monitor_time              TIMESTAMP WITH TIME ZONE NOT NULL, "
					  "  last_apply_time                TIMESTAMP WITH TIME ZONE, "
					  "  last_wal_primary_location      TEXT NOT NULL,   "
					  "  last_wal_standby_location      TEXT,  "
					  "  replication_lag                BIGINT NOT NULL, "
					  "  apply_lag                      BIGINT NOT NULL) ",
					  get_repmgr_schema_quoted(conn));
	log_debug(_("master register: %s\n"), sqlquery);
	res = PQexec(conn, sqlquery);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("unable to create table '%s.repl_monitor': %s\n"),
				get_repmgr_schema_quoted(conn), PQerrorMessage(conn));

		if (res != NULL)
			PQclear(res);

		return false;
	}
	PQclear(res);

	/* CREATE TABLE repl_events */

	sqlquery_snprintf(sqlquery,
					  "CREATE TABLE %s.repl_events (     "
					  "  node_id          INTEGER NOT NULL, "
					  "  event            TEXT NOT NULL, "
					  "  successful       BOOLEAN NOT NULL DEFAULT TRUE, "
					  "  event_timestamp  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP, "
					  "  details          TEXT NULL "
					  " ) ",
					  get_repmgr_schema_quoted(conn));

	log_debug(_("master register: %s\n"), sqlquery);
	res = PQexec(conn, sqlquery);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("unable to create table '%s.repl_events': %s\n"),
				get_repmgr_schema_quoted(conn), PQerrorMessage(conn));

		if (res != NULL)
			PQclear(res);

		return false;
	}
	PQclear(res);

	/* CREATE VIEW repl_status  */
	sqlquery_snprintf(sqlquery,
					  "CREATE VIEW %s.repl_status AS "
					  "  SELECT m.primary_node, m.standby_node, n.name AS standby_name, "
					  "         n.type AS node_type, n.active, last_monitor_time, "
					  "         CASE WHEN n.type='standby' THEN m.last_wal_primary_location ELSE NULL END AS last_wal_primary_location, "
					  "         m.last_wal_standby_location, "
					  "         CASE WHEN n.type='standby' THEN pg_size_pretty(m.replication_lag) ELSE NULL END AS replication_lag, "
					  "         CASE WHEN n.type='standby' THEN age(now(), m.last_apply_time) ELSE NULL END AS replication_time_lag, "
					  "         CASE WHEN n.type='standby' THEN pg_size_pretty(m.apply_lag) ELSE NULL END AS apply_lag, "
					  "         age(now(), CASE WHEN pg_is_in_recovery() THEN %s.repmgr_get_last_updated() ELSE m.last_monitor_time END) AS communication_time_lag "
					  "    FROM %s.repl_monitor m "
					  "    JOIN %s.repl_nodes n ON m.standby_node = n.id "
					  "   WHERE (m.standby_node, m.last_monitor_time) IN ( "
					  "                 SELECT m1.standby_node, MAX(m1.last_monitor_time) "
					  "                  FROM %s.repl_monitor m1 GROUP BY 1 "
					  "            )",
					  get_repmgr_schema_quoted(conn),
					  get_repmgr_schema_quoted(conn),
					  get_repmgr_schema_quoted(conn),
					  get_repmgr_schema_quoted(conn),
					  get_repmgr_schema_quoted(conn));
	log_debug(_("master register: %s\n"), sqlquery);

	res = PQexec(conn, sqlquery);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("unable to create view %s.repl_status: %s\n"),
				get_repmgr_schema_quoted(conn), PQerrorMessage(conn));

		if (res != NULL)
			PQclear(res);

		return false;
	}
	PQclear(res);


	/* an index to improve performance of the view */
	sqlquery_snprintf(sqlquery,
					  "CREATE INDEX idx_repl_status_sort "
					  "    ON %s.repl_monitor (last_monitor_time, standby_node) ",
					  get_repmgr_schema_quoted(conn));

	log_debug(_("master register: %s\n"), sqlquery);
	res = PQexec(conn, sqlquery);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("unable to create index 'idx_repl_status_sort' on '%s.repl_monitor': %s\n"),
				get_repmgr_schema_quoted(conn), PQerrorMessage(conn));

		if (res != NULL)
			PQclear(res);

		return false;
	}
	PQclear(res);


	/* CREATE VIEW repl_show_nodes  */
	sqlquery_snprintf(sqlquery,
					  "CREATE VIEW %s.repl_show_nodes AS "
			                  "SELECT rn.id, rn.conninfo, rn.type, rn.name, rn.cluster,"
			                  "  rn.priority, rn.active, sq.name AS upstream_node_name"
			                  "  FROM %s.repl_nodes as rn"
			                  "  LEFT JOIN %s.repl_nodes AS sq"
			                  "    ON sq.id=rn.upstream_node_id",
			  get_repmgr_schema_quoted(conn),
			  get_repmgr_schema_quoted(conn),
			  get_repmgr_schema_quoted(conn));

	log_debug(_("master register: %s\n"), sqlquery);

	res = PQexec(conn, sqlquery);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("unable to create view %s.repl_show_nodes: %s\n"),
				get_repmgr_schema_quoted(conn), PQerrorMessage(conn));

		if (res != NULL)
			PQclear(res);

		return false;
	}
	PQclear(res);


	/*
	 * XXX Here we MUST try to load the repmgr_function.sql not hardcode it
	 * here
	 */
	sqlquery_snprintf(sqlquery,
					  "CREATE OR REPLACE FUNCTION %s.repmgr_update_standby_location(text) "
					  "  RETURNS boolean "
					  "  AS '$libdir/repmgr_funcs', 'repmgr_update_standby_location' "
					  "  LANGUAGE C STRICT ",
					  get_repmgr_schema_quoted(conn));

	res = PQexec(conn, sqlquery);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "Cannot create the function repmgr_update_standby_location: %s\n",
				PQerrorMessage(conn));

		if (res != NULL)
			PQclear(res);

		return false;
	}
	PQclear(res);

	sqlquery_snprintf(sqlquery,
					  "CREATE OR REPLACE FUNCTION %s.repmgr_get_last_standby_location() "
					  "  RETURNS text "
					  "  AS '$libdir/repmgr_funcs', 'repmgr_get_last_standby_location' "
					  "  LANGUAGE C STRICT ",
					  get_repmgr_schema_quoted(conn));

	res = PQexec(conn, sqlquery);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "Cannot create the function repmgr_get_last_standby_location: %s\n",
				PQerrorMessage(conn));

		if (res != NULL)
			PQclear(res);

		return false;
	}
	PQclear(res);

	return true;
}


static void
write_primary_conninfo(char *line, PGconn *primary_conn)
{
	PQconninfoOption *connOptions;
	PQconninfoOption *option;
	PQExpBufferData conninfo_buf;
	bool application_name_provided = false;

	connOptions = PQconninfo(primary_conn);

	initPQExpBuffer(&conninfo_buf);

	for (option = connOptions; option && option->keyword; option++)
	{
		/*
		 * Skip empty settings and ones which don't make any sense in
		 * recovery.conf
		 */
		if (strcmp(option->keyword, "dbname") == 0 ||
		    strcmp(option->keyword, "replication") == 0 ||
		    (option->val == NULL) ||
		    (option->val != NULL && option->val[0] == '\0'))
			continue;

		if (conninfo_buf.len != 0)
			appendPQExpBufferChar(&conninfo_buf, ' ');

		if (strcmp(option->keyword, "application_name") == 0)
			application_name_provided = true;

		/* XXX escape option->val */
		appendPQExpBuffer(&conninfo_buf, "%s=%s", option->keyword, option->val);
	}

	/* `application_name` not provided - default to repmgr node name */
	if (application_name_provided == false)
		appendPQExpBuffer(&conninfo_buf, " application_name=%s", options.node_name);

	maxlen_snprintf(line, "primary_conninfo = '%s'\n", conninfo_buf.data);
}


/**
 * check_server_version()
 *
 * Verify that the server is MIN_SUPPORTED_VERSION_NUM or later
 *
 * PGconn *conn:
 *   the connection to check
 *
 * char *server_type:
 *   either "master" or "standby"; used to format error message
 *
 * bool exit_on_error:
 *   exit if reported server version is too low; optional to enable some callers
 *   to perform additional cleanup
 *
 * char *server_version_string
 *   passed to get_server_version(), which will place the human-readble
 *   server version string there (e.g. "9.4.0")
 */
static int
check_server_version(PGconn *conn, char *server_type, bool exit_on_error, char *server_version_string)
{
	int			server_version_num = 0;

	server_version_num = get_server_version(conn, server_version_string);
	if (server_version_num < MIN_SUPPORTED_VERSION_NUM)
	{
		if (server_version_num > 0)
			log_err(_("%s requires %s to be PostgreSQL %s or later\n"),
					progname(),
					server_type,
					MIN_SUPPORTED_VERSION
				);

		if (exit_on_error == true)
		{
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		return -1;
	}

	return server_version_num;
}


/*
 * check_master_standby_version_match()
 *
 * Check server versions of supplied connections are compatible for
 * replication purposes.
 *
 * Exits on error.
 */
static void
check_master_standby_version_match(PGconn *conn, PGconn *master_conn)
{
	char		standby_version[MAXVERSIONSTR];
	int			standby_version_num = 0;

	char		master_version[MAXVERSIONSTR];
	int			master_version_num = 0;

	standby_version_num = check_server_version(conn, "standby", true, standby_version);

	/* Verify that master is a supported server version */
	master_version_num = check_server_version(conn, "master", false, master_version);
	if (master_version_num < 0)
	{
		PQfinish(conn);
		PQfinish(master_conn);
		exit(ERR_BAD_CONFIG);
	}

	/* master and standby version should match */
	if ((master_version_num / 100) != (standby_version_num / 100))
	{
		PQfinish(conn);
		PQfinish(master_conn);
		log_err(_("PostgreSQL versions on master (%s) and standby (%s) must match.\n"),
				master_version, standby_version);
		exit(ERR_BAD_CONFIG);
	}
}


/*
 * check_upstream_config()
 *
 * Perform sanity check on upstream server configuration
 *
 * TODO:
 *  - check replication connection is possble
 *  - check user is qualified to perform base backup
 */

static bool
check_upstream_config(PGconn *conn, int server_version_num, bool exit_on_error)
{
	int			i;
	bool		config_ok = true;
	char	   *wal_error_message = NULL;

	/* Check that WAL level is set correctly */
	if (server_version_num < 90400)
	{
		i = guc_set(conn, "wal_level", "=", "hot_standby");
		wal_error_message = _("parameter 'wal_level' must be set to 'hot_standby'");
	}
	else
	{
		char *levels_pre96[] = {
			"hot_standby",
			"logical",
			NULL,
		};

		/*
		 * Note that in 9.6+, "hot_standby" and "archive" are accepted as aliases
		 * for "replica", but current_setting() will of course always return "replica"
		 */
		char *levels_96plus[] = {
			"replica",
			"logical",
			NULL,
		};

		char **levels;
		int j = 0;

		if (server_version_num < 90600)
		{
			levels = (char **)levels_pre96;
			wal_error_message = _("parameter 'wal_level' must be set to 'hot_standby' or 'logical'");
		}
		else
		{
			levels = (char **)levels_96plus;
			wal_error_message = _("parameter 'wal_level' must be set to 'replica' or 'logical'");
		}

		do
		{
			i = guc_set(conn, "wal_level", "=", levels[j]);
			if (i)
			{
				break;
			}
			j++;
		} while (levels[j] != NULL);
	}

	if (i == 0 || i == -1)
	{
		if (i == 0)
			log_err("%s\n",
					wal_error_message);

		if (exit_on_error == true)
		{
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		config_ok = false;
	}

	if (options.use_replication_slots)
	{
		/* Does the server support physical replication slots? */
		if (server_version_num < 90400)
		{
			log_err(_("server version must be 9.4 or later to enable replication slots\n"));

			if (exit_on_error == true)
			{
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}

			config_ok = false;
		}
		/* Server is 9.4 or greater - non-zero `max_replication_slots` required */
		else
		{
			i = guc_set_typed(conn, "max_replication_slots", ">",
							  "0", "integer");
			if (i == 0 || i == -1)
			{
				if (i == 0)
				{
					log_err(_("parameter 'max_replication_slots' must be set to at least 1 to enable replication slots\n"));
					log_hint(_("'max_replication_slots' should be set to at least the number of expected standbys\n"));
					if (exit_on_error == true)
					{
						PQfinish(conn);
						exit(ERR_BAD_CONFIG);
					}

					config_ok = false;
				}
			}
		}

	}
	/*
	 * physical replication slots not available or not requested -
	 * ensure some reasonably high value set for `wal_keep_segments`
	 */
	else
	{
		i = guc_set_typed(conn, "wal_keep_segments", ">=",
						  runtime_options.wal_keep_segments, "integer");
		if (i == 0 || i == -1)
		{
			if (i == 0)
			{
				log_err(_("parameter 'wal_keep_segments' must be be set to %s or greater (see the '-w' option or edit the postgresql.conf of the upstream server.)\n"),
						runtime_options.wal_keep_segments);
				if (server_version_num >= 90400)
				{
					log_hint(_("in PostgreSQL 9.4 and later, replication slots can be used, which "
							   "do not require 'wal_keep_segments' to be set to a high value "
							   "(set parameter 'use_replication_slots' in the configuration file to enable)\n"
								 ));
				}
			}

			if (exit_on_error == true)
			{
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}

			config_ok = false;
		}
	}


	/*
	 * If archive_mode is enabled, check that 'archive_command' is non empty
	 * (however it's not practical to check that it actually represents a valid
	 * command).
	 *
	 * From PostgreSQL 9.5, archive_mode can be one of 'off', 'on' or 'always'
	 * so for ease of backwards compatibility, rather than explicitly check for an
	 * enabled mode, check that it's not "off".
	 */

	if (guc_set(conn, "archive_mode", "!=", "off"))
	{
		i = guc_set(conn, "archive_command", "!=", "");

		if (i == 0 || i == -1)
		{
			if (i == 0)
				log_err(_("parameter 'archive_command' must be set to a valid command\n"));

			if (exit_on_error == true)
			{
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}

			config_ok = false;
		}
	}


	/*
	 * Check that 'hot_standby' is on. This isn't strictly necessary
	 * for the primary server, however the assumption is that we'll be
	 * cloning standbys and thus copying the primary configuration;
	 * this way the standby will be correctly configured by default.
	 */

	i = guc_set(conn, "hot_standby", "=", "on");
	if (i == 0 || i == -1)
	{
		if (i == 0)
			log_err(_("parameter 'hot_standby' must be set to 'on'\n"));

		if (exit_on_error == true)
		{
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		config_ok = false;
	}

	i = guc_set_typed(conn, "max_wal_senders", ">", "0", "integer");
	if (i == 0 || i == -1)
	{
		if (i == 0)
		{
			log_err(_("parameter 'max_wal_senders' must be set to be at least 1\n"));
			log_hint(_("'max_wal_senders' should be set to at least the number of expected standbys\n"));
		}

		if (exit_on_error == true)
		{
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		config_ok = false;
	}

	return config_ok;
}


static bool
update_node_record_set_master(PGconn *conn, int this_node_id)
{
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];

	log_debug(_("setting node %i as master and marking existing master as failed\n"), this_node_id);

	begin_transaction(conn);

	sqlquery_snprintf(sqlquery,
					  "  UPDATE %s.repl_nodes "
					  "     SET active = FALSE "
					  "   WHERE cluster = '%s' "
					  "     AND type = 'master' "
					  "     AND active IS TRUE ",
					  get_repmgr_schema_quoted(conn),
					  options.cluster_name);

	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("Unable to set old master node as inactive: %s\n"),
				PQerrorMessage(conn));
		PQclear(res);

		rollback_transaction(conn);
		return false;
	}

	PQclear(res);

	sqlquery_snprintf(sqlquery,
					  "  UPDATE %s.repl_nodes "
					  "     SET type = 'master', "
					  "         upstream_node_id = NULL "
					  "   WHERE cluster = '%s' "
					  "     AND id = %i ",
					  get_repmgr_schema_quoted(conn),
					  options.cluster_name,
					  this_node_id);

	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("Unable to set current node %i as active master: %s\n"),
				this_node_id,
				PQerrorMessage(conn));
		PQclear(res);

		PQexec(conn, "ROLLBACK");
		return false;
	}

	PQclear(res);

	return commit_transaction(conn);
}


static void
do_check_upstream_config(void)
{
	PGconn	   *conn;
	bool		config_ok;
	int			server_version_num;

	parse_config(&options);

	/* We need to connect to check configuration and start a backup */
	log_info(_("connecting to upstream server\n"));

	conn = establish_db_connection_by_params((const char**)param_keywords, (const char**)param_values, true);


	/* Verify that upstream server is a supported server version */
	log_verbose(LOG_INFO, _("connected to upstream server, checking its state\n"));
	server_version_num = check_server_version(conn, "upstream server", false, NULL);

	config_ok = check_upstream_config(conn, server_version_num, false);

	if (config_ok == true)
	{
		puts(_("No configuration problems found with the upstream server"));
	}

	PQfinish(conn);
}


static char *
make_pg_path(char *file)
{
	maxlen_snprintf(path_buf, "%s%s", pg_bindir, file);

	return path_buf;
}


static void
exit_with_errors(void)
{
	fprintf(stderr, _("%s: following command line errors were encountered.\n"), progname());

	print_error_list(&cli_errors, LOG_ERR);

	fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname());

	exit(ERR_BAD_CONFIG);
}


static void
print_error_list(ItemList *error_list, int log_level)
{
	ItemListCell *cell;

	for (cell = error_list->head; cell; cell = cell->next)
	{
		switch(log_level)
		{
			/* Currently we only need errors and warnings */
			case LOG_ERR:
				log_err("%s\n",  cell->string);
				break;
			case LOG_WARNING:
				log_warning("%s\n",  cell->string);
				break;
		}

	}
}


/*
 * Execute a command via ssh on the remote host.
 *
 * TODO: implement SSH calls using libssh2.
 */
static bool
remote_command(const char *host, const char *user, const char *command, PQExpBufferData *outputbuf)
{
	FILE *fp;
	char ssh_command[MAXLEN];
	PQExpBufferData ssh_host;

	char output[MAXLEN];

	initPQExpBuffer(&ssh_host);

	if (*user != '\0')
	{
		appendPQExpBuffer(&ssh_host, "%s@", user);
	}

	appendPQExpBuffer(&ssh_host, "%s",host);

	maxlen_snprintf(ssh_command,
					"ssh -o Batchmode=yes %s %s",
					ssh_host.data,
					command);

	termPQExpBuffer(&ssh_host);

	log_debug("remote_command(): %s\n", ssh_command);

	fp = popen(ssh_command, "r");

	if (fp == NULL)
	{
		log_err(_("unable to execute remote command:\n%s\n"), ssh_command);
		return false;
	}

	/* TODO: better error handling */
	while (fgets(output, MAXLEN, fp) != NULL)
	{
		appendPQExpBuffer(outputbuf, "%s", output);
	}

	pclose(fp);

	log_verbose(LOG_DEBUG, "remote_command(): output returned was:\n%s", outputbuf->data);

	return true;
}


/*
 * Extract values from provided conninfo string and return
 * formatted as command-line parameters suitable for passing to repmgr
 */
static void
format_db_cli_params(const char *conninfo, char *output)
{
	PQExpBufferData buf;
	char host[MAXLEN] = "";
	char port[MAXLEN] = "";
	char dbname[MAXLEN] = "";
	char user[MAXLEN] = "";

	initPQExpBuffer(&buf);

	get_conninfo_value(conninfo, "host", host);
	get_conninfo_value(conninfo, "port", port);
	get_conninfo_value(conninfo, "dbname", dbname);
	get_conninfo_value(conninfo, "user", user);

	if (host[0])
	{
		appendPQExpBuffer(&buf, "-h %s ", host);
	}

	if (port[0])
	{
		appendPQExpBuffer(&buf, "-p %s ", port);
	}

	if (dbname[0])
	{
		appendPQExpBuffer(&buf, "-d %s ", dbname);
	}

	if (user[0])
	{
		appendPQExpBuffer(&buf, "-U %s ", user);
	}

	strncpy(output, buf.data, MAXLEN);

	termPQExpBuffer(&buf);

}

bool
copy_file(const char *old_filename, const char *new_filename)
{
	FILE  *ptr_old, *ptr_new;
	int  a;

	ptr_old = fopen(old_filename, "r");
	ptr_new = fopen(new_filename, "w");

	if (ptr_old == NULL)
		return false;

	if (ptr_new == NULL)
	{
		fclose(ptr_old);
		return false;
	}

	chmod(new_filename, S_IRUSR | S_IWUSR);

	while(1)
	{
		a = fgetc(ptr_old);

		if (!feof(ptr_old))
		{
			fputc(a, ptr_new);
		}
		else
		{
			break;
		}
	}

	fclose(ptr_new);
	fclose(ptr_old);

	return true;
}

static void
param_set(const char *param, const char *value)
{
	int c;
	int value_len = strlen(value) + 1;

	/*
	 * Scan array to see if the parameter is already set - if not, replace it
	 */
	for (c = 0; c <= param_count && param_keywords[c] != NULL; c++)
	{
		if (strcmp(param_keywords[c], param) == 0)
		{
			if (param_values[c] != NULL)
				pfree(param_values[c]);

			param_values[c] = pg_malloc0(value_len);
			strncpy(param_values[c], value, value_len);

			return;
		}
	}

	/*
	 * Parameter not in array - add it and its associated value
	 */
	if (c < param_count)
	{
		int param_len = strlen(param) + 1;
		param_keywords[c] = pg_malloc0(param_len);
		param_values[c] = pg_malloc0(value_len);

		strncpy(param_keywords[c], param, param_len);
		strncpy(param_values[c], value, value_len);
	}

	/*
	 * It's theoretically possible a parameter couldn't be added as
	 * the array is full, but it's highly improbable so we won't
	 * handle it at the moment.
	 */
}


static void
parse_pg_basebackup_options(const char *pg_basebackup_options, t_basebackup_options *backup_options)
{
	int   options_len = strlen(pg_basebackup_options) + 1;
	char *options_string = pg_malloc(options_len);

	char *options_string_ptr = options_string;
	/*
	 * Add parsed options to this list, then copy to an array
	 * to pass to getopt
	 */
	static ItemList option_argv = { NULL, NULL };

	char *argv_item;
	int c, argc_item = 1;

	char **argv_array;
	ItemListCell *cell;

	int			optindex = 0;

	static struct option long_options[] =
	{
		{"slot", required_argument, NULL, 'S'},
		{"xlog-method", required_argument, NULL, 'X'},
		{NULL, 0, NULL, 0}
	};

	/* Don't attempt to tokenise an empty string */
	if (!strlen(pg_basebackup_options))
		return;

	/*
	 * Copy the string before operating on it with strtok()
	 */
	strncpy(options_string, pg_basebackup_options, options_len);

	while ((argv_item = strtok(options_string_ptr, " ")) != NULL)
	{
		item_list_append(&option_argv, argv_item);

		argc_item++;

		if (options_string_ptr != NULL)
			options_string_ptr = NULL;
	}

	argv_array = pg_malloc0(sizeof(char *) * (argc_item + 2));

	/* Copy a dummy program name to the start of the array */
	argv_array[0] = pg_malloc0(1);
	strncpy(argv_array[0], "", 4);

	c = 1;

	for (cell = option_argv.head; cell; cell = cell->next)
	{
		int argv_len = strlen(cell->string) + 1;

		argv_array[c] = pg_malloc0(argv_len);

		strncpy(argv_array[c], cell->string, argv_len);

		c++;
	}

	argv_array[c] = NULL;

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
		}
	}

	return;
}
