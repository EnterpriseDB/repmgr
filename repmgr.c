/*
 * repmgr.c - Command interpreter for the repmgr package
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
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
 * CLUSTER CROSSCHECK
 * CLUSTER MATRIX
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

#include "log.h"
#include "config.h"
#include "check_dir.h"
#include "strutil.h"
#include "version.h"
#include "compat.h"

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
#define CLUSTER_MATRIX		   15
#define CLUSTER_CROSSCHECK	   16

static int	test_ssh_connection(char *host, char *remote_user);
static int  copy_remote_files(char *host, char *remote_user, char *remote_path,
							  char *local_path, bool is_directory, int server_version_num);
static int  run_basebackup(const char *data_dir, int server_version_num);
static void check_parameters_for_action(const int action);
static bool create_schema(PGconn *conn);
static bool create_recovery_file(const char *data_dir, t_conninfo_param_list *recovery_conninfo);
static void write_primary_conninfo(char *line, t_conninfo_param_list *param_list);
static bool write_recovery_file_line(FILE *recovery_file, char *recovery_file_path, char *line);
static void check_master_standby_version_match(PGconn *conn, PGconn *master_conn);
static int	check_server_version(PGconn *conn, char *server_type, bool exit_on_error, char *server_version_string);
static bool check_upstream_config(PGconn *conn, int server_version_num, bool exit_on_error);
static bool update_node_record_set_master(PGconn *conn, int this_node_id);
static void tablespace_data_append(TablespaceDataList *list, const char *name, const char *oid, const char *location);
static int  get_tablespace_data(PGconn *upstream_conn, TablespaceDataList *list);
static int  get_tablespace_data_barman(char *, TablespaceDataList *);
static void get_barman_property(char *dst, char *name, char *local_repmgr_directory);

static char *string_skip_prefix(const char *prefix, char *string);
static char *string_remove_trailing_newlines(char *string);
static int  build_cluster_matrix(t_node_matrix_rec ***matrix_rec_dest, int *name_length);
static int  build_cluster_crosscheck(t_node_status_cube ***cube_dest, int *name_length);

static char *make_pg_path(char *file);
static char *make_barman_ssh_command(void);

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
static void do_cluster_matrix(void);
static void do_cluster_crosscheck(void);
static void do_cluster_cleanup(void);
static void do_check_upstream_config(void);
static void do_help(void);

static void exit_with_errors(void);
static void print_error_list(ItemList *error_list, int log_level);

static bool remote_command(const char *host, const char *user, const char *command, PQExpBufferData *outputbuf);
static bool local_command(const char *command, PQExpBufferData *outputbuf);

static void format_db_cli_params(const char *conninfo, char *output);
static bool copy_file(const char *old_filename, const char *new_filename);

static bool read_backup_label(const char *local_data_directory, struct BackupLabel *out_backup_label);

static void initialize_conninfo_params(t_conninfo_param_list *param_list, bool set_defaults);
static void copy_conninfo_params(t_conninfo_param_list *dest_list, t_conninfo_param_list *source_list);
static void param_set(t_conninfo_param_list *param_list, const char *param, const char *value);
static char *param_get(t_conninfo_param_list *param_list, const char *param);
static bool parse_conninfo_string(const char *conninfo_str, t_conninfo_param_list *param_list, char *errmsg, bool ignore_application_name);
static void conn_to_param_list(PGconn *conn, t_conninfo_param_list *param_list);
static char *param_list_to_string(t_conninfo_param_list *param_list);

static bool parse_pg_basebackup_options(const char *pg_basebackup_options, t_basebackup_options *backup_options, int server_version_num, ItemList *error_list);

static void config_file_list_init(t_configfile_list *list, int max_size);
static void config_file_list_add(t_configfile_list *list, const char *file, const char *filename, bool in_data_dir);

static void matrix_set_node_status(t_node_matrix_rec **matrix_rec_list, int n, int node_id, int connection_node_id, int connection_status);
static void cube_set_node_status(t_node_status_cube **cube, int n, int node_id, int matrix_node_id, int connection_node_id, int connection_status);

static void drop_replication_slot_if_exists(PGconn *conn, int node_id, char *slot_name);

/* Global variables */
static PQconninfoOption *opts = NULL;

/* conninfo params for the node we're cloning from */
t_conninfo_param_list source_conninfo;

static bool	config_file_required = true;

/* Initialization of runtime options */
t_runtime_options runtime_options = T_RUNTIME_OPTIONS_INITIALIZER;
t_configuration_options options = T_CONFIGURATION_OPTIONS_INITIALIZER;

static char *server_mode = NULL;
static char *server_cmd = NULL;

static char  pg_bindir[MAXLEN] = "";
static char  repmgr_slot_name[MAXLEN] = "";
static char *repmgr_slot_name_ptr = NULL;
static char  path_buf[MAXLEN] = "";
static char  barman_command_buf[MAXLEN] = "";
static char  repmgr_cluster[MAXLEN] = "";

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
		{"terse", no_argument, NULL, 't'},
		{"mode", required_argument, NULL, 'm'},
		{"pwprompt", no_argument, NULL, 'P'},
		{"remote-config-file", required_argument, NULL, 'C'},
		{"help", no_argument, NULL, OPT_HELP},
		{"check-upstream-config", no_argument, NULL, OPT_CHECK_UPSTREAM_CONFIG},
		{"recovery-min-apply-delay", required_argument, NULL, OPT_RECOVERY_MIN_APPLY_DELAY},
		{"pg_rewind", optional_argument, NULL, OPT_PG_REWIND},
		{"csv", no_argument, NULL, OPT_CSV},
		{"node", required_argument, NULL, OPT_NODE},
		{"without-barman", no_argument, NULL, OPT_WITHOUT_BARMAN},
		{"no-upstream-connection", no_argument, NULL, OPT_NO_UPSTREAM_CONNECTION},
		{"copy-external-config-files", optional_argument, NULL, OPT_COPY_EXTERNAL_CONFIG_FILES},
		{"wait-sync", optional_argument, NULL, OPT_REGISTER_WAIT},
		{"log-to-file", no_argument, NULL, OPT_LOG_TO_FILE},
		{"upstream-conninfo", required_argument, NULL, OPT_UPSTREAM_CONNINFO},
		{"replication-user", required_argument, NULL, OPT_REPLICATION_USER},
		{"version", no_argument, NULL, 'V'},
		/* Following options for internal use */
		{"cluster", required_argument, NULL, OPT_CLUSTER},
		{"config-archive-dir", required_argument, NULL, OPT_CONFIG_ARCHIVE_DIR},
		/* Following options deprecated */
		{"local-port", required_argument, NULL, 'l'},
		{"initdb-no-pwprompt", no_argument, NULL, OPT_INITDB_NO_PWPROMPT},
		{"ignore-external-config-files", no_argument, NULL, OPT_IGNORE_EXTERNAL_CONFIG_FILES},
		{"no-conninfo-password", no_argument, NULL, OPT_NO_CONNINFO_PASSWORD},
		{NULL, 0, NULL, 0}
	};

	int			optindex;
	int			c, targ;
	int			action = NO_ACTION;
	bool 		check_upstream_config = false;
	bool 		config_file_parsed = false;
	char 	   *ptr = NULL;

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

	/*
	 * Tell the logger we're a command-line program - this will
	 * ensure any output logged before the logger is initialized
	 * will be formatted correctly
	 */
	logger_output_mode = OM_COMMAND_LINE;

	initialize_conninfo_params(&source_conninfo, true);

	/*
	 * Pre-set any defaults , which can be overwritten if matching
	 * command line parameters are provided.
	 *
	 * Note: PQconndefaults() does not provide a default value for
	 * "dbname", but if none is provided will default to "username"
	 * when the connection is made.
	 */

	for (c = 0; c < source_conninfo.size && source_conninfo.keywords[c]; c++)
	{
		if (strcmp(source_conninfo.keywords[c], "host") == 0 &&
			(source_conninfo.values[c] != NULL))
		{
			strncpy(runtime_options.host, source_conninfo.values[c], MAXLEN);
		}
		else if (strcmp(source_conninfo.keywords[c], "hostaddr") == 0 &&
				(source_conninfo.values[c] != NULL))
		{
			strncpy(runtime_options.host, source_conninfo.values[c], MAXLEN);
		}
		else if (strcmp(source_conninfo.keywords[c], "port") == 0 &&
				 (source_conninfo.values[c] != NULL))
		{
			strncpy(runtime_options.masterport, source_conninfo.values[c], MAXLEN);
		}
		else if (strcmp(source_conninfo.keywords[c], "dbname") == 0 &&
				 (source_conninfo.values[c] != NULL))
		{
			strncpy(runtime_options.dbname, source_conninfo.values[c], MAXLEN);
		}
		else if (strcmp(source_conninfo.keywords[c], "user") == 0 &&
				 (source_conninfo.values[c] != NULL))
		{
			strncpy(runtime_options.username, source_conninfo.values[c], MAXLEN);
		}
	}

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

	while ((c = getopt_long(argc, argv, "?Vd:h:p:U:S:D:f:R:w:k:FWIvb:rcL:tm:C:l:P", long_options,
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
			case OPT_HELP:
				do_help();
				exit(SUCCESS);
			case 'V':
				printf("%s %s (PostgreSQL %s)\n", progname(), REPMGR_VERSION, PG_VERSION);
				exit(SUCCESS);
			case 'd':
				strncpy(runtime_options.dbname, optarg, MAXLEN);
				/* we'll set the dbname parameter below  if we detect it's not a conninfo string */
				runtime_options.connection_param_provided = true;
				break;
			case 'h':
				strncpy(runtime_options.host, optarg, MAXLEN);
				param_set(&source_conninfo, "host", optarg);
				runtime_options.connection_param_provided = true;
				runtime_options.host_param_provided = true;
				break;
			case 'p':
				repmgr_atoi(optarg, "-p/--port", &cli_errors, false);
				param_set(&source_conninfo, "port", optarg);
				strncpy(runtime_options.masterport,
						optarg,
						MAXLEN);
				runtime_options.connection_param_provided = true;
				break;
			case 'U':
				strncpy(runtime_options.username, optarg, MAXLEN);
				param_set(&source_conninfo, "user", optarg);
				runtime_options.connection_param_provided = true;
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
				runtime_options.wal_keep_segments_used = true;
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
			case 'P':
				runtime_options.witness_pwprompt = true;
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
			case OPT_COPY_EXTERNAL_CONFIG_FILES:
				runtime_options.copy_external_config_files = true;
				if (optarg != NULL)
				{
					if (strcmp(optarg, "samepath") == 0)
					{
						runtime_options.copy_external_config_files_destination = CONFIG_FILE_SAMEPATH;
					}
					else if (strcmp(optarg, "pgdata") == 0)
					{
						runtime_options.copy_external_config_files_destination = CONFIG_FILE_PGDATA;
					}
					else
					{
						item_list_append(&cli_errors, _("Value provided for '--copy-external-config-files' must be 'samepath' or 'pgdata'"));
					}
				}
				break;

			case OPT_PG_REWIND:
				if (optarg != NULL)
				{
					strncpy(runtime_options.pg_rewind, optarg, MAXPGPATH);
				}
				runtime_options.pg_rewind_supplied = true;
				break;
			case OPT_CSV:
				runtime_options.csv_mode = true;
				break;
			case OPT_NODE:
				runtime_options.node = repmgr_atoi(optarg, "--node", &cli_errors, false);
				break;
			case OPT_WITHOUT_BARMAN:
				runtime_options.without_barman = true;
				break;
			case OPT_NO_UPSTREAM_CONNECTION:
				runtime_options.no_upstream_connection = true;
				break;
			case OPT_UPSTREAM_CONNINFO:
				strncpy(runtime_options.upstream_conninfo, optarg, MAXLEN);
				break;
			case OPT_NO_CONNINFO_PASSWORD:
				runtime_options.no_conninfo_password = true;
				break;
			case OPT_REGISTER_WAIT:
				runtime_options.wait_register_sync = true;
				if (optarg != NULL)
				{
					runtime_options.wait_register_sync_seconds = repmgr_atoi(optarg, "--wait-sync", &cli_errors, false);
				}
				break;
			case OPT_LOG_TO_FILE:
				runtime_options.log_to_file = true;
				logger_output_mode = OM_DAEMON;
				break;
			case OPT_CONFIG_ARCHIVE_DIR:
				strncpy(runtime_options.config_archive_dir, optarg, MAXLEN);
				break;
			case OPT_CLUSTER:
				strncpy(repmgr_cluster, optarg, MAXLEN);
				break;
			case OPT_REPLICATION_USER:
				strncpy(runtime_options.replication_user, optarg, MAXLEN);
				break;
			/* deprecated options - output a warning */
			case 'l':
				item_list_append(&cli_warnings, _("-l/--local-port is deprecated; repmgr will extract the witness port from the conninfo string in repmgr.conf"));
				break;
			case OPT_INITDB_NO_PWPROMPT:
				item_list_append(&cli_warnings, _("--initdb-no-pwprompt is deprecated and has no effect; use -P/--pwprompt instead"));
				break;
			case OPT_IGNORE_EXTERNAL_CONFIG_FILES:
				item_list_append(&cli_warnings, _("--ignore-external-config-files is deprecated and has no effect; use --copy-external-config-file instead"));
				break;
			case '?':
				/* Actual help option given */
				if (strcmp(argv[optind - 1], "-?") == 0)
				{
					do_help();
					exit(SUCCESS);
				}
				break;
		}
	}

	/*
	 * If -d/--dbname appears to be a conninfo string, validate by attempting
	 * to parse it (and if successful, store the parsed parameters)
	 */
	if (runtime_options.dbname)
	{
		if (strncmp(runtime_options.dbname, "postgresql://", 13) == 0 ||
		   strncmp(runtime_options.dbname, "postgres://", 11) == 0 ||
		   strchr(runtime_options.dbname, '=') != NULL)
		{
			char	   *errmsg = NULL;

			runtime_options.conninfo_provided = true;

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
			param_set(&source_conninfo, "dbname", runtime_options.dbname);
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
	 *   CLUSTER { CROSSCHECK | MATRIX | SHOW | CLEANUP}
	 *
	 * the node part is optional, if we receive it then we shouldn't have
	 * received a -h option
	 */
	if (optind < argc)
	{
		server_mode = argv[optind++];
		if (strcasecmp(server_mode, "STANDBY") != 0 &&
			strcasecmp(server_mode, "MASTER")  != 0 &&
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
			else if (strcasecmp(server_cmd, "CROSSCHECK") == 0)
				action = CLUSTER_CROSSCHECK;
			else if (strcasecmp(server_cmd, "MATRIX") == 0)
				action = CLUSTER_MATRIX;
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
			if (runtime_options.host_param_provided == true)
			{
				PQExpBufferData additional_host_arg;
				initPQExpBuffer(&additional_host_arg);
				appendPQExpBuffer(&additional_host_arg,
								  _("Conflicting parameters:  you can't use %s while providing a node separately."),
								  runtime_options.conninfo_provided == true ? "host=" : "-h/--host");
				item_list_append(&cli_errors, additional_host_arg.data);
			}
			else
			{
				strncpy(runtime_options.host, argv[optind++], MAXLEN);
				param_set(&source_conninfo, "host", runtime_options.host);
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
	 * if --upstream-conninfo was set and can be used (i.e. we're doing STANDBY CLONE)
	 * perform a sanity check on the conninfo params
	 */
	if (action == STANDBY_CLONE && *runtime_options.upstream_conninfo)
	{
		PQconninfoOption *opts;
		char	   *errmsg = NULL;

		opts = PQconninfoParse(runtime_options.upstream_conninfo, &errmsg);

		if (opts == NULL)
		{
			PQExpBufferData conninfo_error;
			initPQExpBuffer(&conninfo_error);
			appendPQExpBuffer(&conninfo_error, _("error parsing conninfo:\n%s"), errmsg);
			item_list_append(&cli_errors, conninfo_error.data);

			termPQExpBuffer(&conninfo_error);
			free(errmsg);
		}
	}

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
	 * Initialize the logger. We'll request STDERR logging only to ensure the
	 * repmgr command never has its output diverted to a logging facility,
	 * which makes little sense for a command line program.
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

	if (options.use_replication_slots && runtime_options.wal_keep_segments_used)
	{
		log_warning(_("-w/--wal-keep-segments has no effect when replication slots in use\n"));
	}

	/*
	 * STANDBY CLONE in Barman mode is incompatible with
	 * `use_replication_slots`.
	 */

	if (action == STANDBY_CLONE &&
		! runtime_options.without_barman
		&& strcmp(options.barman_server, "") != 0
		&& options.use_replication_slots)
		{
			log_err(_("STANDBY CLONE in Barman mode is incompatible with configuration option \"use_replication_slots\""));
			exit(ERR_BAD_CONFIG);
		}

	/* Initialise the repmgr schema name */

	if (strlen(repmgr_cluster))
		/* --cluster parameter provided */
		maxlen_snprintf(repmgr_schema, "%s%s", DEFAULT_REPMGR_SCHEMA_PREFIX,
						repmgr_cluster);
	else
		maxlen_snprintf(repmgr_schema, "%s%s", DEFAULT_REPMGR_SCHEMA_PREFIX,
						options.cluster_name);

	/*
	 * If no value for the repmgr_schema provided, continue only under duress.
	 */
	if (strcmp(repmgr_schema, DEFAULT_REPMGR_SCHEMA_PREFIX) == 0 && !runtime_options.force)
	{
		log_err(_("unable to determine cluster name - please provide a valid configuration file with -f/--config-file\n"));
		log_hint(_("Use -F/--force to continue anyway\n"));
		exit(ERR_BAD_CONFIG);
	}

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
		case CLUSTER_CROSSCHECK:
			do_cluster_crosscheck();
			break;
		case CLUSTER_MATRIX:
			do_cluster_matrix();
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

	if (strlen(options.conninfo))
		conn = establish_db_connection(options.conninfo, true);
	else
		conn = establish_db_connection_by_params((const char**)source_conninfo.keywords,
												 (const char**)source_conninfo.values, true);

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
				(PQstatus(conn) == CONNECTION_OK) ? 0 : -1;
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
matrix_set_node_status(t_node_matrix_rec **matrix_rec_list, int n, int node_id, int connection_node_id, int connection_status)
{
	int i, j;

	for (i = 0; i < n; i++)
	{
		if (matrix_rec_list[i]->node_id == node_id)
		{
			for (j = 0; j < n; j++)
			{
				if (matrix_rec_list[i]->node_status_list[j]->node_id == connection_node_id)
				{
					matrix_rec_list[i]->node_status_list[j]->node_status = connection_status;
					break;
				}
			}
			break;
		}
	}
}

static int
build_cluster_matrix(t_node_matrix_rec ***matrix_rec_dest, int *name_length)
{
	PGconn	   *conn;
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];
	int			n;
	int			i, j;
	int			local_node_id;

	PQExpBufferData command;
	PQExpBufferData command_output;

	t_node_matrix_rec **matrix_rec_list;

	/* We need to connect to get the list of nodes */
	log_info(_("connecting to database\n"));

	if (strlen(options.conninfo))
	{
		conn = establish_db_connection(options.conninfo, true);
		local_node_id = options.node;
	}
	else
	{
		conn = establish_db_connection_by_params((const char**)source_conninfo.keywords,
												 (const char**)source_conninfo.values, true);
		local_node_id = runtime_options.node;
	}

	sqlquery_snprintf(sqlquery,
			  "SELECT conninfo, type, name, upstream_node_name, id, cluster"
			  "  FROM %s.repl_show_nodes ORDER BY id",
			  get_repmgr_schema_quoted(conn));

	log_verbose(LOG_DEBUG, "build_cluster_matrix(): \n%s\n", sqlquery);

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

	/*
	 * Allocate an empty matrix record list
	 *
	 * -2 == NULL  ?
	 * -1 == Error x
	 *  0 == OK    *
	 */
	n = PQntuples(res);

	matrix_rec_list = (t_node_matrix_rec **) pg_malloc0(sizeof(t_node_matrix_rec) * n);


	/* Initialise matrix structure for each node */
	for (i = 0; i < n; i++)
	{
		int name_length_cur;

		matrix_rec_list[i] = (t_node_matrix_rec *) pg_malloc0(sizeof(t_node_matrix_rec));

		matrix_rec_list[i]->node_id = atoi(PQgetvalue(res, i, 4));
		strncpy(matrix_rec_list[i]->node_name, PQgetvalue(res, i, 2), MAXLEN);

		/*
		 * Find the maximum length of a node name
		 */
		name_length_cur	= strlen(matrix_rec_list[i]->node_name);
		if (name_length_cur > *name_length)
			*name_length = name_length_cur;

		matrix_rec_list[i]->node_status_list = (t_node_status_rec **) pg_malloc0(sizeof(t_node_status_rec) * n);

		for (j = 0; j < n; j++)
		{
			matrix_rec_list[i]->node_status_list[j] = (t_node_status_rec *) pg_malloc0(sizeof(t_node_status_rec));
			matrix_rec_list[i]->node_status_list[j]->node_id = atoi(PQgetvalue(res, j, 4));
			matrix_rec_list[i]->node_status_list[j]->node_status = -2;  /* default unknown */
		}
	}

	/* Fetch `repmgr cluster show --csv` output for each node */

	for (i = 0; i < n; i++)
	{
		int connection_status;
		t_conninfo_param_list remote_conninfo;
		char *host, *p;
		int connection_node_id = atoi(PQgetvalue(res, i, 4));
		int			x, y;

		initialize_conninfo_params(&remote_conninfo, false);
		parse_conninfo_string(PQgetvalue(res, i, 0),
							  &remote_conninfo,
							  NULL,
							  false);

		host = param_get(&remote_conninfo, "host");

		conn = establish_db_connection(PQgetvalue(res, i, 0), false);

		connection_status =
			(PQstatus(conn) == CONNECTION_OK) ? 0 : -1;


		matrix_set_node_status(matrix_rec_list,
							   n,
							   local_node_id,
							   connection_node_id,
							   connection_status);


		if (connection_status)
			continue;

		/* We don't need to issue `cluster show --csv` for the local node */
		if (connection_node_id == local_node_id)
			continue;

		initPQExpBuffer(&command);

		/*
		 * We'll pass cluster name and database connection string to the remote
		 * repmgr - those are the only values it needs to work, and saves us
		 * making assumptions about the location of repmgr.conf
		 */
		appendPQExpBuffer(&command,
						  "\"%s -d '%s' --cluster '%s' ",
						  make_pg_path("repmgr"),
						  PQgetvalue(res, i, 0),
						  PQgetvalue(res, i, 5));


		if (strlen(pg_bindir))
		{
			appendPQExpBuffer(&command,
							  "--pg_bindir=");
			appendShellString(&command,
							  pg_bindir);
			appendPQExpBuffer(&command,
							  " ");
		}

		appendPQExpBuffer(&command,
						  " cluster show --csv\"");

		log_verbose(LOG_DEBUG, "build_cluster_matrix(): executing\n%s\n", command.data);

		initPQExpBuffer(&command_output);

		(void)remote_command(
			host,
			runtime_options.remote_user,
			command.data,
			&command_output);

		p = command_output.data;

		termPQExpBuffer(&command);

		for (j = 0; j < n; j++)
		{
			if (sscanf(p, "%d,%d", &x, &y) != 2)
			{
				fprintf(stderr, _("cannot parse --csv output: %s\n"), p);
				PQfinish(conn);
				exit(ERR_INTERNAL);
			}

			matrix_set_node_status(matrix_rec_list,
								   n,
								   connection_node_id,
								   x,
								   (y == -1) ? -1 : 0 );

			while (*p && (*p != '\n'))
				p++;
			if (*p == '\n')
				p++;
		}

		PQfinish(conn);
	}

	PQclear(res);

	*matrix_rec_dest = matrix_rec_list;

	return n;
}



static void
do_cluster_matrix()
{
	int			i, j;
	int			n;
	const char *node_header = "Name";
	int			name_length = strlen(node_header);

	t_node_matrix_rec **matrix_rec_list;

	n = build_cluster_matrix(&matrix_rec_list, &name_length);

	if (runtime_options.csv_mode)
	{
		for (i = 0; i < n; i++)
			for (j = 0; j < n; j++)
				printf("%d,%d,%d\n",
					   matrix_rec_list[i]->node_id,
					   matrix_rec_list[i]->node_status_list[j]->node_id,
					   matrix_rec_list[i]->node_status_list[j]->node_status);

	}
	else
	{
		char c;

		printf("%*s | Id ", name_length, node_header);
		for (i = 0; i < n; i++)
			printf("| %2d ", matrix_rec_list[i]->node_id);
		printf("\n");

		for (i = 0; i < name_length; i++)
			printf("-");
		printf("-+----");
		for (i = 0; i < n; i++)
			printf("+----");
		printf("\n");

		for (i = 0; i < n; i++)
		{
			printf("%*s | %2d ", name_length,
				   matrix_rec_list[i]->node_name,
				   matrix_rec_list[i]->node_id);
			for (j = 0; j < n; j++)
			{
				switch (matrix_rec_list[i]->node_status_list[j]->node_status)
				{
				case -2:
					c = '?';
					break;
				case -1:
					c = 'x';
					break;
				case 0:
					c = '*';
					break;
				default:
					exit(ERR_INTERNAL);
				}

				printf("|  %c ", c);
			}
			printf("\n");
		}
	}
}

static void
cube_set_node_status(t_node_status_cube **cube, int n, int execute_node_id, int matrix_node_id, int connection_node_id, int connection_status)
{
	int h, i, j;


	for (h = 0; h < n; h++)
	{
		if (cube[h]->node_id == execute_node_id)
		{
			for (i = 0; i < n; i++)
			{
				if (cube[h]->matrix_list_rec[i]->node_id == matrix_node_id)
				{
					for (j = 0; j < n; j++)
					{
						if (cube[h]->matrix_list_rec[i]->node_status_list[j]->node_id == connection_node_id)
						{
							cube[h]->matrix_list_rec[i]->node_status_list[j]->node_status = connection_status;
							break;
						}
					}
					break;
				}
			}
		}
	}
}


static int
build_cluster_crosscheck(t_node_status_cube ***dest_cube, int *name_length)
{
	PGconn	   *conn;
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];
	int			h, i, j;

	int			n = 0; /* number of nodes */

	t_node_status_cube **cube;

	/* We need to connect to get the list of nodes */
	log_info(_("connecting to database\n"));
	conn = establish_db_connection(options.conninfo, true);

	sqlquery_snprintf(sqlquery,
			  "SELECT conninfo, type, name, upstream_node_name, id"
			  "	 FROM %s.repl_show_nodes ORDER BY id",
			  get_repmgr_schema_quoted(conn));

	log_verbose(LOG_DEBUG, "build_cluster_crosscheck(): \n%s\n",sqlquery );

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

	/*
	 * Allocate an empty cube matrix structure
	 *
	 * -2 == NULL
	 * -1 == Error
	 *	0 == OK
	 */
	n = PQntuples(res);

	cube = (t_node_status_cube **) pg_malloc(sizeof(t_node_status_cube *) * n);

	for (h = 0; h < n; h++)
	{
		int name_length_cur;

		cube[h] = (t_node_status_cube *) pg_malloc(sizeof(t_node_status_cube));
		cube[h]->node_id = atoi(PQgetvalue(res, h, 4));

		strncpy(cube[h]->node_name, PQgetvalue(res, h, 2), MAXLEN);
		/*
		 * Find the maximum length of a node name
		 */
		name_length_cur	= strlen(cube[h]->node_name);
		if (name_length_cur > *name_length)
			*name_length = name_length_cur;

		cube[h]->matrix_list_rec = (t_node_matrix_rec **) pg_malloc(sizeof(t_node_matrix_rec) * n);

		for (i = 0; i < n; i++)
		{
			cube[h]->matrix_list_rec[i] = (t_node_matrix_rec *) pg_malloc0(sizeof(t_node_matrix_rec));
			cube[h]->matrix_list_rec[i]->node_id = atoi(PQgetvalue(res, i, 4));

			/* we don't need the name here */
			cube[h]->matrix_list_rec[i]->node_name[0] = '\0';

			cube[h]->matrix_list_rec[i]->node_status_list = (t_node_status_rec **) pg_malloc0(sizeof(t_node_status_rec) * n);

			for (j = 0; j < n; j++)
			{
				cube[h]->matrix_list_rec[i]->node_status_list[j] = (t_node_status_rec *) pg_malloc0(sizeof(t_node_status_rec));
				cube[h]->matrix_list_rec[i]->node_status_list[j]->node_id = atoi(PQgetvalue(res, j, 4));
				cube[h]->matrix_list_rec[i]->node_status_list[j]->node_status = -2;  /* default unknown */
			}
		}
	}


	/*
	 * Build the connection cube
	 */

	for (i = 0; i < n; i++)
	{
		int remote_node_id;
		PQExpBufferData command;
		PQExpBufferData command_output;

		char	   *p;

		remote_node_id = atoi(PQgetvalue(res, i, 4));

		initPQExpBuffer(&command);

		appendPQExpBuffer(&command,
						  "%s -d '%s' --cluster '%s' --node=%i ",
						  make_pg_path("repmgr"),
						  PQgetvalue(res, i, 0),
						  options.cluster_name,
						  remote_node_id);

		if (strlen(pg_bindir))
		{
			appendPQExpBuffer(&command,
							  "--pg_bindir=");
			appendShellString(&command,
							  pg_bindir);
			appendPQExpBuffer(&command,
							  " ");
		}

		appendPQExpBuffer(&command,
						  "cluster matrix --csv 2>/dev/null");

		initPQExpBuffer(&command_output);

		if (cube[i]->node_id == options.node)
		{
			(void)local_command(
				command.data,
				&command_output);
		}
		else
		{
			t_conninfo_param_list remote_conninfo;
			char *host;
			PQExpBufferData quoted_command;

			initPQExpBuffer(&quoted_command);
			appendPQExpBuffer(&quoted_command,
							  "\"%s\"",
							  command.data);

			initialize_conninfo_params(&remote_conninfo, false);
			parse_conninfo_string(PQgetvalue(res, i, 0),
								  &remote_conninfo,
								  NULL,
								  false);

			host = param_get(&remote_conninfo, "host");

			log_verbose(LOG_DEBUG, "build_cluster_crosscheck(): executing\n%s\n", quoted_command.data);

			(void)remote_command(
				host,
				runtime_options.remote_user,
				quoted_command.data,
				&command_output);

			termPQExpBuffer(&quoted_command);
		}

		p = command_output.data;

		if(!strlen(command_output.data))
		{
			continue;
		}

		for (j = 0; j < n * n; j++)
		{
			int matrix_rec_node_id;
			int node_status_node_id;
			int node_status;

			if (sscanf(p, "%d,%d,%d", &matrix_rec_node_id, &node_status_node_id, &node_status) != 3)
			{
				fprintf(stderr, _("cannot parse --csv output: %s\n"), p);
				exit(ERR_INTERNAL);
			}

			cube_set_node_status(cube,
								 n,
								 remote_node_id,
								 matrix_rec_node_id,
								 node_status_node_id,
								 node_status);

			while (*p && (*p != '\n'))
				p++;
			if (*p == '\n')
				p++;
		}
	}

	*dest_cube = cube;
	return n;
}



static void
do_cluster_crosscheck(void)
{
	int			i, n;
	char		c;
	const char *node_header = "Name";
	int			name_length = strlen(node_header);

	t_node_status_cube **cube;

	n = build_cluster_crosscheck(&cube, &name_length);

	printf("%*s | Id ", name_length, node_header);
	for (i = 0; i < n; i++)
		printf("| %2d ", cube[i]->node_id);
	printf("\n");

	for (i = 0; i < name_length; i++)
		printf("-");
	printf("-+----");
	for (i = 0; i < n; i++)
		printf("+----");
	printf("\n");

	for (i = 0; i < n; i++)
	{
		int column_node_ix;

		printf("%*s | %2d ", name_length,
			   cube[i]->node_name,
			   cube[i]->node_id);

		for (column_node_ix = 0; column_node_ix < n; column_node_ix++)
		{
			int max_node_status = -2;
			int node_ix;

				/*
				 * The value of entry (i,j) is equal to the
				 * maximum value of all the (i,j,k). Indeed:
				 *
				 * - if one of the (i,j,k) is 0 (node up), then 0
				 *	 (the node is up);
				 *
				 * - if the (i,j,k) are either -1 (down) or -2
				 *	 (unknown), then -1 (the node is down);
				 *
				 * - if all the (i,j,k) are -2 (unknown), then -2
				 *	 (the node is in an unknown state).
				 */

			for(node_ix = 0; node_ix < n; node_ix ++)
			{
				int node_status = cube[node_ix]->matrix_list_rec[i]->node_status_list[column_node_ix]->node_status;
				if (node_status > max_node_status)
					max_node_status = node_status;
			}

			switch (max_node_status)
			{
				case -2:
					c = '?';
					break;
				case -1:
					c = 'x';
					break;
				case 0:
					c = '*';
					break;
				default:
					exit(ERR_INTERNAL);
			}

			printf("|  %c ", c);
		}

		printf("\n");

	}
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
	if (PQstatus(master_conn) != CONNECTION_OK)
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

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		/*
		 * Unlikely to happen and not a problem per-se, but we'll issue a warning
		 * just in case
		 */
		log_warning(_("unable to vacuum table %s.repl_monitor\n"), get_repmgr_schema_quoted(master_conn));
	}


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
	t_node_info node_info = T_NODE_INFO_INITIALIZER;

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
		log_info(_("master register: creating database objects inside the '%s' schema\n"),
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

	/*
	 * Check whether there's an existing record for this node, and
	 * update it if --force set
	 */
	if (get_node_record(conn, options.cluster_name, options.node, &node_info))
	{
		if (!runtime_options.force)
		{
			log_err(_("this node is already registered\n"));
			log_hint(_("use -F/--force to overwrite the existing node record\n"));
			rollback_transaction(conn);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		record_created = update_node_record(conn,
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

	}
	else
	{
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
	}

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

	log_notice(_("master node correctly registered for cluster '%s' with id %d (conninfo: %s)\n"),
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
	conn = establish_db_connection_quiet(options.conninfo);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		if (!runtime_options.force)
		{
			log_err(_("unable to connect to local node %i (\"%s\")\n"),
					options.node,
					options.node_name);
			log_hint(_("use option -F/--force to register a standby which is not running\n"));
			if (PQstatus(conn) == CONNECTION_OK)
				PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		if (!runtime_options.connection_param_provided)
		{
			log_err(_("unable to connect to local node %i (\"%s\") and no master connection parameters provided\n"),
					options.node,
					options.node_name);
			exit(ERR_BAD_CONFIG);
		}
	}


	if (PQstatus(conn) == CONNECTION_OK)
	{
		/* Check we are a standby */
		ret = is_standby(conn);

		if (ret == 0 || ret == -1)
		{
			log_err(_(ret == 0 ? "this node should be a standby (%s)\n" :
					  "connection to node (%s) lost\n"), options.conninfo);

			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/* check if there is a master in this cluster */
	log_info(_("connecting to master database\n"));

	/* Normal case - we can connect to the local node */
	if (PQstatus(conn) == CONNECTION_OK)
	{
		master_conn = get_master_connection(conn, options.cluster_name,
											NULL, NULL);
	}
	/* User is forcing a registration and must have supplied master connection info */
	else
	{
		master_conn = establish_db_connection_by_params((const char**)source_conninfo.keywords,
														(const char**)source_conninfo.values,
														false);
	}

	/*
	 * no amount of --force will make it possible to register the standby
	 * without a master server to connect to
	 */
	if (PQstatus(master_conn) != CONNECTION_OK)
	{
		log_err(_("unable to connect to the master database\n"));
		log_hint(_("a master must be defined before configuring a standby\n"));
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Verify that standby and master are supported and compatible server
	 * versions
	 * TODO: if connection not OK, extract standby's $PG_DATA/PG_VERSION
	 */
	if (PQstatus(conn) == CONNECTION_OK)
	{
		check_master_standby_version_match(conn, master_conn);
	}

	/* Now register the standby */
	log_info(_("registering the standby\n"));

	/*
	 * Check that an active node with the same node_name doesn't exist already
	 */

	node_result = get_node_record_by_name(master_conn,
										  options.cluster_name,
										  options.node_name,
										  &node_record);

	if (node_result)
	{
		if (node_record.active == true && node_record.node_id != options.node)
		{
			log_err(_("Node %i exists already with node_name \"%s\"\n"),
					  node_record.node_id,
					  options.node_name);
			PQfinish(master_conn);
			if (PQstatus(conn) == CONNECTION_OK)
				PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/*
	 * If an upstream node is defined, check if that node exists and is active
	 * If it doesn't exist, and --force set, create a minimal inactive record
	 */

	if (options.upstream_node != NO_UPSTREAM_NODE)
	{
		node_result = get_node_record(master_conn,
									  options.cluster_name,
									  options.upstream_node,
									  &node_record);

		if (!node_result)
		{
			if (!runtime_options.force)
			{
				log_err(_("no record found for upstream node %i\n"),
						options.upstream_node);
				/* footgun alert - only do this if you know what you're doing */
				log_hint(_("use option -F/--force to create a dummy upstream record\n"));
				PQfinish(master_conn);
				if (PQstatus(conn) == CONNECTION_OK)
					PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}

			log_notice(_("creating placeholder record for upstream node %i\n"),
					   options.upstream_node);

			record_created = create_node_record(master_conn,
												"standby register",
												options.upstream_node,
												"standby",
												NO_UPSTREAM_NODE,
												options.cluster_name,
												"",
												runtime_options.upstream_conninfo,
												DEFAULT_PRIORITY,
												NULL,
												false);

			/*
			 * It's possible, in the kind of scenario this functionality is intended
			 * to support, that there's a race condition where the node's actual
			 * record gets inserted, causing the insert of the placeholder record
			 * to fail. If this is the case, we don't worry about this insert failing;
			 * if not we bail out.
			 *
			 * TODO: teach create_node_record() to use ON CONFLICT DO NOTHING for
			 * 9.5 and later.
			 */
			if (record_created == false)
			{
				node_result = get_node_record(master_conn,
											  options.cluster_name,
											  options.upstream_node,
											  &node_record);
				if (!node_result)
				{
					log_err(_("unable to create placeholder record for upstream node %i\n"),
							options.upstream_node);
					PQfinish(master_conn);
					if (PQstatus(conn) == CONNECTION_OK)
						PQfinish(conn);
					exit(ERR_BAD_CONFIG);
				}

				log_info(_("a record for upstream node %i was already created\n"),
						   options.upstream_node);
			}

		}
		else if (node_record.active == false)
		{
			/*
			 * upstream node is inactive and --force not supplied - refuse to register
			 */
			if (!runtime_options.force)
			{
				log_err(_("record for upstream node %i is marked as inactive\n"),
							options.upstream_node);
				log_hint(_("use option -F/--force to register a standby with an inactive upstream node\n"));
				PQfinish(master_conn);
				if (PQstatus(conn) == CONNECTION_OK)
					PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}

			/*
			 * user is using the --force - notify about the potential footgun
			 */
			log_notice(_("registering node %i with inactive upstream node %i\n"),
					   options.node,
					   options.upstream_node);
		}
	}


	/* Check if node record exists */

	node_result = get_node_record(master_conn,
								  options.cluster_name,
								  options.node,
								  &node_record);

	if (node_result && !runtime_options.force)
	{
		log_err(_("node %i is already registered\n"),
				options.node);
		log_hint(_("use option -F/--force to overwrite an existing node record\n"));
		PQfinish(master_conn);
		if (PQstatus(conn) == CONNECTION_OK)
			PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * node record exists - update it
	 * (at this point we have already established that -F/--force is in use)
	 */
	if (node_result)
	{
		record_created = update_node_record(master_conn,
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
	}
	else
	{
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
	}

	if (record_created == false)
	{
		/* XXX add event description */

		create_event_record(master_conn,
							&options,
							options.node,
							"standby_register",
							false,
							NULL);

		PQfinish(master_conn);

		if (PQstatus(conn) == CONNECTION_OK)
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

	/* if --wait-sync option set, wait for the records to synchronise */

	if (PQstatus(conn) == CONNECTION_OK &&
		runtime_options.wait_register_sync == true &&
		runtime_options.wait_register_sync_seconds > 0)
	{
		bool sync_ok = false;
		int timer = 0;
		int node_record_result;
		t_node_info node_record_on_master = T_NODE_INFO_INITIALIZER;
		t_node_info node_record_on_standby = T_NODE_INFO_INITIALIZER;

		node_record_result = get_node_record(master_conn,
											 options.cluster_name,
											 options.node,
											 &node_record_on_master);

		if (node_record_result != 1)
		{
			log_err(_("unable to retrieve node record from master\n"));
			PQfinish(master_conn);
			PQfinish(conn);
			exit(ERR_REGISTRATION_SYNC);
		}

		for (;;)
		{
			bool records_match = true;

			if (runtime_options.wait_register_sync_seconds && runtime_options.wait_register_sync_seconds == timer)
				break;

			node_record_result = get_node_record(conn,
												 options.cluster_name,
												 options.node,
												 &node_record_on_standby);

			if (node_record_result == 0)
			{
				/* no record available yet on standby*/
				records_match = false;
			}
			else if (node_record_result == 1)
			{
				/* compare relevant fields */
				if (node_record_on_standby.upstream_node_id != node_record_on_master.upstream_node_id)
					records_match = false;

				if (node_record_on_standby.type != node_record_on_master.type)
					records_match = false;

				if (node_record_on_standby.priority != node_record_on_master.priority)
					records_match = false;

				if (node_record_on_standby.active != node_record_on_master.active)
					records_match = false;

				if (strcmp(node_record_on_standby.name, node_record_on_master.name) != 0)
					records_match = false;

				if (strcmp(node_record_on_standby.conninfo_str, node_record_on_master.conninfo_str) != 0)
					records_match = false;

				if (strcmp(node_record_on_standby.slot_name, node_record_on_master.slot_name) != 0)
					records_match = false;

				if (records_match == true)
				{
					sync_ok = true;
					break;
				}
			}

			sleep(1);
			timer ++;
		}

		if (sync_ok == false)
		{
			log_err(_("node record was not synchronised after %i seconds\n"),
					runtime_options.wait_register_sync_seconds);
			PQfinish(master_conn);
			PQfinish(conn);
			exit(ERR_REGISTRATION_SYNC);
		}

		log_info(_("node record on standby synchronised from master\n"));
	}

	PQfinish(master_conn);

	if (PQstatus(conn) == CONNECTION_OK)
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
	if (PQstatus(master_conn) != CONNECTION_OK)
	{
		log_err(_("a master must be defined before unregistering a standby\n"));
		exit(ERR_BAD_CONFIG);
	}

	if (runtime_options.node != UNKNOWN_NODE_ID)
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

void
tablespace_data_append(TablespaceDataList *list, const char *name, const char *oid, const char *location)
{
	TablespaceDataListCell *cell;

	cell = (TablespaceDataListCell *) pg_malloc0(sizeof(TablespaceDataListCell));

	if (cell == NULL)
	{
		log_err(_("unable to allocate memory; terminating.\n"));
		exit(ERR_BAD_CONFIG);
	}

	cell->oid      = pg_malloc(1 + strlen(oid     ));
	cell->name     = pg_malloc(1 + strlen(name    ));
	cell->location = pg_malloc(1 + strlen(location));

	strncpy(cell->oid     , oid     , 1 + strlen(oid     ));
	strncpy(cell->name    , name    , 1 + strlen(name    ));
	strncpy(cell->location, location, 1 + strlen(location));

	if (list->tail)
		list->tail->next = cell;
	else
		list->head = cell;

	list->tail = cell;
}

int
get_tablespace_data(PGconn *upstream_conn, TablespaceDataList *list)
{
	int i;
	char sqlquery[QUERY_STR_LEN];
	PGresult *res;

	sqlquery_snprintf(sqlquery,
					  " SELECT spcname, oid, pg_tablespace_location(oid) AS spclocation "
					  "   FROM pg_catalog.pg_tablespace "
					  "  WHERE spcname NOT IN ('pg_default', 'pg_global')");

	res = PQexec(upstream_conn, sqlquery);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("unable to execute tablespace query: %s\n"),
				PQerrorMessage(upstream_conn));

		PQclear(res);

		return ERR_DB_QUERY;
	}

	for (i = 0; i < PQntuples(res); i++)
		tablespace_data_append(list,
							   PQgetvalue(res, i, 0),
							   PQgetvalue(res, i, 1),
							   PQgetvalue(res, i, 2));

	PQclear(res);
	return SUCCESS;
}

char *
string_skip_prefix(const char *prefix, char *string)
{
	int n;

	n = strlen(prefix);

	if (strncmp(prefix, string, n))
		return NULL;
	else
		return string + n;
}

char *
string_remove_trailing_newlines(char *string)
{
	int n;

	n = strlen(string) - 1;

	while (n >= 0 && string[n] == '\n')
		string[n] = 0;

	return string;
}

int
get_tablespace_data_barman
( char *tablespace_data_barman,
  TablespaceDataList *tablespace_list)
{
	/*
	 * Example:
	 * [('main', 24674, '/var/lib/postgresql/tablespaces/9.5/main'), ('alt', 24678, '/var/lib/postgresql/tablespaces/9.5/alt')]
	 */

	char name[MAXLEN];
	char oid[MAXLEN];
	char location[MAXPGPATH];
	char *p = tablespace_data_barman;
	int i;

	tablespace_list->head = NULL;
	tablespace_list->tail = NULL;

	p = string_skip_prefix("[", p);
	if (p == NULL) return -1;

	while (*p == '(')
	{
		p = string_skip_prefix("('", p);
		if (p == NULL) return -1;

		i = strcspn(p, "'");
		strncpy(name, p, i);
		name[i] = 0;

		p = string_skip_prefix("', ", p + i);
		if (p == NULL) return -1;

		i = strcspn(p, ",");
		strncpy(oid, p, i);
		oid[i] = 0;

		p = string_skip_prefix(", '", p + i);
		if (p == NULL) return -1;

		i = strcspn(p, "'");
		strncpy(location, p, i);
		location[i] = 0;

		p = string_skip_prefix("')", p + i);
		if (p == NULL) return -1;

		tablespace_data_append (tablespace_list, name, oid, location);

		if (*p == ']')
			break;

		p = string_skip_prefix(", ", p);
		if (p == NULL) return -1;
	}

	return SUCCESS;
}

void
get_barman_property(char *dst, char *name, char *local_repmgr_directory)
{
	PQExpBufferData command_output;
	char buf[MAXLEN];
	char command[MAXLEN];
	char *p;

	initPQExpBuffer(&command_output);

	maxlen_snprintf(command,
					"grep \"^\t%s:\" %s/show-server.txt",
					name, local_repmgr_directory);
	(void)local_command(command, &command_output);

	maxlen_snprintf(buf, "\t%s: ", name);
	p = string_skip_prefix(buf, command_output.data);
	if (p == NULL)
	{
		log_err("Unexpected output from Barman: %s\n",
				command_output.data);
		exit(ERR_INTERNAL);
	}

	strncpy(dst, p, MAXLEN);
	string_remove_trailing_newlines(dst);

	termPQExpBuffer(&command_output);
}

static void
do_standby_clone(void)
{
	PGconn	   *primary_conn = NULL;
	PGconn	   *source_conn = NULL;
	PGresult   *res;

	/*
	 * conninfo params for the actual upstream node (which might be different
	 * to the node we're cloning from) to write to recovery.conf
	 */
	t_conninfo_param_list recovery_conninfo;
	char		recovery_conninfo_str[MAXLEN];
	bool		upstream_record_found = false;
	int		    upstream_node_id = UNKNOWN_NODE_ID;

	char        datadir_list_filename[MAXLEN];


	enum {
		barman,
		rsync,
		pg_basebackup
	}			mode;

	char		sqlquery[QUERY_STR_LEN];

	int			server_version_num = -1;

	char		cluster_size[MAXLEN];

	int			r = 0,
		retval = SUCCESS;

	int			i;
	bool		pg_start_backup_executed = false;
	bool		target_directory_provided = false;

	char		master_data_directory[MAXPGPATH];
	char		local_data_directory[MAXPGPATH];

	char		local_repmgr_directory[MAXPGPATH];

	char		master_control_file[MAXPGPATH] = "";
	char		local_control_file[MAXPGPATH] = "";

	char	   *first_wal_segment = NULL;
	char	   *last_wal_segment = NULL;

	PQExpBufferData event_details;
	t_configfile_list config_files = T_CONFIGFILE_LIST_INITIALIZER;

	/*
	 * Detecting the appropriate mode
	 */
	if (runtime_options.rsync_only)
		mode = rsync;
	else if (strcmp(options.barman_server, "") != 0 && ! runtime_options.without_barman)
		mode = barman;
	else
		mode = pg_basebackup;

	/*
	 * In rsync mode, we need to check the SSH connection early
	 */
	if (mode == rsync)
	{
		r = test_ssh_connection(runtime_options.host, runtime_options.remote_user);
		if (r != 0)
		{
			log_err(_("aborting, remote host %s is not reachable via SSH.\n"),
					runtime_options.host);
			exit(ERR_BAD_SSH);
		}
	}


	/*
	 * If dest_dir (-D/--pgdata) was provided, this will become the new data
	 * directory (otherwise repmgr will default to using the same directory
	 * path as on the source host).
	 *
	 * Note that barman mode requires -D/--pgdata.
	 *
	 * If -D/--pgdata is not supplied, and we're not cloning from barman,
	 * the source host's data directory will be fetched later, after
	 * we've connected to it.
	 */
	if (runtime_options.dest_dir[0])
	{
		target_directory_provided = true;
		log_notice(_("destination directory '%s' provided\n"),
				   runtime_options.dest_dir);
	}
	else if (mode == barman)
	{
		log_err(_("Barman mode requires a destination directory\n"));
		log_hint(_("use -D/--data-dir to explicitly specify a data directory\n"));
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * target directory (-D/--pgdata) provided - use that as new data directory
	 * (useful when executing backup on local machine only or creating the backup
	 * in a different local directory when backup source is a remote host)
	 */
	if (target_directory_provided == true)
	{
		strncpy(local_data_directory, runtime_options.dest_dir, MAXPGPATH);
	}


	/*
	 * Initialise list of conninfo parameters which will later be used
	 * to create the `primary_conninfo` string in recovery.conf .
	 *
	 * We'll initialise it with the default values as seen by libpq,
	 * and overwrite them with the host settings specified on the command
	 * line. As it's possible the standby will be cloned from a node different
	 * to its intended upstream, we'll later attempt to fetch the
	 * upstream node record and overwrite the values set here with
	 * those from the upstream node record (excluding that record's
	 * application_name)
	 */
	initialize_conninfo_params(&recovery_conninfo, true);

	copy_conninfo_params(&recovery_conninfo, &source_conninfo);

	/* Set the default application name to this node's name */
	param_set(&recovery_conninfo, "application_name", options.node_name);


	/*
	 * If application_name is set in repmgr.conf's conninfo parameter, use
	 * this value (if the source host was provided as a conninfo string, any
	 * application_name values set there will be overridden; we assume the only
	 * reason to pass an application_name via the command line is in the
	 * rare corner case where a user wishes to clone a server without
	 * providing repmgr.conf)
	 */
	if (strlen(options.conninfo))
	{
		char application_name[MAXLEN] = "";

		get_conninfo_value(options.conninfo, "application_name", application_name);
		if (strlen(application_name))
		{
			param_set(&recovery_conninfo, "application_name", application_name);
		}
	}

	/* Sanity-check barman connection and installation */
	if (mode == barman)
	{
		char		command[MAXLEN];
		bool		command_ok;
		/*
		 * Check that there is at least one valid backup
		 */

		log_info(_("Connecting to Barman server to verify backup for %s\n"), options.cluster_name);

		maxlen_snprintf(command, "%s show-backup %s latest > /dev/null",
						make_barman_ssh_command(),
						options.cluster_name);
		command_ok = local_command(command, NULL);
		if (command_ok == false)
		{
			log_err(_("No valid backup for server %s was found in the Barman catalogue\n"),
					options.cluster_name);
			log_hint(_("Refer to the Barman documentation for more information\n"));

			exit(ERR_BARMAN);
		}

		/*
		 * Create the local repmgr subdirectory
		 */

		maxlen_snprintf(local_repmgr_directory, "%s/repmgr",   local_data_directory  );
		maxlen_snprintf(datadir_list_filename,  "%s/data.txt", local_repmgr_directory);

		if (!create_pg_dir(local_data_directory, runtime_options.force))
		{
			log_err(_("unable to use directory %s ...\n"),
					local_data_directory);
			log_hint(_("use -F/--force option to force this directory to be overwritten\n"));
			exit(ERR_BAD_CONFIG);
		}

		if (!create_pg_dir(local_repmgr_directory, runtime_options.force))
		{
			log_err(_("unable to create directory \"%s\" ...\n"),
					local_repmgr_directory);

			exit(ERR_BAD_CONFIG);
		}

		/*
		 * Fetch server parameters from Barman
		 */
		log_info(_("Connecting to Barman server to fetch server parameters\n"));

		maxlen_snprintf(command, "%s show-server %s > %s/show-server.txt",
						make_barman_ssh_command(),
						options.cluster_name,
						local_repmgr_directory);

		command_ok = local_command(command, NULL);

		if (command_ok == false)
		{
			log_err(_("Unable to fetch server parameters from Barman server\n"));

			exit(ERR_BARMAN);
		}
	}

	/*
	 * --upstream-conninfo supplied, which we interpret to imply
	 * --no-upstream-connection as well - the use case for this option is when
	 * the upstream is not available, so no point in checking for it.
	 */

	if (*runtime_options.upstream_conninfo)
		runtime_options.no_upstream_connection = false;

	/* By default attempt to connect to the source server */
	if (runtime_options.no_upstream_connection == false)
	{
		/* Attempt to connect to the upstream server to verify its configuration */
		log_info(_("connecting to upstream node\n"));

		source_conn = establish_db_connection_by_params((const char**)source_conninfo.keywords,
														(const char**)source_conninfo.values,
														false);

		/*
		 * Unless in barman mode, exit with an error;
		 * establish_db_connection_by_params() will have already logged an error message
		 */
		if (PQstatus(source_conn) != CONNECTION_OK)
		{
			if (mode != barman)
			{
				PQfinish(source_conn);
				exit(ERR_DB_CON);
			}
		}
		else
		{
			/*
			 * If a connection was established, perform some sanity checks on the
			 * provided upstream connection
			 */
			t_node_info upstream_node_record = T_NODE_INFO_INITIALIZER;
			int query_result;

			/* Verify that upstream node is a supported server version */
			log_verbose(LOG_INFO, _("connected to upstream node, checking its state\n"));
			server_version_num = check_server_version(source_conn, "master", true, NULL);

			check_upstream_config(source_conn, server_version_num, true);

			if (get_cluster_size(source_conn, cluster_size) == false)
				exit(ERR_DB_QUERY);

			log_info(_("Successfully connected to upstream node. Current installation size is %s\n"),
					 cluster_size);

			/*
			 * If --recovery-min-apply-delay was passed, check that
			 * we're connected to PostgreSQL 9.4 or later
			 */
			if (*runtime_options.recovery_min_apply_delay)
			{
				if (server_version_num < 90400)
				{
					log_err(_("PostgreSQL 9.4 or greater required for --recovery-min-apply-delay\n"));
					PQfinish(source_conn);
					exit(ERR_BAD_CONFIG);
				}
			}

			/*
			 * If the upstream node is a standby, try to connect to the primary too so we
			 * can write an event record
			 */
			if (is_standby(source_conn))
			{
				if (strlen(options.cluster_name))
				{
					primary_conn = get_master_connection(source_conn, options.cluster_name,
														 NULL, NULL);
				}
			}
			else
			{
				primary_conn = source_conn;
			}

			/*
			 * Sanity-check that the master node has a repmgr schema - if not
			 * present, fail with an error (unless -F/--force is used)
			 */

			if (check_cluster_schema(primary_conn) == false)
			{
				if (!runtime_options.force)
				{
					/* schema doesn't exist */
					log_err(_("expected repmgr schema '%s' not found on master server\n"), get_repmgr_schema());
					log_hint(_("check that the master server was correctly registered\n"));
					PQfinish(source_conn);
					exit(ERR_BAD_CONFIG);
				}

				log_warning(_("expected repmgr schema '%s' not found on master server\n"), get_repmgr_schema());
			}

			/* Fetch the source's data directory */
			if (get_pg_setting(source_conn, "data_directory", master_data_directory) == false)
			{
				log_err(_("Unable to retrieve upstream node's data directory\n"));
				log_hint(_("STANDBY CLONE must be run as a database superuser"));
				PQfinish(source_conn);
				exit(ERR_BAD_CONFIG);
			}

			/*
			 * If no target directory was explicitly provided, we'll default to
			 * the same directory as on the source host.
			 */
			if (target_directory_provided == false)
			{
				strncpy(local_data_directory, master_data_directory, MAXPGPATH);

				log_notice(_("setting data directory to: \"%s\"\n"), local_data_directory);
				log_hint(_("use -D/--data-dir to explicitly specify a data directory\n"));
			}

			/*
			 * Copy the source connection so that we have some default values,
			 * particularly stuff like passwords extracted from PGPASSFILE;
			 * these will be overridden from the upstream conninfo, if provided.
			 */
			conn_to_param_list(source_conn, &recovery_conninfo);

			/*
			 * Attempt to find the upstream node record
			 */
			if (options.upstream_node == NO_UPSTREAM_NODE)
				upstream_node_id = get_master_node_id(source_conn, options.cluster_name);
			else
				upstream_node_id = options.upstream_node;

			query_result = get_node_record(source_conn, options.cluster_name, upstream_node_id, &upstream_node_record);

			if (query_result)
			{
				upstream_record_found = true;
				strncpy(recovery_conninfo_str, upstream_node_record.conninfo_str, MAXLEN);
			}
		}
	}

	if (mode == barman && PQstatus(source_conn) != CONNECTION_OK)
	{
		/*
		 * Here we don't have a connection to the upstream node, and are executing
		 * in Barman mode - we can try and connect via the Barman server to extract
		 * the upstream node's conninfo string.
		 *
		 * To do this we need to extract Barman's conninfo string, replace the database
		 * name with the repmgr one (they could well be different) and remotely execute
		 * psql.
		 */
		char		    buf[MAXLEN];
		char		    barman_conninfo_str[MAXLEN];
		t_conninfo_param_list barman_conninfo;
		char		   *errmsg = NULL;
		bool		    parse_success,
					    command_success;
		char		    where_condition[MAXLEN];
		PQExpBufferData command_output;
		PQExpBufferData repmgr_conninfo_buf;

		int c;

		get_barman_property(barman_conninfo_str, "conninfo", local_repmgr_directory);

		initialize_conninfo_params(&barman_conninfo, false);

		/* parse_conninfo_string() here will remove the upstream's `application_name`, if set */
		parse_success = parse_conninfo_string(barman_conninfo_str, &barman_conninfo, errmsg, true);

		if (parse_success == false)
		{
			log_err(_("Unable to parse barman conninfo string \"%s\":\n%s\n"),
					barman_conninfo_str, errmsg);
			exit(ERR_BARMAN);
		}

		/* Overwrite database name in the parsed parameter list */
		param_set(&barman_conninfo, "dbname", runtime_options.dbname);

		/* Rebuild the Barman conninfo string */
		initPQExpBuffer(&repmgr_conninfo_buf);

		for (c = 0; c < barman_conninfo.size && barman_conninfo.keywords[c] != NULL; c++)
		{
			if (repmgr_conninfo_buf.len != 0)
				appendPQExpBufferChar(&repmgr_conninfo_buf, ' ');

			appendPQExpBuffer(&repmgr_conninfo_buf, "%s=",
							  barman_conninfo.keywords[c]);
			appendConnStrVal(&repmgr_conninfo_buf,
							 barman_conninfo.values[c]);
		}

		log_verbose(LOG_DEBUG,
					"repmgr database conninfo string on barman server: %s\n",
					repmgr_conninfo_buf.data);

		switch(options.upstream_node)
		{
			case NO_UPSTREAM_NODE:
				maxlen_snprintf(where_condition, "type='master'");
				break;
			default:
				maxlen_snprintf(where_condition, "id=%d", options.upstream_node);
				break;
		}

		initPQExpBuffer(&command_output);
		maxlen_snprintf(buf,
						"ssh %s \"psql -Aqt \\\"%s\\\" -c \\\""
						" SELECT conninfo"
						" FROM repmgr_%s.repl_nodes"
						" WHERE %s"
						" AND active"
						"\\\"\"", options.barman_server, repmgr_conninfo_buf.data,
						options.cluster_name, where_condition);

		termPQExpBuffer(&repmgr_conninfo_buf);

		command_success = local_command(buf, &command_output);

		if (command_success == false)
		{
			log_err(_("Unable to execute database query via Barman server\n"));
			exit(ERR_BARMAN);
		}
		maxlen_snprintf(recovery_conninfo_str, "%s", command_output.data);
		string_remove_trailing_newlines(recovery_conninfo_str);

		upstream_record_found = true;
		log_verbose(LOG_DEBUG,
					"upstream node conninfo string extracted via barman server: %s\n",
					recovery_conninfo_str);

		termPQExpBuffer(&command_output);
	}

	if (upstream_record_found == true)
	{
		/*  parse returned upstream conninfo string to recovery primary_conninfo params*/
		char	   *errmsg = NULL;
		bool	    parse_success;

		log_verbose(LOG_DEBUG, "parsing upstream conninfo string \"%s\"\n", recovery_conninfo_str);

		/* parse_conninfo_string() here will remove the upstream's `application_name`, if set */

		parse_success = parse_conninfo_string(recovery_conninfo_str, &recovery_conninfo, errmsg, true);
		if (parse_success == false)
		{
			log_err(_("Unable to parse conninfo string \"%s\" for upstream node:\n%s\n"),
					recovery_conninfo_str, errmsg);

			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}
	}
	else
	{
		/*
		 * If no upstream node record found, we'll abort with an error here,
		 * unless -F/--force is used, in which case we'll use the parameters
		 * provided on the command line (and assume the user knows what they're
		 * doing).
		 */

		if (!runtime_options.force)
		{
			log_err(_("No record found for upstream node\n"));
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}
	}


	/* If --replication-user was set, use that value for the primary_conninfo user */
	if (*runtime_options.replication_user)
	{
		param_set(&recovery_conninfo, "user", runtime_options.replication_user);
	}

	if (mode != barman)
	{

		/*
		 * Check the destination data directory can be used
		 * (in Barman mode, this directory will already have been created)
		 */

		if (!create_pg_dir(local_data_directory, runtime_options.force))
		{
			log_err(_("unable to use directory %s ...\n"),
					local_data_directory);
			log_hint(_("use -F/--force option to force this directory to be overwritten\n"));
			exit(ERR_BAD_CONFIG);
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
				PQfinish(source_conn);
				exit(ERR_BAD_CONFIG);
			}

			for (cell = options.tablespace_mapping.head; cell; cell = cell->next)
			{
				sqlquery_snprintf(sqlquery,
								  "SELECT spcname "
								  "  FROM pg_catalog.pg_tablespace "
								  " WHERE pg_tablespace_location(oid) = '%s'",
								  cell->old_dir);
				res = PQexec(source_conn, sqlquery);
				if (PQresultStatus(res) != PGRES_TUPLES_OK)
				{
					log_err(_("unable to execute tablespace query: %s\n"), PQerrorMessage(source_conn));
					PQclear(res);
					PQfinish(source_conn);
					exit(ERR_BAD_CONFIG);
				}

				if (PQntuples(res) == 0)
				{
					log_err(_("no tablespace matching path '%s' found\n"), cell->old_dir);
					PQclear(res);
					PQfinish(source_conn);
					exit(ERR_BAD_CONFIG);
				}
			}
		}

		/*
		 * Obtain configuration file locations
		 * We'll check to see whether the configuration files are in the data
		 * directory - if not we'll have to copy them via SSH, if copying
		 * requested.
		 *
		 * XXX: if configuration files are symlinks to targets outside the data
		 * directory, they won't be copied by pg_basebackup, but we can't tell
		 * this from the below query; we'll probably need to add a check for their
		 * presence and if missing force copy by SSH
		 */

		sqlquery_snprintf(sqlquery,
						  "  WITH dd AS ( "
						  "    SELECT setting AS data_directory"
						  "      FROM pg_catalog.pg_settings "
						  "     WHERE name = 'data_directory' "
						  "  ) "
						  "    SELECT DISTINCT(sourcefile), "
						  "           regexp_replace(sourcefile, '^.*\\/', '') AS filename, "
						  "           sourcefile ~ ('^' || dd.data_directory) AS in_data_dir "
						  "      FROM dd, pg_catalog.pg_settings ps "
						  "     WHERE sourcefile IS NOT NULL "
						  "  ORDER BY 1 ");

		log_debug(_("standby clone: %s\n"), sqlquery);
		res = PQexec(source_conn, sqlquery);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			log_err(_("unable to retrieve configuration file locations: %s\n"),
					PQerrorMessage(source_conn));
			PQclear(res);
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}

		/*
		 * allocate memory for config file array - number of rows returned from
		 * above query + 2 for pg_hba.conf, pg_ident.conf
		 */

		config_file_list_init(&config_files, PQntuples(res) + 2);

		for (i = 0; i < PQntuples(res); i++)
		{
			config_file_list_add(&config_files,
								 PQgetvalue(res, i, 0),
								 PQgetvalue(res, i, 1),
								 strcmp(PQgetvalue(res, i, 2), "t") == 1 ? true : false);
		}

		PQclear(res);

		/* Fetch locations of pg_hba.conf and pg_ident.conf */
		sqlquery_snprintf(sqlquery,
						  "  WITH dd AS ( "
						  "    SELECT setting AS data_directory"
						  "      FROM pg_catalog.pg_settings "
						  "     WHERE name = 'data_directory' "
						  "  ) "
						  "    SELECT ps.setting, "
						  "           regexp_replace(setting, '^.*\\/', '') AS filename, "
						  "           ps.setting ~ ('^' || dd.data_directory) AS in_data_dir "
						  "      FROM dd, pg_catalog.pg_settings ps "
						  "     WHERE ps.name IN ('hba_file', 'ident_file') "
						  "  ORDER BY 1 ");

		log_debug(_("standby clone: %s\n"), sqlquery);
		res = PQexec(source_conn, sqlquery);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			log_err(_("unable to retrieve configuration file locations: %s\n"),
					PQerrorMessage(source_conn));
			PQclear(res);
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}

		for (i = 0; i < PQntuples(res); i++)
		{
			config_file_list_add(&config_files,
								 PQgetvalue(res, i, 0),
								 PQgetvalue(res, i, 1),
								 strcmp(PQgetvalue(res, i, 2), "t") == 1 ? true : false);
		}

		PQclear(res);
	}

	/*
	 * If replication slots requested, create appropriate slot on
	 * the primary; this must be done before pg_start_backup() is
	 * issued, either by us or by pg_basebackup.
	 *
	 * Replication slots are not supported (and not very useful
	 * anyway) in Barman mode.
	 */
	if (mode != barman && options.use_replication_slots)
	{
		PQExpBufferData event_details;
		initPQExpBuffer(&event_details);

		if (create_replication_slot(source_conn, repmgr_slot_name, server_version_num, &event_details) == false)
		{
			log_err("%s\n", event_details.data);

			create_event_record(primary_conn,
								&options,
								options.node,
								"standby_clone",
								false,
								event_details.data);

			PQfinish(source_conn);
			exit(ERR_DB_QUERY);
		}

		termPQExpBuffer(&event_details);
	}

	if (mode == rsync)
	{
		log_notice(_("starting backup (using rsync)...\n"));
	}
	else if (mode == barman)
	{
		log_notice(_("getting backup from Barman...\n"));
	}
	else if (mode == pg_basebackup)
	{
		log_notice(_("starting backup (using pg_basebackup)...\n"));
		if (runtime_options.fast_checkpoint == false)
			log_hint(_("this may take some time; consider using the -c/--fast-checkpoint option\n"));
	}

	if (mode == barman || mode == rsync)
	{
		char		command[MAXLEN];
		char		filename[MAXLEN];
		char		buf[MAXLEN];
		char		basebackups_directory[MAXLEN];
		char		backup_id[MAXLEN] = "";
		char	   *p, *q;
		TablespaceDataList tablespace_list = { NULL, NULL };
		TablespaceDataListCell *cell_t;

		PQExpBufferData tablespace_map;
		bool		tablespace_map_rewrite = false;

		if (mode == barman)
		{

			/*
			 * Locate Barman's base backups directory
			 */

			get_barman_property(basebackups_directory, "basebackups_directory", local_repmgr_directory);

			/*
			 * Read the list of backup files into a local file. In the
			 * process:
			 *
			 * - determine the backup ID;
			 * - check, and remove, the prefix;
			 * - detect tablespaces;
			 * - filter files in one list per tablespace;
			 */

			{
				FILE *fi; /* input stream */
				FILE *fd; /* output for data.txt */
				char prefix[MAXLEN];
				char output[MAXLEN];
				int n;

				maxlen_snprintf(command, "%s list-files --target=data %s latest",
								make_barman_ssh_command(),
								options.cluster_name);

				log_verbose(LOG_DEBUG, "executing:\n  %s\n", command);

				fi = popen(command, "r");
				if (fi == NULL)
				{
					log_err("Cannot launch command: %s\n", command);
					exit(ERR_BARMAN);
				}

				fd = fopen(datadir_list_filename, "w");
				if (fd == NULL)
				{
					log_err("Cannot open file: %s\n", datadir_list_filename);
					exit(ERR_INTERNAL);
				}

				maxlen_snprintf(prefix, "%s/", basebackups_directory);

				while (fgets(output, MAXLEN, fi) != NULL)
				{
					/*
					 * Remove prefix
					 */
					p = string_skip_prefix(prefix, output);
					if (p == NULL)
					{
						log_err("Unexpected output from \"barman list-files\": %s\n",
								output);
						exit(ERR_BARMAN);
					}

					/*
					 * Remove and note backup ID; copy backup.info
					 */
					if (! strcmp(backup_id, ""))
					{
						FILE *fi2;

						n = strcspn(p, "/");

						strncpy(backup_id, p, n);

						strncat(prefix,backup_id,MAXLEN-1);
						strncat(prefix,"/",MAXLEN-1);
						p = string_skip_prefix(backup_id, p);
						p = string_skip_prefix("/", p);

						log_debug("Barman backup_id is: %s\n", backup_id);

						/*
						 * Copy backup.info
						 */
						maxlen_snprintf(command,
										"rsync -a %s:%s/%s/backup.info %s",
										options.barman_server,
										basebackups_directory,
										backup_id,
										local_repmgr_directory);
						(void)local_command(
							command,
							NULL);

						/*
						 * Get tablespace data
						 */
						maxlen_snprintf(filename, "%s/backup.info",
										local_repmgr_directory);
						fi2 = fopen(filename, "r");
						if (fi2 == NULL)
						{
							log_err("Cannot open file: %s\n", filename);
							exit(ERR_INTERNAL);
						}
						while (fgets(buf, MAXLEN, fi2) != NULL)
						{
							q = string_skip_prefix("tablespaces=", buf);
							if (q != NULL && strncmp(q, "None\n", 5))
							{
								get_tablespace_data_barman
									(q, &tablespace_list);
							}
							q = string_skip_prefix("version=", buf);
							if (q != NULL)
							{
								server_version_num = strtol(q, NULL, 10);
							}
						}
						fclose(fi2);
						unlink(filename);

						continue;
					}

					/*
					 * Skip backup.info
					 */
					if (string_skip_prefix("backup.info", p))
						continue;

					/*
					 * Filter data directory files
					 */
					if ((q = string_skip_prefix("data/", p)) != NULL)
					{
						fputs(q, fd);
						continue;
					}

					/*
					 * Filter other files (i.e. tablespaces)
					 */
					for (cell_t = tablespace_list.head; cell_t; cell_t = cell_t->next)
					{
						if ((q = string_skip_prefix(cell_t->oid, p)) != NULL && *q == '/')
						{
							if (cell_t->f == NULL)
							{
								maxlen_snprintf(filename, "%s/%s.txt", local_repmgr_directory, cell_t->oid);
								cell_t->f = fopen(filename, "w");
								if (cell_t->f == NULL)
								{
									log_err("Cannot open file: %s\n", filename);
									exit(ERR_INTERNAL);
								}
							}
							fputs(q + 1, cell_t->f);
							break;
						}
					}
				}

				fclose(fd);

				pclose(fi);
			}

			/* For 9.5 and greater, create our own tablespace_map file */
			if (server_version_num >= 90500)
			{
				initPQExpBuffer(&tablespace_map);
			}

			/*
			 * As of Barman version 1.6.1, the file structure of a backup
			 * is as follows:
			 *
			 * base/ - base backup
			 * wals/ - WAL files associated to the backup
			 *
			 * base/<ID> - backup files
			 *
			 *   here ID has the standard timestamp form yyyymmddThhmmss
			 *
			 * base/<ID>/backup.info - backup metadata, in text format
			 * base/<ID>/data        - data directory
			 * base/<ID>/<OID>       - tablespace with the given oid
			 */

			/*
			 * Copy all backup files from the Barman server
			 */

			maxlen_snprintf(command,
							"rsync --progress -a --files-from=%s %s:%s/%s/data %s",
							datadir_list_filename,
							options.barman_server,
							basebackups_directory,
							backup_id,
							local_data_directory);
			(void)local_command(
				command,
				NULL);
			unlink(datadir_list_filename);

			/*
			 * We must create some PGDATA subdirectories because they are
			 * not included in the Barman backup.
			 *
			 * See class RsyncBackupExecutor in the Barman source (barman/backup_executor.py)
			 * for a definitive list of excluded directories.
			 */
			{
				const char* const dirs[] = {
					/* Only from 10 */
					"pg_wal",
					/* Only from 9.5 */
					"pg_commit_ts",
					/* Only from 9.4 */
					"pg_dynshmem", "pg_logical", "pg_logical/snapshots", "pg_logical/mappings", "pg_replslot",
					/* Already in 9.3 */
					"pg_notify", "pg_serial", "pg_snapshots", "pg_stat", "pg_stat_tmp", "pg_tblspc",
					"pg_twophase", "pg_xlog", 0
				};
				const int vers[] = {
					100000,
					90500,
					90400, 90400, 90400, 90400, 90400,
					0, 0, 0, 0, 0, 0,
					0, -100000, 0
				};
				for (i = 0; dirs[i]; i++)
				{
					/* directory exists in newer versions than this server - skip */
					if (vers[i] > 0 && server_version_num < vers[i])
						continue;

					/* directory existed in earlier versions than this server but has been removed/renamed - skip */
					if (vers[i] < 0 && server_version_num >= abs(vers[i]))
						continue;

					maxlen_snprintf(filename, "%s/%s", local_data_directory, dirs[i]);
					if (mkdir(filename, S_IRWXU) != 0 && errno != EEXIST)
					{
						log_err(_("unable to create the %s directory\n"), dirs[i]);
						exit(ERR_INTERNAL);
					}
				}
			}
		}
		else if (mode == rsync)
		{
			/* For 9.5 and greater, create our own tablespace_map file */
			if (server_version_num >= 90500)
			{
				initPQExpBuffer(&tablespace_map);
			}

			if (start_backup(source_conn, first_wal_segment, runtime_options.fast_checkpoint, server_version_num) == false)
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
			 * Exit code 0 means no error, but we want to ignore exit code 24 as well
			 * as rsync returns that code on "Partial transfer due to vanished source files".
			 * It's quite common for this to happen on the data directory, particularly
			 * with long running rsync on a busy server.
			 */
			if (WIFEXITED(r) && WEXITSTATUS(r) && WEXITSTATUS(r) != 24)
			{
				log_err(_("standby clone: failed copying master data directory '%s'\n"),
							master_data_directory);
				r = retval = ERR_BAD_RSYNC;
				goto stop_backup;
			}

			/* Read backup label copied from primary */
			if (read_backup_label(local_data_directory, &backup_label) == false)
			{
				r = retval = ERR_BAD_BACKUP_LABEL;
				goto stop_backup;
			}

			/* Copy tablespaces and, if required, remap to a new location */
			retval = get_tablespace_data(source_conn, &tablespace_list);
			if (retval != SUCCESS) goto stop_backup;
		}

		for (cell_t = tablespace_list.head; cell_t; cell_t = cell_t->next)
		{
			bool mapping_found = false;
			TablespaceListCell *cell;
			char *tblspc_dir_dest;

			/* Check if tablespace path matches one of the provided tablespace mappings */
			if (options.tablespace_mapping.head != NULL)
			{
				for (cell = options.tablespace_mapping.head; cell; cell = cell->next)
				{
					if (strcmp(cell_t->location, cell->old_dir) == 0)
					{
						mapping_found = true;
						break;
					}
				}
			}

			if (mapping_found == true)
			{
				tblspc_dir_dest = cell->new_dir;
				log_debug(_("mapping source tablespace '%s' (OID %s) to '%s'\n"),
						  cell_t->location, cell_t->oid, tblspc_dir_dest);
			}
			else
			{
				tblspc_dir_dest = cell_t->location;
			}

			/*
			 * Tablespace file copy
			 */

			if (mode == barman)
			{
				create_pg_dir(cell_t->location, false);

				if (cell_t->f != NULL) /* cell_t->f == NULL iff the tablespace is empty */
				{
					maxlen_snprintf(command,
									"rsync --progress -a --files-from=%s/%s.txt %s:%s/%s/%s %s",
									local_repmgr_directory,
									cell_t->oid,
									options.barman_server,
									basebackups_directory,
									backup_id,
									cell_t->oid,
									tblspc_dir_dest);
					(void)local_command(
						command,
						NULL);
					fclose(cell_t->f);
					maxlen_snprintf(filename,
									"%s/%s.txt",
									local_repmgr_directory,
									cell_t->oid);
					unlink(filename);
				}
			}
			else if (mode == rsync)
			{
				/* Copy tablespace directory */
				r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
									  cell_t->location, tblspc_dir_dest,
									  true, server_version_num);

				/*
				 * Exit code 0 means no error, but we want to ignore exit code 24 as well
				 * as rsync returns that code on "Partial transfer due to vanished source files".
				 * It's quite common for this to happen on the data directory, particularly
				 * with long running rsync on a busy server.
				 */
				if (WIFEXITED(r) && WEXITSTATUS(r) && WEXITSTATUS(r) != 24)
				{
					log_err(_("standby clone: failed copying tablespace directory '%s'\n"),
							cell_t->location);
					r = retval = ERR_BAD_RSYNC;
					goto stop_backup;
				}
			}

			/*
			 * If a valid mapping was provide for this tablespace, arrange for it to
			 * be remapped
			 * (if no tablespace mapping was provided, the link will be copied as-is
			 * by pg_basebackup or rsync and no action is required)
			 */
			if (mapping_found == true || mode == barman)
			{
				/* 9.5 and later - append to the tablespace_map file */
				if (server_version_num >= 90500)
				{
					tablespace_map_rewrite = true;
					appendPQExpBuffer(&tablespace_map,
									  "%s %s\n",
									  cell_t->oid,
									  tblspc_dir_dest);
				}
				/* Pre-9.5, we have to manipulate the symlinks in pg_tblspc/ ourselves */
				else
				{
					PQExpBufferData tblspc_symlink;

					initPQExpBuffer(&tblspc_symlink);
					appendPQExpBuffer(&tblspc_symlink, "%s/pg_tblspc/%s",
									  local_data_directory,
									  cell_t->oid);

					if (unlink(tblspc_symlink.data) < 0 && errno != ENOENT)
					{
						log_err(_("unable to remove tablespace symlink %s\n"), tblspc_symlink.data);

						r = retval = ERR_BAD_BASEBACKUP;
						goto stop_backup;
					}

					if (symlink(tblspc_dir_dest, tblspc_symlink.data) < 0)
					{
						log_err(_("unable to create tablespace symlink from %s to %s\n"), tblspc_symlink.data, tblspc_dir_dest);

						r = retval = ERR_BAD_BASEBACKUP;
						goto stop_backup;
					}
				}
			}
		}

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
	 * If `--copy-external-config-files` was provided, copy any configuration
	 * files detected to the appropriate location. Any errors encountered
	 * will not be treated as fatal.
	 */
	if (runtime_options.copy_external_config_files && upstream_record_found)
	{
		int i;
		t_configfile_info *file;

		char *host;

		/* get host from upstream record */
		host = param_get(&recovery_conninfo, "host");

		if (host == NULL)
			host = runtime_options.host;

		log_verbose(LOG_DEBUG, "host for config file is: %s\n", host);
		log_notice(_("copying external configuration files from upstream node\n"));

		r = test_ssh_connection(host, runtime_options.remote_user);
		if (r != 0)
		{
			log_err(_("remote host %s is not reachable via SSH - unable to copy external configuration files\n"),
					   host);
		}
		else
		{
			for (i = 0; i < config_files.entries; i++)
			{
				char dest_path[MAXPGPATH];
				file = config_files.files[i];

				/*
				 * Skip files in the data directory - these will be copied during
				 * the main backup
				 */
				if (file->in_data_directory == true)
					continue;

				if (runtime_options.copy_external_config_files_destination == CONFIG_FILE_SAMEPATH)
				{
					strncpy(dest_path, file->filepath, MAXPGPATH);
				}
				else
				{
					snprintf(dest_path, MAXPGPATH,
							 "%s/%s",
							 local_data_directory,
							 file->filename);
				}

				r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
									  file->filepath, dest_path, false, server_version_num);
				if (WEXITSTATUS(r))
				{
					log_err(_("standby clone: unable to copy config file '%s'\n"),
							file->filename);
				}
			}
		}
	}

	/*
	 * When using rsync, copy pg_control file last, emulating the base backup
	 * protocol.
	 */
	if (mode == rsync)
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
		if (WEXITSTATUS(r))
		{
			log_warning(_("standby clone: failed copying master control file '%s'\n"),
						master_control_file);
			retval = ERR_BAD_SSH;
			goto stop_backup;
		}
	}

stop_backup:

	if (mode == rsync && pg_start_backup_executed)
	{
		log_notice(_("notifying master about backup completion...\n"));
		if (stop_backup(source_conn, last_wal_segment, server_version_num) == false)
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
			drop_replication_slot(source_conn, repmgr_slot_name);
		}

		log_err(_("unable to take a base backup of the master server\n"));
		log_warning(_("destination directory (%s) may need to be cleaned up manually\n"),
					local_data_directory);

		PQfinish(source_conn);
		exit(retval);
	}

	/*
	 * Clean up any $PGDATA subdirectories which may contain
	 * files which won't be removed by rsync and which could
	 * be stale or are otherwise not required
	 */
	if (mode == rsync)
	{
		char	dirpath[MAXLEN] = "";

		if (runtime_options.force)
		{
			/*
			 * Remove any existing WAL from the target directory, since
			 * rsync's --exclude option doesn't do it.
			 */

			if (server_version_num >= 100000)
			{
				maxlen_snprintf(dirpath, "%s/pg_wal/", local_data_directory);
			}
			else
			{
				maxlen_snprintf(dirpath, "%s/pg_xlog/", local_data_directory);
			}

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
	}


	/* Finally, write the recovery.conf file */

	create_recovery_file(local_data_directory, &recovery_conninfo);

	if (mode == barman)
	{
		/* In Barman mode, remove local_repmgr_directory */
		rmtree(local_repmgr_directory, true);
	}

	switch(mode)
	{
		case rsync:
			log_notice(_("standby clone (using rsync) complete\n"));
			break;

		case pg_basebackup:
			log_notice(_("standby clone (using pg_basebackup) complete\n"));
			break;

		case barman:
			log_notice(_("standby clone (from Barman) complete\n"));
			break;
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
						  _("; backup method: "));

		switch(mode)
		{
			case rsync:
				appendPQExpBuffer(&event_details, "rsync");
				break;
			case pg_basebackup:
				appendPQExpBuffer(&event_details, "pg_basebackup");
				break;
			case barman:
				appendPQExpBuffer(&event_details, "barman");
				break;
		}

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

	PQfinish(source_conn);
	exit(retval);
}

static void
parse_lsn(XLogRecPtr *ptr, const char *str)
{
	uint32 high, low;

	if (sscanf(str, "%x/%x", &high, &low) != 2)
		return;

	*ptr = (((XLogRecPtr)high) << 32) + (XLogRecPtr)low;

	return;
}

static XLogRecPtr
parse_label_lsn(const char *label_key, const char *label_value)
{
	XLogRecPtr ptr = InvalidXLogRecPtr;

	parse_lsn(&ptr, label_value);

	/* parse_lsn() will not modify ptr if it can't parse the label value */
	if (ptr == InvalidXLogRecPtr)
	{
		log_err(_("Couldn't parse backup label entry \"%s: %s\" as lsn"),
				label_key, label_value);
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
	 * `pg_ctl promote` returns immediately and (prior to 10.0) has no -w option
	 * so we can't be sure when or if the promotion completes.
	 * For now we'll poll the server until the default timeout (60 seconds)
	 */

	if (*options.service_promote_command)
	{
		maxlen_snprintf(script, "%s", options.service_promote_command);
	}
	else
	{
		maxlen_snprintf(script, "%s -D %s promote",
						make_pg_path("pg_ctl"), data_dir);
	}

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

	for (i = 0; i < promote_check_timeout; i += promote_check_interval)
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
	int			master_id = UNKNOWN_NODE_ID,
				original_upstream_node_id = UNKNOWN_NODE_ID;

	int			r,
				retval;
	char		data_dir[MAXPGPATH];

	bool        success;

	t_conninfo_param_list recovery_conninfo;
	t_node_info local_node_record = T_NODE_INFO_INITIALIZER;
	int         query_result;
	char	   *errmsg = NULL;
	bool        parse_success;

	log_debug("do_standby_follow()\n");

	/*
	 * If -h/--host wasn't provided, attempt to connect to standby
	 * to determine primary, and carry out some other checks while we're
	 * at it.
	 */
	if (runtime_options.host_param_provided == false)
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
		while (PQstatus(master_conn) != CONNECTION_OK && runtime_options.wait_for_master);

		if (PQstatus(master_conn) != CONNECTION_OK)
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
		master_conn = establish_db_connection_by_params((const char**)source_conninfo.keywords, (const char**)source_conninfo.values, true);

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

		PQExpBufferData event_details;
		initPQExpBuffer(&event_details);

		if (create_replication_slot(master_conn, repmgr_slot_name, server_version_num, &event_details) == false)
		{
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

		termPQExpBuffer(&event_details);
	}

	/* Initialise connection parameters to write as `primary_conninfo` */
	initialize_conninfo_params(&recovery_conninfo, false);
	conn_to_param_list(master_conn, &recovery_conninfo);

	/* Set the default application name to this node's name */
	param_set(&recovery_conninfo, "application_name", options.node_name);

	/* If --replication-user was set, use that value for the primary_conninfo user */
	if (*runtime_options.replication_user)
	{
		param_set(&recovery_conninfo, "user", runtime_options.replication_user);
	}


	/*
	 * Fetch our node record so we can write application_name, if set,
	 * and to get the upstream node ID, which we'll need to know if
	 * replication slots are in use and we want to delete the old slot.
	 */
	query_result = get_node_record(master_conn,
								   options.cluster_name,
								   options.node,
								   &local_node_record);

	if (query_result != 1)
	{
		/* this shouldn't happen, but if it does we'll plough on regardless */
		log_warning(_("unable to retrieve record for node %i\n"),
					options.node);
	}
	else
	{
		t_conninfo_param_list local_node_conninfo;

		initialize_conninfo_params(&local_node_conninfo, false);

		parse_success = parse_conninfo_string(local_node_record.conninfo_str, &local_node_conninfo, errmsg, false);

		if (parse_success == false)
		{
			/* this shouldn't happen, but if it does we'll plough on regardless */
			log_warning(_("unable to parse conninfo string \"%s\":\n%s\n"),
					local_node_record.conninfo_str, errmsg);
		}
		else
		{
			char *application_name = param_get(&local_node_conninfo, "application_name");

			if (application_name != NULL && strlen(application_name))
				param_set(&recovery_conninfo, "application_name", application_name);
		}

		if (local_node_record.upstream_node_id != UNKNOWN_NODE_ID)
		{
			original_upstream_node_id = local_node_record.upstream_node_id;
		}
		else
		{
			original_upstream_node_id = master_id;
		}
	}

	log_info(_("changing standby's master to node %i\n"), master_id);

	if (!create_recovery_file(data_dir, &recovery_conninfo))
		exit(ERR_BAD_CONFIG);

	/* Finally, restart the service */
	if (*options.service_restart_command)
	{
		maxlen_snprintf(script, "%s", options.service_restart_command);
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
	 * If replication slots are in use, and an inactive one for this node
	 * (a former standby) exists on the former upstream, drop it.
	 */

	if (options.use_replication_slots && runtime_options.host_param_provided == false && original_upstream_node_id != UNKNOWN_NODE_ID)
	{
		t_node_info upstream_node_record  = T_NODE_INFO_INITIALIZER;
		int			upstream_query_result;

		log_verbose(LOG_INFO, "attempting to remove replication slot from old upstream node %i\n",
					original_upstream_node_id);

		/* XXX should we poll for server restart? */
		conn = establish_db_connection(options.conninfo, true);

		upstream_query_result = get_node_record(conn,
												options.cluster_name,
												original_upstream_node_id,
												&upstream_node_record);

		PQfinish(conn);


		if (upstream_query_result != 1)
		{
			log_warning(_("unable to retrieve node record for old upstream node %i"),
						original_upstream_node_id);
		}
		else
		{
			conn = establish_db_connection_quiet(upstream_node_record.conninfo_str);
			if (PQstatus(conn) != CONNECTION_OK)
			{
				log_info(_("unable to connect to old upstream node %i to remove replication slot\n"),
						 original_upstream_node_id);
				log_hint(_("if reusing this node, you should manually remove any inactive replication slots\n"));
			}
			else
			{
				drop_replication_slot_if_exists(conn,
												original_upstream_node_id,
												local_node_record.slot_name);
			}
		}
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

	log_notice(_("STANDBY FOLLOW successful\n"));

	create_event_record(master_conn,
						&options,
						options.node,
						"standby_follow",
						true,
						NULL);

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
	char        remote_data_directory[MAXPGPATH] = "";
	char        remote_path[MAXPGPATH] = "";

	int         remote_node_id;
	char        remote_node_replication_state[MAXLEN] = "";
	char        remote_archive_config_dir[MAXLEN];
	char	    remote_pg_rewind[MAXLEN];
	int			i,
				r = 0;

	PQExpBufferData remote_command_str;
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
	if (runtime_options.pg_rewind_supplied == true && server_version_num >= 90500)
	{
		log_notice(_("--pg_rewind not required for PostgreSQL 9.5 and later\n"));
	}

	/*
	 * TODO: check that standby's upstream node is the primary
	 * (it's probably not feasible to switch over to a cascaded standby)
	 */

	/* Check that primary is available */
	remote_conn = get_master_connection(local_conn, options.cluster_name, &remote_node_id, remote_conninfo);

	if (PQstatus(remote_conn) != CONNECTION_OK)
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
		log_err(_("unable to connect via SSH to host %s, user %s\n"), remote_host, runtime_options.remote_user);
	}

	if (get_pg_setting(remote_conn, "data_directory", remote_data_directory) == false)
	{
		log_err(_("unable to retrieve master's data directory location\n"));
		PQfinish(remote_conn);
		PQfinish(local_conn);
		exit(ERR_DB_CON);
	}

	log_debug("master's data directory is: %s\n", remote_data_directory);

	maxlen_snprintf(remote_path,
					"%s/PG_VERSION",
					remote_data_directory);

	initPQExpBuffer(&remote_command_str);
	appendPQExpBuffer(&remote_command_str, "ls ");
	appendShellString(&remote_command_str, remote_path);
	appendPQExpBuffer(&remote_command_str, " >/dev/null 2>&1 && echo 1 || echo 0");

	initPQExpBuffer(&command_output);

	(void)remote_command(
		remote_host,
		runtime_options.remote_user,
		remote_command_str.data,
		&command_output);

	termPQExpBuffer(&remote_command_str);

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
		if (runtime_options.pg_rewind_supplied == true)
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

		initPQExpBuffer(&remote_command_str);

		if (strcmp(remote_pg_rewind, "pg_rewind") == 0)
			appendPQExpBuffer(&remote_command_str, "which ");
		else
			appendPQExpBuffer(&remote_command_str, "ls ");

		appendShellString(&remote_command_str, remote_pg_rewind);
		appendPQExpBuffer(&remote_command_str, " >/dev/null 2>&1 && echo 1 || echo 0");

		initPQExpBuffer(&command_output);

		(void)remote_command(
			remote_host,
			runtime_options.remote_user,
			remote_command_str.data,
			&command_output);

		termPQExpBuffer(&remote_command_str);

		if (*command_output.data == '0')
		{
			log_err(_("unable to find pg_rewind on the remote server\n"));
			if (strcmp(remote_pg_rewind, "pg_rewind") == 0)
				log_hint(_("set pg_bindir in repmgr.conf or provide with -b/--pg_bindir\n"));
			else
				log_detail(_("expected location is: %s\n"), remote_pg_rewind);

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

		initPQExpBuffer(&remote_command_str);
		appendPQExpBuffer(&remote_command_str, "ls ");

		appendShellString(&remote_command_str, runtime_options.remote_config_file);
		appendPQExpBuffer(&remote_command_str, " >/dev/null 2>&1 && echo 1 || echo 0");

		initPQExpBuffer(&command_output);

		(void)remote_command(
			remote_host,
			runtime_options.remote_user,
			remote_command_str.data,
			&command_output);

		termPQExpBuffer(&remote_command_str);

		if (*command_output.data == '0')
		{
			log_err(_("unable to find the specified repmgr configuration file on remote server\n"));
			exit(ERR_BAD_CONFIG);
		}

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
		bool		remote_config_file_found = false;

		const char *config_paths[] = {
			runtime_options.config_file,
			"/etc/repmgr.conf",
			NULL
		};

		log_verbose(LOG_INFO, _("no remote configuration file provided - checking default locations\n"));

		for (i = 0; config_paths[i] && remote_config_file_found == false; ++i)
		{
			/*
			 * Don't attempt to check for an empty filename - this might be the case
			 * if no local configuration file was found.
			 */
			if (!strlen(config_paths[i]))
				continue;

			log_verbose(LOG_INFO, _("checking \"%s\"\n"), config_paths[i]);

			initPQExpBuffer(&remote_command_str);
			appendPQExpBuffer(&remote_command_str, "ls ");

			appendShellString(&remote_command_str, config_paths[i]);
			appendPQExpBuffer(&remote_command_str, " >/dev/null 2>&1 && echo 1 || echo 0");

			initPQExpBuffer(&command_output);

			(void)remote_command(
				remote_host,
				runtime_options.remote_user,
				remote_command_str.data,
				&command_output);

			termPQExpBuffer(&remote_command_str);

			if (*command_output.data == '1')
			{
				strncpy(runtime_options.remote_config_file, config_paths[i], MAXLEN);
				log_verbose(LOG_INFO, _("configuration file \"%s\" found on remote server\n"),
							runtime_options.remote_config_file);
				remote_config_file_found = true;
			}

			termPQExpBuffer(&command_output);
		}

		if (remote_config_file_found == false)
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

		initPQExpBuffer(&remote_command_str);
		appendPQExpBuffer(&remote_command_str,
						  "%s standby archive-config -f ",
						  make_pg_path("repmgr"));
		appendShellString(&remote_command_str, runtime_options.remote_config_file);
		appendPQExpBuffer(&remote_command_str,
						  " --config-archive-dir=");
		appendShellString(&remote_command_str, remote_archive_config_dir);

		log_debug("Executing:\n%s\n", remote_command_str.data);

		initPQExpBuffer(&command_output);

		(void)remote_command(
			remote_host,
			runtime_options.remote_user,
			remote_command_str.data,
			&command_output);

		termPQExpBuffer(&command_output);
		termPQExpBuffer(&remote_command_str);
	}

	/*
	 * Stop the remote primary
	 *
	 * We'll issue the pg_ctl command but not force it not to wait; we'll check
	 * the connection from here - and error out if no shutdown is detected
	 * after a certain time.
	 */

	/*
	 * TODO
	 * - notify repmgrd instances that this is a controlled
	 *   event so they don't initiate failover
	 */

	initPQExpBuffer(&remote_command_str);

	if (*options.service_stop_command)
	{
		appendPQExpBuffer(&remote_command_str, "%s", options.service_stop_command);
	}
	else
	{
		appendPQExpBuffer(&remote_command_str,
						  "%s -D ",
						  make_pg_path("pg_ctl"));
		appendShellString(&remote_command_str, remote_data_directory);
		appendPQExpBuffer(&remote_command_str,
						  " -m %s -W stop >/dev/null 2>&1 && echo 1 || echo 0",
						  runtime_options.pg_ctl_mode);
	}

	initPQExpBuffer(&command_output);

	// XXX handle failure

	(void)remote_command(
		remote_host,
		runtime_options.remote_user,
		remote_command_str.data,
		&command_output);

	termPQExpBuffer(&command_output);
	termPQExpBuffer(&remote_command_str);

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

			maxlen_snprintf(remote_path,
							"%s/postmaster.pid",
							remote_data_directory);

			initPQExpBuffer(&remote_command_str);

			appendPQExpBuffer(&remote_command_str, "ls ");
			appendShellString(&remote_command_str, remote_path);
			appendPQExpBuffer(&remote_command_str, " >/dev/null 2>&1 && echo 1 || echo 0");

			initPQExpBuffer(&command_output);

			command_success = remote_command(
				remote_host,
				runtime_options.remote_user,
				remote_command_str.data,
				&command_output);

			termPQExpBuffer(&remote_command_str);

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
		/* Execute pg_rewind */

		initPQExpBuffer(&remote_command_str);

		appendShellString(&remote_command_str, remote_pg_rewind);
		appendPQExpBuffer(&remote_command_str, " -D ");
		appendShellString(&remote_command_str, remote_data_directory);
		appendPQExpBuffer(&remote_command_str, " --source-server=\\'%s\\'", options.conninfo);

		log_notice("Executing pg_rewind on old master server\n");
		log_debug("pg_rewind command is:\n%s\n", remote_command_str.data);

		initPQExpBuffer(&command_output);

		// XXX handle failure

		(void)remote_command(
			remote_host,
			runtime_options.remote_user,
			remote_command_str.data,
			&command_output);

		termPQExpBuffer(&command_output);
		termPQExpBuffer(&remote_command_str);

		/* Restore any previously archived config files */
		initPQExpBuffer(&remote_command_str);

		/* --force */
		appendPQExpBuffer(&remote_command_str,
						  "%s standby restore-config -D ",
						  make_pg_path("repmgr"));
		appendShellString(&remote_command_str, remote_data_directory);

		/*
		 * append pass the configuration file to prevent spurious errors
		 * about missing cluster_name
		 */
		appendPQExpBuffer(&remote_command_str,
						  " -f ");
		appendShellString(&remote_command_str, runtime_options.remote_config_file);

		appendPQExpBuffer(&remote_command_str,
						  " --config-archive-dir=");
		appendShellString(&remote_command_str, remote_archive_config_dir);

		initPQExpBuffer(&command_output);

		// XXX handle failure

		(void)remote_command(
			remote_host,
			runtime_options.remote_user,
			remote_command_str.data,
			&command_output);

		termPQExpBuffer(&command_output);
		termPQExpBuffer(&remote_command_str);

		/* remove any recovery.done file copied in by pg_rewind */
		initPQExpBuffer(&remote_command_str);
		maxlen_snprintf(remote_path,
						"%s/recovery.done",
						remote_data_directory);

		appendPQExpBuffer(&remote_command_str, "test -e ");
		appendShellString(&remote_command_str, remote_path);
		appendPQExpBuffer(&remote_command_str, " && rm -f ");
		appendShellString(&remote_command_str, remote_path);

		initPQExpBuffer(&command_output);

		// XXX handle failure

		(void)remote_command(
			remote_host,
			runtime_options.remote_user,
			remote_command_str.data,
			&command_output);

		termPQExpBuffer(&command_output);
		termPQExpBuffer(&remote_command_str);
	}
	else
	{
		/*
		 * For 9.3/9.4, if pg_rewind is not available on the remote server,
		 * we'll need to force a reclone of the standby using rsync - this may
		 * take some time on larger databases, so use with care!
		 *
		 * Note that following this clone we'll be using `repmgr standby follow`
		 * to start the server - that will mean recovery.conf will be created
		 * for a second time, but as this is a workaround for the absence
		 * of pg_rewind. It's preferable to have `repmgr standby follow` start
		 * the remote database as it can access the remote config file
		 * directly.
		 *
		 * XXX will not work if runtime_options.remote_config_file is empty!
		 */

		initPQExpBuffer(&remote_command_str);

		format_db_cli_params(options.conninfo, repmgr_db_cli_params);

		appendPQExpBuffer(&remote_command_str,
						  "%s -D ",
						  make_pg_path("repmgr"));
		appendShellString(&remote_command_str, remote_data_directory);
		appendPQExpBuffer(&remote_command_str, " -f ");
		appendShellString(&remote_command_str, runtime_options.remote_config_file);
		appendPQExpBuffer(&remote_command_str,
						 " %s --rsync-only --force --ignore-external-config-files standby clone",
						 repmgr_db_cli_params);

		log_debug("Executing:\n%s\n", remote_command_str.data);

		initPQExpBuffer(&command_output);

		(void)remote_command(
			remote_host,
			runtime_options.remote_user,
			remote_command_str.data,
			&command_output);

		termPQExpBuffer(&command_output);
		termPQExpBuffer(&remote_command_str);
	}

	/*
	 * Execute `repmgr standby follow` to create recovery.conf and start
	 * the remote server
	 */
	format_db_cli_params(options.conninfo, repmgr_db_cli_params);

	initPQExpBuffer(&remote_command_str);
	appendPQExpBuffer(&remote_command_str,
					  "%s -D ",
					  make_pg_path("repmgr"));
	appendShellString(&remote_command_str, remote_data_directory);
	appendPQExpBuffer(&remote_command_str, " -f ");
	appendShellString(&remote_command_str, runtime_options.remote_config_file);
	appendPQExpBuffer(&remote_command_str,
					  " %s standby follow",
					  repmgr_db_cli_params);

	log_debug("Executing:\n%s\n", remote_command_str.data);

	(void)remote_command(
		remote_host,
		runtime_options.remote_user,
		remote_command_str.data,
		NULL);

	termPQExpBuffer(&remote_command_str);

	/* verify that new standby is connected and replicating */

	connection_success = false;

	for (i = 0; i < options.reconnect_attempts; i++)
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

	/* A connection was made and it was determined the standby is in recovery */
	log_debug("new standby is in recovery\n");

	/* Check for entry in the new master's pg_stat_replication */

	local_conn = establish_db_connection(options.conninfo, true);

	{
		int			i,
			replication_check_timeout  = 60,
			replication_check_interval = 2;
		bool replication_connection_ok = false;
		PQExpBufferData event_details;

		initPQExpBuffer(&event_details);

		i = 0;
		for (;;)
		{
			query_result = get_node_replication_state(local_conn, remote_node_record.name, remote_node_replication_state);

			if (query_result == -1)
			{
				appendPQExpBuffer(&event_details,
								  _("unable to retrieve replication status for node %i"),
								  remote_node_id);
				log_warning("%s\n", event_details.data);
			}
			else if (query_result == 0)
			{
				log_warning(_("pg_stat_replication record for node %i not yet found\n"), remote_node_id);
			}
			else
			{
				if (strcmp(remote_node_replication_state, "streaming") == 0 ||
					strcmp(remote_node_replication_state, "catchup")  == 0)
				{
					log_verbose(LOG_NOTICE, _("node %i is replicating in state \"%s\"\n"), remote_node_id, remote_node_replication_state);
					replication_connection_ok = true;
					break;
				}
				else if (strcmp(remote_node_replication_state, "startup") == 0)
				{
					log_verbose(LOG_NOTICE, _("node %i is starting up replication\n"), remote_node_id);
				}
				else
				{
					/*
					 * Other possible replication states are:
					 *  - backup
					 *  - UNKNOWN
					 */
					appendPQExpBuffer(&event_details,
									  _("node %i has unexpected replication state \"%s\""),
									  remote_node_id, remote_node_replication_state);
					log_warning("%s\n", event_details.data);
				}
			}

			if (i >= replication_check_timeout)
				break;

			sleep(replication_check_interval);

			i += replication_check_interval;

			/* Reinitialise the string buffer */
			termPQExpBuffer(&event_details);
			initPQExpBuffer(&event_details);
		}

		/*
		 * We were unable to establish that the new standby had a pg_stat_replication
		 * record within the timeout period, so fail with whatever error message
		 * was placed in the string buffer.
		 */
		if (replication_connection_ok == false)
		{
			create_event_record(local_conn,
								&options,
								options.node,
								"standby_switchover",
								false,
								event_details.data);
			PQfinish(local_conn);
			exit(ERR_SWITCHOVER_FAIL);
		}

		termPQExpBuffer(&event_details);
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
			drop_replication_slot_if_exists(remote_conn,
											remote_node_id,
											local_node_record.slot_name);
		}

		PQfinish(remote_conn);
	}

	/* TODO: verify this node's record was updated correctly */

	create_event_record(local_conn,
						&options,
						options.node,
						"standby_switchover",
						true,
						NULL);

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
					  "     FROM pg_catalog.pg_settings "
					  "    WHERE name = 'data_directory') "
					  " SELECT distinct(sourcefile) AS config_file"
					  "   FROM dd, pg_catalog.pg_settings ps "
					  "  WHERE ps.sourcefile IS NOT NULL "
					  "    AND ps.sourcefile ~ ('^' || dd.setting) "
					  "     UNION "
					  "  SELECT ps.setting  AS config_file"
					  "    FROM dd, pg_catalog.pg_settings ps "
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
		log_warning(_("unable to delete %s\n"), runtime_options.config_archive_dir);
		log_detail(_("directory may need to be manually removed\n"));
	}
	else
	{
		log_verbose(LOG_NOTICE, "directory %s deleted\n", runtime_options.config_archive_dir);
	}

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

	char		witness_port[MAXLEN] = "";
	char		repmgr_user[MAXLEN] = "";
	char		repmgr_db[MAXLEN] = "";

	/*
	 * Extract the repmgr user and database names from the conninfo string
	 * provided in repmgr.conf
	 */
	get_conninfo_value(options.conninfo, "user", repmgr_user);
	get_conninfo_value(options.conninfo, "dbname", repmgr_db);

	param_set(&source_conninfo, "user", repmgr_user);
	param_set(&source_conninfo, "dbname", repmgr_db);

	/* We need to connect to check configuration and copy it */
	masterconn = establish_db_connection_by_params((const char**)source_conninfo.keywords, (const char**)source_conninfo.values, false);

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

	/* TODO: possibly allow the user to override this with a custom command? */
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
	if (*options.service_start_command)
	{
		maxlen_snprintf(script, "%s", options.service_start_command);
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
	if (WEXITSTATUS(r))
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
	if (*options.service_reload_command)
	{
		maxlen_snprintf(script, "%s", options.service_reload_command);
	}
	else
	{
		maxlen_snprintf(script, "%s %s -w -D %s reload",
						make_pg_path("pg_ctl"),
						options.pg_ctl_options, runtime_options.dest_dir);
	}

	log_info(_("reloading witness server configuration: %s\n"), script);
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
	bool		event_is_register = false;
	char		event_type[MAXLEN];

	/*
	 * Extract the repmgr user and database names from the conninfo string
	 * provided in repmgr.conf
	 */
	get_conninfo_value(options.conninfo, "user", repmgr_user);
	get_conninfo_value(options.conninfo, "dbname", repmgr_db);

	param_set(&source_conninfo, "user", repmgr_user);
	param_set(&source_conninfo, "dbname", repmgr_db);

	/* masterconn will only be set when called from do_witness_create() */
	if (PQstatus(masterconn) != CONNECTION_OK)
	{
		event_is_register = true;

		masterconn = establish_db_connection_by_params((const char**)source_conninfo.keywords, (const char**)source_conninfo.values, false);

		if (PQstatus(masterconn) != CONNECTION_OK)
		{
			/* No event logging possible here as we can't connect to the master */
			log_err(_("unable to connect to master\n"));
			exit(ERR_DB_CON);
		}
	}

	/* set the event type based on how we were called */
	if (event_is_register == true)
		strcpy(event_type, "witness_register");
	else
		strcpy(event_type, "witness_create");

	/* establish a connection to the witness, and create the schema */
	witnessconn = establish_db_connection(options.conninfo, false);

	if (PQstatus(witnessconn) != CONNECTION_OK)
	{
		create_event_record(masterconn,
							&options,
							options.node,
							event_type,
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
							event_type,
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
							event_type,
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
							event_type,
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
						event_type,
						true,
						NULL);

	PQfinish(masterconn);

	log_notice(_("configuration has been successfully copied to the witness\n"));
}


static void
do_witness_unregister(void)
{
	PGconn	   *witness_conn;
	PGconn	   *master_conn;

	int 		target_node_id;
	t_node_info node_info = T_NODE_INFO_INITIALIZER;

	bool		node_record_deleted;


	log_info(_("connecting to witness database\n"));
	witness_conn = establish_db_connection(options.conninfo, true);

	if (PQstatus(witness_conn) != CONNECTION_OK)
	{
		log_err(_("unable to connect to witness server\n"));
		exit(ERR_DB_CON);
	}

	/* Check if there is a schema for this cluster */
	if (check_cluster_schema(witness_conn) == false)
	{
		/* schema doesn't exist */
		log_err(_("schema '%s' doesn't exist.\n"), get_repmgr_schema());
		PQfinish(witness_conn);
		exit(ERR_BAD_CONFIG);
	}

	/* check if there is a master in this cluster */
	log_info(_("connecting to master server\n"));
	master_conn = get_master_connection(witness_conn, options.cluster_name,
										NULL, NULL);
	if (PQstatus(master_conn) != CONNECTION_OK)
	{
		log_err(_("unable to connect to master server\n"));
		PQfinish(witness_conn);
		exit(ERR_BAD_CONFIG);
	}

	if (runtime_options.node != UNKNOWN_NODE_ID)
		target_node_id = runtime_options.node;
	else
		target_node_id = options.node;

	/* Check node exists and is really a witness */

	if (!get_node_record(master_conn, options.cluster_name, target_node_id, &node_info))
	{
		log_err(_("No record found for node %i\n"), target_node_id);
		PQfinish(witness_conn);
		PQfinish(master_conn);
		exit(ERR_BAD_CONFIG);
	}

	if (node_info.type != WITNESS)
	{
		log_err(_("Node %i is not a witness server\n"), target_node_id);
		PQfinish(witness_conn);
		PQfinish(master_conn);
		exit(ERR_BAD_CONFIG);
	}

	log_info(_("unregistering the witness server\n"));
	node_record_deleted = delete_node_record(master_conn,
										     target_node_id,
											 "witness unregister");

	if (node_record_deleted == false)
	{
		PQfinish(master_conn);
		PQfinish(witness_conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Delete node record on witness server too, if it exists.
	 * As the witness server is not part of replication, if the node record continues
	 * to exist, running `repmgr cluster show` on the witness node would erroneously
	 * show the witness server as still registered.
	 */
	if (get_node_record(witness_conn, options.cluster_name, target_node_id, &node_info))
	{
		/*
		 * We don't really care at this point if the node record couldn't be
		 * deleted
		 */
		node_record_deleted = delete_node_record(witness_conn,
												 target_node_id,
												 "witness unregister");
	}

	/* Log the event */
	create_event_record(master_conn,
						&options,
						target_node_id,
						"witness_unregister",
						true,
						NULL);

	PQfinish(master_conn);
	PQfinish(witness_conn);

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
	printf(_("  %s [OPTIONS] witness {create|register|unregister}\n"), progname());
	printf(_("  %s [OPTIONS] cluster {show|matrix|crosscheck|cleanup}\n"), progname());
	printf(_("\n"));
	printf(_("General options:\n"));
	printf(_("  -?, --help                          show this help, then exit\n"));
	printf(_("  -V, --version                       output version information, then exit\n"));
	printf(_("\n"));
	printf(_("Logging options:\n"));
	printf(_("  -L, --log-level                     set log level (overrides configuration file; default: NOTICE)\n"));
	printf(_("  --log-to-file                       log to file (or logging facility) defined in repmgr.conf\n"));
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
	printf(_("STANDBY CLONE configuration options:\n"));
	printf(_("  -c, --fast-checkpoint               force fast checkpoint\n"));
	printf(_("  --copy-external-config-files[={samepath|pgdata}]\n" \
			 "                                      copy configuration files located outside the \n" \
			 "                                        data directory to the same path on the standby (default) or to the\n" \
			 "                                        PostgreSQL data directory\n"));
	printf(_("  --no-conninfo-password              do not write passwords into primary_conninfo\n"));
	printf(_("  --no-upstream-connection            when using Barman, do not connect to upstream node\n"));
	printf(_("  -r, --rsync-only                    use only rsync, not pg_basebackup\n"));
	printf(_("  --upstream-conninfo                 'primary_conninfo' value to write in recovery.conf\n" \
			 "                                        when the intended upstream server does not yet exist\n"));
	printf(_("  --recovery-min-apply-delay=VALUE    set recovery_min_apply_delay in recovery.conf (PostgreSQL 9.4 and later)\n"));
	printf(_("  --replication-user                  username to set in 'primary_conninfo' in recovery.conf\n"));
	printf(_("  --without-barman                    do not use Barman even if configured\n"));
	printf(_("  -w, --wal-keep-segments             minimum value for the GUC wal_keep_segments (default: %s)\n"), DEFAULT_WAL_KEEP_SEGMENTS);

	printf(_("\n"));
	printf(_("Other command-specific configuration options:\n"));

	printf(_("  --wait-sync[=VALUE]                 (standby register) wait for the node record to synchronise to the\n"\
			 "                                        standby (optional timeout in seconds)\n"));
	printf(_("  --recovery-min-apply-delay=VALUE    (standby follow) set recovery_min_apply_delay\n" \
			 "                                        in recovery.conf (PostgreSQL 9.4 and later)\n"));
	printf(_("  --replication-user                  (standby follow) username to set in 'primary_conninfo' in recovery.conf\n"));
	printf(_("  -W, --wait                          (standby follow) wait for a master to appear\n"));
	printf(_("  -m, --mode                          (standby switchover) shutdown mode (\"fast\" - default, \"smart\" or \"immediate\")\n"));
	printf(_("  -C, --remote-config-file            (standby switchover) path to the configuration file on the current master\n"));
	printf(_("  --pg_rewind[=VALUE]                 (standby switchover) 9.3/9.4 only - use pg_rewind if available,\n" \
			 "                                        optionally providing a path to the binary\n"));
	printf(_("  -k, --keep-history=VALUE            (cluster cleanup) retain indicated number of days of history (default: 0)\n"));
	printf(_("  --csv                               (cluster show, cluster matrix) output in CSV mode:\n" \
			 "                                        0 = OK, -1 = down, -2 = unknown\n"));
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
	printf(_(" witness register      - registers a witness server\n"));
	printf(_(" witness unregister    - unregisters a witness server\n"));
	printf(_(" cluster show          - displays information about cluster nodes\n"));
	printf(_(" cluster matrix        - displays the cluster's connection matrix\n" \
	         "                           as seen from the current node\n"));
	printf(_(" cluster crosscheck    - displays the cluster's connection matrix\n" \
	         "                           as seen from all nodes\n"));
	printf(_(" cluster cleanup       - prunes or truncates monitoring history\n" \
			 "                         (monitoring history creation requires repmgrd\n" \
			 "                         with --monitoring-history option)\n"));
}


/*
 * Creates a recovery.conf file for a standby
 *
 * A database connection pointer is required for escaping primary_conninfo
 * parameters. When cloning from Barman and --no-upstream-conne ) this might not be
 */
static bool
create_recovery_file(const char *data_dir, t_conninfo_param_list *recovery_conninfo)
{
	FILE	   *recovery_file;
	char		recovery_file_path[MAXLEN];
	char		line[MAXLEN];
	mode_t		um;

	maxlen_snprintf(recovery_file_path, "%s/%s", data_dir, RECOVERY_COMMAND_FILE);

	/* Set umask to 0600 */
	um = umask((~(S_IRUSR | S_IWUSR)) & (S_IRWXG | S_IRWXO));
	recovery_file = fopen(recovery_file_path, "w");
	umask(um);

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

	/*
	 * the user specified --upstream-conninfo string - copy that
	 */
	if (strlen(runtime_options.upstream_conninfo))
	{
		char *escaped = escape_recovery_conf_value(runtime_options.upstream_conninfo);
		maxlen_snprintf(line, "primary_conninfo = '%s'\n",
						escaped);
		free(escaped);
	}
	/*
	 * otherwise use the conninfo inferred from the upstream connection
	 * and/or node record
	 */
	else
	{
		write_primary_conninfo(line, recovery_conninfo);
	}

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


static void
write_primary_conninfo(char *line, t_conninfo_param_list *param_list)
{
	PQExpBufferData conninfo_buf;
	bool application_name_provided = false;
	int c;
	char *escaped;

	initPQExpBuffer(&conninfo_buf);

	for (c = 0; c < param_list->size && param_list->keywords[c] != NULL; c++)
	{
		/*
		 * Skip empty settings and ones which don't make any sense in
		 * recovery.conf
		 */
		if (strcmp(param_list->keywords[c], "dbname") == 0 ||
		    strcmp(param_list->keywords[c], "replication") == 0 ||
			(runtime_options.no_conninfo_password &&
			 strcmp(param_list->keywords[c], "password") == 0) ||
		    (param_list->values[c] == NULL) ||
		    (param_list->values[c] != NULL && param_list->values[c][0] == '\0'))
			continue;

		if (conninfo_buf.len != 0)
			appendPQExpBufferChar(&conninfo_buf, ' ');

		if (strcmp(param_list->keywords[c], "application_name") == 0)
			application_name_provided = true;

		appendPQExpBuffer(&conninfo_buf, "%s=", param_list->keywords[c]);
		appendConnStrVal(&conninfo_buf, param_list->values[c]);
	}

	/* `application_name` not provided - default to repmgr node name */
	if (application_name_provided == false)
	{
		if (strlen(options.node_name))
		{
			appendPQExpBuffer(&conninfo_buf, " application_name=");
		    appendConnStrVal(&conninfo_buf, options.node_name);
		}
		else
		{
			appendPQExpBuffer(&conninfo_buf, " application_name=repmgr");
		}
	}
	escaped = escape_recovery_conf_value(conninfo_buf.data);

	maxlen_snprintf(line, "primary_conninfo = '%s'\n", escaped);

	free(escaped);

	termPQExpBuffer(&conninfo_buf);
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
	for (i = 0; truebin_paths[i] && r != 0; ++i)
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
		log_info(_("unable to connect to remote host (%s) via SSH.\n"), host);
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

		if (server_version_num >= 100000)
		{
			appendPQExpBuffer(&rsync_flags, "%s",
							  " --exclude=pg_wal/*");
		}
		else
		{
			appendPQExpBuffer(&rsync_flags, "%s",
							  " --exclude=pg_xlog/*");
		}

		appendPQExpBuffer(&rsync_flags, "%s",
						  " --exclude=pg_log/* --exclude=pg_stat_tmp/*");

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

	log_debug("copy_remote_files(): r = %i; WIFEXITED: %i; WEXITSTATUS: %i\n", r, WIFEXITED(r), WEXITSTATUS(r));

	/* exit code 24 indicates vanished files, which isn't a problem for us */
	if (WEXITSTATUS(r) && WEXITSTATUS(r) != 24)
		log_verbose(LOG_WARNING, "copy_remote_files(): rsync returned unexpected exit status %i \n", WEXITSTATUS(r));

	return r;
}


static int
run_basebackup(const char *data_dir, int server_version_num)
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
	parse_pg_basebackup_options(options.pg_basebackup_options, &backup_options, server_version_num, NULL);

	/* Create pg_basebackup command line options */

	initPQExpBuffer(&params);

	appendPQExpBuffer(&params, " -D %s", data_dir);

	/*
	 * conninfo string provided - pass it to pg_basebackup as the -d option
	 * (pg_basebackup doesn't require or want a database name, but for
	 * consistency with other applications accepts a conninfo string
	 * under -d/--dbname)
	 */
	if (runtime_options.conninfo_provided == true)
	{
		t_conninfo_param_list conninfo;
		char *conninfo_str;

		initialize_conninfo_params(&conninfo, false);

		/* string will already have been parsed */
		(void) parse_conninfo_string(runtime_options.dbname, &conninfo, NULL, false);

		if (*runtime_options.replication_user)
			param_set(&conninfo, "user", runtime_options.replication_user);

		conninfo_str = param_list_to_string(&conninfo);

		appendPQExpBuffer(&params, " -d '%s'", conninfo_str);

		pfree(conninfo_str);
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

		if (strlen(runtime_options.replication_user))
		{
			appendPQExpBuffer(&params, " -U %s", runtime_options.replication_user);
		}
		else if (strlen(runtime_options.username))
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
	 * (from 10, -X/--wal-method=stream)
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
	 *   of values for --wal-method (--xlog-method) and --slot - we're not checking that, just that
	 *   we're not overriding any user-supplied values
	 */
	if (server_version_num >= 90600 && options.use_replication_slots)
	{
		bool slot_add = true;

		/*
		 * Check whether 'pg_basebackup_options' in repmgr.conf has the --slot option set,
		 * or if --wal-method (--xlog-method) is set to a value other than "stream"
		 * (in which case we can't use --slot).
		 */
		if (strlen(backup_options.slot) || (strlen(backup_options.xlog_method) && strcmp(backup_options.xlog_method, "stream") != 0)) {
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
			if (runtime_options.connection_param_provided)
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
			 * To register a standby we only need the repmgr.conf; usually
			 * we don't need connection parameters to the master because we
			 * can detect the master in repl_nodes. However in certain cases
			 * it may be desirable to register a standby which hasn't yet
			 * been started, which requires the use of --force *and* provision
			 * of the master connection string, in which case we don't need the
			 * warning.
			 */
			if (runtime_options.connection_param_provided && runtime_options.force == false)
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
			if (runtime_options.connection_param_provided)
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
			if (runtime_options.connection_param_provided)
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

				if (runtime_options.host_param_provided && !runtime_options.dest_dir[0])
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

			if (runtime_options.no_upstream_connection == true &&
				(strcmp(options.barman_server, "") == 0 || runtime_options.without_barman == true))
			{
				item_list_append(&cli_warnings, _("--no-upstream-connection only effective in Barman mode"));
			}

			if (*runtime_options.upstream_conninfo && runtime_options.no_conninfo_password == true)
			{
				item_list_append(&cli_warnings, _("--no-conninfo-password ineffective when specifying --upstream-conninfo"));
			}

			if (*runtime_options.upstream_conninfo && *runtime_options.replication_user)
			{
				item_list_append(&cli_warnings, _("--replication-user ineffective when specifying --upstream-conninfo"));
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

		case CLUSTER_MATRIX:
			/* config file not required if database connection parameters and cluster name supplied */
			config_file_required = false;

			if (strlen(repmgr_cluster) && runtime_options.node == UNKNOWN_NODE_ID)
				item_list_append(&cli_errors, _("--node required when executing CLUSTER MATRIX with --cluster"));

			break;

		case CLUSTER_SHOW:
			/* config file not required if database connection parameters and cluster name supplied */
			config_file_required = false;
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

		if (runtime_options.copy_external_config_files)
		{
			item_list_append(&cli_warnings, _("--copy-external-config-files can only be used when executing STANDBY CLONE"));
		}

		if (*runtime_options.recovery_min_apply_delay)
		{
			item_list_append(&cli_warnings, _("--recovery-min-apply-delay can only be used when executing STANDBY CLONE"));
		}

		if (runtime_options.rsync_only)
		{
			item_list_append(&cli_warnings, _("-r/--rsync-only can only be used when executing STANDBY CLONE"));
		}

		if (runtime_options.wal_keep_segments_used)
		{
			item_list_append(&cli_warnings, _("-w/--wal-keep-segments can only be used when executing STANDBY CLONE"));
		}

		if (runtime_options.no_upstream_connection == true)
		{
			item_list_append(&cli_warnings, _("--no-upstream-connection can only be used when executing STANDBY CLONE in Barman mode"));
		}

		if (*runtime_options.upstream_conninfo)
		{
			item_list_append(&cli_warnings, _("--upstream-conninfo can only be used when executing STANDBY CLONE"));
		}

		if (runtime_options.no_conninfo_password == true)
		{
			item_list_append(&cli_warnings, _("--no-conninfo-password can only be used when executing STANDBY CLONE"));
		}
	}

	if (action != STANDBY_CLONE && action != STANDBY_FOLLOW)
	{
		if (*runtime_options.replication_user)
		{
			item_list_append(&cli_warnings, _("--replication-user can only be used when executing STANDBY CLONE or STANDBY FOLLOW"));
		}
	}

	/* Warn about parameters which apply to STANDBY REGISTER only */
	if (action != STANDBY_REGISTER)
	{
		if (runtime_options.wait_register_sync)
		{
			item_list_append(&cli_warnings, _("--wait-sync can only be used when executing STANDBY REGISTER"));
		}
	}

	/* Warn about parameters which apply to STANDBY SWITCHOVER only */
	if (action != STANDBY_SWITCHOVER)
	{
		if (runtime_options.pg_rewind_supplied == true)
		{
			item_list_append(&cli_warnings, _("--pg_rewind can only be used when executing STANDBY SWITCHOVER"));
		}
	}

	/* Warn about parameters which apply to WITNESS UNREGISTER only */
	if (action != WITNESS_UNREGISTER && action != STANDBY_UNREGISTER && action != CLUSTER_MATRIX)
	{
		if (runtime_options.node != UNKNOWN_NODE_ID)
		{
			item_list_append(&cli_warnings, _("--node not required with this action"));
		}
	}

    /* Warn about parameters which apply only to CLUSTER SHOW and CLUSTER MATRIX */
	if (action != CLUSTER_SHOW && action != CLUSTER_MATRIX)
	{
		if (runtime_options.csv_mode)
		{
			item_list_append(&cli_warnings, _("--csv can only be used when executing CLUSTER SHOW or CLUSTER MATRIX"));
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
	log_debug(_("create_schema: %s\n"), sqlquery);
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

	log_debug(_("create_schema: %s\n"), sqlquery);
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
	log_debug(_("create_schema: %s\n"), sqlquery);
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

	log_debug(_("create_schema: %s\n"), sqlquery);
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
	log_debug(_("create_schema: %s\n"), sqlquery);

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

	log_debug(_("create_schema: %s\n"), sqlquery);
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
			                  "SELECT rn.id, rn.conninfo, "
					          "  rn.type, rn.name, rn.cluster,"
			                  "  rn.priority, rn.active, sq.name AS upstream_node_name"
			                  "  FROM %s.repl_nodes as rn"
			                  "  LEFT JOIN %s.repl_nodes AS sq"
			                  "    ON sq.id=rn.upstream_node_id",
			  get_repmgr_schema_quoted(conn),
			  get_repmgr_schema_quoted(conn),
			  get_repmgr_schema_quoted(conn));

	log_debug(_("create_schema: %s\n"), sqlquery);

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
 *   passed to get_server_version(), which will place the human-readable
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
 * Perform sanity check on upstream server configuration before starting cloning
 * process
 *
 * TODO:
 *  - check user is qualified to perform base backup
 */

static bool
check_upstream_config(PGconn *conn, int server_version_num, bool exit_on_error)
{
	int			i;
	bool		config_ok = true;
	char	   *wal_error_message = NULL;
	t_basebackup_options  backup_options = T_BASEBACKUP_OPTIONS_INITIALIZER;
	bool		backup_options_ok = true;
	ItemList	backup_option_errors = { NULL, NULL };
	bool		xlog_stream = true;

	enum {
		barman,
		rsync,
		pg_basebackup
	}			mode;


	/*
	 * Detecting the intended cloning mode
	 */
	if (runtime_options.rsync_only)
		mode = rsync;
	else if (strcmp(options.barman_server, "") != 0 && ! runtime_options.without_barman)
		mode = barman;
	else
		mode = pg_basebackup;

	/*
	 * Parse `pg_basebackup_options`, if set, to detect whether --xlog-method
	 * has been set to something other than `stream` (i.e. `fetch`), as
	 * this will influence some checks
	 */

	backup_options_ok = parse_pg_basebackup_options(
		options.pg_basebackup_options,
		&backup_options, server_version_num,
		&backup_option_errors);

	if (backup_options_ok == false)
	{
		if (exit_on_error == true)
		{
			log_err(_("error(s) encountered parsing 'pg_basebackup_options'\n"));
			print_error_list(&backup_option_errors, LOG_ERR);
			log_hint(_("'pg_basebackup_options' is: '%s'\n"), options.pg_basebackup_options);
			exit(ERR_BAD_CONFIG);
		}

		config_ok = false;
	}

	if (strlen(backup_options.xlog_method) && strcmp(backup_options.xlog_method, "stream") != 0)
		xlog_stream = false;

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
	 * physical replication slots not available or not requested - check if
	 * there are any circumstances where `wal_keep_segments` should be set
	 */
	else if (mode != barman)
	{
		bool check_wal_keep_segments = false;
		char min_wal_keep_segments[MAXLEN] = "1";

		/*
		 * -w/--wal-keep-segments was supplied - check against that value
		 */
		if (runtime_options.wal_keep_segments_used == true)
		{
			check_wal_keep_segments = true;
			strncpy(min_wal_keep_segments, runtime_options.wal_keep_segments, MAXLEN);
		}

		/*
		 * A non-zero `wal_keep_segments` value will almost certainly be required
		 * if rsync mode is being used, or pg_basebackup with --xlog-method=fetch,
		 * *and* no restore command has been specified
		 */
		else if ( (runtime_options.rsync_only == true || xlog_stream == false)
			 && strcmp(options.restore_command, "") == 0)
		{
			check_wal_keep_segments = true;
		}

		if (check_wal_keep_segments == true)
		{
			i = guc_set_typed(conn, "wal_keep_segments", ">=", min_wal_keep_segments, "integer");

			if (i == 0 || i == -1)
			{
				if (i == 0)
				{
					log_err(_("parameter 'wal_keep_segments' on the upstream server must be be set to %s or greater\n"),
							min_wal_keep_segments);
					log_hint(_("Choose a value sufficiently high enough to retain enough WAL "
							   "until the standby has been cloned and started.\n "
							   "Alternatively set up WAL archiving using e.g. PgBarman and configure "
							   "'restore_command' in repmgr.conf to fetch WALs from there.\n"
								 ));
					if (server_version_num >= 90400)
					{
						log_hint(_("In PostgreSQL 9.4 and later, replication slots can be used, which "
								   "do not require 'wal_keep_segments' to be set "
								   "(set parameter 'use_replication_slots' in repmgr.conf to enable)\n"
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

	/*
	 * If using pg_basebackup, ensure sufficient replication connections can be made.
	 * There's no guarantee they'll still be available by the time pg_basebackup
	 * is executed, but there's nothing we can do about that.
	 */
	if (mode == pg_basebackup)
	{

		PGconn	  **connections;
		int			i;
		int			min_replication_connections = 1,
					possible_replication_connections = 0;

		t_conninfo_param_list repl_conninfo;

		/* Make a copy of the connection parameter arrays, and append "replication" */

		initialize_conninfo_params(&repl_conninfo, false);

		conn_to_param_list(conn, &repl_conninfo);

		param_set(&repl_conninfo, "replication", "1");

		if (*runtime_options.replication_user)
			param_set(&repl_conninfo, "user", runtime_options.replication_user);

		/*
		 * work out how many replication connections are required (1 or 2)
		 */

		if (xlog_stream == true)
			min_replication_connections += 1;

		log_verbose(LOG_NOTICE, "checking for available walsenders on upstream node (%i required)\n", min_replication_connections);

		connections = pg_malloc0(sizeof(PGconn *) * min_replication_connections);

		/* Attempt to create the minimum number of required concurrent connections */
		for (i = 0; i < min_replication_connections; i++)
		{
			PGconn *replication_conn;

			replication_conn = establish_db_connection_by_params((const char**)repl_conninfo.keywords, (const char**)repl_conninfo.values, false);

			if (PQstatus(replication_conn) == CONNECTION_OK)
			{
				connections[i] = replication_conn;
				possible_replication_connections++;
			}
		}

		/* Close previously created connections */
		for (i = 0; i < possible_replication_connections; i++)
		{
			PQfinish(connections[i]);
		}

		if (possible_replication_connections < min_replication_connections)
		{
			config_ok = false;
			log_err(_("unable to establish necessary replication connections\n"));
			log_hint(_("increase 'max_wal_senders' by at least %i\n"), min_replication_connections - possible_replication_connections);

			if (exit_on_error == true)
			{
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}
		}

		log_verbose(LOG_INFO, "sufficient walsenders available on upstream node (%i required)\n", min_replication_connections);
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

	/* We need to connect to check configuration and start a backup */
	log_info(_("connecting to upstream server\n"));

	conn = establish_db_connection_by_params((const char**)source_conninfo.keywords, (const char**)source_conninfo.values, true);

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


static char *
make_barman_ssh_command(void)
{
	static char config_opt[MAXLEN] = "";

	if (strlen(options.barman_config))
		maxlen_snprintf(config_opt,
						" --config=%s",
						options.barman_config);

	maxlen_snprintf(barman_command_buf,
					"ssh %s barman%s",
					options.barman_server,
					config_opt);

	return barman_command_buf;
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
					"ssh -o Batchmode=yes %s %s %s",
					options.ssh_options,
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

	if (outputbuf != NULL)
	{
		/* TODO: better error handling */
		while (fgets(output, MAXLEN, fp) != NULL)
		{
			appendPQExpBuffer(outputbuf, "%s", output);
		}
	}
	else
	{
		/*
		 * When executed remotely, repmgr commands which execute pg_ctl (particularly
		 * `repmgr standby follow`) will see the pg_ctl command appear to fail with a
		 * non-zero return code when the output from the executed pg_ctl command
		 * has nowhere to go, even though the command actually succeeds. We'll consume an
		 * arbitrary amount of output and throw it away to work around this.
		 */
		int i = 0;
		while (fgets(output, MAXLEN, fp) != NULL && i < 10)
		{
			i++;
		}
	}

	pclose(fp);

	if (outputbuf != NULL)
		log_verbose(LOG_DEBUG, "remote_command(): output returned was:\n%s", outputbuf->data);

	return true;
}


/*
 * Execute a command locally. If outputbuf == NULL, discard the
 * output.
 */
static bool
local_command(const char *command, PQExpBufferData *outputbuf)
{
	FILE *fp;
	char output[MAXLEN];
	int retval;

	if (outputbuf == NULL)
	{
		retval = system(command);
		return (retval == 0) ? true : false;
	}
	else
	{
		fp = popen(command, "r");

		if (fp == NULL)
		{
			log_err(_("unable to execute local command:\n%s\n"), command);
			return false;
		}

		/* TODO: better error handling */
		while (fgets(output, MAXLEN, fp) != NULL)
		{
			appendPQExpBuffer(outputbuf, "%s", output);
		}

		pclose(fp);

		if (outputbuf->data != NULL)
			log_verbose(LOG_DEBUG, "local_command(): output returned was:\n%s", outputbuf->data);
		else
			log_verbose(LOG_DEBUG, "local_command(): no output returned\n");

		return true;
	}
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
initialize_conninfo_params(t_conninfo_param_list *param_list, bool set_defaults)
{
	PQconninfoOption *defs = NULL;
	PQconninfoOption *def;
	int c;

	defs = PQconndefaults();
	param_list->size = 0;

	/* Count maximum number of parameters */
	for (def = defs; def->keyword; def++)
		param_list->size ++;

	/* Initialize our internal parameter list */
	param_list->keywords = pg_malloc0(sizeof(char *) * (param_list->size + 1));
	param_list->values = pg_malloc0(sizeof(char *) * (param_list->size + 1));

	for (c = 0; c < param_list->size; c++)
	{
		param_list->keywords[c] = NULL;
		param_list->values[c] = NULL;
	}

	if (set_defaults == true)
	{
		/* Pre-set any defaults */

		for (def = defs; def->keyword; def++)
		{
			if (def->val != NULL && def->val[0] != '\0')
			{
				param_set(param_list, def->keyword, def->val);
			}
		}
	}
}


static void
copy_conninfo_params(t_conninfo_param_list *dest_list, t_conninfo_param_list *source_list)
{
	int c;
	for (c = 0; c < source_list->size && source_list->keywords[c] != NULL; c++)
	{
		if (source_list->values[c] != NULL && source_list->values[c][0] != '\0')
		{
			param_set(dest_list, source_list->keywords[c], source_list->values[c]);
		}
	}
}

static void
param_set(t_conninfo_param_list *param_list, const char *param, const char *value)
{
	int c;
	int value_len = strlen(value) + 1;

	/*
	 * Scan array to see if the parameter is already set - if not, replace it
	 */
	for (c = 0; c < param_list->size && param_list->keywords[c] != NULL; c++)
	{
		if (strcmp(param_list->keywords[c], param) == 0)
		{
			if (param_list->values[c] != NULL)
				pfree(param_list->values[c]);

			param_list->values[c] = pg_malloc0(value_len);
			strncpy(param_list->values[c], value, value_len);

			return;
		}
	}

	/*
	 * Parameter not in array - add it and its associated value
	 */
	if (c < param_list->size)
	{
		int param_len = strlen(param) + 1;
		param_list->keywords[c] = pg_malloc0(param_len);
		param_list->values[c] = pg_malloc0(value_len);

		strncpy(param_list->keywords[c], param, param_len);
		strncpy(param_list->values[c], value, value_len);
	}

	/*
	 * It's theoretically possible a parameter couldn't be added as
	 * the array is full, but it's highly improbable so we won't
	 * handle it at the moment.
	 */
}


static char *
param_get(t_conninfo_param_list *param_list, const char *param)
{
	int c;

	for (c = 0; c < param_list->size && param_list->keywords[c] != NULL; c++)
	{
		if (strcmp(param_list->keywords[c], param) == 0)
		{
			if (param_list->values[c] != NULL && param_list->values[c][0] != '\0')
				return param_list->values[c];
            else
                return NULL;
		}
	}

	return NULL;
}


/*
 * Parse a conninfo string into a t_conninfo_param_list
 *
 * See conn_to_param_list() to do the same for a PQconn
 */
static bool
parse_conninfo_string(const char *conninfo_str, t_conninfo_param_list *param_list, char *errmsg, bool ignore_application_name)
{
	PQconninfoOption *connOptions;
	PQconninfoOption *option;

	connOptions = PQconninfoParse(conninfo_str, &errmsg);

	if (connOptions == NULL)
		return false;

	for (option = connOptions; option && option->keyword; option++)
	{
		/* Ignore non-set or blank parameter values*/
		if ((option->val == NULL) ||
		   (option->val != NULL && option->val[0] == '\0'))
			continue;

		/* Ignore application_name */
		if (ignore_application_name == true && strcmp(option->keyword, "application_name") == 0)
			continue;

		param_set(param_list, option->keyword, option->val);
	}

	return true;
}


/*
 * Parse a PQconn into a t_conninfo_param_list
 *
 * See parse_conninfo_string() to do the same for a conninfo string
 */
static void
conn_to_param_list(PGconn *conn, t_conninfo_param_list *param_list)
{
	PQconninfoOption *connOptions;
	PQconninfoOption *option;

	connOptions = PQconninfo(conn);
	for (option = connOptions; option && option->keyword; option++)
	{
		/* Ignore non-set or blank parameter values*/
		if ((option->val == NULL) ||
		   (option->val != NULL && option->val[0] == '\0'))
			continue;

		param_set(param_list, option->keyword, option->val);
	}
}


static char *
param_list_to_string(t_conninfo_param_list *param_list)
{
	int c;
	PQExpBufferData conninfo_buf;
	char *conninfo_str;
	int len;

	initPQExpBuffer(&conninfo_buf);

	for (c = 0; c < param_list->size && param_list->keywords[c] != NULL; c++)
	{
		if (param_list->values[c] != NULL && param_list->values[c][0] != '\0')
		{
			if (c > 0)
				appendPQExpBufferChar(&conninfo_buf, ' ');

			appendPQExpBuffer(&conninfo_buf,
							  "%s=%s",
							  param_list->keywords[c],
							  param_list->values[c]);
		}
	}

	len = strlen(conninfo_buf.data) + 1;
	conninfo_str = pg_malloc0(len);

	strncpy(conninfo_str, conninfo_buf.data, len);

	termPQExpBuffer(&conninfo_buf);

	return conninfo_str;
}




static bool
parse_pg_basebackup_options(const char *pg_basebackup_options, t_basebackup_options *backup_options, int server_version_num, ItemList *error_list)
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

	struct option *long_options;

	bool		backup_options_ok = true;

	/* We're only interested in these options */
	static struct option long_options_9[] =
	{
		{"slot", required_argument, NULL, 'S'},
		{"xlog-method", required_argument, NULL, 'X'},
		{NULL, 0, NULL, 0}
	};

	/*
	 * From PostgreSQL 10, --xlog-method is renamed --wal-method
	 * and there's also --no-slot, which we'll want to consider.
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

	/* Copy the string before operating on it with strtok() */
	strncpy(options_string, pg_basebackup_options, options_len);

	/* Extract arguments into a list and keep a count of the total */
	while ((argv_item = strtok(options_string_ptr, " ")) != NULL)
	{
		item_list_append(&option_argv, argv_item);

		argc_item++;

		if (options_string_ptr != NULL)
			options_string_ptr = NULL;
	}

	/*
	 * Array of argument values to pass to getopt_long - this will need to
	 * include an empty string as the first value (normally this would be
	 * the program name)
	 */
	argv_array = pg_malloc0(sizeof(char *) * (argc_item + 2));

	/* Insert a blank dummy program name at the start of the array */
	argv_array[0] = pg_malloc0(1);

	c = 1;

	/*
	 * Copy the previously extracted arguments from our list to the array
	 */
	for (cell = option_argv.head; cell; cell = cell->next)
	{
		int argv_len = strlen(cell->string) + 1;

		argv_array[c] = pg_malloc0(argv_len);

		strncpy(argv_array[c], cell->string, argv_len);

		c++;
	}

	argv_array[c] = NULL;

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

	return backup_options_ok;
}

static void
config_file_list_init(t_configfile_list *list, int max_size)
{
	list->size = max_size;
	list->entries = 0;
	list->files = pg_malloc0(sizeof(t_configfile_info *) * max_size);
}


static void
config_file_list_add(t_configfile_list *list, const char *file, const char *filename, bool in_data_dir)
{
	/* Failsafe to prevent entries being added beyond the end */
	if (list->entries == list->size)
		return;

	list->files[list->entries] = pg_malloc0(sizeof(t_configfile_info));


	strncpy(list->files[list->entries]->filepath, file, MAXPGPATH);
	canonicalize_path(list->files[list->entries]->filepath);


	strncpy(list->files[list->entries]->filename, filename, MAXPGPATH);
	list->files[list->entries]->in_data_directory = in_data_dir;

	list->entries ++;
}


static void
drop_replication_slot_if_exists(PGconn *conn, int node_id, char *slot_name)
{
	t_replication_slot  slot_info;
	int					query_res;

	query_res = get_slot_record(conn,slot_name, &slot_info);

	if (query_res)
	{
		if (slot_info.active == false)
		{
			if (drop_replication_slot(conn, slot_name) == true)
			{
				log_notice(_("replication slot \"%s\" deleted on node %i\n"), slot_name, node_id);
			}
			else
			{
				log_err(_("unable to delete replication slot \"%s\" on node %i\n"), slot_name, node_id);
			}
		}
		/* if active replication slot exists, call Houston as we have a problem */
		else
		{
			log_err(_("replication slot \"%s\" is still active on node %i\n"), slot_name, node_id);
		}
	}
}
