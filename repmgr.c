/*
 * repmgr.c - Command interpreter for the repmgr package
 * Copyright (C) 2ndQuadrant, 2010-2015
 *
 * This module is a command-line utility to easily setup a cluster of
 * hot standby servers for an HA environment
 *
 * Commands implemented are:
 *
 * MASTER REGISTER
 *
 * STANDBY REGISTER
 * STANDBY UNREGISTER
 * STANDBY CLONE
 * STANDBY FOLLOW
 * STANDBY PROMOTE
 *
 * CLUSTER SHOW
 * CLUSTER CLEANUP
 *
 * WITNESS CREATE
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

#include <stdio.h>
#include <stdlib.h>
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

#define RECOVERY_FILE "recovery.conf"

#define NO_ACTION			0		/* Dummy default action */
#define MASTER_REGISTER		1
#define STANDBY_REGISTER	2
#define STANDBY_UNREGISTER	3
#define STANDBY_CLONE		4
#define STANDBY_PROMOTE		5
#define STANDBY_FOLLOW		6
#define WITNESS_CREATE		7
#define CLUSTER_SHOW		8
#define CLUSTER_CLEANUP		9



static bool create_recovery_file(const char *data_dir);
static int	test_ssh_connection(char *host, char *remote_user);
static int  copy_remote_files(char *host, char *remote_user, char *remote_path,
							  char *local_path, bool is_directory, int server_version_num);
static int  run_basebackup(const char *data_dir);
static void check_parameters_for_action(const int action);
static bool create_schema(PGconn *conn);
static void write_primary_conninfo(char *line);
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
static void do_witness_create(void);
static void do_cluster_show(void);
static void do_cluster_cleanup(void);
static void do_check_upstream_config(void);

static void error_list_append(char *error_message);
static void exit_with_errors(void);
static void help(const char *progname);

/* Global variables */
static const char *progname;
static const char *keywords[6];
static const char *values[6];
static bool		   config_file_required = true;

/* XXX This should be mapped into a command line option */
bool		require_password = false;

/* Initialization of runtime options */
t_runtime_options runtime_options = T_RUNTIME_OPTIONS_INITIALIZER;
t_configuration_options options = T_CONFIGURATION_OPTIONS_INITIALIZER;

static char *server_mode = NULL;
static char *server_cmd = NULL;

static char  pg_bindir[MAXLEN] = "";
static char  repmgr_slot_name[MAXLEN] = "";
static char *repmgr_slot_name_ptr = NULL;
static char  path_buf[MAXLEN] = "";

/* Collate command line errors here for friendlier reporting */
static ErrorList cli_errors = { NULL, NULL };

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
		{"dest-dir", required_argument, NULL, 'D'},
		{"local-port", required_argument, NULL, 'l'},
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
		{"initdb-no-pwprompt", no_argument, NULL, 1},
		{"check-upstream-config", no_argument, NULL, 2},
		{"recovery-min-apply-delay", required_argument, NULL, 3},
		{"ignore-external-config-files", no_argument, NULL, 4},
		{"help", no_argument, NULL, '?'},
		{"version", no_argument, NULL, 'V'},
		{NULL, 0, NULL, 0}
	};

	int			optindex;
	int			c, targ;
	int			action = NO_ACTION;
	bool 		check_upstream_config = false;
	bool 		wal_keep_segments_used = false;
	bool 		config_file_parsed = false;
	char 	   *ptr = NULL;

	progname = get_progname(argv[0]);

	/* Prevent getopt_long() from printing an error message */
	opterr = 0;

	while ((c = getopt_long(argc, argv, "?Vd:h:p:U:S:D:l:f:R:w:k:FWIvb:r:c", long_options,
							&optindex)) != -1)
	{
		switch (c)
		{
			case '?':
				help(progname);
				exit(SUCCESS);
			case 'V':
				printf("%s %s (PostgreSQL %s)\n", progname, REPMGR_VERSION, PG_VERSION);
				exit(SUCCESS);
			case 'd':
				strncpy(runtime_options.dbname, optarg, MAXLEN);
				break;
			case 'h':
				strncpy(runtime_options.host, optarg, MAXLEN);
				break;
			case 'p':
				if (atoi(optarg) > 0)
					strncpy(runtime_options.masterport, optarg, MAXLEN);
				break;
			case 'U':
				strncpy(runtime_options.username, optarg, MAXLEN);
				break;
			case 'S':
				strncpy(runtime_options.superuser, optarg, MAXLEN);
				break;
			case 'D':
				strncpy(runtime_options.dest_dir, optarg, MAXFILENAME);
				break;
			case 'l':
				if (atoi(optarg) > 0)
					strncpy(runtime_options.localport, optarg, MAXLEN);
				break;
			case 'f':
				strncpy(runtime_options.config_file, optarg, MAXLEN);
				break;
			case 'R':
				strncpy(runtime_options.remote_user, optarg, MAXLEN);
				break;
			case 'w':
				if (atoi(optarg) > 0)
				{
					strncpy(runtime_options.wal_keep_segments, optarg, MAXLEN);
					wal_keep_segments_used = true;
				}
				break;
			case 'k':
				if (atoi(optarg) > 0)
					runtime_options.keep_history = atoi(optarg);
				else
					runtime_options.keep_history = 0;
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
			case 1:
				runtime_options.initdb_no_pwprompt = true;
				break;
			case 2:
				check_upstream_config = true;
				break;
			case 3:
				targ = strtol(optarg, &ptr, 10);

				if(targ < 1)
				{
					error_list_append(_("Invalid value provided for '-r/--recovery-min-apply-delay'"));
					break;
				}
				if(ptr && *ptr)
				{
					if(strcmp(ptr, "ms") != 0 && strcmp(ptr, "s") != 0 &&
					   strcmp(ptr, "min") != 0 && strcmp(ptr, "h") != 0 &&
					   strcmp(ptr, "d") != 0)
					{
						error_list_append(_("Value provided for '-r/--recovery-min-apply-delay' must be one of ms/s/min/h/d"));
						break;
					}
				}

				strncpy(runtime_options.recovery_min_apply_delay, optarg, MAXLEN);
				break;
			case 4:
				runtime_options.ignore_external_config_files = true;
				break;
			default:
			{
				PQExpBufferData unknown_option;
				initPQExpBuffer(&unknown_option);
				appendPQExpBuffer(&unknown_option, _("Unknown option '%s'"), argv[optind - 1]);

				error_list_append(unknown_option.data);
			}
		}
	}

	/* Exit here already if errors in command line options found */
	if(cli_errors.head != NULL)
	{
		exit_with_errors();
	}


	if(check_upstream_config == true)
	{
		do_check_upstream_config();
		exit(SUCCESS);
	}

	/*
	 * Now we need to obtain the action, this comes in one of these forms:
	 *   MASTER REGISTER |
	 *   STANDBY {REGISTER | UNREGISTER | CLONE [node] | PROMOTE | FOLLOW [node]} |
	 *   WITNESS CREATE |
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
			strcasecmp(server_mode, "WITNESS") != 0 &&
			strcasecmp(server_mode, "CLUSTER") != 0)
		{
			PQExpBufferData unknown_mode;
			initPQExpBuffer(&unknown_mode);
			appendPQExpBuffer(&unknown_mode, _("Unknown server mode '%s'"), server_mode);
			error_list_append(unknown_mode.data);
		}
	}

	if (optind < argc)
	{
		server_cmd = argv[optind++];
		/* check posibilities for all server modes */
		if (strcasecmp(server_mode, "MASTER") == 0)
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
		}
	}

	if (action == NO_ACTION) {
		if(server_cmd == NULL)
		{
			error_list_append("No server command provided");
		}
		else
		{
			PQExpBufferData unknown_action;
			initPQExpBuffer(&unknown_action);
			appendPQExpBuffer(&unknown_action, _("Unknown server command '%s'"), server_cmd);
			error_list_append(unknown_action.data);
		}
	}

	/* For some actions we still can receive a last argument */
	if (action == STANDBY_CLONE)
	{
		if (optind < argc)
		{
			if (runtime_options.host[0])
			{
				error_list_append(_("Conflicting parameters:  you can't use -h while providing a node separately."));
			}
			else
			{
				strncpy(runtime_options.host, argv[optind++], MAXLEN);
			}
		}
	}

	if (optind < argc)
	{
		PQExpBufferData too_many_args;
		initPQExpBuffer(&too_many_args);
		appendPQExpBuffer(&too_many_args, _("too many command-line arguments (first extra is \"%s\")"), argv[optind]);
		error_list_append(too_many_args.data);
	}

	check_parameters_for_action(action);

	/*
	 * Sanity checks for command line parameters completed by now;
	 * any further errors will be runtime ones
	 */
	if(cli_errors.head != NULL)
	{
		exit_with_errors();
	}

	if (!runtime_options.dbname[0])
	{
		if (getenv("PGDATABASE"))
			strncpy(runtime_options.dbname, getenv("PGDATABASE"), MAXLEN);
		else if (getenv("PGUSER"))
			strncpy(runtime_options.dbname, getenv("PGUSER"), MAXLEN);
		else
			strncpy(runtime_options.dbname, DEFAULT_DBNAME, MAXLEN);
	}

	/*
	 * If no primary port (-p, --port) provided, explicitly set the
	 * default PostgreSQL port.
	 */
	if (!runtime_options.masterport[0])
	{
		strncpy(runtime_options.masterport, DEFAULT_MASTER_PORT, MAXLEN);
	}


	if (runtime_options.verbose && runtime_options.config_file[0])
	{
		log_notice(_("opening configuration file: %s\n"),
				   runtime_options.config_file);
	}

	/*
	 * The configuration file is not required for some actions (e.g. 'standby clone'),
	 * however if available we'll parse it anyway for options like 'log_level',
	 * 'use_replication_slots' etc.
	 */
	config_file_parsed = load_config(runtime_options.config_file, &options, argv[0]);

	/*
	 * Initialise pg_bindir - command line parameter will override
	 * any setting in the configuration file
	 */
	if(!strlen(runtime_options.pg_bindir))
	{
		strncpy(runtime_options.pg_bindir, options.pg_bindir, MAXLEN);
	}

	/* Add trailing slash */
	if(strlen(runtime_options.pg_bindir))
	{
		int len = strlen(runtime_options.pg_bindir);
		if(runtime_options.pg_bindir[len - 1] != '/')
		{
			maxlen_snprintf(pg_bindir, "%s/", runtime_options.pg_bindir);
		}
		else
		{
			strncpy(pg_bindir, runtime_options.pg_bindir, MAXLEN);
		}
	}

	keywords[2] = "user";
	values[2] = (runtime_options.username[0]) ? runtime_options.username : NULL;
	keywords[3] = "dbname";
	values[3] = runtime_options.dbname;
	keywords[4] = "application_name";
	values[4] = (char *) progname;
	keywords[5] = NULL;
	values[5] = NULL;

	/*
	 * Initialize the logger.  If verbose command line parameter was input,
	 * make sure that the log level is at least INFO.  This is mainly useful
	 * for STANDBY CLONE.  That doesn't require a configuration file where a
	 * logging level might be specified at, but it often requires detailed
	 * logging to troubleshoot problems.
	 */
	logger_init(&options, progname, options.loglevel, options.logfacility);
	if (runtime_options.verbose)
		logger_min_verbose(LOG_INFO);

	/*
	 * Node configuration information is not needed for all actions, with
	 * STANDBY CLONE being the main exception.
	 */
	if (config_file_required)
	{
		if (options.node == NODE_NOT_FOUND)
		{
			if(config_file_parsed == true)
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

	if(options.use_replication_slots && wal_keep_segments_used)
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
	if(options.use_replication_slots)
	{
		maxlen_snprintf(repmgr_slot_name, "repmgr_slot_%i", options.node);
		repmgr_slot_name_ptr = repmgr_slot_name;
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
		case WITNESS_CREATE:
			do_witness_create();
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

	/* We need to connect to check configuration */
	log_info(_("connecting to database\n"));
	conn = establish_db_connection(options.conninfo, true);

	sqlquery_snprintf(sqlquery,
					  "SELECT conninfo, type "
					  "  FROM %s.repl_nodes ",
					  get_repmgr_schema_quoted(conn));
	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Unable to retrieve node information from the database\n%s\n"),
				PQerrorMessage(conn));
		log_notice(_("HINT: Please check that all nodes have been registered\n"));

		PQclear(res);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}
	PQfinish(conn);

	printf("Role      | Connection String\n");
	for (i = 0; i < PQntuples(res); i++)
	{
		conn = establish_db_connection(PQgetvalue(res, i, 0), false);
		if (PQstatus(conn) != CONNECTION_OK)
			strcpy(node_role, "  FAILED");
		else if (strcmp(PQgetvalue(res, i, 1), "witness") == 0)
			strcpy(node_role, "  witness");
		else if (is_standby(conn))
			strcpy(node_role, "  standby");
		else
			strcpy(node_role, "* master");

		printf("%-10s", node_role);
		printf("| %s\n", PQgetvalue(res, i, 0));

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
		log_err(_("cluster cleanup: Couldn't clean history\n%s\n"),
				PQerrorMessage(master_conn));
		PQclear(res);
		PQfinish(master_conn);
		exit(ERR_BAD_CONFIG);
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
}


static void
do_master_register(void)
{
	PGconn	   *conn;
	PGconn	   *master_conn;

	bool		schema_exists = false;
	int			ret;

	bool		record_created;

	conn = establish_db_connection(options.conninfo, true);

	/* Verify that master is a supported server version */
	log_info(_("connecting to master database\n"));
	check_server_version(conn, "master", true, NULL);

	/* Check we are a master */
	log_info(_("connected to master, checking its state\n"));
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

	if(!schema_exists)
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

	/* XXX we should check if a node with a different ID is registered as
	   master, otherwise it would be possible to insert a duplicate record
	   with --force, which would result in an unwelcome "multi-master" situation
	*/

	/* Delete any existing record for this node if --force set */
	if (runtime_options.force)
	{
		PGresult *res;
		bool node_record_deleted;

		begin_transaction(conn);

		res = get_node_record(conn, options.cluster_name, options.node);
		if (PQntuples(res))
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

		commit_transaction(conn);
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
										repmgr_slot_name_ptr);

	if(record_created == false)
	{
		PQfinish(conn);
		exit(ERR_DB_QUERY);
	}

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
		log_err(_("a master must be defined before configuring a slave\n"));
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

	record_created = create_node_record(master_conn,
										"standby register",
										options.node,
										"standby",
										options.upstream_node,
										options.cluster_name,
										options.node_name,
										options.conninfo,
										options.priority,
										repmgr_slot_name_ptr);



	if(record_created == false)
	{
		if(!runtime_options.force)
		{
			log_notice(_("HINT: use option -F/--force to overwrite an existing node record\n"));
		}

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
	int			ret;

	bool		node_record_deleted;

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
		log_err(_("a master must be defined before unregistering a slave\n"));
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Verify that standby and master are supported and compatible server
	 * versions
	 */
	check_master_standby_version_match(conn, master_conn);

	/* Now unregister the standby */
	log_info(_("unregistering the standby\n"));
	node_record_deleted = delete_node_record(master_conn,
										     options.node,
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
						options.node,
						"standby_unregister",
						true,
						NULL);

	PQfinish(master_conn);
	PQfinish(conn);

	log_info(_("standby unregistration complete\n"));
	log_notice(_("standby node correctly unregistered for cluster %s with id %d (conninfo: %s)\n"),
			   options.cluster_name, options.node, options.conninfo);
	return;
}


static void
do_standby_clone(void)
{
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

	char		master_data_directory[MAXFILENAME];
	char		local_data_directory[MAXFILENAME];

	char		master_config_file[MAXFILENAME] = "";
	char		local_config_file[MAXFILENAME] = "";
	bool		config_file_outside_pgdata = false;

	char		master_hba_file[MAXFILENAME] = "";
	char		local_hba_file[MAXFILENAME] = "";
	bool		hba_file_outside_pgdata = false;

	char		master_ident_file[MAXFILENAME] = "";
	char		local_ident_file[MAXFILENAME] = "";
	bool		ident_file_outside_pgdata = false;

	char		master_control_file[MAXFILENAME] = "";
	char		local_control_file[MAXFILENAME] = "";

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

	/* Connection parameters for master only */
	keywords[0] = "host";
	values[0] = runtime_options.host;
	keywords[1] = "port";
	values[1] = runtime_options.masterport;

	/* Connect to check configuration */
	log_info(_("connecting to upstream node\n"));
	upstream_conn = establish_db_connection_by_params(keywords, values, true);

	/* Verify that upstream node is a supported server version */
	log_info(_("connected to upstream node, checking its state\n"));
	server_version_num = check_server_version(upstream_conn, "master", true, NULL);

	check_upstream_config(upstream_conn, server_version_num, true);

	if(get_cluster_size(upstream_conn, cluster_size) == false)
		exit(ERR_DB_QUERY);

	log_info(_("Successfully connected to upstream node. Current installation size is %s\n"),
			 cluster_size);

	/*
	 * If --recovery-min-apply-delay was passed, check that
	 * we're connected to PostgreSQL 9.4 or later
	 */

	if(*runtime_options.recovery_min_apply_delay)
	{
		if(get_server_version(upstream_conn, NULL) < 90400)
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
	 # not set, fail with an error
	 */

	if(options.tablespace_mapping.head != NULL)
	{
		TablespaceListCell *cell;

		if(get_server_version(upstream_conn, NULL) < 90400)
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
							  "WHERE pg_tablespace_location(oid) = '%s'",
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
			strncpy(master_data_directory, PQgetvalue(res, i, 1), MAXFILENAME);
		}
		else if (strcmp(PQgetvalue(res, i, 0), "config_file") == 0)
		{
			if(strcmp(PQgetvalue(res, i, 2), "f") == 0)
			{
				config_file_outside_pgdata = true;
				external_config_file_copy_required = true;
				strncpy(master_config_file, PQgetvalue(res, i, 1), MAXFILENAME);
			}
		}
		else if (strcmp(PQgetvalue(res, i, 0), "hba_file") == 0)
		{
			if(strcmp(PQgetvalue(res, i, 2), "f") == 0)
			{
				hba_file_outside_pgdata  = true;
				external_config_file_copy_required = true;
				strncpy(master_hba_file, PQgetvalue(res, i, 1), MAXFILENAME);
			}
		}
		else if (strcmp(PQgetvalue(res, i, 0), "ident_file") == 0)
		{
			if(strcmp(PQgetvalue(res, i, 2), "f") == 0)
			{
				ident_file_outside_pgdata = true;
				external_config_file_copy_required = true;
				strncpy(master_ident_file, PQgetvalue(res, i, 1), MAXFILENAME);
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
		strncpy(local_data_directory, runtime_options.dest_dir, MAXFILENAME);
		strncpy(local_config_file, runtime_options.dest_dir, MAXFILENAME);
		strncpy(local_hba_file, runtime_options.dest_dir, MAXFILENAME);
		strncpy(local_ident_file, runtime_options.dest_dir, MAXFILENAME);
	}
	/*
	 * Otherwise use the same data directory as on the remote host
	 */
	else
	{
		strncpy(local_data_directory, master_data_directory, MAXFILENAME);
		strncpy(local_config_file, master_config_file, MAXFILENAME);
		strncpy(local_hba_file, master_hba_file, MAXFILENAME);
		strncpy(local_ident_file, master_ident_file, MAXFILENAME);
	}

	log_notice(_("starting backup...\n"));

	/*
	 * When using rsync only, we need to check the SSH connection early
	 */
	if(runtime_options.rsync_only)
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
		log_notice(_("HINT: Use -F/--force option to force this directory to be overwritten\n"));
		r = ERR_BAD_CONFIG;
		retval = ERR_BAD_CONFIG;
		goto stop_backup;
	}

	if(runtime_options.rsync_only)
	{
		/*
		 * From pg 9.1 default is to wait for a sync standby to ack, avoid that by
		 * turning off sync rep for this session
		 */
		if(set_config_bool(upstream_conn, "synchronous_commit", false) == false)
		{
			PQfinish(upstream_conn);
			exit(ERR_BAD_CONFIG);
		}

		if(start_backup(upstream_conn, first_wal_segment, runtime_options.fast_checkpoint) == false)
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
		if (r != 0)
		{
			log_warning(_("standby clone: failed copying master data directory '%s'\n"),
						master_data_directory);
			goto stop_backup;
		}

		/* Handle tablespaces */

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
			PQfinish(upstream_conn);
			exit(ERR_BAD_CONFIG);
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

			if(options.tablespace_mapping.head != NULL)
			{
				for (cell = options.tablespace_mapping.head; cell; cell = cell->next)
				{
					if(strcmp( tblspc_dir_src.data, cell->old_dir) == 0)
					{
						mapping_found = true;
						break;
					}
				}
			}

			if(mapping_found == true)
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

			/* Update symlink in pg_tblspc */
			if(mapping_found == true)
			{
				PQExpBufferData tblspc_symlink;

				initPQExpBuffer(&tblspc_symlink);
				appendPQExpBuffer(&tblspc_symlink, "%s/pg_tblspc/%s",
								  local_data_directory,
								  tblspc_oid.data);

				if (unlink(tblspc_symlink.data) < 0 && errno != ENOENT)
				{
					log_err(_("unable to remove tablespace symlink %s\n"), tblspc_symlink.data);
					exit(ERR_BAD_CONFIG);
				}
				if (symlink(tblspc_dir_dst.data, tblspc_symlink.data) < 0)
				{
					log_err(_("unable to create tablespace symlink from %s to %s\n"), tblspc_symlink.data, tblspc_dir_dst.data);
					exit(ERR_BAD_CONFIG);
				}


			}
		}

		PQclear(res);
	}
	else
	{
		r = run_basebackup(local_data_directory);
		if (r != 0)
		{
			log_warning(_("standby clone: base backup failed\n"));
			retval = ERR_BAD_BASEBACKUP;
			goto stop_backup;
		}
	}

	/*
	 * If configuration files were not inside the data directory, we;ll need to
	 * copy them via SSH (unless `--ignore-external-config-files` was provided)
	 *
	 * TODO: add option to place these files in the same location on the
	 * standby server as on the primary?
	 */

	if(external_config_file_copy_required && !runtime_options.ignore_external_config_files)
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

		if(config_file_outside_pgdata)
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

		if(hba_file_outside_pgdata)
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

		if(ident_file_outside_pgdata)
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
	if(runtime_options.rsync_only)
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

	if(runtime_options.rsync_only && pg_start_backup_executed)
	{
		log_notice(_("notifying master about backup completion...\n"));
		if(stop_backup(upstream_conn, last_wal_segment) == false)
		{
			r = ERR_BAD_BASEBACKUP;
			retval = ERR_BAD_BASEBACKUP;
		}
	}

	/* If the backup failed then exit */
	if (r != 0)
	{
		log_err(_("unable to take a base backup of the master server\n"));
		log_warning(_("destination directory (%s) may need to be cleaned up manually\n"),
				local_data_directory);
		PQfinish(upstream_conn);
		exit(retval);
	}

	/*
	 * Remove existing WAL from the target directory, since
	 * rsync's --exclude option doesn't do it.
	 */
	if (runtime_options.force)
	{
		char	script[MAXLEN];
		maxlen_snprintf(script, "rm -rf %s/pg_xlog/*",
						local_data_directory);
		r = system(script);
		if (r != 0)
		{
			log_err(_("unable to empty local WAL directory %s/pg_xlog/\n"),
					local_data_directory);
			exit(ERR_BAD_RSYNC);
		}
	}

	/* Finally, write the recovery.conf file */
	create_recovery_file(local_data_directory);

	/*
	 * If replication slots requested, create appropriate slot on the primary;
	 * create_recovery_file() will already have written `primary_slot_name` into
	 * `recovery.conf`
	 */
	if(options.use_replication_slots)
	{
		if(create_replication_slot(upstream_conn, repmgr_slot_name) == false)
		{
			PQfinish(upstream_conn);
			exit(ERR_DB_QUERY);
		}
	}

	if(runtime_options.rsync_only)
	{
		log_notice(_("standby clone (using rsync) complete\n"));
	}
	else
	{
		log_notice(_("standby clone (using pg_basebackup) complete\n"));
	}

	/*
	 * XXX It might be nice to provide the following options:
	 * - have repmgr start the daemon automatically
	 * - provide a custom pg_ctl command
	 */

	log_notice(_("HINT: you can now start your PostgreSQL server\n"));
	if (target_directory_provided)
	{
		log_notice(_("for example : pg_ctl -D %s start\n"),
				   local_data_directory);
	}
	else
	{
		log_notice(_("for example : /etc/init.d/postgresql start\n"));
	}

	/* Log the event */
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

	create_event_record(upstream_conn,
						&options,
						options.node,
						"standby_clone",
						true,
						event_details.data);

	PQfinish(upstream_conn);
	exit(retval);
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
	log_info(_("connected to standby, checking its state\n"));

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
		if(!retval)
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
	if(update_node_record_set_master(conn, options.node) == false)
	{
		initPQExpBuffer(&details);
		appendPQExpBuffer(&details,
						  _("unable to update node record for node %i"),
						  options.node);

		log_err("%s\n", details.data);

		create_event_record(NULL,
							&options,
							options.node,
							"repmgrd_failover_promote",
							false,
							details.data);

		exit(ERR_DB_QUERY);
	}


	initPQExpBuffer(&details);
	appendPQExpBuffer(&details,
					  "Node %i was successfully promoted to master",
					  options.node);

	log_notice(_("STANDBY PROMOTE successful.  You should REINDEX any hash indexes you have.\n"));

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


static void
do_standby_follow(void)
{
	PGconn	   *conn;

	char		script[MAXLEN];
	char		master_conninfo[MAXLEN];
	PGconn	   *master_conn;
	int			master_id;

	int			r,
				retval;
	char		data_dir[MAXLEN];

	bool        success;


	/* We need to connect to check configuration */
	log_info(_("connecting to standby database\n"));
	conn = establish_db_connection(options.conninfo, true);
	log_info(_("connected to standby, checking its state\n"));

	/* Check we are in a standby node */
	retval = is_standby(conn);
	if (retval == 0 || retval == -1)
	{
		log_err(_(retval == 0 ? "this command should be executed on a standby node\n" :
				  "connection to node lost!\n"));

		PQfinish(conn);
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

	/* Check we are going to point to a master */
	retval = is_standby(master_conn);
	if (retval)
	{
		log_err(_(retval == 1 ? "the node to follow should be a master\n" :
				  "connection to node lost!\n"));

		PQfinish(conn);
		PQfinish(master_conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Verify that standby and master are supported and compatible server
	 * versions
	 */
	check_master_standby_version_match(conn, master_conn);

	/*
	 * set the host and masterport variables with the master ones before
	 * closing the connection because we will need them to recreate the
	 * recovery.conf file
	 */
	strncpy(runtime_options.host, PQhost(master_conn), MAXLEN);
	strncpy(runtime_options.masterport, PQport(master_conn), MAXLEN);
	strncpy(runtime_options.username, PQuser(master_conn), MAXLEN);

	log_info(_("changing standby's master\n"));

	/* Get the data directory full path */
	success = get_pg_setting(conn, "data_directory", data_dir);
	PQfinish(conn);

	if (success == false)
	{
		log_err(_("unable to determine data directory\n"));
		exit(ERR_BAD_CONFIG);
	}

	/* write the recovery.conf file */
	if (!create_recovery_file(data_dir))
		exit(ERR_BAD_CONFIG);

	/* Finally, restart the service */
	maxlen_snprintf(script, "%s %s -w -D %s -m fast restart",
					make_pg_path("pg_ctl"), options.pg_ctl_options, data_dir);

	log_notice(_("restarting server using '%s'\n"),
			   script);

	r = system(script);
	if (r != 0)
	{
		log_err(_("unable to restart server\n"));
		exit(ERR_NO_RESTART);
	}

	if(update_node_record_set_upstream(master_conn, options.cluster_name,
									   options.node, master_id) == false)
	{
		log_err(_("unable to update upstream node"));
		PQfinish(master_conn);

		exit(ERR_BAD_CONFIG);
	}
	PQfinish(master_conn);

	return;
}


static void
do_witness_create(void)
{
	PGconn	   *masterconn;
	PGconn	   *witnessconn;
	PGresult   *res;
	char		sqlquery[QUERY_STR_LEN];

	char		script[MAXLEN];
	char		buf[MAXLEN];
	FILE	   *pg_conf = NULL;

	int			r = 0,
				retval;

	char		master_hba_file[MAXLEN];
	bool        success;
	bool		record_created;

	/* Connection parameters for master only */
	keywords[0] = "host";
	values[0] = runtime_options.host;
	keywords[1] = "port";
	values[1] = runtime_options.masterport;

	/* We need to connect to check configuration and copy it */
	masterconn = establish_db_connection_by_params(keywords, values, true);
	if (!masterconn)
	{
		/* No event logging possible as we can't connect to the master */
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
	if (!create_pg_dir(runtime_options.dest_dir, runtime_options.force))
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

	sprintf(script, "%s %s -D %s init -o \"%s-U %s\"",
			make_pg_path("pg_ctl"),
			options.pg_ctl_options, runtime_options.dest_dir,
			runtime_options.initdb_no_pwprompt ? "" : "-W ",
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

	xsnprintf(buf, sizeof(buf), "\n#Configuration added by %s\n", progname);
	fputs(buf, pg_conf);

	/*
	 * If not specified by the user, the default port for the witness server
	 * is 5499; this is intended to support running the witness server as
	 * a separate instance on a normal node server, rather than on its own
	 * dedicated server.
	 */
	if (!runtime_options.localport[0])
		strncpy(runtime_options.localport, "5499", MAXLEN);

	xsnprintf(buf, sizeof(buf), "port = %s\n", runtime_options.localport);
	fputs(buf, pg_conf);

	xsnprintf(buf, sizeof(buf), "shared_preload_libraries = 'repmgr_funcs'\n");
	fputs(buf, pg_conf);

	xsnprintf(buf, sizeof(buf), "listen_addresses = '*'\n");
	fputs(buf, pg_conf);

	fclose(pg_conf);


	/* start new instance */
	sprintf(script, "%s %s -w -D %s start",
			make_pg_path("pg_ctl"),
			options.pg_ctl_options, runtime_options.dest_dir);
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
	if (runtime_options.username[0] && runtime_options.localport[0] && strcmp(runtime_options.username,"postgres") != 0)
        {
		/* create required user; needs to be superuser to create untrusted language function in c */
		sprintf(script, "%s -p %s --superuser --login -U %s %s",
				make_pg_path("createuser"),
				runtime_options.localport, runtime_options.superuser, runtime_options.username);
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
	if(runtime_options.dbname[0] && strcmp(runtime_options.dbname,"postgres") != 0 && runtime_options.localport[0])
	{
		/* create required db */
		sprintf(script, "%s -p %s -U %s --owner=%s %s",
				make_pg_path("createdb"),
				runtime_options.localport, runtime_options.superuser, runtime_options.username, runtime_options.dbname);
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
		char *errmsg = _("unable to retrieve location of pg_hba.conf");
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
		char *errmsg = _("unable to copy pg_hba.conf from master");
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

	/* reload to adapt for changed pg_hba.conf */
	sprintf(script, "%s %s -w -D %s reload",
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

	/* register ourselves in the master */

	record_created = create_node_record(masterconn,
										"witness create",
										options.node,
										"witness",
										NO_UPSTREAM_NODE,
										options.cluster_name,
										options.node_name,
										options.conninfo,
										options.priority,
										NULL);

	if(record_created == false)
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

	/* establish a connection to the witness, and create the schema */
	witnessconn = establish_db_connection(options.conninfo, true);

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
							_("unable to create schema on witness"));
		PQfinish(masterconn);
		PQfinish(witnessconn);
		exit(ERR_BAD_CONFIG);
	}

	commit_transaction(witnessconn);

	/* copy configuration from master, only repl_nodes is needed */
	if (!copy_configuration(masterconn, witnessconn, options.cluster_name))
	{
		create_event_record(masterconn,
							&options,
							options.node,
							"witness_create",
							false,
							_("Unable to copy configuration from master"));
		PQfinish(masterconn);
		PQfinish(witnessconn);
		exit(ERR_BAD_CONFIG);
	}

	/* drop superuser powers if needed */
	if (runtime_options.username[0] && runtime_options.localport[0] && strcmp(runtime_options.username,"postgres") != 0)
	{
		sqlquery_snprintf(sqlquery, "ALTER ROLE %s NOSUPERUSER", runtime_options.username);
		log_info(_("revoking superuser status on user %s: %s.\n"),
				   runtime_options.username, sqlquery);

		log_debug(_("witness create: %s\n"), sqlquery);
		res = PQexec(witnessconn, sqlquery);
		if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			log_err(_("unable to alter user privileges for user %s: %s\n"),
					runtime_options.username,
					PQerrorMessage(witnessconn));
			PQfinish(masterconn);
			PQfinish(witnessconn);
			exit(ERR_DB_QUERY);
		}
	}

	/* Log the event */
	create_event_record(masterconn,
						&options,
						options.node,
						"witness_create",
						true,
						NULL);

	PQfinish(masterconn);
	PQfinish(witnessconn);

	log_notice(_("configuration has been successfully copied to the witness\n"));
}


static void
help(const char *progname)
{
	printf(_("%s: replication management tool for PostgreSQL\n"), progname);
	printf(_("\n"));
	printf(_("Usage:\n"));
	printf(_("  %s [OPTIONS] master  {register}\n"), progname);
	printf(_("  %s [OPTIONS] standby {register|unregister|clone|promote|follow}\n"),
		   progname);
	printf(_("  %s [OPTIONS] cluster {show|cleanup}\n"), progname);
	printf(_("\n"));
	printf(_("General options:\n"));
	printf(_("  -?, --help                          show this help, then exit\n"));
	printf(_("  -V, --version                       output version information, then exit\n"));
	printf(_("  -v, --verbose                       output verbose activity information\n"));
	printf(_("\nConnection options:\n"));
	printf(_("  -d, --dbname=DBNAME                 database to connect to\n"));
	printf(_("  -h, --host=HOSTNAME                 database server host or socket directory\n"));
	printf(_("  -p, --port=PORT                     database server port\n"));
	printf(_("  -U, --username=USERNAME             database user name to connect as\n"));
	printf(_("\nConfiguration options:\n"));
	printf(_("  -b, --pg_bindir=PATH                path to PostgreSQL binaries (optional)\n"));
	printf(_("  -D, --data-dir=DIR                  local directory where the files will be\n" \
			 "                                      copied to\n"));
	printf(_("  -l, --local-port=PORT               standby or witness server local port\n"));
	printf(_("  -f, --config-file=PATH              path to the configuration file\n"));
	printf(_("  -R, --remote-user=USERNAME          database server username for rsync\n"));
	printf(_("  -S, --superuser=USERNAME            superuser username for witness database\n" \
			 "                                      (default: postgres)\n"));
	printf(_("  -w, --wal-keep-segments=VALUE       minimum value for the GUC\n" \
			 "                                      wal_keep_segments (default: %s)\n"), DEFAULT_WAL_KEEP_SEGMENTS);
	printf(_("  -k, --keep-history=VALUE            keeps indicated number of days of history\n"));
	printf(_("  -F, --force                         force potentially dangerous operations to happen\n"));
	printf(_("  -W, --wait                          wait for a master to appear\n"));
	printf(_("  -r, --rsync-only                    use only rsync to clone a standby\n"));
	printf(_("  -c, --fast-checkpoint               force fast checkpoint when cloning a standby\n"));
	printf(_("  --recovery-min-apply-delay=VALUE    set recovery_min_apply_delay in recovery.conf\n" \
 			 "                                      when cloning a standby (PostgreSQL 9.4 and later)\n"));
	printf(_("  --ignore-external-config-files      don't copy configuration files located outside \n" \
			 "                                      the data directory when cloning a standby\n"));
	printf(_("  --initdb-no-pwprompt                don't require superuser password when running initdb\n"));
	printf(_("  --check-upstream-config             verify upstream server configuration\n"));
	printf(_("\n%s performs the following node management tasks:\n\n"), progname);
	printf(_("COMMANDS:\n"));
	printf(_(" master register         - registers the master in a cluster\n"));
	printf(_(" standby clone [node]    - creates a new standby\n"));
	printf(_(" standby register        - registers a standby in a cluster\n"));
	printf(_(" standby unregister      - unregisters a standby in a cluster\n"));
	printf(_(" standby promote         - promotes a specific standby to master\n"));
	printf(_(" standby follow          - makes standby follow a new master\n"));
	printf(_(" cluster show            - displays information about cluster nodes\n"));
	printf(_(" cluster cleanup         - prunes or truncates monitoring history\n" \
			 "                           (monitoring history creation requires repmgrd\n" \
			 "                           with --monitoring-history option)\n"));
}


/*
 * Creates a recovery file for a standby.
 */
static bool
create_recovery_file(const char *data_dir)
{
	FILE	   *recovery_file;
	char		recovery_file_path[MAXLEN];
	char		line[MAXLEN];

	maxlen_snprintf(recovery_file_path, "%s/%s", data_dir, RECOVERY_FILE);

	recovery_file = fopen(recovery_file_path, "w");
	if (recovery_file == NULL)
	{
		log_err(_("unable to create recovery.conf file at '%s'\n"), recovery_file_path);
		return false;
	}

	log_debug(_("create_recovery_file(): creating '%s'...\n"), recovery_file_path);

	/* standby_mode = 'on' */
	maxlen_snprintf(line, "standby_mode = 'on'\n");

	if(write_recovery_file_line(recovery_file, recovery_file_path, line) == false)
		return false;

	log_debug(_("recovery.conf: %s"), line);

	/* primary_conninfo = '...' */
	write_primary_conninfo(line);

	if(write_recovery_file_line(recovery_file, recovery_file_path, line) == false)
		return false;

	log_debug(_("recovery.conf: %s"), line);

	/* recovery_target_timeline = 'latest' */
	maxlen_snprintf(line, "recovery_target_timeline = 'latest'\n");

	if(write_recovery_file_line(recovery_file, recovery_file_path, line) == false)
		return false;

	log_debug(_("recovery.conf: %s"), line);

	/* recovery_min_apply_delay = ... (optional) */
	if(*runtime_options.recovery_min_apply_delay)
	{
		maxlen_snprintf(line, "recovery_min_apply_delay = %s\n",
						runtime_options.recovery_min_apply_delay);
		if(write_recovery_file_line(recovery_file, recovery_file_path, line) == false)
			return false;

		log_debug(_("recovery.conf: %s"), line);
	}

	/* primary_slot_name = '...' (optional, for 9.4 and later) */
	if(options.use_replication_slots)
	{
		maxlen_snprintf(line, "primary_slot_name = %s\n",
						repmgr_slot_name);
		if(write_recovery_file_line(recovery_file, recovery_file_path, line) == false)
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
			maxlen_snprintf(script, "ssh -o Batchmode=yes %s %s %s",
							options.ssh_options, host, truebin_paths[i]);
		else
			maxlen_snprintf(script, "ssh -o Batchmode=yes %s %s -l %s %s",
							options.ssh_options, host, remote_user,
							truebin_paths[i]);

		log_debug(_("command is: %s\n"), script);
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
	 */
	if (is_directory)
	{
		/* Files which we don't want */
		appendPQExpBuffer(&rsync_flags, "%s",
						  " --exclude=postmaster.pid --exclude=postmaster.opts --exclude=global/pg_control");

		if(server_version_num >= 90400)
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

		if(server_version_num >= 90400)
		{
			appendPQExpBuffer(&rsync_flags, "%s",
							  " --exclude=pg_replslot/*");
		}

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
run_basebackup(const char *data_dir)
{
	char				script[MAXLEN];
	int					r = 0;
	PQExpBufferData 	params;
	TablespaceListCell *cell;

	/* Create pg_basebackup command line options */

	initPQExpBuffer(&params);

	appendPQExpBuffer(&params, " -D %s", data_dir);

	if(strlen(runtime_options.host))
	{
		appendPQExpBuffer(&params, " -h %s", runtime_options.host);
	}

	if(strlen(runtime_options.masterport))
	{
		appendPQExpBuffer(&params, " -p %s", runtime_options.masterport);
	}

	if(strlen(runtime_options.username))
	{
		appendPQExpBuffer(&params, " -U %s", runtime_options.username);
	}

	if(runtime_options.fast_checkpoint) {
		appendPQExpBuffer(&params, " -c fast");
	}

	if(options.tablespace_mapping.head != NULL)
	{
		for (cell = options.tablespace_mapping.head; cell; cell = cell->next)
		{
			appendPQExpBuffer(&params, " -T %s=%s", cell->old_dir, cell->new_dir);
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
			if (runtime_options.host[0] || runtime_options.masterport[0] ||
				runtime_options.username[0] || runtime_options.dbname[0])
			{
				error_list_append(_("master connection parameters not required when executing MASTER REGISTER"));
			}
			if (runtime_options.dest_dir[0])
			{
				error_list_append(_("destination directory not required when executing MASTER REGISTER"));
			}
			break;
		case STANDBY_REGISTER:

			/*
			 * To register a standby we only need the repmgr.conf we don't
			 * need connection parameters to the master because we can detect
			 * the master in repl_nodes
			 */
			if (runtime_options.host[0] || runtime_options.masterport[0] ||
				runtime_options.username[0] || runtime_options.dbname[0])
			{
				error_list_append(_("master connection parameters not required when executing STANDBY REGISTER"));
			}
			if (runtime_options.dest_dir[0])
			{
				error_list_append(_("destination directory not required when executing STANDBY REGISTER"));
			}
			break;
		case STANDBY_UNREGISTER:

			/*
			 * To unregister a standby we only need the repmgr.conf we don't
			 * need connection parameters to the master because we can detect
			 * the master in repl_nodes
			 */
			if (runtime_options.host[0] || runtime_options.masterport[0] ||
				runtime_options.username[0] || runtime_options.dbname[0])
			{
				error_list_append(_("master connection parameters not required when executing STANDBY UNREGISTER"));
			}
			if (runtime_options.dest_dir[0])
			{
				error_list_append(_("destination directory not required when executing STANDBY UNREGISTER"));
			}
			break;
		case STANDBY_PROMOTE:

			/*
			 * To promote a standby we only need the repmgr.conf we don't want
			 * connection parameters to the master because we will try to
			 * detect the master in repl_nodes if we can't find it then the
			 * promote action will be cancelled
			 */
			if (runtime_options.host[0] || runtime_options.masterport[0] ||
				runtime_options.username[0] || runtime_options.dbname[0])
			{
				error_list_append(_("master connection parameters not required when executing STANDBY PROMOTE"));
			}
			if (runtime_options.dest_dir[0])
			{
				error_list_append(_("destination directory not required when executing STANDBY PROMOTE"));
			}
			break;
		case STANDBY_FOLLOW:

			/*
			 * To make a standby follow a master we only need the repmgr.conf
			 * we don't want connection parameters to the new master because
			 * we will try to detect the master in repl_nodes if we can't find
			 * it then the follow action will be cancelled
			 */
			if (runtime_options.host[0] || runtime_options.masterport[0] ||
				runtime_options.username[0] || runtime_options.dbname[0])
			{
				error_list_append(_("master connection parameters not required when executing STANDBY FOLLOW"));
			}
			if (runtime_options.dest_dir[0])
			{
				error_list_append(_("destination directory not required when executing STANDBY FOLLOW"));
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
				error_list_append(_("master hostname (-h/--host) required when executing STANDBY CLONE"));
			}

			if (strcmp(runtime_options.dbname, "") == 0)
			{
				error_list_append(_("master database name (-d/--dbname) required when executing STANDBY CLONE"));
			}

			if (strcmp(runtime_options.username, "") == 0)
			{
				error_list_append(_("master database username (-U/--username) required when executing STANDBY CLONE"));
			}

			config_file_required = false;
			break;
		case WITNESS_CREATE:
			/* allow all parameters to be supplied */
			break;
		case CLUSTER_SHOW:
			/* allow all parameters to be supplied */
			break;
		case CLUSTER_CLEANUP:
			/* allow all parameters to be supplied */
			break;
	}

	if(action != STANDBY_CLONE)
	{
		if(runtime_options.rsync_only)
		{
			error_list_append(_("--rsync-only can only be used when executing STANDBY CLONE"));
		}

		if(runtime_options.fast_checkpoint)
		{
			error_list_append(_("--fast-checkpoint can only be used when executing STANDBY CLONE"));
		}

		if(runtime_options.ignore_external_config_files)
		{
			error_list_append(_("--ignore-external-config-files can only be used when executing STANDBY CLONE"));
		}

		if(*runtime_options.recovery_min_apply_delay)
		{
			error_list_append(_("--recovery-min-apply-delay can only be used when executing STANDBY CLONE"));
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

		if(res != NULL)
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

		if(res != NULL)
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

		if(res != NULL)
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
					  "  upstream_node_id INTEGER NULL REFERENCES %s.repl_nodes (id), "
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

		if(res != NULL)
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

		if(res != NULL)
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

		if(res != NULL)
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

		if(res != NULL)
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

		if(res != NULL)
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

		if(res != NULL)
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

		if(res != NULL)
			PQclear(res);

		return false;
	}
	PQclear(res);

	return true;
}


/* This function uses global variables to determine connection settings. Special
 * usage of the PGPASSWORD variable is handled, but strongly discouraged */
static void
write_primary_conninfo(char *line)
{
	char		host_buf[MAXLEN] = "";
	char		conn_buf[MAXLEN] = "";
	char		user_buf[MAXLEN] = "";
	char		appname_buf[MAXLEN] = "";
	char		password_buf[MAXLEN] = "";

	/* Environment variable for password (UGLY, please use .pgpass!) */
	const char *password = getenv("PGPASSWORD");

	if (password != NULL)
	{
		maxlen_snprintf(password_buf, " password=%s", password);
	}
	else if (require_password)
	{
		log_err(_("password required but none provided and PGPASSWORD not set\n"));
		exit(ERR_BAD_PASSWORD);
	}

	if (runtime_options.host[0])
	{
		maxlen_snprintf(host_buf, " host=%s", runtime_options.host);
	}

	if (runtime_options.username[0])
	{
		maxlen_snprintf(user_buf, " user=%s", runtime_options.username);
	}

	if (options.node_name[0])
	{
		maxlen_snprintf(appname_buf, " application_name=%s", options.node_name);
	}

	maxlen_snprintf(conn_buf, "port=%s%s%s%s%s",
	   (runtime_options.masterport[0]) ? runtime_options.masterport : "5432",
					host_buf, user_buf, password_buf,
					appname_buf);

	maxlen_snprintf(line, "primary_conninfo = '%s'\n", conn_buf);
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
	if(server_version_num < MIN_SUPPORTED_VERSION_NUM)
	{
		if (server_version_num > 0)
			log_err(_("%s requires %s to be PostgreSQL %s or later\n"),
					progname,
					server_type,
					MIN_SUPPORTED_VERSION
				);

		if(exit_on_error == true)
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
	if(master_version_num < 0)
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
	if(server_version_num < 90300)
	{
		i = guc_set(conn, "wal_level", "=", "hot_standby");
		wal_error_message = _("parameter 'wal_level' must be set to 'hot_standby'");
	}
	else
	{
		char *levels[] = {
			"hot_standby",
			"logical",
		};

		int j = 0;
		wal_error_message = _("parameter 'wal_level' must be set to 'hot_standby' or 'logical'");

		for(; j < 2; j++)
		{
			i = guc_set(conn, "wal_level", "=", levels[j]);
			if(i)
			{
				break;
			}
		}
	}

	if (i == 0 || i == -1)
	{
		if (i == 0)
			log_err("%s\n",
					wal_error_message);

		if(exit_on_error == true)
		{
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		config_ok = false;
	}

	if(options.use_replication_slots)
	{
		/* Does the server support physical replication slots? */
		if(server_version_num < 90400)
		{
			log_err(_("server version must be 9.4 or later to enable replication slots\n"));

			if(exit_on_error == true)
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
					log_notice(_("HINT: 'max_replication_slots' should be set to at least the number of expected standbys\n"));
					if(exit_on_error == true)
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
				if(server_version_num >= 90400)
				{
					log_notice(_("HINT: in PostgreSQL 9.4 and later, replication slots can be used, which "
							   "do not require 'wal_keep_segments' to be set to a high value "
							   "(set parameter 'use_replication_slots' in the configuration file to enable)\n"
								 ));
				}
			}

			if(exit_on_error == true)
			{
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}

			config_ok = false;
		}
	}

	i = guc_set(conn, "archive_mode", "=", "on");
	if (i == 0 || i == -1)
	{
		if (i == 0)
			log_err(_("parameter 'archive_mode' must be set to 'on'\n"));

		if(exit_on_error == true)
		{
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		config_ok = false;
	}


	/*
	 * check that 'archive_command' is non empty (however it's not practical to
	 * check that it's actually valid)
	 *
	 * if 'archive_mode' is not on, pg_settings returns '(disabled)' regardless
	 * of what's in 'archive_command', so until 'archive_mode' is on we can't
	 * properly check it.
	 */

	if(guc_set(conn, "archive_mode", "=", "on"))
	{
		i = guc_set(conn, "archive_command", "!=", "");

		if (i == 0 || i == -1)
		{
			if (i == 0)
				log_err(_("parameter 'archive_command' must be set to a valid command\n"));

			if(exit_on_error == true)
			{
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}

			config_ok = false;
		}
	}


	i = guc_set(conn, "hot_standby", "=", "on");
	if (i == 0 || i == -1)
	{
		if (i == 0)
			log_err(_("parameter 'hot_standby' must be set to 'on'\n"));

		if(exit_on_error == true)
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
			log_notice(_("HINT: 'max_wal_senders' should be set to at least the number of expected standbys\n"));
		}

		if(exit_on_error == true)
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

	log_debug(_("Setting %i as master and marking existing master as failed\n"), this_node_id);

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

	/* Connection parameters for upstream server only */
	keywords[0] = "host";
	values[0] = runtime_options.host;
	keywords[1] = "port";
	values[1] = runtime_options.masterport;
	keywords[2] = "dbname";
	values[2] = runtime_options.dbname;

	/* We need to connect to check configuration and start a backup */
	log_info(_("connecting to upstream server\n"));
	conn = establish_db_connection_by_params(keywords, values, true);

	/* Verify that upstream server is a supported server version */
	log_info(_("connected to upstream server, checking its state\n"));
	server_version_num = check_server_version(conn, "upstream server", false, NULL);

	config_ok = check_upstream_config(conn, server_version_num, false);

	if(config_ok == true)
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
error_list_append(char *error_message)
{
	ErrorListCell *cell;

	cell = (ErrorListCell *) pg_malloc0(sizeof(ErrorListCell));

	if(cell == NULL)
	{
		log_err(_("unable to allocate memory; terminating.\n"));
		exit(ERR_BAD_CONFIG);
	}

	cell->error_message = error_message;

	if(cli_errors.tail)
	{
		cli_errors.tail->next = cell;
	}
	else
	{
		cli_errors.head = cell;
	}

	cli_errors.tail = cell;
}


static void
exit_with_errors(void)
{
	ErrorListCell *cell;

	fprintf(stderr, _("%s: Replication manager \n"), progname);

	for (cell = cli_errors.head; cell; cell = cell->next)
	{
		fprintf(stderr, "[ERROR] %s\n", cell->error_message);
	}

	fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);

	exit(ERR_BAD_CONFIG);
}
