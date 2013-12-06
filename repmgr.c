/*
 * repmgr.c - Command interpreter for the repmgr
 * Copyright (C) 2ndQuadrant, 2010-2012
 *
 * This module is a command-line utility to easily setup a cluster of
 * hot standby servers for an HA environment
 *
 * Commands implemented are.
 * MASTER REGISTER
 * STANDBY REGISTER, STANDBY CLONE, STANDBY FOLLOW, STANDBY PROMOTE
 * CLUSTER SHOW, CLUSTER CLEANUP
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

#include "log.h"
#include "config.h"
#include "check_dir.h"
#include "strutil.h"
#include "version.h"

#define RECOVERY_FILE "recovery.conf"
#define RECOVERY_DONE_FILE "recovery.done"

#define NO_ACTION		 0		/* Not a real action, just to initialize */
#define MASTER_REGISTER	 1
#define STANDBY_REGISTER 2
#define STANDBY_CLONE 	 3
#define STANDBY_PROMOTE  4
#define STANDBY_FOLLOW 	 5
#define WITNESS_CREATE   6
#define CLUSTER_SHOW     7
#define CLUSTER_CLEANUP  8

static bool create_recovery_file(const char *data_dir);
static int test_ssh_connection(char *host, char *remote_user);
static int	copy_remote_files(char *host, char *remote_user, char *remote_path,
                              char *local_path, bool is_directory);
static bool check_parameters_for_action(const int action);
static bool create_schema(PGconn *conn);
static bool copy_configuration(PGconn *masterconn, PGconn *witnessconn);
static void write_primary_conninfo(char* line);

static void do_master_register(void);
static void do_standby_register(void);
static void do_standby_clone(void);
static void do_standby_promote(void);
static void do_standby_follow(void);
static void do_witness_create(void);
static void do_cluster_show(void);
static void do_cluster_cleanup(void);

static void usage(void);
static void help(const char *progname);

/* Global variables */
static const char *progname;
static const char *keywords[6];
static const char *values[6];
char repmgr_schema[MAXLEN];
bool need_a_node = true;

/* XXX This should be mapped into a command line option */
bool require_password = false;

/* Initialization of runtime options */
t_runtime_options runtime_options = { "", "", "", "", "", "", DEFAULT_WAL_KEEP_SEGMENTS, false, false, false, false, "", "", 0 };
t_configuration_options options = { "", -1, "", MANUAL_FAILOVER, -1, "", "", "", "", "", "", -1 };

static char		*server_mode = NULL;
static char		*server_cmd = NULL;

int
main(int argc, char **argv)
{
	static struct option long_options[] =
	{
		{"dbname", required_argument, NULL, 'd'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"username", required_argument, NULL, 'U'},
		{"dest-dir", required_argument, NULL, 'D'},
		{"local-port", required_argument, NULL, 'l'},
		{"config-file", required_argument, NULL, 'f'},
		{"remote-user", required_argument, NULL, 'R'},
		{"wal-keep-segments", required_argument, NULL, 'w'},
		{"keep-history", required_argument, NULL, 'k'},
		{"force", no_argument, NULL, 'F'},
		{"wait", no_argument, NULL, 'W'},
		{"ignore-rsync-warning", no_argument, NULL, 'I'},
		{"verbose", no_argument, NULL, 'v'},
		{NULL, 0, NULL, 0}
	};

	int			optindex;
	int			c;
	int			action = NO_ACTION;
	const char *password = getenv("PGPASSWORD");

	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			help(progname);
			exit(SUCCESS);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			printf("%s %s (PostgreSQL %s)\n", progname, REPMGR_VERSION, PG_VERSION);
			exit(SUCCESS);
		}
	}


	while ((c = getopt_long(argc, argv, "d:h:p:U:D:l:f:R:w:k:FWIv", long_options,
	                        &optindex)) != -1)
	{
		switch (c)
		{
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
				strncpy(runtime_options.wal_keep_segments, optarg, MAXLEN);
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
		default:
			usage();
			exit(ERR_BAD_CONFIG);
		}
	}

	/*
	 * Now we need to obtain the action, this comes in one of these forms:
	 * MASTER REGISTER |
	 * STANDBY {REGISTER | CLONE [node] | PROMOTE | FOLLOW [node]} |
	 * WITNESS CREATE
	 * CLUSTER {SHOW | CLEANUP}
	 *
	 * the node part is optional, if we receive it then we shouldn't
	 * have received a -h option
	 */
	if (optind < argc)
	{
		server_mode = argv[optind++];
		if (strcasecmp(server_mode, "STANDBY") != 0 && strcasecmp(server_mode, "MASTER") != 0 &&
		        strcasecmp(server_mode, "WITNESS") != 0 &&
		        strcasecmp(server_mode, "CLUSTER") != 0 )
		{
			usage();
			exit(ERR_BAD_CONFIG);
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
			else if (strcasecmp(server_cmd, "CLONE") == 0)
				action = STANDBY_CLONE;
			else if (strcasecmp(server_cmd, "PROMOTE") == 0)
				action = STANDBY_PROMOTE;
			else if (strcasecmp(server_cmd, "FOLLOW") == 0)
				action = STANDBY_FOLLOW;
		}
		else if (strcasecmp(server_mode, "CLUSTER") == 0)
		{
			if( strcasecmp(server_cmd, "SHOW") == 0)
				action = CLUSTER_SHOW;
			else if(strcasecmp(server_cmd, "CLEANUP") == 0)
				action = CLUSTER_CLEANUP;
		}
		else if (strcasecmp(server_mode, "WITNESS") == 0)
			if (strcasecmp(server_cmd, "CREATE") == 0)
				action = WITNESS_CREATE;
	}

	if (action == NO_ACTION)
	{
		usage();
		exit(ERR_BAD_CONFIG);
	}

	/* For some actions we still can receive a last argument */
	if (action == STANDBY_CLONE)
	{
		if (optind < argc)
		{
			if (runtime_options.host[0])
			{
				log_err(_("Conflicting parameters:  you can't use -h while providing a node separately.\n"));
				usage();
				exit(ERR_BAD_CONFIG);
			}
			strncpy(runtime_options.host, argv[optind++], MAXLEN);
		}
	}

	switch (optind < argc)
	{
	case 0:
		break;
	default:
		log_err(_("%s: too many command-line arguments (first extra is \"%s\")\n"),
		        progname, argv[optind]);
		usage();
		exit(ERR_BAD_CONFIG);
	}

	if (!check_parameters_for_action(action))
		exit(ERR_BAD_CONFIG);

	if (!runtime_options.dbname[0])
	{
		if (getenv("PGDATABASE"))
			strncpy(runtime_options.dbname, getenv("PGDATABASE"), MAXLEN);
		else if (getenv("PGUSER"))
			strncpy(runtime_options.dbname, getenv("PGUSER"), MAXLEN);
		else
			strncpy(runtime_options.dbname, DEFAULT_DBNAME, MAXLEN);
	}

	/* Read the configuration file, normally repmgr.conf */
	if (!runtime_options.config_file[0])
		strncpy(runtime_options.config_file, DEFAULT_CONFIG_FILE, MAXLEN);

	if (runtime_options.verbose)
		printf(_("Opening configuration file: %s\n"), runtime_options.config_file);

	/*
	 * XXX Do not read config files for action where it is not required (clone
	 * for example).
	 */
	parse_config(runtime_options.config_file, &options);

	keywords[2] = "user";
	values[2] = (runtime_options.username[0]) ? runtime_options.username : NULL;
	keywords[3] = "dbname";
	values[3] = runtime_options.dbname;
	keywords[4] = "application_name";
	values[4] = (char *) progname;

	if (password != NULL)
	{
		keywords[5] = "password";
		values[5] = password;
	}
	else
	{
		keywords[5] = NULL;
		values[5] = NULL;
	}

	/*
	 * Initialize the logger.  If verbose command line parameter was
	 * input, make sure that the log level is at least INFO.  This
	 * is mainly useful for STANDBY CLONE.  That doesn't require a
	 * configuration file where a logging level might be specified
	 * at, but it often requires detailed logging to troubleshoot
	 * problems.
	 */
	logger_init(progname, options.loglevel, options.logfacility);
	if (runtime_options.verbose)
		logger_min_verbose(LOG_INFO);

	/*
	 * Node configuration information is not needed for all actions,
	 * with STANDBY CLONE being the main exception.
	 */
	if (need_a_node)
	{
		if (options.node == -1)
		{
			log_err(_("Node information is missing. "
			          "Check the configuration file.\n"));
			exit(ERR_BAD_CONFIG);
		}
	}

	/* Prepare the repmgr schema variable */
	snprintf(repmgr_schema, MAXLEN, "%s%s", DEFAULT_REPMGR_SCHEMA_PREFIX, options.cluster_name);

	switch (action)
	{
	case MASTER_REGISTER:
		do_master_register();
		break;
	case STANDBY_REGISTER:
		do_standby_register();
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
		usage();
		exit(ERR_BAD_CONFIG);
	}
	logger_shutdown();

	return 0;
}

static void
do_cluster_show(void)
{
	PGconn	 *conn;
	PGresult *res;
	char	 sqlquery[QUERY_STR_LEN];
	char	 node_role[MAXLEN];
	int		 i;

	/* We need to connect to check configuration */
	log_info(_("%s connecting to database\n"), progname);
	conn = establishDBConnection(options.conninfo, true);

	sqlquery_snprintf(sqlquery, "SELECT conninfo, witness FROM %s.repl_nodes;", repmgr_schema);
	res = PQexec(conn, sqlquery);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Cannot get node information, have you registered them?\n%s\n"), PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}
	PQfinish(conn);

	printf("Role      | Connection String \n");
	for (i = 0; i < PQntuples(res); i++)
	{
		conn = establishDBConnection(PQgetvalue(res, i, 0), false);
		if (PQstatus(conn) != CONNECTION_OK)
			strcpy(node_role, "  FAILED");
		else if (strcmp(PQgetvalue(res, i, 1), "t") == 0)
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
	int         master_id;
	PGconn   *conn = NULL;
	PGconn   *master_conn = NULL;
	PGresult *res;
	char     sqlquery[QUERY_STR_LEN];

	/* We need to connect to check configuration */
	log_info(_("%s connecting to database\n"), progname);
	conn = establishDBConnection(options.conninfo, true);

	/* check if there is a master in this cluster */
	log_info(_("%s connecting to master database\n"), progname);
	master_conn = getMasterConnection(conn, repmgr_schema, options.cluster_name,
	                                  &master_id, NULL);
	if (!master_conn)
	{
		log_err(_("cluster cleanup: cannot connect to master\n"));
		PQfinish(conn);
		exit(ERR_DB_CON);
	}
	PQfinish(conn);

	if (runtime_options.keep_history > 0)
	{
		sqlquery_snprintf(sqlquery, "DELETE FROM %s.repl_monitor "
		                  " WHERE age(now(), last_monitor_time) >= '%d days'::interval;",
		                  repmgr_schema, runtime_options.keep_history);
	}
	else
	{
		sqlquery_snprintf(sqlquery, "TRUNCATE TABLE %s.repl_monitor;", repmgr_schema);
	}
	res = PQexec(master_conn, sqlquery);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err(_("cluster cleanup: Couldn't clean history\n%s\n"), PQerrorMessage(master_conn));
		PQclear(res);
		PQfinish(master_conn);
		exit(ERR_BAD_CONFIG);
	}
	PQclear(res);

	/* Let's VACUUM the table to avoid autovacuum to be launched in an unexpected hour */
	sqlquery_snprintf(sqlquery, "VACUUM %s.repl_monitor;", repmgr_schema);
	res = PQexec(master_conn, sqlquery);

	/* XXX There is any need to check this VACUUM happens without problems? */

	PQclear(res);
	PQfinish(master_conn);
}


static void
do_master_register(void)
{
	PGconn		*conn;
	PGresult	*res;
	char 		sqlquery[QUERY_STR_LEN];

	bool		schema_exists = false;
	char		schema_quoted[MAXLEN];
	char 		master_version[MAXVERSIONSTR];

	conn = establishDBConnection(options.conninfo, true);

	/* master should be v9 or better */
	log_info(_("%s connecting to master database\n"), progname);
	pg_version(conn, master_version);
	if (strcmp(master_version, "") == 0)
	{
		PQfinish(conn);
		log_err( _("%s needs master to be PostgreSQL 9.0 or better\n"), progname);
		return;
	}

	/* Check we are a master */
	log_info(_("%s connected to master, checking its state\n"), progname);
	if (is_standby(conn))
	{
		log_err(_("Trying to register a standby node as a master\n"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Assemble a quoted schema name
	 * XXX This is not currently used due to a merge conflict, but
	 * probably should be */
	if (false)
	{
		char *identifier = PQescapeIdentifier(conn, repmgr_schema,
		                                      strlen(repmgr_schema));

		maxlen_snprintf(schema_quoted, "%s", identifier);
		PQfreemem(identifier);
	}

	/* Check if there is a schema for this cluster */
	sqlquery_snprintf(sqlquery,
	                  "SELECT 1 FROM pg_namespace "
	                  "WHERE nspname = '%s'", repmgr_schema);
	log_debug(_("master register: %s\n"), sqlquery);
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Can't get info about schemas: %s\n"), PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	if (PQntuples(res) > 0)			/* schema exists */
	{
		if (!runtime_options.force)	/* and we are not forcing so error */
		{
			log_notice(_("Schema %s already exists.\n"), repmgr_schema);
			PQclear(res);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}
		schema_exists = true;
	}
	PQclear(res);

	if (!schema_exists)
	{
		log_info(_("master register: creating database objects inside the %s schema\n"), repmgr_schema);

		/* ok, create the schema */
		if (!create_schema(conn))
			return;
	}
	else
	{
		PGconn *master_conn;
		int		id;

		/* Ensure there isn't any other master already registered */
		master_conn = getMasterConnection(conn, repmgr_schema,
		                                  options.cluster_name, &id,NULL);
		if (master_conn != NULL)
		{
			PQfinish(master_conn);
			log_warning(_("There is a master already in cluster %s\n"), options.cluster_name);
			exit(ERR_BAD_CONFIG);
		}
	}

	/* Now register the master */
	if (runtime_options.force)
	{
		sqlquery_snprintf(sqlquery, "DELETE FROM %s.repl_nodes "
		                  " WHERE id = %d",
		                  repmgr_schema, options.node);
		log_debug(_("master register: %s\n"), sqlquery);

		if (!PQexec(conn, sqlquery))
		{
			log_warning(_("Cannot delete node details, %s\n"),
			            PQerrorMessage(conn));
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	sqlquery_snprintf(sqlquery, "INSERT INTO %s.repl_nodes (id, cluster, name, conninfo, priority) "
	                  "VALUES (%d, '%s', '%s', '%s', %d)",
	                  repmgr_schema, options.node, options.cluster_name, options.node_name,
	                  options.conninfo, options.priority);
	log_debug(_("master register: %s\n"), sqlquery);

	if (!PQexec(conn, sqlquery))
	{
		log_warning(_("Cannot insert node details, %s\n"),
		            PQerrorMessage(conn));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	PQfinish(conn);
	log_notice(_("Master node correctly registered for cluster %s with id %d (conninfo: %s)\n"),
	           options.cluster_name, options.node, options.conninfo);
	return;
}


static void
do_standby_register(void)
{
	PGconn		*conn;
	PGconn		*master_conn;
	int			master_id;

	PGresult	*res;
	char 		sqlquery[QUERY_STR_LEN];
	char		schema_quoted[MAXLEN];

	char master_version[MAXVERSIONSTR];
	char standby_version[MAXVERSIONSTR];

	/* XXX: A lot of copied code from do_master_register! Refactor */

	log_info(_("%s connecting to standby database\n"), progname);
	conn = establishDBConnection(options.conninfo, true);

	/* should be v9 or better */
	log_info(_("%s connected to standby, checking its state\n"), progname);
	pg_version(conn, standby_version);
	if (strcmp(standby_version, "") == 0)
	{
		PQfinish(conn);
		log_err(_("%s needs standby to be PostgreSQL 9.0 or better\n"), progname);
		exit(ERR_BAD_CONFIG);
	}

	/* Check we are a standby */
	if (!is_standby(conn))
	{
		log_err(_("repmgr: This node should be a standby (%s)\n"), options.conninfo);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Assemble a quoted schema name
	 * XXX This is not currently used due to a merge conflict, but
	 * probably should be */
	if (false)
	{
		char *identifier = PQescapeIdentifier(conn, repmgr_schema,
		                                      strlen(repmgr_schema));

		maxlen_snprintf(schema_quoted, "%s", identifier);
		PQfreemem(identifier);
	}

	/* Check if there is a schema for this cluster */
	sqlquery_snprintf(sqlquery, "SELECT 1 FROM pg_namespace WHERE nspname = '%s'", repmgr_schema);
	log_debug(_("standby register: %s\n"), sqlquery);
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Can't get info about tablespaces: %s\n"), PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	if (PQntuples(res) == 0)
	{
		/* schema doesn't exist */
		log_err(_("Schema %s doesn't exists.\n"), repmgr_schema);
		PQclear(res);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}
	PQclear(res);

	/* check if there is a master in this cluster */
	log_info(_("%s connecting to master database\n"), progname);
	master_conn = getMasterConnection(conn, repmgr_schema, options.cluster_name,
	                                  &master_id, NULL);
	if (!master_conn)
	{
		log_err(_("A master must be defined before configuring a slave\n"));
		exit(ERR_BAD_CONFIG);
	}

	/* master should be v9 or better */
	log_info(_("%s connected to master, checking its state\n"), progname);
	pg_version(master_conn, master_version);
	if (strcmp(master_version, "") == 0)
	{
		PQfinish(conn);
		PQfinish(master_conn);
		log_err(_("%s needs master to be PostgreSQL 9.0 or better\n"), progname);
		exit(ERR_BAD_CONFIG);
	}

	/* master and standby version should match */
	if (strcmp(master_version, standby_version) != 0)
	{
		PQfinish(conn);
		PQfinish(master_conn);
		log_err(_("%s needs versions of both master (%s) and standby (%s) to match.\n"),
		        progname, master_version, standby_version);
		exit(ERR_BAD_CONFIG);
	}

	/* Now register the standby */
	log_info(_("%s registering the standby\n"), progname);
	if (runtime_options.force)
	{
		sqlquery_snprintf(sqlquery, "DELETE FROM %s.repl_nodes "
		                  " WHERE id = %d",
		                  repmgr_schema, options.node);

		log_debug(_("standby register: %s\n"), sqlquery);

		if (!PQexec(master_conn, sqlquery))
		{
			log_err(_("Cannot delete node details, %s\n"),
			        PQerrorMessage(master_conn));
			PQfinish(master_conn);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	sqlquery_snprintf(sqlquery, "INSERT INTO %s.repl_nodes(id, cluster, name, conninfo, priority) "
	                  "VALUES (%d, '%s', '%s', '%s', %d)",
	                  repmgr_schema, options.node, options.cluster_name, options.node_name,
	                  options.conninfo, options.priority);
	log_debug(_("standby register: %s\n"), sqlquery);

	if (!PQexec(master_conn, sqlquery))
	{
		log_err(_("Cannot insert node details, %s\n"),
		        PQerrorMessage(master_conn));
		PQfinish(master_conn);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	log_info(_("%s registering the standby complete\n"), progname);
	PQfinish(master_conn);
	PQfinish(conn);
	log_notice(_("Standby node correctly registered for cluster %s with id %d (conninfo: %s)\n"),
	           options.cluster_name, options.node, options.conninfo);
	return;
}


static void
do_standby_clone(void)
{
	PGconn		*conn;
	PGresult	*res;
	char		sqlquery[QUERY_STR_LEN];

	int			r = 0;
	int			i;
	bool		flag_success = false;
	bool		test_mode = false;

	char		tblspc_dir[MAXFILENAME];

	char		master_data_directory[MAXFILENAME];
	char		local_data_directory[MAXFILENAME];
	char		master_xlog_directory[MAXFILENAME];
	char		local_xlog_directory[MAXFILENAME];
	char		master_stats_temp_directory[MAXFILENAME];
	char		local_stats_temp_directory[MAXFILENAME];

	char		master_control_file[MAXFILENAME];
	char		local_control_file[MAXFILENAME];
	char		master_config_file[MAXFILENAME];
	char		local_config_file[MAXFILENAME];
	char		master_hba_file[MAXFILENAME];
	char		local_hba_file[MAXFILENAME];
	char		master_ident_file[MAXFILENAME];
	char		local_ident_file[MAXFILENAME];

	char		*first_wal_segment = NULL;
	const char	*last_wal_segment  = NULL;

	char		master_version[MAXVERSIONSTR];

	/*
	 * if dest_dir has been provided, we copy everything in the same path
	 * if dest_dir is set and the master have tablespace, repmgr will stop
	 * because it is more complex to remap the path for the tablespaces and it
	 * does not look useful at the moment
	 */
	if (runtime_options.dest_dir[0])
	{
		test_mode = true;
		log_notice(_("%s Destination directory %s provided, try to clone everything in it.\n"), progname, runtime_options.dest_dir);
	}

	/* Connection parameters for master only */
	keywords[0] = "host";
	values[0] = runtime_options.host;
	keywords[1] = "port";
	values[1] = runtime_options.masterport;

	/* We need to connect to check configuration and start a backup */
	log_info(_("%s connecting to master database\n"), progname);
	conn = establishDBConnectionByParams(keywords, values, true);

	/* primary should be v9 or better */
	log_info(_("%s connected to master, checking its state\n"), progname);
	pg_version(conn, master_version);
	if (strcmp(master_version, "") == 0)
	{
		PQfinish(conn);
		log_err(_("%s needs master to be PostgreSQL 9.0 or better\n"), progname);
		exit(ERR_BAD_CONFIG);
	}

	/* Check we are cloning a primary node */
	if (is_standby(conn))
	{
		PQfinish(conn);
		log_err(_("\nThe command should clone a primary node\n"));
		exit(ERR_BAD_CONFIG);
	}

	/* And check if it is well configured */
	if (!guc_setted(conn, "wal_level", "=", "hot_standby"))
	{
		PQfinish(conn);
		log_err(_("%s needs parameter 'wal_level' to be set to 'hot_standby'\n"), progname);
		exit(ERR_BAD_CONFIG);
	}
	if (!guc_setted(conn, "wal_keep_segments", ">=", runtime_options.wal_keep_segments))
	{
		PQfinish(conn);
		log_err(_("%s needs parameter 'wal_keep_segments' to be set to %s or greater (see the '-w' option or edit the postgresql.conf of the PostgreSQL master.)\n"), progname, runtime_options.wal_keep_segments);
		exit(ERR_BAD_CONFIG);
	}
	if (!guc_setted(conn, "archive_mode", "=", "on"))
	{
		PQfinish(conn);
		log_err(_("%s needs parameter 'archive_mode' to be set to 'on'\n"), progname);
		exit(ERR_BAD_CONFIG);
	}
	if (!guc_setted(conn, "hot_standby", "=", "on"))
	{
		PQfinish(conn);
		log_err(_("%s needs parameter 'hot_standby' to be set to 'on'\n"), progname);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Check if the tablespace locations exists and that we can write to
	 * them.
	 */
	if (strcmp(master_version, "9.0") == 0 || strcmp(master_version, "9.1") == 0)
		sqlquery_snprintf(sqlquery,
		                  "SELECT spclocation "
		                  "  FROM pg_tablespace "
		                  "WHERE spcname NOT IN ('pg_default', 'pg_global')");
	else
		sqlquery_snprintf(sqlquery,
		                  "SELECT pg_tablespace_location(oid) spclocation "
		                  "  FROM pg_tablespace "
		                  "WHERE spcname NOT IN ('pg_default', 'pg_global')");

	log_debug("standby clone: %s\n", sqlquery);

	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Can't get info about tablespaces: %s\n"), PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}
	for (i = 0; i < PQntuples(res); i++)
	{
		if (test_mode)
		{
			log_err("Can't clone in test mode when master have tablespace\n");
			PQclear(res);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		strncpy(tblspc_dir, PQgetvalue(res, i, 0), MAXFILENAME);
		/*
		 * Check this directory could be used for tablespace
		 * this will create the directory a bit too early
		 * XXX build an array of tablespace to create later in the backup
		 */
		if (!create_pgdir(tblspc_dir, runtime_options.force))
		{
			PQclear(res);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}
	}
	PQclear(res);

	/* Get the data directory full path and the configuration files location */
	sqlquery_snprintf(sqlquery,
	                  "SELECT name, setting "
	                  "  FROM pg_settings "
	                  " WHERE name IN ('data_directory', 'config_file', 'hba_file', 'ident_file', 'stats_temp_directory')");
	log_debug(_("standby clone: %s\n"), sqlquery);
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Can't get info about data directory and configuration files: %s\n"), PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* We need all 5 parameters, and they can be retrieved only by superusers */
	if (PQntuples(res) != 5)
	{
		log_err("%s: STANDBY CLONE should be run by a SUPERUSER\n", progname);
		PQclear(res);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		if (strcmp(PQgetvalue(res, i, 0), "data_directory") == 0)
			strncpy(master_data_directory, PQgetvalue(res, i, 1), MAXFILENAME);
		else if (strcmp(PQgetvalue(res, i, 0), "config_file") == 0)
			strncpy(master_config_file, PQgetvalue(res, i, 1), MAXFILENAME);
		else if (strcmp(PQgetvalue(res, i, 0), "hba_file") == 0)
			strncpy(master_hba_file, PQgetvalue(res, i, 1), MAXFILENAME);
		else if (strcmp(PQgetvalue(res, i, 0), "ident_file") == 0)
			strncpy(master_ident_file, PQgetvalue(res, i, 1), MAXFILENAME);
		else if (strcmp(PQgetvalue(res, i, 0), "stats_temp_directory") == 0)
			strncpy(master_stats_temp_directory, PQgetvalue(res, i, 1), MAXFILENAME);
		else
			log_warning(_("unknown parameter: %s\n"), PQgetvalue(res, i, 0));
	}
	PQclear(res);

	log_info(_("Successfully connected to primary. Current installation size is %s\n"), get_cluster_size(conn));

	/*
	 * XXX  master_xlog_directory should be discovered from master configuration
	 * but it is not possible via SQL. We need to use a command via ssh
	 */
	maxlen_snprintf(master_xlog_directory, "%s/pg_xlog", master_data_directory);
	if (test_mode)
	{
		strncpy(local_data_directory, runtime_options.dest_dir, MAXFILENAME);
		strncpy(local_config_file, runtime_options.dest_dir, MAXFILENAME);
		strncpy(local_hba_file, runtime_options.dest_dir, MAXFILENAME);
		strncpy(local_ident_file, runtime_options.dest_dir, MAXFILENAME);
		maxlen_snprintf(local_stats_temp_directory, "%s/pg_stat_tmp", runtime_options.dest_dir);
		maxlen_snprintf(local_xlog_directory, "%s/pg_xlog", runtime_options.dest_dir);
	}
	else
	{
		strncpy(local_data_directory, master_data_directory, MAXFILENAME);
		strncpy(local_config_file, master_config_file, MAXFILENAME);
		strncpy(local_hba_file, master_hba_file, MAXFILENAME);
		strncpy(local_ident_file, master_ident_file, MAXFILENAME);
		strncpy(local_stats_temp_directory, master_stats_temp_directory, MAXFILENAME);
		strncpy(local_xlog_directory, master_xlog_directory, MAXFILENAME);
	}

	r = test_ssh_connection(runtime_options.host, runtime_options.remote_user);
	if (r != 0)
	{
		log_err(_("%s: Aborting, remote host %s is not reachable.\n"), progname, runtime_options.host);
		PQfinish(conn);
		exit(ERR_BAD_SSH);
	}

	log_notice(_("Starting backup...\n"));

	/*
	 * in pg 9.1 default is to wait for a sync standby to ack,
	 * avoid that by turning off sync rep for this session
	 */
	sqlquery_snprintf(sqlquery, "SET synchronous_commit TO OFF");
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_err("Can't set synchronous_commit: %s\n", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * inform the master we will start a backup and get the first XLog filename
	 * so we can say to the user we need those files
	 */
	sqlquery_snprintf(
	    sqlquery,
	    "SELECT pg_xlogfile_name(pg_start_backup('repmgr_standby_clone_%ld'))",
	    time(NULL));
	log_debug(_("standby clone: %s\n"), sqlquery);
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Can't start backup: %s\n"), PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	if (runtime_options.verbose)
	{
		char	*first_wal_seg_pq = PQgetvalue(res, 0, 0);
		size_t	 buf_sz			  = strlen(first_wal_seg_pq);

		first_wal_segment = malloc(buf_sz + 1);
		xsnprintf(first_wal_segment, buf_sz + 1, "%s", first_wal_seg_pq);
	}

	PQclear(res);

	/* Check the directory could be used as a PGDATA dir */
	if (!create_pgdir(local_data_directory, runtime_options.force))
	{
		log_err(_("%s: couldn't use directory %s ...\nUse --force option to force\n"),
		        progname, local_data_directory);
		goto stop_backup;
	}

	/*
	 * 1) first move global/pg_control
	 *
	 * 2) then move data_directory ommiting the files we have already moved and
	 *    pg_xlog content
	 *
	 * 3) finally We need to backup configuration files (that could be on other
	 *    directories, debian like systems likes to do that), so look at
	 *    config_file, hba_file and ident_file but we can omit
	 *    external_pid_file ;)
	 *
	 * On error we need to return but before that execute pg_stop_backup()
	 */

	/* need to create the global sub directory */
	maxlen_snprintf(master_control_file, "%s/global/pg_control", master_data_directory);
	maxlen_snprintf(local_control_file, "%s/global", local_data_directory);
	log_info(_("standby clone: master control file '%s'\n"), master_control_file);
	if (!create_directory(local_control_file))
	{
		log_err(_("%s: couldn't create directory %s ...\n"),
		        progname, local_control_file);
		goto stop_backup;
	}

	log_info(_("standby clone: master control file '%s'\n"), master_control_file);
	r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
	                      master_control_file, local_control_file,
	                      false);
	if (r != 0)
	{
		log_warning(_("standby clone: failed copying master control file '%s'\n"), master_control_file);
		goto stop_backup;
	}

	log_info(_("standby clone: master data directory '%s'\n"), master_data_directory);
	r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
	                      master_data_directory, local_data_directory,
	                      true);
	if (r != 0)
	{
		log_warning(_("standby clone: failed copying master data directory '%s'\n"), master_data_directory);
		goto stop_backup;
	}

	/*
	 * Copy tablespace locations, i'm doing this separately because i couldn't
	 * find and appropiate rsync option but besides we could someday make all
	 * these rsync happen concurrently
	 * XXX We may not do that if we are in test_mode but it does not hurt too much
	 * (except if a tablespace is created during the test)
	 */
	if (strcmp(master_version, "9.0") == 0 || strcmp(master_version, "9.1") == 0)
		sqlquery_snprintf(sqlquery,
		                  "SELECT spclocation "
		                  "  FROM pg_tablespace "
		                  "  WHERE spcname NOT IN ('pg_default', 'pg_global')");
	else
		sqlquery_snprintf(sqlquery,
		                  "SELECT pg_tablespace_location(oid) spclocation "
		                  "  FROM pg_tablespace "
		                  "  WHERE spcname NOT IN ('pg_default', 'pg_global')");

	log_debug("standby clone: %s\n", sqlquery);

	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Can't get info about tablespaces: %s\n"), PQerrorMessage(conn));
		PQclear(res);
		goto stop_backup;
	}
	for (i = 0; i < PQntuples(res); i++)
	{
		strncpy(tblspc_dir, PQgetvalue(res, i, 0), MAXFILENAME);
		log_info(_("standby clone: master tablespace '%s'\n"), tblspc_dir);
		r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
		                      tblspc_dir, tblspc_dir,
		                      true);
		if (r != 0)
		{
			log_warning(_("standby clone: failed copying tablespace directory '%s'\n"), tblspc_dir);
			PQclear(res);
			goto stop_backup;
		}
	}
	PQclear(res);

	log_info(_("standby clone: master config file '%s'\n"), master_config_file);
	r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
	                      master_config_file, local_config_file,
	                      false);
	if (r != 0)
	{
		log_warning(_("standby clone: failed copying master config file '%s'\n"), master_config_file);
		goto stop_backup;
	}

	log_info(_("standby clone: master hba file '%s'\n"), master_hba_file);
	r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
	                      master_hba_file, local_hba_file,
	                      false);
	if (r != 0)
	{
		log_warning(_("standby clone: failed copying master hba file '%s'\n"), master_hba_file);
		goto stop_backup;
	}

	log_info(_("standby clone: master ident file '%s'\n"), master_ident_file);
	r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
	                      master_ident_file, local_ident_file,
	                      false);
	if (r != 0)
	{
		log_warning(_("standby clone: failed copying master ident file '%s'\n"), master_ident_file);
		goto stop_backup;
	}

	/* we success so far, flag that to allow a better HINT */
	flag_success = true;

stop_backup:

	/*
	 * Inform the master that we have finished the backup.
	 */
	log_notice(_("Finishing backup...\n"));
	sqlquery_snprintf(sqlquery, "SELECT pg_xlogfile_name(pg_stop_backup())");
	log_debug(_("standby clone: %s\n"), sqlquery);

	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Can't stop backup: %s\n"), PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		exit(ERR_STOP_BACKUP);
	}
	last_wal_segment = PQgetvalue(res, 0, 0);

	if (runtime_options.verbose)
		log_info(_("%s requires primary to keep WAL files %s until at least %s\n"),
		         progname, first_wal_segment, last_wal_segment);

	/* Finished with the database connection now */
	PQclear(res);
	PQfinish(conn);

	/*
	 * Only free the first_wal_segment since it was copied out of the
	 * pqresult.
	 */
	free(first_wal_segment);
	first_wal_segment = NULL;

	/* If the rsync failed then exit */
	if (r != 0)
	{
		log_err(_("Couldn't rsync the master...\nYou have to cleanup the destination directory (%s) manually!\n"),
		        local_data_directory);
		exit(ERR_BAD_RSYNC);
	}

	/*
	 * We need to create the pg_xlog sub directory too.
	 */
	if (!create_directory(local_xlog_directory))
	{
		log_err(_("%s: couldn't create directory %s, you will need to do it manually...\n"),
		        progname, local_xlog_directory);
		r = ERR_NEEDS_XLOG; /* continue, but eventually exit returning error */
	}

	/* Finally, write the recovery.conf file */
	create_recovery_file(local_data_directory);

	/*
	 * We don't start the service yet because we still may want to
	 * move the directory
	 */
	log_notice(_("%s standby clone complete\n"), progname);

	/*  HINT message : what to do next ? */
	if (flag_success)
	{
		log_notice("HINT: You can now start your postgresql server\n");
		if (test_mode)
		{
			log_notice(_("for example : pg_ctl -D %s start\n"), local_data_directory);
		}
		else
		{
			log_notice("for example : /etc/init.d/postgresql start\n");
		}
	}
	exit(r);
}


static void
do_standby_promote(void)
{
	PGconn		*conn;
	PGresult	*res;
	char 		sqlquery[QUERY_STR_LEN];
	char		script[MAXLEN];

	PGconn		*old_master_conn;
	int			old_master_id;

	int			r;
	char		data_dir[MAXLEN];
	char		recovery_file_path[MAXFILENAME];
	char		recovery_done_path[MAXFILENAME];

	char	standby_version[MAXVERSIONSTR];

	/* We need to connect to check configuration */
	log_info(_("%s connecting to master database\n"), progname);
	conn = establishDBConnection(options.conninfo, true);

	/* we need v9 or better */
	log_info(_("%s connected to master, checking its state\n"), progname);
	pg_version(conn, standby_version);
	if (strcmp(standby_version, "") == 0)
	{
		log_err(_("%s needs standby to be PostgreSQL 9.0 or better\n"), progname);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Check we are in a standby node */
	if (!is_standby(conn))
	{
		log_err(_("%s: The command should be executed on a standby node\n"), progname);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* we also need to check if there isn't any master already */
	old_master_conn = getMasterConnection(conn, repmgr_schema, options.cluster_name,
	                                      &old_master_id, NULL);
	if (old_master_conn != NULL)
	{
		log_err(_("There is a master already in this cluster\n"));
		PQfinish(old_master_conn);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	log_notice(_("%s: Promoting standby\n"), progname);

	/* Get the data directory full path and the last subdirectory */
	sqlquery_snprintf(sqlquery, "SELECT setting "
	                  " FROM pg_settings WHERE name = 'data_directory'");
	log_debug(_("standby promote: %s\n"), sqlquery);
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Can't get info about data directory: %s\n"), PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}
	strcpy(data_dir, PQgetvalue(res, 0, 0));
	PQclear(res);
	PQfinish(conn);

	log_info(_("%s: Marking recovery done\n"), progname);
	maxlen_snprintf(recovery_file_path, "%s/%s", data_dir, RECOVERY_FILE);
	maxlen_snprintf(recovery_done_path, "%s/%s", data_dir, RECOVERY_DONE_FILE);
	rename(recovery_file_path, recovery_done_path);

	/*
	 * We assume the pg_ctl script is in the PATH.  Restart and wait for
	 * the server to finish starting, so that the check below will
	 * find an active server rather than one starting up.  This may
	 * hang for up the default timeout (60 seconds).
	 */
	log_notice(_("%s: restarting server using pg_ctl\n"), progname);
	maxlen_snprintf(script, "pg_ctl -D %s -w -m fast restart", data_dir);
	r = system(script);
	if (r != 0)
	{
		log_err(_("Can't restart PostgreSQL server\n"));
		exit(ERR_NO_RESTART);
	}

	/* reconnect to check we got promoted */
	log_info(_("%s connecting to now restarted database\n"), progname);
	conn = establishDBConnection(options.conninfo, true);
	if (is_standby(conn))
	{
		log_err(_("\n%s: STANDBY PROMOTE failed, this is still a standby node.\n"), progname);
	}
	else
	{
		log_err(_("\n%s: STANDBY PROMOTE successful.  You should REINDEX any hash indexes you have.\n"), progname);
	}
	PQfinish(conn);
	return;
}


static void
do_standby_follow(void)
{
	PGconn		*conn;
	PGresult	*res;
	char 		sqlquery[QUERY_STR_LEN];
	char		script[MAXLEN];
	char		master_conninfo[MAXLEN];
	PGconn		*master_conn;
	int			master_id;

	int			r;
	char		data_dir[MAXLEN];

	char	master_version[MAXVERSIONSTR];
	char	standby_version[MAXVERSIONSTR];

	/* We need to connect to check configuration */
	log_info(_("%s connecting to standby database\n"), progname);
	conn = establishDBConnection(options.conninfo, true);

	/* Check we are in a standby node */
	log_info(_("%s connected to standby, checking its state\n"), progname);
	if (!is_standby(conn))
	{
		log_err(_("\n%s: The command should be executed in a standby node\n"), progname);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* should be v9 or better */
	pg_version(conn, standby_version);
	if (strcmp(standby_version, "") == 0)
	{
		log_err(_("\n%s needs standby to be PostgreSQL 9.0 or better\n"), progname);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * we also need to check if there is any master in the cluster
	 * or wait for one to appear if we have set the wait option
	 */
	log_info(_("%s discovering new master...\n"), progname);

	do
	{
		master_conn = getMasterConnection(conn, repmgr_schema,
		                                  options.cluster_name, &master_id,(char *) &master_conninfo);
	}
	while (master_conn == NULL && runtime_options.wait_for_master);

	if (master_conn == NULL)
	{
		log_err(_("There isn't a master to follow in this cluster\n"));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Check we are going to point to a master */
	if (is_standby(master_conn))
	{
		log_err(_("%s: The node to follow should be a master\n"), progname);
		PQfinish(conn);
		PQfinish(master_conn);
		exit(ERR_BAD_CONFIG);
	}

	/* should be v9 or better */
	log_info(_("%s connected to master, checking its state\n"), progname);
	pg_version(master_conn, master_version);
	if (strcmp(master_version, "") == 0)
	{
		log_err(_("%s needs master to be PostgreSQL 9.0 or better\n"), progname);
		PQfinish(conn);
		PQfinish(master_conn);
		exit(ERR_BAD_CONFIG);
	}

	/* master and standby version should match */
	if (strcmp(master_version, standby_version) != 0)
	{
		log_err(_("%s needs versions of both master (%s) and standby (%s) to match.\n"),
		        progname, master_version, standby_version);
		PQfinish(conn);
		PQfinish(master_conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * set the host and masterport variables with the master ones
	 * before closing the connection because we will need them to
	 * recreate the recovery.conf file
	 */
	strncpy(runtime_options.host, PQhost(master_conn), MAXLEN);
	strncpy(runtime_options.masterport, PQport(master_conn), MAXLEN);
	PQfinish(master_conn);

	log_info(_("%s Changing standby's master"),progname);

	/* Get the data directory full path */
	sqlquery_snprintf(sqlquery, "SELECT setting "
	                  " FROM pg_settings WHERE name = 'data_directory'");
	log_debug(_("standby follow: %s\n"), sqlquery);
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Can't get info about data directory: %s\n"), PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}
	strcpy(data_dir, PQgetvalue(res, 0, 0));
	PQclear(res);
	PQfinish(conn);

	/* write the recovery.conf file */
	if (!create_recovery_file(data_dir))
		exit(ERR_BAD_CONFIG);

	/* Finally, restart the service */
	/* We assume the pg_ctl script is in the PATH */
	maxlen_snprintf(script, "pg_ctl -w -D %s -m fast restart", data_dir);
	r = system(script);
	if (r != 0)
	{
		log_err(_("Can't restart service\n"));
		exit(ERR_NO_RESTART);
	}

	return;
}


static void
do_witness_create(void)
{
	PGconn 		*masterconn;
	PGconn 		*witnessconn;
	PGresult	*res;
	char 		sqlquery[QUERY_STR_LEN];

	char		script[MAXLEN];
	char		buf[MAXLEN];
	FILE		*pg_conf = NULL;

	int			r = 0;
	int			i;

	char		master_version[MAXVERSIONSTR];

	char		master_hba_file[MAXLEN];

	/* Check this directory could be used as a PGDATA dir */
	if (!create_pgdir(runtime_options.dest_dir, runtime_options.force))
	{
		log_err(_("witness create: couldn't create data directory (\"%s\") for witness"),
		        runtime_options.dest_dir);
		exit(ERR_BAD_CONFIG);
	}

	/* Connection parameters for master only */
	keywords[0] = "host";
	values[0] = runtime_options.host;
	keywords[1] = "port";
	values[1] = runtime_options.masterport;

	/* We need to connect to check configuration and copy it */
	masterconn = establishDBConnectionByParams(keywords, values, true);
	if (!masterconn)
	{
		log_err(_("%s: could not connect to master\n"), progname);
		exit(ERR_DB_CON);
	}

	/* primary should be v9 or better */
	pg_version(masterconn, master_version);
	if (strcmp(master_version, "") == 0)
	{
		log_err(_("%s needs master to be PostgreSQL 9.0 or better\n"), progname);
		PQfinish(masterconn);
		exit(ERR_BAD_CONFIG);
	}

	/* Check we are connecting to a primary node */
	if (is_standby(masterconn))
	{
		log_err(_("The command should not run on a standby node\n"));
		PQfinish(masterconn);
		exit(ERR_BAD_CONFIG);
	}

	log_info(_("Successfully connected to primary.\n"));

	r = test_ssh_connection(runtime_options.host, runtime_options.remote_user);
	if (r != 0)
	{
		log_err(_("%s: Aborting, remote host %s is not reachable.\n"), progname, runtime_options.host);
		PQfinish(masterconn);
		exit(ERR_BAD_SSH);
	}

	/*
	 * To create a witness server we need to:
	 * 1) initialize the cluster
	 * 2) register the witness in repl_nodes
	 * 3) copy configuration from master
	 */

	/* Create the cluster for witness */
	/* We assume the pg_ctl script is in the PATH */
	sprintf(script, "pg_ctl -D %s init -o \"-W\"", runtime_options.dest_dir);
	log_info("Initialize cluster for witness: %s.\n", script);

	r = system(script);
	if (r != 0)
	{
		log_err("Can't iniatialize cluster for witness server\n");
		PQfinish(masterconn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * default port for the witness is 5499,
	 * but user can provide a different one
	 */
	snprintf(buf, sizeof(buf), "%s/postgresql.conf", runtime_options.dest_dir);
	pg_conf = fopen(buf, "a");
	if (pg_conf == NULL)
	{
		log_err(_("\n%s: could not open \"%s\" for adding extra config: %s\n"), progname, buf, strerror(errno));
		PQfinish(masterconn);
		exit(ERR_BAD_CONFIG);
	}

	snprintf(buf, sizeof(buf), "\n#Configuration added by %s\n", progname);
	fputs(buf, pg_conf);

	if (!runtime_options.localport[0])
		strncpy(runtime_options.localport, "5499", MAXLEN);
	snprintf(buf, sizeof(buf), "port = %s\n", runtime_options.localport);
	fputs(buf, pg_conf);

	snprintf(buf, sizeof(buf), "shared_preload_libraries = 'repmgr_funcs'\n") ;
	fputs(buf, pg_conf);

	snprintf(buf, sizeof(buf), "listen_addresses = '*'\n") ;
	fputs(buf, pg_conf);

	fclose(pg_conf);

	/* Get the pg_hba.conf full path */
	sqlquery_snprintf(sqlquery, "SELECT name, setting "
	                  "  FROM pg_settings "
	                  " WHERE name IN ('hba_file')");
	log_debug(_("witness create: %s"), sqlquery);
	res = PQexec(masterconn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_err(_("Can't get info about pg_hba.conf: %s\n"), PQerrorMessage(masterconn));
		PQclear(res);
		PQfinish(masterconn);
		exit(ERR_DB_QUERY);
	}
	for (i = 0; i < PQntuples(res); i++)
	{
		if (strcmp(PQgetvalue(res, i, 0), "hba_file") == 0)
			strcpy(master_hba_file, PQgetvalue(res, i, 1));
		else
			log_err(_("uknown parameter: %s"), PQgetvalue(res, i, 0));
	}
	PQclear(res);

	r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
	                      master_hba_file, runtime_options.dest_dir,
	                      false);
	if (r != 0)
	{
		log_err(_("Can't rsync the pg_hba.conf file from master\n"));
		PQfinish(masterconn);
		exit(ERR_BAD_CONFIG);
	}

	/* start new instance */
	sprintf(script, "pg_ctl -w -D %s start", runtime_options.dest_dir);
	log_info(_("Start cluster for witness: %s"), script);
	r = system(script);
	if (r != 0)
	{
		log_err(_("Can't start cluster for witness server\n"));
		PQfinish(masterconn);
		exit(ERR_BAD_CONFIG);
	}

	/* register ourselves in the master */
	sqlquery_snprintf(sqlquery, "INSERT INTO %s.repl_nodes(id, cluster, name, conninfo, priority, witness) "
	                  "VALUES (%d, '%s', '%s', '%s', %d, true)",
	                  repmgr_schema, options.node, options.cluster_name, options.node_name, options.conninfo);

	log_debug(_("witness create: %s"), sqlquery);
	if (!PQexec(masterconn, sqlquery))
	{
		log_err(_("Cannot insert node details, %s\n"), PQerrorMessage(masterconn));
		PQfinish(masterconn);
		exit(ERR_DB_QUERY);
	}

	/* establish a connection to the witness, and create the schema */
	witnessconn = establishDBConnection(options.conninfo, true);

	log_info(_("Starting copy of configuration from master...\n"));

	if (!create_schema(witnessconn))
	{
		PQfinish(masterconn);
		PQfinish(witnessconn);
		exit(ERR_BAD_CONFIG);
	}

	/* copy configuration from master, only repl_nodes is needed */
	if (!copy_configuration(masterconn, witnessconn))
	{
		PQfinish(masterconn);
		PQfinish(witnessconn);
		exit(ERR_BAD_CONFIG);
	}
	PQfinish(masterconn);
	PQfinish(witnessconn);

	log_notice(_("Configuration has been successfully copied to the witness\n"));
}



static void
usage(void)
{
	log_err(_("\n\n%s: Replicator manager \n"), progname);
	log_err(_("Try \"%s --help\" for more information.\n"), progname);
}



static void
help(const char *progname)
{
	printf(_("\n%s: Replicator manager \n"), progname);
	printf(_("Usage:\n"));
	printf(_(" %s [OPTIONS] master	{register}\n"), progname);
	printf(_(" %s [OPTIONS] standby {register|clone|promote|follow}\n"),
	       progname);
	printf(_(" %s [OPTIONS] cluster {show|cleanup}\n"), progname);
	printf(_("\nGeneral options:\n"));
	printf(_("	--help					   show this help, then exit\n"));
	printf(_("	--version				   output version information, then exit\n"));
	printf(_("	--verbose				   output verbose activity information\n"));
	printf(_("\nConnection options:\n"));
	printf(_("	-d, --dbname=DBNAME		   database to connect to\n"));
	printf(_("	-h, --host=HOSTNAME		   database server host or socket directory\n"));
	printf(_("	-p, --port=PORT			   database server port\n"));
	printf(_("	-U, --username=USERNAME	   database user name to connect as\n"));
	printf(_("\nConfiguration options:\n"));
	printf(_("	-D, --data-dir=DIR		   local directory where the files will be copied to\n"));
	printf(_("	-l, --local-port=PORT      standby or witness server local port\n"));
	printf(_("	-f, --config_file=PATH	   path to the configuration file\n"));
	printf(_("	-R, --remote-user=USERNAME database server username for rsync\n"));
	printf(_("	-w, --wal-keep-segments=VALUE  minimum value for the GUC wal_keep_segments (default: 5000)\n"));
	printf(_("	-I, --ignore-rsync-warning ignore rsync partial transfer warning\n"));
	printf(_("  -k, --keep-history=VALUE   keeps indicated number of days of history\n"));
	printf(_("	-F, --force				   force potentially dangerous operations to happen\n"));
	printf(_("	-W, --wait				   wait for a master to appear"));

	printf(_("\n%s performs some tasks like clone a node, promote it "), progname);
	printf(_("or making follow another node and then exits.\n"));
	printf(_("COMMANDS:\n"));
	printf(_(" master register		 - registers the master in a cluster\n"));
	printf(_(" standby register		 - registers a standby in a cluster\n"));
	printf(_(" standby clone [node]	 - allows creation of a new standby\n"));
	printf(_(" standby promote		 - allows manual promotion of a specific standby into a "));
	printf(_("new master in the event of a failover\n"));
	printf(_(" standby follow		 - allows the standby to re-point itself to a new master\n"));
	printf(_(" cluster show            - print node information\n"));
	printf(_(" cluster cleanup         - cleans monitor's history\n"));
}


/*
 * Creates a recovery file for a standby.
 */
static bool
create_recovery_file(const char *data_dir)
{
	FILE		*recovery_file;
	char		recovery_file_path[MAXLEN];
	char		line[MAXLEN];

	maxlen_snprintf(recovery_file_path, "%s/%s", data_dir, RECOVERY_FILE);

	recovery_file = fopen(recovery_file_path, "w");
	if (recovery_file == NULL)
	{
		log_err(_("could not create recovery.conf file, it could be necessary to create it manually\n"));
		return false;
	}

	maxlen_snprintf(line, "standby_mode = 'on'\n");
	if (fputs(line, recovery_file) == EOF)
	{
		log_err(_("recovery file could not be written, it could be necessary to create it manually\n"));
		fclose(recovery_file);
		return false;
	}

	write_primary_conninfo(line);

	if (fputs(line, recovery_file) == EOF)
	{
		log_err(_("recovery file could not be written, it could be necessary to create it manually\n"));
		fclose(recovery_file);
		return false;
	}

	/*FreeFile(recovery_file);*/
	fclose(recovery_file);

	return true;
}

static int
test_ssh_connection(char *host, char *remote_user)
{
	char script[MAXLEN];
	int	 r;

	/* On some OS, true is located in a different place than in Linux */
#ifdef __FreeBSD__
#define TRUEBIN_PATH "/usr/bin/true"
#else
#define TRUEBIN_PATH "/bin/true"
#endif

	/* Check if we have ssh connectivity to host before trying to rsync */
	if (!remote_user[0])
		maxlen_snprintf(script, "ssh -o Batchmode=yes %s %s", host, TRUEBIN_PATH);
	else
		maxlen_snprintf(script, "ssh -o Batchmode=yes %s -l %s %s", host, remote_user, TRUEBIN_PATH);

	log_debug(_("command is: %s"), script);
	r = system(script);
	if (r != 0)
		log_info(_("Can not connect to the remote host (%s)\n"), host);
	return r;
}

static int
copy_remote_files(char *host, char *remote_user, char *remote_path,
                  char *local_path, bool is_directory)
{
	char script[MAXLEN];
	char rsync_flags[MAXLEN];
	char host_string[MAXLEN];
	int	 r;

	if (strnlen(options.rsync_options, MAXLEN) == 0)
		maxlen_snprintf(
		    rsync_flags, "%s",
		    "--archive --checksum --compress --progress --rsh=ssh");
	else
		maxlen_snprintf(rsync_flags, "%s", options.rsync_options);

	if (runtime_options.force)
		strcat(rsync_flags, " --delete");

	if (!remote_user[0])
	{
		maxlen_snprintf(host_string, "%s", host);
	}
	else
	{
		maxlen_snprintf(host_string,"%s@%s",remote_user,host);
	}

	if (is_directory)
	{
		strcat(rsync_flags, " --exclude=pg_xlog* --exclude=pg_control --exclude=*.pid");
		maxlen_snprintf(script, "rsync %s %s:%s/* %s",
		                rsync_flags, host_string, remote_path, local_path);
	}
	else
	{
		maxlen_snprintf(script, "rsync %s %s:%s %s",
		                rsync_flags, host_string, remote_path, local_path);
	}

	log_info(_("rsync command line:  '%s'\n"), script);

	r = system(script);

	/*
	 * If we are transfering a directory (data directory, tablespace directories)
	 * then we can ignore some rsync warnings.  If we get some of those errors, we
	 * treat them as 0 only if passed the --ignore-rsync-warning command-line option.
	 *
	 * List of ignorable rsync errors:
	 *   24     Partial transfer due to vanished source files
	 */
	if ((WEXITSTATUS(r) == 24) && is_directory)
	{
		if (runtime_options.ignore_rsync_warn)
		{
			r = 0;
			log_info(_("rsync partial transfer warning ignored\n"));
		}
		else
			log_warning( _("\nrsync completed with return code 24: "
			               "\"Partial transfer due to vanished source files\".\n"
			               "This can happen because of normal operation "
			               "on the master server, but it may indicate an "
			               "unexpected change during cloning.  If you are certain "
			               "no changes were made to the master, try cloning "
			               "again using \"repmgr --force --ignore-rsync-warning\"."));
	}
	if (r != 0)
		log_err(_("Can't rsync from remote file or directory (%s:%s)\n"),
		        host_string, remote_path);

	return r;
}


/*
 * Tries to avoid useless or conflicting parameters
 */
static bool
check_parameters_for_action(const int action)
{
	bool ok = true;

	switch (action)
	{
	case MASTER_REGISTER:
		/*
		 * To register a master we only need the repmgr.conf
		 * all other parameters are at least useless and could be
		 * confusing so reject them
		 */
		if (runtime_options.host[0] || runtime_options.masterport[0] || runtime_options.username[0] ||
		        runtime_options.dbname[0])
		{
			log_err(_("You can't use connection parameters to the master when issuing a MASTER REGISTER command.\n"));
			usage();
			ok = false;
		}
		if (runtime_options.dest_dir[0])
		{
			log_err(_("You don't need a destination directory for MASTER REGISTER command\n"));
			usage();
			ok = false;
		}
		break;
	case STANDBY_REGISTER:
		/*
		 * To register a standby we only need the repmgr.conf
		 * we don't need connection parameters to the master
		 * because we can detect the master in repl_nodes
		 */
		if (runtime_options.host[0] || runtime_options.masterport[0] || runtime_options.username[0] ||
		        runtime_options.dbname[0])
		{
			log_err(_("You can't use connection parameters to the master when issuing a STANDBY REGISTER command.\n"));
			usage();
			ok = false;
		}
		if (runtime_options.dest_dir[0])
		{
			log_err(_("You don't need a destination directory for STANDBY REGISTER command\n"));
			usage();
			ok = false;
		}
		break;
	case STANDBY_PROMOTE:
		/*
		 * To promote a standby we only need the repmgr.conf
		 * we don't want connection parameters to the master
		 * because we will try to detect the master in repl_nodes
		 * if we can't find it then the promote action will be cancelled
		 */
		if (runtime_options.host[0] || runtime_options.masterport[0] || runtime_options.username[0] ||
		        runtime_options.dbname[0])
		{
			log_err(_("You can't use connection parameters to the master when issuing a STANDBY PROMOTE command.\n"));
			usage();
			ok = false;
		}
		if (runtime_options.dest_dir[0])
		{
			log_err(_("You don't need a destination directory for STANDBY PROMOTE command\n"));
			usage();
			ok = false;
		}
		break;
	case STANDBY_FOLLOW:
		/*
		 * To make a standby follow a master we only need the repmgr.conf
		 * we don't want connection parameters to the new master
		 * because we will try to detect the master in repl_nodes
		 * if we can't find it then the follow action will be cancelled
		 */
		if (runtime_options.host[0] || runtime_options.masterport[0] || runtime_options.username[0] ||
		        runtime_options.dbname[0])
		{
			log_err(_("You can't use connection parameters to the master when issuing a STANDBY FOLLOW command.\n"));
			usage();
			ok = false;
		}
		if (runtime_options.dest_dir[0])
		{
			log_err(_("You don't need a destination directory for STANDBY FOLLOW command\n"));
			usage();
			ok = false;
		}
		break;
	case STANDBY_CLONE:
		/*
		 * Issue a friendly notice that the configuration file is not
		 * necessary nor read at all in when performing a STANDBY CLONE
		 * action.
		 */
		if (runtime_options.config_file[0])
		{
			log_notice(_("Only command line parameters for the connection "
			             "to the master are used when issuing a STANDBY CLONE command. "
			             "The passed configuration file is neither required nor used for "
			             "its node configuration portions\n\n"));
		}
		/*
		 * To clone a master into a standby we need connection parameters
		 * repmgr.conf is useless because we don't have a server running in
		 * the standby; warn the user, but keep going.
		 */
		if (runtime_options.host == NULL)
		{
			log_notice(_("You need to use connection parameters to "
			             "the master when issuing a STANDBY CLONE command."));
			ok = false;
		}
		need_a_node = false;
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

	return ok;
}



static bool
create_schema(PGconn *conn)
{
	char 		sqlquery[QUERY_STR_LEN];

	sqlquery_snprintf(sqlquery, "CREATE SCHEMA %s", repmgr_schema);
	log_debug(_("master register: %s\n"), sqlquery);
	if (!PQexec(conn, sqlquery))
	{
		log_err(_("Cannot create the schema %s: %s\n"),
		        repmgr_schema, PQerrorMessage(conn));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* ... the tables */
	sqlquery_snprintf(sqlquery, "CREATE TABLE %s.repl_nodes (        "
	                  "  id        integer primary key, "
	                  "  cluster   text    not null,    "
	                  "  name      text    not null,    "
	                  "  conninfo  text    not null,    "
	                  "  priority  integer not null,    "
	                  "  witness   boolean not null default false)", repmgr_schema);
	log_debug(_("master register: %s\n"), sqlquery);
	if (!PQexec(conn, sqlquery))
	{
		log_err(_("Cannot create the table %s.repl_nodes: %s\n"),
		        repmgr_schema, PQerrorMessage(conn));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	sqlquery_snprintf(sqlquery, "CREATE TABLE %s.repl_monitor ( "
	                  "  primary_node                   INTEGER NOT NULL, "
	                  "  standby_node                   INTEGER NOT NULL, "
	                  "  last_monitor_time              TIMESTAMP WITH TIME ZONE NOT NULL, "
	                  "  last_wal_primary_location      TEXT NOT NULL,   "
	                  "  last_wal_standby_location      TEXT,  "
	                  "  replication_lag                BIGINT NOT NULL, "
	                  "  apply_lag                      BIGINT NOT NULL) ", repmgr_schema);
	log_debug(_("master register: %s\n"), sqlquery);
	if (!PQexec(conn, sqlquery))
	{
		log_err(_("Cannot create the table %s.repl_monitor: %s\n"),
		        repmgr_schema, PQerrorMessage(conn));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* a view */
	sqlquery_snprintf(sqlquery, "CREATE VIEW %s.repl_status AS "
	                  " SELECT primary_node, standby_node, name AS standby_name, last_monitor_time, "
	                  "        last_wal_primary_location, last_wal_standby_location, "
	                  "        pg_size_pretty(replication_lag) replication_lag, "
	                  "        pg_size_pretty(apply_lag) apply_lag, "
	                  "        age(now(), last_monitor_time) AS time_lag "
	                  "   FROM %s.repl_monitor JOIN %s.repl_nodes ON standby_node = id "
	                  "  WHERE (standby_node, last_monitor_time) IN (SELECT standby_node, MAX(last_monitor_time) "
	                  "                                                FROM %s.repl_monitor GROUP BY 1)",
	                  repmgr_schema, repmgr_schema, repmgr_schema, repmgr_schema);
	log_debug(_("master register: %s\n"), sqlquery);
	if (!PQexec(conn, sqlquery))
	{
		log_err(_("Cannot create the view %s.repl_status: %s\n"),
		        repmgr_schema, PQerrorMessage(conn));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* an index to improve performance of the view */
	sqlquery_snprintf(sqlquery, "CREATE INDEX idx_repl_status_sort "
	                  "    ON %s.repl_monitor (last_monitor_time, standby_node) ",
	                  repmgr_schema);
	log_debug(_("master register: %s\n"), sqlquery);
	if (!PQexec(conn, sqlquery))
	{
		log_err(_("Cannot indexing table %s.repl_monitor: %s\n"),
		        repmgr_schema, PQerrorMessage(conn));
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* XXX Here we MUST try to load the repmgr_function.sql not hardcode it here */
	sqlquery_snprintf(sqlquery,
	                  "CREATE OR REPLACE FUNCTION %s.repmgr_update_standby_location(text) RETURNS boolean "
	                  "AS '$libdir/repmgr_funcs', 'repmgr_update_standby_location' "
	                  "LANGUAGE C STRICT ", repmgr_schema);
	if (!PQexec(conn, sqlquery))
	{
		fprintf(stderr, "Cannot create the function repmgr_update_standby_location: %s\n",
		        PQerrorMessage(conn));
		return false;
	}

	sqlquery_snprintf(sqlquery,
	                  "CREATE OR REPLACE FUNCTION %s.repmgr_get_last_standby_location() RETURNS text "
	                  "AS '$libdir/repmgr_funcs', 'repmgr_get_last_standby_location' "
	                  "LANGUAGE C STRICT ", repmgr_schema);
	if (!PQexec(conn, sqlquery))
	{
		fprintf(stderr, "Cannot create the function repmgr_get_last_standby_location: %s\n",
		        PQerrorMessage(conn));
		return false;
	}

	return true;
}


static bool
copy_configuration(PGconn *masterconn, PGconn *witnessconn)
{
	char		sqlquery[MAXLEN];
	PGresult	*res;
	int			i;

	sqlquery_snprintf(sqlquery, "TRUNCATE TABLE %s.repl_nodes", repmgr_schema);
	log_debug("copy_configuration: %s\n", sqlquery);
	if (!PQexec(witnessconn, sqlquery))
	{
		fprintf(stderr, "Cannot clean node details in the witness, %s\n",
		        PQerrorMessage(witnessconn));
		return false;
	}

	sqlquery_snprintf(sqlquery, "SELECT id, name, conninfo, priority, witness FROM %s.repl_nodes", repmgr_schema);
	res = PQexec(masterconn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Can't get configuration from master: %s\n",
		        PQerrorMessage(masterconn));
		PQclear(res);
		return false;
	}
	for (i = 0; i < PQntuples(res); i++)
	{
		sqlquery_snprintf(sqlquery, "INSERT INTO %s.repl_nodes(id, cluster, name, conninfo, priority, witness) "
		                  "VALUES (%d, '%s', '%s', '%s', %d, '%s')",
		                  repmgr_schema, atoi(PQgetvalue(res, i, 0)),
		                  options.cluster_name, PQgetvalue(res, i, 1),
		                  PQgetvalue(res, i, 2),
		                  atoi(PQgetvalue(res, i, 3)),
		                  PQgetvalue(res, i, 4));

		if (!PQexec(witnessconn, sqlquery))
		{
			fprintf(stderr, "Cannot copy configuration to witness, %s\n",
			        PQerrorMessage(witnessconn));
			PQclear(res);
			return false;
		}
	}

	return true;
}

/* This function uses global variables to determine connection settings. Special
 * usage of the PGPASSWORD variable is handled, but strongly discouraged */
static void
write_primary_conninfo(char* line)
{
	char host_buf[MAXLEN] = "";
	char conn_buf[MAXLEN] = "";
	char user_buf[MAXLEN] = "";
	char appname_buf[MAXLEN] = "";
	char password_buf[MAXLEN] = "";

	/* Environment variable for password (UGLY, please use .pgpass!) */
	const char *password = getenv("PGPASSWORD");
	if (password != NULL)
	{
		maxlen_snprintf(password_buf, " password=%s", password);
	}
	else if (require_password)
	{
		log_err(_("%s: PGPASSWORD not set, but having one is required\n"),
		        progname);
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
	                (runtime_options.masterport[0]) ? runtime_options.masterport : "5432", host_buf, user_buf, password_buf,
	                appname_buf);

	maxlen_snprintf(line, "primary_conninfo = '%s'", conn_buf);

}
