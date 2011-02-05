/*
 * repmgr.c - Command interpreter for the repmgr
 *
 *
 * This module is a command-line utility to easily setup a cluster of
 * hot standby servers for an HA environment
 *
 * Commands implemented are.
 * MASTER REGISTER, STANDBY REGISTER, STANDBY CLONE, STANDBY FOLLOW,
 * STANDBY PROMOTE
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
#include <time.h>
#include <unistd.h>

#include "check_dir.h"
#include "strutil.h"

#define RECOVERY_FILE "recovery.conf"
#define RECOVERY_DONE_FILE "recovery.done"

#define NO_ACTION		 0		/* Not a real action, just to initialize */
#define MASTER_REGISTER	 1
#define STANDBY_REGISTER 2
#define STANDBY_CLONE	 3
#define STANDBY_PROMOTE	 4
#define STANDBY_FOLLOW	 5

static void help(const char *progname);
static bool create_recovery_file(const char *data_dir, char *master_conninfo);
static int	copy_remote_files(char *host, char *remote_user, char *remote_path,
							  char *local_path, bool is_directory);
static bool check_parameters_for_action(const int action);

static void do_master_register(void);
static void do_standby_register(void);
static void do_standby_clone(void);
static void do_standby_promote(void);
static void do_standby_follow(void);

const char *progname;

const char *keywords[6];
const char *values[6];

const char	*dbname = NULL;
char		*host = NULL;
char		*username = NULL;
char		*dest_dir = NULL;
char		*config_file = NULL;
char		*remote_user = NULL;
char		*wal_keep_segments = NULL;
bool		verbose = false;
bool		force = false;

int			numport = 0;
char		*masterport = NULL;

char		*server_mode = NULL;
char		*server_cmd = NULL;

repmgr_config	config = {};

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
		{"config-file", required_argument, NULL, 'f'},
		{"remote-user", required_argument, NULL, 'R'},
		{"wal-keep-segments", required_argument, NULL, 'w'},
		{"force", no_argument, NULL, 'F'},
		{"verbose", no_argument, NULL, 'v'},
		{NULL, 0, NULL, 0}
	};

	int			optindex;
	int			c;
	int			action = NO_ACTION;

	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			help(progname);
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			printf("%s (PostgreSQL) " PG_VERSION "\n", progname);
			exit(0);
		}
	}


	while ((c = getopt_long(argc, argv, "d:h:p:U:D:f:R:w:F:v", long_options,
							&optindex)) != -1)
	{
		switch (c)
		{
		case 'd':
			dbname = optarg;
			break;
		case 'h':
			host = optarg;
			break;
		case 'p':
				masterport = optarg;
				break;
		case 'U':
			username = optarg;
			break;
		case 'D':
			dest_dir = optarg;
				break;
		case 'f':
			config_file = optarg;
				break;
		case 'R':
			remote_user = optarg;
				break;
		case 'w':
			wal_keep_segments = optarg;
			break;
		case 'F':
			force = true;
			break;
		case 'v':
			verbose = true;
			break;
		default:
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
			exit(1);
		}
	}

	/*
	 * Now we need to obtain the action, this comes in one of these forms:
	 * MASTER REGISTER |
	 * STANDBY {REGISTER | CLONE [node] | PROMOTE | FOLLOW [node]}
	 *
	 * the node part is optional, if we receive it then we shouldn't
	 * have received a -h option
	 */
	if (optind < argc)
	{
		server_mode = argv[optind++];
		if (strcasecmp(server_mode, "STANDBY") != 0 &&
			strcasecmp(server_mode, "MASTER") != 0)
		{
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
					progname);
			exit(1);
		}
	}

	if (optind < argc)
	{
		server_cmd = argv[optind++];
		if (strcasecmp(server_cmd, "REGISTER") == 0)
		{
			/*
			 * we don't use this info in any other place so i will
			 * just execute the compare again instead of having an
			 * additional variable to hold a value that we will use
			 * no more
			 */
			if (strcasecmp(server_mode, "MASTER") == 0)
				action = MASTER_REGISTER;
			else if (strcasecmp(server_mode, "STANDBY") == 0)
				action = STANDBY_REGISTER;
		}
		else if (strcasecmp(server_cmd, "CLONE") == 0)
			action = STANDBY_CLONE;
		else if (strcasecmp(server_cmd, "PROMOTE") == 0)
			action = STANDBY_PROMOTE;
		else if (strcasecmp(server_cmd, "FOLLOW") == 0)
			action = STANDBY_FOLLOW;
		else
		{
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
					progname);
			exit(1);
		}
	}

	/* For some actions we still can receive a last argument */
	if (action == STANDBY_CLONE)
	{
		if (optind < argc)
		{
			if (host != NULL)
			{
				fprintf(stderr, _("Conflicting parameters you can't use -h while providing a node separately. Try \"%s --help\" for more information.\n"), progname);
				exit(1);
			}
			host = argv[optind++];
		}
	}

	switch (optind < argc)
	{
	case 0:
		break;
	default:
		fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"),
		        progname, argv[optind + 1]);
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
					progname);
		exit(1);
	}

	if (!check_parameters_for_action(action))
		exit(1);

	if (config_file == NULL)
	{
		const int buf_sz = 3 + sizeof(CONFIG_FILE);

		config_file = malloc(buf_sz);
		xsnprintf(config_file, buf_sz, "./%s", CONFIG_FILE);
	}

	if (wal_keep_segments == NULL)
	{
		wal_keep_segments = malloc(5);
		strcpy(wal_keep_segments, "5000");
	}

	if (dbname == NULL)
	{
		if (getenv("PGDATABASE"))
			dbname = getenv("PGDATABASE");
		else if (getenv("PGUSER"))
			dbname = getenv("PGUSER");
		else
			dbname = "postgres";
	}
	
	/*
	 * Read the configuration file: repmgr.conf
	 */
	parse_config(config_file, &config);
	if (config.node == -1)
	{
		fprintf(stderr, "Node information is missing. "
		        "Check the configuration file.\n");
		exit(1);
	}

	keywords[2] = "user";
	values[2] = username;
	keywords[3] = "dbname";
	values[3] = dbname;
	keywords[4] = "application_name";
	values[4] = (char *) progname;
	keywords[5] = NULL;
	values[5] = NULL;

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
	default:
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
					progname);
		exit(1);
	}

	return 0;
}


static void
do_master_register(void)
{
	PGconn		*conn;
	PGresult	*res;
	char		sqlquery[QUERY_STR_LEN];

	bool		schema_exists = false;
	char		master_version[MAXVERSIONSTR];

	conn = establishDBConnection(config.conninfo, true);

	/* master should be v9 or better */
	pg_version(conn, master_version);
	if (strcmp(master_version, "") == 0)
	{
		PQfinish(conn);
		fprintf(stderr, _("%s needs master to be PostgreSQL 9.0 or better\n"),
				progname);
		return;
	}

	/* Check we are a master */
	if (is_standby(conn))
	{
		fprintf(stderr, "repmgr: This node should be a master\n");
		PQfinish(conn);
		return;
	}

	/* Check if there is a schema for this cluster */
	sqlquery_sprintf(sqlquery, "SELECT 1 FROM pg_namespace WHERE nspname = 'repmgr_%s'", config.cluster_name);
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Can't get info about schemas: %s\n",
				PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		return;
	}

	if (PQntuples(res) > 0)			/* schema exists */
	{
		if (!force)					/* and we are not forcing so error */
		{
			fprintf(stderr, "Schema repmgr_%s already exists.", config.cluster_name);
			PQclear(res);
			PQfinish(conn);
			return;
		}
		schema_exists = true;
	}
	PQclear(res);

	if (!schema_exists)
	{
		/* ok, create the schema */
		sqlquery_snprintf(sqlquery, "CREATE SCHEMA repmgr_%s", config.cluster_name);
		if (!PQexec(conn, sqlquery))
		{
			fprintf(stderr, "Cannot create the schema repmgr_%s: %s\n",
			        config.cluster_name, PQerrorMessage(conn));
			PQfinish(conn);
			return;
		}

		/* ... the tables */
		sqlquery_snprintf(sqlquery, "CREATE TABLE repmgr_%s.repl_nodes ( "
						  "	 id		   integer primary key, "
						  "	 cluster   text	   not null,	"
		        "  conninfo  text    not null)", config.cluster_name);
		if (!PQexec(conn, sqlquery))
		{
			fprintf(stderr,
			        config.cluster_name, PQerrorMessage(conn));
			PQfinish(conn);
			return;
		}

		sqlquery_snprintf(sqlquery, "CREATE TABLE repmgr_%s.repl_monitor ( "
						  "	 primary_node					INTEGER NOT NULL, "
						  "	 standby_node					INTEGER NOT NULL, "
						  "	 last_monitor_time				TIMESTAMP WITH TIME ZONE NOT NULL, "
						  "	 last_wal_primary_location		TEXT NOT NULL,	 "
						  "	 last_wal_standby_location		TEXT NOT NULL,	 "
						  "	 replication_lag				BIGINT NOT NULL, "
		        "  apply_lag                      BIGINT NOT NULL) ", config.cluster_name);
						  myClusterName);
		if (!PQexec(conn, sqlquery))
		{
			fprintf(stderr,
			        config.cluster_name, PQerrorMessage(conn));
			PQfinish(conn);
			return;
		}

		/* and the view */
		sqlquery_snprintf(sqlquery, "CREATE VIEW repmgr_%s.repl_status AS "
						  "	 WITH monitor_info AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY primary_node, standby_node "
						  " ORDER BY last_monitor_time desc) "
						  "	 FROM repmgr_%s.repl_monitor) "
						  "	 SELECT primary_node, standby_node, last_monitor_time, last_wal_primary_location, "
						  "			last_wal_standby_location, pg_size_pretty(replication_lag) replication_lag, "
						  "			pg_size_pretty(apply_lag) apply_lag, age(now(), last_monitor_time) AS time_lag "
						  "	   FROM monitor_info a "
		        "   WHERE row_number = 1", config.cluster_name, config.cluster_name);
		if (!PQexec(conn, sqlquery))
		{
			fprintf(stderr,
			        config.cluster_name, PQerrorMessage(conn));
			PQfinish(conn);
			return;
		}
	}
	else
	{
		PGconn *master_conn;
		int		id;

		/* Ensure there isn't any other master already registered */
		master_conn = getMasterConnection(conn, config.node, config.cluster_name, &id);
										  NULL);
		if (master_conn != NULL)
		{
			PQfinish(master_conn);
			fprintf(stderr, "There is a master already in this cluster");
			return;
		}
	}

	/* Now register the master */
	if (force)
	{
		sqlquery_snprintf(sqlquery, "DELETE FROM repmgr_%s.repl_nodes "
						  " WHERE id = %d",
		        config.cluster_name, config.node);

		if (!PQexec(conn, sqlquery))
		{
			fprintf(stderr, "Cannot delete node details, %s\n",
					PQerrorMessage(conn));
			PQfinish(conn);
			return;
		}
	}

	sqlquery_snprintf(sqlquery, "INSERT INTO repmgr_%s.repl_nodes "
					  "VALUES (%d, '%s', '%s')",
	        config.cluster_name, config.node, config.cluster_name, config.conninfo);

	if (!PQexec(conn, sqlquery))
	{
		fprintf(stderr, "Cannot insert node details, %s\n",
				PQerrorMessage(conn));
		PQfinish(conn);
		return;
	}

	PQfinish(conn);
	return;
}


static void
do_standby_register(void)
{
	PGconn		*conn;
	PGconn		*master_conn;
	int			master_id;

	PGresult	*res;
	char		sqlquery[QUERY_STR_LEN];

	char master_version[MAXVERSIONSTR];
	char standby_version[MAXVERSIONSTR];

	conn = establishDBConnection(config.conninfo, true);

	/* should be v9 or better */
	pg_version(conn, standby_version);
	if (strcmp(standby_version, "") == 0)
	{
		PQfinish(conn);
		fprintf(stderr, _("%s needs standby to be PostgreSQL 9.0 or better\n"),
				progname);
		return;
	}

	/* Check we are a standby */
	if (!is_standby(conn))
	{
		fprintf(stderr, "repmgr: This node should be a standby\n");
		PQfinish(conn);
		return;
	}

	/* Check if there is a schema for this cluster */
	sqlquery_snprintf(sqlquery, "SELECT 1 FROM pg_namespace WHERE nspname = 'repmgr_%s'", config.cluster_name);
					  "SELECT 1 FROM pg_namespace WHERE nspname = 'repmgr_%s'",
					  myClusterName);
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Can't get info about tablespaces: %s\n",
				PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		return;
	}

	if (PQntuples(res) == 0)		/* schema doesn't exists */
	{
		fprintf(stderr, "Schema repmgr_%s doesn't exists.", config.cluster_name);
		PQclear(res);
		PQfinish(conn);
		return;
	}
	PQclear(res);

	/* check if there is a master in this cluster */
	master_conn = getMasterConnection(conn, config.node, config.cluster_name, &master_id);
									  &master_id, NULL);
	if (!master_conn)
		return;

	/* master should be v9 or better */
	pg_version(master_conn, master_version);
	if (strcmp(master_version, "") == 0)
	{
		PQfinish(conn);
		PQfinish(master_conn);
		fprintf(stderr, _("%s needs master to be PostgreSQL 9.0 or better\n"),
				progname);
		return;
	}

	/* master and standby version should match */
	if (strcmp(master_version, standby_version) != 0)
	{
		PQfinish(conn);
		PQfinish(master_conn);
		fprintf(stderr, _("%s needs versions of both master (%s) and standby (%s) to match.\n"),
				progname, master_version, standby_version);
		return;
	}


	/* Now register the standby */
	if (force)
	{
		sqlquery_snprintf(sqlquery, "DELETE FROM repmgr_%s.repl_nodes "
						  " WHERE id = %d",
		        config.cluster_name, config.node);

		if (!PQexec(master_conn, sqlquery))
		{
			fprintf(stderr, "Cannot delete node details, %s\n",
					PQerrorMessage(master_conn));
			PQfinish(master_conn);
			PQfinish(conn);
			return;
		}
	}

	sqlquery_snprintf(sqlquery, "INSERT INTO repmgr_%s.repl_nodes "
					  "VALUES (%d, '%s', '%s')",
	        config.cluster_name, config.node, config.cluster_name, config.conninfo);

	if (!PQexec(master_conn, sqlquery))
	{
		fprintf(stderr, "Cannot insert node details, %s\n",
				PQerrorMessage(master_conn));
		PQfinish(master_conn);
		PQfinish(conn);
		return;
	}

	PQfinish(master_conn);
	PQfinish(conn);
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
	bool		pg_dir = false;
	char		master_data_directory[MAXLEN];
	char		master_config_file[MAXLEN];
	char		master_hba_file[MAXLEN];
	char		master_ident_file[MAXLEN];

	char		master_control_file[MAXLEN];
	char		local_control_file[MAXLEN];

	char		*first_wal_segment = NULL;
	const char	*last_wal_segment  = NULL;

	char	master_version[MAXVERSIONSTR];

	/* if dest_dir hasn't been provided, initialize to current directory */
	if (dest_dir == NULL)
	{
		dest_dir = malloc(5);
		strcpy(dest_dir, ".");
	}

	/* Check this directory could be used as a PGDATA dir */
	switch (check_dir(dest_dir))
	{
		case 0:
			/* dest_dir not there, must create it */
		if (verbose)
				printf(_("creating directory %s ... "), dest_dir);
			fflush(stdout);

			if (!create_directory(dest_dir))
		{
				fprintf(stderr, _("%s: couldn't create directory %s ... "),
			        progname, dest_dir);
			return;
		}
			break;
		case 1:
			/* Present but empty, fix permissions and use it */
		if (verbose)
				printf(_("fixing permissions on existing directory %s ... "),
					   dest_dir);
			fflush(stdout);

		if (!set_directory_permissions(dest_dir))
			{
				fprintf(stderr, _("%s: could not change permissions of directory \"%s\": %s\n"),
						progname, dest_dir, strerror(errno));
			return;
			}
			break;
		case 2:
			/* Present and not empty */
			fprintf(stderr,
					_("%s: directory \"%s\" exists but is not empty\n"),
					progname, dest_dir);

			pg_dir = is_pg_dir(dest_dir);
			if (pg_dir && !force)
		{
			fprintf(stderr, _("\nThis looks like a PostgreSQL directroy.\n"
								  "If you are sure you want to clone here, "
								  "please check there is no PostgreSQL server "
								  "running and use the --force option\n"));
			return;
		}
		else if (pg_dir && force)
		{
			/* Let it continue */
			break;
		}
			else
			return;
		default:
			/* Trouble accessing directory */
			fprintf(stderr, _("%s: could not access directory \"%s\": %s\n"),
					progname, dest_dir, strerror(errno));
	}

	/* Connection parameters for master only */
	keywords[0] = "host";
	values[0] = host;
	keywords[1] = "port";
	values[1] = masterport;

	/* We need to connect to check configuration and start a backup */
	conn = PQconnectdbParams(keywords, values, true);
	if (!conn)
	{
		fprintf(stderr, _("%s: could not connect to master\n"),
				progname);
		return;
	}

	/* primary should be v9 or better */
	pg_version(conn, master_version);
	if (strcmp(master_version, "") == 0)
	{
		PQfinish(conn);
		fprintf(stderr, _("%s needs master to be PostgreSQL 9.0 or better\n"),
				progname);
		return;
	}

	/* Check we are cloning a primary node */
	if (is_standby(conn))
	{
		PQfinish(conn);
		fprintf(stderr, "\nThe command should clone a primary node\n");
		return;
	}

	/* And check if it is well configured */
	if (!guc_setted(conn, "wal_level", "=", "hot_standby"))
	{
		PQfinish(conn);
		fprintf(stderr, _("%s needs parameter 'wal_level' to be set to 'hot_standby'\n"), progname);
		return;
	}
	if (!guc_setted(conn, "wal_keep_segments", ">=", wal_keep_segments))
	{
		PQfinish(conn);
		fprintf(stderr, _("%s needs parameter 'wal_keep_segments' to be set to %s or greater\n"), progname, wal_keep_segments);
		return;
	}
	if (!guc_setted(conn, "archive_mode", "=", "on"))
	{
		PQfinish(conn);
		fprintf(stderr, _("%s needs parameter 'archive_mode' to be set to 'on'\n"), progname);
		return;
	}

	if (verbose)
		printf(_("Succesfully connected to primary. Current installation size is %s\n"), get_cluster_size(conn));

	/*
	 * Check if the tablespace locations exists and that we can write to them.
	 */
	sqlquery_snprintf(sqlquery,
					  "SELECT spclocation "
					  "  FROM pg_tablespace "
					  "WHERE spcname NOT IN ('pg_default', 'pg_global')");
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Can't get info about tablespaces: %s\n", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		return;
	}
	for (i = 0; i < PQntuples(res); i++)
	{
		char *tblspc_dir = NULL;

		strcpy(tblspc_dir, PQgetvalue(res, i, 0));
		/* Check this directory could be used as a PGDATA dir */
		switch (check_dir(tblspc_dir))
		{
			case 0:
				/* tblspc_dir not there, must create it */
			if (verbose)
					printf(_("creating directory \"%s\"... "), tblspc_dir);
				fflush(stdout);

				if (!create_directory(tblspc_dir))
			{
					fprintf(stderr,
							_("%s: couldn't create directory \"%s\"... "),
				        progname, tblspc_dir);
				PQclear(res);
				PQfinish(conn);
				return;
			}
				break;
			case 1:
				/* Present but empty, fix permissions and use it */
			if (verbose)
					printf(_("fixing permissions on existing directory \"%s\"... "),
						   tblspc_dir);
				fflush(stdout);

				if (!set_directory_permissions(tblspc_dir))
				{
					fprintf(stderr, _("%s: could not change permissions of directory \"%s\": %s\n"),
							progname, tblspc_dir, strerror(errno));
				PQclear(res);
				PQfinish(conn);
				return;
				}
				break;
			case 2:
				/* Present and not empty */
				if (!force)
				{
					fprintf(
						stderr,
						_("%s: directory \"%s\" exists but is not empty\n"),
						progname, tblspc_dir);
					PQclear(res);
					PQfinish(conn);
					return;
				}
			default:
				/* Trouble accessing directory */
				fprintf(stderr,
						_("%s: could not access directory \"%s\": %s\n"),
						progname, tblspc_dir, strerror(errno));
				PQclear(res);
				PQfinish(conn);
				return;
		}
	}

	fprintf(stderr, "Starting backup...\n");

	/* Get the data directory full path and the configuration files location */
	sqlquery_snprintf(
		sqlquery,
		"SELECT name, setting "
		"  FROM pg_settings "
		"  WHERE name IN ('data_directory', 'config_file', 'hba_file', "
		"    'ident_file')");
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Can't get info about data directory and configuration files: %s\n", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		return;
	}
	for (i = 0; i < PQntuples(res); i++)
	{
		if (strcmp(PQgetvalue(res, i, 0), "data_directory") == 0)
			strcpy(master_data_directory, PQgetvalue(res, i, 1));
		else if (strcmp(PQgetvalue(res, i, 0), "config_file") == 0)
			strcpy(master_config_file, PQgetvalue(res, i, 1));
		else if (strcmp(PQgetvalue(res, i, 0), "hba_file") == 0)
			strcpy(master_hba_file, PQgetvalue(res, i, 1));
		else if (strcmp(PQgetvalue(res, i, 0), "ident_file") == 0)
			strcpy(master_ident_file, PQgetvalue(res, i, 1));
		else
			fprintf(stderr, _("uknown parameter: %s"), PQgetvalue(res, i, 0));
	}
	PQclear(res);

	/*
	 * inform the master we will start a backup and get the first XLog filename
	 * so we can say to the user we need those files
	 */
	sqlquery_snprintf(
		sqlquery,
		"SELECT pg_xlogfile_name(pg_start_backup('repmgr_standby_clone_%ld'))",
		time(NULL));
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Can't start backup: %s\n", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		return;
	}

	if (verbose)
	{
		char	*first_wal_seg_pq = PQgetvalue(res, 0, 0);
		size_t	 buf_sz			  = strlen(first_wal_seg_pq);

		first_wal_segment = malloc(buf_sz + 1);
		xsnprintf(first_wal_segment, buf_sz + 1, "%s", first_wal_seg_pq);
	}

	PQclear(res);

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
	maxlen_snprintf(master_control_file, "%s/global/pg_control",
					master_data_directory);
	maxlen_snprintf(local_control_file, "%s/global", dest_dir);
	if (!create_directory(local_control_file))
	{
		fprintf(stderr, _("%s: couldn't create directory %s ... "),
				progname, dest_dir);
		goto stop_backup;
	}

	r = copy_remote_files(host, remote_user, master_control_file,
						  local_control_file, false);
	if (r != 0)
		goto stop_backup;

	r = copy_remote_files(host, remote_user, master_data_directory, dest_dir,
						  true);
	if (r != 0)
		goto stop_backup;

	/*
	 * Copy tablespace locations, i'm doing this separately because i couldn't
	 * find and appropiate rsync option but besides we could someday make all
	 * these rsync happen concurrently
	 */
	sqlquery_snprintf(sqlquery,
					  "SELECT spclocation "
					  "  FROM pg_tablespace "
					  "  WHERE spcname NOT IN ('pg_default', 'pg_global')");
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Can't get info about tablespaces: %s\n",
				PQerrorMessage(conn));
		PQclear(res);
		goto stop_backup;
	}
	for (i = 0; i < PQntuples(res); i++)
	{
		r = copy_remote_files(host, remote_user, PQgetvalue(res, i, 0),
							  PQgetvalue(res, i, 0), true);
		if (r != 0)
			goto stop_backup;
	}

	r = copy_remote_files(host, remote_user, master_config_file, dest_dir,
						  false);
	if (r != 0)
		goto stop_backup;

	r = copy_remote_files(host, remote_user, master_hba_file, dest_dir, false);
	if (r != 0)
		goto stop_backup;

	r = copy_remote_files(host, remote_user, master_ident_file, dest_dir,
						  false);
	if (r != 0)
		goto stop_backup;

stop_backup:
	/* inform the master that we have finished the backup */
	conn = PQconnectdbParams(keywords, values, true);
	if (!conn)
	{
		fprintf(stderr, _("%s: could not connect to master\n"),
				progname);
		return;
	}

	fprintf(stderr, "Finishing backup...\n");

	sqlquery_snprintf(sqlquery, "SELECT pg_xlogfile_name(pg_stop_backup())");
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Can't stop backup: %s\n", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		return;
	}
	last_wal_segment = PQgetvalue(res, 0, 0);

	if (verbose)
	{
		printf(
			_("%s requires primary to keep WAL files %s until at least %s\n"),
			progname, first_wal_segment, last_wal_segment);

		/*
		 * Only free the first_wal_segment since it was copied out of the
		 * pqresult.
		 */
		free(first_wal_segment);
		first_wal_segment = NULL;
	}

	PQclear(res);
	PQfinish(conn);

	/* Now, if the rsync failed then exit */
	if (r != 0)
		return;

	/*
	 * We need to create the pg_xlog sub directory too, I'm reusing a variable
	 * here.
	 */
	maxlen_snprintf(local_control_file, "%s/pg_xlog", dest_dir);
	if (!create_directory(local_control_file))
	{
		fprintf(stderr, _("%s: couldn't create directory %s, you will need to do it manually...\n"),
				progname, dest_dir);
	}

	/* Finally, write the recovery.conf file */
	create_recovery_file(dest_dir, NULL);

	/*
	 * We don't start the service because we still may want to move the
	 * directory
	 */
	return;
}


static void
do_standby_promote(void)
{
	PGconn		*conn;
	PGresult	*res;
	char		sqlquery[QUERY_STR_LEN];
	char		script[MAXLEN];

	PGconn		*old_master_conn;
	int			old_master_id;

	int			r;
	char		data_dir[MAXLEN];
	char		recovery_file_path[MAXLEN];
	char		recovery_done_path[MAXLEN];

	char	standby_version[MAXVERSIONSTR];

	/* We need to connect to check configuration */
	conn = establishDBConnection(config.conninfo, true);

	/* we need v9 or better */
	pg_version(conn, standby_version);
	if (strcmp(standby_version, "") == 0)
	{
		PQfinish(conn);
		fprintf(stderr, _("%s needs standby to be PostgreSQL 9.0 or better\n"),
				progname);
		return;
	}

	/* Check we are in a standby node */
	if (!is_standby(conn))
	{
		fprintf(stderr,
				"repmgr: The command should be executed in a standby node\n");
		return;
	}

	/* we also need to check if there isn't any master already */
	old_master_conn = getMasterConnection(conn, config.node, config.cluster_name, &old_master_id);

	if (old_master_conn != NULL)
	{
		PQfinish(old_master_conn);
		fprintf(stderr, "There is a master already in this cluster");
		return;
	}

	if (verbose)
		printf(_("\n%s: Promoting standby...\n"), progname);

	/* Get the data directory full path and the last subdirectory */
	sqlquery_snprintf(sqlquery, "SELECT setting "
					  " FROM pg_settings WHERE name = 'data_directory'");
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Can't get info about data directory: %s\n",
				PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		return;
	}
	strcpy(data_dir, PQgetvalue(res, 0, 0));
	PQclear(res);
	PQfinish(conn);

	maxlen_snprintf(recovery_file_path, "%s/%s", data_dir, RECOVERY_FILE);
	maxlen_snprintf(recovery_done_path, "%s/%s", data_dir, RECOVERY_DONE_FILE);
	rename(recovery_file_path, recovery_done_path);

	/* We assume the pg_ctl script is in the PATH */
	maxlen_snprintf(script, "pg_ctl -D %s -m fast restart", data_dir);
	r = system(script);
	if (r != 0)
	{
		fprintf(stderr, "Can't restart service\n");
		return;
	}

	/* reconnect to check we got promoted */

	/*
	 * XXX i'm removing this because it gives an annoying message saying
	 * couldn't connect but is just the server starting up
	*    conn = establishDBConnection(config.conninfo, true);
	 *	if (is_standby(conn))
	 *		fprintf(stderr, "\n%s: STANDBY PROMOTE failed, this is still a standby node.\n", progname);
	 *	else
	 *		fprintf(stderr, "\n%s: you should REINDEX any hash indexes you have.\n", progname);
	 *	PQfinish(conn);
	 */

	return;
}


static void
do_standby_follow(void)
{
	PGconn		*conn;
	PGresult	*res;
	char		sqlquery[QUERY_STR_LEN];
	char		script[MAXLEN];

	char		master_conninfo[MAXLEN];
	PGconn		*master_conn;
	int			master_id;

	int			r;
	char		data_dir[MAXLEN];

	char	master_version[MAXVERSIONSTR];
	char	standby_version[MAXVERSIONSTR];

	/* We need to connect to check configuration */
	conn = establishDBConnection(config.conninfo, true);

	/* Check we are in a standby node */
	if (!is_standby(conn))
	{
		fprintf(stderr, "\n%s: The command should be executed in a standby node\n", progname);
		return;
	}

	/* should be v9 or better */
	pg_version(conn, standby_version);
	if (strcmp(standby_version, "") == 0)
	{
		PQfinish(conn);
		fprintf(stderr, _("\n%s needs standby to be PostgreSQL 9.0 or better\n"), progname);
		return;
	}

	/* we also need to check if there is any master in the cluster */
	master_conn = getMasterConnection(conn, config.node, config.cluster_name, &master_id);

	if (master_conn == NULL)
	{
		PQfinish(conn);
		fprintf(stderr, "There isn't a master to follow in this cluster");
		return;
	}

	/* Check we are going to point to a master */
	if (is_standby(master_conn))
	{
		PQfinish(conn);
		fprintf(stderr, "%s: The node to follow should be a master\n",
				progname);
		return;
	}

	/* should be v9 or better */
	pg_version(master_conn, master_version);
	if (strcmp(master_version, "") == 0)
	{
		PQfinish(conn);
		PQfinish(master_conn);
		fprintf(stderr, _("%s needs master to be PostgreSQL 9.0 or better\n"),
				progname);
		return;
	}

	/* master and standby version should match */
	if (strcmp(master_version, standby_version) != 0)
	{
		PQfinish(conn);
		PQfinish(master_conn);
		fprintf(stderr, _("%s needs versions of both master (%s) and standby (%s) to match.\n"),
				progname, master_version, standby_version);
		return;
	}

	/*
	 * set the host and masterport variables with the master ones
	 * before closing the connection because we will need them to
	 * recreate the recovery.conf file
	 */

	/*
	 * Copy the hostname to the 'host' global variable from the master
	 * connection.
	 */
	{
		char		*pqhost		 = PQhost(master_conn);
		const int	 host_buf_sz = strlen(pqhost);

		host = malloc(host_buf_sz + 1);
		xsnprintf(host, host_buf_sz, "%s", pqhost);
	}

	masterport = malloc(10);
	strcpy(masterport, PQport(master_conn));
	PQfinish(master_conn);

	if (verbose)
		printf(_("\n%s: Changing standby's master...\n"), progname);

	/* Get the data directory full path */
	sqlquery_snprintf(sqlquery, "SELECT setting "
					  " FROM pg_settings WHERE name = 'data_directory'");
	res = PQexec(conn, sqlquery);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Can't get info about data directory: %s\n",
				PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		return;
	}
	strcpy(data_dir, PQgetvalue(res, 0, 0));
	PQclear(res);
	PQfinish(conn);

	/* write the recovery.conf file */
	if (!create_recovery_file(data_dir, master_conninfo))
		return;

	/* Finally, restart the service */
	/* We assume the pg_ctl script is in the PATH */
	maxlen_snprintf(script, "pg_ctl -D %s -m fast restart", data_dir);
	r = system(script);
	if (r != 0)
	{
		fprintf(stderr, "Can't restart service\n");
		return;
	}

	return;
}


static void
help(const char *progname)
{
	printf(_("\n%s: Replicator manager \n"), progname);
	printf(_("Usage:\n"));
	printf(_(" %s [OPTIONS] master	{register}\n"), progname);
	printf(_(" %s [OPTIONS] standby {register|clone|promote|follow}\n"),
		   progname);
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
	printf(_("	-f, --config_file=PATH	   path to the configuration file\n"));
	printf(_("	-R, --remote-user=USERNAME database server username for rsync\n"));
	printf(_("	-w, --wal-keep-segments=VALUE  minimum value for the GUC wal_keep_segments (default: 5000)\n"));
	printf(_("	-F, --force				   force potentially dangerous operations to happen\n"));

	printf(_("\n%s performs some tasks like clone a node, promote it "), progname);
	printf(_("or making follow another node and then exits.\n"));
	printf(_("COMMANDS:\n"));
	printf(_(" master register		 - registers the master in a cluster\n"));
	printf(_(" standby register		 - registers a standby in a cluster\n"));
	printf(_(" standby clone [node]	 - allows creation of a new standby\n"));
	printf(_(" standby promote		 - allows manual promotion of a specific standby into a "));
	printf(_("new master in the event of a failover\n"));
	printf(_(" standby follow		 - allows the standby to re-point itself to a new master\n"));
}


/*
 * Creates a recovery file for a standby.
 *
 * Writes master_conninfo to recovery.conf if is non-NULL
 */
static bool
create_recovery_file(const char *data_dir, char *master_conninfo)
{
	FILE		*recovery_file;
	char		recovery_file_path[MAXLEN];
	char		line[MAXLEN];

	maxlen_snprintf(recovery_file_path, "%s/%s", data_dir, RECOVERY_FILE);

	recovery_file = fopen(recovery_file_path, "w");
	if (recovery_file == NULL)
	{
		fprintf(stderr, "could not create recovery.conf file, it could be necesary to create it manually\n");
		return false;
	}

	maxlen_snprintf(line, "standby_mode = 'on'\n");
	if (fputs(line, recovery_file) == EOF)
	{
		fprintf(stderr, "recovery file could not be written, it could be necesary to create it manually\n");
		fclose(recovery_file);
		return false;
	}

	/*
	 * Template a password into the connection string in recovery.conf.
	 * Sometimes this is passed by the user explicitly, and otherwise we try to
	 * get it into th environment
	 *
	 * XXX: This is pretty dirty, at least push this up to the caller rather
	 * than hitting environment variables at this level.
	 */
	if (master_conninfo == NULL)
	{
		char *password = getenv("PGPASSWORD");

		if (password == NULL)
		{
			fprintf(stderr,
					_("%s: Panic! PGPASSWORD not set, how can we get here?\n"),
					progname);
			exit(255);
		}

		maxlen_snprintf(line,
						"primary_conninfo = 'host=%s port=%s password=%s'\n",
						host, ((masterport==NULL) ? "5432" : masterport),
						password);
	}
	else
		maxlen_snprintf(line, "primary_conninfo = '%s'\n", master_conninfo);

	if (fputs(line, recovery_file) == EOF)
	{
		fprintf(stderr, "recovery file could not be written, it could be necesary to create it manually\n");
		fclose(recovery_file);
		return false;
	}

	/*FreeFile(recovery_file);*/
	fclose(recovery_file);

	return true;
}


static int
copy_remote_files(char *host, char *remote_user, char *remote_path,
				  char *local_path, bool is_directory)
{
	char script[MAXLEN];
	char options[MAXLEN];
	char host_string[MAXLEN];
	int	 r;

	if (strnlen(config.rsync_options, QUERY_STR_LEN) == 0)
	    sprintf(options, "--archive --checksum --compress --progress --rsh=ssh");
	else
	    strncpy(options, config.rsync_options, QUERY_STR_LEN);
	
	if (force)
		strcat(options, " --delete");

	if (remote_user == NULL)
	{
		maxlen_snprintf(host_string, "%s", host);
	}
	else
	{
		maxlen_snprintf(host_string,"%s@%s",remote_user,host);
	}

	if (is_directory)
	{
		strcat(options,
			   " --exclude=pg_xlog* --exclude=pg_control --exclude=*.pid");
		maxlen_snprintf(script, "rsync %s %s:%s/* %s",
		        options, host_string, remote_path, local_path);
	}
	else
	{
		maxlen_snprintf(script, "rsync %s %s:%s %s/.",
		        options, host_string, remote_path, local_path);
	}

	if (verbose)
		printf("rsync command line:	 '%s'\n",script);

	r = system(script);

	if (r != 0)
		fprintf(stderr,
				_("Can't rsync from remote file or directory (%s:%s)\n"),
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
			if ((host != NULL)	 || (masterport != NULL) ||
				(username != NULL) || (dbname != NULL))
		{
			fprintf(stderr, "\nYou can't use connection parameters to the master when issuing a MASTER REGISTER command.");
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
			ok = false;
			}
		if (dest_dir != NULL)
		{
			fprintf(stderr, "\nYou don't need a destination directory for MASTER REGISTER command");
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
			ok = false;
		}
		break;
	case STANDBY_REGISTER:
		/*
		 * To register a standby we only need the repmgr.conf
		 * we don't need connection parameters to the master
		 * because we can detect the master in repl_nodes
		 */
			if ((host != NULL)	 || (masterport != NULL) ||
				(username != NULL) || (dbname != NULL))
		{
			fprintf(stderr, "\nYou can't use connection parameters to the master when issuing a STANDBY REGISTER command.");
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
			ok = false;
			}
		if (dest_dir != NULL)
		{
			fprintf(stderr, "\nYou don't need a destination directory for STANDBY REGISTER command");
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
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
			if ((host != NULL)	 || (masterport != NULL) ||
				(username != NULL) || (dbname != NULL))
		{
			fprintf(stderr, "\nYou can't use connection parameters to the master when issuing a STANDBY PROMOTE command.");
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
			ok = false;
			}
		if (dest_dir != NULL)
		{
			fprintf(stderr, "\nYou don't need a destination directory for STANDBY PROMOTE command");
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
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
			if ((host != NULL)	 || (masterport != NULL) ||
				(username != NULL) || (dbname != NULL))
		{
			fprintf(stderr, "\nYou can't use connection parameters to the master when issuing a STANDBY FOLLOW command.");
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
			ok = false;
			}
		if (dest_dir != NULL)
		{
			fprintf(stderr, "\nYou don't need a destination directory for STANDBY FOLLOW command");
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
			ok = false;
		}
		break;
	case STANDBY_CLONE:
		/*
		 * To clone a master into a standby we need connection parameters
			 * repmgr.conf is useless because we don't have a server running
		 * in the standby
		 */
		if (config_file != NULL)
		{
			fprintf(stderr, "\nYou need to use connection parameters to the master when issuing a STANDBY CLONE command.");
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
			ok = false;
		}
		break;
	}

	return ok;
}
