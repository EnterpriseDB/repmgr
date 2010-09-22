/*
 * repmgr.c
 * Copyright (c) 2ndQuadrant, 2010
 *
 * Command interpreter for the repmgr
 * This module execute some tasks based on commands and then exit
 */

#include "repmgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#define RECOVERY_FILE "recovery.conf"
#define RECOVERY_DONE_FILE "recovery.done"

#define STANDBY_CLONE 	1
#define STANDBY_PROMOTE 2
#define STANDBY_FOLLOW 	3

static void help(const char *progname);
static void do_standby_clone(void);
static void do_standby_promote(void);
static void do_standby_follow(void);

const char *progname;

const char *keywords[6];
const char *values[6];

const char	*dbname = NULL;
char		*host = NULL;
char		*username = NULL;
const char	*dest_dir = NULL;
bool		verbose = false;

int			numport = 0;
char		*masterport = NULL;
char		*standbyport = NULL;

char		*stndby = NULL;
char		*stndby_cmd = NULL;


int
main(int argc, char **argv)
{
	static struct option long_options[] = {
		{"dbname", required_argument, NULL, 'd'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"username", required_argument, NULL, 'U'},
		{"dest-dir", required_argument, NULL, 'D'},
		{"verbose", no_argument, NULL, 'v'},
		{NULL, 0, NULL, 0}
	};

	int			optindex;
	int			c;
	int			action;

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


	while ((c = getopt_long(argc, argv, "d:h:p:U:D:v", long_options, &optindex)) != -1)
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
                numport++;
                switch (numport)
                {
                    case 1:
                        masterport = optarg;
                        break;
                    case 2:
                        standbyport = optarg;
                        break;
                    default:
                        fprintf(stderr, _("%s: too many parameters of same type; master and standby only\n"), progname);
                }
                break;
			case 'U':
				username = optarg;
				break;
			case 'D':
				dest_dir = optarg;
				break;			
			case 'v':
				verbose = true;
				break;
			default:
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
				exit(1);
		}
	}

    /* Get real command from command line */
    if (optind < argc)
	{
        stndby = argv[optind++];
		if (strcasecmp(stndby, "STANDBY") != 0)
		{
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
			exit(1);
		}	
	}

    if (optind < argc)
	{
        stndby_cmd = argv[optind++];
		if (strcasecmp(stndby_cmd, "CLONE") == 0)  
			action = STANDBY_CLONE;
   		else if (strcasecmp(stndby_cmd, "PROMOTE") == 0) 
			action = STANDBY_PROMOTE;
   		else if (strcasecmp(stndby_cmd, "FOLLOW") == 0) 
			action = STANDBY_FOLLOW;
		else
		{
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
			exit(1);
		}	
	}

	switch (optind < argc)
	{
		case 0:
			break;
		default:
			fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"),
					progname, argv[optind + 1]);
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
			exit(1);
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

    if (standbyport == NULL)
        standbyport = masterport;

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
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
			exit(1);
	}
		
    return 0;
}


static void 
do_standby_clone(void)
{
	PGconn 		*conn;
	PGresult	*res;
	char 		sqlquery[8192];
	char 		script[8192];

	int			r;
	char		data_dir_full_path[MAXLEN];
	char		data_dir[MAXLEN];
	char		recovery_file_path[MAXLEN];

	char		*dummy_file;
	FILE		*recovery_file;

	char		*first_wal_segment, *last_wal_segment;

	char		line[MAXLEN];

    /* Connection parameters for master only */
    keywords[0] = "host";
    values[0] = host;
    keywords[1] = "port";
    values[1] = masterport;

	/* Can we write in this directory? write a dummy file to test that */
	sprintf(dummy_file, "%s/dummy", ((data_dir == NULL) ? "." : data_dir));
    recovery_file = fopen(dummy_file, "w");
    if (recovery_file == NULL)
	{
		fprintf(stderr, _("Can't write in this directory, check permissions"));
		return;
	}
	/* If we could write the file, unlink it... it was just a test */
	fclose(recovery_file);
	unlink(recovery_file);

	/* inform the master we will start a backup */
	conn = PQconnectdbParams(keywords, values, true);	
    if (!conn)
    {
        fprintf(stderr, _("%s: could not connect to master\n"),
                progname);
        return;
    }

	/* primary should be v9 or better */
	if (!is_supported_version(conn))
	{
		PQfinish(conn);
		fprintf(stderr, _("%s needs PostgreSQL 9.0 or better\n", progname)); 
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
	if (!guc_setted("wal_level", "=", "hot_standby"))
	{
		PQfinish(conn);
		fprintf(stderr, _("%s needs parameter 'wal_level' to be set to 'hot_standby'\n", progname)); 
		return;
	}
	if (!guc_setted("wal_keep_segments", "=", "5000"))
	{
		PQfinish(conn);
		fprintf(stderr, _("%s needs parameter 'wal_keep_segments' to be set greater than 0\n", progname)); 
		return;
	}
	if (!guc_setted("archive_mode", "=", "on"))
	{
		PQfinish(conn);
		fprintf(stderr, _("%s needs parameter 'archive_mode' to be set to 'on'\n", progname)); 
		return;
	}

	if (verbose)
		printf(_("Succesfully connected to primary. Current installation size is %s\n", get_cluster_size(conn)));

	fprintf(stderr, "Starting backup...\n");
	
	/* Get the data directory full path and the last subdirectory */
	sprintf(sqlquery, "SELECT setting, "
							" TRIM(SUBSTRING(setting FROM '.[^/]*$'), '/') "
                       " FROM pg_settings WHERE name = 'data_directory'");
    res = PQexec(conn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't get info about data directory: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return;
    }
	strcpy(data_dir_full_path, PQgetvalue(res, 0, 0));
	strcpy(data_dir, PQgetvalue(res, 0, 1));
    PQclear(res);

	sprintf(sqlquery, "SELECT pg_xlogfile_name(pg_start_backup('repmgr_standby_clone_%ld'))", time(NULL));
    res = PQexec(conn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't start backup: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return;
    }
	strcpy(first_wal_segment, PQgetvalue(res, 0, 0));
    PQclear(res);
    PQfinish(conn);

	/* rsync data directory to current location */
	sprintf(script, "rsync --checksum --keep-dirlinks --compress --progress -r %s:%s %s", 
					host, data_dir_full_path, ((data_dir == NULL) ? "." : data_dir));
	r = system(script);
    if (r != 0)
    {
        fprintf(stderr, "Can't rsync data directory\n");
		/* 
		 * we need to return but before that i will let the pg_stop_backup()
		 * happen
		 */
    }
	
    /* inform the master that we have finished the backup */
	conn = PQconnectdbParams(keywords, values, true);	
    if (!conn)
    {
        fprintf(stderr, _("%s: could not connect to master\n"),
                progname);
        return;
    }

    fprintf(stderr, "Finishing backup...\n");

	sprintf(sqlquery, "SELECT pg_xlogfile_name(pg_stop_backup())");
    res = PQexec(conn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't stop backup: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return;
    }
	strcpy(last_wal_segment, PQgetvalue(res, 0, 0));
    PQclear(res);
    PQfinish(conn);

	if (verbose)
		printf(_("%s requires primary to keep WAL files %s until at least %s", 
					progname, first_wal_segment, last_wal_segment));

	/* Now, if the rsync failed then exit */
	if (r != 0)
		return;

	/* Finally, write the recovery.conf file */
	sprintf(recovery_file_path, "%s/%s", data_dir, RECOVERY_FILE);

    recovery_file = fopen(recovery_file_path, "w");
    if (recovery_file == NULL)
    {
        fprintf(stderr, "could not create recovery.conf file, it could be necesary to create it manually\n");
		return;
    }

	sprintf(line, "standby_mode = 'on'\n");
	if (fputs(line, recovery_file) == EOF)
    {
        fprintf(stderr, "recovery file could not be written, it could be necesary to create it manually\n");
    	fclose(recovery_file);
		return;
    }

	sprintf(line, "primary_conninfo = '%s'\n", master_conninfo);
	if (fputs(line, recovery_file) == EOF)
    {
        fprintf(stderr, "recovery file could not be written, it could be necesary to create it manually\n");
    	fclose(recovery_file);
		return;
    }

    /*FreeFile(recovery_file);*/
    fclose(recovery_file);

	/* We don't start the service because we still may want to move the directory */
	return;
}


static void 
do_standby_promote(void)
{
	char    myClusterName[MAXLEN];
	int     myLocalId   = -1;
    char 	myConninfo[MAXLEN]; 

	PGconn 		*conn;
	PGresult	*res;
	char 		sqlquery[8192];
	char 		script[8192];

	int			r;
	char		data_dir[MAXLEN];
	char		recovery_file_path[MAXLEN];
	char		recovery_done_path[MAXLEN];

	/*
	 * Read the configuration file: repmgr.conf
     */
	parse_config(myClusterName, &myLocalId, myConninfo);
	if (myLocalId == -1) 
	{
		fprintf(stderr, "Node information is missing. "
						"Check the configuration file.\n");
		exit(1);
	}

	conn = establishDBConnection(myConninfo, true);	

	/* Check we are in a standby node */
	if (!is_standby(conn))
	{
		fprintf(stderr, "repmgr: The command should be executed in a standby node\n");
		return;
	}

	fprintf(stderr, "Promoting standby...\n");
	
	/* Get the data directory full path and the last subdirectory */
	sprintf(sqlquery, "SELECT setting "
                       " FROM pg_settings WHERE name = 'data_directory'");
    res = PQexec(conn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't get info about data directory: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return;
    }
	strcpy(data_dir, PQgetvalue(res, 0, 0));
    PQclear(res);
    PQfinish(conn);

	sprintf(recovery_file_path, "%s/%s", data_dir, RECOVERY_FILE);
	sprintf(recovery_done_path, "%s/%s", data_dir, RECOVERY_DONE_FILE);
	rename(recovery_file_path, recovery_done_path);

	/* We assume the pg_ctl script is in the PATH */
	sprintf(script, "pg_ctl -D %s -m fast restart", data_dir);
	r = system(script);
    if (r != 0)
    {
        fprintf(stderr, "Can't restart service\n");
		return;
    }

	return;
}


static void 
do_standby_follow(void)
{
	char    myClusterName[MAXLEN];
	int     myLocalId   = -1;
    char 	myConninfo[MAXLEN]; 

	PGconn 		*conn;
	PGresult	*res;
	char 		sqlquery[8192];
	char 		script[8192];

	char		master_conninfo[MAXLEN];

	int			r;
	char		data_dir[MAXLEN];
	char		recovery_file_path[MAXLEN];
	FILE		*recovery_file;

	char		line[MAXLEN];

	/*
	 * Read the configuration file: repmgr.conf
     */
	parse_config(myClusterName, &myLocalId, myConninfo);
	if (myLocalId == -1) 
	{
		fprintf(stderr, "Node information is missing. "
						"Check the configuration file.\n");
		exit(1);
	}

	sprintf(master_conninfo, "host=%s", host);
	conn = establishDBConnection(master_conninfo, true);	

	/* Check we are going to point to a primary */
	if (is_standby(conn))
	{
		fprintf(stderr, "repmgr: The should follow to a primary node\n");
		return;
	}
	PQfinish(conn);

	conn = establishDBConnection(myConninfo, true);	
	/* Check we are in a standby node */
	if (!is_standby(conn))
	{
		fprintf(stderr, "repmgr: The command should be executed in a standby node\n");
		return;
	}

	fprintf(stderr, "Changing standby's primary...\n");
	
	/* Get the data directory full path and the last subdirectory */
	sprintf(sqlquery, "SELECT setting "
                       " FROM pg_settings WHERE name = 'data_directory'");
    res = PQexec(conn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't get info about data directory: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return;
    }
	strcpy(data_dir, PQgetvalue(res, 0, 0));
    PQclear(res);
    PQfinish(conn);

	/* Finally, write the recovery.conf file */
	sprintf(recovery_file_path, "%s/%s", data_dir, RECOVERY_FILE);

    recovery_file = fopen(recovery_file_path, "w");
    if (recovery_file == NULL)
    {
        fprintf(stderr, "could not create recovery.conf file, it could be necesary to create it manually\n");
		return;
    }

	sprintf(line, "standby_mode = 'on'\n");
	if (fputs(line, recovery_file) == EOF)
    {
        fprintf(stderr, "recovery file could not be written, it could be necesary to create it manually\n");
    	fclose(recovery_file);
		return;
    }

	sprintf(line, "primary_conninfo = '%s'\n", master_conninfo);
	if (fputs(line, recovery_file) == EOF)
    {
        fprintf(stderr, "recovery file could not be written, it could be necesary to create it manually\n");
    	fclose(recovery_file);
		return;
    }

    /*FreeFile(recovery_file);*/
    fclose(recovery_file);

	/* Finally, restart the service */
	/* We assume the pg_ctl script is in the PATH */
	sprintf(script, "pg_ctl -D %s -m fast restart", data_dir);
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
    printf(stderr, "\n%s: Replicator manager \n"
					"This command program performs some tasks like clone a node, promote it "
					"or making follow another node and then exits.\n"
                    "COMMANDS:\n"
                    "standby clone [node]  - allows creation of a new standby\n"
                    "standby promote       - allows manual promotion of a specific standby into a "
                                            "new master in the event of a failover\n"
                    "standby follow [node] - allows the standby to re-point itself to a new master\n", progname);
}
