/*
 * repmgr.c
 * Copyright (c) 2ndQuadrant, 2010
 *
 * Command interpreter for the repmgr
 * This module execute some tasks based on commands and then exit
 * 
 * Commands implemented are.
 * STANDBY CLONE, STANDBY FOLLOW, STANDBY PROMOTE
 */

#include "repmgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "check_dir.h"

#define RECOVERY_FILE "recovery.conf"
#define RECOVERY_DONE_FILE "recovery.done"

#define STANDBY_NORMAL  0		/* Not a real action, just to initialize */
#define STANDBY_CLONE 	1
#define STANDBY_PROMOTE 2
#define STANDBY_FOLLOW 	3

static void help(const char *progname);
static bool create_recovery_file(const char *data_dir);

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
	int			action = STANDBY_NORMAL;

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

    /* 
	 * Now we need to obtain the action, this comes in the form:
     * STANDBY {CLONE [node]|PROMOTE|FOLLOW [node]}
	 * 
     * the node part is optional, if we receive it then we shouldn't
     * have received a -h option
     */
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

	/* For STANDBY CLONE and STANDBY FOLLOW we still can receive a last argument */
	if ((action == STANDBY_CLONE) || (action == STANDBY_FOLLOW))
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

	int			r, i;
	char		data_dir_full_path[MAXLEN];
	char		data_dir[MAXLEN];

	const char	*first_wal_segment = NULL; 
    const char	*last_wal_segment = NULL;

	if (data_dir == NULL)
		strcpy(data_dir, ".");

	/* Check this directory could be used as a PGDATA dir */
    switch (check_dir(data_dir))
    {
        case 0:
            /* data_dir not there, must create it */
			if (verbose)
	            printf(_("creating directory %s ... "), data_dir);
            fflush(stdout);

            if (!create_directory(data_dir))
			{
            	fprintf(stderr, _("%s: couldn't create directory %s ... "), 
						progname, data_dir);
				return;
			}
            break;
        case 1:
            /* Present but empty, fix permissions and use it */
			if (verbose)
	            printf(_("fixing permissions on existing directory %s ... "),
   			                data_dir);
            fflush(stdout);

			if (!set_directory_permissions(data_dir))
            {
                fprintf(stderr, _("%s: could not change permissions of directory \"%s\": %s\n"),
                        progname, data_dir, strerror(errno));
				return;
            }
            break;
        case 2:
            /* Present and not empty */
            fprintf(stderr,
                    _("%s: directory \"%s\" exists but is not empty\n"),
                    progname, data_dir);
            return;     
        default:
            /* Trouble accessing directory */
            fprintf(stderr, _("%s: could not access directory \"%s\": %s\n"),
                    progname, data_dir, strerror(errno));
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
	if (!is_supported_version(conn))
	{
		PQfinish(conn);
		fprintf(stderr, _("%s needs PostgreSQL 9.0 or better\n"), progname); 
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
	if (!guc_setted(conn, "wal_keep_segments", ">=", "5000"))
	{
		PQfinish(conn);
		fprintf(stderr, _("%s needs parameter 'wal_keep_segments' to be set to 5000 or greater\n"), progname); 
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

	/* Check if the tablespace locations exists and that we can write to them */
	sprintf(sqlquery, "select location from pg_tablespace where spcname not in ('pg_default', 'pg_global')");
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
		/* Check this directory could be used as a PGDATA dir */
	    switch (check_dir(PQgetvalue(res, i, 0)))
	    {
	        case 0:
	            /* data_dir not there, must create it */
				if (verbose)
		            printf(_("creating directory \"%s\"... "), data_dir);
	            fflush(stdout);
	
	            if (!create_directory(data_dir))
				{
	            	fprintf(stderr, _("%s: couldn't create directory \"%s\"... "), 
							progname, data_dir);
					PQclear(res);
					PQfinish(conn);
					return;
				}
	            break;
	        case 1:
	            /* Present but empty, fix permissions and use it */
				if (verbose)
		            printf(_("fixing permissions on existing directory \"%s\"... "),
	   			                data_dir);
   	         fflush(stdout);
	
   	         if (!set_directory_permissions(data_dir))
   	         {
   	             fprintf(stderr, _("%s: could not change permissions of directory \"%s\": %s\n"),
   	                     progname, data_dir, strerror(errno));
					PQclear(res);
					PQfinish(conn);
					return;
   	         }
   	         break;
   	     case 2:
   	         /* Present and not empty */
			fprintf(stderr,
   	        		_("%s: directory \"%s\" exists but is not empty\n"),
   	                progname, data_dir);
			PQclear(res);
			PQfinish(conn);
   	        return;     
   	     default:
   	         /* Trouble accessing directory */
   	        fprintf(stderr, _("%s: could not access directory \"%s\": %s\n"),
   	                progname, data_dir, strerror(errno));
			PQclear(res);
			PQfinish(conn);
			return;
		}
	}
		
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

	/* 
     * inform the master we will start a backup and get the first XLog filename
	 * so we can say to the user we need those files
     */
	sprintf(sqlquery, "SELECT pg_xlogfile_name(pg_start_backup('repmgr_standby_clone_%ld'))", time(NULL));
    res = PQexec(conn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't start backup: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return;
    }
	first_wal_segment = PQgetvalue(res, 0, 0);
	PQclear(res);
	PQfinish(conn);

	/* rsync data directory to data_dir */
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
	last_wal_segment = PQgetvalue(res, 0, 0);
    PQclear(res);
    PQfinish(conn);

	if (verbose)
		printf(_("%s requires primary to keep WAL files %s until at least %s"), 
					progname, first_wal_segment, last_wal_segment);

	/* Now, if the rsync failed then exit */
	if (r != 0)
		return;

	/* Finally, write the recovery.conf file */
	create_recovery_file(dest_dir);

	/* We don't start the service because we still may want to move the directory */
	return;
}


static void 
do_standby_promote(void)
{
	PGconn 		*conn;
	PGresult	*res;
	char 		sqlquery[8192];
	char 		script[8192];

	int			r;
	char		data_dir[MAXLEN];
	char		recovery_file_path[MAXLEN];
	char		recovery_done_path[MAXLEN];

    /* Connection parameters for standby. always use localhost for standby */
    values[0] = "localhost";
    values[1] = standbyport;

	/* We need to connect to check configuration */
	conn = PQconnectdbParams(keywords, values, true);	
    if (!conn)
    {
        fprintf(stderr, _("%s: could not connect to master\n"),
                progname);
        return;
    }

	/* Check we are in a standby node */
	if (!is_standby(conn))
	{
		fprintf(stderr, "repmgr: The command should be executed in a standby node\n");
		return;
	}

	/* XXX also we need to check if there isn't any master already */

	if (verbose)
		printf(_("\n%s: Promoting standby...\n"), progname);
	
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
	PGconn 		*conn;
	PGresult	*res;
	char 		sqlquery[8192];
	char 		script[8192];

	int			r;
	char		data_dir[MAXLEN];

    /* Connection parameters for master */
    values[0] = host;
    values[1] = masterport;

	conn = PQconnectdbParams(keywords, values, true);	
    if (!conn)
    {
        fprintf(stderr, _("%s: could not connect to master\n"),
                progname);
        return;
    }

	/* Check we are going to point to a master */
	if (is_standby(conn))
	{
		fprintf(stderr, "repmgr: The node to follow should be a master\n");
		return;
	}
	PQfinish(conn);

    /* Connection parameters for standby. always use localhost for standby */
    values[0] = "localhost";
    values[1] = standbyport;

	/* We need to connect to check configuration */
	conn = PQconnectdbParams(keywords, values, true);	
    if (!conn)
    {
        fprintf(stderr, _("%s: could not connect to the local standby\n"),
                progname);
        return;
    }

	/* Check we are in a standby node */
	if (!is_standby(conn))
	{
		fprintf(stderr, "repmgr: The command should be executed in a standby node\n");
		return;
	}

	if (verbose)
		printf(_("\n%s: Changing standby's master...\n"), progname);
	
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
	if (!create_recovery_file(data_dir))
		return;		

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
    printf(_("\n%s: Replicator manager \n"), progname);
    printf(_("Usage:\n"));
    printf(_(" %s [OPTIONS] standby {clone|promote|follow} [master]\n"), progname);
    printf(_("\nOptions:\n"));
	printf(_("  --help                    show this help, then exit\n"));
	printf(_("  --version                 output version information, then exit\n"));
	printf(_("  --verbose                 output verbose activity information\n"));
	printf(_("\nConnection options:\n"));
	printf(_("  -d, --dbname=DBNAME       database to connect to\n"));
	printf(_("  -h, --host=HOSTNAME       database server host or socket directory\n"));
	printf(_("  -p, --port=PORT           database server port\n"));
	printf(_("  -U, --username=USERNAME   user name to connect as\n"));
    printf(_("\n%s performs some tasks like clone a node, promote it "), progname);
    printf(_("or making follow another node and then exits.\n"));
    printf(_("COMMANDS:\n"));
    printf(_(" standby clone [node]  - allows creation of a new standby\n"));
    printf(_(" standby promote       - allows manual promotion of a specific standby into a "));
    printf(_("new master in the event of a failover\n"));
    printf(_(" standby follow [node] - allows the standby to re-point itself to a new master\n"));
}


static bool
create_recovery_file(const char *data_dir)
{
	FILE		*recovery_file;
	char		recovery_file_path[MAXLEN];
	char		line[MAXLEN];

	sprintf(recovery_file_path, "%s/%s", data_dir, RECOVERY_FILE);

    recovery_file = fopen(recovery_file_path, "w");
    if (recovery_file == NULL)
    {
        fprintf(stderr, "could not create recovery.conf file, it could be necesary to create it manually\n");
		return false;
    }

	sprintf(line, "standby_mode = 'on'\n");
	if (fputs(line, recovery_file) == EOF)
    {
        fprintf(stderr, "recovery file could not be written, it could be necesary to create it manually\n");
    	fclose(recovery_file);
		return false;
    }

	sprintf(line, "primary_conninfo = '%s'\n", host);
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
