/*
 * repmgr.c
 * Copyright (c) 2ndQuadrant, 2010
 *
 * Command interpreter for the repmgr
 * This module execute isome tasks based on commands and then exit
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

#include "repmgr.h"

#define RECOVERY_FILE "recovery.conf"
#define RECOVERY_DONE_FILE "recovery.done"


void help(void);
void do_standby_clone(char *master);
void do_standby_promote(void);
void do_standby_follow(char *master);


int
main(int argc, char **argv)
{

	if (argc != 3 && argc != 4)
		help();

	/* XXX should we check the master pre requisites? */


	/* Check what is the action we need to execute */
	/* XXX Probably we can do this better but it works for now */
	if (strcasecmp(argv[1], "STANDBY") == 0)
	{
		if (strcasecmp(argv[2], "CLONE") == 0)
		{
			/* 
			 * For STANDBY CLONE we should receive the hostname or ip
			 * of the node being cloned, it should be the third argument
			 */
			if (argc == 3)
				help();
				
			do_standby_clone(argv[3]);
		}
		else if (strcasecmp(argv[2], "PROMOTE") == 0)
		{
			/* 
			 * For STANDBY PROMOTE we doesn't need any arguments 
			 */
			if (argc == 4)
				help();
			do_standby_promote();
		}
		else if (strcasecmp(argv[2], "FOLLOW") == 0)
		{
			/* 
			 * For STANDBY FOLLOW we should receive the hostname or ip
			 * of the node being cloned, it should be the third argument
			 */
			if (argc == 3)
				help();
			do_standby_follow(argv[3]);
		}
		else
			help();
	}
	else
		help();
		
    return 0;
}


void 
do_standby_clone(char *master)
{
	PGconn 		*conn;
	PGresult	*res;
	char 		sqlquery[8192];
	char 		script[8192];
	
	char		master_conninfo[MAXLEN];

	int			r;
	char		data_dir_full_path[MAXLEN];
	char		data_dir[MAXLEN];
	char		recovery_file_path[MAXLEN];
	FILE		*recovery_file;

	char		line[MAXLEN];

	sprintf(master_conninfo, "host=%s", master);

	/* inform the master we will start a backup */
	conn = establishDBConnection(master_conninfo, true);	

	/* Check we are cloning a primary node */
	if (is_standby(conn))
	{
		fprintf(stderr, "repmgr: The command should clone a primary node\n");
		return;
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

	sprintf(sqlquery, "SELECT pg_start_backup('repmgr_standby_clone_%ld')", time(NULL));
    res = PQexec(conn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't start backup: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return;
    }
    PQclear(res);
    PQfinish(conn);

	/* rsync data directory to current location */
	sprintf(script, "rsync -r %s:%s .", master, data_dir_full_path);
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
    conn = establishDBConnection(master_conninfo, true);

    fprintf(stderr, "Finishing backup...\n");

	sprintf(sqlquery, "SELECT pg_stop_backup()");
    res = PQexec(conn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't stop backup: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return;
    }
    PQclear(res);
    PQfinish(conn);

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


void 
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


void 
do_standby_follow(char *master)
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

	sprintf(master_conninfo, "host=%s", master);
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


void 
help(void)
{
    fprintf(stderr, "repmgr: Replicator manager \n"
					"This command program performs some tasks like clone a node, promote it "
					"or making follow another node and then exits.\n"
                    "COMMANDS:\n"
                    "standby clone [node]  - allows creation of a new standby\n"
                    "standby promote       - allows manual promotion of a specific standby into a "
                                            "new master in the event of a failover\n"
                    "standby follow [node] - allows the standby to re-point itself to a new master\n");
	exit(1);
}
