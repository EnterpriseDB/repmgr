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


void checkNodeConfiguration(char *conninfo);
void getPrimaryConnection(void);


void help(void);
void do_standby_clone(char *conninfo);


int
main(int argc, char **argv)
{
	char    myClusterName[MAXLEN];
	int     myLocalId   = -1;
    char 	myConninfo[MAXLEN]; 

	if (argc != 2)
		help();

	/*
	 * Read the configuration file: repmgr.conf
     */
	parse_config(myClusterName, &myLocalId, myConninfo);
	if (myLocalId == -1) 
	{
		fprintf(stderr, "Node information is missing. "
						"Check the configuration file.");
		exit(1);
	}

	/* XXX should we check the master pre requisites? */


	/* Check what is the action we need to execute */
	/* XXX Probably we can do this better but it works for now */
	if (strcasecmp(argv[1], "STANDBY") == 0)
	{
		if (strcasecmp(argv[2], "CLONE") == 0)
			do_standby_clone(myConninfo);
		else if (strcasecmp(argv[2], "PROMOTE") == 0)
			do_standby_promote();
		else if (strcasecmp(argv[2], "FOLLOW") == 0)
			do_standby_follow();
		else
			help();
	}
	else
		help();
		
    return 0;
}


void 
do_standby_clone(char *conninfo)
{
	PGconn 		*conn;
	PGresult	*res;
	char 		sqlquery[8192];
	char 		script[8192];

	int			r;
	char		data_dir_full_path[MAXLEN];
	char		*current_dir;
	char		data_dir[MAXLEN];
	char		recovery_file_path[MAXLEN];
	FILE		*recovery_file;

	char		line[MAXLEN];

	/* inform the master we will start a backup */
	conn = establishDBConnection(conninfo, true);	

	fprintf(stderr, "Starting backup...");
	
	/* Get the data directory full path and the last subdirectory */
	sprintf(sqlquery, "SELECT setting, "
							" TRIM(SUBSTRING(setting FROM '.[^/]*$'), '/') "
                       " FROM pg_settings WHERE name = 'data_directory'");
    res = PQexec(conn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't get info about data directory: %s", PQerrorMessage(conn));
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
        fprintf(stderr, "Can't start backup: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return;
    }
    PQclear(res);
    PQfinish(conn);

	/* rsync data directory to current location */
	sprintf(script, "rsync -r %s .", data_dir_full_path);
	r = system(script);
    if (r != 0)
    {
        fprintf(stderr, "Can't rsync data directory");
		return;
    }
	
    /* inform the master that we have finished the backup */
    conn = establishDBConnection(conninfo, true);

    fprintf(stderr, "Finishing backup...");

	sprintf(sqlquery, "SELECT pg_stop_backup()");
    res = PQexec(conn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't stop backup: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return;
    }
    PQclear(res);
    PQfinish(conn);

	/* Finally, write the recovery.conf file */
	getcwd(current_dir, MAXLEN); 
	strcpy(recovery_file_path, current_dir);
	strcat(recovery_file_path, "/");	
	strcat(recovery_file_path, data_dir);	
	strcat(recovery_file_path, "/");	
	strcat(recovery_file_path, RECOVERY_FILE);	
    free(current_dir);

    recovery_file = fopen(recovery_file_path, "w");
    if (recovery_file == NULL)
    {
        fprintf(stderr, "could not create recovery.conf file, it could be necesary to create it manually");
		return;
    }

	strcpy(line, "standby_mode = on\n");
	if (fputs(line, recovery_file) == EOF)
    {
        fprintf(stderr, "recovery file could not be written, it could be necesary to create it manually");
    	fclose(recovery_file);
		return;
    }

	strcpy(line, "primary_conninfo = ");
	strcat(line, conninfo);
	if (fputs(line, recovery_file) == EOF)
    {
        fprintf(stderr, "recovery file could not be written, it could be necesary to create it manually");
    	fclose(recovery_file);
		return;
    }

    /*FreeFile(recovery_file);*/
    fclose(recovery_file);

	return;
}


void 
do_standby_promote(char *conninfo)
{
	PGconn 		*conn;
	PGresult	*res;
	char 		sqlquery[8192];
	char 		script[8192];

	char		data_dir[MAXLEN];
	char		recovery_file_path[MAXLEN];
	char		recovery_done_path[MAXLEN];

	/* inform the master we will start a backup */
	conn = establishDBConnection(conninfo, true);	

	fprintf(stderr, "Promoting standby...");
	
	/* Get the data directory full path and the last subdirectory */
	sprintf(sqlquery, "SELECT setting "
                       " FROM pg_settings WHERE name = 'data_directory'");
    res = PQexec(conn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't get info about data directory: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return;
    }
	strcpy(data_dir, PQgetvalue(res, 0, 0));
    PQclear(res);
    PQfinish(conn);

	strcpy(recovery_file_path, data_dir);
	strcat(recovery_file_path, "/"); 
	strcpy(recovery_file_path, RECOVERY_FILE);

	strcpy(recovery_done_path, data_dir);
	strcat(recovery_done_path, "/"); 
	strcpy(recovery_done_path, RECOVERY_DONE);
	rename(recovery_file_path, recovery_done_path);

	sprintf(script, "pg_ctl -D %s restart", data_dir);
	r = system(script);
    if (r != 0)
    {
        fprintf(stderr, "Can't restart service");
		return;
    }

	return;
}


void 
do_standby_follow(char *conninfo)
{
	PGconn 		*conn;
	PGresult	*res;
	char 		sqlquery[8192];

	char		data_dir[MAXLEN];
	char		recovery_file_path[MAXLEN];
	FILE		*recovery_file;

	char		line[MAXLEN];

	/* inform the master we will start a backup */
	conn = establishDBConnection(conninfo, true);	

	fprintf(stderr, "Changing standby's primary...");
	
	/* Get the data directory full path and the last subdirectory */
	sprintf(sqlquery, "SELECT setting "
                       " FROM pg_settings WHERE name = 'data_directory'");
    res = PQexec(conn, sqlquery);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Can't get info about data directory: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return;
    }
	strcpy(data_dir, PQgetvalue(res, 0, 0));
    PQclear(res);
    PQfinish(conn);

	/* Finally, write the recovery.conf file */
	strcpy(recovery_file_path, data_dir);	
	strcat(recovery_file_path, "/");	
	strcat(recovery_file_path, RECOVERY_FILE);	

    recovery_file = fopen(recovery_file_path, "w");
    if (recovery_file == NULL)
    {
        fprintf(stderr, "could not create recovery.conf file, it could be necesary to create it manually");
		return;
    }

	strcpy(line, "standby_mode = on\n");
	if (fputs(line, recovery_file) == EOF)
    {
        fprintf(stderr, "recovery file could not be written, it could be necesary to create it manually");
    	fclose(recovery_file);
		return;
    }

	strcpy(line, "primary_conninfo = ");
	strcat(line, conninfo);
	if (fputs(line, recovery_file) == EOF)
    {
        fprintf(stderr, "recovery file could not be written, it could be necesary to create it manually");
    	fclose(recovery_file);
		return;
    }

    /*FreeFile(recovery_file);*/
    fclose(recovery_file);

	return;
}


void 
help(void)
{
    fprintf(stderr, "repmgr: command program that performs tasks and then exits.\n"
                    "COMMANDS:\n"
                    "standby clone - allows creation of a new standby\n"
                    "standby promote - allows manual promotion of a specific standby into a "
                                      "new master in the event of a failover\n"
                    "standby follow - allows the standby to re-point itself to a new master");
	exit(1);
}
