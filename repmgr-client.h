/*
 * repmgr-client.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#ifndef _REPMGR_CLIENT_H_
#define _REPMGR_CLIENT_H_

#include <getopt_long.h>
#include "log.h"



#define NO_ACTION			   0	/* Dummy default action */
#define MASTER_REGISTER		   1
#define STANDBY_REGISTER	   2
#define STANDBY_UNREGISTER	   3
#define STANDBY_CLONE		   4
#define STANDBY_PROMOTE		   5
#define STANDBY_FOLLOW		   6
#define STANDBY_SWITCHOVER	   7
#define STANDBY_ARCHIVE_CONFIG 8
#define STANDBY_RESTORE_CONFIG 9
#define WITNESS_CREATE		   10
#define WITNESS_REGISTER	   11
#define WITNESS_UNREGISTER	   12
#define CLUSTER_SHOW		   13
#define CLUSTER_CLEANUP		   14
#define CLUSTER_MATRIX		   15
#define CLUSTER_CROSSCHECK	   16
#define CLUSTER_EVENT		   17
#define BDR_REGISTER		   18
#define BDR_UNREGISTER		   19

/* command line options without short versions */
#define OPT_HELP						   1
#define OPT_CHECK_UPSTREAM_CONFIG		   2
#define OPT_RECOVERY_MIN_APPLY_DELAY	   3
#define OPT_COPY_EXTERNAL_CONFIG_FILES	   4
#define OPT_CONFIG_ARCHIVE_DIR			   5
#define OPT_PG_REWIND					   6
#define OPT_PWPROMPT					   7
#define OPT_CSV							   8
#define OPT_NODE						   9
#define OPT_NODE_ID					 	   10
#define OPT_NODE_NAME				 	   11
#define OPT_WITHOUT_BARMAN				   12
#define OPT_NO_UPSTREAM_CONNECTION		   13
#define OPT_REGISTER_WAIT				   14
#define OPT_CLUSTER						   15
#define OPT_LOG_TO_FILE					   16
#define OPT_UPSTREAM_CONNINFO			   17
/* replaces --no-conninfo-password */
#define OPT_USE_RECOVERY_CONNINFO_PASSWORD 18
#define OPT_REPLICATION_USER			   19
#define OPT_EVENT						   20
#define OPT_LIMIT						   21
#define OPT_ALL							   22
/* deprecated since 3.3 */
#define OPT_NO_CONNINFO_PASSWORD		   999


static struct option long_options[] =
{
/* general options */
	{"version", no_argument, NULL, 'V'},
	{"help", no_argument, NULL, OPT_HELP},

/* general configuration options */
	{"config-file", required_argument, NULL, 'f'},
	{"force", no_argument, NULL, 'F'},
	{"pg_bindir", required_argument, NULL, 'b'},

/* connection options */
	{"dbname", required_argument, NULL, 'd'},
	{"host", required_argument, NULL, 'h'},
	{"port", required_argument, NULL, 'p'},
	{"remote-user", required_argument, NULL, 'R'},
	{"superuser", required_argument, NULL, 'S'},
	{"username", required_argument, NULL, 'U'},

/* node options */
	{"pgdata", required_argument, NULL, 'D'},
	/* legacy alias for -D/--pgdata*/
	{"data-dir", required_argument, NULL, 'D'},
	{"node-id", required_argument, NULL, OPT_NODE_ID},
	{"node-name", required_argument, NULL, OPT_NODE_NAME},

/* logging options */
	{"log-level", required_argument, NULL, 'L'},
	{"log-to-file", no_argument, NULL, OPT_LOG_TO_FILE},
	{"terse", required_argument, NULL, 't'},
	{"verbose", no_argument, NULL, 'v'},

/* standby clone options */
	{"copy-external-config-files", optional_argument, NULL, OPT_COPY_EXTERNAL_CONFIG_FILES},
	{"fast-checkpoint", no_argument, NULL, 'c'},
	{"rsync-only", no_argument, NULL, 'r'},
	{"wal-keep-segments", required_argument, NULL, 'w'},
	{"no-upstream-connection", no_argument, NULL, OPT_NO_UPSTREAM_CONNECTION},
	{"recovery-min-apply-delay", required_argument, NULL, OPT_RECOVERY_MIN_APPLY_DELAY},
	{"replication-user", required_argument, NULL, OPT_REPLICATION_USER},
	{"upstream-conninfo", required_argument, NULL, OPT_UPSTREAM_CONNINFO},
	{"use-recovery-conninfo-password", no_argument, NULL, OPT_USE_RECOVERY_CONNINFO_PASSWORD},
	{"without-barman", no_argument, NULL, OPT_WITHOUT_BARMAN},

/* standby register options */
	{"wait-sync", optional_argument, NULL, OPT_REGISTER_WAIT},

/* event options */
	{"all", no_argument, NULL, OPT_ALL },
	{"event", required_argument, NULL, OPT_EVENT },
	{"limit", required_argument, NULL, OPT_LIMIT },

/* deprecated */
	{"no-conninfo-password", no_argument, NULL, OPT_NO_CONNINFO_PASSWORD},

/* not yet handled */
	{"keep-history", required_argument, NULL, 'k'},
	{"wait", no_argument, NULL, 'W'},
	{"rsync-only", no_argument, NULL, 'r'},
	{"fast-checkpoint", no_argument, NULL, 'c'},
	{"mode", required_argument, NULL, 'm'},
	{"remote-config-file", required_argument, NULL, 'C'},
	{"check-upstream-config", no_argument, NULL, OPT_CHECK_UPSTREAM_CONFIG},
	{"pg_rewind", optional_argument, NULL, OPT_PG_REWIND},
	{"pwprompt", optional_argument, NULL, OPT_PWPROMPT},
	{"csv", no_argument, NULL, OPT_CSV},
	{"node", required_argument, NULL, OPT_NODE},
	{"without-barman", no_argument, NULL, OPT_WITHOUT_BARMAN},
	{"copy-external-config-files", optional_argument, NULL, OPT_COPY_EXTERNAL_CONFIG_FILES},
	{"wait-sync", optional_argument, NULL, OPT_REGISTER_WAIT},
	/* Following options for internal use */
	{"cluster", required_argument, NULL, OPT_CLUSTER},
	{"config-archive-dir", required_argument, NULL, OPT_CONFIG_ARCHIVE_DIR},

	{NULL, 0, NULL, 0}
};



static void do_help(void);



static const char *action_name(const int action);
static void exit_with_errors(void);
static void print_item_list(ItemList *item_list);
static void check_cli_parameters(const int action);

static void write_primary_conninfo(char *line, t_conninfo_param_list *param_list);
static bool write_recovery_file_line(FILE *recovery_file, char *recovery_file_path, char *line);

#endif
