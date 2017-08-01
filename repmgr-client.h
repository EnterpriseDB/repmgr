/*
 * repmgr-client.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#ifndef _REPMGR_CLIENT_H_
#define _REPMGR_CLIENT_H_

#include <getopt_long.h>
#include "log.h"



#define NO_ACTION			   0	/* Dummy default action */
#define PRIMARY_REGISTER	   1
#define PRIMARY_UNREGISTER	   2
#define STANDBY_REGISTER	   3
#define STANDBY_UNREGISTER	   4
#define STANDBY_CLONE		   5
#define STANDBY_PROMOTE		   6
#define STANDBY_FOLLOW		   7
#define STANDBY_SWITCHOVER	   8
#define BDR_REGISTER		   9
#define BDR_UNREGISTER		   10
#define NODE_STATUS			   11
#define NODE_CHECK			   12
#define NODE_ARCHIVE_CONFIG    13
#define NODE_RESTORE_CONFIG    14
#define CLUSTER_SHOW		   15
#define CLUSTER_CLEANUP		   16
#define CLUSTER_MATRIX		   17
#define CLUSTER_CROSSCHECK	   18
#define CLUSTER_EVENT		   19

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
#define OPT_DRY_RUN						   23
#define OPT_UPSTREAM_NODE_ID			   24
/* deprecated since 3.3 */
#define OPT_DATA_DIR					   998
#define OPT_NO_CONNINFO_PASSWORD		   999


static struct option long_options[] =
{
/* general options */
	{"version", no_argument, NULL, 'V'},
	{"help", no_argument, NULL, OPT_HELP},

/* general configuration options */
	{"config-file", required_argument, NULL, 'f'},
	{"dry-run", no_argument, NULL, OPT_DRY_RUN},
	{"force", no_argument, NULL, 'F'},
	{"pg_bindir", required_argument, NULL, 'b'},
	{"wait", no_argument, NULL, 'W'},

/* connection options */
	{"dbname", required_argument, NULL, 'd'},
	{"host", required_argument, NULL, 'h'},
	{"port", required_argument, NULL, 'p'},
	{"remote-user", required_argument, NULL, 'R'},
	{"superuser", required_argument, NULL, 'S'},
	{"username", required_argument, NULL, 'U'},

/* node options */
	{"pgdata", required_argument, NULL, 'D'},
	{"node-id", required_argument, NULL, OPT_NODE_ID},
	{"node-name", required_argument, NULL, OPT_NODE_NAME},

/* logging options */
	{"log-level", required_argument, NULL, 'L'},
	{"log-to-file", no_argument, NULL, OPT_LOG_TO_FILE},
	{"terse", required_argument, NULL, 't'},
	{"verbose", no_argument, NULL, 'v'},

/* output options */
	{"csv", no_argument, NULL, OPT_CSV},

/* standby clone options */
	{"copy-external-config-files", optional_argument, NULL, OPT_COPY_EXTERNAL_CONFIG_FILES},
	{"fast-checkpoint", no_argument, NULL, 'c'},
	{"wal-keep-segments", required_argument, NULL, 'w'},
	{"no-upstream-connection", no_argument, NULL, OPT_NO_UPSTREAM_CONNECTION},
	{"recovery-min-apply-delay", required_argument, NULL, OPT_RECOVERY_MIN_APPLY_DELAY},
	{"replication-user", required_argument, NULL, OPT_REPLICATION_USER},
	{"upstream-conninfo", required_argument, NULL, OPT_UPSTREAM_CONNINFO},
	{"upstream-node-id", required_argument, NULL, OPT_UPSTREAM_NODE_ID},
	{"use-recovery-conninfo-password", no_argument, NULL, OPT_USE_RECOVERY_CONNINFO_PASSWORD},
	{"without-barman", no_argument, NULL, OPT_WITHOUT_BARMAN},

/* standby register options */
	{"wait-sync", optional_argument, NULL, OPT_REGISTER_WAIT},

/* event options */
	{"all", no_argument, NULL, OPT_ALL },
	{"event", required_argument, NULL, OPT_EVENT },
	{"limit", required_argument, NULL, OPT_LIMIT },

/* Following options for internal use */
	{"config-archive-dir", required_argument, NULL, OPT_CONFIG_ARCHIVE_DIR},

/* deprecated */
	{"no-conninfo-password", no_argument, NULL, OPT_NO_CONNINFO_PASSWORD},
	/* legacy alias for -D/--pgdata*/
	{"data-dir", required_argument, NULL, OPT_DATA_DIR},

/* not yet handled */
	{"keep-history", required_argument, NULL, 'k'},
	{"mode", required_argument, NULL, 'm'},
	{"remote-config-file", required_argument, NULL, 'C'},
	{"check-upstream-config", no_argument, NULL, OPT_CHECK_UPSTREAM_CONFIG},
	{"pg_rewind", optional_argument, NULL, OPT_PG_REWIND},
	{"pwprompt", optional_argument, NULL, OPT_PWPROMPT},

	{"node", required_argument, NULL, OPT_NODE},
	{"without-barman", no_argument, NULL, OPT_WITHOUT_BARMAN},
	{"copy-external-config-files", optional_argument, NULL, OPT_COPY_EXTERNAL_CONFIG_FILES},
	{"wait-sync", optional_argument, NULL, OPT_REGISTER_WAIT},

	{NULL, 0, NULL, 0}
};



static void do_help(void);



static const char *action_name(const int action);

static void check_cli_parameters(const int action);

static void write_primary_conninfo(char *line, t_conninfo_param_list *param_list);
static bool write_recovery_file_line(FILE *recovery_file, char *recovery_file_path, char *line);

#endif
