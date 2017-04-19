/*
 * repmgr-client.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include <getopt_long.h>

#include "config.h"

#ifndef _REPMGR_CLIENT_H_
#define _REPMGR_CLIENT_H_

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
#define BDR_REGISTER           17
#define BDR_UNREGISTER         18

/* command line options without short versions */
#define OPT_HELP                         1
#define OPT_CHECK_UPSTREAM_CONFIG        2
#define OPT_RECOVERY_MIN_APPLY_DELAY     3
#define OPT_COPY_EXTERNAL_CONFIG_FILES   4
#define OPT_CONFIG_ARCHIVE_DIR           5
#define OPT_PG_REWIND                    6
#define OPT_PWPROMPT                     7
#define OPT_CSV                          8
#define OPT_NODE                         9
#define OPT_WITHOUT_BARMAN               10
#define OPT_NO_UPSTREAM_CONNECTION       11
#define OPT_REGISTER_WAIT                12
#define OPT_CLUSTER                      13
#define OPT_LOG_TO_FILE                  14
#define OPT_UPSTREAM_CONNINFO            15
#define OPT_NO_CONNINFO_PASSWORD         16
#define OPT_REPLICATION_USER             17

static struct option long_options[] =
{
	{"version", no_argument, NULL, 'V'},
	{"help", no_argument, NULL, OPT_HELP},
	{"dbname", required_argument, NULL, 'd'},
	{"host", required_argument, NULL, 'h'},
	{"port", required_argument, NULL, 'p'},
	{"username", required_argument, NULL, 'U'},
	{"superuser", required_argument, NULL, 'S'},
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
	{"terse", required_argument, NULL, 't'},
	{"mode", required_argument, NULL, 'm'},
	{"remote-config-file", required_argument, NULL, 'C'},
	{"check-upstream-config", no_argument, NULL, OPT_CHECK_UPSTREAM_CONFIG},
	{"recovery-min-apply-delay", required_argument, NULL, OPT_RECOVERY_MIN_APPLY_DELAY},
	{"pg_rewind", optional_argument, NULL, OPT_PG_REWIND},
	{"pwprompt", optional_argument, NULL, OPT_PWPROMPT},
	{"csv", no_argument, NULL, OPT_CSV},
	{"node", required_argument, NULL, OPT_NODE},
	{"without-barman", no_argument, NULL, OPT_WITHOUT_BARMAN},
	{"no-upstream-connection", no_argument, NULL, OPT_NO_UPSTREAM_CONNECTION},
	{"copy-external-config-files", optional_argument, NULL, OPT_COPY_EXTERNAL_CONFIG_FILES},
	{"wait-sync", optional_argument, NULL, OPT_REGISTER_WAIT},
	{"log-to-file", no_argument, NULL, OPT_LOG_TO_FILE},
	{"upstream-conninfo", required_argument, NULL, OPT_UPSTREAM_CONNINFO},
	{"replication-user", required_argument, NULL, OPT_REPLICATION_USER},
	{"no-conninfo-password", no_argument, NULL, OPT_NO_CONNINFO_PASSWORD},
	/* Following options for internal use */
	{"cluster", required_argument, NULL, OPT_CLUSTER},
	{"config-archive-dir", required_argument, NULL, OPT_CONFIG_ARCHIVE_DIR},
	{NULL, 0, NULL, 0}
};

static void do_help(void);

#endif
