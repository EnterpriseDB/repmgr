/*
 * repmgr-client.h
 * Copyright (c) EnterpriseDB Corporation, 2010-2021
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
 */

#ifndef _REPMGR_CLIENT_H_
#define _REPMGR_CLIENT_H_

#include <getopt_long.h>
#include "log.h"

enum RepmgrAction
{
  NO_ACTION             =  0, /* Dummy default action */
  PRIMARY_REGISTER      =  1,
  PRIMARY_UNREGISTER    =  2,
  STANDBY_REGISTER      =  3,
  STANDBY_UNREGISTER    =  4,
  STANDBY_CLONE         =  5,
  STANDBY_PROMOTE       =  6,
  STANDBY_FOLLOW        =  7,
  STANDBY_SWITCHOVER    =  8,
  WITNESS_REGISTER      =  9,
  WITNESS_UNREGISTER    = 10,
  NODE_STATUS           = 11,
  NODE_CHECK            = 12,
  NODE_SERVICE          = 13,
  NODE_REJOIN           = 14,
  NODE_CONTROL          = 15,
  CLUSTER_SHOW          = 16,
  CLUSTER_CLEANUP       = 17,
  CLUSTER_MATRIX        = 18,
  CLUSTER_CROSSCHECK    = 19,
  CLUSTER_EVENT         = 20,
  SERVICE_STATUS        = 21,
  SERVICE_PAUSE         = 22,
  SERVICE_UNPAUSE       = 23,
  DAEMON_START          = 24,
  DAEMON_STOP           = 25
};

/* command line options without short versions */
enum RepmgrActionOption
{
  OPT_HELP 						                  = 1001,
  OPT_COPY_EXTERNAL_CONFIG_FILES 	      = 1002,
  OPT_CSV 							                = 1003,
  OPT_NODE_ID 						              = 1004,
  OPT_NODE_NAME 					              = 1005,
  OPT_WITHOUT_BARMAN 				            = 1006,
  OPT_NO_UPSTREAM_CONNECTION 		        = 1007,
  OPT_WAIT_SYNC 					              = 1008,
  OPT_LOG_TO_FILE 					            = 1009,
  OPT_UPSTREAM_CONNINFO 			          = 1010,
  OPT_REPLICATION_USER 			            = 1011,
  OPT_EVENT 						                = 1012,
  OPT_LIMIT 						                = 1013,
  OPT_ALL 							                = 1014,
  OPT_DRY_RUN 						              = 1015,
  OPT_UPSTREAM_NODE_ID 	                = 1016,
  OPT_ACTION 						                = 1017,
  OPT_LIST_ACTIONS 			                = 1018,
  OPT_CHECKPOINT 				                = 1019,
  OPT_IS_SHUTDOWN_CLEANLY 	        		= 1020,
  OPT_ALWAYS_PROMOTE 			        	    = 1021,
  OPT_FORCE_REWIND 				              = 1022,
  OPT_NAGIOS 						                = 1023,
  OPT_ARCHIVE_READY 				            = 1024,
  OPT_OPTFORMAT 					              = 1025,
  OPT_REPLICATION_LAG 			        	  = 1026,
  OPT_CONFIG_FILES 				              = 1027,
  OPT_SIBLINGS_FOLLOW 			        	  = 1028,
  OPT_ROLE 						                  = 1029,
  OPT_DOWNSTREAM 				                = 1030,
  OPT_UPSTREAM 					                = 1031,
  OPT_SLOTS 						                = 1032,
  OPT_HAS_PASSFILE 			                = 1033,
  OPT_WAIT_START 				                = 1034,
  OPT_REPL_CONN 					              = 1035,
  OPT_REMOTE_NODE_ID 		                = 1036,
  OPT_REPLICATION_CONF_ONLY 		        = 1037,
  OPT_NO_WAIT 						              = 1038,
  OPT_MISSING_SLOTS 				            = 1039,
  OPT_REPMGRD_NO_PAUSE 		              = 1040,
  OPT_VERSION_NUMBER 			              = 1041,
  OPT_DATA_DIRECTORY_CONFIG             = 1042,
  OPT_COMPACT 						              = 1043,
  OPT_DETAIL 						                = 1044,
  OPT_REPMGRD_FORCE_UNPAUSE             = 1045,
  OPT_REPLICATION_CONFIG_OWNER 	        = 1046,
  OPT_DB_CONNECTION 				            = 1047,
  OPT_VERIFY_BACKUP 				            = 1048,
  OPT_RECOVERY_MIN_APPLY_DELAY          = 1049,
  OPT_REPMGRD 						              = 1050
};

/* These options are for internal use only */
enum RepmgrActionOptionInternal
{
  OPT_CONFIG_ARCHIVE_DIR 			      = 2001,
  OPT_DISABLE_WAL_RECEIVER 		      = 2002,
  OPT_ENABLE_WAL_RECEIVER 			    = 2003,
  OPT_DUMP_CONFIG 					        = 2004
};

/* deprecated since 4.0 */
#define OPT_CHECK_UPSTREAM_CONFIG		    999


static struct option long_options[] =
{
/* general options */
	{"help", no_argument, NULL, OPT_HELP},
	{"version", no_argument, NULL, 'V'},
	{"version-number", no_argument, NULL, OPT_VERSION_NUMBER},

/* general configuration options */
	{"config-file", required_argument, NULL, 'f'},
	{"dry-run", no_argument, NULL, OPT_DRY_RUN},
	{"force", no_argument, NULL, 'F'},
	{"pg_bindir", required_argument, NULL, 'b'},
	{"wait", optional_argument, NULL, 'w'},
	{"no-wait", no_argument, NULL, 'W'},
	{"compact", no_argument, NULL, OPT_COMPACT},
	{"detail", no_argument, NULL, OPT_DETAIL},
	{"dump-config", no_argument, NULL, OPT_DUMP_CONFIG},

/* connection options */
	{"dbname", required_argument, NULL, 'd'},
	{"host", required_argument, NULL, 'h'},
	{"port", required_argument, NULL, 'p'},
	{"remote-user", required_argument, NULL, 'R'},
	{"superuser", required_argument, NULL, 'S'},
	{"username", required_argument, NULL, 'U'},

/* general node options */
	{"pgdata", required_argument, NULL, 'D'},
	{"node-id", required_argument, NULL, OPT_NODE_ID},
	{"node-name", required_argument, NULL, OPT_NODE_NAME},
	{"remote-node-id", required_argument, NULL, OPT_REMOTE_NODE_ID},

/* logging options */
	{"log-level", required_argument, NULL, 'L'},
	{"log-to-file", no_argument, NULL, OPT_LOG_TO_FILE},
	{"quiet",  no_argument, NULL, 'q'},
	{"terse", no_argument, NULL, 't'},
	{"verbose", no_argument, NULL, 'v'},

/* output options */
	{"csv", no_argument, NULL, OPT_CSV},
	{"nagios", no_argument, NULL, OPT_NAGIOS},
	{"optformat", no_argument, NULL, OPT_OPTFORMAT},

/* "standby clone" options */
	{"copy-external-config-files", optional_argument, NULL, OPT_COPY_EXTERNAL_CONFIG_FILES},
	{"fast-checkpoint", no_argument, NULL, 'c'},
	{"no-upstream-connection", no_argument, NULL, OPT_NO_UPSTREAM_CONNECTION},
	{"replication-user", required_argument, NULL, OPT_REPLICATION_USER},
	{"upstream-conninfo", required_argument, NULL, OPT_UPSTREAM_CONNINFO},
	{"upstream-node-id", required_argument, NULL, OPT_UPSTREAM_NODE_ID},
	{"without-barman", no_argument, NULL, OPT_WITHOUT_BARMAN},
	{"replication-conf-only", no_argument, NULL, OPT_REPLICATION_CONF_ONLY},
	{"verify-backup", no_argument, NULL, OPT_VERIFY_BACKUP },
	{"recovery-min-apply-delay", required_argument, NULL, OPT_RECOVERY_MIN_APPLY_DELAY },
	/* deprecate this once Pg11 and earlier are unsupported */
	{"recovery-conf-only", no_argument, NULL, OPT_REPLICATION_CONF_ONLY},

/* "standby register" options */
	{"wait-start", required_argument, NULL, OPT_WAIT_START},
	{"wait-sync", optional_argument, NULL, OPT_WAIT_SYNC},

/* "standby switchover" options
 *
 * Note: --force-rewind accepted to pass to "node rejoin"
 */
	{"always-promote", no_argument, NULL, OPT_ALWAYS_PROMOTE},
	{"siblings-follow", no_argument, NULL, OPT_SIBLINGS_FOLLOW},
	{"repmgrd-no-pause", no_argument, NULL, OPT_REPMGRD_NO_PAUSE},
	{"repmgrd-force-unpause", no_argument, NULL, OPT_REPMGRD_FORCE_UNPAUSE},

/* "node status" options */
	{"is-shutdown-cleanly", no_argument, NULL, OPT_IS_SHUTDOWN_CLEANLY},

/* "node check" options */
	{"archive-ready", no_argument, NULL, OPT_ARCHIVE_READY},
	{"downstream", no_argument, NULL, OPT_DOWNSTREAM},
	{"upstream", no_argument, NULL, OPT_UPSTREAM},
	{"replication-lag", no_argument, NULL, OPT_REPLICATION_LAG},
	{"role", no_argument, NULL, OPT_ROLE},
	{"slots", no_argument, NULL, OPT_SLOTS},
	{"missing-slots", no_argument, NULL, OPT_MISSING_SLOTS},
	{"repmgrd", no_argument, NULL, OPT_REPMGRD},
	{"has-passfile", no_argument, NULL, OPT_HAS_PASSFILE},
	{"replication-connection", no_argument, NULL, OPT_REPL_CONN},
	{"data-directory-config", no_argument, NULL, OPT_DATA_DIRECTORY_CONFIG},
	{"replication-config-owner", no_argument, NULL, OPT_REPLICATION_CONFIG_OWNER},
	{"db-connection", no_argument, NULL, OPT_DB_CONNECTION},

/* "node rejoin" options */
	{"config-files", required_argument, NULL, OPT_CONFIG_FILES},
    {"config-archive-dir", required_argument, NULL, OPT_CONFIG_ARCHIVE_DIR},
	{"force-rewind", optional_argument, NULL, OPT_FORCE_REWIND},

/* "node service" options */
	{"action", required_argument, NULL, OPT_ACTION},
	{"list-actions", no_argument, NULL, OPT_LIST_ACTIONS},
	{"checkpoint", no_argument, NULL, OPT_CHECKPOINT},

/* "cluster event" options */
	{"all", no_argument, NULL, OPT_ALL},
	{"event", required_argument, NULL, OPT_EVENT},
	{"limit", required_argument, NULL, OPT_LIMIT},

/* "cluster cleanup" options */
	{"keep-history", required_argument, NULL, 'k'},

/* undocumented options for testing */
	{"disable-wal-receiver", no_argument, NULL, OPT_DISABLE_WAL_RECEIVER},
	{"enable-wal-receiver", no_argument, NULL, OPT_ENABLE_WAL_RECEIVER},

/* deprecated */
	{"check-upstream-config", no_argument, NULL, OPT_CHECK_UPSTREAM_CONFIG},
	/* previously used by "standby switchover" */
	{"remote-config-file", required_argument, NULL, 'C'},
	{NULL, 0, NULL, 0}
};


static void do_help(void);

static const char *action_name(const int action);

static void check_cli_parameters(const int action);

#endif							/* _REPMGR_CLIENT_H_ */
