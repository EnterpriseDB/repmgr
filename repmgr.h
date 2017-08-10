/*
 * repmgr.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#ifndef _REPMGR_CONFIG_H
#define _REPMGR_CONFIG_H
#include <config.h>
#endif

#ifndef _REPMGR_H_
#define _REPMGR_H_

#include <stdio.h>
#include <stdlib.h>

#include <libpq-fe.h>
#include <postgres_fe.h>
#include <pqexpbuffer.h>


#include "repmgr_version.h"
#include "errcode.h"
#include "strutil.h"
#include "configfile.h"
#include "dbutils.h"
#include "log.h"

#define MIN_SUPPORTED_VERSION		"9.5"
#define MIN_SUPPORTED_VERSION_NUM	90500

#define REPLICATION_TYPE_PHYSICAL 1
#define REPLICATION_TYPE_BDR	  2

#define UNKNOWN_SERVER_VERSION_NUM -1

#define NODE_NOT_FOUND		-1
#define NO_UPSTREAM_NODE	-1
#define UNKNOWN_NODE_ID		-1


/*
 * various default values - ensure repmgr.conf.sample is update
 * if any of these are changed
 */
#define DEFAULT_LOCATION                 "default"
#define DEFAULT_PRIORITY		         100
#define DEFAULT_RECONNECTION_ATTEMPTS    6   /* seconds */
#define DEFAULT_RECONNECTION_INTERVAL    10  /* seconds */
#define DEFAULT_MONITORING_INTERVAL      2   /* seconds */
#define DEFAULT_ASYNC_QUERY_TIMEOUT      60  /* seconds */
#define DEFAULT_PRIMARY_NOTIFICATION_TIMEOUT 60  /* seconds */
#define DEFAULT_PRIMARY_FOLLOW_TIMEOUT   60  /* seconds */
#define DEFAULT_BDR_RECOVERY_TIMEOUT     30  /* seconds */
#define DEFAULT_ARCHIVER_LAG_WARNING     16  /* WAL files */
#define DEFAULT_ARCHIVER_LAG_CRITICAL    128 /* WAL files */
#define	DEFAULT_REPLICATION_LAG_WARNING  300 /* seconds */
#define DEFAULT_REPLICATION_LAG_CRITICAL 600 /* seconds */


#ifndef RECOVERY_COMMAND_FILE
#define RECOVERY_COMMAND_FILE "recovery.conf"
#endif

#ifndef TABLESPACE_MAP
#define TABLESPACE_MAP "tablespace_map"
#endif



#endif /* _REPMGR_H_ */
