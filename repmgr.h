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

#define MIN_SUPPORTED_VERSION		"9.4"
#define MIN_SUPPORTED_VERSION_NUM	90400
#define UNKNOWN_SERVER_VERSION_NUM -1

#define NODE_NOT_FOUND		-1
#define NO_UPSTREAM_NODE	-1
#define UNKNOWN_NODE_ID		-1

#define REPLICATION_TYPE_PHYSICAL 1
#define REPLICATION_TYPE_BDR	  2


#define DEFAULT_LOCATION                 "default"
#define DEFAULT_PRIORITY		         100
#define DEFAULT_RECONNECTION_ATTEMPTS    6
#define DEFAULT_RECONNECTION_INTERVAL    10
#define DEFAULT_STATS_REPORTING_INTERVAL 2
#define DEFAULT_ASYNC_QUERY_TIMEOUT      60
#define DEFAULT_PRIMARY_NOTIFICATION_TIMEOUT 60
#define DEFAULT_PRIMARY_FOLLOW_TIMEOUT 60

#define FAILOVER_NODES_MAX_CHECK 50


#ifndef RECOVERY_COMMAND_FILE
#define RECOVERY_COMMAND_FILE "recovery.conf"
#endif

#ifndef TABLESPACE_MAP
#define TABLESPACE_MAP "tablespace_map"
#endif

#define ERRBUFF_SIZE 512



#endif /* _REPMGR_H_ */
