/*
 * repmgr.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 */
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
#include "config.h"
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

#define BDR_MONITORING_LOCAL	1
#define BDR_MONITORING_PRIORITY 2

#define DEFAULT_PRIORITY		 100
#define FAILOVER_NODES_MAX_CHECK 50


#ifndef RECOVERY_COMMAND_FILE
#define RECOVERY_COMMAND_FILE "recovery.conf"
#endif

#ifndef TABLESPACE_MAP
#define TABLESPACE_MAP "tablespace_map"
#endif

#define ERRBUFF_SIZE 512

#define WITNESS_DEFAULT_PORT "5499" /* If this value is ever changed, remember
									 * to update comments and documentation */

#endif
