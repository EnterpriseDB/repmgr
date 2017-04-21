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

#define MIN_SUPPORTED_VERSION		"9.3"
#define MIN_SUPPORTED_VERSION_NUM	90300

#define NODE_NOT_FOUND      -1
#define NO_UPSTREAM_NODE	-1
#define UNKNOWN_NODE_ID     -1

#define REPLICATION_TYPE_PHYSICAL 1
#define REPLICATION_TYPE_BDR      2

#define BDR_MONITORING_LOCAL    1
#define BDR_MONITORING_PRIORITY 2

#define MANUAL_FAILOVER		0
#define AUTOMATIC_FAILOVER	1
#define DEFAULT_PRIORITY		 100
#define FAILOVER_NODES_MAX_CHECK 50


#endif
