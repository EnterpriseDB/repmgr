/*
 * dbutils.h
 * Copyright (c) 2ndQuadrant, 2010
 *
 */

#ifndef _REPMGR_H_
#define _REPMGR_H_

#include "postgres_fe.h"
#include "libpq-fe.h"

#include "dbutils.h"
#include "config.h"


#define PRIMARY_MODE		0
#define STANDBY_MODE		1

#define MAXLEN				80
#define CONFIG_FILE			"repmgr.conf"

#endif
