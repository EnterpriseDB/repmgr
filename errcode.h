/*
 * errcode.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#ifndef _ERRCODE_H_
#define _ERRCODE_H_

/* Exit return codes */

#define SUCCESS 0
#define ERR_BAD_CONFIG 1
#define ERR_BAD_RSYNC 2
#define ERR_NO_RESTART 4
#define ERR_DB_CON 6
#define ERR_DB_QUERY 7
#define ERR_PROMOTED 8
#define ERR_STR_OVERFLOW 10
#define ERR_FAILOVER_FAIL 11
#define ERR_BAD_SSH 12
#define ERR_SYS_FAILURE 13
#define ERR_BAD_BASEBACKUP 14
#define ERR_INTERNAL 15
#define ERR_MONITORING_FAIL 16
#define ERR_BAD_BACKUP_LABEL 17
#define ERR_SWITCHOVER_FAIL 18
#define ERR_BARMAN 19
#define ERR_REGISTRATION_SYNC 20

#endif	 /* _ERRCODE_H_ */

