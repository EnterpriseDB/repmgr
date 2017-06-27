/*
 * repmgr.c - repmgr extension
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 *
 * This is the actual extension code; see repmgr-client.c for the code which
 * generates the repmgr binary
 */

#include "postgres.h"
#include "fmgr.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "replication/walreceiver.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

#define MAXFNAMELEN		64

PG_MODULE_MAGIC;


void		_PG_init(void);
void		_PG_fini(void);

Datum		request_vote(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(request_vote);

/*
 * Module load callback
 */
void
_PG_init(void)
{
	elog(INFO, "repmgr init");
}
/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	elog(INFO, "repmgr fini");
}


Datum
request_vote(PG_FUNCTION_ARGS)
{
	uint32 node_id = PG_GETARG_INT32(0);

	elog(INFO, "id is %i", node_id);

	PG_RETURN_BOOL(true);
}
