/*
 * repmgr_wrapper_funcs.c
 * Copyright (c) 2ndQuadrant, 2010
 *
 * Expose some backend functions in SQL
 */

#include "postgres.h"
#include "fmgr.h"
#include "access/xlog.h"

PG_MODULE_MAGIC;

Datum last_xlog_replay_timestamp(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(last_xlog_replay_timestamp);

Datum
last_xlog_replay_timestamp(PG_FUNCTION_ARGS)
{
TimestampTz rTime;
bool        fromSource;

	if (!InRecovery)
		PG_RETURN_NULL();
	else
	{
		GetXLogReceiptTime(&rTime, &fromStream);
	    PG_RETURN_TIMESTAMPTZ(rTime);
	}
}
