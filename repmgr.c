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
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"

#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "pgstat.h"


#include "voting.h"

#define UNKNOWN_NODE_ID		-1

#define TRANCHE_NAME "repmgrd"

PG_MODULE_MAGIC;

typedef enum {
	LEADER_NODE,
	FOLLOWER_NODE,
	CANDIDATE_NODE
} NodeState;

typedef struct repmgrdSharedState
{
	LWLockId	lock;			/* protects search/modification */
	TimestampTz last_updated;
	/* streaming failover */
	NodeState	node_state;
	NodeVotingStatus voting_status;
	int current_electoral_term;
	int candidate_node_id;
	bool follow_new_primary;
	/* BDR failover */
    int bdr_failover_handler;
}	repmgrdSharedState;

static repmgrdSharedState *shared_state = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;


void		_PG_init(void);
void		_PG_fini(void);

static void repmgr_shmem_startup(void);

Datum		standby_set_last_updated(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(standby_set_last_updated);

Datum		standby_get_last_updated(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(standby_get_last_updated);


Datum		request_vote(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(request_vote);

Datum		get_voting_status(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(get_voting_status);

Datum		set_voting_status_initiated(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(set_voting_status_initiated);

Datum		other_node_is_candidate(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(other_node_is_candidate);

Datum		notify_follow_primary(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(notify_follow_primary);

Datum		get_new_primary(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(get_new_primary);

Datum		reset_voting_status(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(reset_voting_status);

Datum		am_bdr_failover_handler(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(am_bdr_failover_handler);

Datum		unset_bdr_failover_handler(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(unset_bdr_failover_handler);


/*
 * Module load callback
 */
void
_PG_init(void)
{
	elog(INFO, "repmgr init");

	// error here?
	if (!process_shared_preload_libraries_in_progress)
		return;

	RequestAddinShmemSpace(MAXALIGN(sizeof(repmgrdSharedState)));

#if (PG_VERSION_NUM >= 90600)
	RequestNamedLWLockTranche(TRANCHE_NAME, 1);
#else
	RequestAddinLWLocks(1);
#endif

	/*
	 * Install hooks.
	 */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = repmgr_shmem_startup;

}
/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hook */
	shmem_startup_hook = prev_shmem_startup_hook;
}


/*
 * shmem_startup hook: allocate or attach to shared memory,
 */
static void
repmgr_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	shared_state = NULL;

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	shared_state = ShmemInitStruct("repmgrd shared state",
								   sizeof(repmgrdSharedState),
								   &found);

	if (!found)
	{
		/* Initialise shared memory struct */
#if (PG_VERSION_NUM >= 90600)
		shared_state->lock = &(GetNamedLWLockTranche(TRANCHE_NAME))->lock;
#else
		shared_state->lock = LWLockAssign();
#endif

		shared_state->current_electoral_term = 0;
		shared_state->voting_status = VS_NO_VOTE;
		shared_state->candidate_node_id = UNKNOWN_NODE_ID;
		shared_state->follow_new_primary = false;
		shared_state->bdr_failover_handler = UNKNOWN_NODE_ID;
	}

	LWLockRelease(AddinShmemInitLock);
}


/* ==================== */
/* monitoring functions */
/* ==================== */

/* update and return last updated with current timestamp */
Datum
standby_set_last_updated(PG_FUNCTION_ARGS)
{
	TimestampTz last_updated = GetCurrentTimestamp();

	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);
	shared_state->last_updated = last_updated;
	LWLockRelease(shared_state->lock);

	PG_RETURN_TIMESTAMPTZ(last_updated);
}


/* get last updated timestamp */
Datum
standby_get_last_updated(PG_FUNCTION_ARGS)
{
	TimestampTz last_updated;

	/* Safety check... */
	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);
	last_updated = shared_state->last_updated;
	LWLockRelease(shared_state->lock);

	PG_RETURN_TIMESTAMPTZ(last_updated);
}




/* ===================*/
/* failover functions */
/* ===================*/

Datum
request_vote(PG_FUNCTION_ARGS)
{
#ifndef BDR_ONLY
	StringInfoData	query;
	XLogRecPtr our_lsn = InvalidXLogRecPtr;

	/* node_id used for logging purposes */
	int requesting_node_id = PG_GETARG_INT32(0);
	int current_electoral_term = PG_GETARG_INT32(1);

	int		ret;
	bool	isnull;

	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);

	/* this node has initiated voting or already responded to another node */
	if (shared_state->voting_status != VS_NO_VOTE)
	{
		LWLockRelease(shared_state->lock);

		PG_RETURN_NULL();
	}

	elog(INFO, "requesting node id is %i for electoral term %i (our term: %i)",
		 requesting_node_id, current_electoral_term,
		 shared_state->current_electoral_term);

	SPI_connect();

	initStringInfo(&query);

	appendStringInfo(
		&query,
#if (PG_VERSION_NUM >= 100000)
		"SELECT pg_catalog.pg_last_wal_receive_lsn()");
#else
		"SELECT pg_catalog.pg_last_xlog_receive_location()");
#endif

	elog(INFO, "query: %s", query.data);
	ret = SPI_execute(query.data, true, 0);

	if (ret < 0)
	{
		SPI_finish();
		elog(WARNING, "unable to retrieve last received LSN");
		PG_RETURN_LSN(InvalidOid);
	}

	our_lsn = DatumGetLSN(SPI_getbinval(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc,
										1, &isnull));


	elog(INFO, "Our LSN is %X/%X",
		 (uint32) (our_lsn >> 32),
		 (uint32) our_lsn);

	/* indicate this node has responded to a vote request */
	shared_state->voting_status = VS_VOTE_REQUEST_RECEIVED;
	shared_state->current_electoral_term = current_electoral_term;

	LWLockRelease(shared_state->lock);

	// should we free "query" here?
	SPI_finish();

	PG_RETURN_LSN(our_lsn);
#else
	PG_RETURN(InvalidOid);
#endif
}



Datum
get_voting_status(PG_FUNCTION_ARGS)
{
#ifndef BDR_ONLY
	NodeVotingStatus voting_status;

	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);
	voting_status = shared_state->voting_status;
	LWLockRelease(shared_state->lock);

	PG_RETURN_INT32(voting_status);
#else
	PG_RETURN_INT32(VS_UNKNOWN);
#endif
}

Datum
set_voting_status_initiated(PG_FUNCTION_ARGS)
{
#ifndef BDR_ONLY
	int electoral_term;

	LWLockAcquire(shared_state->lock, LW_SHARED);
	shared_state->voting_status = VS_VOTE_INITIATED;
	shared_state->current_electoral_term += 1;

	electoral_term = shared_state->current_electoral_term;
	LWLockRelease(shared_state->lock);

	elog(INFO, "setting voting term to %i", electoral_term);

	PG_RETURN_INT32(electoral_term);
#else
	PG_RETURN_INT32(-1);
#endif
}

Datum
other_node_is_candidate(PG_FUNCTION_ARGS)
{
#ifndef BDR_ONLY
	int  requesting_node_id = PG_GETARG_INT32(0);
	int  electoral_term = PG_GETARG_INT32(1);

	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);

	if (shared_state->current_electoral_term == electoral_term)
	{
		if (shared_state->candidate_node_id != UNKNOWN_NODE_ID)
		{
			elog(INFO, "node %i requesting candidature, but node %i already candidate",
				 requesting_node_id,
				 shared_state->candidate_node_id);
			PG_RETURN_BOOL(false);
		}
	}

	shared_state->candidate_node_id = requesting_node_id;
	LWLockRelease(shared_state->lock);

	elog(INFO, "node %i is candidate", requesting_node_id);
	PG_RETURN_BOOL(true);
#else
	PG_RETURN_BOOL(false);
#endif
}

Datum
notify_follow_primary(PG_FUNCTION_ARGS)
{
#ifndef BDR_ONLY
	int primary_node_id = PG_GETARG_INT32(0);

	if (!shared_state)
		PG_RETURN_NULL();

	elog(INFO, "received notification to follow node %i", primary_node_id);

	LWLockAcquire(shared_state->lock, LW_SHARED);

	/* Explicitly set the primary node id */
	shared_state->candidate_node_id = primary_node_id;
	shared_state->follow_new_primary = true;
	LWLockRelease(shared_state->lock);
#endif
	PG_RETURN_VOID();
}


Datum
get_new_primary(PG_FUNCTION_ARGS)
{
	int new_primary_node_id = UNKNOWN_NODE_ID;

	if (!shared_state)
		PG_RETURN_NULL();

#ifndef BDR_ONLY
	LWLockAcquire(shared_state->lock, LW_SHARED);

	if (shared_state->follow_new_primary == true)
		new_primary_node_id = shared_state->candidate_node_id;

	LWLockRelease(shared_state->lock);
#endif
	PG_RETURN_INT32(new_primary_node_id);
}


Datum
reset_voting_status(PG_FUNCTION_ARGS)
{
#ifndef BDR_ONLY
	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);

	shared_state->voting_status = VS_NO_VOTE;
	shared_state->candidate_node_id = UNKNOWN_NODE_ID;
	shared_state->follow_new_primary = false;

	LWLockRelease(shared_state->lock);
#endif
	PG_RETURN_VOID();
}


Datum
am_bdr_failover_handler(PG_FUNCTION_ARGS)
{
	int node_id = PG_GETARG_INT32(0);
	bool am_handler = false;

	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);

	if (shared_state->bdr_failover_handler == UNKNOWN_NODE_ID)
	{
		shared_state->bdr_failover_handler = node_id;
		am_handler = true;
	}
	else if (shared_state->bdr_failover_handler == node_id)
	{
		am_handler = true;
	}

	LWLockRelease(shared_state->lock);

	PG_RETURN_BOOL(am_handler);
}


Datum
unset_bdr_failover_handler(PG_FUNCTION_ARGS)
{
	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);
	shared_state->bdr_failover_handler = UNKNOWN_NODE_ID;
	LWLockRelease(shared_state->lock);

	PG_RETURN_VOID();
}
