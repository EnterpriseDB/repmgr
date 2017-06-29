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

#define MAXFNAMELEN		64
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
	NodeState	node_state;
	NodeVotingStatus voting_status;
	int candidate_node_id;
}	repmgrdSharedState;

static repmgrdSharedState *shared_state = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;


void		_PG_init(void);
void		_PG_fini(void);

static void repmgr_shmem_startup(void);

Datum		request_vote(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(request_vote);

Datum		get_voting_status(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(get_voting_status);

Datum		set_voting_status_initiated(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(set_voting_status_initiated);

Datum other_node_is_candidate(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(other_node_is_candidate);
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

		shared_state->voting_status = VS_NO_VOTE;
		shared_state->candidate_node_id = UNKNOWN_NODE_ID;
	}

	LWLockRelease(AddinShmemInitLock);
}


Datum
request_vote(PG_FUNCTION_ARGS)
{
	/* node_id used for logging purposes */
	int requesting_node_id = PG_GETARG_INT32(0);
	XLogRecPtr requesting_node_last_lsn = PG_GETARG_LSN(1);
	StringInfoData	query;

	int		ret;
	bool	isnull;

	int lsn_diff;

	NodeVotingStatus voting_status;

	LWLockAcquire(shared_state->lock, LW_SHARED);
	voting_status = shared_state->voting_status;
	LWLockRelease(shared_state->lock);

	/* this node has initiated voting or already responded to another node */
	if (voting_status != VS_NO_VOTE)
		PG_RETURN_NULL();

	elog(INFO, "id is %i, lsn: %X/%X",
		 requesting_node_id,
		 (uint32) (requesting_node_last_lsn >> 32),
		 (uint32) requesting_node_last_lsn);

	SPI_connect();

	initStringInfo(&query);
	appendStringInfo(
		&query,
		"SELECT ('%X/%X'::pg_lsn - pg_catalog.pg_last_wal_receive_lsn()::pg_lsn)::INT",
		(uint32) (requesting_node_last_lsn >> 32),
		(uint32) requesting_node_last_lsn);

	elog(INFO, "query: %s", query.data);
	ret = SPI_execute(query.data, true, 0);

	// xxx handle errors
	lsn_diff = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
										   SPI_tuptable->tupdesc,
										   1, &isnull));

	elog(INFO, "XXX diff is %i",
		 lsn_diff);

   	SPI_finish();

	/* indicate this node has responded to a vote request */
	LWLockAcquire(shared_state->lock, LW_SHARED);
	shared_state->voting_status = VS_VOTE_REQUEST_RECEIVED;
	LWLockRelease(shared_state->lock);

	PG_RETURN_INT32(lsn_diff);
}



Datum
get_voting_status(PG_FUNCTION_ARGS)
{
	NodeVotingStatus voting_status;

	LWLockAcquire(shared_state->lock, LW_SHARED);
	voting_status = shared_state->voting_status;
	LWLockRelease(shared_state->lock);

	PG_RETURN_INT32(voting_status);
}

Datum
set_voting_status_initiated(PG_FUNCTION_ARGS)
{
	LWLockAcquire(shared_state->lock, LW_SHARED);
	shared_state->voting_status = VS_VOTE_INITIATED;
	LWLockRelease(shared_state->lock);

	PG_RETURN_VOID();
}

Datum
other_node_is_candidate(PG_FUNCTION_ARGS)
{
	int  requesting_node_id = PG_GETARG_INT32(0);

	LWLockAcquire(shared_state->lock, LW_SHARED);

	if (shared_state->candidate_node_id != UNKNOWN_NODE_ID)
	{
		elog(INFO, "node %i requesting candidature, but node %i already candidate",
				  requesting_node_id,
				  shared_state->candidate_node_id);
		PG_RETURN_BOOL(false);
	}

	shared_state->candidate_node_id = requesting_node_id;
	LWLockRelease(shared_state->lock);

	elog(INFO, "node %i is candidate", requesting_node_id);
	PG_RETURN_BOOL(true);
}
