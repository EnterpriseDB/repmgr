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

#include "voting.h"

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
		/* First time through ... */
#if (PG_VERSION_NUM >= 90600)
		shared_state->lock = &(GetNamedLWLockTranche(TRANCHE_NAME))->lock;
#else
		shared_state->lock = LWLockAssign();
#endif

		shared_state->voting_status = VS_NO_VOTE;
	}

	LWLockRelease(AddinShmemInitLock);
}


Datum
request_vote(PG_FUNCTION_ARGS)
{
	uint32 node_id = PG_GETARG_INT32(0);

	elog(INFO, "id is %i", node_id);

	PG_RETURN_BOOL(true);
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
