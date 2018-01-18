/*
 * repmgr.c - repmgr extension
 *
 * Copyright (c) 2ndQuadrant, 2010-2018
 *
 * This is the actual extension code; see repmgr-client.c for the code which
 * generates the repmgr binary
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

#if (PG_VERSION_NUM >= 90400)
#include "utils/pg_lsn.h"
#endif

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

typedef enum
{
	LEADER_NODE,
	FOLLOWER_NODE,
	CANDIDATE_NODE
} NodeState;

typedef struct repmgrdSharedState
{
	LWLockId	lock;			/* protects search/modification */
	TimestampTz last_updated;
	int			local_node_id;
	/* streaming failover */
	NodeVotingStatus voting_status;
	int			current_electoral_term;
	int			candidate_node_id;
	bool		follow_new_primary;
	/* BDR failover */
	int			bdr_failover_handler;
} repmgrdSharedState;

static repmgrdSharedState *shared_state = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;


void		_PG_init(void);
void		_PG_fini(void);

static void repmgr_shmem_startup(void);

Datum		set_local_node_id(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(set_local_node_id);

Datum		get_local_node_id(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(get_local_node_id);

Datum		standby_set_last_updated(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(standby_set_last_updated);

Datum		standby_get_last_updated(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(standby_get_last_updated);

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
	elog(DEBUG1, "repmgr init");

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

		shared_state->local_node_id = UNKNOWN_NODE_ID;
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

Datum
set_local_node_id(PG_FUNCTION_ARGS)
{
	int			local_node_id = UNKNOWN_NODE_ID;

	if (!shared_state)
		PG_RETURN_NULL();

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	local_node_id = PG_GETARG_INT32(0);

	LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);

	/* only set local_node_id once, as it should never change */
	if (shared_state->local_node_id == UNKNOWN_NODE_ID)
	{
		shared_state->local_node_id = local_node_id;
	}

	LWLockRelease(shared_state->lock);

	PG_RETURN_VOID();
}


Datum
get_local_node_id(PG_FUNCTION_ARGS)
{
	int			local_node_id = UNKNOWN_NODE_ID;

	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);
	local_node_id = shared_state->local_node_id;
	LWLockRelease(shared_state->lock);

	PG_RETURN_INT32(local_node_id);
}


/* update and return last updated with current timestamp */
Datum
standby_set_last_updated(PG_FUNCTION_ARGS)
{
	TimestampTz last_updated = GetCurrentTimestamp();

	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);
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

	LWLockAcquire(shared_state->lock, LW_SHARED);
	last_updated = shared_state->last_updated;
	LWLockRelease(shared_state->lock);

	PG_RETURN_TIMESTAMPTZ(last_updated);
}




/* ===================*/
/* failover functions */
/* ===================*/


Datum
notify_follow_primary(PG_FUNCTION_ARGS)
{
	int			primary_node_id = UNKNOWN_NODE_ID;

	if (!shared_state)
		PG_RETURN_NULL();

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	primary_node_id = PG_GETARG_INT32(0);

	LWLockAcquire(shared_state->lock, LW_SHARED);

	/* only do something if local_node_id is initialised */
	if (shared_state->local_node_id != UNKNOWN_NODE_ID)
	{
		elog(INFO, "node %i received notification to follow node %i",
			 shared_state->local_node_id,
			 primary_node_id);

		LWLockRelease(shared_state->lock);
		LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);
		/* Explicitly set the primary node id */
		shared_state->candidate_node_id = primary_node_id;
		shared_state->follow_new_primary = true;
	}

	LWLockRelease(shared_state->lock);

	PG_RETURN_VOID();
}


Datum
get_new_primary(PG_FUNCTION_ARGS)
{
	int			new_primary_node_id = UNKNOWN_NODE_ID;

	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);

	if (shared_state->follow_new_primary == true)
		new_primary_node_id = shared_state->candidate_node_id;

	LWLockRelease(shared_state->lock);

	if (new_primary_node_id == UNKNOWN_NODE_ID)
		PG_RETURN_NULL();

	PG_RETURN_INT32(new_primary_node_id);
}


Datum
reset_voting_status(PG_FUNCTION_ARGS)
{
	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);

	/* only do something if local_node_id is initialised */
	if (shared_state->local_node_id != UNKNOWN_NODE_ID)
	{
		LWLockRelease(shared_state->lock);
		LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);

		shared_state->voting_status = VS_NO_VOTE;
		shared_state->candidate_node_id = UNKNOWN_NODE_ID;
		shared_state->follow_new_primary = false;
	}

	LWLockRelease(shared_state->lock);

	PG_RETURN_VOID();
}


Datum
am_bdr_failover_handler(PG_FUNCTION_ARGS)
{
	int			node_id = UNKNOWN_NODE_ID;
	bool		am_handler = false;

	if (!shared_state)
		PG_RETURN_NULL();

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	node_id = PG_GETARG_INT32(0);

	LWLockAcquire(shared_state->lock, LW_SHARED);

	if (shared_state->bdr_failover_handler == UNKNOWN_NODE_ID)
	{
		LWLockRelease(shared_state->lock);
		LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);
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

	/* only do something if local_node_id is initialised */
	if (shared_state->local_node_id != UNKNOWN_NODE_ID)
	{
		LWLockRelease(shared_state->lock);
		LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);

		shared_state->bdr_failover_handler = UNKNOWN_NODE_ID;

		LWLockRelease(shared_state->lock);
	}

	PG_RETURN_VOID();
}
