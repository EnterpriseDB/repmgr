/*
 * repmgr.c - repmgr extension
 *
 * Copyright (c) 2ndQuadrant, 2010-2020
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
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"

#include "utils/timestamp.h"

#include "lib/stringinfo.h"
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "pgstat.h"

#include "voting.h"

#define UNKNOWN_NODE_ID		-1
#define ELECTION_RERUN_NOTIFICATION -2
#define UNKNOWN_PID			-1

#define TRANCHE_NAME "repmgrd"
#define REPMGRD_STATE_FILE PGSTAT_STAT_PERMANENT_DIRECTORY "/repmgrd_state.txt"
#define REPMGRD_STATE_FILE_BUF_SIZE 128

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
	int			repmgrd_pid;
	char		repmgrd_pidfile[MAXPGPATH];
	bool		repmgrd_paused;
	/* streaming failover */
	int			upstream_node_id;
	TimestampTz upstream_last_seen;
	NodeVotingStatus voting_status;
	int			current_electoral_term;
	int			candidate_node_id;
	bool		follow_new_primary;
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

Datum		set_upstream_last_seen(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(set_upstream_last_seen);

Datum		get_upstream_last_seen(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(get_upstream_last_seen);

Datum		get_upstream_node_id(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(get_upstream_node_id);

Datum		set_upstream_node_id(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(set_upstream_node_id);

Datum		notify_follow_primary(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(notify_follow_primary);

Datum		get_new_primary(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(get_new_primary);

Datum		reset_voting_status(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(reset_voting_status);

Datum		set_repmgrd_pid(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(set_repmgrd_pid);

Datum		get_repmgrd_pid(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(get_repmgrd_pid);

Datum		get_repmgrd_pidfile(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(get_repmgrd_pidfile);

Datum		repmgrd_is_running(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(repmgrd_is_running);

Datum		repmgrd_pause(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(repmgrd_pause);

Datum		repmgrd_is_paused(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(repmgrd_is_paused);

Datum		get_wal_receiver_pid(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(get_wal_receiver_pid);


/*
 * Module load callback
 */
void
_PG_init(void)
{
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
		shared_state->repmgrd_pid = UNKNOWN_PID;
		memset(shared_state->repmgrd_pidfile, 0, MAXPGPATH);
		shared_state->repmgrd_paused = false;
		shared_state->current_electoral_term = 0;
		shared_state->upstream_node_id = UNKNOWN_NODE_ID;
		/* arbitrary "magic" date to indicate this field hasn't been updated */
		shared_state->upstream_last_seen = POSTGRES_EPOCH_JDATE;
		shared_state->voting_status = VS_NO_VOTE;
		shared_state->candidate_node_id = UNKNOWN_NODE_ID;
		shared_state->follow_new_primary = false;
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
	int			stored_node_id = UNKNOWN_NODE_ID;
	int			paused = -1;

	if (!shared_state)
		PG_RETURN_NULL();

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	local_node_id = PG_GETARG_INT32(0);

	/* read state file and if exists/valid, update "repmgrd_paused" */
	{
		FILE	   *file = NULL;

		file = AllocateFile(REPMGRD_STATE_FILE, PG_BINARY_R);

		if (file != NULL)
		{
			int			buffer_size = REPMGRD_STATE_FILE_BUF_SIZE;
			char		buffer[REPMGRD_STATE_FILE_BUF_SIZE];

			if (fgets(buffer, buffer_size, file) != NULL)
			{
				if (sscanf(buffer, "%i:%i", &stored_node_id, &paused) != 2)
				{
					elog(WARNING, "unable to parse repmgrd state file");
				}
				else
				{
					elog(DEBUG1, "node_id: %i; paused: %i", stored_node_id, paused);
				}
			}

			FreeFile(file);
		}

	}

	LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);

	/* only set local_node_id once, as it should never change */
	if (shared_state->local_node_id == UNKNOWN_NODE_ID)
	{
		shared_state->local_node_id = local_node_id;
	}

	/* only update if state file valid */
	if (stored_node_id == shared_state->local_node_id)
	{
		if (paused == 0)
		{
			shared_state->repmgrd_paused = false;
		}
		else if (paused == 1)
		{
			shared_state->repmgrd_paused = true;
		}
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


Datum
set_upstream_last_seen(PG_FUNCTION_ARGS)
{
	int			upstream_node_id = UNKNOWN_NODE_ID;

	if (!shared_state)
		PG_RETURN_VOID();

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	upstream_node_id = PG_GETARG_INT32(0);

	LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);

	shared_state->upstream_last_seen = GetCurrentTimestamp();
	shared_state->upstream_node_id = upstream_node_id;
	LWLockRelease(shared_state->lock);

	PG_RETURN_VOID();
}


Datum
get_upstream_last_seen(PG_FUNCTION_ARGS)
{
	long		secs;
	int			microsecs;
	TimestampTz last_seen;

	if (!shared_state)
		PG_RETURN_INT32(-1);

	LWLockAcquire(shared_state->lock, LW_SHARED);

	last_seen = shared_state->upstream_last_seen;

	LWLockRelease(shared_state->lock);

	/*
	 * "last_seen" is initialised with the PostgreSQL epoch as a
	 * "magic" value to indicate the field hasn't ever been updated
	 * by repmgrd. We return -1 instead, rather than imply that the
	 * primary was last seen at the turn of the century.
	 */
	if (last_seen == POSTGRES_EPOCH_JDATE)
		PG_RETURN_INT32(-1);


	TimestampDifference(last_seen, GetCurrentTimestamp(),
						&secs, &microsecs);

	/* let's hope repmgrd never runs for more than a century or so without seeing a primary */
	PG_RETURN_INT32((uint32)secs);
}


Datum
get_upstream_node_id(PG_FUNCTION_ARGS)
{
	int			upstream_node_id = UNKNOWN_NODE_ID;

	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);
	upstream_node_id = shared_state->upstream_node_id;
	LWLockRelease(shared_state->lock);

	PG_RETURN_INT32(upstream_node_id);
}

Datum
set_upstream_node_id(PG_FUNCTION_ARGS)
{
	int			upstream_node_id = UNKNOWN_NODE_ID;
	int			local_node_id = UNKNOWN_NODE_ID;

	if (!shared_state)
		PG_RETURN_NULL();

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	upstream_node_id = PG_GETARG_INT32(0);

	LWLockAcquire(shared_state->lock, LW_SHARED);
	local_node_id = shared_state->local_node_id;
	LWLockRelease(shared_state->lock);

	if (local_node_id == upstream_node_id)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("upstream node id cannot be the same as the local node id"))));

	LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);
	shared_state->upstream_node_id = upstream_node_id;
	LWLockRelease(shared_state->lock);

	PG_RETURN_VOID();
}


/* ===================*/
/* failover functions */
/* ===================*/


Datum
notify_follow_primary(PG_FUNCTION_ARGS)
{
	int			primary_node_id = UNKNOWN_NODE_ID;

	if (!shared_state)
		PG_RETURN_VOID();

	if (PG_ARGISNULL(0))
		PG_RETURN_VOID();

	primary_node_id = PG_GETARG_INT32(0);

	LWLockAcquire(shared_state->lock, LW_SHARED);

	/* only do something if local_node_id is initialised */
	if (shared_state->local_node_id != UNKNOWN_NODE_ID)
	{
		if (primary_node_id == ELECTION_RERUN_NOTIFICATION)
		{
			elog(INFO, "node %i received notification to rerun promotion candidate election",
				 shared_state->local_node_id);
		}
		else
		{
			elog(INFO, "node %i received notification to follow node %i",
				 shared_state->local_node_id,
				 primary_node_id);
		}

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
		PG_RETURN_INT32(UNKNOWN_NODE_ID);

	LWLockAcquire(shared_state->lock, LW_SHARED);

	if (shared_state->follow_new_primary == true)
		new_primary_node_id = shared_state->candidate_node_id;

	LWLockRelease(shared_state->lock);

	if (new_primary_node_id == UNKNOWN_NODE_ID)
		PG_RETURN_INT32(UNKNOWN_NODE_ID);

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


/*
 * Returns the repmgrd pid; or NULL if none set; or -1 if set but repmgrd
 * process not running (TODO!)
 */
Datum
get_repmgrd_pid(PG_FUNCTION_ARGS)
{
	int repmgrd_pid = UNKNOWN_PID;

	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);
	repmgrd_pid = shared_state->repmgrd_pid;
	LWLockRelease(shared_state->lock);

	PG_RETURN_INT32(repmgrd_pid);
}


/*
 * Returns the repmgrd pidfile
 */
Datum
get_repmgrd_pidfile(PG_FUNCTION_ARGS)
{
	char repmgrd_pidfile[MAXPGPATH];

	if (!shared_state)
		PG_RETURN_NULL();

	memset(repmgrd_pidfile, 0, MAXPGPATH);

	LWLockAcquire(shared_state->lock, LW_SHARED);
	strncpy(repmgrd_pidfile, shared_state->repmgrd_pidfile, MAXPGPATH);
	LWLockRelease(shared_state->lock);

	if (repmgrd_pidfile[0] == '\0')
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(cstring_to_text(repmgrd_pidfile));
}

Datum
set_repmgrd_pid(PG_FUNCTION_ARGS)
{
	int repmgrd_pid = UNKNOWN_PID;
	char *repmgrd_pidfile = NULL;

	if (!shared_state)
		PG_RETURN_VOID();

	if (PG_ARGISNULL(0))
	{
		repmgrd_pid = UNKNOWN_PID;
	}
	else
	{
		repmgrd_pid = PG_GETARG_INT32(0);
	}

	elog(DEBUG3, "set_repmgrd_pid(): provided pid is %i", repmgrd_pid);

	if (repmgrd_pid != UNKNOWN_PID && !PG_ARGISNULL(1))
	{
		repmgrd_pidfile = text_to_cstring(PG_GETARG_TEXT_PP(1));
		elog(INFO, "set_repmgrd_pid(): provided pidfile is %s", repmgrd_pidfile);
	}

	LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);

	shared_state->repmgrd_pid = repmgrd_pid;
	memset(shared_state->repmgrd_pidfile, 0, MAXPGPATH);

	if (repmgrd_pidfile != NULL)
	{
		strncpy(shared_state->repmgrd_pidfile, repmgrd_pidfile, MAXPGPATH);
	}

	LWLockRelease(shared_state->lock);
	PG_RETURN_VOID();
}


Datum
repmgrd_is_running(PG_FUNCTION_ARGS)
{
	int repmgrd_pid = UNKNOWN_PID;
	int kill_ret;

	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);
	repmgrd_pid = shared_state->repmgrd_pid;
	LWLockRelease(shared_state->lock);

	/* No PID registered - assume not running */
	if (repmgrd_pid == UNKNOWN_PID)
	{
		PG_RETURN_BOOL(false);
	}

	kill_ret = kill(repmgrd_pid, 0);

	if (kill_ret == 0)
	{
		PG_RETURN_BOOL(true);
	}

	PG_RETURN_BOOL(false);
}


Datum
repmgrd_pause(PG_FUNCTION_ARGS)
{
	bool		pause;
	FILE	   *file = NULL;
	StringInfoData buf;

	if (!shared_state)
		PG_RETURN_NULL();

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	pause = PG_GETARG_BOOL(0);

	LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);
	shared_state->repmgrd_paused = pause;
	LWLockRelease(shared_state->lock);

	/* write state to file */
	file = AllocateFile(REPMGRD_STATE_FILE, PG_BINARY_W);

	if (file == NULL)
	{
		elog(WARNING, "unable to allocate %s", REPMGRD_STATE_FILE);

		PG_RETURN_VOID();
	}

	elog(DEBUG1, "allocated");

	initStringInfo(&buf);

	LWLockAcquire(shared_state->lock, LW_SHARED);

	appendStringInfo(&buf, "%i:%i",
					 shared_state->local_node_id,
					 pause ? 1 : 0);
	LWLockRelease(shared_state->lock);

	if (fwrite(buf.data, strlen(buf.data) + 1, 1, file) != 1)
	{
		elog(WARNING, _("unable to write to file %s"), REPMGRD_STATE_FILE);
	}

	pfree(buf.data);

	FreeFile(file);

	PG_RETURN_VOID();
}


Datum
repmgrd_is_paused(PG_FUNCTION_ARGS)
{
	bool is_paused;

	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);
	is_paused = shared_state->repmgrd_paused;
	LWLockRelease(shared_state->lock);

	PG_RETURN_BOOL(is_paused);
}


Datum
get_wal_receiver_pid(PG_FUNCTION_ARGS)
{
	int wal_receiver_pid;

	if (!shared_state)
		PG_RETURN_NULL();

	wal_receiver_pid = WalRcv->pid;

	PG_RETURN_INT32(wal_receiver_pid);
}
