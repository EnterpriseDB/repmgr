/*
 * repmgr_funcs.c
 * Copyright (c) 2ndQuadrant, 2010
 *
 * Shared memory state management and some backend functions in SQL
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

/* same definition as the one in xlog_internal.h */
#define MAXFNAMELEN		64

PG_MODULE_MAGIC;

/*
 * Global shared state
 */
typedef struct repmgrSharedState
{
	LWLockId	lock;			/* protects search/modification */
	char		location[MAXFNAMELEN];	/* last known xlog location */
	TimestampTz last_updated;
}	repmgrSharedState;

/* Links to shared memory state */
static repmgrSharedState *shared_state = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

void		_PG_init(void);
void		_PG_fini(void);

static void repmgr_shmem_startup(void);
static Size repmgr_memsize(void);

static bool repmgr_set_standby_location(char *locationstr);

Datum		repmgr_update_standby_location(PG_FUNCTION_ARGS);
Datum		repmgr_get_last_standby_location(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(repmgr_update_standby_location);
PG_FUNCTION_INFO_V1(repmgr_get_last_standby_location);

Datum		repmgr_update_last_updated(PG_FUNCTION_ARGS);
Datum		repmgr_get_last_updated(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(repmgr_update_last_updated);
PG_FUNCTION_INFO_V1(repmgr_get_last_updated);



/*
 * Module load callback
 */
void
_PG_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.  (We don't throw error here because it seems useful to
	 * allow the repmgr functions to be created even when the module isn't
	 * active.	The functions must protect themselves against being called
	 * then, however.)
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in repmgr_shmem_startup().
	 */
	RequestAddinShmemSpace(repmgr_memsize());

#if (PG_VERSION_NUM >= 90600)
	RequestNamedLWLockTranche("repmgr", 1);
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
	/* Uninstall hooks. */
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

	shared_state = ShmemInitStruct("repmgr shared state",
								   sizeof(repmgrSharedState),
								   &found);

	if (!found)
	{
		/* First time through ... */
#if (PG_VERSION_NUM >= 90600)
		shared_state->lock = &(GetNamedLWLockTranche("repmgr"))->lock;
#else
		shared_state->lock = LWLockAssign();
#endif
		snprintf(shared_state->location,
				 sizeof(shared_state->location), "%X/%X", 0, 0);
	}

	LWLockRelease(AddinShmemInitLock);
}


/*
 * Estimate shared memory space needed.
 */
static Size
repmgr_memsize(void)
{
	return MAXALIGN(sizeof(repmgrSharedState));
}


static bool
repmgr_set_standby_location(char *locationstr)
{
	/* Safety check... */
	if (!shared_state)
		return false;

	LWLockAcquire(shared_state->lock, LW_EXCLUSIVE);
	strncpy(shared_state->location, locationstr, MAXFNAMELEN);
	LWLockRelease(shared_state->lock);

	return true;
}


/* SQL Functions */

/* Read last xlog location reported by this standby from shared memory */
Datum
repmgr_get_last_standby_location(PG_FUNCTION_ARGS)
{
	char		location[MAXFNAMELEN];

	/* Safety check... */
	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);
	strncpy(location, shared_state->location, MAXFNAMELEN);
	LWLockRelease(shared_state->lock);

	PG_RETURN_TEXT_P(cstring_to_text(location));
}


/* Set update last xlog location reported by this standby to shared memory */
Datum
repmgr_update_standby_location(PG_FUNCTION_ARGS)
{
	text	   *location = PG_GETARG_TEXT_P(0);
	char	   *locationstr;

	/* Safety check... */
	if (!shared_state)
		PG_RETURN_BOOL(false);

	locationstr = text_to_cstring(location);

	PG_RETURN_BOOL(repmgr_set_standby_location(locationstr));
}

/* update and return last updated with current timestamp */
Datum
repmgr_update_last_updated(PG_FUNCTION_ARGS)
{
	TimestampTz last_updated = GetCurrentTimestamp();

	/* Safety check... */
	if (!shared_state)
		PG_RETURN_NULL();

	LWLockAcquire(shared_state->lock, LW_SHARED);
	shared_state->last_updated = last_updated;
	LWLockRelease(shared_state->lock);

	PG_RETURN_TIMESTAMPTZ(last_updated);
}


/* get last updated timestamp */
Datum
repmgr_get_last_updated(PG_FUNCTION_ARGS)
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


