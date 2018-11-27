/*
 * controldata.h
 * Copyright (c) 2ndQuadrant, 2010-2018
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */

#ifndef _CONTROLDATA_H_
#define _CONTROLDATA_H_

#include "postgres_fe.h"
#include "catalog/pg_control.h"

#define MAX_VERSION_STRING 24
/*
 * A simplified representation of pg_control containing only those fields
 * required by repmgr.
 */
typedef struct
{
	bool		control_file_processed;
	uint64		system_identifier;
	DBState		state;
	XLogRecPtr	checkPoint;
	uint32		data_checksum_version;
} ControlFileInfo;



/* Same for 9.3, 9.4 */
typedef struct CheckPoint93
{
	XLogRecPtr	redo;			/* next RecPtr available when we began to
								 * create CheckPoint (i.e. REDO start point) */
	TimeLineID	ThisTimeLineID; /* current TLI */
	TimeLineID	PrevTimeLineID; /* previous TLI, if this record begins a new
								 * timeline (equals ThisTimeLineID otherwise) */
	bool		fullPageWrites; /* current full_page_writes */
	uint32		nextXidEpoch;	/* higher-order bits of nextXid */
	TransactionId nextXid;		/* next free XID */
	Oid			nextOid;		/* next free OID */
	MultiXactId nextMulti;		/* next free MultiXactId */
	MultiXactOffset nextMultiOffset;	/* next free MultiXact offset */
	TransactionId oldestXid;	/* cluster-wide minimum datfrozenxid */
	Oid			oldestXidDB;	/* database with minimum datfrozenxid */
	MultiXactId oldestMulti;	/* cluster-wide minimum datminmxid */
	Oid			oldestMultiDB;	/* database with minimum datminmxid */
	pg_time_t	time;			/* time stamp of checkpoint */

	TransactionId oldestActiveXid;
} CheckPoint93;


/* Same for 9.5, 9.6, 10, HEAD */
typedef struct CheckPoint95
{
	XLogRecPtr	redo;			/* next RecPtr available when we began to
								 * create CheckPoint (i.e. REDO start point) */
	TimeLineID	ThisTimeLineID; /* current TLI */
	TimeLineID	PrevTimeLineID; /* previous TLI, if this record begins a new
								 * timeline (equals ThisTimeLineID otherwise) */
	bool		fullPageWrites; /* current full_page_writes */
	uint32		nextXidEpoch;	/* higher-order bits of nextXid */
	TransactionId nextXid;		/* next free XID */
	Oid			nextOid;		/* next free OID */
	MultiXactId nextMulti;		/* next free MultiXactId */
	MultiXactOffset nextMultiOffset;	/* next free MultiXact offset */
	TransactionId oldestXid;	/* cluster-wide minimum datfrozenxid */
	Oid			oldestXidDB;	/* database with minimum datfrozenxid */
	MultiXactId oldestMulti;	/* cluster-wide minimum datminmxid */
	Oid			oldestMultiDB;	/* database with minimum datminmxid */
	pg_time_t	time;			/* time stamp of checkpoint */
	TransactionId oldestCommitTsXid;	/* oldest Xid with valid commit
										 * timestamp */
	TransactionId newestCommitTsXid;	/* newest Xid with valid commit
										 * timestamp */

	TransactionId oldestActiveXid;
} CheckPoint95;


typedef struct ControlFileData93
{
	uint64		system_identifier;

	uint32		pg_control_version;		/* PG_CONTROL_VERSION */
	uint32		catalog_version_no;		/* see catversion.h */

	DBState		state;			/* see enum above */
	pg_time_t	time;			/* time stamp of last pg_control update */
	XLogRecPtr	checkPoint;		/* last check point record ptr */
	XLogRecPtr	prevCheckPoint; /* previous check point record ptr */

	CheckPoint93	checkPointCopy; /* copy of last check point record */

	XLogRecPtr	unloggedLSN;	/* current fake LSN value, for unlogged rels */

	XLogRecPtr	minRecoveryPoint;
	TimeLineID	minRecoveryPointTLI;
	XLogRecPtr	backupStartPoint;
	XLogRecPtr	backupEndPoint;
	bool		backupEndRequired;

	int			wal_level;
	int			MaxConnections;
	int			max_prepared_xacts;
	int			max_locks_per_xact;

	uint32		maxAlign;		/* alignment requirement for tuples */
	double		floatFormat;	/* constant 1234567.0 */

	uint32		blcksz;			/* data block size for this DB */
	uint32		relseg_size;	/* blocks per segment of large relation */

	uint32		xlog_blcksz;	/* block size within WAL files */
	uint32		xlog_seg_size;	/* size of each WAL segment */

	uint32		nameDataLen;	/* catalog name field width */
	uint32		indexMaxKeys;	/* max number of columns in an index */

	uint32		toast_max_chunk_size;	/* chunk size in TOAST tables */

	/* flag indicating internal format of timestamp, interval, time */
	bool		enableIntTimes; /* int64 storage enabled? */

	/* flags indicating pass-by-value status of various types */
	bool		float4ByVal;	/* float4 pass-by-value? */
	bool		float8ByVal;	/* float8, int8, etc pass-by-value? */

	/* Are data pages protected by checksums? Zero if no checksum version */
	uint32		data_checksum_version;

} ControlFileData93;


/*
 * Following field added since 9.3:
 *
 * 	int			max_worker_processes;
 */

typedef struct ControlFileData94
{
	uint64		system_identifier;

	uint32		pg_control_version;		/* PG_CONTROL_VERSION */
	uint32		catalog_version_no;		/* see catversion.h */

	DBState		state;			/* see enum above */
	pg_time_t	time;			/* time stamp of last pg_control update */
	XLogRecPtr	checkPoint;		/* last check point record ptr */
	XLogRecPtr	prevCheckPoint; /* previous check point record ptr */

	CheckPoint93	checkPointCopy; /* copy of last check point record */

	XLogRecPtr	unloggedLSN;	/* current fake LSN value, for unlogged rels */

	XLogRecPtr	minRecoveryPoint;
	TimeLineID	minRecoveryPointTLI;
	XLogRecPtr	backupStartPoint;
	XLogRecPtr	backupEndPoint;
	bool		backupEndRequired;

	int			wal_level;
	bool		wal_log_hints;
	int			MaxConnections;
	int			max_worker_processes;
	int			max_prepared_xacts;
	int			max_locks_per_xact;

	uint32		maxAlign;		/* alignment requirement for tuples */
	double		floatFormat;	/* constant 1234567.0 */

	uint32		blcksz;			/* data block size for this DB */
	uint32		relseg_size;	/* blocks per segment of large relation */

	uint32		xlog_blcksz;	/* block size within WAL files */
	uint32		xlog_seg_size;	/* size of each WAL segment */

	uint32		nameDataLen;	/* catalog name field width */
	uint32		indexMaxKeys;	/* max number of columns in an index */

	uint32		toast_max_chunk_size;	/* chunk size in TOAST tables */
	uint32		loblksize;		/* chunk size in pg_largeobject */

	bool		enableIntTimes; /* int64 storage enabled? */

	bool		float4ByVal;	/* float4 pass-by-value? */
	bool		float8ByVal;	/* float8, int8, etc pass-by-value? */

	/* Are data pages protected by checksums? Zero if no checksum version */
	uint32		data_checksum_version;

} ControlFileData94;



/*
 * Following field added since 9.4:
 *
 *	bool		track_commit_timestamp;
 *
 * Unchanged in 9.6
 *
 * In 10, following field appended *after* "data_checksum_version":
 *
 *	char		mock_authentication_nonce[MOCK_AUTH_NONCE_LEN];
 *
 * (but we don't care about that)
 */

typedef struct ControlFileData95
{
	uint64		system_identifier;

	uint32		pg_control_version;		/* PG_CONTROL_VERSION */
	uint32		catalog_version_no;		/* see catversion.h */

	DBState		state;			/* see enum above */
	pg_time_t	time;			/* time stamp of last pg_control update */
	XLogRecPtr	checkPoint;		/* last check point record ptr */
	XLogRecPtr	prevCheckPoint; /* previous check point record ptr */

	CheckPoint95	checkPointCopy; /* copy of last check point record */

	XLogRecPtr	unloggedLSN;	/* current fake LSN value, for unlogged rels */

	XLogRecPtr	minRecoveryPoint;
	TimeLineID	minRecoveryPointTLI;
	XLogRecPtr	backupStartPoint;
	XLogRecPtr	backupEndPoint;
	bool		backupEndRequired;

	int			wal_level;
	bool		wal_log_hints;
	int			MaxConnections;
	int			max_worker_processes;
	int			max_prepared_xacts;
	int			max_locks_per_xact;
	bool		track_commit_timestamp;

	uint32		maxAlign;		/* alignment requirement for tuples */
	double		floatFormat;	/* constant 1234567.0 */

	uint32		blcksz;			/* data block size for this DB */
	uint32		relseg_size;	/* blocks per segment of large relation */

	uint32		xlog_blcksz;	/* block size within WAL files */
	uint32		xlog_seg_size;	/* size of each WAL segment */

	uint32		nameDataLen;	/* catalog name field width */
	uint32		indexMaxKeys;	/* max number of columns in an index */

	uint32		toast_max_chunk_size;	/* chunk size in TOAST tables */
	uint32		loblksize;		/* chunk size in pg_largeobject */

	bool		enableIntTimes; /* int64 storage enabled? */

	bool		float4ByVal;	/* float4 pass-by-value? */
	bool		float8ByVal;	/* float8, int8, etc pass-by-value? */

	uint32		data_checksum_version;

} ControlFileData95;

/*
 * Following field removed in 11:
 *
 *  XLogRecPtr	prevCheckPoint;
 *
 * In 10, following field appended *after* "data_checksum_version":
 *
 * 	char		mock_authentication_nonce[MOCK_AUTH_NONCE_LEN];
 *
 * (but we don't care about that)
 */

typedef struct ControlFileData11
{
	uint64		system_identifier;

	uint32		pg_control_version;		/* PG_CONTROL_VERSION */
	uint32		catalog_version_no;		/* see catversion.h */

	DBState		state;			/* see enum above */
	pg_time_t	time;			/* time stamp of last pg_control update */
	XLogRecPtr	checkPoint;		/* last check point record ptr */

	CheckPoint95	checkPointCopy; /* copy of last check point record */

	XLogRecPtr	unloggedLSN;	/* current fake LSN value, for unlogged rels */

	XLogRecPtr	minRecoveryPoint;
	TimeLineID	minRecoveryPointTLI;
	XLogRecPtr	backupStartPoint;
	XLogRecPtr	backupEndPoint;
	bool		backupEndRequired;

	int			wal_level;
	bool		wal_log_hints;
	int			MaxConnections;
	int			max_worker_processes;
	int			max_prepared_xacts;
	int			max_locks_per_xact;
	bool		track_commit_timestamp;

	uint32		maxAlign;		/* alignment requirement for tuples */
	double		floatFormat;	/* constant 1234567.0 */

	uint32		blcksz;			/* data block size for this DB */
	uint32		relseg_size;	/* blocks per segment of large relation */

	uint32		xlog_blcksz;	/* block size within WAL files */
	uint32		xlog_seg_size;	/* size of each WAL segment */

	uint32		nameDataLen;	/* catalog name field width */
	uint32		indexMaxKeys;	/* max number of columns in an index */

	uint32		toast_max_chunk_size;	/* chunk size in TOAST tables */
	uint32		loblksize;		/* chunk size in pg_largeobject */

	bool		enableIntTimes; /* int64 storage enabled? */

	bool		float4ByVal;	/* float4 pass-by-value? */
	bool		float8ByVal;	/* float8, int8, etc pass-by-value? */

	uint32		data_checksum_version;

} ControlFileData11;


extern int get_pg_version(const char *data_directory, char *version_string);
extern DBState get_db_state(const char *data_directory);
extern const char *describe_db_state(DBState state);
extern int	get_data_checksum_version(const char *data_directory);
extern uint64 get_system_identifier(const char *data_directory);
extern XLogRecPtr get_latest_checkpoint_location(const char *data_directory);

#endif							/* _CONTROLDATA_H_ */
