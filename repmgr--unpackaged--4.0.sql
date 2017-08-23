-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION repmgr" to load this file. \quit

-- extract the current schema name
-- NOTE: this assumes there will be only one schema matching 'repmgr_%';
-- user is responsible for ensuring this is the case

CREATE TEMPORARY TABLE repmgr_old_schema (old_schema TEXT);
INSERT INTO repmgr_old_schema (old_schema)
SELECT nspname AS old_schema
  FROM pg_catalog.pg_namespace
 WHERE nspname LIKE 'repmgr_%'
 LIMIT 1;

-- move old objects into new schema
DO $repmgr$
DECLARE
  schema TEXT;
BEGIN
  SELECT old_schema FROM repmgr_old_schema
    INTO schema;
  EXECUTE format('ALTER TABLE %I.repl_nodes SET SCHEMA repmgr', schema);
  EXECUTE format('ALTER TABLE %I.repl_events SET SCHEMA repmgr', schema);
  EXECUTE format('ALTER TABLE %I.repl_monitor SET SCHEMA repmgr', schema);
  EXECUTE format('ALTER VIEW %I.repl_show_nodes SET SCHEMA repmgr', schema);
  EXECUTE format('ALTER VIEW %I.repl_status SET SCHEMA repmgr', schema);
END$repmgr$;

-- convert "repmgr_$cluster.repl_nodes" to "repmgr.nodes"
CREATE TABLE nodes (
  node_id          INTEGER     PRIMARY KEY,
  upstream_node_id INTEGER     NULL REFERENCES repmgr.nodes (node_id) DEFERRABLE,
  active           BOOLEAN     NOT NULL DEFAULT TRUE,
  node_name        TEXT        NOT NULL,
  type             TEXT        NOT NULL CHECK (type IN('primary','standby','bdr')),
  location         TEXT        NOT NULL DEFAULT 'default',
  priority         INT         NOT NULL DEFAULT 100,
  conninfo         TEXT        NOT NULL,
  repluser         VARCHAR(63) NOT NULL,
  slot_name        TEXT        NULL,
  config_file      TEXT        NOT NULL
);

INSERT INTO nodes
  (node_id, upstream_node_id, active, node_name, type, location, priority, conninfo, repluser, slot_name, config_file)
SELECT id, upstream_node_id, active, name,
       CASE WHEN type = 'master' THEN 'primary' ELSE type END,
       'default', priority, conninfo, 'unknown', slot_name, 'unknown'
  FROM repl_nodes
 ORDER BY id;


-- convert "repmgr_$cluster.repl_event" to "event"

ALTER TABLE repl_events RENAME TO events;

-- convert "repmgr_$cluster.repl_monitor" to "monitoring_history"

CREATE TABLE monitoring_history (
  primary_node_id                INTEGER NOT NULL,
  standby_node_id                INTEGER NOT NULL,
  last_monitor_time              TIMESTAMP WITH TIME ZONE NOT NULL,
  last_apply_time                TIMESTAMP WITH TIME ZONE,
  last_wal_primary_location      PG_LSN NOT NULL,
  last_wal_standby_location      PG_LSN,
  replication_lag                BIGINT NOT NULL,
  apply_lag                      BIGINT NOT NULL
);

INSERT INTO monitoring_history
  (primary_node_id, standby_node_id, last_monitor_time,  last_apply_time, last_wal_primary_location, last_wal_standby_location, replication_lag, apply_lag)
SELECT primary_node_id, standby_node_id, last_monitor_time,  last_apply_time, last_wal_primary_location, last_wal_standby_location, replication_lag, apply_lag
  FROM repl_monitor;

CREATE INDEX idx_monitoring_history_time
          ON monitoring_history (last_monitor_time, standby_node_id);


-- recreate VIEW

DROP VIEW IF EXISTS repl_show_nodes;

CREATE VIEW show_nodes AS
   SELECT n.node_id,
          n.node_name,
          n.active,
          n.upstream_node_id,
          un.node_name AS upstream_node_name,
          n.type,
          n.priority,
          n.conninfo
     FROM nodes n
LEFT JOIN nodes un
       ON un.node_id = n.upstream_node_id;

DROP VIEW IF EXISTS repl_status;

-- CREATE VIEW status ... ;

/* drop old tables */
DROP TABLE repl_nodes;
DROP TABLE repl_monitor;


/* repmgrd functions */

CREATE FUNCTION request_vote(INT,INT)
  RETURNS pg_lsn
  AS '$libdir/repmgr', 'request_vote'
  LANGUAGE C STRICT;

CREATE FUNCTION get_voting_status()
  RETURNS INT
  AS '$libdir/repmgr', 'get_voting_status'
  LANGUAGE C STRICT;

CREATE FUNCTION set_voting_status_initiated()
  RETURNS INT
  AS '$libdir/repmgr', 'set_voting_status_initiated'
  LANGUAGE C STRICT;

CREATE FUNCTION other_node_is_candidate(INT, INT)
  RETURNS BOOL
  AS '$libdir/repmgr', 'other_node_is_candidate'
  LANGUAGE C STRICT;

CREATE FUNCTION notify_follow_primary(INT)
  RETURNS VOID
  AS '$libdir/repmgr', 'notify_follow_primary'
  LANGUAGE C STRICT;

CREATE FUNCTION get_new_primary()
  RETURNS INT
  AS '$libdir/repmgr', 'get_new_primary'
  LANGUAGE C STRICT;

CREATE FUNCTION reset_voting_status()
  RETURNS VOID
  AS '$libdir/repmgr', 'reset_voting_status'
  LANGUAGE C STRICT;


CREATE FUNCTION am_bdr_failover_handler(INT)
  RETURNS BOOL
  AS '$libdir/repmgr', 'am_bdr_failover_handler'
  LANGUAGE C STRICT;


CREATE FUNCTION unset_bdr_failover_handler()
  RETURNS VOID
  AS '$libdir/repmgr', 'unset_bdr_failover_handler'
  LANGUAGE C STRICT;

-- remove temporary table
DROP TABLE repmgr_old_schema;
