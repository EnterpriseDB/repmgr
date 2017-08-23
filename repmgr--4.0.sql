-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION repmgr" to load this file. \quit

CREATE TABLE nodes (
  node_id          INTEGER     PRIMARY KEY,
  upstream_node_id INTEGER     NULL REFERENCES nodes (node_id) DEFERRABLE,
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

CREATE TABLE events (
  node_id          INTEGER NOT NULL,
  event            TEXT NOT NULL,
  successful       BOOLEAN NOT NULL DEFAULT TRUE,
  event_timestamp  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  details          TEXT NULL
);

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
CREATE INDEX idx_monitoring_history_time
          ON monitoring_history (last_monitor_time, standby_node_id);

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
