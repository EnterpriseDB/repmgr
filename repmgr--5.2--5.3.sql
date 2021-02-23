-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION repmgr" to load this file. \quit

CREATE OR REPLACE FUNCTION set_local_node_id(INT)
  RETURNS VOID
  AS 'MODULE_PATHNAME', 'repmgr_set_local_node_id'
  LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION repmgr.get_local_node_id()
  RETURNS INT
  AS 'MODULE_PATHNAME', 'repmgr_get_local_node_id'
  LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION standby_set_last_updated()
  RETURNS TIMESTAMP WITH TIME ZONE
  AS 'MODULE_PATHNAME', 'repmgr_standby_set_last_updated'
  LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION standby_get_last_updated()
  RETURNS TIMESTAMP WITH TIME ZONE
  AS 'MODULE_PATHNAME', 'repmgr_standby_get_last_updated'
  LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION set_upstream_last_seen(INT)
  RETURNS VOID
  AS 'MODULE_PATHNAME', 'repmgr_set_upstream_last_seen'
  LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION get_upstream_last_seen()
  RETURNS INT
  AS 'MODULE_PATHNAME', 'repmgr_get_upstream_last_seen'
  LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION get_upstream_node_id()
  RETURNS INT
  AS 'MODULE_PATHNAME', 'repmgr_get_upstream_node_id'
  LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION set_upstream_node_id(INT)
  RETURNS VOID
  AS 'MODULE_PATHNAME', 'repmgr_set_upstream_node_id'
  LANGUAGE C STRICT;

/* failover functions */

CREATE OR REPLACE FUNCTION notify_follow_primary(INT)
  RETURNS VOID
  AS 'MODULE_PATHNAME', 'repmgr_notify_follow_primary'
  LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION get_new_primary()
  RETURNS INT
  AS 'MODULE_PATHNAME', 'repmgr_get_new_primary'
  LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION reset_voting_status()
  RETURNS VOID
  AS 'MODULE_PATHNAME', 'repmgr_reset_voting_status'
  LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION get_wal_receiver_pid()
  RETURNS INT
  AS 'MODULE_PATHNAME', 'repmgr_get_wal_receiver_pid'
  LANGUAGE C STRICT;
