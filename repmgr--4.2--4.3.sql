-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION repmgr" to load this file. \quit

CREATE FUNCTION set_upstream_last_seen()
  RETURNS VOID
  AS 'MODULE_PATHNAME', 'set_upstream_last_seen'
  LANGUAGE C STRICT;

CREATE FUNCTION get_upstream_last_seen()
  RETURNS INT
  AS 'MODULE_PATHNAME', 'get_upstream_last_seen'
  LANGUAGE C STRICT;

CREATE FUNCTION get_wal_receiver_pid()
  RETURNS INT
  AS 'MODULE_PATHNAME', 'get_wal_receiver_pid'
  LANGUAGE C STRICT;
