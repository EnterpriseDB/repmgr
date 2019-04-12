-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION repmgr" to load this file. \quit

DROP FUNCTION set_upstream_last_seen();

CREATE FUNCTION set_upstream_last_seen(INT)
  RETURNS VOID
  AS 'MODULE_PATHNAME', 'set_upstream_last_seen'
  LANGUAGE C STRICT;

CREATE FUNCTION get_upstream_node_id()
  RETURNS INT
  AS 'MODULE_PATHNAME', 'get_upstream_node_id'
  LANGUAGE C STRICT;

CREATE FUNCTION set_upstream_node_id(INT)
  RETURNS VOID
  AS 'MODULE_PATHNAME', 'set_upstream_node_id'
  LANGUAGE C STRICT;
