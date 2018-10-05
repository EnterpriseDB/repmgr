-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION repmgr" to load this file. \quit

CREATE FUNCTION repmgrd_is_running()
  RETURNS BOOL
  AS 'MODULE_PATHNAME', 'repmgrd_is_running'
  LANGUAGE C STRICT;

CREATE FUNCTION repmgrd_pause(BOOL)
  RETURNS VOID
  AS 'MODULE_PATHNAME', 'repmgrd_pause'
  LANGUAGE C STRICT;

CREATE FUNCTION repmgrd_is_paused()
  RETURNS BOOL
  AS 'MODULE_PATHNAME', 'repmgrd_is_paused'
  LANGUAGE C STRICT;
