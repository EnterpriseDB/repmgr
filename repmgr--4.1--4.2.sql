-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION repmgr" to load this file. \quit

CREATE FUNCTION get_repmgrd_pid()
  RETURNS INT
  AS 'MODULE_PATHNAME', 'get_repmgrd_pid'
  LANGUAGE C STRICT;

CREATE FUNCTION get_repmgrd_pidfile()
  RETURNS TEXT
  AS 'MODULE_PATHNAME', 'get_repmgrd_pidfile'
  LANGUAGE C STRICT;

CREATE FUNCTION set_repmgrd_pid(INT, TEXT)
  RETURNS VOID
  AS 'MODULE_PATHNAME', 'set_repmgrd_pid'
  LANGUAGE C STRICT;

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
