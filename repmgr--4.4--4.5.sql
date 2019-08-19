-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION repmgr" to load this file. \quit

ALTER FUNCTION set_repmgrd_pid(INT, TEXT) RETURNS NULL ON NULL INPUT;

