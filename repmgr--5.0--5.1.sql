-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION repmgr" to load this file. \quit

DROP FUNCTION am_bdr_failover_handler(INT);
DROP FUNCTION unset_bdr_failover_handler();
