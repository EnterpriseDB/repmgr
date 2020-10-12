-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION repmgr" to load this file. \quit

SELECT pg_catalog.pg_extension_config_dump('repmgr.nodes', '');
SELECT pg_catalog.pg_extension_config_dump('repmgr.events', '');
SELECT pg_catalog.pg_extension_config_dump('repmgr.monitoring_history', '');

