-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION repmgr" to load this file. \quit

ALTER TABLE repmgr.nodes
  DROP CONSTRAINT nodes_type_check,
  ADD CONSTRAINT nodes_type_check CHECK (type IN('primary','standby','witness','bdr','bdr_standby'));

 type             TEXT        NOT NULL CHECK (type IN('primary','standby','witness','bdr','bdr_standby')),
