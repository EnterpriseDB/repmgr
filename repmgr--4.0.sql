-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION repmgr" to load this file. \quit

CREATE TABLE nodes (
  node_id          INTEGER PRIMARY KEY,
  type             TEXT    NOT NULL CHECK (type IN('master','standby','witness','bdr'))
);
