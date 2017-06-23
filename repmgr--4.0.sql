-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION repmgr" to load this file. \quit

CREATE TABLE nodes (
  node_id          INTEGER PRIMARY KEY,
  upstream_node_id INTEGER NULL REFERENCES nodes (node_id) DEFERRABLE,
  active           BOOLEAN NOT NULL DEFAULT TRUE,
  node_name        TEXT    NOT NULL,
  type             TEXT    NOT NULL CHECK (type IN('primary','standby','witness','bdr')),

  priority         INT     NOT NULL DEFAULT 100,
  conninfo         TEXT    NOT NULL,
  repluser         TEXT    NOT NULL,
  slot_name        TEXT    NULL
);

CREATE TABLE events (
  node_id          INTEGER NOT NULL,
  event            TEXT NOT NULL,
  successful       BOOLEAN NOT NULL DEFAULT TRUE,
  event_timestamp  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  details          TEXT NULL
);


CREATE VIEW show_nodes AS
   SELECT n.node_id,
          n.node_name,
          un.node_name AS upstream_node_name,
          n.type,
          n.priority,
          n.conninfo
     FROM nodes n
LEFT JOIN nodes un
       ON un.node_id = n.upstream_node_id;

