/*
 * repmgr.sql
 *
 * Copyright (C) 2ndQuadrant, 2010-2011
 *
 */

CREATE USER repmgr;
CREATE SCHEMA repmgr;

/*
 * The table repl_nodes keeps information about all machines in
 * a cluster
 */
CREATE TABLE repl_nodes (
  id            integer primary key,
  cluster   text        not null,       -- Name to identify the cluster
  conninfo      text    not null
);
ALTER TABLE repl_nodes OWNER TO repmgr;

/*
 * Keeps monitor info about every node and their relative "position"
 * to primary
 */
CREATE TABLE repl_monitor (
  primary_node                   INTEGER NOT NULL,
  standby_node                   INTEGER NOT NULL,
  last_monitor_time                      TIMESTAMP WITH TIME ZONE NOT NULL,
  last_wal_primary_location      TEXT NOT NULL,
  last_wal_standby_location      TEXT NOT NULL,
  replication_lag                BIGINT NOT NULL,
  apply_lag                      BIGINT NOT NULL
);
ALTER TABLE repl_monitor OWNER TO repmgr;

CREATE INDEX idx_repl_monitor_last_monitor_sort ON repl_monitor(last_monitor_time, standby_node);

/*
 * This view shows the latest monitor info about every node.
 * Interesting thing to see:
 * replication_lag: in bytes (this is how far the latest xlog record
 *                            we have received is from master)
 * apply_lag: in bytes (this is how far the latest xlog record
 *                      we have applied is from the latest record we
 *                      have received)
 * time_lag: how many seconds are we from being up-to-date with master
 */
CREATE VIEW repl_status AS
SELECT primary_node, standby_node, last_monitor_time, last_wal_primary_location,
       last_wal_standby_location, pg_size_pretty(replication_lag) replication_lag,
       pg_size_pretty(apply_lag) apply_lag,
       age(now(), last_monitor_time) AS time_lag
 FROM repl_monitor
WHERE (standby_node, last_monitor_time) IN (SELECT standby_node, MAX(last_monitor_time)
                                              FROM repl_monitor GROUP BY 1);

ALTER VIEW repl_status OWNER TO repmgr;
