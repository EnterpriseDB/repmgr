-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION repmgr" to load this file. \quit

CREATE TABLE repmgr.nodes (
  node_id          INTEGER     PRIMARY KEY,
  upstream_node_id INTEGER     NULL REFERENCES nodes (node_id) DEFERRABLE,
  active           BOOLEAN     NOT NULL DEFAULT TRUE,
  node_name        TEXT        NOT NULL,
  type             TEXT        NOT NULL CHECK (type IN('primary','standby','witness','bdr')),
  location         TEXT        NOT NULL DEFAULT 'default',
  priority         INT         NOT NULL DEFAULT 100,
  conninfo         TEXT        NOT NULL,
  repluser         VARCHAR(63) NOT NULL,
  slot_name        TEXT        NULL,
  config_file      TEXT        NOT NULL
);

SELECT pg_catalog.pg_extension_config_dump('repmgr.nodes', '');

CREATE TABLE repmgr.events (
  node_id          INTEGER NOT NULL,
  event            TEXT NOT NULL,
  successful       BOOLEAN NOT NULL DEFAULT TRUE,
  event_timestamp  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  details          TEXT NULL
);

SELECT pg_catalog.pg_extension_config_dump('repmgr.events', '');

CREATE TABLE repmgr.monitoring_history (
  primary_node_id                INTEGER NOT NULL,
  standby_node_id                INTEGER NOT NULL,
  last_monitor_time              TIMESTAMP WITH TIME ZONE NOT NULL,
  last_apply_time                TIMESTAMP WITH TIME ZONE,
  last_wal_primary_location      PG_LSN NOT NULL,
  last_wal_standby_location      PG_LSN,
  replication_lag                BIGINT NOT NULL,
  apply_lag                      BIGINT NOT NULL
);

CREATE INDEX idx_monitoring_history_time
          ON repmgr.monitoring_history (last_monitor_time, standby_node_id);

SELECT pg_catalog.pg_extension_config_dump('repmgr.monitoring_history', '');

CREATE VIEW repmgr.show_nodes AS
   SELECT n.node_id,
          n.node_name,
          n.active,
          n.upstream_node_id,
          un.node_name AS upstream_node_name,
          n.type,
          n.priority,
          n.conninfo
     FROM repmgr.nodes n
LEFT JOIN repmgr.nodes un
       ON un.node_id = n.upstream_node_id;

CREATE TABLE repmgr.voting_term (
  term INT NOT NULL
);

CREATE UNIQUE INDEX voting_term_restrict
ON repmgr.voting_term ((TRUE));

CREATE RULE voting_term_delete AS
   ON DELETE TO repmgr.voting_term
   DO INSTEAD NOTHING;


/* ================= */
/* repmgrd functions */
/* ================= */

/* monitoring functions */

CREATE FUNCTION set_local_node_id(INT)
  RETURNS VOID
  AS 'MODULE_PATHNAME', 'set_local_node_id'
  LANGUAGE C STRICT;

CREATE FUNCTION get_local_node_id()
  RETURNS INT
  AS 'MODULE_PATHNAME', 'get_local_node_id'
  LANGUAGE C STRICT;

CREATE FUNCTION standby_set_last_updated()
  RETURNS TIMESTAMP WITH TIME ZONE
  AS 'MODULE_PATHNAME', 'standby_set_last_updated'
  LANGUAGE C STRICT;

CREATE FUNCTION standby_get_last_updated()
  RETURNS TIMESTAMP WITH TIME ZONE
  AS 'MODULE_PATHNAME', 'standby_get_last_updated'
  LANGUAGE C STRICT;

CREATE FUNCTION set_upstream_last_seen(INT)
  RETURNS VOID
  AS 'MODULE_PATHNAME', 'set_upstream_last_seen'
  LANGUAGE C STRICT;

CREATE FUNCTION get_upstream_last_seen()
  RETURNS INT
  AS 'MODULE_PATHNAME', 'get_upstream_last_seen'
  LANGUAGE C STRICT;

CREATE FUNCTION get_upstream_node_id()
  RETURNS INT
  AS 'MODULE_PATHNAME', 'get_upstream_node_id'
  LANGUAGE C STRICT;

CREATE FUNCTION set_upstream_node_id(INT)
  RETURNS VOID
  AS 'MODULE_PATHNAME', 'set_upstream_node_id'
  LANGUAGE C STRICT;

/* failover functions */

CREATE FUNCTION notify_follow_primary(INT)
  RETURNS VOID
  AS 'MODULE_PATHNAME', 'notify_follow_primary'
  LANGUAGE C STRICT;

CREATE FUNCTION get_new_primary()
  RETURNS INT
  AS 'MODULE_PATHNAME', 'get_new_primary'
  LANGUAGE C STRICT;

CREATE FUNCTION reset_voting_status()
  RETURNS VOID
  AS 'MODULE_PATHNAME', 'reset_voting_status'
  LANGUAGE C STRICT;

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
  LANGUAGE C CALLED ON NULL INPUT;

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

CREATE FUNCTION get_wal_receiver_pid()
  RETURNS INT
  AS 'MODULE_PATHNAME', 'get_wal_receiver_pid'
  LANGUAGE C STRICT;




/* views */

CREATE VIEW repmgr.replication_status AS
  SELECT m.primary_node_id, m.standby_node_id, n.node_name AS standby_name,
 	     n.type AS node_type, n.active, last_monitor_time,
         CASE WHEN n.type='standby' THEN m.last_wal_primary_location ELSE NULL END AS last_wal_primary_location,
         m.last_wal_standby_location,
         CASE WHEN n.type='standby' THEN pg_catalog.pg_size_pretty(m.replication_lag) ELSE NULL END AS replication_lag,
         CASE WHEN n.type='standby' THEN
           CASE WHEN replication_lag > 0 THEN age(now(), m.last_apply_time) ELSE '0'::INTERVAL END
           ELSE NULL
         END AS replication_time_lag,
         CASE WHEN n.type='standby' THEN pg_catalog.pg_size_pretty(m.apply_lag) ELSE NULL END AS apply_lag,
         AGE(NOW(), CASE WHEN pg_catalog.pg_is_in_recovery() THEN repmgr.standby_get_last_updated() ELSE m.last_monitor_time END) AS communication_time_lag
    FROM repmgr.monitoring_history m
    JOIN repmgr.nodes n ON m.standby_node_id = n.node_id
   WHERE (m.standby_node_id, m.last_monitor_time) IN (
	          SELECT m1.standby_node_id, MAX(m1.last_monitor_time)
			    FROM repmgr.monitoring_history m1 GROUP BY 1
         );

