/*
 * Update a repmgr 2.x installation to repmgr 3.0
 * ----------------------------------------------
 *
 * 1. Stop any running repmgrd instances
 * 2. On the master node, execute the SQL statements listed below,
 *    taking care to identify the master node and any inactive
 *    nodes
 * 3. Restart repmgrd (being sure to use repmgr 3.0)
 */

/*
 * Set the search path to the name of the schema used by
 * your repmgr installation
 * (this should be "repmgr_" + the cluster name defined in
 * 'repmgr.conf')
 */

-- SET search_path TO 'name_of_repmgr_schema';

BEGIN;

ALTER TABLE repl_nodes RENAME TO repl_nodes2_0;

CREATE TABLE repl_nodes (
  id               INTEGER PRIMARY KEY,
  type             TEXT    NOT NULL CHECK (type IN('master','standby','witness')),
  upstream_node_id INTEGER NULL REFERENCES repl_nodes (id),
  cluster          TEXT    NOT NULL,
  name             TEXT    NOT NULL,
  conninfo         TEXT    NOT NULL,
  slot_name        TEXT    NULL,
  priority         INTEGER NOT NULL,
  active           BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO repl_nodes
           (id, type, cluster, name, conninfo, priority)
     SELECT id,
            CASE
               WHEN witness IS TRUE THEN 'witness'
               ELSE 'standby'
            END AS type,
            cluster,
            name,
            conninfo,
            priority + 100
       FROM repl_nodes2_0;

/*
 * You'll need to set the master explicitly; the following query
 * should identify the master node ID but will only work if all
 * standby servers are connected:
 *
 * SELECT id FROM repmgr_test.repl_nodes WHERE name NOT IN (SELECT application_name FROM pg_stat_replication)
 *
 * If in doubt, execute 'repmgr cluster show' will definitively identify
 * the master.
 */
UPDATE repl_nodes SET type = 'master' WHERE id = $master_id;

/* If any nodes are known to be inactive, update them here */

-- UPDATE repl_nodes SET active = FALSE WHERE id IN (...);

/* There's also an event table which we need to create */
CREATE TABLE repl_events (
  node_id          INTEGER NOT NULL,
  event            TEXT NOT NULL,
  successful       BOOLEAN NOT NULL DEFAULT TRUE,
  event_timestamp  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  details          TEXT NULL
);

/* When you're sure of your changes, commit them */

-- COMMIT;


/*
 * execute the following command when you are sure you no longer
 * require the old table:
 */

-- DROP TABLE repl_nodes2_0;
