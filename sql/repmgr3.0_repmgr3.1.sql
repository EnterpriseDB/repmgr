/*
 * Update a repmgr 3.0 installation to repmgr 3.1
 * ----------------------------------------------
 *
 * 1. Stop any running repmgrd instances
 * 2. On the master node, execute the SQL statements listed below,
 *    taking care to identify the master node and any inactive
 *    nodes
 * 3. Restart repmgrd (being sure to use repmgr 3.1)
 */

/*
 * Set the search path to the name of the schema used by
 * your repmgr installation
 * (this should be "repmgr_" + the cluster name defined in
 * 'repmgr.conf')
 */

-- SET search_path TO 'name_of_repmgr_schema';

BEGIN;

-- We have this new view which gives the upstream node information when
-- checking on the nodes table

CREATE VIEW repl_show_nodes AS 
SELECT rn.id, rn.conninfo, rn.type, rn.name, rn.cluster,
	rn.priority, rn.active, sq.name AS upstream_node_name
FROM repl_nodes as rn LEFT JOIN repl_nodes AS sq ON sq.id=rn.upstream_node_id;

COMMIT;


