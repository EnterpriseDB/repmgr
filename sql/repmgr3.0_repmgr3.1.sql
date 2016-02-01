/*
 * Update a repmgr 3.0 installation to repmgr 3.1
 * ----------------------------------------------
 *
 * The new repmgr package should be installed first. Then
 * carry out these steps:
 *
 *   1. (If repmgrd is used) stop any running repmgrd instances
 *   2. On the master node, execute the SQL statements listed below
 *   3. (If repmgrd is used) restart repmgrd
 */

/*
 * If your repmgr installation is not included in your repmgr
 * user's search path, please set the search path to the name
 * of the repmgr schema to ensure objects are installed in
 * the correct location.
 *
 * The repmgr schema is  "repmgr_" + the cluster name defined in
 * 'repmgr.conf'.
 */

-- SET search_path TO 'name_of_repmgr_schema';

BEGIN;

-- New view "repl_show_nodes" which also displays the server's
-- upstream node

CREATE VIEW repl_show_nodes AS
SELECT rn.id, rn.conninfo, rn.type, rn.name, rn.cluster,
	rn.priority, rn.active, sq.name AS upstream_node_name
FROM repl_nodes as rn LEFT JOIN repl_nodes AS sq ON sq.id=rn.upstream_node_id;

COMMIT;
