/*
 * Update a repmgr 3.1.1 installation to repmgr 3.1.2
 * --------------------------------------------------
 *
 * This update is only required if repmgrd is being used in conjunction
 * with a witness server.
 *
 * The new repmgr package should be installed first. Then
 * carry out these steps:
 *
 *   1. (If repmgrd is used) stop any running repmgrd instances
 *   2. On the master node, execute the SQL statement listed below
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

ALTER TABLE repl_nodes DROP CONSTRAINT repl_nodes_upstream_node_id_fkey,
      ADD CONSTRAINT repl_nodes_upstream_node_id_fkey FOREIGN KEY (upstream_node_id) REFERENCES repl_nodes(id) DEFERRABLE;
COMMIT;
