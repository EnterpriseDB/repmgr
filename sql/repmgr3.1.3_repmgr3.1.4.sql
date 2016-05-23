/*
 * Update a repmgr 3.1.3 installation to repmgr 3.1.4
 * --------------------------------------------------
 *
 * The new repmgr package should be installed first. Then
 * carry out these steps:
 * 
 *   1. On the master node, execute the SQL statements listed below;
 * 
 *   2. (optional, recommended) add the ssh_hostname parameter to
 *      repmgr.conf, and update the repl_nodes table accordingly
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

ALTER TABLE repl_nodes ADD COLUMN ssh_hostname text;

COMMIT;
