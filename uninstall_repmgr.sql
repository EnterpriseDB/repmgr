/*
 * uninstall_repmgr.sql
 *
 * Copyright (c) Heroku, 2010
 *
 */

DROP TABLE IF EXISTS repl_nodes;
DROP TABLE IF EXISTS repl_monitor;
DROP VIEW IF EXISTS repl_status;

DROP SCHEMA repmgr;
DROP USER repmgr;
