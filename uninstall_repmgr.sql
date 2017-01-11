/*
 * uninstall_repmgr.sql
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 *
 */

DROP TABLE IF EXISTS repl_nodes;
DROP TABLE IF EXISTS repl_monitor;
DROP VIEW IF EXISTS repl_status;

DROP SCHEMA repmgr;
DROP USER repmgr;
