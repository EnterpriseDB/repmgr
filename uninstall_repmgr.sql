/*
 * uninstall_repmgr.sql
 *
 * Copyright (C) 2ndQuadrant, 2010-2015
 *
 */

DROP TABLE IF EXISTS repl_nodes;
DROP TABLE IF EXISTS repl_monitor;
DROP VIEW IF EXISTS repl_status;

DROP SCHEMA repmgr;
DROP USER repmgr;
