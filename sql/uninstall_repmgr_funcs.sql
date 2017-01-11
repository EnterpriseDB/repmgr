/*
 * uninstall_repmgr_funcs.sql
 * Copyright (c) 2ndQuadrant, 2010-2017
 *
 */

DROP FUNCTION repmgr_update_standby_location(text);
DROP FUNCTION repmgr_get_last_standby_location();

DROP FUNCTION repmgr_update_last_updated();
DROP FUNCTION repmgr_get_last_updated();
