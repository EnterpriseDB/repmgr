-- minimal SQL tests
--
-- comprehensive tests will require a working replication cluster
-- set up using the "repmgr" binary and with "repmgrd" running

-- extension
CREATE EXTENSION repmgr;

-- tables
SELECT * FROM repmgr.nodes;
SELECT * FROM repmgr.events;
SELECT * FROM repmgr.monitoring_history;

-- views

SELECT * FROM repmgr.replication_status;
SELECT * FROM repmgr.show_nodes;

-- functions
SELECT repmgr.get_new_primary();
SELECT repmgr.get_voting_status();
SELECT repmgr.reset_voting_status();
SELECT repmgr.standby_get_last_updated();
SELECT repmgr.standby_set_last_updated();
SELECT repmgr.unset_bdr_failover_handler();
