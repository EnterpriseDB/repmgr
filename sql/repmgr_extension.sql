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
SELECT repmgr.am_bdr_failover_handler(-1);
SELECT repmgr.am_bdr_failover_handler(NULL);
SELECT repmgr.get_new_primary();
SELECT repmgr.get_voting_status();
SELECT repmgr.notify_follow_primary(-1);
SELECT repmgr.notify_follow_primary(NULL);
SELECT repmgr.other_node_is_candidate(-1,-1);
SELECT repmgr.other_node_is_candidate(-1,NULL);
SELECT repmgr.other_node_is_candidate(NULL,-1);
SELECT repmgr.other_node_is_candidate(NULL,NULL);
SELECT repmgr.request_vote(-1,-1);
SELECT repmgr.request_vote(-1,NULL);
SELECT repmgr.request_vote(NULL,-1);
SELECT repmgr.request_vote(NULL,NULL);
SELECT repmgr.reset_voting_status();
SELECT repmgr.set_local_node_id(-1);
SELECT repmgr.set_local_node_id(NULL);
SELECT repmgr.set_voting_status_initiated();
SELECT repmgr.standby_get_last_updated();
SELECT repmgr.standby_set_last_updated();
SELECT repmgr.unset_bdr_failover_handler();
