repmgr: Replication Manager for PostgreSQL
==========================================

`repmgr` is a suite of open-source tools to manage replication and failover
within a cluster of PostgreSQL servers. It enhances PostgreSQL's built-in
replication capabilities with utilities to set up standby servers, monitor
replication, and perform administrative tasks such as failover or switchover
operations.

`repmgr 4` is a complete rewrite of the existing `repmgr` codebase.

Commands
--------

repmgr cluster event [--all] [--node-id] [--node-name] [--event] [--event-matching]