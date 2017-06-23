repmgr: Replication Manager for PostgreSQL
==========================================

`repmgr` is a suite of open-source tools to manage replication and failover
within a cluster of PostgreSQL servers. It enhances PostgreSQL's built-in
replication capabilities with utilities to set up standby servers, monitor
replication, and perform administrative tasks such as failover or switchover
operations.

`repmgr 4` is a complete rewrite of the existing `repmgr` codebase.


Supports PostgreSQL 9.6 and later; support for 9.3 will be dropped, 9.4/9.5
may be supported if feasible.

Building from source
--------------------

Simply:

    ./configure && make install

Ensure `pg_config` for the target PostgreSQL version is in `$PATH`.


Commands
--------

Currently available:

    repmgr primary register
    repmgr primary unregister

    repmgr standby clone
    repmgr standby register
    repmgr standby unregister
    repmgr standby promote
    repmgr standby follow

    repmgr cluster event [--all] [--node-id] [--node-name] [--event] [--event-matching]


Backwards compatibility
-----------------------

See also: doc/changes-in-repmgr4.md

`repmgr` is now implemented as a PostgreSQL extension. NOTE: no need to
install the extension, this will be done automatically by `repmgr primary register`.

Metadata tables have been revised and are not backwards-compatible
with 3.x. (however future DDL updates will be easier as they can be
carried out via the ALTER EXTENSION mechanism.

TODO: extension upgrade script for pre-4.0

Some configuration items have had their names changed for consistency
and clarity e.g. `node` => `node_id`. `repmgr` will issue a warning
about deprecated/altered options.

Some configuration items have been changed to command line options,
and vice-versa, e.g. to avoid hard-coding things like a node's
upstream ID which might change.

TODO: possibly add a config file conversion script/function.
