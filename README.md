repmgr: Replication Manager for PostgreSQL
==========================================

`repmgr` is a suite of open-source tools to manage replication and failover
within a cluster of PostgreSQL servers. It enhances PostgreSQL's built-in
replication capabilities with utilities to set up standby servers, monitor
replication, and perform administrative tasks such as failover or switchover
operations.

`repmgr 4` is a complete rewrite of the existing `repmgr` codebase.


Supports PostgreSQL 9.6 and later; support for 9.3 has been dropped, 9.4/9.5
may be supported if feasible.

Building from source
--------------------

Simply:

    ./configure && make install

Ensure `pg_config` for the target PostgreSQL version is in `$PATH`.


Reference
---------

### repmgr commands

The following commands are available:

    repmgr primary register
    repmgr primary unregister

    repmgr standby clone
    repmgr standby register
    repmgr standby unregister
    repmgr standby promote
    repmgr standby follow

    repmgr bdr register
    repmgr bdr unregister

    repmgr cluster show
    repmgr cluster event [--all] [--node-id] [--node-name] [--event] [--event-matching]


* `primary register`

    Registers a primary in a streaming replication cluster, and configures
    it for use with repmgr.  This command needs to be executed before any
    standby nodes are registered.

    `master register` can be used as an alias for `primary register`.

* `cluster show`

    Displays information about each active node in the replication cluster. This
    command polls each registered server and shows its role (`master` / `standby` /
    `bdr`) and status. It polls each server directly and can be run on any node
    in the cluster; this is also useful when analyzing connectivity from a particular
    node.

    This command requires either a valid `repmgr.conf` file or a database connection
    string to one of the registered nodes; no  additional arguments are needed.

    Example:

        $ repmgr -f /etc/repmgr.conf cluster show

         ID | Name  | Role    | Status    | Upstream | Connection string
        ----+-------+---------+-----------+----------+-----------------------------------------
         1  | node1 | primary | * running |          | host=db_node1 dbname=repmgr user=repmgr
         2  | node2 | standby |   running | node1    | host=db_node2 dbname=repmgr user=repmgr
         3  | node3 | standby |   running | node1    | host=db_node3 dbname=repmgr user=repmgr

    To show database connection errors when polling nodes, run the command in
    `--verbose` mode.

    The `cluster show` command accepts an optional parameter `--csv`, which
    outputs the replication cluster's status in a simple CSV format, suitable for
    parsing by scripts:

        $ repmgr -f /etc/repmgr.conf cluster show --csv
        1,-1,-1
        2,0,0
        3,0,1

    The columns have following meanings:

        - node ID
        - availability (0 = available, -1 = unavailable)
        - recovery state (0 = not in recovery, 1 = in recovery, -1 = unknown)

    Note that the availability is tested by connecting from the node where
    `repmgr cluster show` is executed, and does not necessarily imply the node
    is down.


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

Generating event notifications with repmgr/repmgrd
--------------------------------------------------

Each time `repmgr` or `repmgrd` perform a significant event, a record
of that event is written into the `repl_events` table together with
a timestamp, an indication of failure or success, and further details
if appropriate. This is useful for gaining an overview of events
affecting the replication cluster. However note that this table has
advisory character and should be used in combination with the `repmgr`
and PostgreSQL logs to obtain details of any events.

Example output after a master was registered and a standby cloned
and registered:

    repmgr=# SELECT * from repmgr_test.repl_events ;
     node_id |      event       | successful |        event_timestamp        |                                       details
    ---------+------------------+------------+-------------------------------+-------------------------------------------------------------------------------------
           1 | master_register  | t          | 2016-01-08 15:04:39.781733+09 |
           2 | standby_clone    | t          | 2016-01-08 15:04:49.530001+09 | Cloned from host 'repmgr_node1', port 5432; backup method: pg_basebackup; --force: N
           2 | standby_register | t          | 2016-01-08 15:04:50.621292+09 |
    (3 rows)

Additionally, event notifications can be passed to a user-defined program
or script which can take further action, e.g. send email notifications.
This is done by setting the `event_notification_command` parameter in
`repmgr.conf`.

This parameter accepts the following format placeholders:

    %n - node ID
    %e - event type
    %s - success (1 or 0)
    %t - timestamp
    %d - details

The values provided for "%t" and "%d" will probably contain spaces,
so should be quoted in the provided command configuration, e.g.:

    event_notification_command='/path/to/some/script %n %e %s "%t" "%d"'

Additionally the following format placeholders are available for the event
type `bdr_failover`:

    %c - conninfo string of the next available node
    %a - name of the next available node

These should always be quoted.

By default, all notification type will be passed to the designated script;
the notification types can be filtered to explicitly named ones:

    event_notifications=master_register,standby_register

The following event types are available:

  ...
  * `bdr_failover`
  * `bdr_register`
  * `bdr_unregister`

Note that under some circumstances (e.g. no replication cluster master could
be located), it will not be possible to write an entry into the `repl_events`
table, in which case `event_notification_command` can serve as a fallback.
