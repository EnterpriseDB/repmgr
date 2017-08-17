repmgr: Replication Manager for PostgreSQL
==========================================

`repmgr` is a suite of open-source tools to manage replication and failover
within a cluster of PostgreSQL servers. It enhances PostgreSQL's built-in
replication capabilities with utilities to set up standby servers, monitor
replication, and perform administrative tasks such as failover or switchover
operations.

`repmgr 4` is a complete rewrite of the existing `repmgr` codebase.

Supports PostgreSQL 9.5 and later; support for PostgreSQL 9.3 and 9.4 has been
dropped. Please continue to use repmgrd 3.x for those versions.

### BDR support

`repmgr 4` supports monitoring of a two-node BDR 2.0 cluster. PostgreSQL 9.6 is
required for BDR 2.0. Note that BDR 2.0 is not publicly available; please contact
2ndQuadrant for details. `repmgr 4` will support future public BDR releases.

Overview
--------

The `repmgr` suite provides two main tools:

- `repmgr` - a command-line tool used to perform administrative tasks such as:
    - setting up standby servers
    - promoting a standby server to master
    - switching over master and standby servers
    - displaying the status of servers in the replication cluster

- `repmgrd` is a daemon which actively monitors servers in a replication cluster
   and performs the following tasks:
    - monitoring and recording replication performance
    - performing failover by detecting failure of the master and
      promoting the most suitable standby server
    - provide notifications about events in the cluster to a user-defined
      script which can perform tasks such as sending alerts by email

`repmgr` supports and enhances PostgreSQL's built-in streaming replication,
which provides a single read/write master server and one or more read-only
standbys containing near-real time copies of the master server's database.

### Concepts

This guide assumes that you are familiar with PostgreSQL administration and
streaming replication concepts. For further details on streaming
replication, see this link:

  https://www.postgresql.org/docs/current/interactive/warm-standby.html#STREAMING-REPLICATION

The following terms are used throughout the `repmgr` documentation.

- `replication cluster`

In the `repmgr` documentation, "replication cluster" refers to the network
of PostgreSQL servers connected by streaming replication.

- `node`

A `node` is a server within a replication cluster.

- `upstream node`

This is the node a standby server is connected to; either the master server or in
the case of cascading replication, another standby.

- `failover`

This is the action which occurs if a master server fails and a suitable standby
is  promoted as the new master. The `repmgrd` daemon supports automatic failover
to minimise downtime.

- `switchover`

In certain circumstances, such as hardware or operating system maintenance,
it's necessary to take a master server offline; in this case a controlled
switchover is necessary, whereby a suitable standby is promoted and the
existing master removed from the replication cluster in a controlled manner.
The `repmgr` command line client provides this functionality.

### repmgr user and metadata

In order to effectively manage a replication cluster, `repmgr` needs to store
information about the servers in the cluster in a dedicated database schema.
This schema is automatically by the `repmgr` extension, which is installed
during the first step in initialising a `repmgr`-administered cluster
(`repmgr primary register`) and contains the following objects:

tables:
  - `repmgr.events`: records events of interest
  - `repmgr.nodes`: connection and status information for each server in the
    replication cluster
  - `repmgr.monitor`: historical standby monitoring information written by `repmgrd`
     XXX not yet implemented

views:
  - `repmgr.show_nodes`: based on the table `repl_nodes`, additionally showing the
     name of the server's upstream node
  - `repmgr.status`: when `repmgrd`'s monitoring is enabled, shows current monitoring
    status for each node
     XXX not yet implemented

The `repmgr` metadata schema can be stored in an existing database or in its own
dedicated database. Note that the `repmgr` metadata schema cannot reside on a database
server which is not part of the replication cluster managed by `repmgr`.

A database user must be available for `repmgr` to access this database and perform
necessary changes. This user does not need to be a superuser, however some operations
such as initial installation of the `repmgr` extension will require a superuser
connection (this can be specified where required with the command line option
`--superuser`).

Installation
------------

### System requirements

`repmgr` is developed and tested on Linux and OS X, but should work on any
UNIX-like system supported by PostgreSQL itself.

`repmgr 4` supports PostgreSQL from version 9.5. If you need to using `repmgr`
on earlier versions of PostgreSQL 9.3 or 9.4, please use `repmgr 3.3`.

All servers in the replication cluster must be running the same major version of
PostgreSQL, and we recommend that they also run the same minor version.

The `repmgr` tools must be installed on each server in the replication cluster.

A dedicated system user for `repmgr` is *not* required; as many `repmgr` and
`repmgrd` actions require direct access to the PostgreSQL data directory,
these commands should be executed by the `postgres` user.

Passwordless `ssh` connectivity between all servers in the replication cluster
is not required, but is necessary in the following cases:

* if you need `repmgr` to copy configuration files from outside the PostgreSQL
  data directory (in which case `rsync` is also required)
* to perform switchover operations
* when executing `repmgr cluster matrix` and `repmgr cluster crosscheck`

* * *

> *TIP*: We recommend using a session multiplexer utility such as `screen` or
> `tmux` when performing long-running actions (such as cloning a database)
> on a remote server - this will ensure the `repmgr` action won't be prematurely
> terminated if your `ssh` session to the server is interrupted or closed.

* * *

### Packages

Release tarballs are also available:

    https://github.com/2ndQuadrant/repmgr/releases
    http://repmgr.org/


### Building from source

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
    repmgr standby switchover

    repmgr bdr register
    repmgr bdr unregister

    repmgr node status
    repmgr node check

    repmgr cluster show
    repmgr cluster matrix
    repmgr cluster crosscheck
    repmgr cluster event


* `primary register`

    Registers a primary in a streaming replication cluster, and configures
    it for use with repmgr.  This command needs to be executed before any
    standby nodes are registered.

    `master register` can be used as an alias for `primary register`.

* `standby switchover`

    ...

    If other standbys (siblings of the promotion candidate) are connected
    to the demotion candidate, if `--siblings-follow` is specified `repmgr`
    can instruct these to follow the new primary. Note this can only work
    if the configuration file on each sibling is the same path as specifed
    in -f/--config-file or -C/--remote-config-file.

* `node status`

    Displays an overview of a node's basic information and replication
    status. This command must be run on the local node.

    Sample output (execute `repmgr node status`):

        Node "node1":
            PostgreSQL version: 10beta1
            Total data size: 30 MB
            Conninfo: host=localhost dbname=repmgr user=repmgr connect_timeout=2
            Role: primary
            WAL archiving: off
            Archive command: (none)
            Replication connections: 2 (of maximal 10)
            Replication slots: 0 (of maximal 10)
            Replication lag: n/a

    See `repmgr node check` to diagnose issues.

* `node check`

    Performs some health checks on a node from a replication perspective.
    This command must be run on the local node.

    Sample output (execute `repmgr node check`):

        Node "node1":
            Server role: OK (node is primary)
            Replication lag: OK (N/A - node is primary)
            WAL archiving: OK (0 pending files)
            Downstream servers: OK (2 of 2 downstream nodes attached)
            Replication slots: OK (node has no replication slots)

    Additionally each check can be performed individually by supplying
    an additional command line parameter, e.g.:

        $ repmgr node check --role
        OK (node is primary)

    Parameters for individual checks are as follows:

    * `--role`: checks if the node has the expected role
    * `--replication-lag"`: checks if the node is lagging by more than
        `replication_lag_warning` or `replication_lag_critical` seconds.
    * `--archive-ready`: checks for WAL files which have not yet been archived
    * `--downstream`: checks that the expected downstream nodes are attached
    * `--slots`: checks there are no inactive replication slots

    Individual checks can also be output in a Nagios-compatible format with
    the option `--nagios`.


* `cluster show`

    Displays information about each active node in the replication cluster. This
    command polls each registered server and shows its role (`primary` / `standby` /
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
    is down. See `repmgr cluster matrix` and `repmgr cluster crosscheck` to get
    a better overviews of connections between nodes.


* `cluster matrix` and `cluster crosscheck`

    These commands display connection information for each pair of
    nodes in the replication cluster.

    - `cluster matrix` runs a `cluster show` on each node and arranges
      the results in a matrix, recording success or failure;

    - `cluster crosscheck` runs a `cluster matrix` on each node and
      combines the results in a single matrix, providing a full
      overview of connections between all databases in the cluster.

    These commands require a valid `repmgr.conf` file on each node.
    Additionally passwordless `ssh` connections are required between
    all nodes.

    Example 1 (all nodes up):

        $ repmgr -f /etc/repmgr.conf cluster matrix

        Name   | Id |  1 |  2 |  3
        -------+----+----+----+----
         node1 |  1 |  * |  * |  *
         node2 |  2 |  * |  * |  *
         node3 |  3 |  * |  * |  *

    Here `cluster matrix` is sufficient to establish the state of each
    possible connection.


    Example 2 (node1 and `node2` up, `node3` down):

        $ repmgr -f /etc/repmgr.conf cluster matrix

        Name   | Id |  1 |  2 |  3
        -------+----+----+----+----
         node1 |  1 |  * |  * |  x
         node2 |  2 |  * |  * |  x
         node3 |  3 |  ? |  ? |  ?

    Each row corresponds to one server, and indicates the result of
    testing an outbound connection from that server.

    Since `node3` is down, all the entries in its row are filled with
    "?", meaning that there we cannot test outbound connections.

    The other two nodes are up; the corresponding rows have "x" in the
    column corresponding to node3, meaning that inbound connections to
    that node have failed, and "*" in the columns corresponding to
    node1 and node2, meaning that inbound connections to these nodes
    have succeeded.

    In this case, `cluster crosscheck` gives the same result as `cluster
    matrix`, because from any functioning node we can observe the same
    state: `node1` and `node2` are up, `node3` is down.

    Example 3 (all nodes up, firewall dropping packets originating
               from `node1` and directed to port 5432 on node3)

    Running `cluster matrix` from `node1` gives the following output:

        $ repmgr -f /etc/repmgr.conf cluster matrix

        Name   | Id |  1 |  2 |  3
        -------+----+----+----+----
         node1 |  1 |  * |  * |  x
         node2 |  2 |  * |  * |  *
         node3 |  3 |  ? |  ? |  ?

    (Note this may take some time depending on the `connect_timeout`
    setting in the registered node `conninfo` strings; default is 1
    minute which means without modification the above command would
    take around 2 minutes to run; see comment elsewhere about setting
    `connect_timeout`)

    The matrix tells us that we cannot connect from `node1` to `node3`,
    and that (therefore) we don't know the state of any outbound
    connection from node3.

    In this case, the `cluster crosscheck` command is more informative:

        $ repmgr -f /etc/repmgr.conf cluster crosscheck

        Name   | Id |  1 |  2 |  3
        -------+----+----+----+----
         node1 |  1 |  * |  * |  x
         node2 |  2 |  * |  * |  *
         node3 |  3 |  * |  * |  *

    What happened is that `cluster crosscheck` merged its own `cluster
    matrix` with the `cluster matrix` output from `node2`; the latter is
    able to connect to `node3` and therefore determine the state of
    outbound connections from that node.

* `cluster event`

    This outputs a formatted list of cluster events, as stored in the
    `repmgr.events` table. Output is in reverse chronological order, and
    can be filtered with the following options:

        * `--all`: outputs all entries
        * `--limit`: set the maximum number of entries to output (default: 20)
        * `--node-id`: restrict entries to node with this ID
        * `--node-name`: restrict entries to node with this name
        * `--event`: filter specific event

    Example:

        $ repmgr -f /etc/repmgr.conf cluster event --event=standby_register
         Node ID | Name  | Event            | OK | Timestamp           | Details
        ---------+-------+------------------+----+---------------------+--------------------------------
         3       | node3 | standby_register | t  | 2017-08-17 10:28:55 | standby registration succeeded
         2       | node2 | standby_register | t  | 2017-08-17 10:28:53 | standby registration succeeded


Backwards compatibility
-----------------------

`repmgr` is now implemented as a PostgreSQL extension, and all database
objects used by repmgr are stored in a dedicated `repmgr` schema, rather
than `repmgr_$cluster_name`. Note there is no need to install the extension,
this will be done automatically by `repmgr primary register`.

Metadata tables have been revised and are not backwards-compatible
with repmgr 3.x. (however future DDL updates will be easier as they can be
carried out via the ALTER EXTENSION mechanism.).

An extension upgrade script will be provided for pre-4.0 installations;
note this will require the existing `repmgr_$cluster_name` schema to
be renamed to `repmgr` beforehand.

Some configuration items have had their names changed for consistency
and clarity e.g. `node` => `node_id`. `repmgr` will issue a warning
about deprecated/altered options.

Some configuration items have been changed to command line options,
and vice-versa, e.g. to avoid hard-coding items such as a a node's
upstream ID, which might change over time.

See file `doc/changes-in-repmgr4.md` for more details.


Generating event notifications with repmgr/repmgrd
--------------------------------------------------

Each time `repmgr` or `repmgrd` perform a significant event, a record
of that event is written into the `repmgr.events` table together with
a timestamp, an indication of failure or success, and further details
if appropriate. This is useful for gaining an overview of events
affecting the replication cluster. However note that this table has
advisory character and should be used in combination with the `repmgr`
and PostgreSQL logs to obtain details of any events.

Example output after a primary was registered and a standby cloned
and registered:

    repmgr=# SELECT * from repmgr.events ;
     node_id |      event       | successful |        event_timestamp        |                                       details
    ---------+------------------+------------+-------------------------------+-------------------------------------------------------------------------------------
           1 | primary_register  | t          | 2016-01-08 15:04:39.781733+09 |
           2 | standby_clone    | t          | 2016-01-08 15:04:49.530001+09 | Cloned from host 'repmgr_node1', port 5432; backup method: pg_basebackup; --force: N
           2 | standby_register | t          | 2016-01-08 15:04:50.621292+09 |
    (3 rows)

Alternatively use `repmgr cluster event` to output a list of events.

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
type `bdr_failover` and optionally `bdr_recovery`:

    %c - conninfo string of the next available node
    %a - name of the next available node

These should always be quoted.

By default, all notification type will be passed to the designated script;
the notification types can be filtered to explicitly named ones:

    event_notifications=primary_register,standby_register

The following event types are available:

  * `master_register`
  * `standby_register`
  * `standby_unregister`
  * `standby_clone`
  * `standby_promote`
  * `standby_follow`
  * `standby_disconnect_manual`
  * `repmgrd_start`
  * `repmgrd_shutdown`
  * `repmgrd_failover_promote`
  * `repmgrd_failover_follow`
  * `bdr_failover`
  * `bdr_reconnect`
  * `bdr_recovery`
  * `bdr_register`
  * `bdr_unregister`

Note that under some circumstances (e.g. no replication cluster master could
be located), it will not be possible to write an entry into the `repmgr.events`
table, in which case executing a script via `event_notification_command` can
serve as a fallback by generating some form of notification.


Diagnostics
-----------

    $ repmgr -f /etc/repmgr.conf node service --list-actions
    Following commands would be executed for each action:

        start: "/usr/bin/pg_ctl -l /var/log/postgresql/startup.log -w -D '/var/lib/pgsql/data' start"
         stop: "/usr/bin/pg_ctl -l /var/log/postgresql/startup.log -D '/var/lib/pgsql/data' -m fast -W stop"
      restart: "/usr/bin/pg_ctl -l /var/log/postgresql/startup.log -w -D '/var/lib/pgsql/data' restart"
       reload: "/usr/bin/pg_ctl -l /var/log/postgresql/startup.log -w -D '/var/lib/pgsql/data' reload"
      promote: "/usr/bin/pg_ctl -l /var/log/postgresql/startup.log -w -D '/var/lib/pgsql/data' promote"
