repmgr: Replication Manager for PostgreSQL clusters
===================================================

`repmgr` is an open-source tool to mananage replication and failover
between multiple PostgreSQL servers. It enhances PostgreSQL's built-in
hot-standby capabilities with tools to set up standby servers, monitor
replication, and perform administrative tasks such as failover or manual
switchover operations.

This document covers `repmgr 3`, which supports PostgreSQL 9.4 and 9.3.
This version can use `pg_basebackup` to clone standby servers, supports
replication slots and cascading replication, doesn't require a restart
after promotion, and has many usability improvements.

Please continue to use `repmgr 2` with earlier PostgreSQL 9.x versions.
For a list of changes since `repmgr 2` and instructions on upgrading to
`repmgr 3`, see the "Upgrading from repmgr 2" section below.

Overview
--------

The `repmgr` command-line tool is used to perform administrative tasks,
and the `repmgrd` daemon is used to optionally monitor replication and
manage automatic failover.

To get started, each PostgreSQL node in your cluster must have a
`repmgr.conf` file. The current master node must be registered using
`repmgr master register`. Existing standby servers can be registered
using `repmgr standby register`. A new standby server can be created
using `repmgr standby clone` followed by `repmgr standby register`.

See the "QUICKSTART.md" file for examples of how to use these commands.

Once the cluster is in operation, run `repmgr cluster show` to see the
status of the registered primary and standby nodes. Any standby can be
manually promoted using `repmgr standby promote`. Other standby nodes
can be told to follow the new master using `repmgr standby follow`.

Next, for detailed monitoring, you must run `repmgrd` (with the same
configuration file) on all your nodes. Replication status information is
stored in a custom schema along with information about registered nodes.
You also need `repmgrd` to configure automatic failover in your cluster.

See the "FAILOVER.md" file for an explanation of how to set up automatic
failover.

Requirements
------------

`repmgr` is developed and tested on Linux, but it should work on any
UNIX-like system which PostgreSQL itself supports.

All nodes must be running the same major version of PostgreSQL, and we
recommend that they also run the same minor version. This version of
`repmgr` (v3) supports PostgreSQL 9.3 and 9.4.

Earlier versions of `repmgr` required password-less SSH access between
nodes in order to clone standby servers using `rsync`. `repmgr 3` can
use `pg_basebackup` instead in most circumstances; ssh is not needed.

You will need to use rsync only if your PostgreSQL configuration files
are outside your data directory (as on Debian). See the "SSH-RSYNC.md"
file for details on configuring password-less SSH between your nodes.

Installation
------------

`repmgr` must be installed on each PostgreSQL server node.

* Packages
  - RPM packages for RedHat-based distributions are available from PGDG
  - Debian/Ubuntu provide .deb packages.

  It is also possible to build .deb and .rpm packages directly from the
  `repmgr` source. The `debian` and `RHEL` directories contain the
  necessary files.

* Source installation
  - `repmgr` source code is available at
    https://github.com/2ndQuadrant/repmgr
  - tar.gz files can be downloaded from
    https://github.com/2ndQuadrant/repmgr/releases
  - To install from source, just run:

    sudo make USE_PGXS=1 install

Configuration
-------------

### Server configuration

By default, `repmgr` uses PostgreSQL's build-in replication protocol
for communicating with remote servers, e.g. when cloning a primary.
However, password-less SSH logins may need to be enable for the database
system user (typically `postgres`) between all server nodes if you
wish `repmgr` to copy configuration files not located in

### PostgreSQL configuration

PostgreSQL node needs to be configured for replication with the following
settings:

    wal_level = 'hot_standby'      # minimal, archive, hot_standby, or logical
    archive_mode = on              # allows archiving to be done
    archive_command = 'cd .'       # command to use to archive a logfile segment
    max_wal_senders = 10           # max number of walsender processes
    wal_keep_segments = 5000       # in logfile segments, 16MB each; 0 disables
    hot_standby = on               # "on" allows queries during recovery

Note that `repmgr` expects a default of 5000 `wal_keep_segments`, although this
value can be overridden when executing the `repmgr` client.

From PostgreSQL 9.4, replication slots are available, which remove the
requirement to retain a fixed number of WAL logfile segments. See
"repmgr configuration" for details.

Additionally, `repmgr` requires a dedicated PostgreSQL superuser account
and a database in which to store monitoring and replication data. The `repmgr`
user account will also be used for replication connections from the standby,
so a separate replication user with the `REPLICATION` privilege is not required.
The database can in principle be any database, including the default `postgres`
one, however it's probably advisable to create a dedicated database for `repmgr`
usage. Both user and database will be created by `repmgr`.


### repmgr configuration

Each PostgreSQL node requires a `repmgr.conf` configuration file containing
identification and database connection information. This is a sample
minimal configuration:

    cluster=test
    node=1
    node_name=node1
    conninfo='host=repmgr_node1 user=repmgr_usr dbname=repmgr_db'
    pg_bindir=/path/to/postgres/bin

* `cluster`: common name for the replication cluster; this must be the same on all nodes
* `node`: a unique, abitrary integer identifier
* `name`: a unique, human-readable name
* `conninfo`: a standard conninfo string enabling repmgr to connect to the
   control database; user and name must be the same on all nodes, while other
   parameters such as port may differ. The `host` parameter *must* be a hostname
   resolvable by all nodes on the cluster.
* `pg_bindir`: (optional) location of PostgreSQL binaries, if not in the default $PATH

Note that the configuration file should *not* be stored inside the PostgreSQL
data directory. The configuration file can be specified with the
`-f, --config-file=PATH` option and can have any arbitrary name.`repmgr`
will fail with an error if it does not find the specified file. If no
configuration file is specified, `repmgr` will search for `repmgr.conf`
in the current working directory; if no file is found it will continue
with default values.

The master node must be registered first using `repmgr master register`,
and each standby needs to be registered using `repmgr standby register`
tool; this inserts details about each node into the control database.

Example replication setup
-------------------------

See the QUICKSTART.md file for an annotated example setup.


Failover
--------

To promote a standby to master, on the standby execute e.g.:

    repmgr -f $HOME/repmgr/repmgr.conf --verbose standby promote

`repmgr` will attempt to connect to the current master to verify that it
is not available (if it is, `repmgr` will not promote the standby).

Other standby servers need to be told to follow the new master with e.g.:

    repmgr -f $HOME/repmgr/repmgr.conf --verbose standby follow

See file `autofailover_quick_setup.rst` for details on setting up
automated failover.


Converting a failed master to a standby
---------------------------------------

Often it's desirable to bring a failed master back into replication
as a standby. First, ensure that the master's PostgreSQL server is
no longer running; then use `repmgr standby clone` to re-sync its
data directory with the current master, e.g.:

    repmgr -f $HOME/repmgr/repmgr.conf \
      --force --rsync-only \
      -h node2 -d repmgr_db -U repmgr_usr --verbose \
      standby clone

Here it's essential to use the command line options `--force`, to
ensure `repmgr` will re-use the existing data directory, and
`--rsync-only`, which causes `repmgr` to use `rsync` rather than
`pg_basebackup`, as the latter can only be used to clone a fresh
standby.

The node can then be restarted.

The node will then need to be re-registered with `repmgr`; again
the `--force` option is required to update the existing record:

     repmgr -f $HOME/repmgr/repmgr.conf
       --force \
       standby register



Replication management with repmgrd
-----------------------------------

`repmgrd` is a management and monitoring daemon which runs on standby nodes
and which can automate actions such as failover and updating standbys to
follow the new master.`repmgrd`   can be started simply with e.g.:

    repmgrd -f $HOME/repmgr/repmgr.conf --verbose > $HOME/repmgr/repmgr.log 2>&1

or alternatively:

    repmgrd -f $HOME/repmgr/repmgr.conf --verbose --monitoring-history > $HOME/repmgr/repmgrd.log 2>&1

which will track replication advance or lag on all registerd standbys.

For permanent operation, we recommend using the options `-d/--daemonize` to
detach the `repmgrd` process, and `-p/--pid-file` to write the process PID
to a file.

Example log output (at default log level):

    [2015-03-11 13:15:40] [INFO] checking cluster configuration with schema 'repmgr_test'
    [2015-03-11 13:15:40] [INFO] checking node 2 in cluster 'test'
    [2015-03-11 13:15:40] [INFO] reloading configuration file and updating repmgr tables
    [2015-03-11 13:15:40] [INFO] starting continuous standby node monitoring


Witness server
--------------

In a situation caused e.g. by a network interruption between two
data centres, it's important to avoid a "split-brain" situation where
both sides of the network assume they are the active segment and the
side without an active master unilaterally promotes one of its standbys.

To prevent this situation happening, it's essential to ensure that one
network segment has a "voting majority", so other segments will know
they're in the minority and not attempt to promote a new master. Where
an odd number of servers exists, this is not an issue. However, if each
network has an even number of nodes, it's necessary to provide some way
of ensuring a majority, which is where the witness server becomes useful.

This is not a fully-fledged standby node and is not integrated into
replication, but it effectively represents the "casting vote" when
deciding which network segment has a majority. A witness server can
be set up using `repmgr witness create` (see below for details) and
can run on a dedicated server or an existing node. Note that it only
makes sense to create a witness server in conjunction with running
`repmgrd`; the witness server will require its own `repmgrd` instance.


Monitoring
----------

When `repmgrd` is running with the option `-m/--monitoring-history`, it will
constantly write node status information to the `repl_monitor` table, which can
be queried easily using the view `repl_status`:

    repmgr_db=# SELECT * FROM repmgr_test.repl_status;
    -[ RECORD 1 ]-------------+-----------------------------
    primary_node              | 1
    standby_node              | 2
    standby_name              | node2
    node_type                 | standby
    active                    | t
    last_monitor_time         | 2015-03-11 14:02:34.51713+09
    last_wal_primary_location | 0/3012AF0
    last_wal_standby_location | 0/3012AF0
    replication_lag           | 0 bytes
    replication_time_lag      | 00:00:03.463085
    apply_lag                 | 0 bytes
    communication_time_lag    | 00:00:00.955385

Cascading replication
---------------------

Cascading replication - where a standby can connect to an upstream node and not
the master server itself - was introduced in PostgreSQL 9.2. `repmgr` and
`repmgrd` support cascading replication by keeping track of the relationship
between standby servers - each node record is stored with the node id of its
upstream ("parent") server (except of course the master server).

In a failover situation where the master node fails and a top-level standby
is promoted, a standby connected to another standby will not be affected
and continue working as normal (even if the upstream standby it's connected
to becomes the master node). If however the node's direct upstream fails,
the "cascaded standby" will attempt to reconnect to that node's parent.

To configure standby servers for cascading replication, add the parameter
`upstream_node` to `repmgr.conf` and set it to the id of the node it should
connect to, e.g.:

    cluster=test
    node=2
    node_name=node2
    upstream_node=1

Replication slots
-----------------

Replication slots were introduced with PostgreSQL 9.4 and enable standbys to
notify the master of their WAL consumption, ensuring that the master will
not remove any WAL files until they have been received by all standbys.
This mitigates the requirement to manage WAL file retention using
`wal_keep_segments` etc., with the caveat that if a standby fails, no WAL
files will be removed until the standby's replication slot is deleted.

To enable replication slots, set the boolean parameter `use_replication_slots`
in `repmgr.conf`:

    use_replication_slots=1

`repmgr` will automatically generate an appropriate slot name, which is
stored in the `repl_nodes` table.

Note that `repmgr` will fail with an error if this option is specified when
working with PostgreSQL 9.3.

Further reading:
 * http://www.postgresql.org/docs/current/interactive/warm-standby.html#STREAMING-REPLICATION-SLOTS
 * http://blog.2ndquadrant.com/postgresql-9-4-slots/

Upgrading from repmgr 2
-----------------------

`repmgr 3` is largely compatible with `repmgr 2`; the only step required
to upgrade is to update the `repl_nodes` table to the definition needed
by `repmgr 3`. See the file `sql/repmgr2_repmgr3.sql` for details on how
to do this.

`repmgrd` must *not* be running while `repl_nodes` is being updated.

Existing `repmgr.conf` files can be retained as-is.

---------------------------------------

Reference
---------

### repmgr command reference

Not all of these commands need the ``repmgr.conf`` file, but they need to be able to
connect to the remote and local databases.

You can teach it which is the remote database by using the -h parameter or
as a last parameter in standby clone and standby follow. If you need to specify
a port different then the default 5432 you can specify a -p parameter.
Standby is always considered as localhost and a second -p parameter will indicate
its port if is different from the default one.

* `master register`

    Registers a master in a cluster. This command needs to be executed before any
    standby nodes are registered.

* `standby register`

    Registers a standby with `repmgr`. This command needs to be executed to enable
    promote/follow operations and to allow `repmgrd` to work with the node.
    An existing standby can be registered using this command.

* `standby clone [node to be cloned]`

    Clones a new standby node from the data directory of the master (or
    an upstream cascading standby) using `pg_basebackup` or `rsync`.
    Additionally it will create the `recovery.conf` file required to
    start the server as a standby. This command does not require
    `repmgr.conf` to be provided, but does require connection details
    of the master or upstream server as command line parameters.

    Provide the `-D/--data-dir` option to specify the destination data
    directory; if not, the same directory path as on the source server
    will be used. By default, `pg_basebackup` will be used to copy data
    from the master or upstream node but this can only be used for
    bootstrapping new installations. To update an existing but 'stale'
    data directory (for example belonging to a failed master), `rsync`
    must be used by specifying `--rsync-only`. In this case,
    password-less SSH connections between servers are required.

* `standby promote`

    Promotes a standby to a master if the current master has failed. This
    command requires a valid `repmgr.conf` file for the standby, either
    specified explicitly  with `-f/--config-file` or located in the current
    working directory; no additional arguments are required.

    If the standby promotion succeeds, the server will not need to be
    restarted. However any other standbys will need to follow the new server,
    by using `standby follow` (see below); if `repmgrd` is active, it will
    handle this.

    This command will not function if the current master is still running.

* `witness create`

    Creates a witness server as a separate PostgreSQL instance. This instance
    can be on a separate server or a server running an existing node. The
    witness server contain a copy of the repmgr metadata tables but will not
    be set up as a standby; instead it will update its metadata copy each
    time a failover occurs.

    Note that it only makes sense to create a witness server if `repmgrd`
    is in use; see section "witness server" above.

    By default the witness server will use port 5499 to facilitate easier setup
    on a server running an existing node.

* `standby follow`

    Attaches the standby to a new master. This command requires a valid
    `repmgr.conf` file for the standby, either specified explicitly with
    `-f/--config-file` or located in the current working directory; no
    additional arguments are required.

    This command will force a restart of the standby server. It can only be used
    to attach a standby to a new master node.

* `cluster show`

    Displays information about each node in the replication cluster. This
    command polls each registered server and shows its role (master / standby /
    witness) or "FAILED" if the node doesn't respond. It polls each server
    directly and can be run on any node in the cluster; this is also useful
    when analyzing connectivity from a particular node.

    This command requires a valid `repmgr.conf` file for the node on which it is
    executed, either specified explicitly with `-f/--config-file` or located in
    the current working directory; no additional arguments are required.

    Example:

        repmgr -f /path/to/repmgr.conf cluster show
        Role      | Connection String
        * master  | host=node1 dbname=repmgr_db user=repmgr_usr
          standby | host=node2 dbname=repmgr_db user=repmgr_usr
          standby | host=node3 dbname=repmgr_db user=repmgr_usr


* `cluster cleanup`

    Purges monitoring history from the `repl_monitor` table to prevent excessive
    table growth. Use the `-k/--keep-history` to specify the number of days of
    monitoring history to retain. This command can be used manually or as a
    cronjob.

    This command requires a valid `repmgr.conf` file for the node on which it is
    executed, either specified explicitly with `-f/--config-file` or located in
    the current working directory; no additional arguments are required.

### repmgr configuration file

See `repmgr.conf.sample` for an example configuration file with available
configuration settings annotated.

### repmgr database schema

`repmgr` creates a small schema for its own use in the database specified in
each node's `conninfo` configuration parameter. This database can in principle
be any database. The schema name is the global `cluster` name prefixed
with `repmgr_`, so for the example setup above the schema name is
`repmgr_test`.

The schema contains two tables:

* `repl_nodes`
  stores information about all registered servers in the cluster
* `repl_monitor`
  stores monitoring information about each node (generated by `repmgrd` with
  `-m/--monitoring-history` option enabled)

and one view:
* `repl_status`
  summarizes the latest monitoring information for each node (generated by `repmgrd` with
  `-m/--monitoring-history` option enabled)

### Error codes

`repmgr` or `repmgrd` will return one of the following error codes on program
exit:

* SUCCESS (0)             Program ran successfully.
* ERR_BAD_CONFIG (1)      Configuration file could not be parsed or was invalid
* ERR_BAD_RSYNC (2)       An rsync call made by the program returned an error
* ERR_NO_RESTART (4)      An attempt to restart a PostgreSQL instance failed
* ERR_DB_CON (6)          Error when trying to connect to a database
* ERR_DB_QUERY (7)        Error while executing a database query
* ERR_PROMOTED (8)        Exiting program because the node has been promoted to master
* ERR_BAD_PASSWORD (9)    Password used to connect to a database was rejected
* ERR_STR_OVERFLOW (10)   String overflow error
* ERR_FAILOVER_FAIL (11)  Error encountered during failover (repmgrd only)
* ERR_BAD_SSH (12)        Error when connecting to remote host via SSH
* ERR_SYS_FAILURE (13)    Error when forking (repmgrd only)
* ERR_BAD_BASEBACKUP (14) Error when executing pg_basebackup


Support and Assistance
----------------------

2ndQuadrant provides 24x7 production support for repmgr, including
configuration assistance, installation verification and training for
running a robust replication cluster. For further details see:

* http://2ndquadrant.com/en/support/

There is a mailing list/forum to discuss contributions or issues
http://groups.google.com/group/repmgr

The IRC channel #repmgr is registered with freenode.

Further information is available at http://www.repmgr.org/

We'd love to hear from you about how you use repmgr. Case studies and
news are always welcome. Send us an email at info@2ndQuadrant.com, or
send a postcard to

    repmgr
    c/o 2ndQuadrant
    7200 The Quorum
    Oxford Business Park North
    Oxford
    OX4 2JZ
    United Kingdom

Thanks from the repmgr core team.

* Ian Barwick
* Jaime Casanova
* Abhijit Menon-Sen
* Simon Riggs
* Cedric Villemain

Further reading
---------------

* http://blog.2ndquadrant.com/announcing-repmgr-2-0/
* http://blog.2ndquadrant.com/managing-useful-clusters-repmgr/
* http://blog.2ndquadrant.com/easier_postgresql_90_clusters/
