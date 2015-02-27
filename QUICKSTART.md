repmgr: Quickstart guide
========================

`repmgr` is an open-source tool suite for mananaging replication and failover
among multiple PostgreSQL server nodes. It enhances PostgreSQL's built-in
hot-standby capabilities with a set of administration tools for monitoring
replication, setting up standby servers and performing failover/switchover
operations.

This quickstart guide assumes you are familiar with PostgreSQL replication
setup and Linux/UNIX system administration. For a more detailed tutorial
covering setup on a variety of different systems, see the README.rst file.

Conceptual Overview
-------------------

`repmgr` provides two binaries:

 - `repmgr`: a command-line client to manage replication and `repmgr` configuration
 - `repmgrd`: an optional daemon process which runs on standby nodes to monitor
   replication and node status

Each PostgreSQL node requires a `repmgr.conf` configuration file; additionally
it must be "registered" using the `repmgr` command-line client. `repmgr` stores
information about managed nodes in a custom schema on the node's current master
database.


Requirements
------------

`repmgr` works with PostgreSQL 9.0 and later. All server nodes must be running the
same PostgreSQL major version, and preferably should be running the same minor
version.

`repmgr` will work on any Linux or UNIX-like environment capable of running
PostgreSQL. `rsync` must also be installed.


Installation
------------

`repmgr` must be installed on each PostgreSQL server node.

* Packages
 - RPM packages for RedHat-based distributions are available from PGDG
 - Debian/Ubuntu provide .deb packages.

  It is also possible to build .deb packages directly from the `repmgr` source;
  see README.rst for further details.

* Source installation
 - `repmgr` source code is hosted at github (https://github.com/2ndQuadrant/repmgr);
   tar.gz files can be downloaded from https://github.com/2ndQuadrant/repmgr/releases .

   `repmgr` can be built easily using PGXS:

       sudo make USE_PGXS=1 install


Configuration
-------------

### Server configuration

Password-less SSH logins must be enabled for the database system user (typically `postgres`)
between all server nodes to enable `repmgr` to copy required files.

### PostgreSQL configuration

The master PostgreSQL node needs to be configured for replication with the
following settings:

    wal_level = 'hot_standby'      # minimal, archive, hot_standby, or logical
    archive_mode = on              # allows archiving to be done
    archive_command = 'cd .'       # command to use to archive a logfile segment
    max_wal_senders = 10           # max number of walsender processes
    wal_keep_segments = 5000       # in logfile segments, 16MB each; 0 disables
    hot_standby = on               # "on" allows queries during recovery

Note that `repmgr` expects a default of 5000 wal_keep_segments, although this
value can be overridden when executing the `repmgr` client.

Additionally, `repmgr` requires a dedicated PostgreSQL superuser account
and a database in which to store monitoring and replication data. The `repmgr`
user account will also be used for replication connections from the standby,
so a seperate replication user with the `REPLICATION` privilege is not required.
The database can in principle be any database, including the default `postgres`
one, however it's probably advisable to create a dedicated database for `repmgr`
usage.


### repmgr configuration

Each PostgreSQL node requires a `repmgr.conf` configuration file containing
identification and database connection information:

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
`-f, --config-file=PATH` option and can have any arbitrary name. If no
configuration file is specified, `repmgr` will search for `repmgr.conf`
in the current working directory.

Each node configuration needs to be registered with `repmgr`, either using the
`repmgr` command line tool, or the `repmgrd` daemon; for details see below. Details
about each node are inserted into the `repmgr` database (for details see below).


Replication setup and monitoring
--------------------------------

For the purposes of this guide, we'll assume the database user will be
`repmgr_usr` and the database will be `repmgr_db`, and that the following
environment variables are set on each node:

 - $HOME: the PostgreSQL system user's home directory
 - $PGDATA: the PostgreSQL data directory


Master setup
------------

1. Configure PostgreSQL

  - create user and database:

	```
	CREATE ROLE repmgr_usr LOGIN SUPERUSER;
	CREATE DATABASE repmgr_db OWNER repmgr_usr;
	```

  - configure `postgresql.conf` for replication (see above)

  - update `pg_hba.conf`, e.g.:

	```
	host    repmgr_db       repmgr_usr  192.168.1.0/24         trust
	host    replication     repmgr_usr  192.168.1.0/24         trust
	```

  Restart the PostgreSQL server after making these changes.
2. Create the `repmgr` configuration file:

        $ cat $HOME/repmgr/repmgr.conf
        cluster=test
        node=1
        node_name=node1
        conninfo='host=repmgr_node1 user=repmgr_usr dbname=repmgr_db'
        pg_bindir=/path/to/postgres/bin

   (For an annotated `repmgr.conf` file, see `repmgr.conf.sample` in the
   repository's root directory).

3. Register the master node with `repmgr`:

        $ repmgr -f $HOME/repmgr/repmgr.conf --verbose master register
        [2014-07-04 10:43:42] [INFO] repmgr mgr connecting to master database
        [2014-07-04 10:43:42] [INFO] repmgr connected to master, checking its state
        [2014-07-04 10:43:42] [INFO] master register: creating database objects inside the repmgr_test schema
        [2014-07-04 10:43:43] [NOTICE] Master node correctly registered for cluster test with id 1 (conninfo: host=localhost user=repmgr_usr dbname=repmgr_db)


Slave/standby setup
-------------------

1. Use `repmgr` to clone the master:

        $ repmgr -D $PGDATA -d repmgr_db -U repmgr_usr -R postgres --verbose standby clone 192.168.1.2
        Opening configuration file: ./repmgr.conf
        [2014-07-04 10:49:00] [ERROR] Did not find the configuration file './repmgr.conf', continuing
        [2014-07-04 10:49:00] [INFO] repmgr connecting to master database
        [2014-07-04 10:49:00] [INFO] repmgr connected to master, checking its state
        [2014-07-04 10:49:00] [INFO] Successfully connected to primary. Current installation size is 1807 MB
        [2014-07-04 10:49:00] [NOTICE] Starting backup...
        [2014-07-04 10:49:00] [INFO] creating directory "/path/to/data/"...
        (...)
        [2014-07-04 10:53:19] [NOTICE] Finishing backup...
        NOTICE:  pg_stop_backup complete, all required WAL segments have been archived
        [2014-07-04 10:53:21] [INFO] repmgr requires primary to keep WAL files 0000000100000000000000AD until at least 0000000100000000000000AD
        [2014-07-04 10:53:21] [NOTICE] repmgr standby clone complete
        [2014-07-04 10:53:21] [NOTICE] HINT: You can now start your postgresql server
        [2014-07-04 10:53:21] [NOTICE] for example : /etc/init.d/postgresql start

  -R is the database system user on the master node. At this point it does not matter
  if the `repmgr.conf` file is not found.

  This will clone the PostgreSQL database files from the master, including its
  `postgresql.conf` and `pg_hba.conf` files, and additionally automatically create
  the `recovery.conf` file containing the correct parameters to start streaming
  from the primary node.

2. Start the PostgreSQL server

3. Create the `repmgr` configuration file:

        $ cat $HOME/repmgr/repmgr.conf
        cluster=test
        node=2
        node_name=node2
        conninfo='host=repmgr_node2 user=repmgr_usr dbname=repmgr_db'
        pg_bindir=/path/to/postgres/bin

4. Register the master node with `repmgr`:

        $ repmgr -f $HOME/repmgr/repmgr.conf --verbose standby register
        Opening configuration file: /path/to/repmgr/repmgr.conf
        [2014-07-04 11:48:13] [INFO] repmgr connecting to standby database
        [2014-07-04 11:48:13] [INFO] repmgr connected to standby, checking its state
        [2014-07-04 11:48:13] [INFO] repmgr connecting to master database
        [2014-07-04 11:48:13] [INFO] finding node list for cluster 'test'
        [2014-07-04 11:48:13] [INFO] checking role of cluster node 'host=repmgr_node1 user=repmgr_usr dbname=repmgr_db'
        [2014-07-04 11:48:13] [INFO] repmgr connected to master, checking its state
        [2014-07-04 11:48:13] [INFO] repmgr registering the standby
        [2014-07-04 11:48:13] [INFO] repmgr registering the standby complete
        [2014-07-04 11:48:13] [NOTICE] Standby node correctly registered for cluster test with id 2 (conninfo: host=localhost user=repmgr_usr dbname=repmgr_db)

Monitoring
----------

`repmgrd` is a management and monitoring daemon which runs on standby nodes
and which and can automate remote actions. It can be started simply with e.g.:

    repmgrd -f $HOME/repmgr/repmgr.conf --verbose > $HOME/repmgr/repmgr.log 2>&1

or alternatively:

    repmgrd -f $HOME/repmgr/repmgr.conf --verbose --monitoring-history > $HOME/repmgr/repmgrd.log 2>&1

which will track advance or lag of the replication in every standby in the
`repl_monitor` table.

Example log output:

    [2014-07-04 11:55:17] [INFO] repmgrd Connecting to database 'host=localhost user=repmgr_usr dbname=repmgr_db'
    [2014-07-04 11:55:17] [INFO] repmgrd Connected to database, checking its state
    [2014-07-04 11:55:17] [INFO] repmgrd Connecting to primary for cluster 'test'
    [2014-07-04 11:55:17] [INFO] finding node list for cluster 'test'
    [2014-07-04 11:55:17] [INFO] checking role of cluster node 'host=repmgr_node1 user=repmgr_usr dbname=repmgr_db'
    [2014-07-04 11:55:17] [INFO] repmgrd Checking cluster configuration with schema 'repmgr_test'
    [2014-07-04 11:55:17] [INFO] repmgrd Checking node 2 in cluster 'test'
    [2014-07-04 11:55:17] [INFO] Reloading configuration file and updating repmgr tables
    [2014-07-04 11:55:17] [INFO] repmgrd Starting continuous standby node monitoring


Failover
--------

To promote a standby to master, on the standby execute e.g.:

    repmgr -f  $HOME/repmgr/repmgr.conf --verbose standby promote

`repmgr` will attempt to connect to the current master to verify that it
is not available (if it is, `repmgr` will not promote the standby).

Other standby servers need to be told to follow the new master with:

    repmgr -f  $HOME/repmgr/repmgr.conf --verbose standby follow

See file `autofailover_quick_setup.rst` for details on setting up
automated failover.


repmgr database schema
----------------------

`repmgr` creates a small schema for its own use in the database specified in
each node's conninfo configuration parameter. This database can in principle
be any database. The schema name is the global `cluster` name prefixed
with `repmgr_`, so for the example setup above the schema name is
`repmgr_test`.

The schema contains two tables:

* `repl_nodes`
  stores information about all registered servers in the cluster
* `repl_monitor`
  stores monitoring information about each node

and one view, `repl_status`, which summarizes the latest monitoring information
for each node.


Further reading
---------------

* http://blog.2ndquadrant.com/announcing-repmgr-2-0/
* http://blog.2ndquadrant.com/managing-useful-clusters-repmgr/
* http://blog.2ndquadrant.com/easier_postgresql_90_clusters/

