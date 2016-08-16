repmgr: Replication Manager for PostgreSQL
==========================================

`repmgr` is a suite of open-source tools to manage replication and failover
within a cluster of PostgreSQL servers. It enhances PostgreSQL's built-in
replication capabilities with utilities to set up standby servers, monitor
replication, and perform administrative tasks such as failover or switchover
operations.


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


`repmgr` supports and enhances PostgreSQL's built-in streaming replication, which
provides a single read/write master server and one or more read-only standbys
containing near-real time copies of the master server's database.

For a multi-master replication solution, please see 2ndQuadrant's BDR
(bi-directional replication) extension.

http://2ndquadrant.com/en-us/resources/bdr/

For selective replication, e.g. of individual tables or databases from one server
to another, please see 2ndQuadrant's pglogical extension.

http://2ndquadrant.com/en-us/resources/pglogical/

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

- `witness server`

`repmgr` provides functionality to set up a so-called "witness server" to
assist in determining a new master server in a failover situation with more
than one standby. The witness server itself is not part of the replication
cluster, although it does contain a copy of the repmgr metadata schema
(see below).

The purpose of a witness server is to provide a "casting vote" where servers
in the replication cluster are split over more than one location. In the event
of a loss of connectivity between locations, the presence or absence of
the witness server will decide whether a server at that location is promoted
to master; this is to prevent a "split-brain" situation where an isolated
location interprets a network outage as a failure of the (remote) master and
promotes a (local) standby.

A witness server only needs to be created if `repmgrd` is in use.

### repmgr user and metadata

In order to effectively manage a replication cluster, `repmgr` needs to store
information about the servers in the cluster in a dedicated database schema.
This schema is automatically created during the first step in initialising
a `repmgr`-controlled cluster (`repmgr master register`) and contains the
following objects:

tables:
  - `repl_events`: records events of interest
  - `repl_nodes`: connection and status information for each server in the
    replication cluster
  - `repl_monitor`: historical standby monitoring information written by `repmgrd`

views:
  - `repl_show_nodes`: based on the table `repl_nodes`, additionally showing the
     name of the server's  upstream node
  - `repl_status`: when `repmgrd`'s monitoring is enabled, shows current monitoring
    status for each node

The `repmgr` metadata schema can be stored in an existing database or in its own
dedicated database.

A dedicated database superuser is required to own the meta-database as well as carry
out administrative actions.

Installation
------------

### System requirements

`repmgr` is developed and tested on Linux and OS X, but should work on any
UNIX-like system supported by PostgreSQL itself.

Current versions of `repmgr` support PostgreSQL from version 9.3. If you are
interested in using `repmgr` on earlier versions of PostgreSQL you can download
version 2.1 which supports PostgreSQL from version 9.1.

All servers in the replication cluster must be running the same major version of
PostgreSQL, and we recommend that they also run the same minor version.

The `repmgr` tools must be installed on each server in the replication cluster.

A dedicated system user for `repmgr` is *not* required; as many `repmgr` and
`repmgrd` actions require direct access to the PostgreSQL data directory,
it should be executed by the `postgres` user.

Additionally, we recommend installing `rsync` and enabling passwordless
`ssh` connectivity between all servers in the replication cluster.

* * *

> *TIP*: We recommend using a session multiplexer utility such as `screen` or
> `tmux` when performing long-running actions (such as cloning a database)
> on a remote server - this will ensure the `repmgr` action won't be prematurely
> terminated if your `ssh` session to the server is interrupted or closed.

* * *

### Packages

We recommend installing `repmgr` using the available packages for your
system.

- RedHat/CentOS: RPM packages for `repmgr` are available via Yum through
  the PostgreSQL Global Development Group RPM repository ( http://yum.postgresql.org/ ).
  Follow the instructions for your distribution (RedHat, CentOS,
  Fedora, etc.) and architecture as detailed at yum.postgresql.org.

  2ndQuadrant also provides its own RPM packages which are made available
  at the same time as each `repmgr` release, as it can take some days for
  them to become available via the main PGDG repository. See here for details:

     http://repmgr.org/yum-repository.html

- Debian/Ubuntu: the most recent `repmgr` packages are available from the
  PostgreSQL Community APT repository ( http://apt.postgresql.org/ ).
  Instructions can be found in the APT section of the PostgreSQL Wiki
  ( https://wiki.postgresql.org/wiki/Apt ).

See `PACKAGES.md` for details on building .deb and .rpm packages from the
`repmgr` source code.


### Source installation

`repmgr` source code can be obtained directly from the project GitHub repository:

    git clone https://github.com/2ndQuadrant/repmgr

Release tarballs are also available:

    https://github.com/2ndQuadrant/repmgr/releases
    http://repmgr.org/downloads.php

`repmgr` is compiled in the same way as a PostgreSQL extension using the PGXS
infrastructure, e.g.:

    sudo make USE_PGXS=1 install

`repmgr` can be built from source in any environment suitable for building
PostgreSQL itself.


### Configuration

`repmgr` and `repmgrd` use a common configuration file, by default called
`repmgr.conf` (although any name can be used if explicitly specified).
At the very least, `repmgr.conf` must contain the connection parameters
for the local `repmgr` database; see `repmgr configuration file` below
for more details.

The configuration file will be searched for in the following locations:

- a configuration file specified by the `-f/--config-file` command line option
- `repmgr.conf` in the local directory
- `/etc/repmgr.conf`
- the directory reported by `pg_config --sysconfdir`

Note that if a file is explicitly specified with `-f/--config-file`, an error will
be raised if it is not found or not readable and no attempt will be made to check
default locations; this is to prevent `repmgr` reading the wrong file.

For a full list of annotated configuration items, see the file `repmgr.conf.sample`.

The following parameters in the configuration file can be overridden with
command line options:

- `-L/--log-level`
- `-b/--pg_bindir`


### Command line options and environment variables

For some commands, e.g. `repmgr standby clone`, database connection parameters
need to be provided. Like other PostgreSQL utilities, following standard
parameters can be used:

- `-d/--dbname=DBNAME`
- `-h/--host=HOSTNAME`
- `-p/--port=PORT`
- `-U/--username=USERNAME`

If `-d/--dbname` contains an `=` sign or starts with a valid URI prefix (`postgresql://`
or `postgres://`), it is treated as a conninfo string. See the PostgreSQL
documentation for further details:

  https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING

Note that if a `conninfo` string is provided, values set in this will override any
provided as individual parameters. For example, with `-d 'host=foo' --host bar`, `foo`
will be chosen over `bar`.

Like other PostgreSQL utilities, `repmgr` will default to any values set in environment
variables if explicit command line parameters are not provided. See the PostgreSQL
documentation for further details:

  https://www.postgresql.org/docs/current/static/libpq-envars.html


Setting up a simple replication cluster with repmgr
---------------------------------------------------

The following section will describe how to set up a basic replication cluster
with a master and a standby server using the `repmgr` command line tool.
It is assumed PostgreSQL is installed on both servers in the cluster,
`rsync` is available and password-less SSH connections are possible between
both servers.

* * *

> *TIP*: for testing `repmgr`, it's possible to use multiple PostgreSQL
> instances running on different ports on the same computer, with
> password-less SSH access to `localhost` enabled.

* * *

### PostgreSQL configuration

On the master server, a PostgreSQL instance must be initialised and running.
The following replication settings must be included in `postgresql.conf`:


    # Enable replication connections; set this figure to at least one more
    # than the number of standbys which will connect to this server
    # (note that repmgr will execute `pg_basebackup` in WAL streaming mode,
    # which requires two free WAL senders)

    max_wal_senders = 10

    # Ensure WAL files contain enough information to enable read-only queries
    # on the standby

    wal_level = 'hot_standby'

    # How much WAL to retain on the master to allow a temporarily
    # disconnected standby to catch up again. The larger this is, the
    # longer the standby can be disconnected. This is needed only in
    # 9.3; from 9.4, replication slots can be used instead (see below).

    wal_keep_segments = 5000

    # Enable read-only queries on a standby
    # (Note: this will be ignored on a master but we recommend including
    # it anyway)

    hot_standby = on

    # Enable WAL file archiving
    archive_mode = on

    # Set archive command to a script or application that will safely store
    # you WALs in a secure place. /bin/true is an example of a command that
    # ignores archiving. Use something more sensible.
    archive_command = '/bin/true'


* * *

> *TIP*: rather than editing these settings in the default `postgresql.conf`
> file, create a separate file such as `postgresql.replication.conf` and
> include it from the end of the main configuration file with:
> `include 'postgresql.replication.conf'`

* * *

Create a dedicated PostgreSQL superuser account and a database for
the `repmgr` metadata, e.g.

    createuser -s repmgr
    createdb repmgr -O repmgr

For the examples in this document, the name `repmgr` will be used for both
user and database, but any names can be used.

Ensure the `repmgr` user has appropriate permissions in `pg_hba.conf` and
can connect in replication mode; `pg_hba.conf` should contain entries
similar to the following:

    local   replication   repmgr                              trust
    host    replication   repmgr      127.0.0.1/32            trust
    host    replication   repmgr      192.168.1.0/24          trust

    local   repmgr        repmgr                              trust
    host    repmgr        repmgr      127.0.0.1/32            trust
    host    repmgr        repmgr      192.168.1.0/24          trust

Adjust according to your network environment and authentication requirements.

On the standby, do not create a PostgreSQL instance, but do ensure an empty
directory is available for the `postgres` system user to create a data
directory.


### repmgr configuration file

Create a `repmgr.conf` file on the master server. The file must contain at
least the following parameters:

    cluster=test
    node=1
    node_name=node1
    conninfo='host=repmgr_node1 user=repmgr dbname=repmgr'

- `cluster`: an arbitrary name for the replication cluster; this must be identical
     on all nodes
- `node`: a unique integer identifying the node
- `node_name`: a unique string identifying the node; we recommend a name
     specific to the server (e.g. 'server_1'); avoid names indicating the
     current replication role like 'master' or 'standby' as the server's
     role could change.
- `conninfo`: a valid connection string for the `repmgr` database on the
     *current* server. (On the standby, the database will not yet exist, but
     `repmgr` needs to know the connection details to complete the setup
     process).

`repmgr.conf` should not be stored inside the PostgreSQL data directory,
as it could be overwritten when setting up or reinitialising the PostgreSQL
server. See section `Configuration` above for further details about `repmgr.conf`.

`repmgr` will create a schema named after the cluster and prefixed with `repmgr_`,
e.g. `repmgr_test`; we also recommend that you set the `repmgr` user's search path
to include this schema name, e.g.

    ALTER USER repmgr SET search_path TO repmgr_test, "$user", public;

* * *

> *TIP*: for Debian-based distributions we recommend explictly setting
> `pg_bindir` to the directory where `pg_ctl` and other binaries not in
> the standard path are located. For PostgreSQL 9.5 this would be
> `/usr/lib/postgresql/9.5/bin/`.

* * *


### Initialise the master server

To enable `repmgr` to support a replication cluster, the master node must
be registered with `repmgr`, which creates the `repmgr` database and adds
a metadata record for the server:

    $ repmgr -f repmgr.conf master register
    [2016-01-07 16:56:46] [NOTICE] master node correctly registered for cluster test with id 1 (conninfo: host=repmgr_node1 user=repmgr dbname=repmgr)

The metadata record looks like this:

    repmgr=# SELECT * FROM repmgr_test.repl_nodes;
     id |  type   | upstream_node_id | cluster | name  |                  conninfo                   | slot_name | priority | active
    ----+---------+------------------+---------+-------+---------------------------------------------+-----------+----------+--------
      1 | master  |                  | test    | node1 | host=repmgr_node1 dbname=repmgr user=repmgr |           |      100 | t
    (1 row)

Each server in the replication cluster will have its own record and will be updated
when its status or role changes.

### Clone the standby server

Create a `repmgr.conf` file on the standby server. It must contain at
least the same parameters as the master's `repmgr.conf`, but with
the values `node`, `node_name` and `conninfo` adjusted accordingly, e.g.:

    cluster=test
    node=2
    node_name=node2
    conninfo='host=repmgr_node2 user=repmgr dbname=repmgr'

Clone the standby with:

    $ repmgr -h repmgr_node1 -U repmgr -d repmgr -D /path/to/node2/data/ -f /etc/repmgr.conf standby clone
    [2016-01-07 17:21:26] [NOTICE] destination directory '/path/to/node2/data/' provided
    [2016-01-07 17:21:26] [NOTICE] starting backup...
    [2016-01-07 17:21:26] [HINT] this may take some time; consider using the -c/--fast-checkpoint option
    NOTICE:  pg_stop_backup complete, all required WAL segments have been archived
    [2016-01-07 17:21:28] [NOTICE] standby clone (using pg_basebackup) complete
    [2016-01-07 17:21:28] [NOTICE] you can now start your PostgreSQL server
    [2016-01-07 17:21:28] [HINT] for example : pg_ctl -D /path/to/node2/data/ start

This will clone the PostgreSQL data directory files from the master at `repmgr_node1`
using PostgreSQL's `pg_basebackup` utility. A `recovery.conf` file containing the
correct parameters to start streaming from this master server will be created
automatically, and unless otherwise specified, the `postgresql.conf` and `pg_hba.conf`
files will be copied from the master.

Be aware that when initially cloning a standby, you will need to ensure
that all required WAL files remain available while the cloning is taking
place. To ensure this happens when using the default `pg_basebackup` method,
`repmgr` will set `pg_basebackup`'s `--xlog-method` parameter to `stream`,
which will ensure all WAL files generated during the cloning process are
streamed in parallel with the main backup. Note that this requires two
replication connections to be available.

To override this behaviour, in `repmgr.conf` set `pg_basebackup`'s
`--xlog-method` parameter to `fetch`:

    pg_basebackup_options='--xlog-method=fetch'

and ensure that `wal_keep_segments` is set to an appropriately high value.
See the `pg_basebackup` documentation for details:

    https://www.postgresql.org/docs/current/static/app-pgbasebackup.html

Make any adjustments to the standby's PostgreSQL configuration files now,
then start the server.

* * *

> *NOTE*: `repmgr standby clone` does not require `repmgr.conf`, however we
> recommend providing this as `repmgr` will set the `application_name` parameter
> in `recovery.conf` as the value provided in `node_name`, making it easier to
> identify the node in `pg_stat_replication`. It's also possible to provide some
> advanced options for controlling the standby cloning process; see next section
> for details.

* * *

### Verify replication is functioning

Connect to the master server and execute:

    repmgr=# SELECT * FROM pg_stat_replication;
    -[ RECORD 1 ]----+------------------------------
    pid              | 7704
    usesysid         | 16384
    usename          | repmgr
    application_name | node2
    client_addr      | 192.168.1.2
    client_hostname  |
    client_port      | 46196
    backend_start    | 2016-01-07 17:32:58.322373+09
    backend_xmin     |
    state            | streaming
    sent_location    | 0/3000220
    write_location   | 0/3000220
    flush_location   | 0/3000220
    replay_location  | 0/3000220
    sync_priority    | 0
    sync_state       | async


### Register the standby

Register the standby server with:

    repmgr -f /etc/repmgr.conf standby register
    [2016-01-08 11:13:16] [NOTICE] standby node correctly registered for cluster test with id 2 (conninfo: host=repmgr_node2 user=repmgr dbname=repmgr)

Connect to the standby server's `repmgr` database and check the `repl_nodes`
table:

    repmgr=# SELECT * FROM repmgr_test.repl_nodes ORDER BY id;
     id |  type   | upstream_node_id | cluster | name  |                  conninfo                   | slot_name | priority | active
    ----+---------+------------------+---------+-------+---------------------------------------------+-----------+----------+--------
      1 | master  |                  | test    | node1 | host=repmgr_node1 dbname=repmgr user=repmgr |           |      100 | t
      2 | standby |                1 | test    | node2 | host=repmgr_node2 dbname=repmgr user=repmgr |           |      100 | t
    (2 rows)

The standby server now has a copy of the records for all servers in the
replication cluster. Note that the relationship between master and standby is
explicitly defined via the `upstream_node_id` value, which shows here that the
standby's upstream server is the replication cluster master. While of limited
use in a simple master/standby replication cluster, this information is required
to effectively manage cascading replication (see below).


Advanced options for cloning a standby
--------------------------------------

The above section demonstrates the simplest possible way to cloneb a standby
server. Depending on your circumstances, finer-grained controlover the cloning
process may be necessary.

### pg_basebackup options when cloning a standby

By default, `pg_basebackup` performs a checkpoint before beginning the backup
process. However, a normal checkpoint may take some time to complete;
a fast checkpoint can be forced with the `-c/--fast-checkpoint` option.
However this may impact performance of the server being cloned from
so should be used with care.

Further options can be passed to the `pg_basebackup` utility via
the setting `pg_basebackup_options` in `repmgr.conf`. See the PostgreSQL
documentation for more details of available options:
  https://www.postgresql.org/docs/current/static/app-pgbasebackup.html

### Using rsync to clone a standby

By default `repmgr` uses the `pg_basebackup` utility to clone a standby's
data directory from the master. Under some circumstances it may be
desirable to use `rsync` to do this, such as when resyncing the data
directory of a failed server with an active replication node.

To use `rsync` instead of `pg_basebackup`, provide the `-r/--rsync-only`
option when executing `repmgr standby clone`.

Note that `repmgr` forces `rsync` to use `--checksum` mode to ensure that all
the required files are copied. This results in additional I/O on both source
and destination server as the contents of files existing on both servers need
to be compared, meaning this method is not necessarily faster than making a
fresh clone with `pg_basebackup`.

### Dealing with PostgreSQL configuration files

By default, `repmgr` will attempt to copy the standard configuration files
(`postgresql.conf`, `pg_hba.conf` and `pg_ident.conf`) even if they are located
outside of the data directory (though currently they will be copied
into the standby's data directory). To prevent this happening, when executing
`repmgr standby clone` provide the `--ignore-external-config-files` option.

If using `rsync` to clone a standby, additional control over which files
not to transfer is possible by configuring `rsync_options` in `repmgr.conf`,
which enables any valid `rsync` options to be passed to that command, e.g.:

    rsync_options='--exclude=postgresql.local.conf'

### Controlling `primary_conninfo` in `recovery.conf`

`repmgr` will create the `primary_conninfo` setting in `recovery.conf` based
on the connection parameters provided to `repmgr standby clone` and PostgreSQL's
standard connection defaults, including any environment variables set on the
local node.

To include specific connection parameters other than the standard host, port,
username and database values (e.g. `sslmode`), include these in a `conninfo`-style
tring passed to `repmgr` with `-d/--dbname` (see above for details), and/or set
appropriate environment variables.

Note that PostgreSQL will always set explicit defaults for `sslmode` and
`sslcompression`.


Setting up cascading replication with repmgr
--------------------------------------------

Cascading replication, introduced with PostgreSQL 9.2, enables a standby server
to replicate from another standby server rather than directly from the master,
meaning replication changes "cascade" down through a hierarchy of servers. This
can be used to reduce load on the master and minimize bandwith usage between
sites.

`repmgr` supports cascading replication. When cloning a standby, in `repmgr.conf`
set the parameter `upstream_node` to the id of the server the standby
should connect to, and `repmgr` will perform the clone using this server
and create `recovery.conf` to point to it. Note that if `upstream_node`
is not explicitly provided, `repmgr` will use the master as the server
to clone from.

To demonstrate cascading replication, ensure you have a master and standby
set up as shown above in the section "Setting up a simple replication cluster
with repmgr". Create an additional standby server with `repmgr.conf` looking
like this:

    cluster=test
    node=3
    node_name=node3
    conninfo='host=repmgr_node3 user=repmgr dbname=repmgr'
    upstream_node=2

Ensure `upstream_node` contains the `node` id of the previously
created standby. Clone this standby (using the connection parameters
for the existing standby) and register it:

    $ repmgr -h repmgr_node2 -U repmgr -d repmgr -D /path/to/node3/data/ -f /etc/repmgr.conf standby clone
    [2016-01-08 13:44:52] [NOTICE] destination directory 'node_3/data/' provided
    [2016-01-08 13:44:52] [NOTICE] starting backup (using pg_basebackup)...
    [2016-01-08 13:44:52] [HINT] this may take some time; consider using the -c/--fast-checkpoint option
    [2016-01-08 13:44:52] [NOTICE] standby clone (using pg_basebackup) complete
    [2016-01-08 13:44:52] [NOTICE] you can now start your PostgreSQL server
    [2016-01-08 13:44:52] [HINT] for example : pg_ctl -D /path/to/node_3/data start

    $ repmgr -f /etc/repmgr.conf standby register
    [2016-01-08 14:04:32] [NOTICE] standby node correctly registered for cluster test with id 3 (conninfo: host=repmgr_node3 dbname=repmgr user=repmgr)

After starting the standby, the `repl_nodes` table will look like this:

    repmgr=# SELECT * FROM repmgr_test.repl_nodes ORDER BY id;
     id |  type   | upstream_node_id | cluster | name  |                  conninfo                   | slot_name | priority | active
    ----+---------+------------------+---------+-------+---------------------------------------------+-----------+----------+--------
      1 | master  |                  | test    | node1 | host=repmgr_node1 dbname=repmgr user=repmgr |           |      100 | t
      2 | standby |                1 | test    | node2 | host=repmgr_node2 dbname=repmgr user=repmgr |           |      100 | t
      3 | standby |                2 | test    | node3 | host=repmgr_node3 dbname=repmgr user=repmgr |           |      100 | t
    (3 rows)


Using replication slots with repmgr
-----------------------------------

Replication slots were introduced with PostgreSQL 9.4 and are designed to ensure
that any standby connected to the master using a replication slot will always
be able to retrieve the required WAL files. This removes the need to manually
manage WAL file retention by estimating the number of WAL files that need to
be maintained on the master using `wal_keep_segments`. Do however be aware
that if a standby is disconnected, WAL will continue to accumulate on the master
until either the standby reconnects or the replication slot is dropped.

To enable `repmgr` to use replication slots, set the boolean parameter
`use_replication_slots` in `repmgr.conf`:

    use_replication_slots=1

Note that `repmgr` will fail with an error if this option is specified when
working with PostgreSQL 9.3.

Replication slots must be enabled in `postgresql.conf` by setting the parameter
`max_replication_slots` to at least the number of expected standbys (changes
to this parameter require a server restart).

When cloning a standby, `repmgr` will automatically generate an appropriate
slot name, which is stored in the `repl_nodes` table, and create the slot
on the master:

    repmgr=# SELECT * from repl_nodes ORDER BY id;
     id |  type   | upstream_node_id | cluster | name  |                 conninfo                 |   slot_name   | priority | active
    ----+---------+------------------+---------+-------+------------------------------------------+---------------+----------+--------
      1 | master  |                  | test    | node1 | host=localhost dbname=repmgr user=repmgr | repmgr_slot_1 |      100 | t
      2 | standby |                1 | test    | node2 | host=localhost dbname=repmgr user=repmgr | repmgr_slot_2 |      100 | t
      3 | standby |                1 | test    | node3 | host=localhost dbname=repmgr user=repmgr | repmgr_slot_3 |      100 | t

    repmgr=# SELECT * FROM pg_replication_slots ;
       slot_name   | plugin | slot_type | datoid | database | active | active_pid | xmin | catalog_xmin | restart_lsn
    ---------------+--------+-----------+--------+----------+--------+------------+------+--------------+-------------
     repmgr_slot_3 |        | physical  |        |          | t      |      26060 |      |              | 0/50028F0
     repmgr_slot_2 |        | physical  |        |          | t      |      26079 |      |              | 0/50028F0
    (2 rows)

Note that a slot name will be created by default for the master but not
actually used unless the master is converted to a standby using e.g.
`repmgr standby switchover`.


Further information on replication slots in the PostgreSQL documentation:
    https://www.postgresql.org/docs/current/interactive/warm-standby.html#STREAMING-REPLICATION-SLOTS


Promoting a standby server with repmgr
--------------------------------------

If a master server fails or needs to be removed from the replication cluster,
a new master server must be designated, to ensure the cluster continues
working correctly. This can be done with `repmgr standby promote`, which promotes
the standby on the current server to master

To demonstrate this, set up a replication cluster with a master and two attached
standby servers so that the `repl_nodes` table looks like this:

    repmgr=# SELECT * FROM repmgr_test.repl_nodes ORDER BY id;
     id |  type   | upstream_node_id | cluster | name  |                  conninfo                   | slot_name | priority | active
    ----+---------+------------------+---------+-------+---------------------------------------------+-----------+----------+--------
      1 | master  |                  | test    | node1 | host=repmgr_node1 dbname=repmgr user=repmgr |           |      100 | t
      2 | standby |                1 | test    | node2 | host=repmgr_node2 dbname=repmgr user=repmgr |           |      100 | t
      3 | standby |                1 | test    | node3 | host=repmgr_node3 dbname=repmgr user=repmgr |           |      100 | t
    (3 rows)

Stop the current master with e.g.:

    $ pg_ctl -D /path/to/node_1/data -m fast stop

At this point the replication cluster will be in a partially disabled state with
both standbys accepting read-only connections while attempting to connect to the
stopped master. Note that the `repl_nodes` table will not yet have been updated
and will still show the master as active.

Promote the first standby with:

    $ repmgr -f /etc/repmgr.conf standby promote

This will produce output similar to the following:

    [2016-01-08 16:07:31] [ERROR] connection to database failed: could not connect to server: Connection refused
            Is the server running on host "repmgr_node1" (192.161.2.1) and accepting
            TCP/IP connections on port 5432?
    could not connect to server: Connection refused
            Is the server running on host "repmgr_node1" (192.161.2.1) and accepting
            TCP/IP connections on port 5432?

    [2016-01-08 16:07:31] [NOTICE] promoting standby
    [2016-01-08 16:07:31] [NOTICE] promoting server using '/usr/bin/postgres/pg_ctl -D /path/to/node_2/data promote'
    server promoting
    [2016-01-08 16:07:33] [NOTICE] STANDBY PROMOTE successful

Note: the first `[ERROR]` is `repmgr` attempting to connect to the current
master to verify that it has failed. If a valid master is found, `repmgr`
will refuse to promote a standby.

The `repl_nodes` table will now look like this:

     id |  type   | upstream_node_id | cluster | name  |                  conninfo                   | slot_name | priority | active
    ----+---------+------------------+---------+-------+---------------------------------------------+-----------+----------+--------
      1 | master  |                  | test    | node1 | host=repmgr_node1 dbname=repmgr user=repmgr |           |      100 | f
      2 | master  |                  | test    | node2 | host=repmgr_node2 dbname=repmgr user=repmgr |           |      100 | t
      3 | standby |                1 | test    | node3 | host=repmgr_node3 dbname=repmgr user=repmgr |           |      100 | t
    (3 rows)

The previous master has been marked as inactive, and `node2`'s `upstream_node_id`
has been cleared as it's now the "topmost" server in the replication cluster.

However the sole remaining standby is still trying to replicate from the failed
master; `repmgr standby follow` must now be executed to rectify this situation.


Following a new master server with repmgr
-----------------------------------------

Following the failure or removal of the replication cluster's existing master
server, `repmgr standby follow` can be used to make 'orphaned' standbys
follow the new master and catch up to its current state.

To demonstrate this, assuming a replication cluster in the same state as the
end of the preceding section ("Promoting a standby server with repmgr"),
execute this:

    $ repmgr -f /etc/repmgr.conf -D /path/to/node_3/data/ -h repmgr_node2 -U repmgr -d repmgr standby follow
    [2016-01-08 16:57:06] [NOTICE] restarting server using '/usr/bin/postgres/pg_ctl -D /path/to/node_3/data/ -w -m fast restart'
    waiting for server to shut down.... done
    server stopped
    waiting for server to start.... done
    server started

The standby is now replicating from the new master and `repl_nodes` has been
updated to reflect this:

     id |  type   | upstream_node_id | cluster | name  |                  conninfo                   | slot_name | priority | active
    ----+---------+------------------+---------+-------+---------------------------------------------+-----------+----------+--------
      1 | master  |                  | test    | node1 | host=repmgr_node1 dbname=repmgr user=repmgr |           |      100 | f
      2 | master  |                  | test    | node2 | host=repmgr_node2 dbname=repmgr user=repmgr |           |      100 | t
      3 | standby |                2 | test    | node3 | host=repmgr_node3 dbname=repmgr user=repmgr |           |      100 | t
    (3 rows)


Note that with cascading replication, `repmgr standby follow` can also be
used to detach a standby from its current upstream server and follow the
master. However it's currently not possible to have it follow another standby;
we hope to improve this in a future release.


Performing a switchover with repmgr
-----------------------------------

A typical use-case for replication is a combination of master and standby
server, with the standby serving as a backup which can easily be activated
in case of a problem with the master. Such an unplanned failover would
normally be handled by promoting the standby, after which an appropriate
action must be taken to restore the old master.

In some cases however it's desirable to promote the standby in a planned
way, e.g. so maintenance can be performed on the master; this kind of switchover
is supported by the `repmgr standby switchover` command.

`repmgr standby switchover` differs from other `repmgr` actions in that it
also performs actions on another server, for which reason you must provide
both passwordless SSH access and the path of `repmgr.conf` on that server.

* * *

> *NOTE* `repmgr standby switchover` performs a relatively complex series
> of operations on two servers, and should therefore be performed after
> careful preparation and with adequate attention. In particular you should
> be confident that your network environment is stable and reliable.
>
> We recommend running `repmgr standby switchover` at the most verbose
> logging level (`--log-level DEBUG --verbose`) and capturing all output
> to assist troubleshooting any problems.
>
> Please also read carefully the list of caveats below.

* * *

To demonstrate switchover, we will assume a replication cluster running on
PostgreSQL 9.5 or later with a master (`node1`) and a standby (`node2`);
after the switchover `node2` should become the master with `node1` following it.

The switchover command must be run from the standby which is to be promoted,
and in its simplest form looks like this:

   repmgr -f /etc/repmgr.conf -C /etc/repmgr.conf standby switchover

`-f /etc/repmgr.conf` is, as usual the local `repmgr` node's configuration file.
`-C /etc/repmgr.conf` is the path to the configuration file on the current
master, which is required to execute `repmgr` remotely on that server;
if it is not provided with `-C`, `repmgr` will check the same path as on the
local server, as well as the normal default locations. `repmgr` will check
this file can be found before performing any further actions.

    $ repmgr -f /etc/repmgr.conf -C /etc/repmgr.conf standby switchover -v
    [2016-01-27 16:38:33] [NOTICE] using configuration file "/etc/repmgr.conf"
    [2016-01-27 16:38:33] [NOTICE] switching current node 2 to master server and demoting current master to standby...
    [2016-01-27 16:38:34] [NOTICE] 5 files copied to /tmp/repmgr-node1-archive
    [2016-01-27 16:38:34] [NOTICE] connection to database failed: FATAL:  the database system is shutting down

    [2016-01-27 16:38:34] [NOTICE] current master has been stopped
    [2016-01-27 16:38:34] [ERROR] connection to database failed: FATAL:  the database system is shutting down

    [2016-01-27 16:38:34] [NOTICE] promoting standby
    [2016-01-27 16:38:34] [NOTICE] promoting server using '/usr/local/bin/pg_ctl -D /var/lib/postgresql/9.5/node_2/data promote'
    server promoting
    [2016-01-27 16:38:36] [NOTICE] STANDBY PROMOTE successful
    [2016-01-27 16:38:36] [NOTICE] Executing pg_rewind on old master server
    [2016-01-27 16:38:36] [NOTICE] 5 files copied to /var/lib/postgresql/9.5/data
    [2016-01-27 16:38:36] [NOTICE] restarting server using '/usr/local/bin/pg_ctl -w -D /var/lib/postgresql/9.5/node_1/data -m fast restart'
    pg_ctl: PID file "/var/lib/postgresql/9.5/node_1/data/postmaster.pid" does not exist
    Is server running?
    starting server anyway
    [2016-01-27 16:38:37] [NOTICE] node 1 is replicating in state "streaming"
    [2016-01-27 16:38:37] [NOTICE] switchover was successful

Messages containing the line `connection to database failed: FATAL: the database
system is shutting down` are not errors - `repmgr` is polling the old master database
to make sure it has shut down correctly. `repmgr` will also archive any
configuration files in the old master's data directory as they will otherwise
be overwritten by `pg_rewind`; they are restored once the `pg_rewind` operation
has completed.

The old master is now replicating as a standby from the new master and `repl_nodes`
should have been updated to reflect this:

    repmgr=# SELECT * from repl_nodes ORDER BY id;
     id |  type   | upstream_node_id | cluster | name  |                 conninfo                 | slot_name | priority | active
    ----+---------+------------------+---------+-------+------------------------------------------+-----------+----------+--------
      1 | standby |                2 | test    | node1 | host=localhost dbname=repmgr user=repmgr |           |      100 | t
      2 | master  |                  | test    | node2 | host=localhost dbname=repmgr user=repmgr |           |      100 | t
    (2 rows)


### Caveats

- The functionality provided `repmgr standby switchover` is primarily aimed
  at a two-server master/standby replication cluster and currently does
  not support additional standbys.
- `repmgr standby switchover` is designed to use the `pg_rewind` utility,
  standard in 9.5 and later and available for seperately in 9.3 and 9.4
  (see note below)
- `pg_rewind` *requires* that either `wal_log_hints` is enabled, or that
   data checksums were enabled when the cluster was initialized. See the
  `pg_rewind` documentation for details:
     https://www.postgresql.org/docs/current/static/app-pgrewind.html
- `repmgrd` should not be running when a switchover is carried out, otherwise
  the `repmgrd` may try and promote a standby by itself.
- Any other standbys attached to the old master will need to be manually
  instructed to point to the new master (e.g. with `repmgr standby follow`).
- You must ensure that following a server start using `pg_ctl`, log output
  is not send to STDERR (the default behaviour). If logging is not configured,
  We recommend setting `logging_collector=on` in `postgresql.conf` and
  providing an explicit `-l/--log` setting in `repmgr.conf`'s `pg_ctl_options`
  parameter.

We hope to remove some of these restrictions in future versions of `repmgr`.


### Switchover and PostgreSQL 9.3/9.4

In order to efficiently reintegrate a demoted master into the replication
cluster as a standby, it's necessary to resynchronise its data directory
with that of the current master, as it's very likely that their timelines
will have diverged slightly following the shutdown of the old master.

The utility `pg_rewind` provides an efficient way of doing this, however
is not included in the core PostgreSQL distribution for versions 9.3 and 9.4.
However, `pg_rewind` is available separately for these versions and we
strongly recommend its installation. To use it with versions 9.3 and 9.4,
provide the command line option `--pg_rewind`, optionally with the
path to the `pg_rewind` binary location if not installed in the PostgreSQL
`bin` directory.

`pg_rewind` for versions 9.3 and 9.4 can be obtained from:
  https://github.com/vmware/pg_rewind

Note that building this version of `pg_rewind` requires the PostgreSQL source
code. Also, PostgreSQL 9.3 does not provide `wal_log_hints`, meaning data
checksums must have been enabled when the database was initialized.

If `pg_rewind` is not available, as a fallback `repmgr` will use `repmgr
standby clone` to resynchronise the old master's data directory using
`rsync`. However, in order to ensure all files are synchronised, the
entire data directory on both servers must be scanned, a process which
can take some time on larger databases, in which case you should
consider making a fresh standby clone.


Unregistering a standby from a replication cluster
--------------------------------------------------

To unregister a running standby, execute:

    repmgr standby unregister -f /etc/repmgr.conf

This will remove the standby record from `repmgr`'s internal metadata
table (`repl_nodes`). A `standby_unregister` event notification will be
recorded in the `repl_events` table.

Note that this command will not stop the server itself or remove
it from the replication cluster.

If the standby is not running, the command can be executed on another
node by providing the id of the node to be unregistered using
the command line parameter `--node`, e.g. executing the following
command on the master server will unregister the standby with
id 3:

    repmgr standby unregister -f /etc/repmgr.conf --node=3


Automatic failover with `repmgrd`
---------------------------------

`repmgrd` is a management and monitoring daemon which runs on standby nodes
and which can automate actions such as failover and updating standbys to
follow the new master.

To use `repmgrd` for automatic failover, the following `repmgrd` options must
be set in `repmgr.conf`:

    failover=automatic
    promote_command='repmgr standby promote -f /etc/repmgr.conf'
    follow_command='repmgr standby follow -f /etc/repmgr.conf'

(See `repmgr.conf.sample` for further `repmgrd`-specific settings).

Additionally, `postgresql.conf` must contain the following line:

    shared_preload_libraries = 'repmgr_funcs'

When `failover` is set to `automatic`, upon detecting failure of the current
master, `repmgrd` will execute one of `promote_command` or `follow_command`,
depending on whether the current server is becoming the new master or
needs to follow another server which has become the new master. Note that
these commands can be any valid shell script which results in one of these
actions happening, but we strongly recommend executing `repmgr` directly.

`repmgrd` can be started simply with e.g.:

    repmgrd -f /etc/repmgr.conf --verbose >> $HOME/repmgr/repmgr.log 2>&1

For permanent operation, we recommend using the options `-d/--daemonize` to
detach the `repmgrd` process, and `-p/--pid-file` to write the process PID
to a file.

Note that currently `repmgrd` is not required to run on the master server.

To demonstrate automatic failover, set up a 3-node replication cluster (one master
and two standbys streaming directly from the master) so that the `repl_nodes`
table looks like this:

    repmgr=# SELECT * FROM repmgr_test.repl_nodes ORDER BY id;
     id |  type   | upstream_node_id | cluster | name  |                  conninfo                   | slot_name | priority | active
    ----+---------+------------------+---------+-------+---------------------------------------------+-----------+----------+--------
      1 | master  |                  | test    | node1 | host=repmgr_node1 dbname=repmgr user=repmgr |           |      100 | t
      2 | standby |                1 | test    | node2 | host=repmgr_node2 dbname=repmgr user=repmgr |           |      100 | t
      3 | standby |                1 | test    | node3 | host=repmgr_node3 dbname=repmgr user=repmgr |           |      100 | t
    (3 rows)


Start `repmgrd` on each standby and verify that it's running by examining
the log output, which at log level INFO will look like this:

    [2016-01-05 13:15:40] [INFO] checking cluster configuration with schema 'repmgr_test'
    [2016-01-05 13:15:40] [INFO] checking node 2 in cluster 'test'
    [2016-01-05 13:15:40] [INFO] reloading configuration file and updating repmgr tables
    [2016-01-05 13:15:40] [INFO] starting continuous standby node monitoring

Each `repmgrd` should also have noted its successful startup in the `repl_events`
table:

    repmgr=# SELECT * FROM repl_events WHERE event = 'repmgrd_start';
     node_id |     event     | successful |        event_timestamp        | details
    ---------+---------------+------------+-------------------------------+---------
           2 | repmgrd_start | t          | 2016-01-27 18:22:38.080231+09 |
           3 | repmgrd_start | t          | 2016-01-27 18:22:38.08756+09  |
    (2 rows)

Now stop the current master server with e.g.:

    pg_ctl -D /path/to/node1/data -m immediate stop

This will force the master node to shut down straight away, aborting all
processes and transactions.  This will cause a flurry of activity in
the `repmgrd` log files as each `repmgrd` detects the failure of the master
and a failover decision is made. Here extracts from the standby server
promoted to new master:

    [2016-01-06 18:32:58] [WARNING] connection to upstream has been lost, trying to recover... 15 seconds before failover decision
    [2016-01-06 18:33:03] [WARNING] connection to upstream has been lost, trying to recover... 10 seconds before failover decision
    [2016-01-06 18:33:08] [WARNING] connection to upstream has been lost, trying to recover... 5 seconds before failover decision
    ...
    [2016-01-06 18:33:18] [NOTICE] this node is the best candidate to be the new master, promoting...
    ...
    [2016-01-06 18:33:20] [NOTICE] STANDBY PROMOTE successful

and here from the standby server which is now following the new master:

    [2016-01-06 18:32:58] [WARNING] connection to upstream has been lost, trying to recover... 15 seconds before failover decision
    [2016-01-06 18:33:03] [WARNING] connection to upstream has been lost, trying to recover... 10 seconds before failover decision
    [2016-01-06 18:33:08] [WARNING] connection to upstream has been lost, trying to recover... 5 seconds before failover decision
    ...
    [2016-01-06 18:33:23] [NOTICE] node 2 is the best candidate for new master, attempting to follow...
    [2016-01-06 18:33:23] [INFO] changing standby's master
    ...
    [2016-01-06 18:33:25] [NOTICE] node 3 now following new upstream node 2

The `repl_nodes` table should have been updated to reflect the new situation,
with the original master (`node1`) marked as inactive, and standby `node3`
now following the new master (`node2`):

    repmgr=# SELECT * from repl_nodes ORDER BY id;
     id |  type   | upstream_node_id | cluster | name  |                 conninfo                 | slot_name | priority | active
    ----+---------+------------------+---------+-------+------------------------------------------+-----------+----------+--------
      1 | master  |                  | test    | node1 | host=localhost dbname=repmgr user=repmgr |           |      100 | f
      2 | master  |                  | test    | node2 | host=localhost dbname=repmgr user=repmgr |           |      100 | t
      3 | standby |                2 | test    | node3 | host=localhost dbname=repmgr user=repmgr |           |      100 | t
    (3 rows)

The `repl_events` table will contain a summary of what happened to each server
during the failover:

    repmgr=# SELECT * from repmgr_test.repl_events where event_timestamp>='2016-01-06 18:30';
     node_id |          event           | successful |        event_timestamp        |                         details
    ---------+--------------------------+------------+-------------------------------+----------------------------------------------------------
           2 | standby_promote          | t          | 2016-01-06 18:33:20.061736+09 | node 2 was successfully promoted to master
           2 | repmgrd_failover_promote | t          | 2016-01-06 18:33:20.067132+09 | node 2 promoted to master; old master 1 marked as failed
           3 | repmgrd_failover_follow  | t          | 2016-01-06 18:33:25.331012+09 | node 3 now following new upstream node 2
    (3 rows)


`repmgrd` log rotation
----------------------

Note that currently `repmgrd` does not provide logfile rotation. To ensure
the current logfile does not grow indefinitely, configure your system's `logrotate`
to do this. Sample configuration to rotate logfiles weekly with retention
for up to 52 weeks and rotation forced if a file grows beyond 100Mb:

    /var/log/postgresql/repmgr-9.5.log {
        missingok
        compress
        rotate 52
        maxsize 100M
        weekly
        create 0600 postgres postgres
    }


`repmgrd` and PostgreSQL connection settings
--------------------------------------------

In addition to the `repmgr` configuration settings, parameters in the
`conninfo` string influence how `repmgr` makes a network connection to
PostgreSQL. In particular, if another server in the replication cluster
is unreachable at network level, system network settings will influence
the length of time it takes to determine that the connection is not possible.

In particular explicitly setting a parameter for `connect_timeout` should
be considered; the effective minimum value of `2` (seconds) will ensure
that a connection failure at network level is reported as soon as possible,
otherwise depending on the system settings (e.g. `tcp_syn_retries` in Linux)
a delay of a minute or more is possible.

For further details on `conninfo` network connection parameters, see:

  https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS


Monitoring with `repmgrd`
-------------------------

When `repmgrd` is running with the option `-m/--monitoring-history`, it will
constantly write standby node status information to the `repl_monitor` table,
providing a near-real time overview of replication status on all nodes
in the cluster.

The view `repl_status` shows the most recent state for each node, e.g.:

    repmgr=# SELECT * FROM repmgr_test.repl_status;
    -[ RECORD 1 ]-------------+-----------------------------
    primary_node              | 1
    standby_node              | 2
    standby_name              | node2
    node_type                 | standby
    active                    | t
    last_monitor_time         | 2016-01-05 14:02:34.51713+09
    last_wal_primary_location | 0/3012AF0
    last_wal_standby_location | 0/3012AF0
    replication_lag           | 0 bytes
    replication_time_lag      | 00:00:03.463085
    apply_lag                 | 0 bytes
    communication_time_lag    | 00:00:00.955385

The interval in which monitoring history is written is controlled by the
configuration parameter `monitor_interval_secs`; default is 2.

As this can generate a large amount of monitoring data in the `repl_monitor`
table , it's advisable to regularly purge historical data with
`repmgr cluster cleanup`; use the `-k/--keep-history` to specify how
many day's worth of data should be retained.

It's possible to use `repmgrd` to provide monitoring only for some or all
nodes by setting `failover = manual` in the node's `repmgr.conf`. In the
event of the node's upstream failing, no failover action will be taken
and the node will require manual intervention to be reattached to replication.
If this occurs, event notification `standby_disconnect_manual` will be
created.

Note that when a standby node is not streaming directly from its upstream
node, e.g. recovering WAL from an archive, `apply_lag` will always appear as
`0 bytes`.


Using a witness server with repmgrd
------------------------------------

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


repmgrd and cascading replication
---------------------------------

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

By default, all notifications will be passed; the notification types
can be filtered to explicitly named ones:

    event_notifications=master_register,standby_register,witness_create

The following event types are available:

  * `master_register`
  * `standby_register`
  * `standby_unregister`
  * `standby_clone`
  * `standby_promote`
  * `standby_follow`
  * `standby_switchover`
  * `standby_disconnect_manual`
  * `witness_create`
  * `witness_register`
  * `witness_unregister`
  * `repmgrd_start`
  * `repmgrd_shutdown`
  * `repmgrd_failover_promote`
  * `repmgrd_failover_follow`

Note that under some circumstances (e.g. no replication cluster master could
be located), it will not be possible to write an entry into the `repl_events`
table, in which case `event_notification_command` can serve as a fallback.


Upgrading repmgr
----------------

`repmgr` is updated regularly with point releases (e.g. 3.0.2 to 3.0.3)
containing bugfixes and other minor improvements. Any substantial new
functionality will be included in a feature release (e.g. 3.0.x to 3.1.x).

In general `repmgr` can be upgraded as-is without any further action required,
however feature releases may require the `repmgr` database to be upgraded.
An SQL script will be provided - please check the release notes for details.

Reference
---------

### Default values

For some command line and most configuration file parameters, `repmgr` falls
back to default values if values for these are not explicitly provided.

The file `repmgr.conf.sample` documents the default value of configuration
parameters if one is set. Of particular note is the log level, which
defaults to NOTICE; particularly when using repmgr from the command line
it may be useful to set this to a higher level with `-L/--log-level`. e.g.
to `INFO`.

Execute `repmgr --help` to see the default values for various command
line parameters, particularly database connection parameters.

See the section `Configuration` above for information on how the
configuration file is located if `-f/--config-file` is not supplied.

### repmgr commands

The `repmgr` command line tool accepts commands for specific servers in the
replication in the format "`server_type` `action`", or for the entire
replication cluster in the format "`cluster` `action`". Each command is
described below.

In general, each command needs to be provided with the path to `repmgr.conf`,
which contains connection details for the local database.


* `master register`

    Registers a master in a cluster. This command needs to be executed before any
    standby nodes are registered.

    `primary register` can be used as an alias for `master register`.

* `standby register`

    Registers a standby with `repmgr`. This command needs to be executed to enable
    promote/follow operations and to allow `repmgrd` to work with the node.
    An existing standby can be registered using this command.

* `standby unregister`

    Unregisters a standby with `repmgr`. This command does not affect the actual
    replication, just removes the standby's entry from the `repl_nodes` table.

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

* `standby switchover`

    Promotes a standby to master and demotes the existing master to a standby.
    This command must be run on the standby to be promoted, and requires a
    password-less SSH connection to the current master. Additionally the
    location of the master's `repmgr.conf` file must be provided with
    `-C/--remote-config-file`.

    `repmgrd` should not be active if a switchover is attempted. This
    restriction may be lifted in a later version.

* `standby follow`

    Attaches the standby to a new master. This command requires a valid
    `repmgr.conf` file for the standby, either specified explicitly with
    `-f/--config-file` or located in the current working directory; no
    additional arguments are required.

    This command will force a restart of the standby server. It can only be used
    to attach a standby to a new master node.

* `witness create`

    Creates a witness server as a separate PostgreSQL instance. This instance
    can be on a separate server or a server running an existing node. The
    witness server contain a copy of the repmgr metadata tables but will not
    be set up as a standby; instead it will update its metadata copy each
    time a failover occurs.

    Note that it only makes sense to create a witness server if `repmgrd`
    is in use; see section "Using a witness server" above.

    This command requires a `repmgr.conf` file containing a valid conninfo
    string for the server to be created, as well as the other minimum required
    parameters detailed in the section `repmgr configuration file` above.

    By default the witness server will use port 5499 to facilitate easier setup
    on a server running an existing node. To use a different port, supply
    this explicitly in the `repmgr.conf` conninfo string.

    This command also requires the location of the witness server's data
    directory to be provided (`-D/--datadir`) as well as valid connection
    parameters for the master server. If not explicitly provided,
    database and user names will be extracted from the `conninfo` string in
    `repmgr.conf`.

    By default this command will create a superuser and a repmgr user.
    The `repmgr` user name will be extracted from the `conninfo` string
    in `repmgr.conf`.

* `witness register`

    This will set up the witness server configuration, including the witness
    server's copy of the `repmgr` meta database, on a running PostgreSQL
    instance and register the witness server with the master. It requires
    the same command line options as `witness create`.

* `witness unregister`

    Removes the entry for a witness server from the `repl_nodes` table. This
    command will not shut down the witness server or remove its data directory.

* `cluster show`

    Displays information about each active node in the replication cluster. This
    command polls each registered server and shows its role (master / standby /
    witness) or `FAILED` if the node doesn't respond. It polls each server
    directly and can be run on any node in the cluster; this is also useful
    when analyzing connectivity from a particular node.

    This command requires a valid `repmgr.conf` file to be provided; no
    additional arguments are needed.

    Example:

        $ repmgr -f /etc/repmgr.conf cluster show

        Role      | Name  | Upstream | Connection String
        ----------+-------|----------|----------------------------------------
        * master  | node1 |          | host=db_node1 dbname=repmgr user=repmgr
          standby | node2 | node1    | host=db_node2 dbname=repmgr user=repmgr
          standby | node3 | node2    | host=db_node3 dbname=repmgr user=repmgr

    To show database connection errors when polling nodes, run the command in
    `--verbose` mode.

    The `cluster show` command now accepts the optional parameter `--csv`, which
    outputs the replication cluster's status in a simple CSV format, suitable for
    parsing by scripts:

        $ repmgr -f /etc/repmgr.conf cluster show --csv
        1,-1
        2,0
        3,1

    The first column is the node's ID, and the second column represents the
    node's status (0 = master, 1 = standby, -1 = failed).

* `cluster cleanup`

    Purges monitoring history from the `repl_monitor` table to prevent excessive
    table growth. Use the `-k/--keep-history` to specify the number of days of
    monitoring history to retain. This command can be used manually or as a
    cronjob.

    This command requires a valid `repmgr.conf` file for the node on which it is
    executed, either specified explicitly with `-f/--config-file` or located in
    the current working directory; no additional arguments are required.


### Error codes

`repmgr` or `repmgrd` will return one of the following error codes on program
exit:

* SUCCESS (0)               Program ran successfully.
* ERR_BAD_CONFIG (1)        Configuration file could not be parsed or was invalid
* ERR_BAD_RSYNC (2)         An rsync call made by the program returned an error (repmgr only)
* ERR_NO_RESTART (4)        An attempt to restart a PostgreSQL instance failed
* ERR_DB_CON (6)            Error when trying to connect to a database
* ERR_DB_QUERY (7)          Error while executing a database query
* ERR_PROMOTED (8)          Exiting program because the node has been promoted to master
* ERR_STR_OVERFLOW (10)     String overflow error
* ERR_FAILOVER_FAIL (11)    Error encountered during failover (repmgrd only)
* ERR_BAD_SSH (12)          Error when connecting to remote host via SSH (repmgr only)
* ERR_SYS_FAILURE (13)      Error when forking (repmgrd only)
* ERR_BAD_BASEBACKUP (14)   Error when executing pg_basebackup (repmgr only)
* ERR_MONITORING_FAIL (16)  Unrecoverable error encountered during monitoring (repmgrd only)
* ERR_BAD_BACKUP_LABEL (17) Corrupt or unreadable backup label encountered (repmgr only)
* ERR_SWITCHOVER_FAIL (18)  Error encountered during switchover (repmgr only)


Support and Assistance
----------------------

2ndQuadrant provides 24x7 production support for `repmgr`, including
configuration assistance, installation verification and training for
running a robust replication cluster. For further details see:

* http://2ndquadrant.com/en/support/

There is a mailing list/forum to discuss contributions or issues:

* http://groups.google.com/group/repmgr

The IRC channel #repmgr is registered with freenode.

Please report bugs and other issues to:

* https://github.com/2ndQuadrant/repmgr

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

* http://blog.2ndquadrant.com/improvements-in-repmgr-3-1-4/
* http://blog.2ndquadrant.com/managing-useful-clusters-repmgr/
* http://blog.2ndquadrant.com/easier_postgresql_90_clusters/
