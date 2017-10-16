repmgr: Replication Manager for PostgreSQL
==========================================

`repmgr` is a suite of open-source tools to manage replication and failover
within a cluster of PostgreSQL servers. It enhances PostgreSQL's built-in
replication capabilities with utilities to set up standby servers, monitor
replication, and perform administrative tasks such as failover or switchover
operations.

`repmgr 4` is a complete rewrite of the existing `repmgr` codebase, allowing
the use of all of the latest features in PostgreSQL replication.

PostgreSQL 10, 9.6 and 9.5 are fully supported.
PostgreSQL 9.4 and 9.3 are supported, with some restrictions.

### BDR support

`repmgr 4` supports monitoring of a two-node BDR 2.0 cluster on PostgreSQL 9.6
only. Note that BDR 2.0 is not publicly available; please contact 2ndQuadrant
for details. `repmgr 4` will support future public BDR releases.

Changes in repmgr4 and backwards compatibility
-----------------------------------------------

`repmgr` is now implemented as a PostgreSQL extension, and all database objects
used by repmgr are stored in a dedicated `repmgr` schema, rather than
`repmgr_$cluster_name`. Note there is no need to install the extension, this
will be done automatically by `repmgr primary register`.

Some configuration items have had their names changed for consistency and clarity,
e.g. `node` => `node_id`. `repmgr` will issue a warning about deprecated/altered
options.

Some configuration items have been changed to command line options, and vice-
versa, e.g. to avoid hard-coding items such as a a node's upstream ID, which
might change over time.

See file `doc/changes-in-repmgr4.md` for more details.

To upgrade from repmgr 3.x, both the `repmgr` metadatabase and all repmgr
configuration files need to be converted. This is quite straightforward and
scripts are provided to assist with this. See document
`doc/upgrading-from-repmgr3.md` for further details.


Overview
--------

The `repmgr` suite provides two main tools:

- `repmgr` - a command-line tool used to perform administrative tasks such as:
    - setting up standby servers
    - promoting a standby server to primary
    - switching over primary and standby servers
    - displaying the status of servers in the replication cluster

- `repmgrd` is a daemon which actively monitors servers in a replication cluster
   and performs the following tasks:
    - monitoring and recording replication performance
    - performing failover by detecting failure of the primary and
      promoting the most suitable standby server
    - provide notifications about events in the cluster to a user-defined
      script which can perform tasks such as sending alerts by email

`repmgr` supports and enhances PostgreSQL's built-in streaming replication,
which provides a single read/write primary server and one or more read-only
standbys containing near-real time copies of the primary server's database.

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

This is the node a standby server is connected to; either the primary server or in
the case of cascading replication, another standby.

- `failover`

This is the action which occurs if a primary server fails and a suitable standby
is  promoted as the new primary. The `repmgrd` daemon supports automatic failover
to minimise downtime.

- `switchover`

In certain circumstances, such as hardware or operating system maintenance,
it's desirable to promote a suitable standby to primary, and demote the current
primary to a standby role in a controlled way. This enables work to be carried
out on the former primary without interrupting the replication cluster.
The `repmgr` command line client provides this functionality with the
`repmgr standby switchover` command.

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
  - `repmgr.monitoring_history`: historical standby monitoring information written by `repmgrd`

views:
  - `repmgr.show_nodes`: based on the table `repl_nodes`, additionally showing the
     name of the server's upstream node
  - `repmgr.replication_status`: when `repmgrd`'s monitoring is enabled, shows current monitoring
    status for each standby.

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

`repmgr 4` supports PostgreSQL from version 9.3.

If upgrading from `repmgr 3`, please see the separate upgrade guide
`doc/upgrading-from-repmgr3.md`.

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

* * *

> *NOTE*: packages are currently being prepared for the repmgr 4.0beta release.

* * *

We recommend installing `repmgr` using the available packages for your
system.

- RedHat/CentOS: RPM packages for `repmgr` are available via Yum through
  the PostgreSQL Global Development Group RPM repository ( http://yum.postgresql.org/ ).
  Follow the instructions for your distribution (RedHat, CentOS,
  Fedora, etc.) and architecture as detailed at yum.postgresql.org.

  2ndQuadrant also provides its own RPM packages which are made available
  at the same time as each `repmgr` release, as it can take some days for
  them to become available via the main PGDG repository. See here for details:

     https://repmgr.org/yum-repository.html

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
    https://repmgr.org/


### Building from source

Simply:

    ./configure && make install

Ensure `pg_config` for the target PostgreSQL version is in `$PATH`, and that you
have installed the development package for the postgres version you are compiling
against.


Configuration
-------------

`repmgr` and `repmgrd` use a common configuration file, by default called
`repmgr.conf` (although any name can be used if explicitly specified).
`repmgr.conf` must contain a number of required parameters, including
the database connection string for the local node and the location
of its data directory; other values will be inferred from defaults if
not explicitly supplied. See section `repmgr configuration file` below
for more details.

The configuration file will be searched for in the following locations:

- a configuration file specified by the `-f/--config-file` command line option
- a location specified by the package maintainer (if `repmgr` was installed
  from a package and the package maintainer has specified the configuration
  file location)
- `repmgr.conf` in the local directory
- `/etc/repmgr.conf`
- the directory reported by `pg_config --sysconfdir`

Note that if a file is explicitly specified with `-f/--config-file`, an error will
be raised if it is not found or not readable and no attempt will be made to check
default locations; this is to prevent `repmgr` unexpectedly reading the wrong file.

For a full list of annotated configuration items, see the file `repmgr.conf.sample`.

The following parameters in the configuration file can be overridden with
command line options:

- `log_level` with `-L/--log-level`
- `pg_bindir` with `-b/--pg_bindir`

### Logging

By default `repmgr` and `repmgrd` will log directly to `STDERR`. For `repmgrd`
we recommend capturing output in a logfile or using your system's log facility;
see `repmgr.conf.sample` for details.

As a command line utility, `repmgr` will log directly to the console by default.
However in some circumstances, such as when `repmgr` is executed by `repmgrd`
during a failover event, it makes sense to capture `repmgr`'s log output - this
can be done by supplying the command-line option `--log-to-file` to `repmgr`.

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

Like other PostgreSQL utilities based on `libpq`, `repmgr` will default to any values
set in environment variables if explicit command line parameters are not provided.
See the PostgreSQL documentation for further details:

  https://www.postgresql.org/docs/current/static/libpq-envars.html


Setting up a basic replication cluster with repmgr
---------------------------------------------------

The following section will describe how to set up a basic replication cluster
with a primary and a standby server using the `repmgr` command line tool.
It is assumed PostgreSQL is installed on both servers in the cluster,
`rsync` is available and passwordless SSH connections are possible between
both servers.

* * *

> *TIP*: for testing `repmgr`, it's possible to use multiple PostgreSQL
> instances running on different ports on the same computer, with
> passwordless SSH access to `localhost` enabled.

* * *

### PostgreSQL configuration

On the primary server, a PostgreSQL instance must be initialised and running.
The following replication settings may need to be adjusted:

    # Enable replication connections; set this figure to at least one more
    # than the number of standbys which will connect to this server
    # (note that repmgr will execute `pg_basebackup` in WAL streaming mode,
    # which requires two free WAL senders)

    max_wal_senders = 10

    # Ensure WAL files contain enough information to enable read-only queries
    # on the standby.
    #
    #  PostgreSQL 9.5 and earlier: one of 'hot_standby' or 'logical'
    #  PostgreSQL 9.6 and later: one of 'replica' or 'logical'
    #    ('hot_standby' will still be accepted as an alias for 'replica')
    #
    # See: https://www.postgresql.org/docs/current/static/runtime-config-wal.html#GUC-WAL-LEVEL

    wal_level = 'hot_standby'

    # Enable read-only queries on a standby
    # (Note: this will be ignored on a primary but we recommend including
    # it anyway)

    hot_standby = on

    # Enable WAL file archiving
    archive_mode = on

    # Set archive command to a script or application that will safely store
    # you WALs in a secure place. /bin/true is an example of a command that
    # ignores archiving. Use something more sensible.
    archive_command = '/bin/true'

    # If you have configured `pg_basebackup_options`
    # in `repmgr.conf` to include the setting `--xlog-method=fetch` (from
    # PostgreSQL 10 `--wal-method=fetch`), *and* you have not set
    # `restore_command` in `repmgr.conf`to fetch WAL files from another
    # source such as Barman, you'll need to set `wal_keep_segments` to a
    # high enough value to ensure that all WAL files generated while
    # the standby is being cloned are retained until the standby starts up.
    #
    # wal_keep_segments = 5000

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

Create a `repmgr.conf` file on the primary server. The file must contain at
least the following parameters:

    node_id=1
    node_name=node1
    conninfo='host=node1 user=repmgr dbname=repmgr connect_timeout=2'
    data_directory='/var/lib/postgresql/data'

- `node_id`: a unique integer identifying the node; note this must be a positive
     32 bit signed integer between 1 and 2147483647
- `node_name`: a unique string identifying the node; we recommend a name
     specific to the server (e.g. 'server_1'); avoid names indicating the
     current replication role like 'primary' or 'standby' as the server's
     role could change.
- `conninfo`: a valid connection string for the `repmgr` database on the
     *current* server. (On the standby, the database will not yet exist, but
     `repmgr` needs to know the connection details to complete the setup
     process).
- `data_directory`: the node's data directory

`repmgr.conf` should not be stored inside the PostgreSQL data directory,
as it could be overwritten when setting up or reinitialising the PostgreSQL
server. See section `Configuration` above for further details about `repmgr.conf`.

`repmgr` will install the `repmgr` extension, which creates a `repmgr` schema
containing the `repmgr` metadata tables as well as other functions and views.
We also recommend that you set the `repmgr` user's search path
to include this schema name, e.g.

    ALTER USER repmgr SET search_path TO repmgr, "$user", public;

* * *

> *TIP*: for Debian-based distributions we recommend explicitly setting
> `pg_bindir` to the directory where `pg_ctl` and other binaries not in
> the standard path are located. For PostgreSQL 9.6 this would be
> `/usr/lib/postgresql/9.6/bin/`.

* * *

See `repmgr.conf.sample` for details of all available configuration parameters.


### Register the primary server

To enable `repmgr` to support a replication cluster, the primary node must
be registered with `repmgr`. This installs the `repmgr` extension and
metadata objects, and adds a metadata record for the primary server:

    $ repmgr -f /etc/repmgr.conf primary register

    INFO: connecting to primary database...
    NOTICE: attempting to install extension "repmgr"
    NOTICE: "repmgr" extension successfully installed
    NOTICE: primary node record (id: 1) registered

Verify status of the cluster like this by using the `repmgr cluster show`
command:

    $ repmgr -f /etc/repmgr.conf cluster show
     ID | Name  | Role    | Status    | Upstream | Location | Connection string
    ----+-------+---------+-----------+----------+----------+--------------------------------------
     1  | node1 | primary | * running |          | default  | host=node1 dbname=repmgr user=repmgr

The record in the `repmgr` metadata table will look like this:

    repmgr=# SELECT * FROM repmgr.nodes;
    -[ RECORD 1 ]----+---------------------------------------
    node_id          | 1
    upstream_node_id |
    active           | t
    node_name        | node1
    type             | primary
    location         | default
    priority         | 100
    conninfo         | host=node1 dbname=repmgr user=repmgr
    repluser         | repmgr
    slot_name        |
    config_file      | /etc/repmgr.conf

Each server in the replication cluster will have its own record and will be updated
when its status or role changes.


### Clone the standby server

Create a `repmgr.conf` file on the standby server. It must contain at
least the same parameters as the primary's `repmgr.conf`, but with
the mandatory values `node`, `node_name`, `conninfo` (and possibly
`data_directory`) adjusted accordingly, e.g.:

    node=2
    node_name=node2
    conninfo='host=node2 user=repmgr dbname=repmgr'
    data_directory='/var/lib/postgresql/data'

Clone the standby with:

    $ repmgr -h node1 -U repmgr -d repmgr -f /etc/repmgr.conf standby clone

    NOTICE: using configuration file "/etc/repmgr.conf"
    NOTICE: destination directory "/var/lib/postgresql/data" provided
    INFO: connecting to source node
    NOTICE: checking for available walsenders on source node (2 required)
    INFO: sufficient walsenders available on source node (2 required)
    INFO: creating directory "/var/lib/postgresql/data"...
    NOTICE: starting backup (using pg_basebackup)...
    HINT: this may take some time; consider using the -c/--fast-checkpoint option
    INFO: executing:
      pg_basebackup -l "repmgr base backup" -D /var/lib/postgresql/data -h node1 -U repmgr -X stream
    NOTICE: standby clone (using pg_basebackup) complete
    NOTICE: you can now start your PostgreSQL server
    HINT: for example: pg_ctl -D /var/lib/postgresql/data start

This will clone the PostgreSQL data directory files from the primary at `node1`
using PostgreSQL's `pg_basebackup` utility. A `recovery.conf` file containing the
correct parameters to start streaming from this primary server will be created
automatically.

Note that by default, any configuration files in the primary's data directory will be
copied to the standby. Typically these will be `postgresql.conf`, `postgresql.auto.conf`,
`pg_hba.conf` and `pg_ident.conf`. These may require modification before the standby
is started so it functions as desired.

In some cases (e.g. on Debian or Ubuntu Linux installations), PostgreSQL's
configuration files are located outside of the data directory and will
not be copied by default. `repmgr` can copy these files, either to the same
location on the standby server (provided appropriate directory and file permissions
are available), or into the standby's data directory. This requires passwordless
SSH access to the primary server. Add the option `--copy-external-config-files`
to the `repmgr standby clone` command; by default files will be copied to
the same path as on the upstream server. To have them placed in the standby's
data directory, specify `--copy-external-config-files=pgdata`, but note that
any include directives in the copied files may need to be updated.

*Caveat*: when copying external configuration files: `repmgr` will only be able
to detect files which contain active settings. If a file is referenced by
an include directive but is empty, only contains comments or contains
settings which have not been activated, the file will not be copied.

* * *

> *TIP*: for reliable configuration file management we recommend using a
> configuration management tool such as Ansible, Chef, Puppet or Salt.

* * *

Be aware that when initially cloning a standby, you will need to ensure
that all required WAL files remain available while the cloning is taking
place. To ensure this happens when using the default `pg_basebackup` method,
`repmgr` will set `pg_basebackup`'s `--xlog-method` parameter to `stream`,
which will ensure all WAL files generated during the cloning process are
streamed in parallel with the main backup. Note that this requires two
replication connections to be available (`repmgr` will verify sufficient
connections are available before attempting to clone).

To override this behaviour, in `repmgr.conf` set `pg_basebackup`'s
`--xlog-method` parameter to `fetch`:

    pg_basebackup_options='--xlog-method=fetch'

and ensure that `wal_keep_segments` is set to an appropriately high value.
See the `pg_basebackup` documentation for details:

    https://www.postgresql.org/docs/current/static/app-pgbasebackup.html

> *NOTE*: From PostgreSQL 10, `pg_basebackup`'s `--xlog-method` parameter
> has been renamed to `--wal-method`.

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

Connect to the primary server and execute:

    repmgr=# SELECT * FROM pg_stat_replication;
    -[ RECORD 1 ]----+------------------------------
    pid              | 19111
    usesysid         | 16384
    usename          | repmgr
    application_name | node2
    client_addr      | 192.168.1.12
    client_hostname  |
    client_port      | 50378
    backend_start    | 2017-08-28 15:14:19.851581+09
    backend_xmin     |
    state            | streaming
    sent_location    | 0/7000318
    write_location   | 0/7000318
    flush_location   | 0/7000318
    replay_location  | 0/7000318
    sync_priority    | 0
    sync_state       | async


### Register the standby

Register the standby server with:

    $ repmgr -f /etc/repmgr.conf standby register
    NOTICE: standby node "node2" (id: 2) successfully registered

Check the node is registered by executing `repmgr cluster show` on the standby:

    $ repmgr -f /etc/repmgr.conf cluster show
     ID | Name  | Role    | Status    | Upstream | Location | Connection string
    ----+-------+---------+-----------+----------+----------+--------------------------------------
     1  | node1 | primary | * running |          | default  | host=node1 dbname=repmgr user=repmgr
     2  | node2 | standby |   running | node1    | default  | host=node2 dbname=repmgr user=repmgr

The standby server now has a copy of the records for all servers in the
replication cluster.
* * *

> *TIP*: depending on your environment and workload, it may take some time for
> the standby's node record to propagate from the primary to the standby. Some
> actions (such as starting `repmgrd`) require that the standby's node record
> is present and up-to-date to function correctly - by providing the option
> `--wait-sync` to the `repmgr standby register` command, `repmgr` will wait
> until the record is synchronised before exiting. An optional timeout (in
> seconds) can be added to this option (e.g. `--wait-sync=60`).

* * *

Under some circumstances you may wish to register a standby which is not
yet running; this can be the case when using provisioning tools to create
a complex replication cluster. In this case, by using the `-F/--force`
option and providing the connection parameters to the primary server,
the standby can be registered.

Similarly, with cascading replication it may be necessary to register
a standby whose upstream node has not yet been registered - in this case,
using `-F/--force` will result in the creation of an inactive placeholder
record for the upstream node, which will however later need to be registered
with the `-F/--force` option too.

When used with `standby register`, care should be taken that use of the
`-F/--force` option does not result in an incorrectly configured cluster.

### Using Barman to clone a standby

`repmgr standby clone` can use Barman (the "Backup and
Replication manager", https://www.pgbarman.org/), as a provider of both
base backups and WAL files.

Barman support provides the following advantages:

- the primary node does not need to perform a new backup every time a
  new standby is cloned;
- a standby node can be disconnected for longer periods without losing
  the ability to catch up, and without causing accumulation of WAL
  files on the primary node;
- therefore, `repmgr` does not need to use replication slots, and on the
  primary node, `wal_keep_segments` does not need to be set.

> *NOTE*: In view of the above, Barman support is incompatible with
> the `use_replication_slots` setting in `repmgr.conf`.

In order to enable Barman support for `repmgr standby clone`, following
prerequisites must be met:

- the `barman_server` setting in `repmgr.conf` is the same as the
  server configured in Barman;
- the `barman_host` setting in `repmgr.conf` is set to the SSH
  hostname of the Barman server;
- the `restore_command` setting in `repmgr.conf` is configured to
  use a copy of the `barman-wal-restore` script shipped with the
  `barman-cli` package (see below);
- the Barman catalogue includes at least one valid backup for this
  server.

> *NOTE*: Barman support is automatically enabled if `barman_server`
> is set. Normally it is a good practice to use Barman, for instance
> when fetching a base backup while cloning a standby; in any case,
> Barman mode can be disabled using the `--without-barman` command
> line option.

> *NOTE*: if you have a non-default SSH configuration on the Barman
> server, e.g. using a port other than 22, then you can set those
> parameters in a dedicated Host section in `~/.ssh/config`
> corresponding to the value of `barman_host` in `repmgr.conf`. See
> the "Host" section in `man 5 ssh_config` for more details.

`barman-wal-restore` is a Python script provided by the Barman
development team as part of the `barman-cli` package (Barman 2.0
and later; for Barman 1.x the script is provided separately as
`barman-wal-restore.py`).

`restore_command` must then be set in `repmgr.conf` as follows:

    <script> <Barman hostname> <cluster_name> %f %p

For instance, suppose that we have installed Barman on the `barmansrv`
host, and that `barman-wal-restore` is located as an executable at
`/usr/bin/barman-wal-restore`;  `repmgr.conf` should include the following
lines:

    barman_host=barmansrv
    barman_server=somedb
    restore_command=/usr/bin/barman-wal-restore barmansrv somedb %f %p

> *NOTE*: `barman-wal-restore` supports command line switches to
> control parallelism (`--parallel=N`) and compression (`--bzip2`,
> `--gzip`).

NOTE: to use a non-default Barman configuration file on the Barman server,
specify this in `repmgr.conf` with `barman_config`:

    barman_config=/path/to/barman.conf

It's now possible to clone a standby from Barman, e.g.:

    NOTICE: using configuration file "/etc/repmgr.conf"
    NOTICE: destination directory "/var/lib/postgresql/data" provided
    INFO: connecting to Barman server to verify backup for test_cluster
    INFO: checking and correcting permissions on existing directory "/var/lib/postgresql/data"
    INFO: creating directory "/var/lib/postgresql/data/repmgr"...
    INFO: connecting to Barman server to fetch server parameters
    INFO: connecting to upstream node
    INFO: connected to source node, checking its state
    INFO: successfully connected to source node
    DETAIL: current installation size is 29 MB
    NOTICE: retrieving backup from Barman...
    receiving file list ...
    (...)
    NOTICE: standby clone (from Barman) complete
    NOTICE: you can now start your PostgreSQL server
    HINT: for example: pg_ctl -D /var/lib/postgresql/data start

As with cloning directly from the primary, the standby must be registered
after the server has started.


Advanced options for cloning a standby
--------------------------------------

The above section demonstrates the simplest possible way to clone a standby
server. Depending on your circumstances, finer-grained control over the
cloning process may be necessary.

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

### Managing passwords

If replication connections to a standby's upstream server are password-protected,
the standby must be able to provide the password so it can begin streaming
replication.

The recommended way to do this is to store the password in the `postgres`
user's `~/.pgpass` file. It's also possible to store the password in the
environment variable `PGPASSWORD`, however this is not recommended for
security reasons. For more details see:

    https://www.postgresql.org/docs/current/static/libpq-pgpass.html

If for whatever reason you wish to include the password in `recovery.conf`,
set `use_primary_conninfo_password` to `true` in `repmgr.conf`. This
will read a password set in `PGPASSWORD` (but not `~/.pgpass`) and place
it into the`primary_conninfo` string in `recovery.conf`. Note that `PGPASSWORD`
will need to be set during any action which causes `recovery.conf` to be
rewritten, e.g. `standby follow`.

It is of course also possible to include the password value in the `conninfo`
string for each node, but this is obviously a security risk and should be
avoided.

### Separate replication user

In some circumstances it might be desirable to create a dedicated replication-only
user (in addition to the user who manages the `repmgr` metadata). In this case,
the replication user should be set in `repmgr.conf` via the parameter
`replication_user`; `repmgr` will use this value when making replication connections
and generating `recovery.conf`. This value will also be stored in the `repmgr.nodes`
table for each node; it no longer needs to be explicitly specified when
cloning a node or executing `repmgr standby follow`.


Setting up cascading replication with repmgr
--------------------------------------------

Cascading replication, introduced with PostgreSQL 9.2, enables a standby server
to replicate from another standby server rather than directly from the primary,
meaning replication changes "cascade" down through a hierarchy of servers. This
can be used to reduce load on the primary and minimize bandwidth usage between
sites.

`repmgr` supports cascading replication. When cloning a standby, set the
command-line parameter `--upstream-node-id` to the `node_id` of the
server the standby should connect to, and `repmgr` will create `recovery.conf`
pointing to this server. Note that if `--upstream-node-id` is not explicitly
provided, `repmgr` will set the standby's `recovery.conf` to point to the primary
node.

To demonstrate cascading replication, ensure you have a primary and standby
set up as shown above in the section "Setting up a simple replication cluster
with repmgr". Create an additional standby server with `repmgr.conf` looking
like this:

    node_id=3
    node_name=node3
    conninfo='host=node3 user=repmgr dbname=repmgr'
    data_directory='/var/lib/postgresql/data'

Clone this standby (using the connection parameters for the existing standby),
ensuring `--upstream-node-id` is provide with the `node_id` of the previously
created standby, and register it:

    $ repmgr -h node2 -U repmgr -d repmgr -f /etc/repmgr.conf standby clone --upstream-node-id=2
    NOTICE: using configuration file "/etc/repmgr.conf"
    NOTICE: destination directory "/var/lib/postgresql/data" provided
    INFO: connecting to upstream node
    INFO: connected to source node, checking its state
    NOTICE: checking for available walsenders on upstream node (2 required)
    INFO: sufficient walsenders available on upstream node (2 required)
    INFO: successfully connected to source node
    DETAIL: current installation size is 29 MB
    INFO: creating directory "/var/lib/postgresql/data"...
    NOTICE: starting backup (using pg_basebackup)...
    HINT: this may take some time; consider using the -c/--fast-checkpoint option
    INFO: executing: 'pg_basebackup -l "repmgr base backup" -D /var/lib/postgresql/data -h node2 -U repmgr -X stream '
    NOTICE: standby clone (using pg_basebackup) complete
    NOTICE: you can now start your PostgreSQL server
    HINT: for example: pg_ctl -D /var/lib/postgresql/data start

    $ repmgr -f /etc/repmgr.conf standby register --upstream-node-id=2
    NOTICE: standby node "node2" (id: 2) successfully registered

After starting the standby, the cluster will look like this:

    $ repmgr -f /etc/repmgr.conf cluster show
     ID | Name  | Role    | Status    | Upstream | Location | Connection string
    ----+-------+---------+-----------+----------+----------+--------------------------------------
     1  | node1 | primary | * running |          | default  | host=node1 dbname=repmgr user=repmgr
     2  | node2 | standby |   running | node1    | default  | host=node2 dbname=repmgr user=repmgr
     3  | node3 | standby |   running | node2    | default  | host=node3 dbname=repmgr user=repmgr


* * *

> *TIP*: under some circumstances when setting up a cascading replication
> cluster, you may wish to clone a downstream standby whose upstream node
> does not yet exist. In this case you can clone from the primary (or
> another upstream node) and provide the parameter `--upstream-conninfo`
> to set explicitly the upstream's `primary_conninfo` string in `recovery.conf`.

* * *


Using replication slots with repmgr
-----------------------------------

Replication slots were introduced with PostgreSQL 9.4 and are designed to ensure
that any standby connected to the primary using a replication slot will always
be able to retrieve the required WAL files. This removes the need to manually
manage WAL file retention by estimating the number of WAL files that need to
be maintained on the primary using `wal_keep_segments`. Do however be aware
that if a standby is disconnected, WAL will continue to accumulate on the primary
until either the standby reconnects or the replication slot is dropped.

To enable `repmgr` to use replication slots, set the boolean parameter
`use_replication_slots` in `repmgr.conf`:

    use_replication_slots=true

Replication slots must be enabled in `postgresql.conf` by setting the parameter
`max_replication_slots` to at least the number of expected standbys (changes
to this parameter require a server restart).

When cloning a standby, `repmgr` will automatically generate an appropriate
slot name, which is stored in the `repmgr.nodes` table, and create the slot
on the upstream node:

    repmgr=# SELECT node_id, upstream_node_id, active, node_name, type, priority, slot_name
               FROM repmgr.nodes ORDER BY node_id;
     node_id | upstream_node_id | active | node_name |  type   | priority |   slot_name
    ---------+------------------+--------+-----------+---------+----------+---------------
           1 |                  | t      | node1     | primary |      100 | repmgr_slot_1
           2 |                1 | t      | node2     | standby |      100 | repmgr_slot_2
           3 |                1 | t      | node3     | standby |      100 | repmgr_slot_3
    (3 rows)


    repmgr=# SELECT slot_name, slot_type, active, active_pid FROM pg_replication_slots ;
       slot_name   | slot_type | active | active_pid
    ---------------+-----------+--------+------------
     repmgr_slot_2 | physical  | t      |      23658
     repmgr_slot_3 | physical  | t      |      23687
    (2 rows)

Note that a slot name will be created by default for the primary but not
actually used unless the primary is converted to a standby using e.g.
`repmgr standby switchover`.

Further information on replication slots in the PostgreSQL documentation:
    https://www.postgresql.org/docs/current/interactive/warm-standby.html#STREAMING-REPLICATION-SLOTS

* * *

> *TIP*: while replication slots can be useful for streaming replication, it's
> recommended to monitor for inactive slots as these will cause WAL files to
> build up indefinitely, possibly leading to server failure.
>
> As an alternative we recommend using 2ndQuadrant's Barman, which offloads
> WAL management to a separate server, negating the need to use replication
> slots to reserve WAL. See section "Using Barman to clone a standby" for more
> details on using `repmgr` together with Barman.

* * *

Promoting a standby server with repmgr
--------------------------------------

If a primary server fails or needs to be removed from the replication cluster,
a new primary server must be designated, to ensure the cluster continues
working correctly. This can be done with `repmgr standby promote`, which promotes
the standby on the current server to primary.

To demonstrate this, set up a replication cluster with a primary and two attached
standby servers so that the cluster looks like this:

    $ repmgr -f /etc/repmgr.conf cluster show
     ID | Name  | Role    | Status    | Upstream | Location | Connection string
    ----+-------+---------+-----------+----------+----------+--------------------------------------
     1  | node1 | primary | * running |          | default  | host=node1 dbname=repmgr user=repmgr
     2  | node2 | standby |   running | node1    | default  | host=node2 dbname=repmgr user=repmgr
     3  | node3 | standby |   running | node1    | default  | host=node3 dbname=repmgr user=repmgr

Stop the current primary with e.g.:

    $ pg_ctl -D /var/lib/postgresql/data -m fast stop

At this point the replication cluster will be in a partially disabled state, with
both standbys accepting read-only connections while attempting to connect to the
stopped primary. Note that the `repmgr` metadata table will not yet have been updated;
executing `repmgr cluster show` will note the discrepancy:

    $ repmgr -f /etc/repmgr.conf cluster show
     ID | Name  | Role    | Status        | Upstream | Location | Connection string
    ----+-------+---------+---------------+----------+----------+--------------------------------------
     1  | node1 | primary | ? unreachable |          | default  | host=node1 dbname=repmgr user=repmgr
     2  | node2 | standby |   running     | node1    | default  | host=node2 dbname=repmgr user=repmgr
     3  | node3 | standby |   running     | node1    | default  | host=node3 dbname=repmgr user=repmgr

    WARNING: following issues were detected
      node "node1" (ID: 1) is registered as an active primary but is unreachable

Now promote the first standby with:

    $ repmgr -f /etc/repmgr.conf standby promote

This will produce output similar to the following:

    INFO: connecting to standby database
    NOTICE: promoting standby
    DETAIL: promoting server using "pg_ctl -l /var/log/postgresql/startup.log -w -D '/var/lib/postgresql/data' promote"
    server promoting
    INFO: reconnecting to promoted server
    NOTICE: STANDBY PROMOTE successful
    DETAIL: node 2 was successfully promoted to primary

Executing `repmgr cluster show` will show the current state; as there is now an
active primary, the previous warning will not be displayed:

    $ repmgr -f /etc/repmgr.conf cluster show
     ID | Name  | Role    | Status    | Upstream | Location | Connection string
    ----+-------+---------+-----------+----------+----------+--------------------------------------
     1  | node1 | primary | - failed  |          | default  | host=node1 dbname=repmgr user=repmgr
     2  | node2 | primary | * running |          | default  | host=node2 dbname=repmgr user=repmgr
     3  | node3 | standby |   running | node1    | default  | host=node3 dbname=repmgr user=repmgr

However the sole remaining standby is still trying to replicate from the failed
primary; `repmgr standby follow` must now be executed to rectify this situation.


Following a new primary server with repmgr
------------------------------------------

Following the failure or removal of the replication cluster's existing primary
server, `repmgr standby follow` can be used to make 'orphaned' standbys
follow the new primary and catch up to its current state.

To demonstrate this, assuming a replication cluster in the same state as the
end of the preceding section ("Promoting a standby server with repmgr"),
execute this:

    $ repmgr -f /etc/repmgr.conf repmgr standby follow
    INFO: changing node 3's primary to node 2
    NOTICE: restarting server using "pg_ctl -l /var/log/postgresql/startup.log -w -D '/var/lib/postgresql/data' restart"
    waiting for server to shut down......... done
    server stopped
    waiting for server to start.... done
    server started
    NOTICE: STANDBY FOLLOW successful
    DETAIL: node 3 is now attached to node 2

The standby is now replicating from the new primary and `repmgr cluster show`
output reflects this:

    $ repmgr -f /etc/repmgr.conf cluster show
     ID | Name  | Role    | Status    | Upstream | Location | Connection string
    ----+-------+---------+-----------+----------+----------+--------------------------------------
     1  | node1 | primary | - failed  |          | default  | host=node1 dbname=repmgr user=repmgr
     2  | node2 | primary | * running |          | default  | host=node2 dbname=repmgr user=repmgr
     3  | node3 | standby |   running | node2    | default  | host=node3 dbname=repmgr user=repmgr

Note that with cascading replication, `repmgr standby follow` can also be
used to detach a standby from its current upstream server and follow the
primary. However it's currently not possible to have it follow another standby;
we hope to improve this in a future release.


Performing a switchover with repmgr
-----------------------------------

A typical use-case for replication is a combination of primary and standby
server, with the standby serving as a backup which can easily be activated
in case of a problem with the primary. Such an unplanned failover would
normally be handled by promoting the standby, after which an appropriate
action must be taken to restore the old primary.

* * *

> *NOTE* `repmgr standby switchover` can only be executed on a standby directly
> attached to the primary server.

* * *

In some cases however it's desirable to promote the standby in a planned
way, e.g. so maintenance can be performed on the primary; this kind of switchover
is supported by the `repmgr standby switchover` command.

`repmgr standby switchover` differs from other `repmgr` actions in that it
also performs actions on another server (the primary to be demoted to a standby),
which requires passwordless SSH access to that server from the one where
`repmgr standby switchover` is executed.

* * *

> *NOTE* `repmgr standby switchover` performs a relatively complex series
> of operations on two servers, and should therefore be performed after
> careful preparation and with adequate attention. In particular you should
> be confident that your network environment is stable and reliable.
>
> Additionally you should be sure that the current primary can be shut down
> quickly and cleanly. In particular, access from applications should be
> minimized or preferably blocked completely. Also be aware that if there
> is a backlog of files waiting to be archived, PostgreSQL will not shut
> down until archiving completes.
>
> We recommend running `repmgr standby switchover` at the most verbose
> logging level (`--log-level DEBUG --verbose`) and capturing all output
> to assist troubleshooting any problems.
>
> Please also read carefully the sections `Preparing for switchover` and
> `Caveats` below.

* * *

To demonstrate switchover, we will assume a replication cluster running on
PostgreSQL 9.5 or later with a primary (`node1`) and a standby (`node2`);
after the switchover `node2` should become the primary with `node1` following it.

The switchover command must be run from the standby which is to be promoted,
and in its simplest form looks like this:

    $ repmgr -f /etc/repmgr.conf standby switchover
    NOTICE: executing switchover on node "node2" (ID: 2)
    INFO: searching for primary node
    INFO: checking if node 1 is primary
    INFO: current primary node is 1
    INFO: SSH connection to host "localhost" succeeded
    INFO: archive mode is "off"
    INFO: replication lag on this standby is 0 seconds
    NOTICE: local node "node2" (ID: 2) will be promoted to primary; current primary "node1" (ID: 1) will be demoted to standby
    NOTICE: stopping current primary node "node1" (ID: 1)
    NOTICE: issuing CHECKPOINT
    DETAIL: executing server command "pg_ctl -l /var/log/postgres/startup.log -D '/var/lib/pgsql/data' -m fast -W stop"
    INFO: checking primary status; 1 of 6 attempts
    NOTICE: current primary has been cleanly shut down at location 0/3001460
    NOTICE: promoting standby to primary
    DETAIL: promoting server "node2" (ID: 2) using "pg_ctl -l /var/log/postgres/startup.log -w -D '/var/lib/pgsql/data' promote"
    server promoting
    NOTICE: STANDBY PROMOTE successful
    DETAIL: server "node2" (ID: 2) was successfully promoted to primary
    INFO: setting node 1's primary to node 2
    NOTICE: starting server using  "pg_ctl -l /var/log/postgres/startup.log -w -D '/var/lib/pgsql/data' restart"
    NOTICE: NODE REJOIN successful
    DETAIL: node 1 is now attached to node 2
    NOTICE: switchover was successful
    DETAIL: node "node2" is now primary
    NOTICE: STANDBY SWITCHOVER is complete

The old primary is now replicating as a standby from the new primary, and the
cluster status will now look like this:

    $ repmgr -f /etc/repmgr.conf cluster show
     ID | Name  | Role    | Status    | Upstream | Location | Connection string
    ----+-------+---------+-----------+----------+----------+--------------------------------------
     1  | node1 | standby |   running | node2    | default  | host=node1 dbname=repmgr user=repmgr
     2  | node2 | primary | * running |          | default  | host=node2 dbname=repmgr user=repmgr

### Preparing for switchover

As mentioned above, success of the switchover operation depends on `repmgr`
being able to shut down the current primary server quickly and cleanly.

Double-check which commands will be used to stop/start/restart the current
primary; execute:

    repmgr -f /etc./repmgr.conf node service --list --action=stop
    repmgr -f /etc./repmgr.conf node service --list --action=start
    repmgr -f /etc./repmgr.conf node service --list --action=restart

* * *

> *NOTE* on systemd systems we strongly recommend using the appropriate
> `systemctl` commands (typically run via `sudo`) to ensure systemd is
> informed about the status of the PostgreSQL service.

* * *

Check that access from applications is minimized or preferably blocked
completely, so applications are not unexpectedly interrupted.

Check there is no significant replication lag on standbys attached to the
current primary.

If WAL file archiving is set up, check that there is no backlog of files waiting
to be archived, as PostgreSQL will not finally shut down until all these have been
archived. If there is a backlog exceeding `archive_ready_warning` WAL files,
`repmgr` will emit a warning before attempting to perform a switchover; you can also check
manually with `repmgr node check --archive-ready`.

Ensure that `repmgrd` is *not* running to prevent it unintentionally promoting a node.

Finally, consider executing `repmgr standby switchover` with the `--dry-run` option;
this will perform any necessary checks and inform you about success/failure, and
stop before the first actual command is run (which would be the shutdown of the
current primary). Example output:

    $ repmgr standby switchover --siblings-follow --dry-run
    NOTICE: checking switchover on node "node2" (ID: 2) in --dry-run mode
    INFO: SSH connection to host "localhost" succeeded
    INFO: archive mode is "off"
    INFO: replication lag on this standby is 0 seconds
    INFO: all sibling nodes are reachable via SSH
    NOTICE: local node "node2" (ID: 2) will be promoted to primary; current primary "node1" (ID: 1) will be demoted to standby
    INFO: following shutdown command would be run on node "node1":
      "pg_ctl -l /var/log/postgresql/startup.log -D '/var/lib/postgresql/data' -m fast -W stop"


### Handling other attached standbys

By default, `repmgr` will not do anything with other standbys attached to the
original primary; if there were a second standby (`node3`), executing
`repmgr standby switchover` as above would result in a cascaded standby
situation, with `node3` still being attached to `node1`:

    $ repmgr -f /etc/repmgr.conf cluster show
     ID | Name  | Role    | Status    | Upstream | Location | Connection string
    ----+-------+---------+-----------+----------+----------+--------------------------------------
     1  | node1 | standby |   running | node2    | default  | host=node1 dbname=repmgr user=repmgr
     2  | node2 | primary | * running |          | default  | host=node2 dbname=repmgr user=repmgr
     3  | node3 | standby |   running | node1    | default  | host=node3 dbname=repmgr user=repmgr

However, if executed with the option `--siblings-follow`, `repmgr` will repoint
any standbys attached to the original primary (the "siblings" of the original
standby) to point to the new primary:

    $ repmgr -f /etc/repmgr.conf standby switchover --siblings-follow
    NOTICE: executing switchover on node "node2" (ID: 2)
    NOTICE: issuing CHECKPOINT
    NOTICE: executing server command "pg_ctl -l /var/log/postgres/startup.log -D '/var/lib/pgsql/data' -m fast -W stop"
    INFO: checking primary status; 1 of 6 attempts
    NOTICE: current primary has been shut down at location 0/30005F8
    NOTICE: promoting standby
    DETAIL: promoting server using "pg_ctl -l /var/log/postgres/startup.log -w -D '/var/lib/pgsql/data' promote"
    waiting for server to promote.... done
    server promoted
    INFO: reconnecting to promoted server
    NOTICE: STANDBY PROMOTE successful
    DETAIL: node 2 was successfully promoted to primary
    INFO: changing node 1's primary to node 2
    NOTICE: restarting server using "pg_ctl -l /var/log/postgres/startup.log -w -D '/var/lib/pgsql/data' restart"
    pg_ctl: PID file "/var/lib/pgsql/data/postmaster.pid" does not exist
    Is server running?
    starting server anyway
    NOTICE: NODE REJOIN successful
    DETAIL: node 1 is now attached to node 2
    NOTICE: switchover was successful
    DETAIL: node "node2" is now primary
    NOTICE: executing STANDBY FOLLOW on 1 of 1 siblings
    INFO: changing node 3's primary to node 2
    NOTICE: restarting server using "pg_ctl -l /var/log/postgres/startup.log -w -D '/var/lib/pgsql/data' restart"
    NOTICE: STANDBY FOLLOW successful
    DETAIL: node 3 is now attached to node 2
    INFO: STANDBY FOLLOW successfully executed on all reachable sibling nodes
    NOTICE: STANDBY SWITCHOVER is complete

and the cluster status will now look like this:

    $ repmgr -f /etc/repmgr.conf cluster show
     ID | Name  | Role    | Status    | Upstream | Location | Connection string
    ----+-------+---------+-----------+----------+----------+--------------------------------------
     1  | node1 | standby |   running | node2    | default  | host=node1 dbname=repmgr user=repmgr
     2  | node2 | primary | * running |          | default  | host=node2 dbname=repmgr user=repmgr
     3  | node3 | standby |   running | node2    | default  | host=node3 dbname=repmgr user=repmgr


### Caveats

- If using PostgreSQL 9.3 or 9.4, you should ensure that the shutdown command
  is configured to use PostgreSQL's `fast` shutdown mode (the default in 9.5
  and later). If relying on `pg_ctl` to perform database server operations,
  you should include `-m fast` in `pg_ctl_options` in `repmgr.conf`.
- `pg_rewind` *requires* that either `wal_log_hints` is enabled, or that
  data checksums were enabled when the cluster was initialized. See the
  `pg_rewind` documentation for details:
     https://www.postgresql.org/docs/current/static/app-pgrewind.html
- `repmgrd` should not be running with setting `failover=automatic` in
  `repmgr.conf` when a switchover is carried out, otherwise the `repmgrd`
  daemon may try and promote a standby by itself.

We hope to remove some of these restrictions in future versions of `repmgr`.


Unregistering a standby from a replication cluster
--------------------------------------------------

To unregister a running standby, execute:

    repmgr standby unregister -f /etc/repmgr.conf

This will remove the standby record from `repmgr`'s internal metadata
table (`repmgr.nodes`). A `standby_unregister` event notification will be
recorded in the `repmgr.events` table.

Note that this command will not stop the server itself or remove it from
the replication cluster. Note that if the standby was using a replication
slot, this will not be removed.

If the standby is not running, the command can be executed on another
node by providing the id of the node to be unregistered using
the command line parameter `--node-id`, e.g. executing the following
command on the master server will unregister the standby with
id `3`:

    repmgr standby unregister -f /etc/repmgr.conf --node-id=3


Automatic failover with `repmgrd`
---------------------------------

`repmgrd` is a management and monitoring daemon which runs on each node in
a replication cluster. It can automate actions such as failover and
updating standbys to follow the new primary, as well as providing monitoring
information about the state of each standby.

To use `repmgrd`, its associated function library must be included in
`postgresql.conf` with:

    shared_preload_libraries = 'repmgr'

Changing this setting requires a restart of PostgreSQL; for more details see:

  https://www.postgresql.org/docs/current/static/runtime-config-client.html#GUC-SHARED-PRELOAD-LIBRARIES

Additionally the following `repmgrd` options *must* be set in `repmgr.conf`
(adjust configuration file locations as appropriate):

    failover=automatic
    promote_command='repmgr standby promote -f /etc/repmgr.conf --log-to-file'
    follow_command='repmgr standby follow -f /etc/repmgr.conf --log-to-file --upstream-node-id=%n'

Note that the `--log-to-file` option will cause `repmgr`'s output to be logged to
the destination configured to receive log output for `repmgrd`.
See `repmgr.conf.sample` for further `repmgrd`-specific settings

The `follow_command` should provide the `--upstream-node-id=%n` option to
`repmgr standby follow`; the `%n` will be replaced by `repmgrd` with the ID
of the new primary. If this is not provided, if the original primary comes back
online after the new primary is promoted, there is a risk that
`repmgr standby follow` will follow the original primary.

When `failover` is set to `automatic`, upon detecting failure of the current
primary, `repmgrd` will execute one of `promote_command` or `follow_command`,
depending on whether the current server is to become the new primary, or
needs to follow another server which has become the new primary. Note that
these commands can be any valid shell script which results in one of these
two actions happening, but if `repmgr`'s `standby follow` or `standby promote`
commands are not executed (either directly as shown here, or from a script which
performs other actions), the `repmgr` metadata will not be updated and
monitoring will no longer function reliably.

To demonstrate automatic failover, set up a 3-node replication cluster (one primary
and two standbys streaming directly from the primary) so that the cluster looks
something like this:

    $ repmgr -f /etc/repmgr.conf cluster show
     ID | Name  | Role    | Status    | Upstream | Location | Connection string
    ----+-------+---------+-----------+----------+----------+--------------------------------------
     1  | node1 | primary | * running |          | default  | host=node1 dbname=repmgr user=repmgr
     2  | node2 | standby |   running | node1    | default  | host=node2 dbname=repmgr user=repmgr
     3  | node3 | standby |   running | node1    | default  | host=node3 dbname=repmgr user=repmgr

Start `repmgrd` on each standby and verify that it's running by examining the
log output, which at log level `INFO` will look like this:

    [2017-08-24 17:31:00] [NOTICE] using configuration file "/etc/repmgr.conf"
    [2017-08-24 17:31:00] [INFO] connecting to database "host=node2 dbname=repmgr user=repmgr"
    [2017-08-24 17:31:00] [NOTICE] starting monitoring of node "node2" (ID: 2)
    [2017-08-24 17:31:00] [INFO] monitoring connection to upstream node "node1" (node ID: 1)

Each `repmgrd` should also have recorded its successful startup as an event:

    $ repmgr -f /etc/repmgr.conf cluster event --event=repmgrd_start
     Node ID | Name  | Event         | OK | Timestamp           | Details
    ---------+-------+---------------+----+---------------------+-------------------------------------------------------------
     3       | node3 | repmgrd_start | t  | 2017-08-24 17:35:54 | monitoring connection to upstream node "node1" (node ID: 1)
     2       | node2 | repmgrd_start | t  | 2017-08-24 17:35:50 | monitoring connection to upstream node "node1" (node ID: 1)
     1       | node1 | repmgrd_start | t  | 2017-08-24 17:35:46 | monitoring cluster primary "node1" (node ID: 1)

Now stop the current primary server with e.g.:

    pg_ctl -D /path/to/node1/data -m immediate stop

This will force the primary to shut down straight away, aborting all processes
and transactions.  This will cause a flurry of activity in the `repmgrd` log
files as each `repmgrd` detects the failure of the primary and a failover
decision is made. This is an extract from the log of a standby server ("node2")
which has promoted to new primary after failure of the original primary ("node1").

    [2017-08-24 23:32:01] [INFO] node "node2" (node ID: 2) monitoring upstream node "node1" (node ID: 1) in normal state
    [2017-08-24 23:32:08] [WARNING] unable to connect to upstream node "node1" (node ID: 1)
    [2017-08-24 23:32:08] [INFO] checking state of node 1, 1 of 5 attempts
    [2017-08-24 23:32:08] [INFO] sleeping 1 seconds until next reconnection attempt
    [2017-08-24 23:32:09] [INFO] checking state of node 1, 2 of 5 attempts
    [2017-08-24 23:32:09] [INFO] sleeping 1 seconds until next reconnection attempt
    [2017-08-24 23:32:10] [INFO] checking state of node 1, 3 of 5 attempts
    [2017-08-24 23:32:10] [INFO] sleeping 1 seconds until next reconnection attempt
    [2017-08-24 23:32:11] [INFO] checking state of node 1, 4 of 5 attempts
    [2017-08-24 23:32:11] [INFO] sleeping 1 seconds until next reconnection attempt
    [2017-08-24 23:32:12] [INFO] checking state of node 1, 5 of 5 attempts
    [2017-08-24 23:32:12] [WARNING] unable to reconnect to node 1 after 5 attempts
    INFO:  setting voting term to 1
    INFO:  node 2 is candidate
    INFO:  node 3 has received request from node 2 for electoral term 1 (our term: 0)
    [2017-08-24 23:32:12] [NOTICE] this node is the winner, will now promote self and inform other nodes
    INFO: connecting to standby database
    NOTICE: promoting standby
    DETAIL: promoting server using '/home/barwick/devel/builds/HEAD/bin/pg_ctl -l /tmp/postgres.5602.log -w -D '/tmp/repmgr-test/node_2/data' promote'
    INFO: reconnecting to promoted server
    NOTICE: STANDBY PROMOTE successful
    DETAIL: node 2 was successfully promoted to primary
    INFO:  node 3 received notification to follow node 2
    [2017-08-24 23:32:13] [INFO] switching to primary monitoring mode

The cluster status will now look like this, with the original primary (`node1`)
marked as inactive, and standby `node3` now following the new primary (`node2`):

    $ repmgr -f /etc/repmgr.conf cluster show
     ID | Name  | Role    | Status    | Upstream | Location | Connection string
    ----+-------+---------+-----------+----------+----------+----------------------------------------------------
     1  | node1 | primary | - failed  |          | default  | host=node1 dbname=repmgr user=repmgr
     2  | node2 | primary | * running |          | default  | host=node2 dbname=repmgr user=repmgr
     3  | node3 | standby |   running | node2    | default  | host=node3 dbname=repmgr user=repmgr

`repmgr cluster event` will display a summary of what happened to each server
during the failover:

    $ repmgr -f /etc/repmgr.conf cluster event
     Node ID | Name  | Event                    | OK | Timestamp           | Details
    ---------+-------+--------------------------+----+---------------------+-----------------------------------------------------------------------------------
     3       | node3 | repmgrd_failover_follow  | t  | 2017-08-24 23:32:16 | node 3 now following new upstream node 2
     3       | node3 | standby_follow           | t  | 2017-08-24 23:32:16 | node 3 is now attached to node 2
     2       | node2 | repmgrd_failover_promote | t  | 2017-08-24 23:32:13 | node 2 promoted to primary; old primary 1 marked as failed
     2       | node2 | standby_promote          | t  | 2017-08-24 23:32:13 | node 2 was successfully promoted to primary


### `repmgrd` and PostgreSQL connection settings

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



### Monitoring with `repmgrd`

When `repmgrd` is running with the option `monitoring_history=true`, it will
constantly write standby node status information to the `monitoring_history`
table, providing a near-real time overview of replication status on all nodes
in the cluster.

The view `replication_status` shows the most recent state for each node, e.g.:

    repmgr=# select * from repmgr.replication_status;
    -[ RECORD 1 ]-------------+------------------------------
    primary_node_id           | 1
    standby_node_id           | 2
    standby_name              | node2
    node_type                 | standby
    active                    | t
    last_monitor_time         | 2017-08-24 16:28:41.260478+09
    last_wal_primary_location | 0/6D57A00
    last_wal_standby_location | 0/5000000
    replication_lag           | 29 MB
    replication_time_lag      | 00:00:11.736163
    apply_lag                 | 15 MB
    communication_time_lag    | 00:00:01.365643


The interval in which monitoring history is written is controlled by the
configuration parameter `monitor_interval_secs`; default is 2.

As this can generate a large amount of monitoring data in the table
`repmgr.monitoring_history`. it's advisable to regularly purge historical data
using the `repmgr cluster cleanup` command ; use the `-k/--keep-history` to
specify how many day's worth of data should be retained.

It's possible to use `repmgrd` to provide monitoring only for some or all
nodes by setting `failover=manual` in the node's `repmgr.conf` file. In the
event of the node's upstream failing, no failover action will be taken
and the node will require manual intervention to be reattached to replication.
If this occurs, an event notification `standby_disconnect_manual` will be
created.

Note that when a standby node is not streaming directly from its upstream
node, e.g. recovering WAL from an archive, `apply_lag` will always appear as
`0 bytes`.

* * *

> *TIP*: if monitoring history is enabled, the contents of the `monitoring_history`
> table will be replicated to attached standbys. This means there will be a small but
> constant stream of replication activity which may not be desirable. To prevent
> this, convert the table to an `UNLOGGED` one with:
>
>    ALTER TABLE repmgr.monitoring_history SET UNLOGGED;
>
> This will however mean that monitoring history will not be available on
> another node following a failover, and the view `repmgr.replication_status`
> will not work on standbys.

### `repmgrd` log output

In normal operation, `repmgrd` remains passive until a connection issue
with either the upstream or local node is detected. Otherwise there's not
much to log, so to confirm `repmgrd` is actually running, it emits log lines
like this at regular intervals:

    ...
    [2017-08-28 08:51:27] [INFO] node "node2" (node ID: 2) monitoring upstream node "node1" (node ID: 1) in normal state
    [2017-08-28 08:51:43] [INFO] node "node2" (node ID: 2) monitoring upstream node "node1" (node ID: 1) in normal state
    ...

Timing of these entries is determined by the configuration file setting
`log_status_interval`, which specifies the interval in seconds (default: `300`).

### "degraded monitoring" mode

In certain circumstances, `repmgrd` is not able to fulfill its primary mission
of monitoring the nodes' upstream server. In these cases it enters "degraded
monitoring" mode, where `repmgrd` remains active but is waiting for the situation
to be resolved.

Cases where this happens are:

- a failover situation has occurred, no nodes in the primary node's location are visible
- a failover situation has occurred, but no promotion candidate is available
- a failover situation has occurred, but the promotion candidate could not be promoted
- a failover situation has occurred, but the node was unable to follow the new primary
- a failover situation has occurred, but no primary has become available
- a failover situation has occurred, but automatic failover is not enabled for the node
- repmgrd is monitoring the primary node, but it is not available

Example output in a situation where there is only one standby with `failover=manual`,
and the primary node is unavailable (but is later restarted):

    [2017-08-29 10:59:19] [INFO] node "node2" (node ID: 2) monitoring upstream node "node1" (node ID: 1) in normal state (automatic failover disabled)
    [2017-08-29 10:59:33] [WARNING] unable to connect to upstream node "node1" (node ID: 1)
    [2017-08-29 10:59:33] [INFO] checking state of node 1, 1 of 5 attempts
    [2017-08-29 10:59:33] [INFO] sleeping 1 seconds until next reconnection attempt
    (...)
    [2017-08-29 10:59:37] [INFO] checking state of node 1, 5 of 5 attempts
    [2017-08-29 10:59:37] [WARNING] unable to reconnect to node 1 after 5 attempts
    [2017-08-29 10:59:37] [NOTICE] this node is not configured for automatic failover so will not be considered as promotion candidate
    [2017-08-29 10:59:37] [NOTICE] no other nodes are available as promotion candidate
    [2017-08-29 10:59:37] [HINT] use "repmgr standby promote" to manually promote this node
    [2017-08-29 10:59:37] [INFO] node "node2" (node ID: 2) monitoring upstream node "node1" (node ID: 1) in degraded state (automatic failover disabled)
    [2017-08-29 10:59:53] [INFO] node "node2" (node ID: 2) monitoring upstream node "node1" (node ID: 1) in degraded state (automatic failover disabled)
    [2017-08-29 11:00:45] [NOTICE] reconnected to upstream node 1 after 68 seconds, resuming monitoring
    [2017-08-29 11:00:57] [INFO] node "node2" (node ID: 2) monitoring upstream node "node1" (node ID: 1) in normal state (automatic failover disabled)

By default, `repmgrd` will continue in degraded monitoring mode indefinitely.
However a timeout (in seconds) can be set with `degraded_monitoring_timeout`.

### `repmgrd` log rotation

To ensure the current `repmgrd` logfile does not grow indefinitely, configure
your system's `logrotate` to do this. Sample configuration to rotate logfiles
weekly with retention for up to 52 weeks and rotation forced if a file grows
beyond 100Mb:

    /var/log/postgresql/repmgr-9.6.log {
        missingok
        compress
        rotate 52
        maxsize 100M
        weekly
        create 0600 postgres postgres
    }


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

Handling network splits with `repmgrd`
--------------------------------------

A common pattern for replication cluster setups is to spread servers over
more than one datacentre. This can provide benefits such as geographically-
distributed read replicas and DR (disaster recovery capability). However
this also means there is a risk of disconnection at network level between
datacentre locations, which would result in a split-brain scenario if
servers in a secondary data centre were no longer able to see the primary
in the main data centre and promoted a standby among themselves.

Previous `repmgr` versions used the concept of a `witness server` to
artificially create a quorum of servers in a particular location, ensuring
that nodes in another location will not elect a new primary if they
are unable to see the majority of nodes. However this approach does not
scale well, particularly with more complex replication setups, e.g.
where the majority of nodes are located outside of the primary datacentre.
It also means the `witness` node needs to be managed as an extra PostgreSQL
outside of the main replication cluster, which adds administrative and
programming complexity.

`repmgr4` introduces the concept of `location`: each node is associated
with an arbitrary location string (default is `default`); this is set
in `repmgr.conf`, e.g.:

    node_id=1
    node_name=node1
    conninfo='host=node1 user=repmgr dbname=repmgr connect_timeout=2'
    data_directory='/var/lib/postgresql/data'
    location='dc1'

In a failover situation, `repmgrd` will check if any servers in the
same location as the current primary node are visible.  If not, `repmgrd`
will assume a network interruption and not promote any node in any
other location (it will however enter "degraded monitoring" mode until
a primary becomes visible.



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
    repmgr node rejoin

    repmgr cluster show
    repmgr cluster matrix
    repmgr cluster crosscheck
    repmgr cluster event


* `primary register`

    Registers a primary in a streaming replication cluster, and configures
    it for use with repmgr, including installing the `repmgr` extension.
    This command needs to be executed before any  standby nodes are registered.
    Execute with the `--dry-run` option to check what would happen without
    actually registering the primary.

    `master register` can be used as an alias for `primary register`.

* `primary unregister`

   Unregisters an inactive primary node from the `repmgr` metadata. This is
   typically when the primary has failed and is being removed from the cluster
   after a new primary has been promoted. Execute with the `--dry-run` option
   to check what would happen without actually unregistering the node.

   `master unregister` can be used as an alias for `primary unregister`.

* `standby clone [node to be cloned]`

    Clones a new standby node from the data directory of the primary (or an
    upstream cascading standby) using `pg_basebackup`. This will also create
    the `recovery.conf` file required to start the server as a streaming
    replication standby.

    Execute with the`--dry-run` option to check what would happen without actually
    cloning the standby. Note this will not simulate the actual cloning process,
    but will check the prerequisites are met for cloning the standby.

* `standby register`

    Registers a standby with `repmgr`. This command needs to be executed to enable
    promote/follow operations and to allow `repmgrd` to work with the node.
    An existing standby can be registered using this command. Execute with the
    `--dry-run` option to check what would happen without actually registering the
    standby.

* `standby unregister`

    Unregisters a standby with `repmgr`. This command does not affect the actual
    replication, just removes the standby's entry from the `repmgr.nodes` table.
    See also section "Unregistering a standby from a replication cluster".

* `standby promote`

    Promotes a standby to a primary if the current primary has failed. This
    command requires a valid `repmgr.conf` file for the standby, either
    specified explicitly  with `-f/--config-file` or located in the current
    working directory; no additional arguments are required.

    If the standby promotion succeeds, the server will not need to be
    restarted. However any other standbys will need to follow the new server,
    by using `standby follow` (see below); if `repmgrd` is active, it will
    handle this automatically.

    This command will fail with an error if the current primary is still running.

* `standby follow`

    Attaches the standby to a new primary. This command requires a valid
    `repmgr.conf` file for the standby, either specified explicitly with
    `-f/--config-file` or located in the current working directory; no
    additional arguments are required.

    This command will force a restart of the standby server. It can only be used
    to attach a standby to a new primary node.

* `standby switchover`

    Promotes a standby to primary and demotes the existing primary to a standby.
    This command must be run on the standby to be promoted, and requires a
    passwordless SSH connection to the current primary.

    If other standbys (siblings of the promotion candidate) are connected
    to the demotion candidate, `repmgr`  can instruct these to follow the
    new primary if the option `--siblings-follow` is specified.

    Execute with the `--dry-run` option to test the switchover as far as
    possible without actually changing the status of either node.

    `repmgrd` should not be active on any nodes while a switchover is being
    carried out. This   restriction may be lifted in a later version.

    For more details see the section "Performing a switchover with repmgr".

* `node status`

    Displays an overview of a node's basic information and replication
    status. This command must be run on the local node.

    Sample output (execute `repmgr node status`):

        Node "node1":
            PostgreSQL version: 10beta1
            Total data size: 30 MB
            Conninfo: host=node1 dbname=repmgr user=repmgr connect_timeout=2
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

* `node rejoin`

    Enables a stopped node to be rejoined to the replication cluster.

    Note that currently `node rejoin` can only be used to attach a standby to the
    current primary, not another standby.

    The node must have been shut down cleanly; if this was not the case, it will
    need to be manually started (remove any existing `recovery.conf` file) until
    it has reached a consistent recovery point, then shut down cleanly/

    Usage:

        repmgr node rejoin -d '$conninfo'

    where `$conninfo` is the conninfo string of any reachable node in the cluster.
    `repmgr.conf` for the stopped node *must* be supplied explicitly if not
    otherwise available.

    `node rejoin` can optionally use `pg_rewind` to re-integrate a node which has
    diverged from the rest of the cluster, typically a failed primary.
    `pg_rewind` is available in PostgreSQL 9.5 and later.

    *NOTE*: `pg_rewind` *requires* that either `wal_log_hints` is enabled, or that
    data checksums were enabled when the cluster was initialized. See the
    `pg_rewind` documentation for details:

         https://www.postgresql.org/docs/current/static/app-pgrewind.html


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

         ID | Name  | Role    | Status    | Upstream | Location | Connection string
        ----+-------+---------+-----------+----------+----------+--------------------------------------
         1  | node1 | primary | * running |          | default  | host=node1 dbname=repmgr user=repmgr
         2  | node2 | standby |   running | node1    | default  | host=node2 dbname=repmgr user=repmgr
         3  | node3 | standby |   running | node1    | default  | host=node3 dbname=repmgr user=repmgr

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
    all nodes. Both commands can provided output in CSV format if the
    `--csv` command line option is provided.

    Example 1 (all nodes up):

        $ repmgr -f /etc/repmgr.conf cluster matrix

        Name   | Id |  1 |  2 |  3
        -------+----+----+----+----
         node1 |  1 |  * |  * |  *
         node2 |  2 |  * |  * |  *
         node3 |  3 |  * |  * |  *

    Here `cluster matrix` is sufficient to establish the state of each
    possible connection.


    Example 2 (`node1` and `node2` up, `node3` down):

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

    The other two nodes are up; the corresponding rows have `x` in the
    column corresponding to `node3`, meaning that inbound connections to
    that node have failed, and `*` in the columns corresponding to
    node1 and node2, meaning that inbound connections to these nodes
    have succeeded.

    In this case, `cluster crosscheck` gives the same result as `cluster
    matrix`, because from any functioning node we can observe the same
    state: `node1` and `node2` are up, `node3` is down.

    Example 3 (all nodes up, firewall dropping packets originating
               from `node1` and directed to port 5432 on `node3`)

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

* `cluster cleanup`

    Purges monitoring history from the `repmgr.monitoring_history` table to
    prevent excessive table growth. Use the `-k/--keep-history` to specify the
    number of days of monitoring history to retain. This command can be used
    manually or as a cronjob.

    This command requires a valid `repmgr.conf` file for the node on which it is
    executed, either specified explicitly with `-f/--config-file` or located in
    the current working directory; no additional arguments are required.


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
           1 | primary_register | t          | 2016-01-08 15:04:39.781733+09 |
           2 | standby_clone    | t          | 2016-01-08 15:04:49.530001+09 | Cloned from host 'repmgr_node1', port 5432; backup method: pg_basebackup; --force: N
           2 | standby_register | t          | 2016-01-08 15:04:50.621292+09 |
    (3 rows)

Alternatively, use `repmgr cluster event` to output a formatted list of events.

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

  * `primary_register`
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

Note that under some circumstances (e.g. no replication cluster primary could
be located), it will not be possible to write an entry into the `repmgr.events`
table, in which case executing a script via `event_notification_command` can
serve as a fallback by generating some form of notification.

Upgrading repmgr
----------------

`repmgr` is updated regularly with point releases (e.g. 3.0.2 to 3.0.3)
containing bugfixes and other minor improvements. Any substantial new
functionality will be included in a feature release (e.g. 3.0.x to 3.1.x).


Diagnostics
-----------

    $ repmgr -f /etc/repmgr.conf node service --list-actions
    Following commands would be executed for each action:

        start: "/usr/bin/pg_ctl -l /var/log/postgresql/startup.log -w -D '/var/lib/pgsql/data' start"
         stop: "/usr/bin/pg_ctl -l /var/log/postgresql/startup.log -D '/var/lib/pgsql/data' -m fast -W stop"
      restart: "/usr/bin/pg_ctl -l /var/log/postgresql/startup.log -w -D '/var/lib/pgsql/data' restart"
       reload: "/usr/bin/pg_ctl -l /var/log/postgresql/startup.log -w -D '/var/lib/pgsql/data' reload"
      promote: "/usr/bin/pg_ctl -l /var/log/postgresql/startup.log -w -D '/var/lib/pgsql/data' promote"


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

* http://blog.2ndquadrant.com/repmgr-3-2-is-here-barman-support-brand-new-high-availability-features/
* http://blog.2ndquadrant.com/improvements-in-repmgr-3-1-4/
* http://blog.2ndquadrant.com/managing-useful-clusters-repmgr/
* http://blog.2ndquadrant.com/easier_postgresql_90_clusters/
