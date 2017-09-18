Upgrading from repmgr 3
=======================

The upgrade process consists of two steps:

    1) converting the repmgr.conf configuration files
    2) upgrading the repmgr schema.

Scripts are provided to assist both with converting repmgr.conf
and upgrading the schema.

Converting repmgr.conf configuration files
------------------------------------------

With a completely new repmgr version, we've taken the opportunity
to rename some configuration items have had their names changed for
clarity and consistency, both between the configuration file and
the column names in `repmgr.nodes` (e.g. `node` → `node_id`), and
also for consistency with PostgreSQL naming conventions
(e.g. `loglevel` → `log_level`).

Other configuration items have been changed to command line options,
and vice-versa, e.g. to avoid hard-coding items such as a a node's
upstream ID, which might change over time.

`repmgr` will issue a warning about deprecated/altered options.


### Changed parameters

Following parameters have been added:

    - `data_directory`: this is mandatory and must contain the path
        to the node's data directory
    - `monitoring_history`: this replaces the `repmgrd` command line
        option `--monitoring-history`

Following parameters have been renamed:

    - `node` → `node_id`
    - `loglevel` → `log_level`
    - `logfacility` → `log_facility`
    - `logfile` → `log_file`
    - `master_reponse_timeout` → `async_query_timeout`

Following parameters have been removed:

    - `cluster` is no longer required and will be ignored.
    - `upstream_node_id` is replaced by the command-line parameter
         `--upstream-node-id`

### Conversion script

To assist with conversion of `repmgr.conf` files, a Perl script
is provided in `contrib/convert-config.pl`. Use like this:

    $ ./convert-config.pl /etc/repmgr.conf
    node_id=2
    node_name=node2
    conninfo=host=localhost dbname=repmgr user=repmgr port=5602
    pg_ctl_options='-l /tmp/postgres.5602.log'
    pg_bindir=/home/barwick/devel/builds/HEAD/bin
    rsync_options=--exclude=postgresql.local.conf --archive
    log_level=DEBUG
    pg_basebackup_options=--no-slot
    data_directory=

The converted file is printed to `STDOUT` and the original file is not
changed.

Please note that the parameter `data_directory` *must* be provided;
if not already present, the conversion script will add an empty
placeholder parameter.


Upgrading the repmgr schema
---------------------------

Ensure `repmgrd` is not running, or any cron jobs which execute the
`repmgr` binary.

Install `repmgr4`; any `repmgr3` packages should be uninstalled
(if not automatically installed already).

### Manually create the repmgr extension

In the database used by the existing `repmgr` configuration, execute:

    CREATE EXTENSION repmgr FROM unpackaged;

This will move and convert all objects from the existing schema
into the new, standard `repmgr` schema.

> *NOTE* there must be only one schema matching 'repmgr_%' in the
> database, otherwise this step may not work.

### Re-register each node

This is necessary to update the `repmgr` metadata with some additional items.

On the primary node, execute e.g.

    repmgr primary register -f /etc/repmgr.conf --force

On each standby node, execute e.g.

    repmgr standby register -f /etc/repmgr.conf --force

Check the data is updated as expected by examining the `repmgr.nodes` table;
restart `repmgrd` if required.

The original `repmgr_$cluster` schema can be dropped at any time.

* * *

> *TIP* If you don't care about any data from the existing `repmgr` installation,
> (e.g. the contents of the `events` and `monitoring` tables), the manual
> "CREATE EXTENSION" step can be skipped; just re-register each node, starting
> with the primary node, and the `repmgr` extension will be automatically created.

* * *
