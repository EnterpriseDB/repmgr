FAQ - Frequently Asked Questions about repmgr
=============================================

This FAQ applies to `repmgr` 3.0 and later.

General
-------

- What's the difference between the repmgr versions?

  repmgr 3.x builds on the improved replication facilities added
  in PostgreSQL 9.3, as well as improved automated failover support
  via `repmgrd`, and is not compatible with PostgreSQL 9.2 and earlier.

  repmgr 2.x supports PostgreSQL 9.0 onwards. While it is compatible
  with  PostgreSQL 9.3 and later, we recommend repmgr v3.

- What's the advantage of using replication slots?

  Replication slots, introduced in PostgreSQL 9.4, ensure that the
  master server will retain WAL files until they have been consumed
  by all standby servers. This makes WAL file management much easier,
  and if used `repmgr` will no longer insist on a fixed number (default: 5000)
  of WAL files being preserved.

  (However this does mean that if a standby is no longer connected to the
  master, the master will retain WAL files indefinitely).

- How many replication slots should I define in `max_replication_slots`?

  Normally at least same number as the number of standbys which will connect
  to the node. Note that changes to `max_replication_slots` require a server
  restart to take effect, and as there is no particular penalty for unused
  replication slots, setting a higher figure will make adding new nodes
  easier.

- Does `repmgr` support hash indexes?

  No. Hash indexes and replication do not mix well and their use is
  explicitly discouraged; see:
    https://www.postgresql.org/docs/current/interactive/sql-createindex.html#AEN74175

`repmgr`
--------

- When should I use the --rsync-only option?

  By default, `repmgr` uses `pg_basebackup` to clone a standby from
  a master. However, `pg_basebackup` copies the entire data directory, which
  can take some time depending on installation size. If you have an
  existing but "stale" standby, `repmgr` can use `rsync` instead,
  which means only changed or added files need to be copied.

- Can I register an existing master/standby?

  Yes, this is no problem.

- How can a failed master be re-added as a standby?

  This is a two-stage process. First, the failed master's data directory
  must be re-synced with the current master; secondly the failed master
  needs to be re-registered as a standby. The section "Converting a failed
  master to a standby" in the `README.md` file contains more detailed
  information on this process.

- Is there an easy way to check my master server is correctly configured
  for use with `repmgr`?

  Yes - execute `repmgr` with the `--check-upstream-config` option, and it
  will let you know which items in `postgresql.conf` need to be modified.

- Even though I specified custom `rsync` options, `repmgr` appends
  the `--checksum` - why?

  When syncing a stale data directory from an active server, it's
  essential that `rsync` compares the content of files rather than
  just timestamp and size, to ensure that all changed files are
  copied and prevent corruption.

- When cloning a standby, how can I prevent `repmgr` from copying
  `postgresql.conf` and `pg_hba.conf` from the PostgreSQL configuration
  directory in `/etc`?

  Use the command line option `--ignore-external-config-files`

- How can I prevent `repmgr` from copying local configuration files
  in the data directory?

  If you're updating an existing but stale data directory which
  contains e.g. configuration files you don't want to be overwritten
  with the same file from the master, specify the files in the
  `rsync_options` configuration option, e.g.

      rsync_options=--exclude=postgresql.local.conf

  This option is only available when using the `--rsync-only` option.

- How can I make the witness server use a particular port?

  By default the witness server is configured to use port 5499; this
  is intended to support running the witness server as  a separate
  instance on a normal node server, rather than on its own dedicated server.

  To specify different port for the witness server, supply the port number
  in the `conninfo` string in `repmgr.conf`
  (repmgr 3.0.1 and earlier: use the `-l/--local-port` option)

- Do I need to include `shared_preload_libraries = 'repmgr_funcs'`
  in `postgresql.conf` if I'm not using `repmgrd`?

  No, the `repmgr_funcs` library is only needed when running `repmgrd`.
  If you later decide to run `repmgrd`, you just need to add
  `shared_preload_libraries = 'repmgr_funcs'` and restart PostgreSQL.

- I've provided replication permission for the `repmgr` user in `pg_hba.conf`
  but `repmgr`/`repmgrd` complains it can't connect to the server... Why?

  `repmgr`/`repmgrd` need to be able to connect to the repmgr database
  with a normal connection to query metadata. The `replication` connection
  permission is for PostgreSQL's streaming replication and doesn't
  necessarily need to be the `repmgr` user.

- When cloning a standby, why do I need to provide the connection parameters
  for the primary server on the command line, not in the configuration file?

  Cloning a standby is a one-time action; the role of the server being cloned
  from could change, so fixing it in the configuration file would create
  confusion. If `repmgr` needs to establish a connection to the primary
  server, it can retrieve this from the `repl_nodes` table or if necessary
  scan the replication cluster until it locates the active primary.

- Why is there no foreign key on the `node_id` column in the `repl_events`
  table?

  Under some circumstances event notifications can be generated for servers
  which have not yet been registered; it's also useful to retain a record
  of events which includes servers removed from the replication cluster
  which no longer have an entry in the `repl_nodes` table.


`repmgrd`
---------

- Do I need a witness server?

  Not necessarily. However if you have an uneven number of nodes spread
  over more than one network segment, a witness server will enable
  better handling of a 'split brain' situation by providing a "casting
  vote" on the preferred network segment.

- How can I prevent a node from ever being promoted to master?

  In `repmgr.conf`, set its priority to a value of 0 or less.

  Additionally, if `failover` is set to `manual`, the node will never
  be considered as a promotion candidate.

- Does `repmgrd` support delayed standbys?

  `repmgrd` can monitor delayed standbys - those set up with
  `recovery_min_apply_delay` set to a non-zero value in `recovery.conf` -
  but as it's not currently possible to directly examine the value
  applied to the standby, `repmgrd` may not be able to properly evaluate
  the node as a promotion candidate.

  We recommend that delayed standbys are explicitly excluded from promotion
  by setting `priority` to 0 in `repmgr.conf`.

  Note that after registering a delayed standby, `repmgrd` will only start
  once the metadata added in the master node has been replicated.

- How can I get `repmgrd` to rotate its logfile?

  Configure your system's `logrotate` service to do this; see example
  in README.md

- I've recloned a failed master as a standby, but `repmgrd` refuses to start?

  Check you registered the standby after recloning. If unregistered the standby
  cannot be considered as a promotion candidate even if `failover` is set to
  `automatic`, which is probably not what you want. `repmgrd` will start if
  `failover` is set to `manual` so the node's replication status can still
  be monitored, if desired.
