FAQ - Frequently Asked Questions about repmgr
=============================================


General
-------

- What's the difference between the repmgr versions?

  repmgr v3 builds on the improved replication facilities added
  in PostgreSQL 9.3, as well as improved automated failover support
  via `repmgrd`, and is not compatible with PostgreSQL 9.2 and earlier.

  repmgr v2 supports PostgreSQL 9.0 onwards. While it is compatible
  with  PostgreSQL 9.3 and later, we recommend repmgr v3.

- What's the advantage of using replication slots?

  Replication slots, introduced in PostgreSQL 9.4, ensure that the
  master server will retain WAL files until they have been consumed
  by all standby servers. This makes WAL file management much easier,
  and if used `repmgr` will no longer insist on a fixed number (default: 5000)
  of WAL files being preserved.

  (However this does mean that if a standby is no longer connected to the
  master, the master will retain WAL files indefinitely).

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

- Is there an easy way to check my master server is correctly configured
  for use with `repmgr`?

  Yes - execute `repmgr` with the `--check-upstream-config` option, and it
  will let you know which items in `postgresql.conf` need to be modified.

`repmgrd`
---------

- Do I need a witness server?

  Not necessarily. However if you have an uneven number of nodes spread
  over more than one network segment, a witness server will enable
  better handling of a 'split brain' situation by providing a "casting
  vote" on the preferred network segment.