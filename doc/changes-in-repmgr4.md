
New command line options
------------------------

- `--dry-run`: repmgr will attempt to perform the action as far as possible
   without making any changes to the database

Changed command line options
----------------------------


### repmgr

- `--replication-user` is now passed when registering the master server (and
  optionally when registering a standby), *not* during standby clone/follow.
  The value (defaults to the user in the conninfo string) will be stored in
  the repmgr metadata for use by  standby clone/follow..

### repmgrd

- `--monitoring-history` is deprecated and has been replaced by the
  configuration file option `monitoring_history`. This enables the
  setting to be changed without having to modify system service files.

Removed configuration file options
----------------------------------

- `upstream_node`: see note about `--upstream-node-id` above.

Logging changes
---------------

- Following configuration file parameters have been renamed for consistency
  with other parameters (and conform to the pattern used by PostgreSQL itself,
  which uses the prefix `log_` for logging parameters):
  - `loglevel` has been renamed to `log_level`
  - `logfile` has been renamed to `log_file`
  - `logfacility` has been renamed to `log_facility`
- default value for `log_level` is `INFO` rather than `NOTICE`.
- new parameter `log_status_interval`

