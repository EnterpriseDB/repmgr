
Standardisation on `primary`
----------------------------

To standardise terminolgy, `primary` is used to denote the read/write
node in a streaming replication cluster. `master` is still accepted
as a synonym (e.g. `repmgr master register`).


New command line options
------------------------

- `--dry-run`: repmgr will attempt to perform the action as far as possible
   without making any changes to the database

- `--upstream-node-id`: use to specify the upstream node the standby will
  connect later stream from, when cloning a standby. This replaces the configuration
  file parameter `upstream_node`, as the upstream node is set when the standby
  is initially cloned, but can change over the lifetime of an installation (due
  to failovers, switchovers etc.) so it's pointless/confusing keeping the original
  value around in the config file.

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

Changes to repmgr commands
--------------------------


### `repmgr cluster show`

This now displays the role of each node (e.g. `primary`, `standby`)
and its status in separate columns.

The `--csv` option now emits a third column indicating the recovery
status of the node.


Configuration file changes
--------------------------

### Required settings

The following 4 parameters are mandatory in `repmgr.conf`:

- `node_id`
- `node_name`
- `conninfo`
- `data_directory`


### Renamed settings

Some settings have been renamed for clarity and consistency:

- `node`: now `node_id`
- `name`: now `node_name`
- `master_reponse_timeout`: now `async_query_timeout` to better indicate its
   purpose

- The following configuration file parameters have been renamed for consistency
  with other parameters (and conform to the pattern used by PostgreSQL itself,
  which uses the prefix `log_` for logging parameters):
  - `loglevel` has been renamed to `log_level`
  - `logfile` has been renamed to `log_file`
  - `logfacility` has been renamed to `log_facility`

### Removed settings

- `cluster`: has been removed
- `upstream_node`: see note about `--upstream-node-id` above.
- `retry_promote_interval_secs`: this is now redundant due to changes in the
   failover/promotion mechanism; the new equivalent is `primary_notification_timeout`


### Logging changes

- default value for `log_level` is `INFO` rather than `NOTICE`.
- new parameter `log_status_interval`, which causes `repmgrd` to emit a status log
  line at the specified interval

