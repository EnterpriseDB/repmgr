
New command line options
------------------------

- --dry-run: repmgr will attempt to perform the action as far as possible
   without making any changes to the database

Changed command line options
----------------------------

- --replication-user is now passed when registering the master server (and
  optionally when registering a standby), *not* during standby clone/follow.
  The value (defaults to the user in the conninfo string) will be stored in
  the repmgr metadata for use by  standby clone/follow..