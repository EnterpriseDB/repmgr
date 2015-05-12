repmgr quickstart guide
=======================

This quickstart guide provides some annotated examples on basic
`repmgr` setup. It assumes you are familiar with PostgreSQL replication
concepts setup and Linux/UNIX system administration.

For the purposes of this guide, we'll assume the database user will be
`repmgr_usr` and the database will be `repmgr_db`.


Master setup
------------

1. Configure PostgreSQL

  - create user and database:

	```
	CREATE ROLE repmgr_usr LOGIN SUPERUSER;
	CREATE DATABASE repmgr_db OWNER repmgr_usr;
	```

  - configure `postgresql.conf` for replication (see README.md for sample
    settings)

  - update `pg_hba.conf`, e.g.:

	```
	host    repmgr_db       repmgr_usr  192.168.1.0/24         trust
	host    replication     repmgr_usr  192.168.1.0/24         trust
	```

  Restart the PostgreSQL server after making these changes.

2. Create the `repmgr` configuration file:

        $ cat /path/to/repmgr/node1/repmgr.conf
        cluster=test
        node=1
        node_name=node1
        conninfo='host=repmgr_node1 user=repmgr_usr dbname=repmgr_db'
        pg_bindir=/path/to/postgres/bin

   (For an annotated `repmgr.conf` file, see `repmgr.conf.sample` in the
   repository's root directory).

3. Register the master node with `repmgr`:

        $ repmgr -f /path/to/repmgr/node1/repmgr.conf --verbose master register
        [2015-03-03 17:45:53] [INFO] repmgr connecting to master database
        [2015-03-03 17:45:53] [INFO] repmgr connected to master, checking its state
        [2015-03-03 17:45:53] [INFO] master register: creating database objects inside the repmgr_test schema
        [2015-03-03 17:45:53] [NOTICE] Master node correctly registered for cluster test with id 1 (conninfo: host=localhost user=repmgr_usr dbname=repmgr_db)

Standby setup
-------------

1. Use `repmgr standby clone` to clone a standby from the master:

        repmgr -D /path/to/standby/data -d repmgr_db -U repmgr_usr --verbose standby clone 192.168.1.2
        [2015-03-03 18:18:21] [NOTICE] No configuration file provided and default file './repmgr.conf' not found - continuing with default values
        [2015-03-03 18:18:21] [NOTICE] repmgr Destination directory ' /path/to/standby/data' provided
        [2015-03-03 18:18:21] [INFO] repmgr connecting to upstream node
        [2015-03-03 18:18:21] [INFO] repmgr connected to upstream node, checking its state
        [2015-03-03 18:18:21] [INFO] Successfully connected to upstream node. Current installation size is 27 MB
        [2015-03-03 18:18:21] [NOTICE] Starting backup...
        [2015-03-03 18:18:21] [INFO] creating directory " /path/to/standby/data"...
        [2015-03-03 18:18:21] [INFO] Executing: 'pg_basebackup -l "repmgr base backup"  -h localhost -p 9595 -U repmgr_usr -D  /path/to/standby/data '
        NOTICE:  pg_stop_backup complete, all required WAL segments have been archived
        [2015-03-03 18:18:23] [NOTICE] repmgr standby clone (using pg_basebackup) complete
        [2015-03-03 18:18:23] [NOTICE] HINT: You can now start your postgresql server
        [2015-03-03 18:18:23] [NOTICE] for example : pg_ctl -D  /path/to/standby/data start

  Note that the `repmgr.conf` file is not required when cloning a standby.
  However we recommend providing a valid `repmgr.conf` if you wish to use
  replication slots, or want `repmgr` to log the clone event to the
  `repl_events` table.

  This will clone the PostgreSQL database files from the master, including its
  `postgresql.conf` and `pg_hba.conf` files, and additionally automatically create
  the `recovery.conf` file containing the correct parameters to start streaming
  from the primary node.

2. Start the PostgreSQL server

3. Create the `repmgr` configuration file:

        $ cat /path/node2/repmgr/repmgr.conf
        cluster=test
        node=2
        node_name=node2
        conninfo='host=repmgr_node2 user=repmgr_usr dbname=repmgr_db'
        pg_bindir=/path/to/postgres/bin

4. Register the standby node with `repmgr`:

        $ repmgr -f /path/to/repmgr/node2/repmgr.conf --verbose standby register
        [2015-03-03 18:24:34] [NOTICE] Opening configuration file: /path/to/repmgr/node2/repmgr.conf
        [2015-03-03 18:24:34] [INFO] repmgr connecting to standby database
        [2015-03-03 18:24:34] [INFO] repmgr connecting to master database
        [2015-03-03 18:24:34] [INFO] finding node list for cluster 'test'
        [2015-03-03 18:24:34] [INFO] checking role of cluster node '1'
        [2015-03-03 18:24:34] [INFO] repmgr connected to master, checking its state
        [2015-03-03 18:24:34] [INFO] repmgr registering the standby
        [2015-03-03 18:24:34] [INFO] repmgr registering the standby complete
        [2015-03-03 18:24:34] [NOTICE] Standby node correctly registered for cluster test with id 2 (conninfo: host=localhost user=repmgr_usr dbname=repmgr_db)


This concludes the basic `repmgr` setup of master and standby. The records
created in the `repl_nodes` table should look something like this:

        repmgr_db=# SELECT * from repmgr_test.repl_nodes;
         id |  type   | upstream_node_id | cluster | name  |                     conninfo                    | slot_name | priority | active
        ----+---------+------------------+---------+-------+-------------------------------------------------+-----------+----------+--------
          1 | primary |                  | test    | node1 | host=localhost user=repmgr_usr dbname=repmgr_db |           |        0 | t
          2 | standby |                1 | test    | node2 | host=localhost user=repmgr_usr dbname=repmgr_db |           |        0 | t
        (2 rows)
