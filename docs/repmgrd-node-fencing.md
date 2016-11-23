Fencing a failed master node with repmgrd and pgbouncer
=======================================================

With automatic failover, it's essential to ensure that a failed master
remains inaccessible to your application, even if it comes back online
again, to avoid a split-brain situation.

By using `pgbouncer` together with `repmgrd`, it's possible to combine
automatic failover with a process to isolate the failed master from
your application and ensure that all connections which should go to
the master are directed there smoothly without having to reconfigure
your application. (Note that as a connection pooler, `pgbouncer` can
benefit your application in other ways, but those are beyond the scope
of this document).

* * *

> *WARNING*: automatic failover is tricky to get right. This document
> demonstrates one possible implementation method, however you should
> carefully configure and test any setup to suit the needs of your own
> replication cluster/application.

* * *

In a failover situation, `repmgrd` promotes a standby to master by executing
the command defined in `promote_command`. Normally this would be something like:

    repmgr standby promote -f /etc/repmgr.conf

By wrapping this in a custom script which adjusts the `pgbouncer` configuration
on all nodes, it's possible to fence the failed master and redirect write
connections to the new master.

The script consists of three sections:

* commands to pause `pgbouncer` on all nodes
* the promotion command itself
* commands to reconfigure and restart `pgbouncer` on all nodes

Note that it requires password-less SSH access between all nodes to be able to
update the `pgbouncer` configuration files.

For the purposes of this demonstration, we'll assume there are 3 nodes (master
and two standbys), with `pgbouncer` listening on port 6432 handling connections
to a database called `appdb`.  The `postgres` system user must have write
access to the `pgbouncer` configuration files on all nodes. We'll assume
there's a main `pgbouncer` configuration file, `/etc/pgbouncer.ini`, which uses
the `%include` directive (available from PgBouncer 1.6) to include a separate
configuration file, `/etc/pgbouncer.database.ini`, which will be modified by
`repmgr`.

`/etc/pgbouncer.ini` should look something like this:

    [pgbouncer]

    logfile = /var/log/pgbouncer/pgbouncer.log
    pidfile = /var/run/pgbouncer/pgbouncer.pid

    listen_addr = *
    listen_port = 6532
    unix_socket_dir = /tmp

    auth_type = trust
    auth_file = /etc/pgbouncer.auth

    admin_users = postgres
    stats_users = postgres

    pool_mode = transaction

    max_client_conn = 100
    default_pool_size = 20
    min_pool_size = 5
    reserve_pool_size = 5
    reserve_pool_timeout = 3

    log_connections = 1
    log_disconnections = 1
    log_pooler_errors = 1

    %include /etc/pgbouncer.database.ini

The actual script is as follows; adjust the configurable items as appropriate:

`/var/lib/postgres/repmgr/promote.sh`


    #!/usr/bin/env bash
    set -u
    set -e

    # Configurable items
    PGBOUNCER_HOSTS="node1 node2 node3"
    PGBOUNCER_DATABASE_INI="/etc/pgbouncer.database.ini"
    PGBOUNCER_DATABASE="appdb"
    PGBOUNCER_PORT=6432

    REPMGR_DB="repmgr"
    REPMGR_USER="repmgr"
    REPMGR_SCHEMA="repmgr_test"

    # 1. Pause running pgbouncer instances
    for HOST in $PGBOUNCER_HOSTS
    do
        psql -t -c "pause" -h $HOST -p $PGBOUNCER_PORT -U postgres pgbouncer
    done

    # 2. Promote this node from standby to master

    repmgr standby promote -f /etc/repmgr.conf

    # 3. Reconfigure pgbouncer instances

    PGBOUNCER_DATABASE_INI_NEW="/tmp/pgbouncer.database.ini"

    for HOST in $PGBOUNCER_HOSTS
    do
        # Recreate the pgbouncer config file
        echo -e "[databases]\n" > $PGBOUNCER_DATABASE_INI_NEW

        psql -d $REPMGR_DB -U $REPMGR_USER -t -A \
          -c "SELECT '${PGBOUNCER_DATABASE}-rw= ' || conninfo || ' application_name=pgbouncer_${HOST}' \
              FROM ${REPMGR_SCHEMA}.repl_nodes \
              WHERE active = TRUE AND type='master'" >> $PGBOUNCER_DATABASE_INI_NEW

        psql -d $REPMGR_DB -U $REPMGR_USER -t -A \
          -c "SELECT '${PGBOUNCER_DATABASE}-ro= ' || conninfo || ' application_name=pgbouncer_${HOST}' \
              FROM $REPMGR_SCHEMA.repl_nodes \
              WHERE node_name='${HOST}'" >> $PGBOUNCER_DATABASE_INI_NEW

        rsync $PGBOUNCER_DATABASE_INI_NEW $HOST:$PGBOUNCER_DATABASE_INI

        psql -tc "reload" -h $HOST -p $PGBOUNCER_PORT -U postgres pgbouncer
        psql -tc "resume" -h $HOST -p $PGBOUNCER_PORT -U postgres pgbouncer

    done

    # Clean up generated file
    rm $PGBOUNCER_DATABASE_INI_NEW

    echo "Reconfiguration of pgbouncer complete"

Script and template file should be installed on each node where
`repmgrd` is running.

Finally, set `promote_command` in `repmgr.conf` on each node to
point to the custom promote script:

    promote_command=/var/lib/postgres/repmgr/promote.sh

and reload/restart any running `repmgrd` instances for the changes to take
effect.
