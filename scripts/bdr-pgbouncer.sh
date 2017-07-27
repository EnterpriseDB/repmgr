#!/usr/bin/env bash
set -u
set -e

# Process parameters passed to script
# -----------------------------------
#
# This assumes the repmgr "event_notification_command" is defined like this:
#
#   event_notification_command='/path/to/bdr-pgbouncer.sh %n %e %s "%c" "%a" >> /tmp/bdr-failover.log 2>&1'
#
# Adjust as appropriate.

NODE_ID=$1
EVENT_TYPE=$2
SUCCESS=$3
NEXT_CONNINFO=$4
NEXT_NODE_NAME=$5

if [ "$EVENT_TYPE" != "bdr_failover" ]; then
    echo "unable to handle event type '$EVENT_TYPE'"
    exit
fi

# Define database name here
# -------------------------
#
# Note: this assumes the BDR-enabled database has the same name on
# both hosts

BDR_DBNAME=bdr_db

# Define PgBouncer hosts here
# ---------------------------

PGBOUNCER_HOSTS="host1 host2"
PGBOUNCER_PORTS=(6432 6432)
PGBOUNCER_DATABASE_INI=(/path/to/pgbouncer.database.ini /path/to/pgbouncer.database.ini)


# Define local host info here
# ---------------------------

THIS_HOST="host1"
THIS_PGBOUNCER_PORT="6432"
THIS_DB_PORT="5432"

# Pause all pgbouncer nodes to minimize impact on clients
# -------------------------------------------------------

i=0
for HOST in $PGBOUNCER_HOSTS
do
    PORT="${PGBOUNCER_PORTS[$i]}"

    psql -tc "pause" -h $HOST -p $PORT -U postgres pgbouncer

    i=$((i+1))
done

# Copy pgbouncer database ini file to all nodes and restart pgbouncer
# -------------------------------------------------------------------

i=0
THIS_HOSTPORT="$THIS_HOST$THIS_PGBOUNCER_PORT"
PGBOUNCER_DATABASE_INI_NEW="/tmp/pgbouncer.database.ini.new"

for HOST in $PGBOUNCER_HOSTS
do
    PORT="${PGBOUNCER_PORTS[$i]}"

    # Recreate the pgbouncer config file
    # ----------------------------------
    echo -e "[databases]\n" > $PGBOUNCER_DATABASE_INI_NEW

    echo -e "$BDR_DBNAME= $NEXT_CONNINFO application_name=pgbouncer_$PORT" >> $PGBOUNCER_DATABASE_INI_NEW

    # Copy file to host
    # -----------------
    CONFIG="${PGBOUNCER_DATABASE_INI[$i]}"

    if [ "$HOST$PORT" != "$THIS_HOSTPORT" ]; then
      rsync $PGBOUNCER_DATABASE_INI_NEW $HOST:$CONFIG
    else
      cp $PGBOUNCER_DATABASE_INI_NEW $CONFIG
    fi

    # Reload and resume PgBouncer
    # ---------------------------

    psql -tc "reload" -h $HOST -p $PORT -U postgres pgbouncer
    psql -tc "resume" -h $HOST -p $PORT -U postgres pgbouncer

    i=$((i+1))
done


# Clean up generated file
rm $PGBOUNCER_DATABASE_INI_NEW

echo "Reconfiguration of pgbouncer complete"
