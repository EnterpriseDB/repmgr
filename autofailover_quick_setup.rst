=====================================================
 PostgreSQL Automatic Fail-Over - User Documentation
=====================================================

Automatic Failover
==================

repmgr allows setups for automatic failover when it detects the failure of the master node.
Following is a quick setup for this.

Installation
============

For convenience, we define:

**node1**
    is the hostname fully qualified of the Master server, IP 192.168.1.10
**node2**
    is the hostname fully qualified of the Standby server, IP 192.168.1.11
**witness**
    is the hostname fully qualified of the server used for witness, IP 192.168.1.12

**Note:** It is not recommanded to use name defining status of a server like «masterserver»,
this is a name leading to confusion once a failover take place and the Master is
now on the «standbyserver».

Summary
-------

2 PostgreSQL servers are involved in the replication.  Automatic fail-over need
to vote to decide what server it should promote, thus an odd number is required
and a witness-repmgrd is installed in a third server where it uses a PostgreSQL
cluster to communicate with other repmgrd daemons.

1. Install PostgreSQL in all the servers involved (including the server used for
   witness)

2. Install repmgr in all the servers involved (including the server used for witness)

3. Configure the Master PostreSQL

4. Clone the Master to the Standby using "repmgr standby clone" command

5. Configure repmgr in all the servers involved (including the server used for witness)

6. Register Master and Standby nodes

7. Initiate witness server

8. Start the repmgrd daemons in all nodes

**Note** A complete High-Availability design needs at least 3 servers to still have
a backup node after a first failure.

Install PostgreSQL
------------------

You can install PostgreSQL using any of the recommended methods. You should ensure
it's 9.0 or later.

Install repmgr
--------------

Install repmgr following the steps in the README file.

Configure PostreSQL
-------------------

Log in node1.

Edit the file postgresql.conf and modify the parameters::

  listen_addresses='*'
  wal_level = 'hot_standby'
  archive_mode = on
  archive_command = 'cd .'	 # we can also use exit 0, anything that
                             # just does nothing
  max_wal_senders = 10
  wal_keep_segments = 5000   # 80 GB required on pg_xlog
  hot_standby = on
  shared_preload_libraries = 'repmgr_funcs'

Edit the file pg_hba.conf and add lines for the replication::

  host     repmgr           repmgr      127.0.0.1/32            trust
  host     repmgr           repmgr      192.168.1.10/30         trust
  host     replication      all         192.168.1.10/30         trust

**Note:** It is also possible to use a password authentication (md5), .pgpass file
should be edited to allow connection between each node.

Create the user and database to manage replication::

  su - postgres
  createuser -s repmgr
  createdb -O repmgr repmgr
  psql -f /usr/share/postgresql/9.0/contrib/repmgr_funcs.sql repmgr

Restart the PostgreSQL server::

  pg_ctl -D $PGDATA restart

And check everything is fine in the server log.

Create the ssh-key for the postgres user and copy it to other servers::

  su - postgres
  ssh-keygen             # /!\ do not use a passphrase /!\
  cat ~/.ssh/id_rsa.pub > ~/.ssh/authorized_keys
  chmod 600 ~/.ssh/authorized_keys
  exit
  rsync -avz ~postgres/.ssh/authorized_keys node2:~postgres/.ssh/
  rsync -avz ~postgres/.ssh/authorized_keys witness:~postgres/.ssh/
  rsync -avz ~postgres/.ssh/id_rsa* node2:~postgres/.ssh/
  rsync -avz ~postgres/.ssh/id_rsa* witness:~postgres/.ssh/

Clone Master
------------

Log in node2.

Clone the node1 (the current Master)::

  su - postgres
  repmgr -d repmgr -U repmgr -h node1 standby clone 

Start the PostgreSQL server::

  pg_ctl -D $PGDATA start

And check everything is fine in the server log.

Configure repmgr
----------------

Log in each server and configure repmgr by editing the file
/etc/repmgr/repmgr.conf::

  cluster=my_cluster
  node=1
  node_name=earth
  conninfo='host=192.168.1.10 dbname=repmgr user=repmgr'
  master_response_timeout=60
  reconnect_attempts=6
  reconnect_interval=10
  failover=automatic
  promote_command='promote_command.sh'
  follow_command='repmgr standby follow -f /etc/repmgr/repmgr.conf'

**cluster**
    is the name of the current replication.
**node**
    is the number of the current node (1, 2 or 3 in the current example).
**node_name**
    is an identifier for every node.
**conninfo**
    is used to connect to the local PostgreSQL server (where the configuration file is) from any node. In the witness server configuration it is needed to add a 'port=5499' to the conninfo.
**master_response_timeout**
    is the maximum amount of time we are going to wait before deciding the master has died and start failover procedure.
**reconnect_attempts**
    is the number of times we will try to reconnect to master after a failure has been detected and before start failover procedure.
**reconnect_interval**
    is the amount of time between retries to reconnect to master after a failure has been detected and before start failover procedure.
**failover**
    configure behavior: *manual* or *automatic*.
**promote_command**
    the command executed to do the failover (including the PostgreSQL failover itself). The command must return 0 on success.
**follow_command**
    the command executed to address the current standby to another Master. The command must return 0 on success.

Register Master and Standby
---------------------------

Log in node1.

Register the node as Master::

  su - postgres
  repmgr -f /etc/repmgr/repmgr.conf master register

Log in node2. Register it as a standby::

  su - postgres
  repmgr -f /etc/repmgr/repmgr.conf standby register

Initialize witness server
-------------------------

Log in witness.

Initialize the witness server::

  su - postgres
  repmgr -d repmgr -U repmgr -h 192.168.1.10 -D $WITNESS_PGDATA -f /etc/repmgr/repmgr.conf witness create

It needs information to connect to the master to copy the configuration of the cluster, also it needs to know where it should initialize it's own $PGDATA.
As part of the procees it also ask for the superuser password so it can connect when needed.

Start the repmgrd daemons
-------------------------

Log in node2 and witness.

	su - postgres
	repmgrd -f /etc/repmgr/repmgr.conf --daemonize -> /var/log/postgresql/repmgr.log 2>&1

**Note:** The Master does not need a repmgrd daemon.


Suspend Automatic behavior
==========================

Edit the repmgr.conf of the node to remove from automatic processing and change::

	failover=manual

Then, signal repmgrd daemon::

	su - postgres
	kill -HUP `pidof repmgrd`

Usage
=====

The repmgr documentation is in the README file (how to build, options, etc.)
