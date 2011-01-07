===================================================
repmgr: Replication Manager for PostgreSQL clusters
===================================================

Introduction
============

PostgreSQL 9.0 allow us to have replicated Hot Standby servers 
which we can query and/or use for high availability.

While the main components of the feature are included with
PostgreSQL, the user is expected to manage the high availability
part of it.

repmgr allows you to monitor and manage your replicated PostgreSQL
databases as a single cluster.

repmgr includes two components:

* repmgr: command program that performs tasks and then exits
* repmgrd: management and monitoring daemon that watches the cluster

Requirements
------------

repmgr is currently aimed for installation on UNIX-like systems that include
development tools such as gcc and gmake.  It also requires that the
``rsync`` utility is available in the PATH of the user running the repmgr
programs.

Installation Outline
====================

To install and use repmgr and repmgrd follow these steps:

1. Build repmgr programs 

2. Set up trusted copy between postgres accounts, needed for the
   ``STANDBY CLONE`` step

3. Check your primary server is correctly configured

4. Write a suitable ``repmgr.conf`` for the node

5. Setup repmgrd to aid in failover transitions

Build repmgr programs
---------------------

Both methods of installation will place the binaries at the same location as your
postgres binaries, such as ``psql``.  There are two ways to build it.  The second
requires a full PostgreSQL source code tree to install the program directly into.
The first instead uses the PostgreSQL Extension System (PGXS) to install.  For
this method to work, you will need the pg_config program available in your PATH.
In some distributions of PostgreSQL, this requires installing a separate
development package in addition to the basic server software.

Build repmgr programs - PGXS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you are using a packaged PostgreSQL build and have ``pg_config``
available, the package can be built and installed using PGXS instead::

  tar xvzf repmgr-1.0.tar.gz
  cd repmgr
  make USE_PGXS=1
  make USE_PGXS=1 install

This is preferred to building from the ``contrib`` subdirectory of the main
source code tree.

If you need to remove the source code temporary files from this directory,
that can be done like this::

  make USE_PGXS=1 clean
  
See below for building notes specific to RedHat Linux variants.

Using a full source code tree
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this method, the repmgr distribution is copied into the PostgreSQL source
code tree, assumed to be at the ${postgresql_sources} for this example.
The resulting subdirectory must be named ``contrib/repmgr``, without any
version number::

  cp repmgr.tar.gz ${postgresql_sources}/contrib
  cd ${postgresql_sources}/contrib 
  tar xvzf repmgr-1.0.tar.gz
  cd repmgr
  make
  make install

If you need to remove the source code temporary files from this directory,
that can be done like this::

  make clean

Notes on RedHat Linux, Fedora, and CentOS Builds
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The RPM packages of PostgreSQL put ``pg_config`` into the ``postgresql-devel``
package, not the main server one.  And if you have a RPM install of PostgreSQL
9.0, the entire PostgreSQL binary directory will not be in your PATH by default
either.  Individual utilities are made available via the ``alternatives``
mechanism, but not all commands will be wrapped that way.  The files installed
by repmgr will certainly not be in the default PATH for the postgres user
on such a system.  They will instead be in /usr/pgsql-9.0/bin/ on this
type of system.

When building repmgr against a RPM packaged build, you may discover that some
development packages are needed as well.  The following build errors can
occur::

  /usr/bin/ld: cannot find -lxslt
  /usr/bin/ld: cannot find -lpam
  
Install the following packages to correct those::

  yum install libxslt-devel
  yum install pam-devel

If building repmgr as a regular user, then doing the install into the system
directories using sudo, the syntax is hard.  ``pg_config`` won't be in root's
path either.  The following recipe should work::

  sudo PATH="/usr/pgsql-9.0/bin:$PATH" make USE_PGXS=1 install

Notes on Ubuntu, Debian or other Debian-based Builds
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Debian packages of PostgreSQL put ``pg_config`` into the development package
called ``postgresql-server-dev-$version``.

When building repmgr against a Debian packages build, you may discover that some
development packages are needed as well. You will need the following development
packages installed::

  sudo apt-get install libxslt1-dev libxml2-dev libpam-dev libedit-dev

If you build and install repmgr manually it will not be on the system path. The
binaries will be installed in /usr/lib/postgresql/$version/bin/ which is not on
the default path. The reason behind this is that Ubuntu/Debian systems manage
multiple installed versions of PostgreSQL on the same system through a wrapper
called pg_wrapper and repmgr is not (yet) known to this wrapper.

You can solve this in many different ways, the most Debian like is to make an
alternate for repmgr and repmgrd::

  sudo update-alternatives --install /usr/bin/repmgr repmgr /usr/lib/postgresql/9.0/bin/repmgr 10
  sudo update-alternatives --install /usr/bin/repmgrd repmgrd /usr/lib/postgresql/9.0/bin/repmgrd 10

Confirm software was built correctly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You should now find the repmgr programs available in the subdirectory where
the rest of your PostgreSQL installation is at.  You can confirm the software
is available by checking its version::

  repmgr --version
  repmgrd --version
    
You may need to include the full path of the binary instead, such as this RHEL example::

  /usr/pgsql-9.0/bin/repmgr --version
  /usr/pgsql-9.0/bin/repmgrd --version

Below this binary installation base directory is referred to as PGDIR.

Set up trusted copy between postgres accounts
---------------------------------------------

Initial copy between nodes uses the rsync program running over ssh.  For this 
to work, the postgres accounts on each system need to be able to access files 
on their partner node without a password.

First generate a ssh key, using an empty passphrase, and copy the resulting 
keys and a maching authorization file to a privledged user on the other system::

  [postgres@db1]$ ssh-keygen -t rsa
  Generating public/private rsa key pair.
  Enter file in which to save the key (/var/lib/pgsql/.ssh/id_rsa): 
  Enter passphrase (empty for no passphrase): 
  Enter same passphrase again: 
  Your identification has been saved in /var/lib/pgsql/.ssh/id_rsa.
  Your public key has been saved in /var/lib/pgsql/.ssh/id_rsa.pub.
  The key fingerprint is:
  aa:bb:cc:dd:ee:ff:aa:11:22:33:44:55:66:77:88:99 postgres@db1.domain.com
  [postgres@db1]$ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  [postgres@db1]$ chmod go-rwx ~/.ssh/*
  [postgres@db1]$ cd ~/.ssh
  [postgres@db1]$ scp id_rsa.pub id_rsa authorized_keys user@db2:

Login as a user on the other system, and install the files into the postgres 
user's account::

  [user@db2 ~]$ sudo chown postgres.postgres authorized_keys id_rsa.pub id_rsa
  [user@db2 ~]$ sudo mkdir -p ~postgres/.ssh
  [user@db2 ~]$ sudo chown postgres.postgres ~postgres/.ssh
  [user@db2 ~]$ sudo mv authorized_keys id_rsa.pub id_rsa ~postgres/.ssh
  [user@db2 ~]$ sudo chmod -R go-rwx ~postgres/.ssh

Now test that ssh in both directions works.  You may have to accept some new 
known hosts in the process.

Primary server configuration
============================

PostgreSQL should have been previously built and installed on the system.  Here
is a sample of changes to the ``postgresql.conf`` file::

  listen_addresses='*'
  wal_level = 'hot_standby'
  archive_mode = on
  archive_command = 'cd .'	 # we can also use exit 0, anything that 
                             # just does nothing
  max_wal_senders = 10
  wal_keep_segments = 5000     # 80 GB required on pg_xlog
  hot_standby = on

Also you need to add the machines that will participate in the cluster in 
``pg_hba.conf`` file.  One possibility is to trust all connections from the
replication users from all internal addresses, such as::

  host     all              all         192.168.1.0/24         trust
  host     replication      all         192.168.1.0/24         trust

A more secure setup adds a repmgr user and database, just giving
access to that user::

  host     repmgr           repmgr      192.168.1.0/24         trust
  host     replication      all         192.168.1.0/24         trust

If you give a password to the user, you need to create a ``.pgpass`` file for
them as well to allow automatic login.  In this case you might use the
``md5`` authentication method instead of ``trust`` for the repmgr user.

Configuration File
==================

``repmgr.conf`` is looked for in the directory repmgrd or repmgr exists.
The configuration file should have 3 lines:

It should have these three parameters:

1. cluster: A string (single quoted) that identify the cluster we are on 

2. node: An integer that identify our node in the cluster

3. conninfo: A string (single quoted) specifying how we can connect to this node's PostgreSQL service

Command line syntax
===================

The current supported syntax for the program can be seen using::

  repmgr --help
  
The output from this program looks like this::

  repmgr: Replicator manager 
  Usage:
   repmgr [OPTIONS] master  {register}
   repmgr [OPTIONS] standby {register|clone|promote|follow}

  General options:
    --help                     show this help, then exit
    --version                  output version information, then exit
    --verbose                  output verbose activity information

  Connection options:
    -d, --dbname=DBNAME        database to connect to
    -h, --host=HOSTNAME        database server host or socket directory
    -p, --port=PORT            database server port
    -U, --username=USERNAME    database user name to connect as

  Configuration options:
    -D, --data-dir=DIR         local directory where the files will be copied to
    -f, --config_file=PATH     path to the configuration file
    -R, --remote-user=USERNAME database server username for rsync
    -w, --wal-keep-segments=VALUE  minimum value for the GUC wal_keep_segments (default: 5000)
    -F, --force                force potentially dangerous operations to happen

  repmgr performs some tasks like clone a node, promote it or making follow another node and then exits.
  COMMANDS:
   master register       - registers the master in a cluster
   standby register      - registers a standby in a cluster
   standby clone [node]  - allows creation of a new standby
   standby promote       - allows manual promotion of a specific standby into a new master in the event of a failover
   standby follow        - allows the standby to re-point itself to a new master

The ``--verbose`` option can be useful in troubleshooting issues with
the program.

Commands
========

Not all of these commands need the ``repmgr.conf`` file, but they need to be able to
connect to the remote and local databases.

You can teach it which is the remote database by using the -h parameter or 
as a last parameter in standby clone and standby follow. If you need to specify
a port different then the default 5432 you can specify a -p parameter.
Standby is always considered as localhost and a second -p parameter will indicate
its port if is different from the default one.

* master register

  * Registers a master in a cluster, it needs to be executed before any node is 
    registered

* standby register

  * Registers a standby in a cluster, it needs to be executed before any repmgrd 
    is executed

* standby clone [node to be cloned] 

  * Does a backup via ``rsync`` of the data directory of the primary. And it 
    creates the recovery file we need to start a new hot standby server.
    It doesn't need the ``repmgr.conf`` so it can be executed anywhere on the
    new node.  You can change to the directory you want the new database
    cluster at and execute::

      ./repmgr standby clone 10.68.1.161

    or run from wherever you are with a full path::

     ./repmgr -D /path/to/new/data/directory standby clone 10.68.1.161

    That will make a backup of the primary then you only need to start the server
    using a command like::

      pg_ctl -D /your_data_directory_path start

    Note that some installations will also redirect the output log file when
    executing ``pg_ctl``.

* standby promote 

  * Allows manual promotion of a specific standby into a new primary in the
    event of a failover.  This needs to be executed on the same directory
    where the ``repmgr.conf`` is in the standby, or you can use the ``-f`` option
    to indicate where the ``repmgr.conf`` is at.  It doesn't need any
    additional arguments::

      ./repmgr standby promote

    That will restart your standby postgresql service.

* standby follow 

    * Allows the standby to base itself to the new primary passed as a
      parameter.  This needs to be executed on the same directory where the
      ``repmgr.conf`` is in the standby, or you can use the ``-f`` option
      to indicate where the ``repmgr.conf`` is at.  Example::

        ./repmgr standby follow

Examples
========

Suppose we have 3 nodes: node1 (the initial master), node2 and node3

To make node2 and node3 be standbys of node1, execute this on both nodes
(node2 and node3)::

  repmgr -D /var/lib/postgresql/9.0 standby clone node1

If we lose node1 we can run on node2::

  repmgr -f /home/postgres/repmgr.conf standby promote 

Which makes node2 the new master.  We then run on node3::

  repmgr standby follow

To make node3 follow node2 (rather than node1)

If now we want to add a new node we can a prepare a new server (node4)
and run::

  repmgr -D /var/lib/postgresql/9.0 standby clone node2

NOTE: you need to have $PGDIR/bin (where the PostgreSQL binaries are installed)
in your path for the above to work.  If you don't want that as a permanent
setting, you can temporarily set it before running individual commands like
this::

  PATH=$PGDIR/bin:$PATH repmgr standby promote

repmgr Daemon
=============

Command line syntax
-------------------

The current supported syntax for the program can be seen using::

  repmgrd --help
  
The output from this program looks like this::

  repmgrd: Replicator manager daemon 
  Usage:
   repmgrd [OPTIONS]
  
  Options:
    --help                    show this help, then exit
    --version                 output version information, then exit
    --verbose                 output verbose activity information
    -f, --config_file=PATH    database to connect to
  
  repmgrd monitors a cluster of servers.

The ``--verbose`` option can be useful in troubleshooting issues with
the program.

Setup
-----

To use the repmgrd (repmgr daemon) to monitor standby so we know how is going 
the replication and how far they are from primary, you need to execute the 
``repmgr.sql`` script in the postgres database.

You also need to add a row for every node in the ``repl_node`` table.  This work
may be done for you by the daemon itself, as described below.

Lag monitoring
--------------

To look at the current lag between primary and each node listed
in ``repl_node``, consult the ``repl_status`` view::

  psql -d postgres -c "SELECT * FROM repl_status"

This view shows the latest monitor info from every node.
 
* replication_lag: in bytes.  This is how far the latest xlog record 
  we have received is from master.

* apply_lag: in bytes.  This is how far the latest xlog record
  we have applied is from the latest record we have received.

* time_lag: in seconds.  How many seconds behind the master is this node.

Usage
-----

repmgrd reads the ``repmgr.conf`` file in current directory, or as indicated with -f 
parameter.  It checks if the standby is in repl_nodes and adds it if not.

Before you can run the repmgr daemon (repmgrd) you need to register a master
and at least a standby in a cluster using the ``MASTER REGISTER`` and 
``STANDBY REGISTER`` commands.

For example, following last example and assuming that ``repmgr.conf`` is in postgres
home directory you will run this on the master::

  repmgr -f /home/postgres/repmgr.conf master register

and the same in the standby.

The repmgr daemon creates 2 connections: one to the master and another to the
standby.

Detailed walkthrough
====================

This assumes you've already followed the steps in "Installation Outline" to
install repmgr and repmgr on the system.

The following scenario involves two PostgreSQL installations on the same server
hardware, so that additional systems aren't needed for testing.  A normal
production installation of ``repmgr`` will normally involve two different
systems running on the same port, typically the default of 5432, 
with both using files owned by the ``postgres`` user account.  In places where
``127.0.0.1`` is used as a host name below, you would instead use the name of
the relevant host for that parameter.  You can usually leave out changes
to the port number in this case too.

The test setup assumes you might be using the default installation of
PostgreSQL on port 5432 for some other purpose, and instead relocates these
instances onto different ports running as different users:

* A primary (master) server called “prime," with a user as “prime," who is
  also the owner of the files. This server is operating on port 5433.  This
  server will be known as “node1" in the cluster “test"

* A standby server called “standby", with a user of “standby", who is the
  owner of the files.  This server is operating on port 5434.  This server
  will be known and “node2" on the cluster “test."

* A database exists on “prime" called “testdb."

* The Postgress installation in each of the above is defined as $PGDATA, 
  which is represented here with ``/data/prime`` as the "prime" server and 
  ``/data/standby`` as the "standby" server.

You might setup such an installation by adjusting the login script for the
"prime" and "standby" users as in these two examples::

  # prime
  PGDATA=/data/prime
  PGENGINE=/usr/pgsql-9.0/bin
  PGPORT=5433
  export PGDATA PGENGINE PGPORT
  PATH="$PATH:$PGENGINE"

  # standby
  PGDATA=/data/standby
  PGENGINE=/usr/pgsql-9.0/bin
  PGPORT=5434
  export PGDATA PGENGINE PGPORT
  PATH="$PATH:$PGENGINE"

And then starting/stopping each installation as needed using the ``pg_ctl``
utility.

Note:  naming your nodes based on their starting role is not a recommended
best practice!  As you'll see in this example, once there is a failover, names
strongly associated with one particular role (primary or standby) can become
confusing, once that node no longer has that role.  Future versions of this
walkthrough are expected to use more generic terminology for these names.

Clearing the PostgreSQL installation on the Standby
---------------------------------------------------

Setup a streaming replica, strip away any PostgreSQL installation on the existing replica:

* Stop both servers.

* Go to “standby" database directory and remove the PostgreSQL installation::

    cd $PGDATA
    rm -rf *

  This will delete the entire database installation in ``/data/standby``.

Building the standby
--------------------

Create a directory to store each repmgr configuration in for each node.
In that, there needs to be a ``repmgr.conf`` file for each node in the cluster.
For “prime" we'll assume this is stored in ``/home/prime/repmgr``
and it should contain::

  cluster=test
  node=1
  conninfo='host=127.0.0.1 dbname=dbtest'

On “standby" create the file ``/home/standby/repmgr/repmgr.conf`` with::

  cluster=test
  node=2
  conninfo='host=127.0.0.1 dbname=dbtest'

Next, with “prime" server running, we want to use the ``clone standby`` command
in repmgr to copy over the entire PostgreSQL database cluster onto the
“standby" server.  On the “standby" server, type::

  repmgr -D $PGDATA -p 5433 -U prime -R prime --verbose standby clone localhost

Next, we need a recovery.conf file on “standby" in the $PGDATA directory
that reads as follows::

  standby_mode = 'on'
  primary_conninfo = 'host=127.0.0.1 port=5433'

Make sure that standby has a qualifying role in the database, “testdb" in this
case, and can login. Start ``psql`` on the testdb database on “prime" and at
the testdb# prompt type::

  CREATE ROLE standby SUPERUSER LOGIN

Registering the master and standby
----------------------------------

First, register the master by typing on “prime"::

  repmgr -f /home/prime/repmgr/repmgr.conf --verbose master register

On “standby," edit the ``postgresql.conf`` file and change the port to 5434.

Start the “standby" server.

Register the standby by typing on “standby"::

  repmgr -f /home/standby/repmgr/repmgr.conf --verbose standby register

At this point, you have a functioning primary on “prime" and a functioning
standby server running on “standby."  It's recommended that you insert some
records into the primary server here, then confirm they appear very quickly
(within milliseconds) on the standby.  Also verify that one can make queries
against the standby server and cannot make insertions into the standby database.  

Simulating the failure of the primary server
--------------------------------------------

To simulate the loss of the primary server, simply stop the “prime" server.
At this point, the standby contains the database as it existed at the time of
the “failure" of the primary server.

Promoting the Standby to be the Primary
---------------------------------------

Now you can promote the standby server to be the primary, to allow
applications to read and write to the database again, by typing::

  repmgr -f /home/standby/repmgr/repmgr.conf --verbose standby promote

The server restarts and now has read/write ability.

Bringing the former Primary up as a Standby
-------------------------------------------

To make the former primary act as a standby, which is necessary before
restoring the original roles, type::

  repmgr -U standby -R prime -h 127.0.0.1 -p 5433 -d dbtest --force --verbose standby clone

Stop and restart the “prime" server, which is now acting as a standby server.

Make sure the record(s) inserted the earlier step are still available on the
now standby (prime).  Confirm the database on “prime" is read-only.

Restoring the original roles of prime to primary and standby to standby
-----------------------------------------------------------------------

Now restore to the original configuration by stopping the
“standby" (now acting as a primary), promoting “prime" again to be the
primary server, then bringing up “standby" as a standby with a valid
``recovery.conf`` file on “standby".

Stop the “standby" server::

  repmgr -f /home/prime/repmgr/repmgr.conf standby promote

Now the original primary, “prime" is acting again as primary.

Start the “standby" server and type this on “prime"::

  repmgr standby clone --force -h 127.0.0.1 -p 5434 -U prime -R standby --verbose

Stop the “standby" and change the port to be 5434 in the ``postgresql.conf``
file.

Verify the roles have reversed by attempting to insert a record on “standby"
and on “prime."

The servers are now again acting as primary on “prime" and standby on “standby".

License and Contributions
=========================

repmgr is licensed under the GPL v3.  All of its code and documentation is
Copyright 2010, 2ndQuadrant Limited.  See the files COPYRIGHT and LICENSE for
details.

Contributions to repmgr are welcome, and listed in the file CREDITS.
2ndQuadrant Limited requires that any contributions provide a copyright
assignment and a disclaimer of any work-for-hire ownership claims from the
employer of the developer.  This lets us make sure that all of the repmgr
distribution remains free code.  Please contact info@2ndQuadrant.com for a
copy of the relevant Copyright Assignment Form.
