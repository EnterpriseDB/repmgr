===================================================
repmgr: Replication Manager for PostgreSQL clusters
===================================================

Introduction
============

PostgreSQL 9+ allow us to have replicated Hot Standby servers
which we can query and/or use for high availability.

While the main components of the feature are included with
PostgreSQL, the user is expected to manage the high availability
part of it.

repmgr allows you to monitor and manage your replicated PostgreSQL
databases as a single cluster.  repmgr includes two components:

* repmgr: command program that performs tasks and then exits

* repmgrd: management and monitoring daemon that watches the cluster
  and can automate remote actions.

Supported Releases
------------------

repmgr works with PostgreSQL versions 9.0 and 9.1.

There are currently no incompatibilities when upgrading repmgr from 9.0 to 9.1,
so your 9.0 configuration will work with 9.1

Additional parameters must be added to postgresql.conf to take advantage of
the new 9.1 features such as synchronous replication or hot standby feedback.

Requirements
------------

repmgr is currently aimed for installation on UNIX-like systems that include
development tools such as ``gcc`` and ``gmake``.  It also requires that the
``rsync`` utility is available in the PATH of the user running the repmgr
programs.  Some operations also require PostgreSQL components such
as ``pg_config`` and ``pg_ctl`` be in the PATH.

Introduction to repmgr commands
===============================

Suppose we have 3 nodes: node1 (the initial master), node2 and node3.
To make node2 and node3 be standbys of node1, execute this on both nodes
(node2 and node3)::

  repmgr -D /var/lib/pgsql/9.0 standby clone node1

In order to get full monitoring and easier state transitions,
you register each of the nodes, by creating a ``repmgr.conf`` file
and executing commands like this on the appropriate nodes::

  repmgr -f /var/lib/pgsql/repmgr/repmgr.conf --verbose master register
  repmgr -f /var/lib/pgsql/repmgr/repmgr.conf --verbose standby register

Once everything is registered, you start the repmgrd daemon.  It
will maintain a view showing the state of all the nodes in the cluster,
including how far they are lagging behind the master.

If you lose node1 you can then run this on node2::

  repmgr -f /var/lib/pgsql/repmgr/repmgr.conf standby promote 

To make node2 the new master.  Then on node3 run::

  repmgr -f /var/lib/pgsql/repmgr/repmgr.conf standby follow

To make node3 follow node2 (rather than node1).

If now we want to add a new node, we can a prepare a new server (node4)
and run::

  repmgr -D /var/lib/pgsql/9.0 standby clone node2
  
And if a previously failed node becomes available again, such as
the lost node1 above, you can get it to resynchronize by only copying
over changes made while it was down using.  That hapens with what's
called a forced clone, which overwrites existing data rather than
assuming it starts with an empty database directory tree::

  repmgr -D /var/lib/pgsql/9.0 --force standby clone node1

This can be much faster than creating a brand new node that must
copy over every file in the database.

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

Issues with 32 and 64 bit RPMs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If when building, you receive a series of errors of this form::

  /usr/bin/ld: skipping incompatible /usr/pgsql-9.0/lib/libpq.so when searching for -lpq

This is likely because you have both the 32 and 64 bit versions of the
``postgresql90-devel`` package installed.  You can check that like this::

  rpm -qa --queryformat '%{NAME}\t%{ARCH}\n'  | grep postgresql90-devel

And if two packages appear, one for i386 and one for x86_64, that's not supposed
to be allowed.

This can happen when using the PGDG repo to install that package;
here is an example sessions demonstrating the problem case appearing::

  # yum install postgresql-devel
  ..
  Setting up Install Process
  Resolving Dependencies
  --> Running transaction check
  ---> Package postgresql90-devel.i386 0:9.0.2-2PGDG.rhel5 set to be updated
  ---> Package postgresql90-devel.x86_64 0:9.0.2-2PGDG.rhel5 set to be updated
  --> Finished Dependency Resolution
  
  Dependencies Resolved

  =========================================================================
   Package               Arch      Version              Repository    Size
  =========================================================================
  Installing:
   postgresql90-devel    i386      9.0.2-2PGDG.rhel5    pgdg90        1.5 M
   postgresql90-devel    x86_64    9.0.2-2PGDG.rhel5    pgdg90        1.6 M

Note how both the i386 and x86_64 platform architectures are selected for
installation.  Your main PostgreSQL package will only be compatible with one of
those, and if the repmgr build finds the wrong postgresql90-devel these
"skipping incompatible" messages appear.

In this case, you can temporarily remove both packages, then just install the
correct one for your architecture.  Example::

  rpm -e postgresql90-devel --allmatches
  yum install postgresql90-devel-9.0.2-2PGDG.rhel5.x86_64

Instead just deleting the package from the wrong platform might not leave behind
the correct files, due to the way in which these accidentally happen to interact.
If you already tried to build repmgr before doing this, you'll need to do::

    make USE_PGXS=1 clean

To get rid of leftover files from the wrong architecture.

Notes on Ubuntu, Debian or other Debian-based Builds
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Debian packages of PostgreSQL put ``pg_config`` into the development package
called ``postgresql-server-dev-$version``.

When building repmgr against a Debian packages build, you may discover that some
development packages are needed as well. You will need the following development
packages installed::

  sudo apt-get install libxslt-dev libxml2-dev libpam-dev libedit-dev

If your using Debian packages for PostgreSQL and are building repmgr with the
USE_PGXS option you also need to install the corresponding development package::

  sudo apt-get install postgresql-server-dev-9.0

If you build and install repmgr manually it will not be on the system path. The
binaries will be installed in /usr/lib/postgresql/$version/bin/ which is not on
the default path. The reason behind this is that Ubuntu/Debian systems manage
multiple installed versions of PostgreSQL on the same system through a wrapper
called pg_wrapper and repmgr is not (yet) known to this wrapper.

You can solve this in many different ways, the most Debian like is to make an
alternate for repmgr and repmgrd::

  sudo update-alternatives --install /usr/bin/repmgr repmgr /usr/lib/postgresql/9.0/bin/repmgr 10
  sudo update-alternatives --install /usr/bin/repmgrd repmgrd /usr/lib/postgresql/9.0/bin/repmgrd 10

You can also make a deb package of repmgr using::

  make USE_PGXS=1 deb

This will build a Debian package one level up from where you build, normally the 
same directory that you have your repmgr/ directory in.

Confirm software was built correctly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You should now find the repmgr programs available in the subdirectory where
the rest of your PostgreSQL installation is at.  You can confirm the software
is available by checking its version::

  repmgr --version
  repmgrd --version

You may need to include the full path of the binary instead, such as this
RHEL example::

  /usr/pgsql-9.0/bin/repmgr --version
  /usr/pgsql-9.0/bin/repmgrd --version

Or in this Debian example::

  /usr/lib/postgresql/9.0/bin/repmgr --version
  /usr/lib/postgresql/9.0/bin/repmgrd --version

Below this binary installation base directory is referred to as PGDIR.

Set up trusted copy between postgres accounts
---------------------------------------------

Initial copy between nodes uses the rsync program running over ssh.  For this 
to work, the postgres accounts on each system need to be able to access files 
on their partner node without a password.

First generate a ssh key, using an empty passphrase, and copy the resulting 
keys and a maching authorization file to a privledged user on the other system::

  [postgres@node1]$ ssh-keygen -t rsa
  Generating public/private rsa key pair.
  Enter file in which to save the key (/var/lib/pgsql/.ssh/id_rsa): 
  Enter passphrase (empty for no passphrase): 
  Enter same passphrase again: 
  Your identification has been saved in /var/lib/pgsql/.ssh/id_rsa.
  Your public key has been saved in /var/lib/pgsql/.ssh/id_rsa.pub.
  The key fingerprint is:
  aa:bb:cc:dd:ee:ff:aa:11:22:33:44:55:66:77:88:99 postgres@db1.domain.com
  [postgres@node1]$ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  [postgres@node1]$ chmod go-rwx ~/.ssh/*
  [postgres@node1]$ cd ~/.ssh
  [postgres@node1]$ scp id_rsa.pub id_rsa authorized_keys postgres@node2:

Login as a user on the other system, and install the files into the postgres 
user's account::

  [user@node2 ~]$ sudo chown postgres.postgres authorized_keys id_rsa.pub id_rsa
  [user@node2 ~]$ sudo mkdir -p ~postgres/.ssh
  [user@node2 ~]$ sudo chown postgres.postgres ~postgres/.ssh
  [user@node2 ~]$ sudo mv authorized_keys id_rsa.pub id_rsa ~postgres/.ssh
  [user@node2 ~]$ sudo chmod -R go-rwx ~postgres/.ssh

Now test that ssh in both directions works.  You may have to accept some new 
known hosts in the process.

Primary server configuration
----------------------------

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

Don't forget to restart the database server after making all these changes.

Usage walkthrough
=================

This assumes you've already followed the steps in "Installation Outline" to
install repmgr and repmgrd on the system.

A normal production installation of ``repmgr`` will normally involve two
different systems running on the same port, typically the default of 5432, 
with both using files owned by the ``postgres`` user account.  This
walkthrough assumes the following setup:

* A primary (master) server called "node1," running as the "postgres" user 
  who is also the owner of the files. This server is operating on port 5432.  This
  server will be known as "node1" in the cluster "test".

* A secondary (standby) server called "node2," running as the "postgres" user 
  who is also the owner of the files. This server is operating on port 5432.  This
  server will be known as "node2" in the cluster "test".

* Another standby server called "node3" with a similar configuration to "node2".

* The Postgress installation in each of the above is defined as $PGDATA, 
  which is represented here as ``/var/lib/pgsql/9.0/data``
  
Creating some sample data
-------------------------

If you already have a database with useful data to replicate, you can
skip this step and use it instead.  But if you do not already have
data in this cluster to replication, you can create some like this::

    createdb pgbench
    pgbench -i -s 10 pgbench
	
Examples below will use the database name ``pgbench`` to match this.
Substitute the name of your database instead.  Note that the standby
nodes created here will include information for every database in the
cluster, not just the specified one.  Needing the database name is
mainly for user authentication purposes.

Setting up a repmgr user
------------------------

Make sure that the "standby" user has a role in the database, "pgbench" in this
case, and can login.   On "node1"::

  createuser --login --superuser repmgr

Alternately you could start ``psql`` on the pgbench database on "node1" and at
the node1b# prompt type::

  CREATE ROLE repmgr SUPERUSER LOGIN;

The main advantage of the latter is that you can do it remotely to any
system you already have superuser access to.

Clearing the PostgreSQL installation on the Standby
---------------------------------------------------

To setup a new streaming replica, startin by removing any PostgreSQL
installation on the existing standby nodes.

* Stop any server on "node2" and "node3".  You can confirm the database
  servers running using a command like this::
  
    ps -eaf | grep postgres
	
  And looking for the various database server processes:  server, logger,
  wal writer, and autovacuum launcher.
  
* Go to "node2" and "node3" database directories and remove the PostgreSQL installation::

    cd $PGDATA
    rm -rf *

  This will delete the entire database installation in ``/var/lib/pgsql/9.0/data``.
  Be careful that $PGDATA is defined here; executing ``ls`` to confirm you're
  in the right place is always a good idea before executing ``rm``.

Testing remote access to the master
-----------------------------------

On the "node2" server, first test that you can connect to "node1" the
way repmgr will by executing::

  psql -h node1 -U repmgr -d pgbench

Possible sources for a problem here include:

* Login role specified was not created on "node1"

* The database configuration on "node1" is not listening on a TCP/IP port.
  That could be because the ``listen_addresses`` parameter was not updated,
  or if it was but the server wasn't restarted afterwards.  You can
  test this on "node1" itself the same way::

    psql -h node1 -U repmgr -d pgbench

  With the "-h" parameter forcing a connnection over TCP/IP, rather
  than the default UNIX socket method.

* There is a firewall setup that prevents incoming access to the
  PostgreSQL port (defaulting to 5432) used to access "node1".  In
  this situation you would be able to connect to the "node1" server
  on itself, but not from any other host, and you'd just get a timeout
  when trying rather than a proper error message.
	 
* The ``pg_hba.conf`` file does not list appropriate statements to allow
  this user to login.  In this case you should connect to the server,
  but see an error message mentioning the ``pg_hba.conf``.

Cloning the standby
-------------------

With "node1" server running, we want to use the ``clone standby`` command
in repmgr to copy over the entire PostgreSQL database cluster onto the
"node2" server.  Execute the clone process with::

  repmgr -D $PGDATA -d pgbench -p 5432 -U repmgr -R postgres --verbose standby clone node1

Here "-U" specifies the database user to connect to the master as, while
"-R" specifies what user to run the rsync command as.  Potentially you
could leave out one or both of these, in situations where the user and/or
role setup is the same on each node.

If this fails with an error message about accessing the master database,
you should return to the previous step and confirm access to "node1"
from "node2" with ``psql``, using the same parameters given to repmgr.

NOTE: you need to have $PGDIR/bin (where the PostgreSQL binaries are installed)
in your path for the above to work.  If you don't want that as a permanent
setting, you can temporarily set it before running individual commands like
this::

  PATH=$PGDIR/bin:$PATH repmgr -D $PGDATA ...

Setup repmgr configuration file
-------------------------------

Create a directory to store each repmgr configuration in for each node.
In that, there needs to be a ``repmgr.conf`` file for each node in the cluster.
For each node we'll assume this is stored in ``/var/lib/pgsql/repmgr/repmgr.conf``
following the standard directory structure of a RHEL system.  It should contain::

  cluster=test
  node=1
  conninfo='host=node1 user=repmgr dbname=pgbench'

On "node2" create the file ``/var/lib/pgsql/repmgr/repmgr.conf`` with::

  cluster=test
  node=2
  conninfo='host=node2 user=repmgr dbname=pgbench'

The STANDBY CLONE process should have created a recovery.conf file on
"node2" in the $PGDATA directory that reads as follows::

  standby_mode = 'on'
  primary_conninfo = 'host=node1 port=5432'

Registering the master and standby
----------------------------------

First, register the master by typing on "node1"::

  repmgr -f /var/lib/pgsql/repmgr/repmgr.conf --verbose master register

Then start the "standby" server.

You could now register the standby by typing on "node2"::

  repmgr -f /var/lib/pgsql/repmgr/repmgr.conf --verbose standby register

However, you can instead start repmgrd::

  repmgrd -f /var/lib/pgsql/repmgr/repmgr.conf --verbose > /var/lib/pgsql/repmgr/repmgr.log 2>&1

Which will automatically register your standby system.  And eventually
you need repmgrd running anyway, to save lag monitoring information.
repmgrd will log the deamon activity to the listed file.  You can
watch what it is doing with::

  tail -f /var/lib/pgsql/repmgr/repmgr.log

Hit control-C to exit this tail command when you are done.

Monitoring and testing
----------------------

At this point, you have a functioning primary on "node1" and a functioning
standby server running on "node2".  You can confirm the master knows
about the standby, and that it is keeping it current, by looking at
``repl_status``::

	postgres@node2 $ psql -x -d pgbench -c "SELECT * FROM repmgr_test.repl_status"
	-[ RECORD 1 ]-------------+------------------------------
	primary_node              | 1
	standby_node              | 2
	last_monitor_time         | 2011-02-23 08:19:39.791974-05
	last_wal_primary_location | 0/1902D5E0
	last_wal_standby_location | 0/1902D5E0
	replication_lag           | 0 bytes
	apply_lag                 | 0 bytes
	time_lag                  | 00:26:13.30293

Some tests you might do at this point include:

* Insert some records into the primary server here, confirm they appear
  very quickly (within milliseconds) on the standby, and that the
  repl_status view advances accordingly.

* Verify that you can run queries against the standby server, but
  cannot make insertions into the standby database.  

Simulating the failure of the primary server
--------------------------------------------

To simulate the loss of the primary server, simply stop the "node1" server.
At this point, the standby contains the database as it existed at the time of
the "failure" of the primary server.  If looking at ``repl_status`` on
"node2", you should see the time_lag value increase the longer "node1" 
is down.

Promoting the Standby to be the Primary
---------------------------------------

Now you can promote the standby server to be the primary, to allow
applications to read and write to the database again, by typing::

  repmgr -f /var/lib/pgsql/repmgr/repmgr.conf --verbose standby promote

The server restarts and now has read/write ability.

Bringing the former Primary up as a Standby
-------------------------------------------

To make the former primary act as a standby, which is necessary before
restoring the original roles, type the following on node1::

  repmgr -D $PGDATA -d pgbench -p 5432 -U repmgr -R postgres --verbose --force standby clone node2

Then start the "node1" server, which is now acting as a standby server.
Check 

Make sure the record(s) inserted the earlier step are still available on the
now standby (prime).  Confirm the database on "node1" is read-only.

Restoring the original roles of prime to primary and standby to standby
-----------------------------------------------------------------------

Now restore to the original configuration by stopping
"node2" (now acting as a primary), promoting "node1" again to be the
primary server, then bringing up "node2" as a standby with a valid
``recovery.conf`` file.

Stop the "node2" server::

  repmgr -f /var/lib/pgsql/repmgr/repmgr.conf standby promote

Now the original primary, "node1" is acting again as primary.

Start the "node2" server and type this on "node1"::

  repmgr standby clone --force -h node2 -p 5432 -U postgres -R postgres --verbose

Verify the roles have reversed by attempting to insert a record on "node"
and on "node1".

The servers are now again acting as primary on "node1" and standby on "node2".

Alternate setup:  both servers on one host
==========================================

Another test setup assumes you might be using the default installation of
PostgreSQL on port 5432 for some other purpose, and instead relocates these
instances onto different ports running as different users.  In places where
``127.0.0.1`` is used as a host name, a more traditional configuration
would instead use the name of the relevant host for that parameter. 
You can usually leave out changes to the port number in this case too.

* A primary (master) server called "prime," with a user as "prime," who is
  also the owner of the files. This server is operating on port 5433.  This
  server will be known as "node1" in the cluster "test"

* A standby server called "standby", with a user of "standby", who is the
  owner of the files.  This server is operating on port 5434.  This server
  will be known and "node2" on the cluster "test."

* A database exists on "prime" called "testdb."

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

* Go to "standby" database directory and remove the PostgreSQL installation::

    cd $PGDATA
    rm -rf *

  This will delete the entire database installation in ``/data/standby``.

Building the standby
--------------------

Create a directory to store each repmgr configuration in for each node.
In that, there needs to be a ``repmgr.conf`` file for each node in the cluster.
For "prime" we'll assume this is stored in ``/home/prime/repmgr``
and it should contain::

  cluster=test
  node=1
  conninfo='host=127.0.0.1 dbname=testdb'

On "standby" create the file ``/home/standby/repmgr/repmgr.conf`` with::

  cluster=test
  node=2
  conninfo='host=127.0.0.1 dbname=testdb'

Next, with "prime" server running, we want to use the ``clone standby`` command
in repmgr to copy over the entire PostgreSQL database cluster onto the
"standby" server.  On the "standby" server, type::

  repmgr -D $PGDATA -p 5433 -U prime -R prime --verbose standby clone localhost

Next, we need a recovery.conf file on "standby" in the $PGDATA directory
that reads as follows::

  standby_mode = 'on'
  primary_conninfo = 'host=127.0.0.1 port=5433'

Make sure that standby has a qualifying role in the database, "testdb" in this
case, and can login. Start ``psql`` on the testdb database on "prime" and at
the testdb# prompt type::

  CREATE ROLE standby SUPERUSER LOGIN

Registering the master and standby
----------------------------------

First, register the master by typing on "prime"::

  repmgr -f /home/prime/repmgr/repmgr.conf --verbose master register

On "standby," edit the ``postgresql.conf`` file and change the port to 5434.

Start the "standby" server.

Register the standby by typing on "standby"::

  repmgr -f /home/standby/repmgr/repmgr.conf --verbose standby register

At this point, you have a functioning primary on "prime" and a functioning
standby server running on "standby."  You can confirm the master knows
about the standby, and that it is keeping it current, by running the
following on the master::

  psql -x -d pgbench -c "SELECT * FROM repmgr_test.repl_status"

Some tests you might do at this point include:

* Insert some records into the primary server here, confirm they appear
  very quickly (within milliseconds) on the standby, and that the
  repl_status view advances accordingly.

* Verify that you can run queries against the standby server, but
  cannot make insertions into the standby database.  

Simulating the failure of the primary server
--------------------------------------------

To simulate the loss of the primary server, simply stop the "prime" server.
At this point, the standby contains the database as it existed at the time of
the "failure" of the primary server.

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

  repmgr -U standby -R prime -h 127.0.0.1 -p 5433 -d testdb --force --verbose standby clone

Stop and restart the "prime" server, which is now acting as a standby server.

Make sure the record(s) inserted the earlier step are still available on the
now standby (prime).  Confirm the database on "prime" is read-only.

Restoring the original roles of prime to primary and standby to standby
-----------------------------------------------------------------------

Now restore to the original configuration by stopping the
"standby" (now acting as a primary), promoting "prime" again to be the
primary server, then bringing up "standby" as a standby with a valid
``recovery.conf`` file on "standby".

Stop the "standby" server::

  repmgr -f /home/prime/repmgr/repmgr.conf standby promote

Now the original primary, "prime" is acting again as primary.

Start the "standby" server and type this on "prime"::

  repmgr standby clone --force -h 127.0.0.1 -p 5434 -U prime -R standby --verbose

Stop the "standby" and change the port to be 5434 in the ``postgresql.conf``
file.

Verify the roles have reversed by attempting to insert a record on "standby"
and on "prime."

The servers are now again acting as primary on "prime" and standby on "standby".

Configuration and command reference
===================================

Configuration File
------------------

``repmgr.conf`` is looked for in the directory repmgrd or repmgr exists in.
The configuration file should have 3 lines:

1. cluster: A string (single quoted) that identify the cluster we are on 

2. node: An integer that identify our node in the cluster

3. conninfo: A string (single quoted) specifying how we can connect to this node's PostgreSQL service

repmgr
------

Command line syntax
~~~~~~~~~~~~~~~~~~~

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
    -I, --ignore-rsync-warning ignore rsync partial transfer warning
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

repmgr commands
---------------

Not all of these commands need the ``repmgr.conf`` file, but they need to be able to
connect to the remote and local databases.

You can teach it which is the remote database by using the -h parameter or 
as a last parameter in standby clone and standby follow. If you need to specify
a port different then the default 5432 you can specify a -p parameter.
Standby is always considered as localhost and a second -p parameter will indicate
its port if is different from the default one.

* master register

  * Registers a master in a cluster, it needs to be executed before any
    standby nodes are registered

* standby register

  * Registers a standby in a cluster, it needs to be executed before
    repmgrd will function on the node.

* standby clone [node to be cloned] 

  * Does a backup via ``rsync`` of the data directory of the primary. And it 
    creates the recovery file we need to start a new hot standby server.
    It doesn't need the ``repmgr.conf`` so it can be executed anywhere on the
    new node.  You can change to the directory you want the new database
    cluster at and execute::

      ./repmgr standby clone node1

    or run from wherever you are with a full path::

     ./repmgr -D /path/to/new/data/directory standby clone node1

    That will make a backup of the primary then you only need to start the server
    using a command like::

      pg_ctl -D /your_data_directory_path start

    Note that some installations will also redirect the output log file when
    executing ``pg_ctl``; check the server startup script you are using
    and try to match what it does.

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

repmgrd Daemon
--------------

Command line syntax
~~~~~~~~~~~~~~~~~~~

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

Usage
-----

repmgrd reads the ``repmgr.conf`` file in current directory, or as
indicated with -f parameter.  If run on a standby, it checks if that
standby is in ``repl_nodes`` and adds it if not.

Before you can run repmgrd you need to register a master in a cluster
using the ``MASTER REGISTER`` command.  If run on a master,
repmgrd will exit, as it has nothing to do on them yet.  It is only
targeted at running on standby servers currently.  If converting
a former master into a standby, you will need to start repmgrd
in order to make it fully operational in its new role.

The repmgr daemon creates 2 connections: one to the master and another to the
standby.

Lag monitoring
--------------

repmgrd helps monitor a set of master and standby servers.  You can
see which node is the current master, as well as how far behind each
is from current.

To look at the current lag between primary and each node listed
in ``repl_node``, consult the ``repl_status`` view::

  psql -d postgres -c "SELECT * FROM repmgr_test.repl_status"

This view shows the latest monitor info from every node.
 
* replication_lag: in bytes.  This is how far the latest xlog record 
  we have received is from master.

* apply_lag: in bytes.  This is how far the latest xlog record
  we have applied is from the latest record we have received.

* time_lag: in seconds.  How many seconds behind the master is this node.

Error codes
-----------

When the repmgr or repmgrd program exits, it will set one of the
following 

* SUCCESS 0:  Program ran successfully.
* ERR_BAD_CONFIG 1:  One of the configuration checks the program makes failed.
* ERR_BAD_RSYNC 2:  An rsync call made by the program returned an error.
* ERR_STOP_BACKUP 3:  A ``pg_stop_backup()`` call made by the program didn't succeed.
* ERR_NO_RESTART 4:  An attempt to restart a PostgreSQL instance failed.
* ERR_NEEDS_XLOG 5:  Could note create the ``pg_xlog`` directory when cloning.
* ERR_DB_CON 6:  Error when trying to connect to a database.
* ERR_DB_QUERY 7:  Error executing a database query.
* ERR_PROMOTED 8:  Exiting program because the node has been promoted to master.
* ERR_BAD_PASSWORD 9:  Password used to connect to a database was rejected.

License and Contributions
=========================

repmgr is licensed under the GPL v3.  All of its code and documentation is
Copyright 2010-2011, 2ndQuadrant Limited.  See the files COPYRIGHT and LICENSE for
details.

Main sponsorship of repmgr has been from 2ndQuadrant customers.

Additional work has been sponsored by the 4CaaST project for cloud computing,
which has received funding from the European Union's Seventh Framework Programme
(FP7/2007-2013) under grant agreement 258862.

Contributions to repmgr are welcome, and will be listed in the file CREDITS.
2ndQuadrant Limited requires that any contributions provide a copyright
assignment and a disclaimer of any work-for-hire ownership claims from the
employer of the developer.  This lets us make sure that all of the repmgr
distribution remains free code.  Please contact info@2ndQuadrant.com for a
copy of the relevant Copyright Assignment Form.

Code style
----------

Code in repmgr is formatted to a consistent style using the following command::

  astyle --style=ansi --indent=tab --suffix=none *.c *.h

Contributors should reformat their code similarly before submitting code to
the project, in order to minimize merge conflicts with other work.

Support and Assistance
======================

2ndQuadrant provides 24x7 production support for repmgr, as well as help you
configure it correctly, verify an installation and train you in running a
robust replication cluster.

There is a mailing list/forum to discuss contributions or issues
http://groups.google.com/group/repmgr

#repmgr is registered in freenode IRC

Further information is available at http://www.repmgr.org/

We'd love to hear from you about how you use repmgr. Case studies and
news are always welcome. Send us an email at info@2ndQuadrant.com, or
send a postcard to

repmgr
c/o 2ndQuadrant
7200 The Quorum
Oxford Business Park North
Oxford
OX4 2JZ

Thanks from the repmgr core team

Jaime Casanova
Simon Riggs
Greg Smith
Cedric Villemain

