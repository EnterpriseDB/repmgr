Packaging
=========

Notes on RedHat Linux, Fedora, and CentOS Builds
------------------------------------------------

The RPM packages of PostgreSQL put `pg_config` into the `postgresql-devel`
package, not the main server one.  And if you have a RPM install of PostgreSQL
9.0, the entire PostgreSQL binary directory will not be in your PATH by default
either.  Individual utilities are made available via the `alternatives`
mechanism, but not all commands will be wrapped that way.  The files installed
by repmgr will certainly not be in the default PATH for the postgres user
on such a system.  They will instead be in /usr/pgsql-9.0/bin/ on this
type of system.

When building repmgr against a RPM packaged build, you may discover that some
development packages are needed as well.  The following build errors can
occur:

    /usr/bin/ld: cannot find -lxslt
    /usr/bin/ld: cannot find -lpam

Install the following packages to correct those:


    yum install libxslt-devel
    yum install pam-devel

If building repmgr as a regular user, then doing the install into the system
directories using sudo, the syntax is hard.  `pg_config` won't be in root's
path either.  The following recipe should work:

    sudo PATH="/usr/pgsql-9.0/bin:$PATH" make USE_PGXS=1 install


Issues with 32 and 64 bit RPMs
------------------------------

If when building, you receive a series of errors of this form:

  /usr/bin/ld: skipping incompatible /usr/pgsql-9.0/lib/libpq.so when searching for -lpq

This is likely because you have both the 32 and 64 bit versions of the
`postgresql90-devel` package installed.  You can check that like this:

    rpm -qa --queryformat '%{NAME}\t%{ARCH}\n'  | grep postgresql90-devel

And if two packages appear, one for i386 and one for x86_64, that's not supposed
to be allowed.

This can happen when using the PGDG repo to install that package;
here is an example sessions demonstrating the problem case appearing:


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
correct one for your architecture.  Example:

    rpm -e postgresql90-devel --allmatches
    yum install postgresql90-devel-9.0.2-2PGDG.rhel5.x86_64

Instead just deleting the package from the wrong platform might not leave behind
the correct files, due to the way in which these accidentally happen to interact.
If you already tried to build repmgr before doing this, you'll need to do:

    make USE_PGXS=1 clean

to get rid of leftover files from the wrong architecture.

Notes on Ubuntu, Debian or other Debian-based Builds
----------------------------------------------------

The Debian packages of PostgreSQL put `pg_config` into the development package
called `postgresql-server-dev-$version`.

When building repmgr against a Debian packages build, you may discover that some
development packages are needed as well. You will need the following development
packages installed:

    sudo apt-get install libxslt-dev libxml2-dev libpam-dev libedit-dev

If you're using Debian packages for PostgreSQL and are building repmgr with the
USE_PGXS option you also need to install the corresponding development package:

    sudo apt-get install postgresql-server-dev-9.0

If you build and install repmgr manually it will not be on the system path. The
binaries will be installed in /usr/lib/postgresql/$version/bin/ which is not on
the default path. The reason behind this is that Ubuntu/Debian systems manage
multiple installed versions of PostgreSQL on the same system through a wrapper
called pg_wrapper and repmgr is not (yet) known to this wrapper.

You can solve this in many different ways, the most Debian like is to make an
alternate for repmgr and repmgrd:

    sudo update-alternatives --install /usr/bin/repmgr repmgr /usr/lib/postgresql/9.0/bin/repmgr 10
    sudo update-alternatives --install /usr/bin/repmgrd repmgrd /usr/lib/postgresql/9.0/bin/repmgrd 10

You can also make a deb package of repmgr using:

    make USE_PGXS=1 deb

This will build a Debian package one level up from where you build, normally the
same directory that you have your repmgr/ directory in.
