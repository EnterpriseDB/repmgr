Summary: PostgreSQL 9 replication setup, management and monitoring.
Name: repmgr
Version: 1.2.0
Release: 1
Group: Applications/Database
#Source: https://github.com/sifusam/repmgr
URL: http://www.repmgr.org/
Packager: Jesse Gonzalez <jesse.gonzalez@rackspace.com>
BuildRequires: postgresql91, postgresql91-devel, libxslt-devel, pam-devel, readline-devel, openssl-devel
Requires: postgresql91, rsync
License: GPLv3
Source: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-build

%description
repmgr is a set of open source tools that helps DBAs and System Administrators manage a cluster of PostgreSQL databases.

By taking advantage of the Hot Standby capability introduced in PostgreSQL 9, repmgr greatly simplifies the process of setting up and managing database with high availability and scalability requirements.

repmgr simplifies administration and daily management, enhances productivity and reduces the overall costs of a PostgreSQL cluster by:

* monitoring the replication process
* allowing DBAs to issue high availability operations such as switch-overs and fail-overs

%prep
echo Building %{name}-%{version}-%{release}
%setup -q -n %{name}

%build
PATH=/usr/pgsql-9.1/bin:${PATH} make USE_PGXS=1

%install
mkdir -p %buildroot/usr/pgsql-9.1/bin
PATH=/usr/pgsql-9.1/bin:${PATH} make DESTDIR=%buildroot USE_PGXS=1 install

%clean
[ ${RPM_BUILD_ROOT} != "/" ] && rm -rf ${RPM_BUILD_ROOT}

%files
%defattr(-,root,root)

# the binary files
/usr/pgsql-9.1/bin/repmgr
/usr/pgsql-9.1/bin/repmgrd

#supporting files
/usr/pgsql-9.1/share/contrib/repmgr.sql
/usr/pgsql-9.1/share/contrib/uninstall_repmgr.sql

%changelog
* Thu Jun 28 2102 Jesse Gonzalez <jesse.gonzalez at rackspace.com> - 1.2.0-1
- Initial RPM packaging.
