Summary: repmgr
Name: repmgr
Version: 2.0
Release: 2
License: GPLv3
Group: System Environment/Daemons
URL: http://repmgr.org 
Packager: Nathan Van Overloop <nathan.van.overloop@nexperteam.be>
Vendor: 2ndQuadrant Limited
Distribution: centos
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description
repmgr for centos6

%prep
%setup

%build
export PATH=$PATH:/usr/pgsql-9.3/bin/
%{__make} USE_PGXS=1

%install
[ "%{buildroot}" != "/" ] && %{__rm} -rf %{buildroot}

export PATH=$PATH:/usr/pgsql-9.3/bin/
%{__make} USE_PGXS=1 install DESTDIR=%{buildroot} INSTALL="install -p"
%{__make} USE_PGXS=1 install_prog DESTDIR=%{buildroot} INSTALL="install -p"
%{__make} USE_PGXS=1 install_rhel DESTDIR=%{buildroot} INSTALL="install -p"


%clean
[ "%{buildroot}" != "/" ] && %{__rm} -rf %{buildroot}
 

%files
%defattr(-,root,root)
/usr/bin/repmgr
/usr/bin/repmgrd
/usr/pgsql-9.3/bin/repmgr
/usr/pgsql-9.3/bin/repmgrd
/usr/pgsql-9.3/lib/repmgr_funcs.so
/usr/pgsql-9.3/share/contrib/repmgr.sql
/usr/pgsql-9.3/share/contrib/repmgr_funcs.sql
/usr/pgsql-9.3/share/contrib/uninstall_repmgr.sql
/usr/pgsql-9.3/share/contrib/uninstall_repmgr_funcs.sql
%attr(0755,root,root)/etc/init.d/repmgrd 
%attr(0644,root,root)/etc/sysconfig/repmgrd
%attr(0644,root,root)/etc/repmgr/repmgr.conf.sample

%changelog
* Thu Jun 05 2014 Nathan Van Overloop <nathan.van.overloop@nexperteam.be> 2.0.2
- fix witness creation to create db and user if needed
* Fri Apr 04 2014 Nathan Van Overloop <nathan.van.overloop@nexperteam.be> 2.0.1
- initial build for RHEL6

