#
# Makefile
# Copyright (c) 2ndQuadrant, 2010-2015

repmgrd_OBJS = dbutils.o config.o repmgrd.o log.o strutil.o
repmgr_OBJS = dbutils.o check_dir.o config.o repmgr.o log.o strutil.o

DATA = repmgr.sql uninstall_repmgr.sql

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

all:  repmgrd repmgr
	$(MAKE) -C sql

repmgrd: $(repmgrd_OBJS)
	$(CC) $(CFLAGS) $(repmgrd_OBJS) $(PG_LIBS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o repmgrd
	$(MAKE) -C sql

repmgr: $(repmgr_OBJS)
	$(CC) $(CFLAGS) $(repmgr_OBJS) $(PG_LIBS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o repmgr

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/repmgr
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# XXX: Try to use PROGRAM construct (see pgxs.mk) someday. Right now
# is overriding pgxs install.
install: install_prog install_ext

install_prog:
	mkdir -p '$(DESTDIR)$(bindir)'
	$(INSTALL_PROGRAM) repmgrd$(X) '$(DESTDIR)$(bindir)/'
	$(INSTALL_PROGRAM) repmgr$(X) '$(DESTDIR)$(bindir)/'

install_ext:
	$(MAKE) -C sql install

install_rhel:
	mkdir -p '$(DESTDIR)/etc/init.d/'
	$(INSTALL_PROGRAM) RHEL/repmgrd.init '$(DESTDIR)/etc/init.d/repmgrd'
	mkdir -p '$(DESTDIR)/etc/sysconfig/'
	$(INSTALL_PROGRAM) RHEL/repmgrd.sysconfig '$(DESTDIR)/etc/sysconfig/repmgrd'
	mkdir -p '$(DESTDIR)/etc/repmgr/'
	$(INSTALL_PROGRAM) repmgr.conf.sample '$(DESTDIR)/etc/repmgr/'
	mkdir -p '$(DESTDIR)/usr/bin/'
	$(INSTALL_PROGRAM) repmgrd$(X) '$(DESTDIR)/usr/bin/'
	$(INSTALL_PROGRAM) repmgr$(X) '$(DESTDIR)/usr/bin/'

ifneq (,$(DATA)$(DATA_built))
	@for file in $(addprefix $(srcdir)/, $(DATA)) $(DATA_built); do \
	  echo "$(INSTALL_DATA) $$file '$(DESTDIR)$(datadir)/$(datamoduledir)'"; \
	  $(INSTALL_DATA) $$file '$(DESTDIR)$(datadir)/$(datamoduledir)'; \
	done
endif

clean:
	rm -f *.o
	rm -f repmgrd
	rm -f repmgr
	$(MAKE) -C sql clean

# Get correct version numbers and install paths, depending on your postgres version
PG_VERSION = $(shell pg_config --version | cut -d ' ' -f 2 | cut -d '.' -f 1,2)
REPMGR_VERSION = $(shell grep REPMGR_VERSION version.h | cut -d ' ' -f 3 | cut -d '"' -f 2)
PKGLIBDIR = $(shell pg_config --pkglibdir)
SHAREDIR = $(shell pg_config --sharedir)

deb: repmgrd repmgr
	mkdir -p ./debian/usr/bin
	cp repmgrd repmgr ./debian/usr/bin/
	mkdir -p ./debian$(SHAREDIR)/contrib/
	cp sql/repmgr_funcs.sql ./debian$(SHAREDIR)/contrib/
	cp sql/uninstall_repmgr_funcs.sql ./debian$(SHAREDIR)/contrib/
	mkdir -p ./debian$(PKGLIBDIR)/
	cp sql/repmgr_funcs.so ./debian$(PKGLIBDIR)/
	dpkg-deb --build debian
	mv debian.deb ../postgresql-repmgr-$(PG_VERSION)_$(REPMGR_VERSION).deb
	rm -rf ./debian/usr

