#
# Makefile
# Copyright (c) 2ndQuadrant, 2010-2017

HEADERS = $(wildcard *.h)

repmgrd_OBJS = dbutils.o config.o repmgrd.o log.o strutil.o
repmgr_OBJS = dbutils.o check_dir.o config.o repmgr.o log.o strutil.o dirmod.o compat.o

DATA = repmgr.sql uninstall_repmgr.sql

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS     = $(libpq_pgport)


all: repmgrd repmgr
	$(MAKE) -C sql

repmgrd: $(repmgrd_OBJS)
	$(CC) -o repmgrd $(CFLAGS) $(repmgrd_OBJS) $(PG_LIBS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS)
	$(MAKE) -C sql

repmgr: $(repmgr_OBJS)
	$(CC) -o repmgr $(CFLAGS) $(repmgr_OBJS) $(PG_LIBS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS)

# Make all objects depend on all include files. This is a bit of a
# shotgun approach, but the codebase is small enough that a complete rebuild
# is very fast anyway.
$(repmgr_OBJS): $(HEADERS)
$(repmgrd_OBJS): $(HEADERS)

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

# XXX: This overrides the pgxs install target - we're building two binaries,
# which is not supported by pgxs.mk's PROGRAM construct.
install: install_prog install_ext

install_prog:
	mkdir -p '$(DESTDIR)$(bindir)'
	$(INSTALL_PROGRAM) repmgrd$(X) '$(DESTDIR)$(bindir)/'
	$(INSTALL_PROGRAM) repmgr$(X) '$(DESTDIR)$(bindir)/'

install_ext:
	$(MAKE) -C sql install

# Distribution-specific package building targets
# ----------------------------------------------
#
# XXX we recommend using the PGDG-supplied packages where possible;
# see README.md for details.

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
PGBINDIR = /usr/lib/postgresql/$(PG_VERSION)/bin

deb: repmgrd repmgr
	mkdir -p ./debian/usr/bin ./debian$(PGBINDIR)
	cp repmgrd repmgr ./debian$(PGBINDIR)
	ln -s ../..$(PGBINDIR)/repmgr ./debian/usr/bin/repmgr
	mkdir -p ./debian$(SHAREDIR)/contrib/
	cp sql/repmgr_funcs.sql ./debian$(SHAREDIR)/contrib/
	cp sql/uninstall_repmgr_funcs.sql ./debian$(SHAREDIR)/contrib/
	mkdir -p ./debian$(PKGLIBDIR)/
	cp sql/repmgr_funcs.so ./debian$(PKGLIBDIR)/
	dpkg-deb --build debian
	mv debian.deb ../postgresql-repmgr-$(PG_VERSION)_$(REPMGR_VERSION).deb
	rm -rf ./debian/usr

