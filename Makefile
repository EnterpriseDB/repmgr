#
# Makefile
# Copyright (c) 2ndQuadrant, 2010-2011

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
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/repmgr
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# XXX: Try to use PROGRAM construct (see pgxs.mk) someday. Right now
# is overriding pgxs install.
install:
	$(INSTALL_PROGRAM) repmgrd$(X) '$(DESTDIR)$(bindir)'
	$(INSTALL_PROGRAM) repmgr$(X) '$(DESTDIR)$(bindir)'
	$(MAKE) -C sql install

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

deb: repmgrd repmgr
	mkdir -p ./debian/usr/bin
	cp repmgrd repmgr ./debian/usr/bin/
	mkdir -p ./debian/usr/share/postgresql/9.0/contrib/
	cp sql/repmgr_funcs.sql ./debian/usr/share/postgresql/9.0/contrib/
	cp sql/uninstall_repmgr_funcs.sql ./debian/usr/share/postgresql/9.0/contrib/
	mkdir -p ./debian/usr/lib/postgresql/9.0/lib/
	cp sql/repmgr_funcs.so ./debian/usr/lib/postgresql/9.0/lib/
	dpkg-deb --build debian
	mv debian.deb ../postgresql-repmgr-9.0_1.0.0.deb
	rm -rf ./debian/usr

