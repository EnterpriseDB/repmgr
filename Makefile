#
# Makefile
# Copyright (c) 2ndQuadrant, 2010

repmgrd_OBJS = dbutils.o config.o repmgrd.o log.o
repmgr_OBJS = dbutils.o check_dir.o config.o repmgr.o log.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

all:  repmgrd repmgr

repmgrd: $(repmgrd_OBJS)
	$(CC) $(CFLAGS) $(repmgrd_OBJS) $(PG_LIBS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o repmgrd

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

install:
	$(INSTALL_PROGRAM) repmgrd$(X) '$(DESTDIR)$(bindir)'
	$(INSTALL_PROGRAM) repmgr$(X) '$(DESTDIR)$(bindir)'

clean:
	rm -f *.o
	rm -f repmgrd
	rm -f repmgr

deb: repmgrd repmgr
	mkdir -p ./debian/usr/bin
	cp repmgrd repmgr ./debian/usr/bin/
	dpkg-deb --build debian
	mv debian.deb ../postgresql-repmgr-9.0_1.0.0.deb

