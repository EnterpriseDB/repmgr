#
# Makefile
# Copyright (c) 2ndQuadrant, 2010
#

MODULE_big = repmgr_funcs
DATA_built=repmgr_funcs.sql
DATA=uninstall_repmgr_funcs.sql
OBJS=repmgr_funcs.o

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/repmgr/sql
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
