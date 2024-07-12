# online_advisor/Makefile

MODULE_big = online_advisor
OBJS = \
	$(WIN32RES) \
	online_advisor.o
PGFILEDESC = "online_advisor - suggest missing indexes"

EXTENSION = online_advisor
DATA = online_advisor--1.0.sql

REGRESS = test
REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/online_advisor/online_advisor.conf

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/online_advisor
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
