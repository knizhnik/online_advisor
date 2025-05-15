# online_advisor/Makefile

MODULE_big = online_advisor
OBJS = \
	$(WIN32RES) \
	online_advisor.o
PGFILEDESC = "online_advisor - suggest missing indexes"

EXTENSION = online_advisor
DATA = online_advisor--1.0.sql

REGRESS = test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
