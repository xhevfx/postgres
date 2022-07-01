# contrib/ftcopy/Makefile

MODULE_big = ftcopy
OBJS	= copy.o trigger.o ftcopy.o
PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)
SHLIB_PREREQS = submake-libpq

EXTENSION = ftcopy
DATA = ftcopy--1.0.sql

REGRESS = ftcopy

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/ftcopy
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
