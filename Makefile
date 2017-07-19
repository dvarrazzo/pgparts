#
# pgparts -- simple tables partitioning for PostgreSQL
#
# Makefile for installation and tests
#

PG_CONFIG    ?= pg_config
EXTENSION    = pgparts

EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

# PostgreSQL version as a number, e.g. 9.1.4 -> 901
PGVERSION := $(shell $(PG_CONFIG) --version | awk '{print $$2}')
INTVERSION := $(shell echo $$(($$(echo $(PGVERSION) | sed 's/\([[:digit:]]\{1,\}\)\.\([[:digit:]]\{1,\}\).*/\1*100+\2/'))))

# DOCS         = $(wildcard doc/*.rst)

REGRESS      = testparts badnames testrange archive irregular
ifeq ($(shell echo $$(($(INTVERSION) >= 905))),1)
REGRESS += onconflict
endif
REGRESS_OPTS = --inputdir=test --load-language=plpgsql

all: sql/$(EXTENSION)--$(EXTVERSION).sql

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

DATA = $(wildcard sql/$(EXTENSION)--*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN = sql/$(EXTENSION)--$(EXTVERSION).sql

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
