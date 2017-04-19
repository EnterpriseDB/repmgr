/*
 * config.c - parse repmgr.conf and other configuration-related functionality
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include <sys/stat.h>			/* for stat() */

#include "repmgr.h"
#include "config.h"

const static char *_progname = NULL;

void
set_progname(const char *argv0)
{
	_progname = get_progname(argv0);
}

const char *
progname(void)
{
	return _progname;
}
