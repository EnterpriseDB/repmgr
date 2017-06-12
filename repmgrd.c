/*
 * repmgrd.c - Replication manager daemon
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include "repmgr.h"
#include "config.h"

#include <stdio.h>

static void do_help(void);

int
main(int argc, char **argv)
{
	/* Disallow running as root to prevent directory ownership problems */
	if (geteuid() == 0)
	{
		fprintf(stderr,
				_("%s: cannot be run as root\n"
				  "Please log in (using, e.g., \"su\") as the "
				  "(unprivileged) user that owns "
				  "the data directory.\n"
				),
				progname());
		exit(1);
	}

	do_help();
	return 0;
}

void
do_help(void)
{
	printf(_("%s: replication management daemon for PostgreSQL\n"), progname());
	puts("");

	printf(_("Usage:\n"));
	printf(_("  %s [OPTIONS]\n"), progname());
	printf(_("\n"));
	printf(_("Options:\n"));
	puts("");

	printf(_("General options:\n"));
	printf(_("  -?, --help                show this help, then exit\n"));
	printf(_("  -V, --version             output version information, then exit\n"));

	puts("");

	printf(_("General configuration options:\n"));
	printf(_("  -v, --verbose             output verbose activity information\n"));
	printf(_("  -f, --config-file=PATH    path to the configuration file\n"));

	puts("");

	printf(_("General configuration options:\n"));
	printf(_("  -d, --daemonize           detach process from foreground\n"));
	printf(_("  -p, --pid-file=PATH       write a PID file\n"));
	puts("");

	printf(_("%s monitors a cluster of servers and optionally performs failover.\n"), progname());
}

