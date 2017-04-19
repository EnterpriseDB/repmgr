/*
 * repmgr.c - Command interpreter for the repmgr package
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 *
 * This module is a command-line utility to easily setup a cluster of
 * hot standby servers for an HA environment
 *
 */

#include "repmgr.h"
#include "repmgr-client.h"

int
main(int argc, char **argv)
{
	int			optindex;
	int			c, targ;
	int			action = NO_ACTION;

	set_progname(argv[0]);

	while ((c = getopt_long(argc, argv, "?Vd:h:p:U:S:D:f:R:w:k:FWIvb:rcL:tm:C:", long_options,
							&optindex)) != -1)
	{
		/*
		 * NOTE: some integer parameters (e.g. -p/--port) are stored internally
		 * as strings. We use repmgr_atoi() to check these but discard the
		 * returned integer; repmgr_atoi() will append the error message to the
		 * provided list.
		 */
		switch (c)
		{
			case OPT_HELP:
				do_help();
				exit(SUCCESS);

			case '?':
				/* Actual help option given */
				if (strcmp(argv[optind - 1], "-?") == 0)
				{
					do_help();
					exit(SUCCESS);
				}
				break;

		}
	}

	return SUCCESS;
}


static void
do_help(void)
{
	printf(_("%s: replication management tool for PostgreSQL\n"), progname());
	printf(_("\n"));
	printf(_("Usage:\n"));

}
