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

/* global configuration structures */
t_runtime_options runtime_options = T_RUNTIME_OPTIONS_INITIALIZER;

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
            /*
             * Options which cause repmgr to exit in this block;
             * these are the only ones which can be executed as root user
             */
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
			case 'V':
				printf("%s %s\n", progname(), REPMGR_VERSION);
				exit(SUCCESS);
            /* general options */
			case 'f':
				strncpy(runtime_options.config_file, optarg, MAXLEN);
				break;
		}
	}

	/*
     * Disallow further running as root to prevent directory ownership problems.
     * We check this here to give the root user a chance to execute --help/--version
     * options.
     */
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
	return SUCCESS;
}


static void
do_help(void)
{
	printf(_("%s: replication management tool for PostgreSQL\n"), progname());
	printf(_("\n"));

    /* add a big friendly warning if root is executing "repmgr --help" */
	if (geteuid() == 0)
	{
        printf(_("  **************************************************\n"));
        printf(_("  *** repmgr must be executed by a non-superuser ***\n"));
        printf(_("  **************************************************\n"));
        puts("");
    }
	printf(_("Usage:\n"));

}
