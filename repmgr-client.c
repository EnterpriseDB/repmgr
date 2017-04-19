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

/* Collate command line errors and warnings here for friendlier reporting */
ItemList	cli_errors = { NULL, NULL };
ItemList	cli_warnings = { NULL, NULL };

int
main(int argc, char **argv)
{
	int			optindex;
	int			c, targ;

    char	   *repmgr_node_type = NULL;
    char	   *repmgr_action = NULL;
    bool		valid_repmgr_node_type_found = true;
	int			action = NO_ACTION;
    char	   *dummy_action = "";

	set_progname(argv[0]);

	/*
	 * Tell the logger we're a command-line program - this will
	 * ensure any output logged before the logger is initialized
	 * will be formatted correctly
	 */
	logger_output_mode = OM_COMMAND_LINE;


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

	/* Exit here already if errors in command line options found */
	if (cli_errors.head != NULL)
	{
		exit_with_errors();
	}

	/*
	 * Determine the node type and action; following are valid:
     *
	 *   { MASTER | PRIMARY } REGISTER |
	 *   STANDBY {REGISTER | UNREGISTER | CLONE [node] | PROMOTE | FOLLOW [node] | SWITCHOVER | REWIND} |
	 *   WITNESS { CREATE | REGISTER | UNREGISTER } |
	 *   BDR { REGISTER | UNREGISTER } |
	 *   CLUSTER { CROSSCHECK | MATRIX | SHOW | CLEANUP }
	 *
	 * [node] is an optional hostname, provided instead of the -h/--host optipn
	 */
	if (optind < argc)
	{
		repmgr_node_type = argv[optind++];
    }

    if (optind < argc)
    {
        repmgr_action = argv[optind++];
    }
    else
    {
        repmgr_action = dummy_action;
    }

    if (repmgr_node_type != NULL)
    {
        if (strcasecmp(repmgr_node_type, "MASTER") == 0 || strcasecmp(repmgr_node_type, "PRIMARY") == 0 )
		{
			if (strcasecmp(repmgr_action, "REGISTER") == 0)
				action = MASTER_REGISTER;
		}
        else
        {
            valid_repmgr_node_type_found = false;
        }
    }

	if (action == NO_ACTION)
    {
        PQExpBufferData command_error;
        initPQExpBuffer(&command_error);

        if (repmgr_node_type == NULL)
        {
            appendPQExpBuffer(&command_error,
                              _("no repmgr command provided"));
        }
        else if (valid_repmgr_node_type_found == false && repmgr_action[0] == '\0')
        {
            appendPQExpBuffer(&command_error,
                              _("unknown repmgr node type '%s'"),
                              repmgr_node_type);
        }
		else if (repmgr_action[0] == '\0')
		{
           appendPQExpBuffer(&command_error,
                             _("no action provided for node type '%s'"),
                             repmgr_node_type);
		}
		else
		{
			appendPQExpBuffer(&command_error,
                              _("unknown repmgr action '%s %s'"),
                              repmgr_node_type,
                              repmgr_action);
		}

        item_list_append(&cli_errors, command_error.data);
	}

	/*
	 * Sanity checks for command line parameters completed by now;
	 * any further errors will be runtime ones
	 */
	if (cli_errors.head != NULL)
	{
		exit_with_errors();
	}

	return SUCCESS;
}


static void
do_help(void)
{
	printf(_("%s: replication management tool for PostgreSQL\n"), progname());
    puts("");

    /* add a big friendly warning if root is executing "repmgr --help" */
	if (geteuid() == 0)
	{
        printf(_("  **************************************************\n"));
        printf(_("  *** repmgr must be executed by a non-superuser ***\n"));
        printf(_("  **************************************************\n"));
        puts("");
    }

	printf(_("Usage:\n"));
	printf(_("  %s [OPTIONS] master  register\n"), progname());
    puts("");
	printf(_("General options:\n"));
	printf(_("  -?, --help                          show this help, then exit\n"));
	printf(_("  -V, --version                       output version information, then exit\n"));
    puts("");
	printf(_("General configuration options:\n"));
	printf(_("  -f, --config-file=PATH              path to the configuration file\n"));
    puts("");
}

static void
exit_with_errors(void)
{
	fprintf(stderr, _("The following command line errors were encountered:\n"));

	print_error_list(&cli_errors, LOG_ERR);

	fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname());

	exit(ERR_BAD_CONFIG);
}


static void
print_error_list(ItemList *error_list, int log_level)
{
	ItemListCell *cell;

	for (cell = error_list->head; cell; cell = cell->next)
	{
        fprintf(stderr, "  ");
		switch(log_level)
		{
			/* Currently we only need errors and warnings */
            case LOG_ERROR:
				log_error("%s", cell->string);
				break;
			case LOG_WARNING:
				log_warning("%s", cell->string);
				break;
		}
	}
}
