/*
 * repmgr.c - Command interpreter for the repmgr package
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 *
 * This module is a command-line utility to easily setup a cluster of
 * hot standby servers for an HA environment
 *
 * Commands implemented are:
 *
 * [ MASTER | PRIMARY ] REGISTER
 *
 */

#include "repmgr.h"
#include "repmgr-client.h"

/* global configuration structures */
t_runtime_options runtime_options = T_RUNTIME_OPTIONS_INITIALIZER;
t_configuration_options config_file_options = T_CONFIGURATION_OPTIONS_INITIALIZER;

/* Collate command line errors and warnings here for friendlier reporting */
ItemList	cli_errors = { NULL, NULL };
ItemList	cli_warnings = { NULL, NULL };

int
main(int argc, char **argv)
{
	int			optindex;
	int			c;

    char	   *repmgr_node_type = NULL;
    char	   *repmgr_action = NULL;
    bool		valid_repmgr_node_type_found = true;
	int			action = NO_ACTION;
    char	   *dummy_action = "";

	bool 		config_file_parsed = false;


	set_progname(argv[0]);

	/*
	 * Tell the logger we're a command-line program - this will
	 * ensure any output logged before the logger is initialized
	 * will be formatted correctly. Can be overriden with "--log-to-file".
	 */
	logger_output_mode = OM_COMMAND_LINE;


	while ((c = getopt_long(argc, argv, "?Vf:vtFb:", long_options,
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
			case OPT_HELP: /* --help */
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

			/* general configuration options
             * ----------------------------- */

			/* -f/--config-file */
			case 'f':
				strncpy(runtime_options.config_file, optarg, MAXLEN);
				break;
			/* -F/--force */
			case 'F':
				runtime_options.force = true;
				break;
			/* -b/--pg_bindir */
			case 'b':
				strncpy(runtime_options.pg_bindir, optarg, MAXLEN);
				break;

			/* logging options
             * --------------- */

			/* -L/--log-level */
			case 'L':
			{
				int detected_log_level = detect_log_level(optarg);
				if (detected_log_level != -1)
				{
					strncpy(runtime_options.loglevel, optarg, MAXLEN);
				}
				else
				{
					PQExpBufferData invalid_log_level;
					initPQExpBuffer(&invalid_log_level);
					appendPQExpBuffer(&invalid_log_level, _("Invalid log level \"%s\" provided"), optarg);
					item_list_append(&cli_errors, invalid_log_level.data);
					termPQExpBuffer(&invalid_log_level);
				}
				break;
			}

			/* --log-to-file */
			case OPT_LOG_TO_FILE:
				runtime_options.log_to_file = true;
				logger_output_mode = OM_DAEMON;
				break;
			/* --terse */
			case 't':
				runtime_options.terse = true;
				break;
			/* --verbose */
            case 'v':
				runtime_options.verbose = true;
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


	/*
	 * The configuration file is not required for some actions (e.g. 'standby clone'),
	 * however if available we'll parse it anyway for options like 'log_level',
	 * 'use_replication_slots' etc.
	 */
	config_file_parsed = load_config(runtime_options.config_file,
									 runtime_options.verbose,
									 &config_file_options,
									 argv[0]);
	/*
	 * Initialize the logger. We've previously requested STDERR logging only
     * to ensure the repmgr command doesn't have its output diverted to a logging
     * facility (which usually doesn't make sense for a command line program).
     *
     * If required (e.g. when calling repmgr from repmgrd), this behaviour can be
     * overridden with "--log-to-file".
	 */

	logger_init(&config_file_options, progname());

	if (runtime_options.verbose)
		logger_set_verbose();

	if (runtime_options.terse)
		logger_set_terse();

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
	printf(_("  -b, --pg_bindir=PATH                path to PostgreSQL binaries (optional)\n"));
	printf(_("  -f, --config-file=PATH              path to the configuration file\n"));
	printf(_("  -F, --force                         force potentially dangerous operations to happen\n"));
    puts("");
	printf(_("Logging options:\n"));
	printf(_("  -L, --log-level                     set log level (overrides configuration file; default: NOTICE)\n"));
	printf(_("  --log-to-file                       log to file (or logging facility) defined in repmgr.conf\n"));
	printf(_("  -t, --terse                         don't display hints and other non-critical output\n"));
	printf(_("  -v, --verbose                       display additional log output (useful for debugging)\n"));
	printf(_("\n"));

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
