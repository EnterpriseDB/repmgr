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
 * STANDBY CLONE (wip)
 *
 * CLUSTER EVENT
 */

#include <unistd.h>

#include "repmgr.h"
#include "repmgr-client.h"
#include "repmgr-client-global.h"
#include "repmgr-action-master.h"
#include "repmgr-action-standby.h"
#include "repmgr-action-cluster.h"


/* globally available variables *
 * ============================ */

t_runtime_options runtime_options = T_RUNTIME_OPTIONS_INITIALIZER;
t_configuration_options config_file_options = T_CONFIGURATION_OPTIONS_INITIALIZER;

/* conninfo params for the node we're cloning from */
t_conninfo_param_list source_conninfo;

bool	 config_file_required = true;
char	 pg_bindir[MAXLEN] = "";

char	 repmgr_slot_name[MAXLEN] = "";
char	*repmgr_slot_name_ptr = NULL;

/*
 * if --node-id/--node-name provided, place that node's record here
 * for later use
 */
t_node_info target_node_info = T_NODE_INFO_INITIALIZER;


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

	bool		config_file_parsed = false;


	set_progname(argv[0]);

	/*
	 * Tell the logger we're a command-line program - this will
	 * ensure any output logged before the logger is initialized
	 * will be formatted correctly. Can be overriden with "--log-to-file".
	 */
	logger_output_mode = OM_COMMAND_LINE;


	while ((c = getopt_long(argc, argv, "?Vf:Fb:S:L:vtD:", long_options,
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

			/* connection options */
			/* ------------------ */

			/* -h/--host */
			case 'h':
				strncpy(runtime_options.host, optarg, MAXLEN);
				param_set(&source_conninfo, "host", optarg);
				runtime_options.connection_param_provided = true;
				runtime_options.host_param_provided = true;
				break;

			/* -R/--remote_user */
			case 'R':
				strncpy(runtime_options.remote_user, optarg, MAXLEN);
				break;

			/* -S/--superuser */
			case 'S':
				strncpy(runtime_options.superuser, optarg, MAXLEN);
				break;

			/* node options *
			 * ------------ */

			/* -D/--pgdata/--data-dir */
			case 'D':
				strncpy(runtime_options.data_dir, optarg, MAXPGPATH);
				break;

			/* --node-id */
			case OPT_NODE_ID:
				runtime_options.node_id = repmgr_atoi(optarg, "--node-id", &cli_errors, false);
				break;

			/* --node-name */
			case OPT_NODE_NAME:
				strncpy(runtime_options.node_name, optarg, MAXLEN);
				break;

			/* event options *
			 * ------------- */

			case OPT_EVENT:
				strncpy(runtime_options.event, optarg, MAXLEN);
				break;

			case OPT_LIMIT:
				runtime_options.limit = repmgr_atoi(optarg, "--limit", &cli_errors, false);
				runtime_options.limit_provided = true;
				break;

			case OPT_ALL:
				runtime_options.all = true;
				break;

			/* logging options *
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
	 *	 { MASTER | PRIMARY } REGISTER |
	 *	 STANDBY {REGISTER | UNREGISTER | CLONE [node] | PROMOTE | FOLLOW [node] | SWITCHOVER | REWIND} |
	 *	 WITNESS { CREATE | REGISTER | UNREGISTER } |
	 *	 BDR { REGISTER | UNREGISTER } |
	 *	 CLUSTER { CROSSCHECK | MATRIX | SHOW | CLEANUP | EVENT }
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
		else if(strcasecmp(repmgr_node_type, "CLUSTER") == 0)
		{
			if (strcasecmp(repmgr_action, "EVENT") == 0)
				action = CLUSTER_EVENT;
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

	if (optind < argc)
	{
		PQExpBufferData too_many_args;
		initPQExpBuffer(&too_many_args);
		appendPQExpBuffer(&too_many_args, _("too many command-line arguments (first extra is \"%s\")"), argv[optind]);
		item_list_append(&cli_errors, too_many_args.data);
	}

	check_cli_parameters(action);

	/*
	 * Sanity checks for command line parameters completed by now;
	 * any further errors will be runtime ones
	 */
	if (cli_errors.head != NULL)
	{
		exit_with_errors();
	}

	/*
	 * Print any warnings about inappropriate command line options,
	 * unless -t/--terse set
	 */
	if (cli_warnings.head != NULL && runtime_options.terse == false)
	{
		log_warning(_("following problems with command line parameters detected:"));
		print_item_list(&cli_warnings);
	}

	/*
	 * The configuration file is not required for some actions (e.g. 'standby clone'),
	 * however if available we'll parse it anyway for options like 'log_level',
	 * 'use_replication_slots' etc.
	 */
	config_file_parsed = load_config(runtime_options.config_file,
									 runtime_options.verbose,
									 runtime_options.terse,
									 &config_file_options,
									 argv[0]);

	/* Some configuration file items can be overriden by command line options */
	/* Command-line parameter -L/--log-level overrides any setting in config file*/
	if (*runtime_options.loglevel != '\0')
	{
		strncpy(config_file_options.loglevel, runtime_options.loglevel, MAXLEN);
	}

	/*
	 * Initialise pg_bindir - command line parameter will override
	 * any setting in the configuration file
	 */
	if (!strlen(runtime_options.pg_bindir))
	{
		strncpy(runtime_options.pg_bindir, config_file_options.pg_bindir, MAXLEN);
	}

	/* Add trailing slash */
	if (strlen(runtime_options.pg_bindir))
	{
		int len = strlen(runtime_options.pg_bindir);
		if (runtime_options.pg_bindir[len - 1] != '/')
		{
			maxlen_snprintf(pg_bindir, "%s/", runtime_options.pg_bindir);
		}
		else
		{
			strncpy(pg_bindir, runtime_options.pg_bindir, MAXLEN);
		}
	}

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

	/*
	 * Node configuration information is not needed for all actions, with
	 * STANDBY CLONE being the main exception.
	 */
	if (config_file_required)
	{
		/*
		 * if a configuration file was provided, the configuration file parser
		 * will already have errored out if no valid node_id found
		 */
		if (config_file_options.node_id == NODE_NOT_FOUND)
		{
			log_error(_("no node information was found - "
						"please supply a configuration file"));
			exit(ERR_BAD_CONFIG);
		}
	}

	/*
	 * If a node was specified (by --node-id or --node-name), check it exists
	 * (and pre-populate a record for later use).
	 *
	 * At this point check_cli_parameters() will already have determined
	 * if provision of these is valid for the action, otherwise it unsets them.
	 *
	 * We need to check this much later than other command line parameters
	 * as we need to wait until the configuration file is parsed and we can
	 * obtain the conninfo string.
	 */

	if (runtime_options.node_id != UNKNOWN_NODE_ID || runtime_options.node_name[0] != '\0')
	{
		PGconn *conn;
		int record_found;
		conn = establish_db_connection(config_file_options.conninfo, true);

		if (runtime_options.node_id != UNKNOWN_NODE_ID)
		{
			record_found = get_node_record(conn, runtime_options.node_id, &target_node_info);

			if (!record_found)
			{
				log_error(_("node %i (specified with --node-id) not found"),
						  runtime_options.node_id);
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}
		}
		else if (runtime_options.node_name[0] != '\0')
		{
			char *escaped = escape_string(conn, runtime_options.node_name);
			if (escaped == NULL)
			{
				log_error(_("unable to escape value provided for --node-name"));
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}

			record_found = get_node_record_by_name(conn, escaped, &target_node_info);

			pfree(escaped);
			if (!record_found)
			{
				log_error(_("node %s (specified with --node-name) not found"),
						  runtime_options.node_name);
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}
		}

		PQfinish(conn);
	}

	/*
	 * Initialise slot name, if required (9.4 and later)
	 *
	 * NOTE: the slot name will be defined for each record, including
	 * the master; the `slot_name` column in `repl_nodes` defines
	 * the name of the slot, but does not imply a slot has been created.
	 * The version check for 9.4 or later  is done in check_upstream_config()
	 */
	if (config_file_options.use_replication_slots)
	{
		maxlen_snprintf(repmgr_slot_name, "repmgr_slot_%i", config_file_options.node_id);
		repmgr_slot_name_ptr = repmgr_slot_name;
		log_verbose(LOG_DEBUG, "slot name initialised as: %s", repmgr_slot_name);
	}

	switch (action)
	{
		case MASTER_REGISTER:
			do_master_register();
			break;
		case STANDBY_CLONE:
			do_standby_clone();
			break;
		case CLUSTER_EVENT:
			do_cluster_event();
			break;
		default:
			/* An action will have been determined by this point  */
			break;
	}

	return SUCCESS;
}



/*
 * Check for useless or conflicting parameters, and also whether a
 * configuration file is required.
 *
 * Messages will be added to the command line warning and error lists
 * as appropriate.
 *
 * XXX for each individual actions, check only required actions
 * for non-required actions check warn if provided
 */

static void
check_cli_parameters(const int action)
{
	/* ========================================================================
	 * check all parameters required for an action are provided, and warn
	 * about ineffective actions
	 * ========================================================================
	 */
	switch (action)
	{
		case MASTER_REGISTER:
			/* no required parameters */
		case CLUSTER_EVENT:
			/* no required parameters */
			break;

	}

	/* ========================================================================
	 * warn if parameters provided for an action where they're not relevant
	 * ========================================================================
	 */

	/* --host etc.*/
	if (runtime_options.connection_param_provided)
	{
		switch (action)
		{
			case STANDBY_CLONE:
			case STANDBY_FOLLOW:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("datqabase connection parameters not required when executing %s"),
										action_name(action));
		}
	}

	/* -D/--data-dir */
	if (runtime_options.data_dir[0])
	{
		switch (action)
		{
			case STANDBY_CLONE:
			case STANDBY_FOLLOW:
			case STANDBY_RESTORE_CONFIG:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("-D/--pgdata not required when executing %s"),
										action_name(action));
		}
	}

	/*
	 * --node-id
	 *
	 * NOTE: overrides --node-name, if present
	 */
	if (runtime_options.node_id != UNKNOWN_NODE_ID)
	{
		switch (action)
		{
			case STANDBY_UNREGISTER:
			case WITNESS_UNREGISTER:
			case CLUSTER_EVENT:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--node-id not required when executing %s"),
										action_name(action));
				runtime_options.node_id = UNKNOWN_NODE_ID;
		}
	}

	if (runtime_options.node_name[0])
	{
		switch (action)
		{
			case STANDBY_UNREGISTER:
			case WITNESS_UNREGISTER:
			case CLUSTER_EVENT:
				if (runtime_options.node_id != UNKNOWN_NODE_ID)
				{
					item_list_append(&cli_warnings,
									 _("--node-id provided, ignoring --node-name"));
					memset(runtime_options.node_name, 0, sizeof(runtime_options.node_name));
				}
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--node-name not required when executing %s"),
										action_name(action));
				memset(runtime_options.node_name, 0, sizeof(runtime_options.node_name));
		}
	}

	if (runtime_options.event[0])
	{
		switch (action)
		{
			case CLUSTER_EVENT:
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--event not required when executing %s"),
										action_name(action));
		}
	}

	if (runtime_options.limit_provided)
	{
		switch (action)
		{
			case CLUSTER_EVENT:
				if (runtime_options.limit < 1)
				{
					item_list_append_format(&cli_errors,
											_("value for --limit must be 1 or greater (provided: %i)"),
											runtime_options.limit);
				}
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--limit not required when executing %s"),
										action_name(action));
		}
	}

	if (runtime_options.all)
	{
		switch (action)
		{
			case CLUSTER_EVENT:
				if (runtime_options.limit_provided == true)
				{
					runtime_options.all = false;
					item_list_append(&cli_warnings,
									 _("--limit provided, ignoring --all"));
				}
				break;
			default:
				item_list_append_format(&cli_warnings,
										_("--all not required when executing %s"),
										action_name(action));
		}
	}
}


static const char*
action_name(const int action)
{
	switch(action)
	{
		case MASTER_REGISTER:
			return "MASTER REGISTER";
		case STANDBY_CLONE:
			return "STANDBY CLONE";
		case STANDBY_REGISTER:
			return "STANDBY REGISTER";
		case STANDBY_UNREGISTER:
			return "STANDBY UNREGISTER";
		case WITNESS_UNREGISTER:
			return "WITNESS UNREGISTER";
		case CLUSTER_EVENT:
			return "CLUSTER EVENT";
	}

	return "UNKNOWN ACTION";
}
static void
exit_with_errors(void)
{
	fprintf(stderr, _("The following command line errors were encountered:\n"));

	print_item_list(&cli_errors);

	fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname());

	exit(ERR_BAD_CONFIG);
}


static void
print_item_list(ItemList *item_list)
{
	ItemListCell *cell;

	for (cell = item_list->head; cell; cell = cell->next)
	{
		fprintf(stderr, "  %s\n", cell->string);
	}
}


static void
do_help(void)
{
	printf(_("%s: replication management tool for PostgreSQL\n"), progname());
	puts("");

	/* add a big friendly warning if root is executing "repmgr --help" */
	if (geteuid() == 0)
	{
		printf(_("	**************************************************\n"));
		printf(_("	*** repmgr must be executed by a non-superuser ***\n"));
		printf(_("	**************************************************\n"));
		puts("");
	}

	printf(_("Usage:\n"));
	printf(_("	%s [OPTIONS] master	 register\n"), progname());
	printf(_("	%s [OPTIONS] cluster event\n"), progname());
	puts("");
	printf(_("General options:\n"));
	printf(_("  -?, --help                          show this help, then exit\n"));
	printf(_("  -V, --version                       output version information, then exit\n"));
	puts("");
	printf(_("General configuration options:\n"));
	printf(_("  -b, --pg_bindir=PATH                path to PostgreSQL binaries (optional)\n"));
	printf(_("  -f, --config-file=PATH              path to the repmgr configuration file\n"));
	printf(_("  -F, --force                         force potentially dangerous operations to happen\n"));
	puts("");
	printf(_("Connection options:\n"));
	printf(_("  -S, --superuser=USERNAME            superuser to use if repmgr user is not superuser\n"));
	puts("");
	printf(_("Node-specific options:\n"));
	printf(_("  -D, --pgdata=DIR                    location of the node's data directory \n"));
	printf(_("  --node-id                           specify a node by id (only available for some operations)\n"));
	printf(_("  --node-name                         specify a node by name (only available for some operations)\n"));

	puts("");

	printf(_("Logging options:\n"));
	printf(_("  -L, --log-level                     set log level (overrides configuration file; default: NOTICE)\n"));
	printf(_("  --log-to-file                       log to file (or logging facility) defined in repmgr.conf\n"));
	printf(_("  -t, --terse                         don't display hints and other non-critical output\n"));
	printf(_("  -v, --verbose                       display additional log output (useful for debugging)\n"));
	puts("");
	printf(_("CLUSTER SHOW options:\n"));

	puts("");
}







/*
 * Create the repmgr extension, and grant access for the repmgr
 * user if not a superuser.
 *
 * Note:
 *   This should be the only place where superuser rights are required.
 *   We should also consider possible scenarious where a non-superuser
 *   has sufficient privileges to install the extension.
 */

bool
create_repmgr_extension(PGconn *conn)
{
	PQExpBufferData	  query;
	PGresult		 *res;

	char			 *current_user;
	const char		 *superuser_status;
	bool			  is_superuser;
	PGconn			 *superuser_conn = NULL;
	PGconn			 *schema_create_conn = NULL;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "	  SELECT ae.name, e.extname "
					  "     FROM pg_catalog.pg_available_extensions ae "
					  "LEFT JOIN pg_catalog.pg_extension e "
					  "       ON e.extname=ae.name "
					  "	   WHERE ae.name='repmgr' ");

	res = PQexec(conn, query.data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to execute extension query:\n	%s"),
				PQerrorMessage(conn));
		PQclear(res);

		return false;
	}

	/* 1. Check extension is actually available */

	if (PQntuples(res) == 0)
	{
		log_error(_("\"repmgr\" extension is not available"));
		return false;
	}

	/* 2. Check if extension installed */
	if (PQgetisnull(res, 0, 1) == 0)
	{
		/* TODO: check version */
		log_info(_("extension \"repmgr\" already installed"));
		return true;
	}

	PQclear(res);
	termPQExpBuffer(&query);

	log_notice(_("attempting to install extension \"repmgr\""));

	/* 3. Check if repmgr user is superuser, if not connect as superuser */
	current_user = PQuser(conn);
	superuser_status = PQparameterStatus(conn, "is_superuser");

	is_superuser = (strcmp(superuser_status, "on") == 0) ? true : false;

	if (is_superuser == false)
	{
		if (runtime_options.superuser[0] == '\0')
		{
			log_error(_("\"%s\" is not a superuser and no superuser name supplied"), current_user);
			log_hint(_("supply a valid superuser name with -S/--superuser"));
			return false;
		}

		superuser_conn = establish_db_connection_as_user(config_file_options.conninfo,
														 runtime_options.superuser,
														 false);

		if (PQstatus(superuser_conn) != CONNECTION_OK)
		{
			log_error(_("unable to establish superuser connection as \"%s\""),
					  runtime_options.superuser);
			return false;
		}

		/* check provided superuser really is superuser */
		superuser_status = PQparameterStatus(superuser_conn, "is_superuser");
		if (strcmp(superuser_status, "off") == 0)
		{
			log_error(_("\"%s\" is not a superuser"), runtime_options.superuser);
			PQfinish(superuser_conn);
			return false;
		}

		schema_create_conn = superuser_conn;
	}
	else
	{
		schema_create_conn = conn;
	}

	/* 4. Create extension */
	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "CREATE EXTENSION repmgr");

	res = PQexec(schema_create_conn, query.data);

	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		log_error(_("unable to create \"repmgr\" extension:\n  %s"),
				  PQerrorMessage(schema_create_conn));
		log_hint(_("check that the provided user has sufficient privileges for CREATE EXTENSION"));

		PQclear(res);
		if (superuser_conn != 0)
			PQfinish(superuser_conn);
		return false;
	}

	PQclear(res);

	/* 5. If not superuser, grant usage */
	if (is_superuser == false)
	{
		initPQExpBuffer(&query);

		appendPQExpBuffer(&query,
						  "GRANT USAGE ON SCHEMA repmgr TO %s",
						  current_user);
		res = PQexec(schema_create_conn, query.data);

		termPQExpBuffer(&query);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			log_error(_("unable to grant usage on \"repmgr\" extension to %s:\n  %s"),
					  current_user,
					  PQerrorMessage(schema_create_conn));
			PQclear(res);

			if (superuser_conn != 0)
				PQfinish(superuser_conn);

			return false;
		}

		initPQExpBuffer(&query);
		appendPQExpBuffer(&query,
						  "GRANT ALL ON ALL TABLES IN SCHEMA repmgr TO %s",
						  current_user);
		res = PQexec(schema_create_conn, query.data);

		termPQExpBuffer(&query);

		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			log_error(_("unable to grant permission on tables on \"repmgr\" extension to %s:\n  %s"),
					  current_user,
					  PQerrorMessage(schema_create_conn));
			PQclear(res);

			if (superuser_conn != 0)
				PQfinish(superuser_conn);

			return false;
		}
	}

	if (superuser_conn != 0)
		PQfinish(superuser_conn);

	log_notice(_("\"repmgr\" extension successfully installed"));

	create_event_record(conn,
						&config_file_options,
						config_file_options.node_id,
						"cluster_created",
						true,
						NULL);

	return true;
}


/**
 * check_server_version()
 *
 * Verify that the server is MIN_SUPPORTED_VERSION_NUM or later
 *
 * PGconn *conn:
 *	 the connection to check
 *
 * char *server_type:
 *	 either "master" or "standby"; used to format error message
 *
 * bool exit_on_error:
 *	 exit if reported server version is too low; optional to enable some callers
 *	 to perform additional cleanup
 *
 * char *server_version_string
 *	 passed to get_server_version(), which will place the human-readable
 *	 server version string there (e.g. "9.4.0")
 */
int
check_server_version(PGconn *conn, char *server_type, bool exit_on_error, char *server_version_string)
{
	int			server_version_num = 0;

	server_version_num = get_server_version(conn, server_version_string);
	if (server_version_num < MIN_SUPPORTED_VERSION_NUM)
	{
		if (server_version_num > 0)
			log_error(_("%s requires %s to be PostgreSQL %s or later"),
					  progname(),
					  server_type,
					  MIN_SUPPORTED_VERSION
				);

		if (exit_on_error == true)
		{
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		return -1;
	}

	return server_version_num;
}


int
test_ssh_connection(char *host, char *remote_user)
{
	char		script[MAXLEN];
	int			r = 1, i;

	/* On some OS, true is located in a different place than in Linux
	 * we have to try them all until all alternatives are gone or we
	 * found `true' because the target OS may differ from the source
	 * OS
	 */
	const char *bin_true_paths[] = {
		"/bin/true",
		"/usr/bin/true",
		NULL
	};

	for (i = 0; bin_true_paths[i] && r != 0; ++i)
	{
		if (!remote_user[0])
			maxlen_snprintf(script, "ssh -o Batchmode=yes %s %s %s 2>/dev/null",
							config_file_options.ssh_options, host, bin_true_paths[i]);
		else
			maxlen_snprintf(script, "ssh -o Batchmode=yes %s %s -l %s %s 2>/dev/null",
							config_file_options.ssh_options, host, remote_user,
							bin_true_paths[i]);

		log_verbose(LOG_DEBUG, _("test_ssh_connection(): executing %s"), script);
		r = system(script);
	}

	if (r != 0)
		log_warning(_("unable to connect to remote host '%s' via SSH"), host);

	return r;
}


/*
 * Execute a command locally. If outputbuf == NULL, discard the
 * output.
 */
bool
local_command(const char *command, PQExpBufferData *outputbuf)
{
	FILE *fp;
	char output[MAXLEN];
	int retval;

	if (outputbuf == NULL)
	{
		retval = system(command);
		return (retval == 0) ? true : false;
	}
	else
	{
		fp = popen(command, "r");

		if (fp == NULL)
		{
			log_error(_("unable to execute local command:\n%s"), command);
			return false;
		}

		/* TODO: better error handling */
		while (fgets(output, MAXLEN, fp) != NULL)
		{
			appendPQExpBuffer(outputbuf, "%s", output);
		}

		pclose(fp);

		if (outputbuf->data != NULL)
			log_verbose(LOG_DEBUG, "local_command(): output returned was:\n%s", outputbuf->data);
		else
			log_verbose(LOG_DEBUG, "local_command(): no output returned");

		return true;
	}
}
