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

#include <unistd.h>

#include "repmgr.h"
#include "repmgr-client.h"


/* global configuration structures */
t_runtime_options runtime_options = T_RUNTIME_OPTIONS_INITIALIZER;
t_configuration_options config_file_options = T_CONFIGURATION_OPTIONS_INITIALIZER;

/* Collate command line errors and warnings here for friendlier reporting */
ItemList	cli_errors = { NULL, NULL };
ItemList	cli_warnings = { NULL, NULL };

static bool	config_file_required = true;
static char	 pg_bindir[MAXLEN] = "";

static char	 repmgr_slot_name[MAXLEN] = "";
static char *repmgr_slot_name_ptr = NULL;

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


	while ((c = getopt_long(argc, argv, "?Vf:vtFb:S:L:", long_options,
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

			/* -S/--superuser */
			case 'S':
				strncpy(runtime_options.superuser, optarg, MAXLEN);
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
	 *	 { MASTER | PRIMARY } REGISTER |
	 *	 STANDBY {REGISTER | UNREGISTER | CLONE [node] | PROMOTE | FOLLOW [node] | SWITCHOVER | REWIND} |
	 *	 WITNESS { CREATE | REGISTER | UNREGISTER } |
	 *	 BDR { REGISTER | UNREGISTER } |
	 *	 CLUSTER { CROSSCHECK | MATRIX | SHOW | CLEANUP }
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
		default:
			/* An action will have been determined by this point  */
			break;
	}

	return SUCCESS;
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

	puts("");
}


static void
do_master_register(void)
{
	PGconn	   *conn = NULL;
	PGconn	   *master_conn = NULL;
	int			current_master_id = UNKNOWN_NODE_ID;
	int			ret;

	t_node_info node_info = T_NODE_INFO_INITIALIZER;
	int			record_found;
	bool		record_created;

	log_info(_("connecting to master database..."));

	conn = establish_db_connection(config_file_options.conninfo, true);
	log_verbose(LOG_INFO, _("connected to server, checking its state"));

	/* verify that node is running a supported server version */
	check_server_version(conn, "master", true, NULL);

	/* check that node is actually a master */
	ret = is_standby(conn);
	if (ret)
	{
		log_error(_(ret == 1 ? "server is in standby mode and cannot be registered as a master" :
					"connection to node lost!"));

		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	log_verbose(LOG_INFO, _("server is not in recovery"));

	/* create the repmgr extension if it doesn't already exist */
	if (!create_repmgr_extension(conn))
	{
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Ensure there isn't another active master already registered */
	master_conn = get_master_connection(conn, &current_master_id, NULL);

	if (master_conn != NULL)
	{
		if (current_master_id != config_file_options.node_id)
		{
			/* it's impossible to add a second master to a streaming replication cluster */
			log_error(_("there is already an active registered master (node ID: %i) in this cluster"), current_master_id);
			PQfinish(master_conn);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		/* we've probably connected to ourselves */
		PQfinish(master_conn);
	}


	begin_transaction(conn);

	/*
	 * Check if a node with a different ID is registered as master. This shouldn't
	 * happen but could do if an existing master was shut down without being
	 * unregistered.
	*/

	current_master_id = get_master_node_id(conn);
	if (current_master_id != NODE_NOT_FOUND && current_master_id != config_file_options.node_id)
	{
		log_error(_("another node with id %i is already registered as master"), current_master_id);
		// attempt to connect, add info/hint depending if active...
		log_info(_("a streaming replication cluster can have only one master node"));
		rollback_transaction(conn);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Check whether there's an existing record for this node, and
	 * update it if --force set
	 */

	record_found = get_node_record(conn, config_file_options.node_id, &node_info);

	if (record_found)
	{
		if (!runtime_options.force)
		{
			log_error(_("this node is already registered"));
			log_hint(_("use -F/--force to overwrite the existing node record"));
			rollback_transaction(conn);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}
	}
	else
	{
		node_info.node_id = config_file_options.node_id;
	}

	/* set type to "master", active to "true" and unset upstream_node_id*/
	node_info.type = MASTER;
	node_info.upstream_node_id = NO_UPSTREAM_NODE;
	node_info.active = true;

	/* update node record structure with settings from config file */
	strncpy(node_info.node_name, config_file_options.node_name, MAXLEN);
	strncpy(node_info.conninfo, config_file_options.conninfo, MAXLEN);
	strncpy(node_info.slot_name, repmgr_slot_name_ptr, MAXLEN);
	node_info.priority = config_file_options.priority;

	// XXX if upstream_node_id set, warn that it will be ignored

	if (record_found)
		record_created = update_node_record(conn,
											"master register",
											&node_info);
	else
		record_created = create_node_record(conn,
											"master register",
											&node_info);

	/* Log the event */
	create_event_record(conn,
						&config_file_options,
						config_file_options.node_id,
						"master_register",
						record_created,
						NULL);

	if (record_created == false)
	{
		rollback_transaction(conn);
		PQfinish(conn);

		log_notice(_("unable to register master node - see preceding messages"));
		exit(ERR_DB_QUERY);
	}

	commit_transaction(conn);
	PQfinish(conn);

	log_notice(_("master node record (id: %i) %s"),
			   config_file_options.node_id,
			   record_found ? "updated" : "registered");
	return;
}


// this should be the only place where superuser rights required
static
bool create_repmgr_extension(PGconn *conn)
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
	/* 1. Check if extension installed */
	if (PQgetisnull(res, 0, 1) == 0)
	{
		/* TODO: check version */
		log_info(_("extension \"repmgr\" already installed"));
		return true;
	}

	/* 2. If not, check extension available */

	if (PQgetisnull(res, 0, 0) == 1)
	{
		log_error(_("\"repmgr\" extension is not available"));
		return false;
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
			log_error(_("unable to establish superuser connection as \"%s\""), runtime_options.superuser);
			return false;
		}

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
						  "GRANT ALL ON ALL TABLES IN SCHEMA repmgr TO %s",
						  current_user);
		res = PQexec(schema_create_conn, query.data);

		termPQExpBuffer(&query);

		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			log_error(_("unable to grant usage on \"repmgr\" extension to %s:\n	 %s"),
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
static int
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
