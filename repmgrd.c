/*
 * repmgrd.c - Replication manager daemon
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include "repmgr.h"
#include "config.h"

#include <stdio.h>
#include <signal.h>
#include <sys/stat.h>


#define OPT_HELP	1

static char	   *config_file = NULL;
static bool		verbose = false;
static char	   *pid_file = NULL;
static bool		daemonize = false;

t_configuration_options config_file_options = T_CONFIGURATION_OPTIONS_INITIALIZER;

static t_node_info local_node_info = T_NODE_INFO_INITIALIZER;
static PGconn	   *local_conn = NULL;

static PGconn	   *master_conn = NULL;

/* Collate command line errors here for friendlier reporting */
static ItemList	cli_errors = { NULL, NULL };

/*
 * Record receipt SIGHUP; will cause configuration file to be reread at the
 * appropriate point in the main loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;

static void show_help(void);
static void show_usage(void);
static void daemonize_process(void);
static void check_and_create_pid_file(const char *pid_file);

static void start_monitoring(void);
static void monitor_streaming_master(void);
static void monitor_streaming_standby(void);

#ifndef WIN32
static void setup_event_handlers(void);
static void handle_sighup(SIGNAL_ARGS);
static void handle_sigint(SIGNAL_ARGS);
#endif

static void close_connections();
static void terminate(int retval);

int
main(int argc, char **argv)
{
	int			optindex;
	int			c;
	char		cli_loglevel[MAXLEN] = "";
	bool		cli_monitoring_history = false;

	RecordStatus record_status;

	static struct option long_options[] =
	{
/* general options */
		{"help", no_argument, NULL, OPT_HELP},
		{"version", no_argument, NULL, 'V'},

/* configuration options */
		{"config-file", required_argument, NULL, 'f'},

/* daemon options */
		{"daemonize", no_argument, NULL, 'd'},
		{"pid-file", required_argument, NULL, 'p'},

/* logging options */
		{"log-level", required_argument, NULL, 'L'},
		{"verbose", no_argument, NULL, 'v'},

/* legacy options */
		{"monitoring-history", no_argument, NULL, 'm'},
		{NULL, 0, NULL, 0}
	};

	set_progname(argv[0]);

	/* Disallow running as root */
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

	while ((c = getopt_long(argc, argv, "?Vf:L:vdp:m", long_options, &optindex)) != -1)
	{
		switch (c)
		{

			/* general options */

			case '?':
				/* Actual help option given */
				if (strcmp(argv[optind - 1], "-?") == 0)
				{
					show_help();
					exit(SUCCESS);
				}
				/* unknown option reported by getopt */
				goto unknown_option;
				break;

			case OPT_HELP:
				show_help();
				exit(SUCCESS);

			case 'V':
				/*
				 * in contrast to repmgr3 and earlier, we only display the repmgr version
				 * as it's not specific to a particular PostgreSQL version
				 */
				printf("%s %s\n", progname(), REPMGR_VERSION);
				exit(SUCCESS);

			/* configuration options */

			case 'f':
				config_file = optarg;
				break;

			/* daemon options */

			case 'd':
				daemonize = true;
				break;

			case 'p':
				pid_file = optarg;
				break;

			/* logging options */

			/* -L/--log-level */
			case 'L':
			{
				int detected_cli_loglevel = detect_log_level(optarg);
				if (detected_cli_loglevel != -1)
				{
					strncpy(cli_loglevel, optarg, MAXLEN);
				}
				else
				{
					PQExpBufferData invalid_log_level;
					initPQExpBuffer(&invalid_log_level);
					appendPQExpBuffer(&invalid_log_level, _("invalid log level \"%s\" provided"), optarg);
					item_list_append(&cli_errors, invalid_log_level.data);
					termPQExpBuffer(&invalid_log_level);
				}
				break;
			}
			case 'v':
				verbose = true;
				break;

			/* legacy options */

			case 'm':
				cli_monitoring_history = true;
				break;

			default:
     unknown_option:
				show_usage();
				exit(ERR_BAD_CONFIG);
		}
	}

	/* Exit here already if errors in command line options found */
	if (cli_errors.head != NULL)
	{
		exit_with_cli_errors(&cli_errors);
	}

	/*
	 * Tell the logger we're a daemon - this will ensure any output logged
	 * before the logger is initialized will be formatted correctly
	 */
	logger_output_mode = OM_DAEMON;

	/*
	 * Parse the configuration file, if provided. If no configuration file
	 * was provided, or one was but was incomplete, parse_config() will
	 * abort anyway, with an appropriate message.
	 */
	load_config(config_file, verbose, false, &config_file_options, argv[0]);


	/* Some configuration file items can be overriden by command line options */
	/* Command-line parameter -L/--log-level overrides any setting in config file*/
	if (*cli_loglevel != '\0')
	{
		strncpy(config_file_options.loglevel, cli_loglevel, MAXLEN);
	}

	/*
	 * -m/--monitoring-history, if provided, will override repmgr.conf's
	 * monitoring_history; this is for backwards compatibility as it's
	 * possible this may be baked into various startup scripts.
	 */

	if (cli_monitoring_history == true)
	{
		config_file_options.monitoring_history = true;
	}

	logger_init(&config_file_options, progname());

	if (verbose)
		logger_set_verbose();

	log_info(_("connecting to database '%s'"),
			 config_file_options.conninfo);

	local_conn = establish_db_connection(config_file_options.conninfo, true);

	/*
	 * sanity checks
	 *
	 * Note: previous repmgr versions checked the PostgreSQL version at this
	 * point, but we'll skip that and assume the presence of a node record
	 * means we're dealing with a supported installation.
	 *
	 * The absence of a node record will also indicate that either the node
	 * or repmgr has not been properly configured.
	 */

	/* Retrieve record for this node from the local database */
	record_status = get_node_record(local_conn, config_file_options.node_id, &local_node_info);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("no metadata record found for this node - terminating"));
		log_hint(_("Check that 'repmgr (master|standby) register' was executed for this node"));

		PQfinish(local_conn);
		terminate(ERR_BAD_CONFIG);
	}

	log_debug("node id is %i, upstream is %i",
			  local_node_info.node_id,
			  local_node_info.upstream_node_id);

    /*
     * Check if node record is active - if not, and `failover_mode=automatic`, the node
     * won't be considered as a promotion candidate; this often happens when
     * a failed primary is recloned and the node was not re-registered, giving
     * the impression failover capability is there when it's not. In this case
     * abort with an error and a hint about registering.
     *
     * If `failover_mode=manual`, repmgrd can continue to passively monitor the node, but
     * we should nevertheless issue a warning and the same hint.
     */

    if (local_node_info.active == false)
    {
        char *hint = "Check that 'repmgr (master|standby) register' was executed for this node";

        switch (config_file_options.failover_mode)
        {
			/* "failover_mode" is an enum, all values should be covered here */

            case FAILOVER_AUTOMATIC:
                log_error(_("this node is marked as inactive and cannot be used as a failover target"));
                log_hint(_("%s"), hint);
				PQfinish(local_conn);
                terminate(ERR_BAD_CONFIG);

            case FAILOVER_MANUAL:
                log_warning(_("this node is marked as inactive and will be passively monitored only"));
                log_hint(_("%s"), hint);
                break;
        }
    }


	if (daemonize == true)
	{
		daemonize_process();
	}

	if (pid_file != NULL)
	{
		check_and_create_pid_file(pid_file);
	}

#ifndef WIN32
	setup_event_handlers();
#endif



	start_monitoring();

	/* shut down logging system */
	logger_shutdown();

	return SUCCESS;
}


static void
start_monitoring(void)
{

	log_notice(_("starting monitoring of node %s (ID: %i"),
			   local_node_info.node_name,
			   local_node_info.node_id);

	while(true)
	{
		switch (local_node_info.type)
		{
			case MASTER:
				monitor_streaming_master();
				break;
			case STANDBY:
				monitor_streaming_standby();
				break;
			case WITNESS:
				/* not handled */
				return;
			case UNKNOWN:
				/* should never happen */
				break;
		}

	}
}


static void
monitor_streaming_master(void)
{
	while (true)
	{
		sleep(1);
	}
}


static void
monitor_streaming_standby(void)
{
	while (true)
	{
		sleep(1);
	}
}

static void
daemonize_process(void)
{
	char	   *ptr,
				path[MAXPGPATH];
	pid_t		pid = fork();
	int			ret;

	switch (pid)
	{
		case -1:
			log_error(_("error in fork():\n  %s"), strerror(errno));
			exit(ERR_SYS_FAILURE);
			break;

		case 0:
			/* create independent session ID */
			pid = setsid();
			if (pid == (pid_t) -1)
			{
				log_error(_("error in setsid():\n  %s"), strerror(errno));
				exit(ERR_SYS_FAILURE);
			}

			/* ensure that we are no longer able to open a terminal */
			pid = fork();

			/* error case */
			if (pid == -1)
			{
				log_error(_("error in fork():\n  %s"), strerror(errno));
				exit(ERR_SYS_FAILURE);
			}

			/* parent process */
			if (pid != 0)
			{
				exit(0);
			}

			/* child process */

			memset(path, 0, MAXPGPATH);

			for (ptr = config_file + strlen(config_file); ptr > config_file; --ptr)
			{
				if (*ptr == '/')
				{
					strncpy(path, config_file, ptr - config_file);
				}
			}

			if (*path == '\0')
			{
				*path = '/';
			}

			log_info("dir now %s", path);
			ret = chdir(path);
			if (ret != 0)
			{
				log_error(_("error changing directory to '%s':\n  %s"), path,
						  strerror(errno));
			}

			break;

		default:				/* parent process */
			exit(0);
	}
}

static void
check_and_create_pid_file(const char *pid_file)
{
	struct stat st;
	FILE	   *fd;
	char		buff[MAXLEN];
	pid_t		pid;
	size_t		nread;

	if (stat(pid_file, &st) != -1)
	{
		memset(buff, 0, MAXLEN);

		fd = fopen(pid_file, "r");

		if (fd == NULL)
		{
			log_error(_("PID file %s exists but could not opened for reading"), pid_file);
			log_hint(_("if repmgrd is no longer alive, remove the file and restart repmgrd"));
			exit(ERR_BAD_PIDFILE);
		}

		nread = fread(buff, MAXLEN - 1, 1, fd);

		if (nread == 0 && ferror(fd))
		{
			log_error(_("error reading PID file '%s', aborting"), pid_file);
			exit(ERR_BAD_PIDFILE);
		}

		fclose(fd);

		pid = atoi(buff);

		if (pid != 0)
		{
			if (kill(pid, 0) != -1)
			{
				log_error(_("PID file %s exists and seems to contain a valid PID"), pid_file);
				log_hint(_("if repmgrd is no longer alive, remove the file and restart repmgrd"));
				exit(ERR_BAD_PIDFILE);
			}
		}
	}

	fd = fopen(pid_file, "w");
	if (fd == NULL)
	{
		log_error(_("could not open PID file %s"), pid_file);
		exit(ERR_BAD_CONFIG);
	}

	fprintf(fd, "%d", getpid());
	fclose(fd);
}


#ifndef WIN32
static void
handle_sigint(SIGNAL_ARGS)
{
	terminate(SUCCESS);
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
handle_sighup(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}

static void
setup_event_handlers(void)
{
	pqsignal(SIGHUP, handle_sighup);
	pqsignal(SIGINT, handle_sigint);
	pqsignal(SIGTERM, handle_sigint);
}
#endif


void
show_usage(void)
{
	fprintf(stderr, _("%s: replication management daemon for PostgreSQL\n"), progname());
	fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname());
}

void
show_help(void)
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


static void
close_connections()
{
	if (PQstatus(master_conn) == CONNECTION_OK)
	{
		/* cancel any pending queries to the master */
		if (PQisBusy(master_conn) == 1)
			cancel_query(master_conn, config_file_options.master_response_timeout);
		PQfinish(master_conn);
	}

	if (PQstatus(local_conn) == CONNECTION_OK)
		PQfinish(local_conn);
}


static void
terminate(int retval)
{
	close_connections();
	logger_shutdown();

	if (pid_file)
	{
		unlink(pid_file);
	}

	log_info(_("%s terminating...\n"), progname());

	exit(retval);
}
