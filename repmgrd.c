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

static t_node_info local_node_info;
static PGconn	   *local_conn = NULL;

static PGconn	   *master_conn = NULL;

/*
 * Record receipt SIGHUP; will cause configuration file to be reread at the
 * appropriate point in the main loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;

static void show_help(void);
static void show_usage(void);
static void daemonize_process(void);
static void check_and_create_pid_file(const char *pid_file);

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
	bool		monitoring_history = false;

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

	while ((c = getopt_long(argc, argv, "?Vf:vdp:m", long_options, &optindex)) != -1)
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

			case 'v':
				verbose = true;
				break;

			/* legacy options */

			case 'm':
				monitoring_history = true;
				break;

			default:
     unknown_option:
				show_usage();
				exit(ERR_BAD_CONFIG);
		}
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

	/*
	 * -m/--monitoring-history, if provided, will override repmgr.conf's
	 * monitoring_history; this is for backwards compatibility as it's
	 * possible this may be baked into various startup scripts.
	 */

	if (monitoring_history == true)
	{
		config_file_options.monitoring_history = true;
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

	while(1) {
		sleep(1);
	}
	/* shut down logging system */
	logger_shutdown();

	return SUCCESS;
}


static void
daemonize_process(void)
{
	char	   *ptr,
				path[MAXLEN];
	pid_t		pid = fork();
	int			ret;

	switch (pid)
	{
		case -1:
			log_error(_("error in fork():\n  %s"), strerror(errno));
			exit(ERR_SYS_FAILURE);
			break;

		case 0:			/* child process */
			pid = setsid();
			if (pid == (pid_t) -1)
			{
				log_error(_("error in setsid():\n  %s"), strerror(errno));
				exit(ERR_SYS_FAILURE);
			}

			/* ensure that we are no longer able to open a terminal */
			pid = fork();

			if (pid == -1)		/* error case */
			{
				log_error(_("error in fork():\n  %s"), strerror(errno));
				exit(ERR_SYS_FAILURE);
			}

			if (pid != 0)		/* parent process */
			{
				exit(0);
			}

			/* a child just flows along */

			memset(path, 0, MAXLEN);

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
