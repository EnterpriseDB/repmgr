/*
 * repmgrd.c - Replication manager daemon
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>

#include "portability/instr_time.h"

#include "repmgr.h"
#include "config.h"
#include "voting.h"

#define OPT_HELP	1

typedef enum {
	NODE_STATUS_UNKNOWN = -1,
	NODE_STATUS_UP,
	NODE_STATUS_DOWN
} NodeStatus;


typedef enum {
	FAILOVER_STATE_UNKNOWN = -1,
	FAILOVER_STATE_NONE,
	FAILOVER_STATE_PROMOTED,
	FAILOVER_STATE_PROMOTION_FAILED,
	FAILOVER_STATE_PRIMARY_REAPPEARED,
	FAILOVER_STATE_LOCAL_NODE_FAILURE,
	FAILOVER_STATE_WAITING_NEW_PRIMARY,
	FAILOVER_STATE_FOLLOWED_NEW_PRIMARY,
    FAILOVER_STATE_FOLLOWING_ORIGINAL_PRIMARY,
	FAILOVER_STATE_NO_NEW_PRIMARY,
	FAILOVER_STATE_FOLLOW_FAIL,
	FAILOVER_STATE_NODE_NOTIFICATION_ERROR
} FailoverState;


typedef enum {
	ELECTION_NOT_CANDIDATE = -1,
	ELECTION_WON,
	ELECTION_LOST,
	ELECTION_CANCELLED
} ElectionResult;

typedef enum {
	MS_NORMAL = 0,
	MS_DEGRADED = 1
} MonitoringState;

static char	   *config_file = NULL;
static bool		verbose = false;
static char	   *pid_file = NULL;
static bool		daemonize = false;

t_configuration_options config_file_options = T_CONFIGURATION_OPTIONS_INITIALIZER;

static t_node_info local_node_info = T_NODE_INFO_INITIALIZER;
static PGconn	   *local_conn = NULL;

static t_node_info upstream_node_info = T_NODE_INFO_INITIALIZER;
static PGconn *upstream_conn = NULL;
static PGconn *primary_conn = NULL;

FailoverState failover_state = FAILOVER_STATE_UNKNOWN;

static NodeInfoList standby_nodes = T_NODE_INFO_LIST_INITIALIZER;

/* Collate command line errors here for friendlier reporting */
static ItemList	cli_errors = { NULL, NULL };

static bool        startup_event_logged = false;

static MonitoringState monitoring_state = MS_NORMAL;
static instr_time	degraded_monitoring_start;

/*
 * Record receipt of SIGHUP; will cause configuration file to be reread
 * at the appropriate point in the main loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;

static void show_help(void);
static void show_usage(void);
static void daemonize_process(void);
static void check_and_create_pid_file(const char *pid_file);

static void start_monitoring(void);
static void monitor_streaming_primary(void);
static void monitor_streaming_standby(void);
static void monitor_bdr(void);


#ifndef WIN32
static void setup_event_handlers(void);
static void handle_sighup(SIGNAL_ARGS);
static void handle_sigint(SIGNAL_ARGS);
#endif

static PGconn *try_reconnect(const char *conninfo, NodeStatus *node_status);

static bool do_primary_failover(void);
static bool do_upstream_standby_failover(void);

static ElectionResult do_election(void);
static const char *_print_voting_status(NodeVotingStatus voting_status);
static const char *_print_election_result(ElectionResult result);
static const char *_print_monitoring_state(MonitoringState monitoring_state);

static FailoverState promote_self(void);
static void notify_followers(NodeInfoList *standby_nodes, int follow_node_id);

static t_node_info *poll_best_candidate(NodeInfoList *standby_nodes);

static bool wait_primary_notification(int *new_primary_id);
static FailoverState follow_new_primary(int new_primary_id);

static void reset_node_voting_status(void);

static int calculate_elapsed(instr_time start_time);


static void close_connections();
static void terminate(int retval);

int
main(int argc, char **argv)
{
	int			optindex;
	int			c;
	char		cli_log_level[MAXLEN] = "";
	bool		cli_monitoring_history = false;

	RecordStatus record_status;

	FILE	   *fd;

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

    srand ( time(NULL) );

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
				int detected_cli_log_level = detect_log_level(optarg);
				if (detected_cli_log_level != -1)
				{
					strncpy(cli_log_level, optarg, MAXLEN);
				}
				else
				{
					PQExpBufferData invalid_log_level;
					initPQExpBuffer(&invalid_log_level);
					appendPQExpBuffer(&invalid_log_level,
									  _("invalid log level \"%s\" provided"),
									  optarg);
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

	startup_event_logged = false;
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
	if (*cli_log_level != '\0')
	{
		strncpy(config_file_options.log_level, cli_log_level, MAXLEN);
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


	fd = freopen("/dev/null", "r", stdin);
	if (fd == NULL)
	{
		fprintf(stderr, "error reopening stdin to \"/dev/null\":\n  %s\n",
				strerror(errno));
	}

	fd = freopen("/dev/null", "w", stdout);
	if (fd == NULL)
	{
		fprintf(stderr, "error reopening stdout to \"/dev/null\":\n  %s\n",
				strerror(errno));
	}

	logger_init(&config_file_options, progname());

	if (verbose)
		logger_set_verbose();

	if (log_type == REPMGR_SYSLOG)
	{
		fd = freopen("/dev/null", "w", stderr);

		if (fd == NULL)
		{
			fprintf(stderr, "error reopening stderr to \"/dev/null\":\n  %s\n",
					strerror(errno));
		}
	}


	log_info(_("connecting to database \"%s\""),
			 config_file_options.conninfo);

	/* abort if local node not available at startup */
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
		log_hint(_("check that 'repmgr (primary|standby) register' was executed for this node"));

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
        char *hint = "Check that 'repmgr (primary|standby) register' was executed for this node";

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

	if (config_file_options.failover_mode == FAILOVER_AUTOMATIC)
	{
		/*
		 * check that promote/follow commands are defined, otherwise repmgrd
		 * won't be able to perform any useful action
		 */

		bool required_param_missing = false;

		if (config_file_options.promote_command[0] == '\0'
			&& config_file_options.service_promote_command[0] == '\0')
		{
			log_error(_("either \"promote_command\" or \"service_promote_command\" must be defined in the configuration file"));
			required_param_missing = true;
		}
		if (config_file_options.follow_command[0] == '\0')
		{
			log_error(_("\"follow_command\" must be defined in the configuration file"));
			required_param_missing = true;
		}

		if (required_param_missing == true)
		{
			log_hint(_("add the missing configuration parameter(s) and start repmgrd again"));
			PQfinish(local_conn);
			exit(ERR_BAD_CONFIG);
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

	logger_shutdown();

	return SUCCESS;
}


static void
start_monitoring(void)
{
	log_notice(_("starting monitoring of node \"%s\" (ID: %i)"),
			   local_node_info.node_name,
			   local_node_info.node_id);

	while(true)
	{
		reset_node_voting_status();

		switch (local_node_info.type)
		{
			case PRIMARY:
				monitor_streaming_primary();
				break;
			case STANDBY:
				monitor_streaming_standby();
				break;
			case BDR:
				monitor_bdr();
				return;
			case UNKNOWN:
				/* should never happen */
				break;
		}
	}
}


static void
monitor_streaming_primary(void)
{
	NodeStatus	node_status = NODE_STATUS_UP;
	instr_time	log_status_interval_start;
	PQExpBufferData event_details;

	/* Log startup event */
	if (startup_event_logged == false)
	{
		initPQExpBuffer(&event_details);

		appendPQExpBuffer(&event_details,
						  _("monitoring cluster primary \"%s\" (node ID: %i)"),
						  local_node_info.node_name,
						  local_node_info.node_id);

		create_event_notification(local_conn,
								  &config_file_options,
								  config_file_options.node_id,
								  "repmgrd_start",
								  true,
								  event_details.data);

		startup_event_logged = true;

		log_notice("%s", event_details.data);

		termPQExpBuffer(&event_details);
	}

	INSTR_TIME_SET_CURRENT(log_status_interval_start);

	while (true)
	{

		// cache node list here, refresh at `node_list_refresh_interval`
		// also return reason for inavailability so we can log it
		if (is_server_available(local_node_info.conninfo) == false)
		{

			/* node is down, we were expecting it to be up */
			if (node_status == NODE_STATUS_UP)
			{
				PQExpBufferData event_details;
				instr_time	local_node_unreachable_start;

				INSTR_TIME_SET_CURRENT(local_node_unreachable_start);

				initPQExpBuffer(&event_details);

				appendPQExpBuffer(&event_details,
								  _("unable to connect to local node"));

				log_warning("%s", event_details.data);

				node_status = NODE_STATUS_UNKNOWN;

				PQfinish(local_conn);

				/*
				 * as we're monitoring the primary, no point in trying to write
				 * the event to the database
				 *
				 * XXX possible pre-action event
				 */
				create_event_notification(NULL,
										  &config_file_options,
										  config_file_options.node_id,
										  "repmgrd_local_disconnect",
										  true,
										  event_details.data);

				termPQExpBuffer(&event_details);

				local_conn = try_reconnect(local_node_info.conninfo, &node_status);

				if (node_status == NODE_STATUS_UP)
				{
					int		local_node_unreachable_elapsed = calculate_elapsed(local_node_unreachable_start);

					initPQExpBuffer(&event_details);

					appendPQExpBuffer(&event_details,
									  _("reconnected to local node after %i seconds"),
									  local_node_unreachable_elapsed);
					log_notice("%s", event_details.data);

					create_event_notification(local_conn,
										&config_file_options,
										config_file_options.node_id,
										"repmgrd_local_reconnect",
										true,
										event_details.data);
					termPQExpBuffer(&event_details);

					goto loop;
				}

				monitoring_state = MS_DEGRADED;
				INSTR_TIME_SET_CURRENT(degraded_monitoring_start);
			}

		}


		if (monitoring_state == MS_DEGRADED)
		{
			int		degraded_monitoring_elapsed = calculate_elapsed(degraded_monitoring_start);

			if (config_file_options.degraded_monitoring_timeout > 0
				&& degraded_monitoring_elapsed > config_file_options.degraded_monitoring_timeout)
			{
				initPQExpBuffer(&event_details);

				appendPQExpBuffer(&event_details,
								  _("degraded monitoring timeout (%i seconds) exceeded, terminating"),
								  degraded_monitoring_elapsed);

				log_notice("%s", event_details.data);

				create_event_notification(NULL,
										  &config_file_options,
										  config_file_options.node_id,
										  "repmgrd_terminate",
										  true,
										  event_details.data);

				termPQExpBuffer(&event_details);
				terminate(ERR_MONITORING_TIMEOUT);
			}

			log_debug("monitoring node in degraded state for %i seconds", degraded_monitoring_elapsed);

			if (is_server_available(local_node_info.conninfo) == true)
			{
				local_conn = establish_db_connection(local_node_info.conninfo, false);

				if (PQstatus(local_conn) == CONNECTION_OK)
				{
					node_status = NODE_STATUS_UP;
					monitoring_state = MS_NORMAL;

					initPQExpBuffer(&event_details);

					appendPQExpBuffer(&event_details,
									  _("reconnected to primary node after %i seconds, resuming monitoring"),
									  degraded_monitoring_elapsed);

					create_event_notification(local_conn,
											  &config_file_options,
											  config_file_options.node_id,
											  "repmgrd_local_reconnect",
											  true,
											  event_details.data);

					log_notice("%s", event_details.data);
					termPQExpBuffer(&event_details);

					goto loop;
				}
			}


			// possibly attempt to find another node from cached list
			// check if there's a new primary - if so add hook for fencing?
			// loop, if starts up check status, switch monitoring mode
		}
	loop:
		/* emit "still alive" log message at regular intervals, if requested */
		if (config_file_options.log_status_interval > 0)
		{
			int		log_status_interval_elapsed = calculate_elapsed(log_status_interval_start);

			if (log_status_interval_elapsed >= config_file_options.log_status_interval)
			{
				log_info(_("monitoring primary node \"%s\" (node ID: %i) in %s state"),
						 local_node_info.node_name,
						 local_node_info.node_id,
						 _print_monitoring_state(monitoring_state));

				if (monitoring_state == MS_DEGRADED)
				{
					log_detail(_("waiting primary to reappear"));
				}

				INSTR_TIME_SET_CURRENT(log_status_interval_start);
			}
		}
		sleep(1);
	}
}


static void
monitor_streaming_standby(void)
{
	RecordStatus record_status;
	NodeStatus	upstream_node_status = NODE_STATUS_UP;
	instr_time	log_status_interval_start;
	PQExpBufferData event_details;

	log_debug("monitor_streaming_standby()");

	/*
	 * If no upstream node id is specified in the metadata, we'll try
	 * and determine the current cluster primary in the assumption we
	 * should connect to that by default.
	 */
	if (local_node_info.upstream_node_id == UNKNOWN_NODE_ID)
	{
		local_node_info.upstream_node_id = get_primary_node_id(local_conn);

		/*
		 * Terminate if there doesn't appear to be an active cluster primary.
		 * There could be one or more nodes marked as inactive primaries, and one
		 * of them could actually be a primary, but we can't sensibly monitor
		 * in that state.
		 */
		if (local_node_info.upstream_node_id == NODE_NOT_FOUND)
		{
			// XXX check if there's an inactive record(s) and log detail/hint
			log_error(_("unable to determine an active primary for this cluster, terminating"));
			PQfinish(local_conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	record_status = get_node_record(local_conn, local_node_info.upstream_node_id, &upstream_node_info);

	/*
	 * Terminate if we can't find the record for the node we're supposed
	 * to monitor. This is a "fix-the-config" situation, not a lot else we
	 * can do.
	 */
	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve record for upstream node (ID: %i), terminating"),
					local_node_info.upstream_node_id);
		PQfinish(local_conn);
		exit(ERR_DB_CONN);
	}

	log_debug("connecting to upstream node %i: \"%s\"", upstream_node_info.node_id, upstream_node_info.conninfo);

	upstream_conn = establish_db_connection(upstream_node_info.conninfo, false);

	/*
	 * Upstream node must be running.
	 *
	 * We could possibly have repmgrd skip to degraded monitoring mode until it
	 * comes up, but there doesn't seem to be much point in doint that.
	 */
	if (PQstatus(upstream_conn) != CONNECTION_OK)
	{
		log_error(_("unable connect to upstream node (ID: %i), terminating"),
				  local_node_info.upstream_node_id);
		PQfinish(local_conn);
		exit(ERR_DB_CONN);
	}

	/* refresh upstream node record from upstream node, so it's as up-to-date as possible */
	record_status = get_node_record(upstream_conn, upstream_node_info.node_id, &upstream_node_info);

	if (upstream_node_info.type == STANDBY)
	{
		/*
		 * Currently cascaded standbys need to be able to connect to the primary.
		 * We could possibly add a limited connection mode for cases where this isn't
		 * possible.
		 */
		primary_conn = establish_primary_db_connection(upstream_conn, false);

		if (PQstatus(primary_conn) != CONNECTION_OK)
		{
			log_error(_("unable to connect to primary node"));
			log_hint(_("ensure the primary node is reachable from this node"));
			exit(ERR_DB_CONN);
		}

		log_verbose(LOG_DEBUG, "connected to primary");
	}
	else
	{
		primary_conn = upstream_conn;
	}

	/* Log startup event */
	if (startup_event_logged == false)
	{
		PQExpBufferData event_details;
		initPQExpBuffer(&event_details);

		appendPQExpBuffer(&event_details,
						  _("monitoring upstream node \"%s\" (node ID: %i)"),
						  upstream_node_info.node_name,
						  upstream_node_info.node_id);

		create_event_notification(primary_conn,
								  &config_file_options,
								  config_file_options.node_id,
								  "repmgrd_start",
								  true,
								  event_details.data);

		startup_event_logged = true;

		log_notice("%s", event_details.data);

		termPQExpBuffer(&event_details);
	}

	monitoring_state = MS_NORMAL;
	INSTR_TIME_SET_CURRENT(log_status_interval_start);

	while (true)
	{
		if (is_server_available(upstream_node_info.conninfo) == false)
		{

			/* upstream node is down, we were expecting it to be up */
			if (upstream_node_status == NODE_STATUS_UP)
			{
				instr_time	upstream_node_unreachable_start;

				INSTR_TIME_SET_CURRENT(upstream_node_unreachable_start);

				initPQExpBuffer(&event_details);

				upstream_node_status = NODE_STATUS_UNKNOWN;

				appendPQExpBuffer(&event_details,
								  _("unable to connect to upstream node \"%s\" (node ID: %i)"),
								  upstream_node_info.node_name, upstream_node_info.node_id);

				if (upstream_node_info.type == STANDBY)
				{
					/* XXX possible pre-action event */
					create_event_record(primary_conn,
										&config_file_options,
										config_file_options.node_id,
										"repmgrd_upstream_disconnect",
										true,
										event_details.data);
				}

				log_warning("%s", event_details.data);
				termPQExpBuffer(&event_details);

				PQfinish(upstream_conn);
				upstream_conn = try_reconnect(upstream_node_info.conninfo, &upstream_node_status);

				if (upstream_node_status == NODE_STATUS_UP)
				{
					int		upstream_node_unreachable_elapsed = calculate_elapsed(upstream_node_unreachable_start);

					initPQExpBuffer(&event_details);

					appendPQExpBuffer(&event_details,
									  _("reconnected to upstream node after %i seconds"),
									  upstream_node_unreachable_elapsed);
					log_notice("%s", event_details.data);

					create_event_notification(local_conn,
											  &config_file_options,
											  config_file_options.node_id,
											  "repmgrd_upstream_reconnect",
											  true,
											  event_details.data);
					termPQExpBuffer(&event_details);

					goto loop;
				}

				/* still down after reconnect attempt(s) */
				if (upstream_node_status == NODE_STATUS_DOWN)
				{
					bool failover_done = false;

					if (upstream_node_info.type == PRIMARY)
					{
						failover_done = do_primary_failover();
					}
					else if (upstream_node_info.type == STANDBY)
					{
						failover_done = do_upstream_standby_failover();
					}

					// it's possible it will make sense to return in
					// all cases to restart monitoring
					if (failover_done == true)
						return;
				}
			}
		}

		if (monitoring_state == MS_DEGRADED)
		{
			int		degraded_monitoring_elapsed = calculate_elapsed(degraded_monitoring_start);

			log_debug("monitoring node %i in degraded state for %i seconds",
					  upstream_node_info.node_id,
					  degraded_monitoring_elapsed);

			if (is_server_available(upstream_node_info.conninfo) == true)
			{
				upstream_conn = establish_db_connection(upstream_node_info.conninfo, false);

				if (PQstatus(upstream_conn) == CONNECTION_OK)
				{
					// XXX check here if upstream is still primary
					// -> will be a problem if another node was promoted in the meantime
					// and upstream is now former primary
					// XXX scan other nodes to see if any has become primary

					upstream_node_status = NODE_STATUS_UP;
					monitoring_state = MS_NORMAL;

					if (upstream_node_info.type == PRIMARY)
					{
						primary_conn = upstream_conn;
					}
					else
					{

						if (primary_conn == NULL || PQstatus(primary_conn) != CONNECTION_OK)
						{
							primary_conn = establish_primary_db_connection(upstream_conn, false);
						}
					}

					initPQExpBuffer(&event_details);

					appendPQExpBuffer(&event_details,
									  _("reconnected to upstream node %i after %i seconds, resuming monitoring"),
									  upstream_node_info.node_id,
									  degraded_monitoring_elapsed);

					create_event_notification(primary_conn,
											  &config_file_options,
											  config_file_options.node_id,
											  "repmgrd_upstream_reconnect",
											  true,
											  event_details.data);

					log_notice("%s", event_details.data);
					termPQExpBuffer(&event_details);

					goto loop;
				}
			}
			else
			{
				// unable to connect to former primary - check if another node has
				// been promoted
			}

		}

	loop:

		/* emit "still alive" log message at regular intervals, if requested */
		if (config_file_options.log_status_interval > 0)
		{
			int		log_status_interval_elapsed = calculate_elapsed(log_status_interval_start);

			if (log_status_interval_elapsed >= config_file_options.log_status_interval)
			{
				log_info(_("node \"%s\" (node ID: %i) monitoring upstream node \"%s\" (node ID: %i) in %s state"),
						 local_node_info.node_name,
						 local_node_info.node_id,
						 upstream_node_info.node_name,
						 upstream_node_info.node_id,
						 _print_monitoring_state(monitoring_state));

				if (monitoring_state == MS_DEGRADED)
				{
					log_detail(_("waiting for upstream or another primary to reappear"));
				}

				INSTR_TIME_SET_CURRENT(log_status_interval_start);
			}
		}

		/*
		 * handle local node failure
		 *
		 * currently we'll just check the connection, and try to reconnect
		 *
		 * TODO: add timeout, after which we run in degraded state
		 */
		if (is_server_available(local_node_info.conninfo) == false)
		{
			log_warning(_("connection to local node %i lost"), local_node_info.node_id);

			if (local_conn != NULL)
			{
				PQfinish(local_conn);
				local_conn = NULL;
			}
		}

		if (PQstatus(local_conn) != CONNECTION_OK)
		{
			log_info(_("attempting to reconnect"));
			local_conn = establish_db_connection(config_file_options.conninfo, false);

			if (PQstatus(local_conn) != CONNECTION_OK)
			{
				log_warning(_("reconnection failed"));
			}
			else
			{
				log_info(_("reconnected"));
			}
		}
		sleep(1);
	}
}


static bool
do_primary_failover(void)
{
	/* attempt to initiate voting process */
	ElectionResult election_result = do_election();

	/* XXX add pre-event notification here */
	failover_state = FAILOVER_STATE_UNKNOWN;

	log_debug("election result: %s", _print_election_result(election_result));

	if (election_result == ELECTION_CANCELLED)
	{
		log_notice(_("election cancelled"));
		return false;
	}
	else if (election_result == ELECTION_WON)
	{
		log_notice("I am the winner, will now promote self and inform other nodes");

		failover_state = promote_self();
	}
	else if (election_result == ELECTION_LOST)
	{
		t_node_info *best_candidate;

		log_info(_("I am the candidate but did not get all votes; will now determine the best candidate"));


		/* reset node list */
		get_active_sibling_node_records(local_conn,
										local_node_info.node_id,
										upstream_node_info.node_id,
										&standby_nodes);

		best_candidate = poll_best_candidate(&standby_nodes);

		/*
		 * this can occur in a tie-break situation, where this node establishes
		 * it is the best candidate
		 */
		if (best_candidate->node_id == local_node_info.node_id)
		{
			log_notice("I am the best candidate, will now promote self and inform other nodes");

			failover_state = promote_self();
		}
		else
		{
			PGconn *candidate_conn = NULL;

			log_info("node %i is the best candidate, waiting for it to confirm so I can follow it",
					 best_candidate->node_id);

			/* notify the best candidate so it */

			candidate_conn = establish_db_connection(best_candidate->conninfo, false);

			if (PQstatus(candidate_conn) == CONNECTION_OK)
			{
				notify_follow_primary(candidate_conn, best_candidate->node_id);

				/*  we'll wait for the candidate to get back to us */
				failover_state = FAILOVER_STATE_WAITING_NEW_PRIMARY;
			}
			else
			{
				log_error(_("unable to connect to candidate node (ID: %i)"), best_candidate->node_id);
				failover_state = FAILOVER_STATE_NODE_NOTIFICATION_ERROR;
			}
			PQfinish(candidate_conn);
		}
	}
	else
	{
		log_info(_("follower node awaiting notification from the candidate node"));
		failover_state = FAILOVER_STATE_WAITING_NEW_PRIMARY;
	}


	/*
	 * node has decided it is a follower, so will await notification
	 * from the candidate that it has promoted itself and can be followed
	 */
	if (failover_state == FAILOVER_STATE_WAITING_NEW_PRIMARY)
	{
		int new_primary_id;

		//   --> need timeout in case new primary doesn't come up, then rerun election

		/* either follow or time out; either way resume monitoring */
		if (wait_primary_notification(&new_primary_id) == true)
		{
			/* if primary has reappeared, no action needed */
			if (new_primary_id == upstream_node_info.node_id)
			{
				failover_state = FAILOVER_STATE_FOLLOWING_ORIGINAL_PRIMARY;
			}
			/* if new_primary_id is self, promote */
			else if (new_primary_id == local_node_info.node_id)
			{
				log_notice(_("this node is promotion candidate, promoting"));

				failover_state = promote_self();

				get_active_sibling_node_records(local_conn,
												local_node_info.node_id,
												upstream_node_info.node_id,
												&standby_nodes);

			}
			else
			{
				failover_state = follow_new_primary(new_primary_id);
			}
		}
		else
		{
			failover_state = FAILOVER_STATE_NO_NEW_PRIMARY;
		}
	}

	switch(failover_state)
	{
		case FAILOVER_STATE_PROMOTED:
			log_debug("failover state is PROMOTED");

			/* notify former siblings that they should now follow this node */
			notify_followers(&standby_nodes, local_node_info.node_id);

			/* we no longer care about our former siblings */
			clear_node_info_list(&standby_nodes);

			/* pass control back down to start_monitoring() */
			log_info(_("switching to primary monitoring mode"));

			failover_state = FAILOVER_STATE_NONE;
			return true;

		case FAILOVER_STATE_PRIMARY_REAPPEARED:
			log_debug("failover state is PRIMARY_REAPPEARED");

			/* notify siblings that they should resume following the original primary */
			notify_followers(&standby_nodes, upstream_node_info.node_id);

			/* we no longer care about our former siblings */
			clear_node_info_list(&standby_nodes);

			/* pass control back down to start_monitoring() */
			log_info(_("resuming standby monitoring mode"));
			log_detail(_("original primary \"%s\" (node ID: %i) reappeared"),
					   upstream_node_info.node_name, upstream_node_info.node_id);

			failover_state = FAILOVER_STATE_NONE;
			return true;


		case FAILOVER_STATE_FOLLOWED_NEW_PRIMARY:
			log_info(_("resuming standby monitoring mode"));
			log_detail(_("following new primary \"%s\" (node id: %i)"),
					   upstream_node_info.node_name, upstream_node_info.node_id);
			failover_state = FAILOVER_STATE_NONE;

			return true;

		case FAILOVER_STATE_FOLLOWING_ORIGINAL_PRIMARY:
			log_info(_("resuming standby monitoring mode"));
			log_detail(_("following original primary \"%s\" (node id: %i)"),
					   upstream_node_info.node_name, upstream_node_info.node_id);
			failover_state = FAILOVER_STATE_NONE;

			return true;

		case FAILOVER_STATE_PROMOTION_FAILED:
			log_debug("failover state is PROMOTION FAILED");
			return false;

		case FAILOVER_STATE_FOLLOW_FAIL:
			/*
			 * for whatever reason we were unable to follow the new primary -
			 * continue monitoring in degraded state
			 */
			monitoring_state = MS_DEGRADED;
			INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

			return false;

		case FAILOVER_STATE_NO_NEW_PRIMARY:
		case FAILOVER_STATE_WAITING_NEW_PRIMARY:
			/* pass control back down to start_monitoring() */
			// -> should kick off new election
			return false;

		case FAILOVER_STATE_NODE_NOTIFICATION_ERROR:
		case FAILOVER_STATE_LOCAL_NODE_FAILURE:
		case FAILOVER_STATE_UNKNOWN:
		case FAILOVER_STATE_NONE:
			log_debug("failover state is %i", failover_state);
			return false;
	}

	// should never reach here
	return false;
}

/*
 * do_upstream_standby_failover()
 *
 * Attach cascaded standby to primary
 *
 * Currently we will try to attach to the cluster primary, as "repmgr
 * standby follow" doesn't support attaching to another node.
 *
 * If this becomes supported, it might be worth providing a selection
 * of reconnection strategies as different behaviour might be desirable
 * in different situations;
 * or maybe the option not to reconnect might be required?
 *
 * XXX check this handles replication slots gracefully
 */
static bool
do_upstream_standby_failover(void)
{
	PQExpBufferData event_details;
	t_node_info primary_node_info = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status;
	int r;

	PQfinish(upstream_conn);
	upstream_conn = NULL;

	record_status = get_primary_node_record(local_conn, &primary_node_info);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve primary node record"));
		return false;
	}
	/*
	 * Verify that we can still talk to the cluster primary, even though
	 * the node's upstream is not available
	 */

	// consolidate below code
	if (is_server_available(primary_node_info.conninfo) == false)
	{
		log_warning(_("connection to primary %i lost"), primary_node_info.node_id);

		if (primary_conn != NULL)
		{
			PQfinish(primary_conn);
			primary_conn = NULL;
		}
	}

	if (PQstatus(primary_conn) != CONNECTION_OK)
	{
		log_info(_("attempting to reconnect"));
		primary_conn = establish_db_connection(primary_node_info.conninfo, false);

		if (PQstatus(primary_conn) != CONNECTION_OK)
		{
			log_warning(_("reconnection failed"));
		}
		else
		{
			log_info(_("reconnected"));
		}
	}

	/* grandparent upstream is inactive  */
	if (primary_node_info.active == false)
	{
		// XXX
	}

	/* Close the connection to this server */
	PQfinish(local_conn);
	local_conn = NULL;

	initPQExpBuffer(&event_details);

	log_debug(_("standby follow command is:\n  \"%s\""),
			  config_file_options.follow_command);

	r = system(config_file_options.follow_command);

	if (r != 0)
	{
		appendPQExpBuffer(&event_details,
						  _("unable to execute follow command:\n %s"),
						  config_file_options.follow_command);

		log_error("%s", event_details.data);

		/* It may not possible to write to the event notification
		 * table but we should be able to generate an external notification
		 * if required.
		 */
		create_event_notification(primary_conn,
							&config_file_options,
							local_node_info.node_id,
							"repmgrd_failover_follow",
							false,
							event_details.data);

		termPQExpBuffer(&event_details);
	}

	/* reconnect to local node */
	local_conn = establish_db_connection(config_file_options.conninfo, false);

	if (update_node_record_set_upstream(primary_conn,
										local_node_info.node_id,
										primary_node_info.node_id) == false)
	{
		appendPQExpBuffer(&event_details,
						  _("unable to set node %i's new upstream ID to %i"),
						  local_node_info.node_id,
						  primary_node_info.node_id);

		log_error("%s", event_details.data);

		create_event_notification(NULL,
							&config_file_options,
							local_node_info.node_id,
							"repmgrd_failover_follow",
							false,
							event_details.data);

		termPQExpBuffer(&event_details);

		terminate(ERR_BAD_CONFIG);
	}
	/* update own internal node record */
    record_status = get_node_record(primary_conn, local_node_info.node_id, &local_node_info);


	appendPQExpBuffer(&event_details,
					  _("node %i is now following primary node %i"),
					  local_node_info.node_id,
					  primary_node_info.node_id);

	log_notice("%s", event_details.data);

	create_event_notification(primary_conn,
							  &config_file_options,
							  local_node_info.node_id,
							  "repmgrd_failover_follow",
							  true,
							  event_details.data);

	termPQExpBuffer(&event_details);


	PQfinish(primary_conn);
	primary_conn = NULL;


	return true;
}


static FailoverState
promote_self(void)
{
	PQExpBufferData event_details;
	char *promote_command;
	int r;

	/* Store details of the failed node here */
	t_node_info failed_primary = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status;

	/*
	 * optionally add a delay before promoting the standby; this is mainly
	 * useful for testing (e.g. for reappearance of the original primary)
	 * and is not documented.
	 */
	if (config_file_options.promote_delay > 0)
	{
		log_debug("sleeping %i seconds before promoting standby",
				  config_file_options.promote_delay);
		sleep(config_file_options.promote_delay);
	}

	record_status = get_node_record(local_conn, local_node_info.upstream_node_id, &failed_primary);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve metadata record for failed upstream (ID: %i)"),
				  local_node_info.upstream_node_id);
		return FAILOVER_STATE_PROMOTION_FAILED;
	}

	/* the presence of either of these commands has been established already */
	if (config_file_options.service_promote_command[0] != '\0')
		promote_command = config_file_options.service_promote_command;
	else
		promote_command = config_file_options.promote_command;

	log_debug("promote command is:\n  \"%s\"",
			  promote_command);

	if (log_type == REPMGR_STDERR && *config_file_options.log_file)
	{
		fflush(stderr);
	}

	r = system(promote_command);

	/* connection should stay up, but check just in case */
	if(PQstatus(local_conn) != CONNECTION_OK)
	{
		local_conn = establish_db_connection(local_node_info.conninfo, true);

		/* assume node failed */
		if(PQstatus(local_conn) != CONNECTION_OK)
		{
			log_error(_("unable to reconnect to local node"));
			// XXX handle this
			return FAILOVER_STATE_LOCAL_NODE_FAILURE;
		}
	}

	if (r != 0)
	{
		int primary_node_id;

		upstream_conn = get_primary_connection(local_conn,
											  &primary_node_id, NULL);

		if (PQstatus(upstream_conn) == CONNECTION_OK && primary_node_id == failed_primary.node_id)
		{
			log_notice(_("original primary (id: %i) reappeared before this standby was promoted - no action taken"),
					   failed_primary.node_id);

			initPQExpBuffer(&event_details);
			appendPQExpBuffer(&event_details,
							  _("original primary \"%s\" (node ID: %i) reappeared"),
							  failed_primary.node_name,
							  failed_primary.node_id);

			create_event_notification(upstream_conn,
								&config_file_options,
								local_node_info.node_id,
								"repmgrd_failover_abort",
								true,
								event_details.data);

			termPQExpBuffer(&event_details);

			//primary_conn = NULL;

			// XXX handle this!
			// -> we'll need to let the other nodes know too....
			/* no failover occurred but we'll want to restart connections */
			//failover_done = true;
			return FAILOVER_STATE_PRIMARY_REAPPEARED;
		}

		// handle this
		//  -> check if somehow primary; otherwise go for new election?
		log_error(_("promote command failed"));
		return FAILOVER_STATE_PROMOTION_FAILED;
	}


	initPQExpBuffer(&event_details);

	/* update own internal node record */
	record_status = get_node_record(local_conn, local_node_info.node_id, &local_node_info);

	/*
	 * XXX here we're assuming the promote command updated metadata
	 */
	appendPQExpBuffer(&event_details,
					  _("node %i promoted to primary; old primary %i marked as failed"),
					  local_node_info.node_id,
					  failed_primary.node_id);

	/* local_conn is now the primary connection */
	create_event_notification(local_conn,
						&config_file_options,
						local_node_info.node_id,
						"repmgrd_failover_promote",
						true,
						event_details.data);

	termPQExpBuffer(&event_details);

	return FAILOVER_STATE_PROMOTED;
}


/*
 * Notify follower nodes about which node to follow. Normally this
 * will be the current node, however if the original primary reappeared
 * before this node could be promoted, we'll inform the followers they
 * should resume monitoring the original primary.
 */
static void
notify_followers(NodeInfoList *standby_nodes, int follow_node_id)
{
	NodeInfoListCell *cell;

	log_debug("notify_followers()");
	for (cell = standby_nodes->head; cell; cell = cell->next)
	{
		log_debug("intending to notify node %i... ", cell->node_info->node_id);
		if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
		{
			log_debug("reconnecting to node %i... ", cell->node_info->node_id);

			cell->node_info->conn = establish_db_connection(cell->node_info->conninfo, false);
		}

		if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
		{
			log_debug("unable to reconnect to  %i ... ", cell->node_info->node_id);

			continue;
		}

		log_debug("notifying node %i to follow node %i",
				  cell->node_info->node_id, follow_node_id);
		notify_follow_primary(cell->node_info->conn, follow_node_id);
	}
}


static t_node_info *
poll_best_candidate(NodeInfoList *standby_nodes)
{
	NodeInfoListCell *cell;
	t_node_info *best_candidate = &local_node_info;

	// XXX ensure standby_nodes is set correctly

	/*
	 * we need to definitively decide the best candidate, as in some corner
	 * cases we could end up with two candidate nodes, so they should each
	 * come to the same conclusion
	 */
	for (cell = standby_nodes->head; cell; cell = cell->next)
	{
		if (cell->node_info->last_wal_receive_lsn > best_candidate->last_wal_receive_lsn)
		{
			log_debug("node %i has higher LSN, now best candidate", cell->node_info->node_id);
			best_candidate = cell->node_info;
		}
		else if (cell->node_info->last_wal_receive_lsn == best_candidate->last_wal_receive_lsn)
		{
			if (cell->node_info->priority > best_candidate->priority)
			{
				log_debug("node %i has higher priority, now best candidate", cell->node_info->node_id);
				best_candidate = cell->node_info;
			}
		}
		/* if all else fails, we decide by node_id */
		else if (cell->node_info->node_id < best_candidate->node_id)
		{
			log_debug("node %i has lower node_id, now best candidate", cell->node_info->node_id);
			best_candidate = cell->node_info;
		}
	}

	log_info(_("best candidate is %i"), best_candidate->node_id);

	return best_candidate;
}


static bool
wait_primary_notification(int *new_primary_id)
{
	// XXX make this configurable
	int wait_primary_timeout = 60;
	int i;

	for (i = 0; i < wait_primary_timeout; i++)
	{
		if (get_new_primary(local_conn, new_primary_id) == true)
		{
			log_debug("new primary is %i; elapsed: %i",
					  *new_primary_id, i);
			return true;
		}
		sleep(1);
	}


	log_warning(_("no notifcation received from new primary after %i seconds"),
				wait_primary_timeout);

	return false;
}


static FailoverState
follow_new_primary(int new_primary_id)
{
	PQExpBufferData event_details;
	int r;

	/* Store details of the failed node here */
	t_node_info failed_primary = T_NODE_INFO_INITIALIZER;
	t_node_info new_primary = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status;
	bool new_primary_ok = false;

	record_status = get_node_record(local_conn, new_primary_id, &new_primary);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve metadata record for upstream node (ID: %i)"),
				  new_primary_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	record_status = get_node_record(local_conn, local_node_info.upstream_node_id, &failed_primary);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve metadata record for failed primary (ID: %i)"),
					local_node_info.upstream_node_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	// XXX check if new_primary_id == failed_primary.node_id?

	if (log_type == REPMGR_STDERR && *config_file_options.log_file)
	{
		fflush(stderr);
	}

	log_debug(_("standby follow command is:\n  \"%s\""),
			  config_file_options.follow_command);

	upstream_conn = establish_db_connection(new_primary.conninfo, false);

	if (PQstatus(upstream_conn) == CONNECTION_OK)
	{
		RecoveryType primary_recovery_type = get_recovery_type(upstream_conn);
		if (primary_recovery_type == RECTYPE_PRIMARY)
		{
			new_primary_ok = true;
		}
		else
		{
			log_warning(_("new primary is not in recovery"));
			PQfinish(upstream_conn);
		}
	}

	if (new_primary_ok == false)
	{
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	/*
	 * disconnect from local node, as follow operation will result in
	 * a server restart
	 */

	PQfinish(local_conn);
	local_conn = NULL;

	/* execute the follow command */
	r = system(config_file_options.follow_command);

	if (r != 0)
	{
		PGconn *old_primary_conn;
		/*
		 * The follow action could still fail due to the original primary reappearing
		 * before the candidate could promote itself ("repmgr standby follow" will
		 * refuse to promote another node if the primary is available). However
		 * the new primary will only instruct use to follow it after it's successfully
		 * promoted itself, so that very likely won't be the reason for the failure.
		 *
		 *
		 * TODO: check the new primary too - we could have a split-brain
		 * situation where the old primary reappeared just after the new
		 * one promoted itself.
		 */
		old_primary_conn = establish_db_connection(failed_primary.conninfo, false);

		if (PQstatus(old_primary_conn) == CONNECTION_OK)
		{
			// XXX add event notifications
			RecoveryType upstream_recovery_type = get_recovery_type(old_primary_conn);
			PQfinish(old_primary_conn);

			if (upstream_recovery_type == RECTYPE_PRIMARY)
			{
				log_notice(_("original primary reappeared - no action taken"));
				return FAILOVER_STATE_PRIMARY_REAPPEARED;
			}
		}

		return FAILOVER_STATE_FOLLOW_FAIL;
	}


	/*
	 * refresh local copy of local and primary node records - we get these
	 * directly from the primary to ensure they're the current version
	 */

	record_status = get_node_record(upstream_conn, new_primary_id, &upstream_node_info);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve metadata record found for node %i"),
				  new_primary_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}

	record_status = get_node_record(upstream_conn, local_node_info.node_id, &local_node_info);
	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve metadata record found for node %i"),
				  local_node_info.node_id);
		return FAILOVER_STATE_FOLLOW_FAIL;
	}


	local_conn = establish_db_connection(local_node_info.conninfo, false);
	initPQExpBuffer(&event_details);
	appendPQExpBuffer(&event_details,
					  _("node %i now following new upstream node %i"),
					  local_node_info.node_id,
					  upstream_node_info.node_id);

	log_notice("%s\n", event_details.data);

	create_event_notification(upstream_conn,
						&config_file_options,
						local_node_info.node_id,
						"repmgrd_failover_follow",
						true,
						event_details.data);

	termPQExpBuffer(&event_details);

	return FAILOVER_STATE_FOLLOWED_NEW_PRIMARY;
}


static const char *
_print_voting_status(NodeVotingStatus voting_status)
{
	switch(voting_status)
	{
		case VS_NO_VOTE:
			return "NO VOTE";

		case VS_VOTE_REQUEST_RECEIVED:
			return "VOTE REQUEST RECEIVED";

		case VS_VOTE_INITIATED:
			return "VOTE REQUEST INITIATED";

		case VS_UNKNOWN:
			return "VOTE REQUEST UNKNOWN";
	}

	return "UNKNOWN VOTE REQUEST STATE";
}

static const char *
_print_election_result(ElectionResult result)
{
	switch(result)
	{
		case ELECTION_NOT_CANDIDATE:
			return "NOT CANDIDATE";

		case ELECTION_WON:
			return "WON";

		case ELECTION_LOST:
			return "LOST";

		case ELECTION_CANCELLED:
			return "CANCELLED";
	}

	/* should never reach here */
	return "UNKNOWN";
}

static const char *
_print_monitoring_state(MonitoringState monitoring_state)
{
	switch(monitoring_state)
	{
		case MS_NORMAL:
			return "normal";

		case MS_DEGRADED:
			return "degraded";
	}

	/* should never reach here */
	return "UNKNOWN";
}



static ElectionResult
do_election(void)
{
	int electoral_term = -1;

	int votes_for_me = 0;

	/* we're visible */
	int visible_nodes = 1;

	/*
	 * get voting status from shared memory - should be one of "VS_NO_VOTE"
	 * or "VS_VOTE_REQUEST_RECEIVED". If VS_NO_VOTE, we declare ourselves as
	 * candidate and initiate the voting process.
	 */
	NodeVotingStatus voting_status;

	NodeInfoListCell *cell;

	bool other_node_is_candidate = false;
	bool other_node_is_ahead = false;

	/*
	 * Check if at least one server in the primary's location is visible;
	 * if not we'll assume a network split between this node and the primary
	 * location, and not promote any standby.
	 *
	 * NOTE: this function is only ever called by standbys attached to the current
	 * (unreachable) primary, so "upstream_node_info" will always contain the
	 * primary node record.
	 */
	bool primary_location_seen = false;

	/*
	 * sleep for a random period of 100 ~ 350 ms
	 */

	long unsigned rand_wait = (long) ((rand() % 35) + 10) * 10000;

	log_debug("do_election(): sleeping %lu", rand_wait);
	log_debug("do_election(): primary location is %s", upstream_node_info.location);

	pg_usleep(rand_wait);

	local_node_info.last_wal_receive_lsn = InvalidXLogRecPtr;

	log_debug("do_election(): executing get_voting_status()");
	voting_status = get_voting_status(local_conn);
	log_debug("do_election(): node voting status is %s", _print_voting_status(voting_status));

	if (voting_status == VS_VOTE_REQUEST_RECEIVED)
	{
		/* we've already been requested to vote, so can't become a candidate */
		log_debug("vote request already received, not candidate");
		return ELECTION_NOT_CANDIDATE;
	}

	/*
	 * Here we mark ourselves as candidate, so any further vote requests
	 * are rejected. However it's possible another node has done the
	 * same thing, so when announcing ourselves as candidate to the other
	 * nodes, we'll check for that and withdraw our candidature.
	 */
	electoral_term = set_voting_status_initiated(local_conn);

	/* get all active nodes attached to primary, excluding self */
	get_active_sibling_node_records(local_conn,
									local_node_info.node_id,
									upstream_node_info.node_id,
									&standby_nodes);

	/* no other standbys - win by default */

	if (standby_nodes.node_count == 0)
	{
		log_debug("no other nodes - we win by default");
		return ELECTION_WON;
	}

	for (cell = standby_nodes.head; cell; cell = cell->next)
	{
		/* assume the worst case */
		cell->node_info->is_visible = false;

		cell->node_info->conn = establish_db_connection(cell->node_info->conninfo, false);

		if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
		{
			continue;
		}

		/*
		 * tell the other node we're candidate - if the node has already declared
		 * itself, we withdraw
		 *
		 * XXX check for situations where more than one node could end up as candidate?
		 *
		 * XXX note it's possible some nodes accepted our candidature before we
		 * found out about the other candidate, check what happens in that situation
		 *  -> other node will have info from all the nodes, even if not the vote,
		 *     so it should be able to determine the best node anyway
		 */

		if (announce_candidature(cell->node_info->conn, &local_node_info, cell->node_info, electoral_term) == false)
		{
			log_debug("node %i is candidate",  cell->node_info->node_id);
			other_node_is_candidate = true;

			/* don't notify any further standbys */
			break;
		}

		/*
		 * see if the node is in the primary's location (but skip the check
		 * if we've seen
		 */
		if (primary_location_seen == false)
		{
			if (strncmp(cell->node_info->location, upstream_node_info.location, MAXLEN) == 0)
			{
				primary_location_seen = true;
			}
		}

		cell->node_info->is_visible = true;
		visible_nodes ++;
	}

	if (other_node_is_candidate == true)
	{
		clear_node_info_list(&standby_nodes);

		reset_node_voting_status();
		log_debug("other node is candidate, returning NOT CANDIDATE");
		return ELECTION_NOT_CANDIDATE;
	}

	if (primary_location_seen == false)
	{
		log_notice(_("no nodes from the primary location \"%s\" visible - assuming network split"),
				   upstream_node_info.location);
		log_detail(_("node will enter degraded monitoring state waiting for reconnect"));

		monitoring_state = MS_DEGRADED;
		INSTR_TIME_SET_CURRENT(degraded_monitoring_start);

		reset_node_voting_status();

		return ELECTION_CANCELLED;
	}


	/* get our lsn */
	local_node_info.last_wal_receive_lsn = get_last_wal_receive_location(local_conn);

	log_debug("last receive lsn = %X/%X",
			  (uint32) (local_node_info.last_wal_receive_lsn >> 32),
			  (uint32)  local_node_info.last_wal_receive_lsn);

	/* request vote from each node */

	for (cell = standby_nodes.head; cell; cell = cell->next)
	{
		log_debug("checking node %i...", cell->node_info->node_id);
		/* ignore unreachable nodes */
		if (cell->node_info->is_visible == false)
			continue;
		votes_for_me += request_vote(cell->node_info->conn,
									 &local_node_info,
									 cell->node_info,
									 electoral_term);

		if (cell->node_info->last_wal_receive_lsn > local_node_info.last_wal_receive_lsn)
		{
			/* register if another node is ahead of us */
			other_node_is_ahead = true;
		}
		PQfinish(cell->node_info->conn);
		cell->node_info->conn = NULL;
	}

	/* vote for myself, but only if I believe no-one else is ahead */
	if (other_node_is_ahead == false)
	{
		votes_for_me += 1;
	}

	log_debug(_("%i of of %i votes"), votes_for_me, visible_nodes);

	if (votes_for_me == visible_nodes)
		return ELECTION_WON;

	return ELECTION_LOST;
}


static void
reset_node_voting_status(void)
{
	failover_state = FAILOVER_STATE_NONE;

	if (PQstatus(local_conn) != CONNECTION_OK)
	{
		log_error(_("reset_node_voting_status(): local_conn not set"));
		return;
	}
	reset_voting_status(local_conn);
}


static void
monitor_bdr(void)
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

			log_debug("dir now %s", path);
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

static PGconn *
try_reconnect(const char *conninfo, NodeStatus *node_status)
{
	PGconn *conn;

	int i;

	int max_attempts = config_file_options.reconnect_attempts;

	for (i = 0; i < max_attempts; i++)
	{
		log_info(_("checking state of node, %i of %i attempts"), i, max_attempts);
		if (is_server_available(conninfo) == true)
		{
			log_notice(_("node has recovered, reconnecting"));

			/*
			 * XXX we should also handle the case where node is pingable
			 * but connection denied due to connection exhaustion
			 *  - fall back to degraded monitoring?
			 *  - make that configurable
			 */
			conn = establish_db_connection(conninfo, false);
			if (PQstatus(conn) == CONNECTION_OK)
			{
				*node_status = NODE_STATUS_UP;
				return conn;
			}

			PQfinish(conn);
			log_notice(_("unable to reconnect to node"));
		}
		log_info(_("sleeping %i seconds until next reconnection attempt"),
				 config_file_options.reconnect_interval);
		sleep(config_file_options.reconnect_interval);
	}


	log_warning(_("unable to reconnect to node after %i attempts"), max_attempts);
	*node_status = NODE_STATUS_DOWN;
	return NULL;
}


static void
close_connections()
{
	if (PQstatus(primary_conn) == CONNECTION_OK)
	{
		/* cancel any pending queries to the primary */
		if (PQisBusy(primary_conn) == 1)
			cancel_query(primary_conn, config_file_options.primary_response_timeout);
		PQfinish(primary_conn);
		primary_conn = NULL;
	}

	if (upstream_conn != NULL && PQstatus(upstream_conn) == CONNECTION_OK)
	{
		PQfinish(upstream_conn);
		upstream_conn = NULL;
	}

	if (PQstatus(local_conn) == CONNECTION_OK)
	{
		PQfinish(local_conn);
		local_conn = NULL;
	}
}


static int
calculate_elapsed(instr_time start_time)
{
	instr_time	current_time;

	INSTR_TIME_SET_CURRENT(current_time);

	INSTR_TIME_SUBTRACT(current_time, start_time);

	return (int)INSTR_TIME_GET_DOUBLE(current_time);
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

	log_info(_("%s terminating..."), progname());

	exit(retval);
}
