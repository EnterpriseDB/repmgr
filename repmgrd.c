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
	FAILOVER_STATE_PROMOTED,
	FAILOVER_STATE_PROMOTION_FAILED,
	FAILOVER_STATE_PRIMARY_REAPPEARED,
	FAILOVER_STATE_LOCAL_NODE_FAILURE,
	FAILOVER_STATE_WAITING_NEW_PRIMARY
	// FOLLOWED_NEW_PRIMARY
	// FOLLOW_WAIT_TIMEOUT
} FailoverState;


typedef enum {
	ELECTION_NOT_CANDIDATE = -1,
	ELECTION_WON,
	ELECTION_LOST
} ElectionResult;


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


static NodeInfoList standby_nodes = T_NODE_INFO_LIST_INITIALIZER;

/* Collate command line errors here for friendlier reporting */
static ItemList	cli_errors = { NULL, NULL };

static bool        startup_event_logged = false;

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
static void monitor_streaming_primary(void);
static void monitor_streaming_standby(void);

#ifndef WIN32
static void setup_event_handlers(void);
static void handle_sighup(SIGNAL_ARGS);
static void handle_sigint(SIGNAL_ARGS);
#endif

static PGconn *try_reconnect(const char *conninfo, NodeStatus *node_status);
static ElectionResult do_election(void);
static const char *_print_voting_status(NodeVotingStatus voting_status);
static const char *_print_election_result(ElectionResult result);

static FailoverState promote_self(void);
static void wait_primary_notification(void);
static void notify_followers(NodeInfoList *standby_nodes);

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
		switch (local_node_info.type)
		{
			case PRIMARY:
				monitor_streaming_primary();
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
monitor_streaming_primary(void)
{
	NodeStatus	node_status = NODE_STATUS_UP;
	instr_time	log_status_interval_start;

	/* Log startup event */
	if (startup_event_logged == false)
	{
		create_event_record(local_conn,
							&config_file_options,
							config_file_options.node_id,
							"repmgrd_start",
							true,
							NULL);
		startup_event_logged = true;

		log_notice(_("monitoring cluster primary \"%s\" (node ID: %i)"),
				   local_node_info.node_name,
				   local_node_info.node_id);
	}

	INSTR_TIME_SET_CURRENT(log_status_interval_start);

	while (true)
	{
		double		log_status_interval_elapsed = 0;
		instr_time	log_status_interval_current;

		// cache node list here, refresh at `node_list_refresh_interval`
		if (is_server_available(local_node_info.conninfo) == false)
		{

			/* node is down, we were expecting it to be up */
			if (node_status == NODE_STATUS_UP)
			{
				// log disconnect event
				log_warning(_("unable to connect to local node"));

				node_status = NODE_STATUS_UNKNOWN;

				PQfinish(local_conn);

				local_conn = try_reconnect(local_node_info.conninfo, &node_status);

				if (node_status == NODE_STATUS_UP)
				{
					// log reconnect event, details
					log_notice(_("reconnected to local node"));
					goto loop;
				}
			}

			if (node_status == NODE_STATUS_DOWN)
			{
				// attempt to find another node from cached list
				// loop, if starts up check status, switch monitoring mode
			}
		}

	loop:
		/* emit "still alive" log message at regular intervals, if requested */
		if (config_file_options.log_status_interval > 0)
		{
			INSTR_TIME_SET_CURRENT(log_status_interval_current);
			INSTR_TIME_SUBTRACT(log_status_interval_current, log_status_interval_start);
			log_status_interval_elapsed = INSTR_TIME_GET_DOUBLE(log_status_interval_current);

			if ((int) log_status_interval_elapsed >= config_file_options.log_status_interval)
			{
				log_info(_("monitoring primary node \"%s\" (node ID: %i)"),
						 local_node_info.node_name,
						 local_node_info.node_id);
				INSTR_TIME_SET_CURRENT(log_status_interval_start);
			}
		}
		sleep(1);
	}
}


static void
monitor_streaming_standby(void)
{
	NodeStatus	upstream_node_status = NODE_STATUS_UP;
	instr_time	log_status_interval_start;

	// check result
	(void) get_node_record(local_conn, local_node_info.upstream_node_id, &upstream_node_info);

	// handle failure - do we want to loop here?
	upstream_conn = establish_db_connection(upstream_node_info.conninfo, false);

	// fix for cascaded standbys
	primary_conn = upstream_conn;

	/* Log startup event */
	if (startup_event_logged == false)
	{
		create_event_record(upstream_conn,
							&config_file_options,
							config_file_options.node_id,
							"repmgrd_start",
							true,
							NULL);
		startup_event_logged = true;

		log_notice(_("repmgrd on node \"%s\" (node ID: %i) monitoring upstream node \"%s\" (node ID: %i)"),
				   local_node_info.node_name,
				   local_node_info.node_id,
				   upstream_node_info.node_name,
				   upstream_node_info.node_id);
	}

	INSTR_TIME_SET_CURRENT(log_status_interval_start);

	while (true)
	{
		double		log_status_interval_elapsed = 0;
		instr_time	log_status_interval_current;

		if (is_server_available(upstream_node_info.conninfo) == false)
		{

			/* upstream node is down, we were expecting it to be up */
			if (upstream_node_status == NODE_STATUS_UP)
			{
				// log disconnect event
				log_warning(_("unable to connect to upstream node"));
				upstream_node_status = NODE_STATUS_UNKNOWN;

				PQfinish(upstream_conn);
				upstream_conn = try_reconnect(upstream_node_info.conninfo, &upstream_node_status);

				if (upstream_node_status == NODE_STATUS_UP)
				{
					// log reconnect event
					log_notice(_("reconnected to upstream node"));
					goto loop;
				}

				/* still down after reconnect attempt(s) - */
				if (upstream_node_status == NODE_STATUS_DOWN)
				{
					/* attempt to initiate voting process */
					ElectionResult election_result = do_election();
					FailoverState failover_state = FAILOVER_STATE_UNKNOWN;

					log_debug("election result:  %s", _print_election_result(election_result));

					if (election_result == ELECTION_WON)
					{
						log_notice("I am the winner, will now promote self and inform other nodes");

						failover_state = promote_self();
					}
					else if (election_result == ELECTION_LOST)
					{
						log_info("I am the candidate but did not get all votes; will now determine the best candidate");
					}
					else
					{
						//   --> need timeout in case new primary doesn't come up, then rerun election

						log_info("I am a follower and am waiting to be informed by the winner");
						failover_state = FAILOVER_STATE_WAITING_NEW_PRIMARY;
					}

					switch(failover_state)
					{
						case FAILOVER_STATE_PROMOTED:
							/* inform former siblings that we are Number 1 */

							notify_followers(&standby_nodes);
							/* we no longer care about our former siblings */
							clear_node_info_list(&standby_nodes);

							/* pass control back down to start_monitoring() */
							log_info(_("switching to primary monitoring mode"));

							return;
						case FAILOVER_STATE_WAITING_NEW_PRIMARY:
							/* either follow or time out; either way resume monitoring */
							wait_primary_notification();
							/* pass control back down to start_monitoring() */
							return;
						case FAILOVER_STATE_PROMOTION_FAILED:
						case FAILOVER_STATE_PRIMARY_REAPPEARED:
						case FAILOVER_STATE_LOCAL_NODE_FAILURE:
						case FAILOVER_STATE_UNKNOWN:
							break;
					}
				}

			}
		}

	loop:

		/* emit "still alive" log message at regular intervals, if requested */
		if (config_file_options.log_status_interval > 0)
		{
			INSTR_TIME_SET_CURRENT(log_status_interval_current);
			INSTR_TIME_SUBTRACT(log_status_interval_current, log_status_interval_start);
			log_status_interval_elapsed = INSTR_TIME_GET_DOUBLE(log_status_interval_current);
			if ((int) log_status_interval_elapsed >= config_file_options.log_status_interval)
			{
				log_info(_("node \"%s\" (node ID: %i) monitoring upstream node \"%s\" (node ID: %i)"),
						 local_node_info.node_name,
						 local_node_info.node_id,
						 upstream_node_info.node_name,
						 upstream_node_info.node_id);

				INSTR_TIME_SET_CURRENT(log_status_interval_start);
			}
		}
		sleep(1);
	}
}

static FailoverState
promote_self(void)
{
	char *promote_command;
	int r;

	/* Store details of the failed node here */
	t_node_info failed_primary = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status;

	record_status = get_node_record(local_conn, local_node_info.upstream_node_id, &failed_primary);
	// XXX check success

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

		primary_conn = get_primary_connection(local_conn,
											  &primary_node_id, NULL);

		if (primary_conn != NULL && primary_node_id == failed_primary.node_id)
		{
			log_notice(_("original primary reappeared before this standby was promoted - no action taken"));

			/* XXX log an event here?  */

			PQfinish(primary_conn);
			primary_conn = NULL;

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


	{
		PQExpBufferData event_details;
		initPQExpBuffer(&event_details);

		/* update own internal node record */
		record_status = get_node_record(local_conn, local_node_info.node_id, &local_node_info);

		// XXX we're assuming the promote command updated metadata
		appendPQExpBuffer(&event_details,
						  _("node %i promoted to primary; old primary %i marked as failed"),
						  local_node_info.node_id,
						  failed_primary.node_id);
		/* my_local_conn is now the master */
		create_event_record(local_conn,
							&config_file_options,
							local_node_info.node_id,
							"repmgrd_failover_promote",
							true,
							event_details.data);
		termPQExpBuffer(&event_details);
	}

	return FAILOVER_STATE_PROMOTED;
}

static void
wait_primary_notification(void)
{
	// XXX make this configurable
	int wait_primary_timeout = 60;
	int i;
	int new_primary_id;

	for (i = 0; i < wait_primary_timeout; i++)
	{
		if (get_new_primary(local_conn, &new_primary_id) == true)
		{
			log_debug("XXX new primary is %i", new_primary_id);
			//return;
		}
		sleep(1);
	}
}


static void
notify_followers(NodeInfoList *standby_nodes)
{
	NodeInfoListCell *cell;

	for (cell = standby_nodes->head; cell; cell = cell->next)
	{
		log_debug("intending to notify %i... ", cell->node_info->node_id);
		if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
		{
			log_debug("connection to  %i lost... ", cell->node_info->node_id);

			cell->node_info->conn = establish_db_connection(cell->node_info->conninfo, false);
		}

		if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
		{
			log_debug("unable to reconnect to  %i ... ", cell->node_info->node_id);

			continue;
		}
		log_debug("notifying node %i to follow new primary", cell->node_info->node_id);
		notify_follow_primary(cell->node_info->conn, local_node_info.node_id);
	}
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
	}

	/* should never reach here */
	return "UNKNOWN";
}



// store lsndiffs, in the event we're not the best node,
// i.e. don't get all the votes, we pass the baton to the best node
static ElectionResult
do_election(void)
{
	int electoral_term = -1;

//	int total_eligible_nodes = 0;
	int votes_for_me = 0;

	/* we're visible */
	int visible_nodes = 1;


	// get voting status from shared memory
	// should be "VS_NO_VOTE" or "VS_VOTE_REQUEST_RECEIVED"
	// if VS_NO_VOTE, initiate voting process
	NodeVotingStatus voting_status;

	NodeInfoListCell *cell;

	bool other_node_is_candidate = false;

	/* sleep for a random period of 100 ~ 500 ms
	 * XXX adjust this downwards if feasible
	 */

	long unsigned rand_wait = (long) ((rand() % 50) + 10) * 10000;

	log_debug("do_election(): sleeping %lu", rand_wait);

	pg_usleep(rand_wait);

	local_node_info.last_wal_receive_lsn = InvalidXLogRecPtr;

	log_debug("do_election(): executing get_voting_status()");
	voting_status = get_voting_status(local_conn);
	log_debug("do_election(): node voting status is %s", _print_voting_status(voting_status));

	if (voting_status == VS_VOTE_REQUEST_RECEIVED)
	{
		log_debug("vote request already received, not candidate");
		/* we've already been requested to vote, so can't become a candidate */
		return ELECTION_NOT_CANDIDATE;
	}

	//  mark ourselves as candidate
	//  -> so any further vote requests are rejected
	electoral_term = set_voting_status_initiated(local_conn);

	/* get all active nodes attached to primary, excluding self */
	// XXX include barman node in results

	clear_node_info_list(&standby_nodes);

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

		// XXX handle witness-barman
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
		 */
		// other node:  if not candidate in this term, reset state (but don't bump term)
		if (announce_candidature(cell->node_info->conn, &local_node_info, cell->node_info, electoral_term) == false)
		{
			log_debug("node %i is candidate",  cell->node_info->node_id);
			other_node_is_candidate = true;

			/* don't perform any more checks */
			break;
		}

		cell->node_info->is_visible = true;
		visible_nodes ++;
	}

	if (other_node_is_candidate == true)
	{
		clear_node_info_list(&standby_nodes);

		// XXX do this
		// unset_voting_status_initiated(local_conn);
		return VS_NO_VOTE;
	}

	// XXX check if > 50% visible

	/* check again if we've been asked to vote */

	if (0)
	{
		voting_status = get_voting_status(local_conn);
		log_debug("do_election(): node voting status is %i", (int)voting_status);


		if (voting_status == VS_VOTE_REQUEST_RECEIVED)
		{
			/* we've already been requested to vote, so can't become a candidate */
			return voting_status;
		}
	}

	/* current node votes for itself by default */
	// XXX check returned LSNs, if one is higher than ours, don't vote for ourselves
	// either

	votes_for_me += 1;

	/* get our lsn */
	local_node_info.last_wal_receive_lsn = get_last_wal_receive_location(local_conn);

	log_debug("LAST receive lsn = %X/%X",
			  (uint32) (local_node_info.last_wal_receive_lsn >> 32),
			  (uint32)  local_node_info.last_wal_receive_lsn);
	/* request vote */

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

		PQfinish(cell->node_info->conn);
		cell->node_info->conn = NULL;
	}

	log_notice(_("%i of of %i votes"), votes_for_me, visible_nodes);

	if (votes_for_me == visible_nodes)
		return ELECTION_WON;

	return ELECTION_LOST;
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

	// XXX make this all configurable
	int max_attempts = 5;

	for (i = 0; i < max_attempts; i++)
	{
		log_info(_("checking state of node, %i of %i attempts"), i, max_attempts);
		if (is_server_available(conninfo) == true)
		{
			log_notice(_("node has recovered, reconnecting"));

			// XXX how to handle case where node is reachable
			// but connection denied due to connection exhaustion
			conn = establish_db_connection(conninfo, false);
			if (PQstatus(conn) == CONNECTION_OK)
			{
				*node_status = NODE_STATUS_UP;
				return conn;
			}

			PQfinish(conn);
			log_notice(_("unable to reconnect to node"));
		}
		sleep(1);
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
