/*
 * repmgr-action-standby.c
 *
 * Implements standby actions for the repmgr command line utility
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include "repmgr.h"
#include "dirutil.h"

#include "repmgr-client-global.h"
#include "repmgr-action-standby.h"

static PGconn  *primary_conn = NULL;
static PGconn  *source_conn = NULL;


static char		local_data_directory[MAXPGPATH];
static bool		local_data_directory_provided = false;

static bool		upstream_record_found = false;
static int	    upstream_node_id = UNKNOWN_NODE_ID;
static char		upstream_data_directory[MAXPGPATH];

static t_conninfo_param_list recovery_conninfo;
static char		recovery_conninfo_str[MAXLEN];

static standy_clone_mode mode;

/* used by barman mode */
static char		local_repmgr_tmp_directory[MAXPGPATH];


static void check_barman_config(void);
static char *make_barman_ssh_command(char *buf);
static void	check_source_server(void);


void
do_standby_clone(void)
{

	/*
	 * conninfo params for the actual upstream node (which might be different
	 * to the node we're cloning from) to write to recovery.conf
	 */

	/*
	 * detecting the cloning mode
	 */
	mode = get_standby_clone_mode();

	/*
	 * In rsync mode, we need to check the SSH connection early
	 */
	if (mode == rsync)
	{
		int r;

		r = test_ssh_connection(runtime_options.host, runtime_options.remote_user);
		if (r != 0)
		{
			log_error(_("remote host %s is not reachable via SSH"),
					  runtime_options.host);
			exit(ERR_BAD_SSH);
		}
	}

	/*
	 * If dest_dir (-D/--pgdata) was provided, this will become the new data
	 * directory (otherwise repmgr will default to using the same directory
	 * path as on the source host).
	 *
	 * Note that barman mode requires -D/--pgdata.
	 *
	 * If -D/--pgdata is not supplied, and we're not cloning from barman,
	 * the source host's data directory will be fetched later, after
	 * we've connected to it.
	 */
	if (runtime_options.data_dir[0])
	{
		local_data_directory_provided = true;
		log_notice(_("destination directory '%s' provided"),
				   runtime_options.data_dir);
	}
	else if (mode == barman)
	{
		log_error(_("Barman mode requires a data directory"));
		log_hint(_("use -D/--pgdata to explicitly specify a data directory"));
		exit(ERR_BAD_CONFIG);
	}

	/* Sanity-check barman connection and installation */
	if (mode == barman)
	{
		/* this will exit with ERR_BARMAN if problems found */
		check_barman_config();
	}


	/*
	 * target directory (-D/--pgdata) provided - use that as new data directory
	 * (useful when executing backup on local machine only or creating the backup
	 * in a different local directory when backup source is a remote host)
	 */
	if (local_data_directory_provided == true)
	{
		strncpy(local_data_directory, runtime_options.data_dir, MAXPGPATH);
	}

	/*
	 * Initialise list of conninfo parameters which will later be used
	 * to create the `primary_conninfo` string in recovery.conf .
	 *
	 * We'll initialise it with the default values as seen by libpq,
	 * and overwrite them with the host settings specified on the command
	 * line. As it's possible the standby will be cloned from a node different
	 * to its intended upstream, we'll later attempt to fetch the
	 * upstream node record and overwrite the values set here with
	 * those from the upstream node record (excluding that record's
	 * application_name)
	 */
	initialize_conninfo_params(&recovery_conninfo, true);

	copy_conninfo_params(&recovery_conninfo, &source_conninfo);

	/*
	 * If application_name is set in repmgr.conf's conninfo parameter, use
	 * this value (if the source host was provided as a conninfo string, any
	 * application_name values set there will be overridden; we assume the only
	 * reason to pass an application_name via the command line is in the
	 * rare corner case where a user wishes to clone a server without
	 * providing repmgr.conf)
	 */
	if (strlen(config_file_options.conninfo))
	{
		char application_name[MAXLEN] = "";

		get_conninfo_value(config_file_options.conninfo, "application_name", application_name);
		if (strlen(application_name))
		{
			param_set(&recovery_conninfo, "application_name", application_name);
		}
	}


	/*
	 * --upstream-conninfo supplied, which we interpret to imply
	 * --no-upstream-connection as well - the use case for this option is when
	 * the upstream is not available, so no point in checking for it.
	 */

	if (*runtime_options.upstream_conninfo)
		runtime_options.no_upstream_connection = false;

	/* By default attempt to connect to the source server */
	if (runtime_options.no_upstream_connection == false)
	{
		check_source_server();
	}

}



void
check_barman_config(void)
{
	char		datadir_list_filename[MAXLEN];
	char		barman_command_buf[MAXLEN] = "";

	char		command[MAXLEN];
	bool		command_ok;

	/*
	 * Check that there is at least one valid backup
	 */

	log_info(_("connecting to Barman server to verify backup for %s"), config_file_options.barman_server);

	maxlen_snprintf(command, "%s show-backup %s latest > /dev/null",
					make_barman_ssh_command(barman_command_buf),
				 	config_file_options.barman_server);

	command_ok = local_command(command, NULL);

	if (command_ok == false)
	{
		log_error(_("no valid backup for server %s was found in the Barman catalogue"),
				  config_file_options.barman_server);
		log_hint(_("refer to the Barman documentation for more information\n"));

		exit(ERR_BARMAN);
	}

	/*
	 * Create the local repmgr subdirectory
	 */

	maxlen_snprintf(local_repmgr_tmp_directory,
					"%s/repmgr",  local_data_directory);

	maxlen_snprintf(datadir_list_filename,
					"%s/data.txt", local_repmgr_tmp_directory);

	if (!create_pg_dir(local_data_directory, runtime_options.force))
	{
		log_error(_("unable to use directory %s"),
				local_data_directory);
		log_hint(_("use -F/--force option to force this directory to be overwritten\n"));
			exit(ERR_BAD_CONFIG);
	}

	if (!create_pg_dir(local_repmgr_tmp_directory, runtime_options.force))
	{
		log_error(_("unable to create directory \"%s\""),
				  local_repmgr_tmp_directory);

		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Fetch server parameters from Barman
	 */
	log_info(_("connecting to Barman server to fetch server parameters"));

	maxlen_snprintf(command, "%s show-server %s > %s/show-server.txt",
					make_barman_ssh_command(barman_command_buf),
					config_file_options.barman_server,
					local_repmgr_tmp_directory);

	command_ok = local_command(command, NULL);

	if (command_ok == false)
	{
		log_error(_("unable to fetch server parameters from Barman server"));

		exit(ERR_BARMAN);
	}

}


static char *
make_barman_ssh_command(char *buf)
{
	static char config_opt[MAXLEN] = "";

	if (strlen(config_file_options.barman_config))
		maxlen_snprintf(config_opt,
						" --config=%s",
						config_file_options.barman_config);

	maxlen_snprintf(buf,
					"ssh %s barman%s",
					config_file_options.barman_server,
					config_opt);

	return buf;
}


static void
check_source_server()
{
	int			server_version_num = UNKNOWN_SERVER_VERSION_NUM;
	char		cluster_size[MAXLEN];
	t_node_info node_record = T_NODE_INFO_INITIALIZER;
	int query_result;
	t_extension_status extension_status;

	/* Attempt to connect to the upstream server to verify its configuration */
	log_info(_("connecting to upstream node"));

	source_conn = establish_db_connection_by_params((const char**)source_conninfo.keywords,
													(const char**)source_conninfo.values,
													false);

	/*
	 * Unless in barman mode, exit with an error;
	 * establish_db_connection_by_params() will have already logged an error message
	 */
	if (PQstatus(source_conn) != CONNECTION_OK)
	{
		if (mode == barman)
		{
			return;
		}
		else
		{
			PQfinish(source_conn);
			exit(ERR_DB_CON);
		}
	}

	/*
	 * If a connection was established, perform some sanity checks on the
	 * provided upstream connection
	 */


	/* Verify that upstream node is a supported server version */
	log_verbose(LOG_INFO, _("connected to upstream node, checking its state"));

	server_version_num = check_server_version(source_conn, "master", true, NULL);

	check_upstream_config(source_conn, server_version_num, true);

	if (get_cluster_size(source_conn, cluster_size) == false)
		exit(ERR_DB_QUERY);

	log_info(_("successfully connected to source node"));
	log_detail(_("current installation size is %s"),
			   cluster_size);

	/*
	 * If --recovery-min-apply-delay was passed, check that
	 * we're connected to PostgreSQL 9.4 or later
	 */
	// XXX should this be a config file parameter?
	if (*runtime_options.recovery_min_apply_delay)
	{
		if (server_version_num < 90400)
		{
			log_error(_("PostgreSQL 9.4 or greater required for --recovery-min-apply-delay\n"));
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/*
	 * If the upstream node is a standby, try to connect to the primary too so we
	 * can write an event record
	 */
	if (is_standby(source_conn))
	{
		primary_conn = get_master_connection(source_conn, NULL, NULL);

		// XXX check this worked?
	}
	else
	{
		primary_conn = source_conn;
	}

	/*
	 * Sanity-check that the master node has a repmgr schema - if not
	 * present, fail with an error unless -F/--force is used (to enable
	 * repmgr to be used as a standalone clone tool)
	 */

	extension_status = get_repmgr_extension_status(primary_conn);

	if (extension_status != REPMGR_INSTALLED)
	{
		if (!runtime_options.force)
		{
			/* schema doesn't exist */
			log_error(_("repmgr extension not found on upstream server"));
			log_hint(_("check that the upstream server is part of a repmgr cluster"));
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}

		log_warning(_("repmgr extension not found on upstream server"));
	}

	/* Fetch the source's data directory */
	if (get_pg_setting(source_conn, "data_directory", upstream_data_directory) == false)
	{
		log_error(_("unable to retrieve upstream node's data directory"));
		log_hint(_("STANDBY CLONE must be run as a database superuser"));
		PQfinish(source_conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * If no target data directory was explicitly provided, we'll default to
	 * the source host's data directory.
	 */
	if (local_data_directory_provided == false)
	{
		strncpy(local_data_directory, upstream_data_directory, MAXPGPATH);

		log_notice(_("setting data directory to: \"%s\""), local_data_directory);
		log_hint(_("use -D/--pgdata to explicitly specify a data directory"));
	}

	/*
	 * Copy the source connection so that we have some default values,
	 * particularly stuff like passwords extracted from PGPASSFILE;
	 * these will be overridden from the upstream conninfo, if provided.
	 *
	 * XXX only allow passwords if --use-conninfo-password
	 */
	conn_to_param_list(source_conn, &recovery_conninfo);

	/*
	 * Attempt to find the upstream node record
	 */
	if (config_file_options.upstream_node_id == NO_UPSTREAM_NODE)
		upstream_node_id = get_master_node_id(source_conn);
	else
		upstream_node_id = config_file_options.upstream_node_id;

	query_result = get_node_record(source_conn, upstream_node_id, &node_record);

	if (query_result)
	{
		upstream_record_found = true;
		strncpy(recovery_conninfo_str, node_record.conninfo, MAXLEN);
	}

	/*
	 * check that there's no existing node record with the same name but
	 * different ID
	 */
	query_result = get_node_record_by_name(source_conn, config_file_options.node_name, &node_record);

	if (query_result)
	{
		log_error(_("another node (node_id: %i) already exists with node_name \"%s\""),
				  node_record.node_id,
				  config_file_options.node_name);
		PQfinish(source_conn);
		exit(ERR_BAD_CONFIG);
	}

}


