/*
 * repmgr-action-standby.c
 *
 * Implements standby actions for the repmgr command line utility
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include <sys/stat.h>

#include "repmgr.h"
#include "dirutil.h"
#include "compat.h"

#include "repmgr-client-global.h"
#include "repmgr-action-standby.h"


typedef struct TablespaceDataListCell
{
	struct TablespaceDataListCell *next;
	char	   *name;
	char	   *oid;
	char	   *location;
	/* optional payload */
	FILE       *f;
} TablespaceDataListCell;

typedef struct TablespaceDataList
{
	TablespaceDataListCell *head;
	TablespaceDataListCell *tail;
} TablespaceDataList;

struct BackupLabel
{
	XLogRecPtr start_wal_location;
	char start_wal_file[MAXLEN];
	XLogRecPtr checkpoint_location;
	char backup_from[MAXLEN];
	char backup_method[MAXLEN];
	char start_time[MAXLEN];
	char label[MAXLEN];
	XLogRecPtr min_failover_slot_lsn;
};

static PGconn  *primary_conn = NULL;
static PGconn  *source_conn = NULL;

static int		server_version_num = UNKNOWN_SERVER_VERSION_NUM;

static char		local_data_directory[MAXPGPATH];
static bool		local_data_directory_provided = false;

static bool		upstream_record_found = false;
static int	    upstream_node_id = UNKNOWN_NODE_ID;
static char		upstream_data_directory[MAXPGPATH];

static t_conninfo_param_list recovery_conninfo;
static char		recovery_conninfo_str[MAXLEN];

static t_configfile_list config_files = T_CONFIGFILE_LIST_INITIALIZER;

static standy_clone_mode mode;

// XXX these aren't actually used after being set... remove if no purpose can be found
static char	   *first_wal_segment = NULL;
static char	   *last_wal_segment = NULL;

static bool		pg_start_backup_executed = false;

static struct BackupLabel backup_label;

/* used by barman mode */
static char		local_repmgr_tmp_directory[MAXPGPATH];
static char		datadir_list_filename[MAXLEN];
static char		barman_command_buf[MAXLEN] = "";

static void check_barman_config(void);
static void	check_source_server(void);
static void	check_source_server_via_barman(void);


static void initialise_direct_clone(void);
static void config_file_list_init(t_configfile_list *list, int max_size);
static void config_file_list_add(t_configfile_list *list, const char *file, const char *filename, bool in_data_dir);
static void copy_configuration_files(void);
static void cleanup_data_directory(void);

static int run_basebackup(void);
static int run_file_backup(void);
static bool read_backup_label(const char *local_data_directory, struct BackupLabel *out_backup_label);
static void parse_lsn(XLogRecPtr *ptr, const char *str);
static XLogRecPtr parse_label_lsn(const char *label_key, const char *label_value);

static int get_tablespace_data(PGconn *upstream_conn, TablespaceDataList *list);
static void tablespace_data_append(TablespaceDataList *list, const char *name, const char *oid, const char *location);

static void get_barman_property(char *dst, char *name, char *local_repmgr_directory);
static int  get_tablespace_data_barman(char *, TablespaceDataList *);
static char *make_barman_ssh_command(char *buf);


void
do_standby_clone(void)
{
	PQExpBufferData event_details;
	int r;

	/*
	 * conninfo params for the actual upstream node (which might be different
	 * to the node we're cloning from) to write to recovery.conf
	 */

	mode = get_standby_clone_mode();

	/*
	 * In rsync mode, we need to check the SSH connection early
	 */
	if (mode == rsync)
	{
		r = test_ssh_connection(runtime_options.host, runtime_options.remote_user);
		if (r != 0)
		{
			log_error(_("remote host %s is not reachable via SSH"),
					  runtime_options.host);
			exit(ERR_BAD_SSH);
		}
	}

	/*
	 * If a data directory (-D/--pgdata) was provided, use that, otherwise
	 * repmgr will default to using the same directory path as on the source
	 * host.
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
		log_notice(_("destination directory \"%s\" provided"),
				   runtime_options.data_dir);
	}
	else if (mode == barman)
	{
		log_error(_("Barman mode requires a data directory"));
		log_hint(_("use -D/--pgdata to explicitly specify a data directory"));
		exit(ERR_BAD_CONFIG);
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

	/* Sanity-check barman connection and installation */
	if (mode == barman)
	{
		/* this will exit with ERR_BARMAN if problems found */
		check_barman_config();
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
		runtime_options.no_upstream_connection = true;

	/* By default attempt to connect to the source server */
	if (runtime_options.no_upstream_connection == false)
	{
		check_source_server();
	}

	if (mode == barman && PQstatus(source_conn) != CONNECTION_OK)
	{
		/*
		 * Here we don't have a connection to the upstream node, and are executing
		 * in Barman mode - we can try and connect via the Barman server to extract
		 * the upstream node's conninfo string.
		 *
		 * To do this we need to extract Barman's conninfo string, replace the database
		 * name with the repmgr one (they could well be different) and remotely execute
		 * psql.
		 */
		check_source_server_via_barman();
	}

	if (upstream_record_found == true)
	{
		/*  parse returned upstream conninfo string to recovery primary_conninfo params*/
		char	   *errmsg = NULL;
		bool	    parse_success;

		log_verbose(LOG_DEBUG, "parsing upstream conninfo string \"%s\"", recovery_conninfo_str);

		/* parse_conninfo_string() here will remove the upstream's `application_name`, if set */

		parse_success = parse_conninfo_string(recovery_conninfo_str, &recovery_conninfo, errmsg, true);
		if (parse_success == false)
		{
			log_error(_("unable to parse conninfo string \"%s\" for upstream node:\n%s"),
					  recovery_conninfo_str, errmsg);

			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}
	}
	else
	{
		/*
		 * If no upstream node record found, we'll abort with an error here,
		 * unless -F/--force is used, in which case we'll use the parameters
		 * provided on the command line (and assume the user knows what they're
		 * doing).
		 */

		if (!runtime_options.force)
		{
			log_error(_("no record found for upstream node (upstream_node_id: %i)"),
					  upstream_node_id);
			log_hint(_("use -F/--force to create \"primary_conninfo\" based on command-line parameters"));
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/* If --replication-user was set, use that value for the primary_conninfo user */
	if (*runtime_options.replication_user)
	{
		param_set(&recovery_conninfo, "user", runtime_options.replication_user);
	}

	if (mode != barman)
	{
		initialise_direct_clone();
	}

	switch (mode)
	{
		case rsync:
			log_notice(_("starting backup (using rsync)..."));
			break;
		case pg_basebackup:
			log_notice(_("starting backup (using pg_basebackup)..."));
			if (runtime_options.fast_checkpoint == false)
			{
				log_hint(_("this may take some time; consider using the -c/--fast-checkpoint option"));
			}
			break;
		case barman:
			log_notice(_("getting backup from Barman..."));
			break;
		default:
			/* should never reach here */
			log_error(_("unknown clone mode"));
	}

	if (mode == pg_basebackup)
	{
		r = run_basebackup();
	}
	else
	{
		r = run_file_backup();
	}


	/* If the backup failed then exit */
	if (r != 0)
	{
		/* If a replication slot was previously created, drop it */
		if (config_file_options.use_replication_slots)
		{
			drop_replication_slot(source_conn, repmgr_slot_name);
		}

		log_error(_("unable to take a base backup of the master server"));
		log_warning(_("data directory (%s) may need to be cleaned up manually"),
					local_data_directory);

		PQfinish(source_conn);
		exit(r);
	}


	/*
	 * If `--copy-external-config-files` was provided, copy any configuration
	 * files detected to the appropriate location. Any errors encountered
	 * will not be treated as fatal.
	 *
	 * XXX check this won't run in Barman mode
	 */
	if (runtime_options.copy_external_config_files && config_files.entries)
	{
		copy_configuration_files();
	}

	/* Write the recovery.conf file */

	create_recovery_file(local_data_directory, &recovery_conninfo);

	switch(mode)
	{
		case rsync:
			log_notice(_("standby clone (using rsync) complete"));
			break;

		case pg_basebackup:
			log_notice(_("standby clone (using pg_basebackup) complete"));
			break;

		case barman:
			log_notice(_("standby clone (from Barman) complete"));
			break;
	}

	/*
	 * XXX It might be nice to provide an options to have repmgr start
	 * the PostgreSQL server automatically (e.g. with a custom pg_ctl
	 * command)
	 */

	log_notice(_("you can now start your PostgreSQL server"));

	if (*config_file_options.service_start_command)
	{
		log_hint(_("for example : %s"),
				 config_file_options.service_start_command);
	}
	else if (local_data_directory_provided)
	{
		log_hint(_("for example : pg_ctl -D %s start"),
				 local_data_directory);
	}
	else
	{
		log_hint(_("for example : /etc/init.d/postgresql start"));
	}

	/*
	 * XXX forgetting to (re) register the standby is a frequent cause
	 * of error; we should consider having repmgr automatically
	 * register the standby, either by default with an option
	 * "--no-register", or an option "--register".
	 *
	 * Note that "repmgr standby register" requires the standby to
	 * be running - if not, and we just update the node record,
	 * we'd have an incorrect representation of the replication cluster.
	 * Best combined with an automatic start of the server (see note
	 * above)
	 */

	/*
	 * XXX detect whether a record exists for this node already, and
	 * add a hint about using the -F/--force.
	 */

	log_hint(_("after starting the server, you need to register this standby with \"repmgr standby register\""));

	/* Log the event */

	initPQExpBuffer(&event_details);

	/* Add details about relevant runtime options used */
	appendPQExpBuffer(&event_details,
					  _("Cloned from host '%s', port %s"),
					  runtime_options.host,
					  runtime_options.port);

	appendPQExpBuffer(&event_details,
					  _("; backup method: "));

	switch(mode)
	{
		case rsync:
			appendPQExpBuffer(&event_details, "rsync");
			break;
		case pg_basebackup:
			appendPQExpBuffer(&event_details, "pg_basebackup");
			break;
		case barman:
			appendPQExpBuffer(&event_details, "barman");
			break;
	}

	appendPQExpBuffer(&event_details,
					  _("; --force: %s"),
					  runtime_options.force ? "Y" : "N");

	create_event_record(primary_conn,
						&config_file_options,
						config_file_options.node_id,
						"standby_clone",
						true,
						event_details.data);

	if (PQstatus(primary_conn) == CONNECTION_OK)
		PQfinish(primary_conn);

	if (PQstatus(source_conn) == CONNECTION_OK)
		PQfinish(source_conn);
	exit(r);
}


void
check_barman_config(void)
{
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
		log_hint(_("refer to the Barman documentation for more information"));

		exit(ERR_BARMAN);
	}


	if (!create_pg_dir(local_data_directory, runtime_options.force))
	{
		log_error(_("unable to use directory %s"),
				local_data_directory);
		log_hint(_("use -F/--force option to force this directory to be overwritten"));
			exit(ERR_BAD_CONFIG);
	}


	/*
	 * Create the local repmgr subdirectory
	 */

	maxlen_snprintf(local_repmgr_tmp_directory,
					"%s/repmgr", local_data_directory);

	maxlen_snprintf(datadir_list_filename,
					"%s/data.txt", local_repmgr_tmp_directory);

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



static void
check_source_server()
{
	PGconn			  *superuser_conn = NULL;
	PGconn			  *privileged_conn = NULL;

	char			   cluster_size[MAXLEN];
	t_node_info		   node_record = T_NODE_INFO_INITIALIZER;
	int				   query_result;
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
		PQfinish(source_conn);

		if (mode == barman)
			return;
		else
			exit(ERR_DB_CON);
	}

	/*
	 * If a connection was established, perform some sanity checks on the
	 * provided upstream connection
	 */


	/* Verify that upstream node is a supported server version */
	log_verbose(LOG_INFO, _("connected to source node, checking its state"));

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
			log_error(_("PostgreSQL 9.4 or greater required for --recovery-min-apply-delay"));
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
			if (extension_status == REPMGR_UNKNOWN)
			{
				PQfinish(source_conn);
				exit(ERR_DB_QUERY);
			}

			/* schema doesn't exist */
			log_error(_("repmgr extension not found on source node"));

			if (extension_status == REPMGR_AVAILABLE)
			{
				log_detail(_("repmgr extension is available but not installed in database \"%s\""),
						   param_get(&source_conninfo, "dbname"));
			}
			else if (extension_status == REPMGR_UNAVAILABLE)
			{
				log_detail(_("repmgr extension is not available on the upstream server"));
			}

			log_hint(_("check that the upstream server is part of a repmgr cluster"));
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}

		log_warning(_("repmgr extension not found on source node"));
	}

	/* Fetch the source's data directory */
	get_superuser_connection(&source_conn, &superuser_conn, &privileged_conn);

	if (get_pg_setting(privileged_conn, "data_directory", upstream_data_directory) == false)
	{
		log_error(_("unable to retrieve source node's data directory"));
		log_hint(_("STANDBY CLONE must be run as a database superuser"));
		PQfinish(source_conn);
		if(superuser_conn != NULL)
			PQfinish(superuser_conn);

		exit(ERR_BAD_CONFIG);
	}
	if(superuser_conn != NULL)
		PQfinish(superuser_conn);

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
	 * In the default pg_basebackup mode, we'll cowardly refuse to overwrite
	 * an existing data directory
	 */
	if (mode == pg_basebackup)
	{
		if (is_pg_dir(local_data_directory))
		{
			log_error(_("target data directory appears to be a PostgreSQL data directory"));
			log_detail(_("target data directory is \"%s\""), local_data_directory);
			log_hint(_("ensure the target data directory is empty before running \"STANDBY CLONE\" in pg_basebackup mode"));
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/*
	 * Copy the source connection so that we have some default values,
	 * particularly stuff like passwords extracted from PGPASSFILE;
	 * these will be overridden from the upstream conninfo, if provided.
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


static void
check_source_server_via_barman()
{
	char		    buf[MAXLEN];
	char		    barman_conninfo_str[MAXLEN];
	t_conninfo_param_list barman_conninfo;
	char		   *errmsg = NULL;
	bool		    parse_success,
				    command_success;
	char		    where_condition[MAXLEN];
	PQExpBufferData command_output;
	PQExpBufferData repmgr_conninfo_buf;

	int c;

	get_barman_property(barman_conninfo_str, "conninfo", local_repmgr_tmp_directory);

	initialize_conninfo_params(&barman_conninfo, false);

	/* parse_conninfo_string() here will remove the upstream's `application_name`, if set */
	parse_success = parse_conninfo_string(barman_conninfo_str, &barman_conninfo, errmsg, true);

	if (parse_success == false)
	{
		log_error(_("Unable to parse barman conninfo string \"%s\":\n%s"),
				barman_conninfo_str, errmsg);
		exit(ERR_BARMAN);
	}

	/* Overwrite database name in the parsed parameter list */
	param_set(&barman_conninfo, "dbname", runtime_options.dbname);

	/* Rebuild the Barman conninfo string */
	initPQExpBuffer(&repmgr_conninfo_buf);

	for (c = 0; c < barman_conninfo.size && barman_conninfo.keywords[c] != NULL; c++)
	{
		if (repmgr_conninfo_buf.len != 0)
			appendPQExpBufferChar(&repmgr_conninfo_buf, ' ');

		appendPQExpBuffer(&repmgr_conninfo_buf, "%s=",
						  barman_conninfo.keywords[c]);
		appendConnStrVal(&repmgr_conninfo_buf,
						 barman_conninfo.values[c]);
	}

	log_verbose(LOG_DEBUG,
				"repmgr database conninfo string on barman server: %s",
				repmgr_conninfo_buf.data);

	// XXX check this works in all cases
	maxlen_snprintf(where_condition, "node_id=%i", upstream_node_id);

	initPQExpBuffer(&command_output);
	maxlen_snprintf(buf,
					"ssh %s \"psql -Aqt \\\"%s\\\" -c \\\""
					" SELECT conninfo"
					" FROM repmgr.nodes"
					" WHERE %s"
					" AND active IS TRUE"
					"\\\"\"",
					config_file_options.barman_host,
					repmgr_conninfo_buf.data,
					where_condition);

	termPQExpBuffer(&repmgr_conninfo_buf);

	command_success = local_command(buf, &command_output);

	if (command_success == false)
	{
		log_error(_("unable to execute database query via Barman server"));
		exit(ERR_BARMAN);
	}

	maxlen_snprintf(recovery_conninfo_str, "%s", command_output.data);
	string_remove_trailing_newlines(recovery_conninfo_str);

	upstream_record_found = true;
	log_verbose(LOG_DEBUG,
				"upstream node conninfo string extracted via barman server: %s",
				recovery_conninfo_str);

	termPQExpBuffer(&command_output);
}



/*
 * In pg_basebackup/rsync modes, configure the target data directory
 * if necessary, and fetch information about tablespaces and configuration
 * files.
 */
static void
initialise_direct_clone(void)
{
	PGconn			 *superuser_conn = NULL;
	PGconn			 *privileged_conn = NULL;
	PGresult		 *res;
	PQExpBufferData	  query;
	int		 		  i;

	/*
	 * Check the destination data directory can be used
	 * (in Barman mode, this directory will already have been created)
	 */

	if (!create_pg_dir(local_data_directory, runtime_options.force))
	{
		log_error(_("unable to use directory \"%s\""),
				  local_data_directory);
		log_hint(_("use -F/--force to force this directory to be overwritten"));
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Check that tablespaces named in any `tablespace_mapping` configuration
	 * file parameters exist.
	 *
	 * pg_basebackup doesn't verify mappings, so any errors will not be caught.
	 * We'll do that here as a value-added service.
	 *
	 * -T/--tablespace-mapping is not available as a pg_basebackup option for
	 * PostgreSQL 9.3 - we can only handle that with rsync, so if `--rsync-only`
	 * not set, fail with an error
	 */

	if (config_file_options.tablespace_mapping.head != NULL)
	{
		TablespaceListCell *cell;

		if (server_version_num < 90400 && !runtime_options.rsync_only)
		{
			log_error(_("in PostgreSQL 9.3, tablespace mapping can only be used in conjunction with --rsync-only"));
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}

		for (cell = config_file_options.tablespace_mapping.head; cell; cell = cell->next)
		{
			initPQExpBuffer(&query);

			// XXX escape value
			appendPQExpBuffer(&query,
							  "SELECT spcname "
							  "  FROM pg_catalog.pg_tablespace "
							  " WHERE pg_catalog.pg_tablespace_location(oid) = '%s'",
							  cell->old_dir);
			res = PQexec(source_conn, query.data);

			termPQExpBuffer(&query);

			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				log_error(_("unable to execute tablespace query:\n  %s"),
						  PQerrorMessage(source_conn));
				PQclear(res);
				PQfinish(source_conn);
				exit(ERR_BAD_CONFIG);
			}

			/* TODO: collate errors and output at end of loop */
			if (PQntuples(res) == 0)
			{
				log_error(_("no tablespace matching path '%s' found"),
						  cell->old_dir);
				PQclear(res);
				PQfinish(source_conn);
				exit(ERR_BAD_CONFIG);
			}
		}
	}

	/*
	 * Obtain configuration file locations
	 *
	 * We'll check to see whether the configuration files are in the data
	 * directory - if not we'll have to copy them via SSH, if copying
	 * requested.
	 *
	 * This will require superuser permissions, so we'll attempt to connect
	 * as -S/--superuser (if provided), otherwise check the current connection
	 * user has superuser rights.
	 *
	 * XXX: if configuration files are symlinks to targets outside the data
	 * directory, they won't be copied by pg_basebackup, but we can't tell
	 * this from the below query; we'll probably need to add a check for their
	 * presence and if missing force copy by SSH
	 */

	get_superuser_connection(&source_conn, &superuser_conn, &privileged_conn);

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "  WITH dd AS ( "
					  "    SELECT setting AS data_directory"
					  "      FROM pg_catalog.pg_settings "
					  "     WHERE name = 'data_directory' "
					  "  ) "
					  "    SELECT DISTINCT(sourcefile), "
					  "           pg_catalog.regexp_replace(sourcefile, '^.*\\/', '') AS filename, "
					  "           sourcefile ~ ('^' || dd.data_directory) AS in_data_dir "
					  "      FROM dd, pg_catalog.pg_settings ps "
					  "     WHERE sourcefile IS NOT NULL "
					  "  ORDER BY 1 ");

	log_debug("standby clone: %s", query.data);
	res = PQexec(privileged_conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to retrieve configuration file locations:\n  %s"),
				  PQerrorMessage(privileged_conn));
		PQclear(res);
		PQfinish(source_conn);

		if (superuser_conn != NULL)
			PQfinish(superuser_conn);

		exit(ERR_BAD_CONFIG);
	}

	/*
	 * allocate memory for config file array - number of rows returned from
	 * above query + 2 for pg_hba.conf, pg_ident.conf
	 */

	config_file_list_init(&config_files, PQntuples(res) + 2);

	for (i = 0; i < PQntuples(res); i++)
	{
		config_file_list_add(&config_files,
							 PQgetvalue(res, i, 0),
							 PQgetvalue(res, i, 1),
							 strcmp(PQgetvalue(res, i, 2), "t") == 1 ? true : false);
	}

	PQclear(res);

	/* Fetch locations of pg_hba.conf and pg_ident.conf */
	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  "  WITH dd AS ( "
					  "    SELECT setting AS data_directory"
					  "      FROM pg_catalog.pg_settings "
					  "     WHERE name = 'data_directory' "
					  "  ) "
					  "    SELECT ps.setting, "
					  "           regexp_replace(setting, '^.*\\/', '') AS filename, "
					  "           ps.setting ~ ('^' || dd.data_directory) AS in_data_dir "
					  "      FROM dd, pg_catalog.pg_settings ps "
					  "     WHERE ps.name IN ('hba_file', 'ident_file') "
					  "  ORDER BY 1 ");

	log_debug("standby clone: %s", query.data);
	res = PQexec(privileged_conn, query.data);
	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to retrieve configuration file locations:\n  %s"),
				  PQerrorMessage(privileged_conn));
		PQclear(res);
		PQfinish(source_conn);

		if (superuser_conn != NULL)
			PQfinish(superuser_conn);

		exit(ERR_BAD_CONFIG);
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		config_file_list_add(&config_files,
							 PQgetvalue(res, i, 0),
							 PQgetvalue(res, i, 1),
							 strcmp(PQgetvalue(res, i, 2), "t") == 1 ? true : false);
	}

	PQclear(res);

	/*
	 * If replication slots requested, create appropriate slot on
	 * the primary; this must be done before pg_start_backup() is
	 * issued, either by us or by pg_basebackup.
	 *
	 * Replication slots are not supported (and not very useful
	 * anyway) in Barman mode.
	 */

	if (config_file_options.use_replication_slots)
	{
		PQExpBufferData event_details;
		initPQExpBuffer(&event_details);

		if (create_replication_slot(privileged_conn, repmgr_slot_name, server_version_num, &event_details) == false)
		{
			log_error("%s", event_details.data);

			create_event_record(primary_conn,
								&config_file_options,
								config_file_options.node_id,
								"standby_clone",
								false,
								event_details.data);

			PQfinish(source_conn);

			if (superuser_conn != NULL)
				PQfinish(superuser_conn);

			exit(ERR_DB_QUERY);
		}

		termPQExpBuffer(&event_details);

		log_notice(_("replication slot \"%s\" created on upstream node (node_id: %i)"),
				   repmgr_slot_name,
				   upstream_node_id);
	}

	if (superuser_conn != NULL)
		PQfinish(superuser_conn);

	return;
}


static int
run_basebackup(void)
{
	char				  script[MAXLEN];
	int					  r = 0;
	PQExpBufferData 	  params;
	TablespaceListCell   *cell;
	t_basebackup_options  backup_options = T_BASEBACKUP_OPTIONS_INITIALIZER;

	/*
	 * Parse the pg_basebackup_options provided in repmgr.conf - we'll want
	 * to check later whether certain options were set by the user
	 */
	parse_pg_basebackup_options(config_file_options.pg_basebackup_options,
								&backup_options,
								server_version_num,
								NULL);

	/* Create pg_basebackup command line options */

	initPQExpBuffer(&params);

	appendPQExpBuffer(&params, " -D %s", local_data_directory);

	/*
	 * conninfo string provided - pass it to pg_basebackup as the -d option
	 * (pg_basebackup doesn't require or want a database name, but for
	 * consistency with other applications accepts a conninfo string
	 * under -d/--dbname)
	 */
	if (runtime_options.conninfo_provided == true)
	{
		t_conninfo_param_list conninfo;
		char *conninfo_str;

		initialize_conninfo_params(&conninfo, false);

		/* string will already have been parsed */
		(void) parse_conninfo_string(runtime_options.dbname, &conninfo, NULL, false);

		if (*runtime_options.replication_user)
			param_set(&conninfo, "user", runtime_options.replication_user);

		conninfo_str = param_list_to_string(&conninfo);

		appendPQExpBuffer(&params, " -d '%s'", conninfo_str);

		pfree(conninfo_str);
	}

	/*
	 * Connection parameters not passed to repmgr as conninfo string - provide
	 * them individually to pg_basebackup (-d/--dbname not required)
	 */
	else
	{
		if (strlen(runtime_options.host))
		{
			appendPQExpBuffer(&params, " -h %s", runtime_options.host);
		}

		if (strlen(runtime_options.port))
		{
			appendPQExpBuffer(&params, " -p %s", runtime_options.port);
		}

		if (strlen(runtime_options.replication_user))
		{
			appendPQExpBuffer(&params, " -U %s", runtime_options.replication_user);
		}
		else if (strlen(runtime_options.username))
		{
			appendPQExpBuffer(&params, " -U %s", runtime_options.username);
		}
	}

	if (runtime_options.fast_checkpoint) {
		appendPQExpBuffer(&params, " -c fast");
	}

	if (config_file_options.tablespace_mapping.head != NULL)
	{
		for (cell = config_file_options.tablespace_mapping.head; cell; cell = cell->next)
		{
			appendPQExpBuffer(&params, " -T %s=%s", cell->old_dir, cell->new_dir);
		}
	}

	/*
	 * To ensure we have all the WALs needed during basebackup execution we stream
	 * them as the backup is taking place.
	 *
	 * From 9.6, if replication slots are in use, we'll have previously
	 * created a slot with reserved LSN, and will stream from that slot to avoid
	 * WAL buildup on the master using the -S/--slot, which requires -X/--xlog-method=stream
	 * (from 10, -X/--wal-method=stream)
	 */
	if (!strlen(backup_options.xlog_method))
	{
		appendPQExpBuffer(&params, " -X stream");
	}

	/*
	 * From 9.6, pg_basebackup accepts -S/--slot, which forces WAL streaming to use
	 * the specified replication slot. If replication slot usage is specified, the
	 * slot will already have been created.
	 *
	 * NOTE: currently there's no way of disabling the --slot option while using
	 *   --xlog-method=stream - it's hard to imagine a use case for this, so no
	 *   provision has been made for doing it.
	 *
	 * NOTE:
	 *   It's possible to set 'pg_basebackup_options' with an invalid combination
	 *   of values for --wal-method (--xlog-method) and --slot - we're not checking that, just that
	 *   we're not overriding any user-supplied values
	 */
	if (server_version_num >= 90600 && config_file_options.use_replication_slots)
	{
		bool slot_add = true;

		/*
		 * Check whether 'pg_basebackup_options' in repmgr.conf has the --slot option set,
		 * or if --wal-method (--xlog-method) is set to a value other than "stream"
		 * (in which case we can't use --slot).
		 */
		if (strlen(backup_options.slot) || (strlen(backup_options.xlog_method) && strcmp(backup_options.xlog_method, "stream") != 0)) {
			slot_add = false;
		}

		if (slot_add == true)
		{
			appendPQExpBuffer(&params, " -S %s", repmgr_slot_name_ptr);
		}
	}

	maxlen_snprintf(script,
					"%s -l \"repmgr base backup\" %s %s",
					make_pg_path("pg_basebackup"),
					params.data,
					config_file_options.pg_basebackup_options);

	termPQExpBuffer(&params);

	log_info(_("executing: '%s'"), script);

	/*
	 * As of 9.4, pg_basebackup only ever returns 0 or 1
	 * XXX check for 10
	 */

	r = system(script);

	if (r !=0)
		return r;


	return r;
}


static int
run_file_backup(void)
{
	int r = 0, i;

	char		command[MAXLEN];
	char		filename[MAXLEN];
	char		buf[MAXLEN];
	char		backup_directory[MAXLEN];
	char        backup_id[MAXLEN] = "";
	char       *p, *q;
	PQExpBufferData command_output;
	TablespaceDataList tablespace_list = { NULL, NULL };
	TablespaceDataListCell *cell_t;

	PQExpBufferData tablespace_map;
	bool		tablespace_map_rewrite = false;



	if (mode == barman)
	{

		/*
		 * Locate Barman's backup directory
		 */

		get_barman_property(backup_directory, "backup_directory", local_repmgr_tmp_directory);

		/*
		 * Read the list of backup files into a local file. In the
		 * process:
		 *
		 * - determine the backup ID;
		 * - check, and remove, the prefix;
		 * - detect tablespaces;
		 * - filter files in one list per tablespace;
		 */

		{
			FILE *fi; /* input stream */
			FILE *fd; /* output for data.txt */
			char prefix[MAXLEN];
			char output[MAXLEN];
			int n;

			maxlen_snprintf(command, "%s list-files --target=data %s latest",
							make_barman_ssh_command(barman_command_buf),
							config_file_options.barman_server);

			fi = popen(command, "r");
			if (fi == NULL)
			{
				log_error("cannot launch command: %s", command);
				exit(ERR_BARMAN);
			}

			fd = fopen(datadir_list_filename, "w");
			if (fd == NULL)
			{
				log_error("cannot open file: %s", datadir_list_filename);
				exit(ERR_INTERNAL);
			}

			maxlen_snprintf(prefix, "%s/base/", backup_directory);
			while (fgets(output, MAXLEN, fi) != NULL)
			{
				/*
				 * Remove prefix
				 */
				p = string_skip_prefix(prefix, output);
				if (p == NULL)
				{
					log_error("unexpected output from \"barman list-files\": %s",
							output);
					exit(ERR_BARMAN);
				}

				/*
				 * Remove and note backup ID; copy backup.info
				 */
				if (! strcmp(backup_id, ""))
				{
					FILE *fi2;

					n = strcspn(p, "/");

					strncpy(backup_id, p, n);

					strncat(prefix,backup_id,MAXLEN-1);
					strncat(prefix,"/",MAXLEN-1);
					p = string_skip_prefix(backup_id, p);
					p = string_skip_prefix("/", p);

					/*
					 * Copy backup.info
					 */
					maxlen_snprintf(command,
									"rsync -a %s:%s/base/%s/backup.info %s",
									config_file_options.barman_host,
									backup_directory,
									backup_id,
									local_repmgr_tmp_directory);
					(void)local_command(
						command,
						&command_output);

					/*
					 * Get tablespace data
					 */
					maxlen_snprintf(filename, "%s/backup.info",
									local_repmgr_tmp_directory);
					fi2 = fopen(filename, "r");
					if (fi2 == NULL)
					{
						log_error("cannot open file: %s", filename);
						exit(ERR_INTERNAL);
					}
					while (fgets(buf, MAXLEN, fi2) != NULL)
					{
						q = string_skip_prefix("tablespaces=", buf);
						if (q != NULL && strncmp(q, "None\n", 5))
						{
							get_tablespace_data_barman(q, &tablespace_list);
						}
						q = string_skip_prefix("version=", buf);
						if (q != NULL)
						{
							server_version_num = strtol(q, NULL, 10);
						}
					}
					fclose(fi2);
					unlink(filename);

					continue;
				}

				/*
				 * Skip backup.info
				 */
				if (string_skip_prefix("backup.info", p))
					continue;

				/*
				 * Filter data directory files
				 */
				if ((q = string_skip_prefix("data/", p)) != NULL)
				{
					fputs(q, fd);
					continue;
				}

				/*
				 * Filter other files (i.e. tablespaces)
				 */
				for (cell_t = tablespace_list.head; cell_t; cell_t = cell_t->next)
				{
					if ((q = string_skip_prefix(cell_t->oid, p)) != NULL && *q == '/')
					{
						if (cell_t->f == NULL)
						{
							maxlen_snprintf(filename, "%s/%s.txt", local_repmgr_tmp_directory, cell_t->oid);
							cell_t->f = fopen(filename, "w");
							if (cell_t->f == NULL)
							{
								log_error("cannot open file: %s", filename);
								exit(ERR_INTERNAL);
							}
						}
						fputs(q + 1, cell_t->f);
						break;
					}
				}
			}

			fclose(fd);

			pclose(fi);
		}

		/* For 9.5 and greater, create our own tablespace_map file */
		if (server_version_num >= 90500)
		{
			initPQExpBuffer(&tablespace_map);
		}

		/*
		 * As of Barman version 1.6.1, the file structure of a backup
		 * is as follows:
		 *
		 * base/ - base backup
		 * wals/ - WAL files associated to the backup
		 *
		 * base/<ID> - backup files
		 *
		 *   here ID has the standard timestamp form yyyymmddThhmmss
		 *
		 * base/<ID>/backup.info - backup metadata, in text format
		 * base/<ID>/data        - data directory
		 * base/<ID>/<OID>       - tablespace with the given oid
		 */

		/*
		 * Copy all backup files from the Barman server
		 */
		maxlen_snprintf(command,
						"rsync --progress -a --files-from=%s %s:%s/base/%s/data %s",
						datadir_list_filename,
						config_file_options.barman_host,
						backup_directory,
						backup_id,
						local_data_directory);

		(void)local_command(
			command,
			&command_output);

		unlink(datadir_list_filename);

		/*
		 * We must create some PGDATA subdirectories because they are
		 * not included in the Barman backup.
		 *
		 * See class RsyncBackupExecutor in the Barman source (barman/backup_executor.py)
		 * for a definitive list of excluded directories.
		 */
		{
			const char* const dirs[] = {
				/* Only from 10 */
				"pg_wal",
				/* Only from 9.5 */
				"pg_commit_ts",
				/* Only from 9.4 */
				"pg_dynshmem", "pg_logical", "pg_logical/snapshots", "pg_logical/mappings", "pg_replslot",
				/* Already in 9.3 */
				"pg_notify", "pg_serial", "pg_snapshots", "pg_stat", "pg_stat_tmp", "pg_tblspc",
				"pg_twophase", "pg_xlog", 0
			};
			const int vers[] = {
				100000,
				90500,
				90400, 90400, 90400, 90400, 90400,
				0, 0, 0, 0, 0, 0,
				0, -100000, 0
			};
			for (i = 0; dirs[i]; i++)
			{
				/* directory exists in newer versions than this server - skip */
				if (vers[i] > 0 && server_version_num < vers[i])
					continue;

				/* directory existed in earlier versions than this server but has been removed/renamed - skip */
				if (vers[i] < 0 && server_version_num >= abs(vers[i]))
					continue;

				maxlen_snprintf(filename, "%s/%s", local_data_directory, dirs[i]);
				if (mkdir(filename, S_IRWXU) != 0 && errno != EEXIST)
				{
					log_error(_("unable to create the %s directory"), dirs[i]);
					exit(ERR_INTERNAL);
				}
			}
		}
	}
	else if (mode == rsync)
	{
		/* For 9.5 and greater, create our own tablespace_map file */
		if (server_version_num >= 90500)
		{
			initPQExpBuffer(&tablespace_map);
		}

		if (start_backup(source_conn, first_wal_segment, runtime_options.fast_checkpoint, server_version_num) == false)
		{
			r = ERR_BAD_BASEBACKUP;
			goto stop_backup;
		}

		/*
		 * Note that we've successfully executed pg_start_backup(),
		 * so we know whether or not to execute pg_stop_backup() after
		 * the 'stop_backup' label
		 */
		pg_start_backup_executed = true;

		/*
		 * 1. copy data directory, omitting directories which should not be
		 *    copied, or for which copying would serve no purpose.
		 *
		 * 2. copy pg_control file
		 */

		/* Copy the data directory */
		log_info(_("standby clone: upstream data directory is '%s'"),
				 upstream_data_directory);
		r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
							  upstream_data_directory, local_data_directory,
							  true, server_version_num);
		/*
		 * Exit code 0 means no error, but we want to ignore exit code 24 as well
		 * as rsync returns that code on "Partial transfer due to vanished source files".
		 * It's quite common for this to happen on the data directory, particularly
		 * with long running rsync on a busy server.
		 */
		if (WIFEXITED(r) && WEXITSTATUS(r) && WEXITSTATUS(r) != 24)
		{
			log_error(_("standby clone: failed copying upstream data directory '%s'"),
					upstream_data_directory);
			r = ERR_BAD_RSYNC;
			goto stop_backup;
		}

		/* Read backup label copied from primary */
		if (read_backup_label(local_data_directory, &backup_label) == false)
		{
			r = ERR_BAD_BACKUP_LABEL;
			goto stop_backup;
		}

		/* Copy tablespaces and, if required, remap to a new location */
		r = get_tablespace_data(source_conn, &tablespace_list);
		if (r != SUCCESS) goto stop_backup;
	}

	for (cell_t = tablespace_list.head; cell_t; cell_t = cell_t->next)
	{
		bool mapping_found = false;
		TablespaceListCell *cell;
		char *tblspc_dir_dest;

		/* Check if tablespace path matches one of the provided tablespace mappings */
		if (config_file_options.tablespace_mapping.head != NULL)
		{
			for (cell = config_file_options.tablespace_mapping.head; cell; cell = cell->next)
			{
				if (strcmp(cell_t->location, cell->old_dir) == 0)
				{
					mapping_found = true;
					break;
				}
			}
		}

		if (mapping_found == true)
		{
			tblspc_dir_dest = cell->new_dir;
			log_debug(_("mapping source tablespace '%s' (OID %s) to '%s'"),
					  cell_t->location, cell_t->oid, tblspc_dir_dest);
		}
		else
		{
			tblspc_dir_dest = cell_t->location;
		}

		/*
		 * Tablespace file copy
		 */

		if (mode == barman)
		{
			create_pg_dir(cell_t->location, false);

			if (cell_t->f != NULL) /* cell_t->f == NULL iff the tablespace is empty */
			{
				maxlen_snprintf(command,
								"rsync --progress -a --files-from=%s/%s.txt %s:%s/base/%s/%s %s",
								local_repmgr_tmp_directory,
								cell_t->oid,
								config_file_options.barman_host,
								backup_directory,
								backup_id,
								cell_t->oid,
								tblspc_dir_dest);
				(void)local_command(
					command,
					&command_output);
				fclose(cell_t->f);
				maxlen_snprintf(filename,
								"%s/%s.txt",
								local_repmgr_tmp_directory,
								cell_t->oid);
				unlink(filename);
			}
		}
		else if (mode == rsync)
		{
			/* Copy tablespace directory */
			r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
								  cell_t->location, tblspc_dir_dest,
								  true, server_version_num);

			/*
			 * Exit code 0 means no error, but we want to ignore exit code 24 as well
			 * as rsync returns that code on "Partial transfer due to vanished source files".
			 * It's quite common for this to happen on the data directory, particularly
			 * with long running rsync on a busy server.
			 */
			if (WIFEXITED(r) && WEXITSTATUS(r) && WEXITSTATUS(r) != 24)
			{
				log_error(_("standby clone: failed copying tablespace directory '%s'"),
						cell_t->location);
				r  = ERR_BAD_RSYNC;
				goto stop_backup;
			}
		}

		/*
		 * If a valid mapping was provide for this tablespace, arrange for it to
		 * be remapped
		 * (if no tablespace mapping was provided, the link will be copied as-is
		 * by pg_basebackup or rsync and no action is required)
		 */
		if (mapping_found == true || mode == barman)
		{
			/* 9.5 and later - append to the tablespace_map file */
			if (server_version_num >= 90500)
			{
				tablespace_map_rewrite = true;
				appendPQExpBuffer(&tablespace_map,
								  "%s %s\n",
								  cell_t->oid,
								  tblspc_dir_dest);
			}
			/* Pre-9.5, we have to manipulate the symlinks in pg_tblspc/ ourselves */
			else
			{
				PQExpBufferData tblspc_symlink;

				initPQExpBuffer(&tblspc_symlink);
				appendPQExpBuffer(&tblspc_symlink, "%s/pg_tblspc/%s",
								  local_data_directory,
								  cell_t->oid);

				if (unlink(tblspc_symlink.data) < 0 && errno != ENOENT)
				{
					log_error(_("unable to remove tablespace symlink %s"), tblspc_symlink.data);

					r = ERR_BAD_BASEBACKUP;
					goto stop_backup;
				}

				if (symlink(tblspc_dir_dest, tblspc_symlink.data) < 0)
				{
					log_error(_("unable to create tablespace symlink from %s to %s"), tblspc_symlink.data, tblspc_dir_dest);

					r = ERR_BAD_BASEBACKUP;
					goto stop_backup;
				}
			}
		}
	}

	/*
	 * For 9.5 and later, if tablespace remapping was requested, we'll need
	 * to rewrite the tablespace map file ourselves.
	 * The tablespace map file is read on startup and any links created by
	 * the backend; we could do this ourselves like for pre-9.5 servers, but
	 * it's better to rely on functionality the backend provides.
	 */

	if (server_version_num >= 90500 && tablespace_map_rewrite == true)
	{
		PQExpBufferData tablespace_map_filename;
		FILE	   *tablespace_map_file;
		initPQExpBuffer(&tablespace_map_filename);
		appendPQExpBuffer(&tablespace_map_filename, "%s/%s",
						  local_data_directory,
						  TABLESPACE_MAP);

		/* Unlink any existing file (it should be there, but we don't care if it isn't) */
		if (unlink(tablespace_map_filename.data) < 0 && errno != ENOENT)
		{
			log_error(_("unable to remove tablespace_map file %s: %s"),
					  tablespace_map_filename.data,
					  strerror(errno));

			r = ERR_BAD_BASEBACKUP;
			goto stop_backup;
		}

		tablespace_map_file = fopen(tablespace_map_filename.data, "w");
		if (tablespace_map_file == NULL)
		{
			log_error(_("unable to create tablespace_map file '%s'"), tablespace_map_filename.data);

			r = ERR_BAD_BASEBACKUP;
			goto stop_backup;
		}

		if (fputs(tablespace_map.data, tablespace_map_file) == EOF)
		{
			log_error(_("unable to write to tablespace_map file '%s'"), tablespace_map_filename.data);

			r  = ERR_BAD_BASEBACKUP;
			goto stop_backup;
		}

		fclose(tablespace_map_file);
	}


	/*
	 * When using rsync, copy pg_control file last, emulating the base backup
	 * protocol.
	 */
	if (mode == rsync)
	{
		char		upstream_control_file[MAXPGPATH] = "";
		char		local_control_file[MAXPGPATH] = "";

		maxlen_snprintf(local_control_file, "%s/global", local_data_directory);

		log_info(_("standby clone: local control file '%s'"),
				 local_control_file);

		if (!create_dir(local_control_file))
		{
			log_error(_("couldn't create directory %s"),
					local_control_file);
			goto stop_backup;
		}

		maxlen_snprintf(upstream_control_file, "%s/global/pg_control",
						upstream_data_directory);
		log_debug("standby clone: upstream control file is \"%s\"",
				 upstream_control_file);

		r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
							  upstream_control_file, local_control_file,
							  false, server_version_num);
		if (WEXITSTATUS(r))
		{
			log_warning(_("standby clone: failed copying upstreamcontrol file \"%s\""),
						upstream_control_file);
			r = ERR_BAD_SSH;
			goto stop_backup;
		}
	}

stop_backup:

	if (mode == rsync && pg_start_backup_executed)
	{
		log_notice(_("notifying upstream about backup completion"));
		if (stop_backup(source_conn, last_wal_segment, server_version_num) == false)
		{
			r = ERR_BAD_BASEBACKUP;
		}
	}


	/* clean up copied data directory */
	if (mode == rsync)
	{
		cleanup_data_directory();
	}
	else if (mode == barman)
	{
		/* In Barman mode, remove local_repmgr_directory */
		rmtree(local_repmgr_tmp_directory, true);
	}

	return r;
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
					config_file_options.barman_host,
					config_opt);

	return buf;
}


static int
get_tablespace_data_barman
( char *tablespace_data_barman,
  TablespaceDataList *tablespace_list)
{
	/*
	 * Example:
	 * [('main', 24674, '/var/lib/postgresql/tablespaces/9.5/main'), ('alt', 24678, '/var/lib/postgresql/tablespaces/9.5/alt')]
	 */

	char name[MAXLEN];
	char oid[MAXLEN];
	char location[MAXPGPATH];
	char *p = tablespace_data_barman;
	int i;

	tablespace_list->head = NULL;
	tablespace_list->tail = NULL;

	p = string_skip_prefix("[", p);
	if (p == NULL) return -1;

	while (*p == '(')
	{
		p = string_skip_prefix("('", p);
		if (p == NULL) return -1;

		i = strcspn(p, "'");
		strncpy(name, p, i);
		name[i] = 0;

		p = string_skip_prefix("', ", p + i);
		if (p == NULL) return -1;

		i = strcspn(p, ",");
		strncpy(oid, p, i);
		oid[i] = 0;

		p = string_skip_prefix(", '", p + i);
		if (p == NULL) return -1;

		i = strcspn(p, "'");
		strncpy(location, p, i);
		location[i] = 0;

		p = string_skip_prefix("')", p + i);
		if (p == NULL) return -1;

		tablespace_data_append (tablespace_list, name, oid, location);

		if (*p == ']')
			break;

		p = string_skip_prefix(", ", p);
		if (p == NULL) return -1;
	}

	return SUCCESS;
}


void
get_barman_property(char *dst, char *name, char *local_repmgr_directory)
{
	PQExpBufferData command_output;
	char buf[MAXLEN];
	char command[MAXLEN];
	char *p;

	initPQExpBuffer(&command_output);

	maxlen_snprintf(command,
					"grep \"^\t%s:\" %s/show-server.txt",
					name, local_repmgr_tmp_directory);
	(void)local_command(command, &command_output);

	maxlen_snprintf(buf, "\t%s: ", name);
	p = string_skip_prefix(buf, command_output.data);
	if (p == NULL)
	{
		log_error("unexpected output from Barman: %s",
				  command_output.data);
		exit(ERR_INTERNAL);
	}

	strncpy(dst, p, MAXLEN);
	string_remove_trailing_newlines(dst);

	termPQExpBuffer(&command_output);
}

static void
config_file_list_init(t_configfile_list *list, int max_size)
{
	list->size = max_size;
	list->entries = 0;
	list->files = pg_malloc0(sizeof(t_configfile_info *) * max_size);

	if (list->files == NULL)
	{
		log_error(_("unable to allocate memory; terminating"));
		exit(ERR_OUT_OF_MEMORY);
	}
}


static void
config_file_list_add(t_configfile_list *list, const char *file, const char *filename, bool in_data_dir)
{
	/* Failsafe to prevent entries being added beyond the end */
	if (list->entries == list->size)
		return;

	list->files[list->entries] = pg_malloc0(sizeof(t_configfile_info));

	if (list->files[list->entries] == NULL)
	{
		log_error(_("unable to allocate memory; terminating"));
		exit(ERR_OUT_OF_MEMORY);
	}


	strncpy(list->files[list->entries]->filepath, file, MAXPGPATH);
	canonicalize_path(list->files[list->entries]->filepath);


	strncpy(list->files[list->entries]->filename, filename, MAXPGPATH);
	list->files[list->entries]->in_data_directory = in_data_dir;

	list->entries ++;
}

static void
copy_configuration_files(void)
{
	int i, r;
	t_configfile_info *file;
	char *host;

	/* get host from upstream record */
	host = param_get(&recovery_conninfo, "host");

	if (host == NULL)
		host = runtime_options.host;

	log_verbose(LOG_DEBUG, "fetching configuration files from host \"%s\"", host);
	log_notice(_("copying external configuration files from upstream node"));

	r = test_ssh_connection(host, runtime_options.remote_user);
	if (r != 0)
	{
		log_error(_("remote host %s is not reachable via SSH - unable to copy external configuration files"),
				  host);
		return;
	}

	for (i = 0; i < config_files.entries; i++)
	{
		char dest_path[MAXPGPATH];
		file = config_files.files[i];

		/*
		 * Skip files in the data directory - these will be copied during
		 * the main backup
		 */
		if (file->in_data_directory == true)
			continue;

		if (runtime_options.copy_external_config_files_destination == CONFIG_FILE_SAMEPATH)
		{
			strncpy(dest_path, file->filepath, MAXPGPATH);
		}
		else
		{
			snprintf(dest_path, MAXPGPATH,
					 "%s/%s",
					 local_data_directory,
					 file->filename);
		}

		r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
							  file->filepath, dest_path, false, server_version_num);
		if (WEXITSTATUS(r))
		{
			log_error(_("standby clone: unable to copy config file \"%s\""),
					  file->filename);
		}
	}

	return;
}



static int
get_tablespace_data(PGconn *upstream_conn, TablespaceDataList *list)
{
	PQExpBufferData	  query;
	PGresult *res;
	int i;

	initPQExpBuffer(&query);

	appendPQExpBuffer(&query,
					  " SELECT spcname, oid, pg_catalog.pg_tablespace_location(oid) AS spclocation "
					  "   FROM pg_catalog.pg_tablespace "
					  "  WHERE spcname NOT IN ('pg_default', 'pg_global')");

	res = PQexec(upstream_conn, query.data);

	termPQExpBuffer(&query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error(_("unable to execute tablespace query:\n  %s"),
				  PQerrorMessage(upstream_conn));

		PQclear(res);

		return ERR_DB_QUERY;
	}

	for (i = 0; i < PQntuples(res); i++)
		tablespace_data_append(list,
							   PQgetvalue(res, i, 0),
							   PQgetvalue(res, i, 1),
							   PQgetvalue(res, i, 2));

	PQclear(res);
	return SUCCESS;
}

static void
tablespace_data_append(TablespaceDataList *list, const char *name, const char *oid, const char *location)
{
	TablespaceDataListCell *cell;

	cell = (TablespaceDataListCell *) pg_malloc0(sizeof(TablespaceDataListCell));

	if (cell == NULL)
	{
		log_error(_("unable to allocate memory; terminating"));
		exit(ERR_OUT_OF_MEMORY);
	}

	cell->oid      = pg_malloc(1 + strlen(oid     ));
	cell->name     = pg_malloc(1 + strlen(name    ));
	cell->location = pg_malloc(1 + strlen(location));

	strncpy(cell->oid     , oid     , 1 + strlen(oid     ));
	strncpy(cell->name    , name    , 1 + strlen(name    ));
	strncpy(cell->location, location, 1 + strlen(location));

	if (list->tail)
		list->tail->next = cell;
	else
		list->head = cell;

	list->tail = cell;
}



static void
parse_lsn(XLogRecPtr *ptr, const char *str)
{
	uint32 high, low;

	if (sscanf(str, "%x/%x", &high, &low) != 2)
		return;

	*ptr = (((XLogRecPtr)high) << 32) + (XLogRecPtr)low;

	return;
}


static XLogRecPtr
parse_label_lsn(const char *label_key, const char *label_value)
{
	XLogRecPtr ptr = InvalidXLogRecPtr;

	parse_lsn(&ptr, label_value);

	/* parse_lsn() will not modify ptr if it can't parse the label value */
	if (ptr == InvalidXLogRecPtr)
	{
		log_error(_("couldn't parse backup label entry \"%s: %s\" as lsn"),
				  label_key, label_value);
	}

	return ptr;
}



/*======================================
 * Read entries of interest from the backup label.
 *
 * Sample backup label (with failover slots):
 *
 *		START WAL LOCATION: 0/6000028 (file 000000010000000000000006)
 *		CHECKPOINT LOCATION: 0/6000060
 *		BACKUP METHOD: streamed
 *		BACKUP FROM: master
 *		START TIME: 2016-03-30 12:18:12 AWST
 *		LABEL: pg_basebackup base backup
 *		MIN FAILOVER SLOT LSN: 0/5000000
 *
 *======================================
 */
static bool
read_backup_label(const char *local_data_directory, struct BackupLabel *out_backup_label)
{
	char label_path[MAXPGPATH];
	FILE *label_file;
	int  nmatches = 0;

	char line[MAXLEN];

	out_backup_label->start_wal_location = InvalidXLogRecPtr;
	out_backup_label->start_wal_file[0] = '\0';
	out_backup_label->checkpoint_location = InvalidXLogRecPtr;
	out_backup_label->backup_from[0] = '\0';
	out_backup_label->backup_method[0] = '\0';
	out_backup_label->start_time[0] = '\0';
	out_backup_label->label[0] = '\0';
	out_backup_label->min_failover_slot_lsn = InvalidXLogRecPtr;

	maxlen_snprintf(label_path, "%s/backup_label", local_data_directory);

	label_file = fopen(label_path, "r");
	if (label_file == NULL)
	{
		log_error(_("read_backup_label: could not open backup label file %s: %s"),
				label_path, strerror(errno));
		return false;
	}

	log_info(_("read_backup_label: parsing backup label file '%s'"),
			 label_path);

	while(fgets(line, sizeof line, label_file) != NULL)
	{
		char label_key[MAXLEN];
		char label_value[MAXLEN];
		char newline;

		nmatches = sscanf(line, "%" MAXLEN_STR "[^:]: %" MAXLEN_STR "[^\n]%c",
						  label_key, label_value, &newline);

		if (nmatches != 3)
			break;

		if (newline != '\n')
		{
			log_error(_("read_backup_label: line too long in backup label file. Line begins \"%s: %s\""),
					  label_key, label_value);
			return false;
		}

		log_verbose(LOG_DEBUG, "standby clone: got backup label entry \"%s: %s\"",
					label_key, label_value);

		if (strcmp(label_key, "START WAL LOCATION") == 0)
		{
			char start_wal_location[MAXLEN];
			char wal_filename[MAXLEN];

			nmatches = sscanf(label_value, "%" MAXLEN_STR "s (file %" MAXLEN_STR "[^)]", start_wal_location, wal_filename);

			if (nmatches != 2)
			{
				log_error(_("read_backup_label: unable to parse \"START WAL LOCATION\" in backup label"));
				return false;
			}

			out_backup_label->start_wal_location =
				parse_label_lsn(&label_key[0], start_wal_location);

			if (out_backup_label->start_wal_location == InvalidXLogRecPtr)
				return false;

			(void) strncpy(out_backup_label->start_wal_file, wal_filename, MAXLEN);
			out_backup_label->start_wal_file[MAXLEN-1] = '\0';
		}
		else if (strcmp(label_key, "CHECKPOINT LOCATION") == 0)
		{
			out_backup_label->checkpoint_location =
				parse_label_lsn(&label_key[0], &label_value[0]);

			if (out_backup_label->checkpoint_location == InvalidXLogRecPtr)
				return false;
		}
		else if (strcmp(label_key, "BACKUP METHOD") == 0)
		{
			(void) strncpy(out_backup_label->backup_method, label_value, MAXLEN);
			out_backup_label->backup_method[MAXLEN-1] = '\0';
		}
		else if (strcmp(label_key, "BACKUP FROM") == 0)
		{
			(void) strncpy(out_backup_label->backup_from, label_value, MAXLEN);
			out_backup_label->backup_from[MAXLEN-1] = '\0';
		}
		else if (strcmp(label_key, "START TIME") == 0)
		{
			(void) strncpy(out_backup_label->start_time, label_value, MAXLEN);
			out_backup_label->start_time[MAXLEN-1] = '\0';
		}
		else if (strcmp(label_key, "LABEL") == 0)
		{
			(void) strncpy(out_backup_label->label, label_value, MAXLEN);
			out_backup_label->label[MAXLEN-1] = '\0';
		}
		else if (strcmp(label_key, "MIN FAILOVER SLOT LSN") == 0)
		{
			out_backup_label->min_failover_slot_lsn =
				parse_label_lsn(&label_key[0], &label_value[0]);

			if (out_backup_label->min_failover_slot_lsn == InvalidXLogRecPtr)
				return false;
		}
		else
		{
			log_info("read_backup_label: ignored unrecognised backup label entry \"%s: %s\"",
					label_key, label_value);
		}
	}

	(void) fclose(label_file);

	log_debug("read_backup_label: label is %s; start wal file is %s",
			  out_backup_label->label, out_backup_label->start_wal_file);

	return true;
}


static void
cleanup_data_directory(void)
{
	char	dirpath[MAXLEN] = "";

	if (runtime_options.force)
	{
		/*
		 * Remove any WAL files in the target directory which might have
		 * been left over from previous use of this data directory;
		 * rsync's --exclude option won't do this.
		 */

		if (server_version_num >= 100000)
			maxlen_snprintf(dirpath, "%s/pg_wal/", local_data_directory);
		else
			maxlen_snprintf(dirpath, "%s/pg_xlog/", local_data_directory);

		if (!rmtree(dirpath, false))
		{
			log_error(_("unable to empty local WAL directory %s"),
					dirpath);
			exit(ERR_BAD_RSYNC);
		}
	}

	/*
	 * Remove any existing replication slot directories from previous use
	 * of this data directory; this matches the behaviour of a fresh
	 * pg_basebackup, which would usually result in an empty pg_replslot
	 * directory.
	 *
	 * If the backup label contains a nonzero
	 * 'MIN FAILOVER SLOT LSN' entry we retain the slots and let
	 * the server clean them up instead, matching pg_basebackup's
	 * behaviour when failover slots are enabled.
	 *
	 * NOTE: watch out for any changes in the replication
	 * slot directory name (as of 9.4: "pg_replslot") and
	 * functionality of replication slots
	 */

	if (server_version_num >= 90400 &&
		backup_label.min_failover_slot_lsn == InvalidXLogRecPtr)
	{
		maxlen_snprintf(dirpath, "%s/pg_replslot/",
						local_data_directory);

		log_debug("deleting pg_replslot directory contents");

		if (!rmtree(dirpath, false))
		{
			log_error(_("unable to empty replication slot directory \"%s\""),
					  dirpath);
			exit(ERR_BAD_RSYNC);
		}
	}
}
