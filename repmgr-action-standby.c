/*
 * repmgr-action-standby.c
 *
 * Implements standby actions for the repmgr command line utility
 *
 * Copyright (c) 2ndQuadrant, 2010-2018
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <sys/stat.h>

#include "repmgr.h"
#include "dirutil.h"
#include "compat.h"
#include "controldata.h"

#include "repmgr-client-global.h"
#include "repmgr-action-standby.h"


typedef struct TablespaceDataListCell
{
	struct TablespaceDataListCell *next;
	char	   *name;
	char	   *oid;
	char	   *location;
	/* optional payload */
	FILE	   *f;
} TablespaceDataListCell;

typedef struct TablespaceDataList
{
	TablespaceDataListCell *head;
	TablespaceDataListCell *tail;
} TablespaceDataList;


static PGconn *primary_conn = NULL;
static PGconn *source_conn = NULL;

static char local_data_directory[MAXPGPATH] = "";
static bool local_data_directory_provided = false;

static bool upstream_conninfo_found = false;
static int	upstream_node_id = UNKNOWN_NODE_ID;
static char upstream_data_directory[MAXPGPATH];

static t_conninfo_param_list recovery_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;
static char recovery_conninfo_str[MAXLEN] = "";
static char upstream_repluser[NAMEDATALEN] = "";
static char upstream_user[NAMEDATALEN] = "";

static int	source_server_version_num = UNKNOWN_SERVER_VERSION_NUM;

static t_configfile_list config_files = T_CONFIGFILE_LIST_INITIALIZER;

static standy_clone_mode mode = pg_basebackup;

/* used by barman mode */
static char local_repmgr_tmp_directory[MAXPGPATH];
static char datadir_list_filename[MAXLEN];
static char barman_command_buf[MAXLEN] = "";

static void _do_standby_promote_internal(PGconn *conn);
static void _do_create_recovery_conf(void);

static void check_barman_config(void);
static void check_source_server(void);
static void check_source_server_via_barman(void);
static bool check_upstream_config(PGconn *conn, int server_version_num, t_node_info *node_info, bool exit_on_error);
static void check_primary_standby_version_match(PGconn *conn, PGconn *primary_conn);
static void check_recovery_type(PGconn *conn);

static void initialise_direct_clone(t_node_info *node_record);
static int	run_basebackup(t_node_info *node_record);
static int	run_file_backup(t_node_info *node_record);

static void copy_configuration_files(bool delete_after_copy);

static void drop_replication_slot_if_exists(PGconn *conn, int node_id, char *slot_name);

static void tablespace_data_append(TablespaceDataList *list, const char *name, const char *oid, const char *location);

static void get_barman_property(char *dst, char *name, char *local_repmgr_directory);
static int	get_tablespace_data_barman(char *, TablespaceDataList *);
static char *make_barman_ssh_command(char *buf);

static bool create_recovery_file(t_node_info *node_record, t_conninfo_param_list *recovery_conninfo, char *dest, bool as_file);
static void write_primary_conninfo(PQExpBufferData *dest, t_conninfo_param_list *param_list);

static NodeStatus parse_node_status_is_shutdown_cleanly(const char *node_status_output, XLogRecPtr *checkPoint);
static CheckStatus parse_node_check_archiver(const char *node_check_output, int *files, int *threshold);
static ConnectionStatus parse_remote_node_replication_connection(const char *node_check_output);

/*
 * STANDBY CLONE
 *
 * Event(s):
 *  - standby_clone
 *
 * Parameters:
 *  --upstream-conninfo
 *  --no-upstream-connection
 *  -F/--force
 *  --dry-run
 *  -c/--fast-checkpoint
 *  --copy-external-config-files
 *  --recovery-min-apply-delay
 *  --replication-user (only required if no upstream record)
 *  --without-barman
 *  --recovery-conf-only
 */

void
do_standby_clone(void)
{
	PQExpBufferData event_details;
	int			r = 0;

	/* dummy node record */
	t_node_info local_node_record = T_NODE_INFO_INITIALIZER;

	/*
	 * --recovery-conf-only provided - we'll handle that separately
	 */
	if (runtime_options.recovery_conf_only == true)
	{
		return _do_create_recovery_conf();
	}

	/*
	 * conninfo params for the actual upstream node (which might be different
	 * to the node we're cloning from) to write to recovery.conf
	 */

	mode = get_standby_clone_mode();

	/*
	 * Copy the provided data directory; if a configuration file was provided,
	 * use the (mandatory) value from that; if -D/--pgdata was provided, use
	 * that; otherwise repmgr will default to using the same directory path as
	 * on the source host. The last case will only ever occur when executing
	 * "repmgr standby clone" with no configuration file.
	 *
	 * Note that barman mode requires -D/--pgdata.
	 *
	 * If no data directory is explicitly provided, and we're not cloning from
	 * barman, the source host's data directory will be fetched later, after
	 * we've connected to it, in check_source_server().
	 *
	 */

	get_node_data_directory(local_data_directory);
	if (local_data_directory[0] != '\0')
	{
		local_data_directory_provided = true;
		log_notice(_("destination directory \"%s\" provided"),
				   local_data_directory);
	}
	else if (mode == barman)
	{
		/*
		 * XXX in Barman mode it's still possible to connect to the upstream,
		 * so only fail if that's not available.
		 */
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

	init_node_record(&local_node_record);
	local_node_record.type = STANDBY;

	/*
	 * Initialise list of conninfo parameters which will later be used to
	 * create the `primary_conninfo` string in recovery.conf .
	 *
	 * We'll initialise it with the host settings specified on the command
	 * line. As it's possible the standby will be cloned from a node different
	 * to its intended upstream, we'll later attempt to fetch the upstream
	 * node record and overwrite the values set here with those from the
	 * upstream node record (excluding that record's application_name)
	 */
	initialize_conninfo_params(&recovery_conninfo, false);

	copy_conninfo_params(&recovery_conninfo, &source_conninfo);


	/* Set the default application name to this node's name */
	if (config_file_options.node_id != UNKNOWN_NODE_ID)
	{
		char		application_name[MAXLEN] = "";

		param_set(&recovery_conninfo, "application_name", config_file_options.node_name);

		get_conninfo_value(config_file_options.conninfo, "application_name", application_name);
		if (strlen(application_name) && strncmp(application_name, config_file_options.node_name, MAXLEN) != 0)
		{
			log_notice(_("\"application_name\" is set in repmgr.conf but will be replaced by the node name"));
		}
	}
	else
	{
		/*
		 * this will only happen in corner cases where the node is being
		 * cloned without a configuration file; fall back to "repmgr" if no
		 * application_name provided
		 */
		char	   *application_name = param_get(&source_conninfo, "application_name");

		if (application_name == NULL)
			param_set(&recovery_conninfo, "application_name", "repmgr");
	}



	/*
	 * Do some sanity checks on the proposed data directory; if it exists:
	 *  - check it's openable
	 *  - check if there's an instance running
	 *
	 * We do this here so the check can be part of a --dry-run.
	 */
	switch (check_dir(local_data_directory))
	{
		case DIR_ERROR:
			log_error(_("unable to access specified data directory \"%s\""), local_data_directory);
			log_detail("%s", strerror(errno));
			exit(ERR_BAD_CONFIG);
			break;
		case DIR_NOENT:
			/*
			 * directory doesn't exist
			 * TODO: in --dry-run mode, attempt to create and delete?
			 */
			break;
		case DIR_EMPTY:
			/* Present but empty */
			break;
		case DIR_NOT_EMPTY:
			/* Present but not empty */
			if (is_pg_dir(local_data_directory))
			{
				/* even -F/--force is not enough to overwrite an active directory... */
				if (is_pg_running(local_data_directory))
				{
					log_error(_("specified data directory \"%s\" appears to contain a running PostgreSQL instance"),
							  local_data_directory);
					log_hint(_("ensure the target data directory does not contain a running PostgreSQL instance"));
					exit(ERR_BAD_CONFIG);
				}
			}
			break;
		default:
			break;
	}

	/*
	 * By default attempt to connect to the source node. This will fail if no
	 * connection is possible, unless in Barman mode, in which case we can
	 * fall back to connecting to the source node via Barman.
	 */
	if (runtime_options.no_upstream_connection == false)
	{
		/*
		 * This connects to the source node and performs sanity checks, also
		 * sets "recovery_conninfo_str", "upstream_repluser", "upstream_user" and
		 * "upstream_node_id".
		 *
		 * Will error out if source connection not possible and not in
		 * "barman" mode.
		 */
		check_source_server();
	}
	else {
		upstream_node_id = runtime_options.upstream_node_id;
	}

	/*
	 * if --upstream-conninfo was supplied, use that (will overwrite value set
	 * by check_source_server(), but that's OK)
	 */
	if (runtime_options.upstream_conninfo[0] != '\0')
	{
		strncpy(recovery_conninfo_str, runtime_options.upstream_conninfo, MAXLEN);
		upstream_conninfo_found = true;
	}
	else if (mode == barman && PQstatus(source_conn) != CONNECTION_OK)
	{
		/*
		 * Here we don't have a connection to the upstream node (either
		 * because --no-upstream-connection was supplied, or
		 * check_source_server() was unable to make a connection, and
		 * --upstream-conninfo wasn't supplied.
		 *
		 * As we're executing in Barman mode we can try and connect via the
		 * Barman server to extract the upstream node's conninfo string.
		 *
		 * To do this we need to extract Barman's conninfo string, replace the
		 * database name with the repmgr one (they could well be different)
		 * and remotely execute psql.
		 *
		 * This attempts to set "recovery_conninfo_str".
		 */
		check_source_server_via_barman();
	}

	if (recovery_conninfo_str[0] == '\0')
	{
		log_error(_("unable to determine a connection string to use as \"primary_conninfo\""));
		log_hint(_("use \"--upstream-conninfo\" to explicitly provide a value for \"primary_conninfo\""));
		if (PQstatus(source_conn) == CONNECTION_OK)
			PQfinish(source_conn);
		exit(ERR_BAD_CONFIG);
	}


	if (upstream_conninfo_found == true)
	{
		/*
		 * parse returned upstream conninfo string to recovery
		 * primary_conninfo params
		 */
		char	   *errmsg = NULL;
		bool		parse_success = false;

		log_verbose(LOG_DEBUG, "parsing upstream conninfo string \"%s\"", recovery_conninfo_str);

		/*
		 * parse_conninfo_string() here will remove the upstream's
		 * `application_name`, if set
		 */

		parse_success = parse_conninfo_string(recovery_conninfo_str, &recovery_conninfo, &errmsg, true);

		if (parse_success == false)
		{
			log_error(_("unable to parse conninfo string \"%s\" for upstream node:\n  %s"),
					  recovery_conninfo_str, errmsg);

			if (PQstatus(source_conn) == CONNECTION_OK)
				PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}

		if (upstream_repluser[0] != '\0')
		{
			/* Write the replication user from the node's upstream record */
			param_set(&recovery_conninfo, "user", upstream_repluser);
		}
	}
	else
	{
		/*
		 * If no upstream node record found, we'll abort with an error here,
		 * unless -F/--force is used, in which case we'll use the parameters
		 * provided on the command line (and assume the user knows what
		 * they're doing).
		 */
		if (upstream_node_id == UNKNOWN_NODE_ID)
		{
			log_error(_("unable to determine upstream node"));
			if (PQstatus(source_conn) == CONNECTION_OK)
				PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}

		if (!runtime_options.force)
		{
			log_error(_("no record found for upstream node (upstream_node_id: %i)"),
					  upstream_node_id);
			log_hint(_("use -F/--force to create \"primary_conninfo\" based on command-line parameters"));

			if (PQstatus(source_conn) == CONNECTION_OK)
				PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/*
	 * If copying of external configuration files requested, and any are
	 * detected, perform sanity checks
	 */
	if (PQstatus(source_conn) == CONNECTION_OK && runtime_options.copy_external_config_files == true)
	{
		PGconn	   *superuser_conn = NULL;
		PGconn	   *privileged_conn = NULL;
		bool		external_config_files = false;
		int			i = 0;

		/*
		 * Obtain configuration file locations
		 *
		 * We'll check to see whether the configuration files are in the data
		 * directory - if not we'll have to copy them via SSH, if copying
		 * requested.
		 *
		 * This will require superuser permissions, so we'll attempt to
		 * connect as -S/--superuser (if provided), otherwise check the
		 * current connection user has superuser rights.
		 *
		 * XXX: if configuration files are symlinks to targets outside the
		 * data directory, they won't be copied by pg_basebackup, but we can't
		 * tell this from the below query; we'll probably need to add a check
		 * for their presence and if missing force copy by SSH
		 */
		get_superuser_connection(&source_conn, &superuser_conn, &privileged_conn);

		if (get_configuration_file_locations(privileged_conn, &config_files) == false)
		{
			log_notice(_("unable to proceed without establishing configuration file locations"));
			PQfinish(source_conn);

			if (superuser_conn != NULL)
				PQfinish(superuser_conn);

			exit(ERR_BAD_CONFIG);
		}

		/* check if any files actually outside the data directory */
		for (i = 0; i < config_files.entries; i++)
		{
			t_configfile_info *file = config_files.files[i];

			if (file->in_data_directory == false)
			{
				external_config_files = true;
				break;
			}
		}

		if (external_config_files == true)
		{
			int			r;
			PQExpBufferData msg;

			initPQExpBuffer(&msg);

			appendPQExpBuffer(&msg,
							  _("external configuration files detected, checking SSH connection to host \"%s\""),
							  runtime_options.host);

			if (runtime_options.dry_run == true)
			{
				log_notice("%s", msg.data);
			}
			else
			{
				log_verbose(LOG_INFO, "%s", msg.data);
			}

			termPQExpBuffer(&msg);

			r = test_ssh_connection(runtime_options.host, runtime_options.remote_user);
			if (r != 0)
			{
				log_error(_("remote host \"%s\" is not reachable via SSH - unable to copy external configuration files"),
						  runtime_options.host);
				if (superuser_conn != NULL)
					PQfinish(superuser_conn);
				PQfinish(source_conn);
				exit(ERR_BAD_CONFIG);
			}

			initPQExpBuffer(&msg);

			appendPQExpBuffer(&msg,
							  _("SSH connection to host \"%s\" succeeded"),
							  runtime_options.host);

			if (runtime_options.dry_run == true)
			{
				log_info("%s", msg.data);
			}
			else
			{
				log_verbose(LOG_INFO, "%s", msg.data);
			}

			termPQExpBuffer(&msg);

			/*
			 * Here we'll attempt an initial test copy of the detected external
			 * files, to detect any issues before we run the base backup.
			 *
			 * Note this will exit with an error, unless -F/--force supplied.
			 *
			 * TODO: put the files in a temporary directory and move to their final
			 * destination once the database has been cloned.
			 */

			if (runtime_options.copy_external_config_files_destination == CONFIG_FILE_SAMEPATH)
			{
				/*
				 * Files will be placed in the same path as on the source server;
				 * don't delete after copying.
				 */
				copy_configuration_files(false);

			}
			else
			{
				/*
				 * Files will be placed in the data directory - delete after copying.
				 * They'll be copied again later; see TODO above.
				 */
				copy_configuration_files(true);
			}
		}


		if (superuser_conn != NULL)
			PQfinish(superuser_conn);
	}


	if (runtime_options.dry_run == true)
	{
		if (upstream_node_id != UNKNOWN_NODE_ID)
		{
			log_notice(_("standby will attach to upstream node %i"), upstream_node_id);
		}
		else
		{
			log_warning(_("unable to determine a valid upstream node id"));
		}

		if (mode == pg_basebackup && runtime_options.fast_checkpoint == false)
		{
			log_hint(_("consider using the -c/--fast-checkpoint option"));
		}

		log_info(_("all prerequisites for \"standby clone\" are met"));

		PQfinish(source_conn);
		exit(SUCCESS);
	}

	if (mode != barman)
	{
		initialise_direct_clone(&local_node_record);
	}

	switch (mode)
	{
		case pg_basebackup:
			log_notice(_("starting backup (using pg_basebackup)..."));
			break;
		case barman:
			log_notice(_("retrieving backup from Barman..."));
			break;
		default:
			/* should never reach here */
			log_error(_("unknown clone mode"));
	}

	if (mode == pg_basebackup)
	{
		if (runtime_options.fast_checkpoint == false)
		{
			log_hint(_("this may take some time; consider using the -c/--fast-checkpoint option"));
		}
	}

	switch (mode)
	{
		case pg_basebackup:
			r = run_basebackup(&local_node_record);
			break;
		case barman:
			r = run_file_backup(&local_node_record);
			break;
		default:
			/* should never reach here */
			log_error(_("unknown clone mode"));
	}


	/* If the backup failed then exit */
	if (r != SUCCESS)
	{
		/* If a replication slot was previously created, drop it */
		if (config_file_options.use_replication_slots == true)
		{
			drop_replication_slot(source_conn, local_node_record.slot_name);
		}

		log_error(_("unable to take a base backup of the primary server"));
		log_hint(_("data directory (\"%s\") may need to be cleaned up manually"),
				 local_data_directory);

		PQfinish(source_conn);
		exit(r);
	}


	/*
	 * If `--copy-external-config-files` was provided, copy any configuration
	 * files detected to the appropriate location. Any errors encountered will
	 * not be treated as fatal.
	 *
	 * This won't run in Barman mode as "config_files" is only populated in
	 * "initialise_direct_clone()", which isn't called in Barman mode.
	 */
	if (runtime_options.copy_external_config_files == true && config_files.entries > 0)
	{
		/*
		 * If "--copy-external-config-files=samepath" was used, the files will already
		 * have been copied.
		 */
		if (runtime_options.copy_external_config_files_destination == CONFIG_FILE_PGDATA)
			copy_configuration_files(false);
	}

	/* Write the recovery.conf file */

	if (create_recovery_file(&local_node_record, &recovery_conninfo, local_data_directory, true) == false)
	{
		/* create_recovery_file() will log an error */
		log_notice(_("unable to create recovery.conf; see preceding error messages"));
		log_hint(_("data directory (\"%s\") may need to be cleaned up manually"),
				 local_data_directory);

		PQfinish(source_conn);
		exit(ERR_BAD_CONFIG);
	}

	switch (mode)
	{
		case pg_basebackup:
			log_notice(_("standby clone (using pg_basebackup) complete"));
			break;

		case barman:
			log_notice(_("standby clone (from Barman) complete"));
			break;
	}

	/*
	 * TODO: It might be nice to provide an option to have repmgr start the
	 * PostgreSQL server automatically
	 */

	log_notice(_("you can now start your PostgreSQL server"));

	if (config_file_options.service_start_command[0] != '\0')
	{
		log_hint(_("for example: %s"),
				 config_file_options.service_start_command);
	}
	else if (local_data_directory_provided)
	{
		log_hint(_("for example: pg_ctl -D %s start"),
				 local_data_directory);
	}
	else
	{
		log_hint(_("for example: /etc/init.d/postgresql start"));
	}

	/*
	 * XXX forgetting to (re) register the standby is a frequent cause of
	 * error; we should consider having repmgr automatically register the
	 * standby, either by default with an option "--no-register", or an option
	 * "--register".
	 *
	 * Note that "repmgr standby register" requires the standby to be running
	 * - if not, and we just update the node record, we'd have an incorrect
	 * representation of the replication cluster. Best combined with an
	 * automatic start of the server (see note above)
	 */

	/*
	 * Check for an existing node record, and output the appropriate command
	 * for registering or re-registering.
	 */
	{
		t_node_info node_record = T_NODE_INFO_INITIALIZER;
		RecordStatus record_status = RECORD_NOT_FOUND;

		record_status = get_node_record(primary_conn,
										config_file_options.node_id,
										&node_record);

		if (record_status == RECORD_FOUND)
		{
			log_hint(_("after starting the server, you need to re-register this standby with \"repmgr standby register --force\" to update the existing node record"));
		}
		else
		{
			log_hint(_("after starting the server, you need to register this standby with \"repmgr standby register\""));

		}
	}


	/* Log the event */

	initPQExpBuffer(&event_details);

	/* Add details about relevant runtime options used */
	appendPQExpBuffer(&event_details,
					  _("cloned from host \"%s\", port %s"),
					  runtime_options.host,
					  runtime_options.port);

	appendPQExpBuffer(&event_details,
					  _("; backup method: "));

	switch (mode)
	{
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

	create_event_notification(primary_conn,
							  &config_file_options,
							  config_file_options.node_id,
							  "standby_clone",
							  true,
							  event_details.data);

	if (primary_conn != source_conn && PQstatus(primary_conn) == CONNECTION_OK)
		PQfinish(primary_conn);

	if (PQstatus(source_conn) == CONNECTION_OK)
		PQfinish(source_conn);

	exit(r);
}


void
check_barman_config(void)
{
	char		command[MAXLEN];
	bool		command_ok = false;

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


/*
 * _do_create_recovery_conf()
 *
 * Create recovery.conf for a previously cloned instance.
 *
 * Prerequisites:
 *
 * - data directory must be provided
 * - the instance should not be running
 * - an existing "recovery.conf" file can only be overwritten with
 *   -F/--force
 * - connection parameters for an existing, running node must be provided
 * - --upstream-node-id, if provided, will be "primary_conninfo",
 *   otherwise primary node id; node must exist; unless -F/--force
 *   provided, must be active and connection possible
 * - if replication slots in use, create (respect --dry-run)
 *
 * not compatible with --no-upstream-connection
 *
 */

static void
_do_create_recovery_conf(void)
{
	t_node_info local_node_record = T_NODE_INFO_INITIALIZER;
	t_node_info upstream_node_record = T_NODE_INFO_INITIALIZER;

	RecordStatus record_status = RECORD_NOT_FOUND;
	char		recovery_file_path[MAXPGPATH] = "";
	struct stat st;
	bool		node_is_running = false;
	bool		slot_creation_required = false;
	PGconn	   *upstream_conn = NULL;
	PGconn	   *upstream_repl_conn = NULL;

	get_node_data_directory(local_data_directory);

	if (local_data_directory[0] == '\0')
	{
		log_error(_("no data directory provided"));
		log_hint(_("provide the node's \"repmgr.conf\" file with -f/--config-file or the data directory with -D/--pgdata"));
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Do some sanity checks on the data directory to make sure
	 * it contains a valid but dormant instance
	 */
	switch (check_dir(local_data_directory))
	{
		case DIR_ERROR:
			log_error(_("unable to access specified data directory \"%s\""), local_data_directory);
			log_detail("%s", strerror(errno));
			exit(ERR_BAD_CONFIG);
			break;
		case DIR_NOENT:
			log_error(_("specified data directory \"%s\" does not exist"), local_data_directory);
			exit(ERR_BAD_CONFIG);
			break;
		case DIR_EMPTY:
			log_error(_("specified data directory \"%s\" is empty"), local_data_directory);
			exit(ERR_BAD_CONFIG);
			break;
		case DIR_NOT_EMPTY:
			/* Present but not empty */
			if (!is_pg_dir(local_data_directory))
			{
				log_error(_("specified data directory \"%s\" does not contain a PostgreSQL instance"), local_data_directory);
				exit(ERR_BAD_CONFIG);
			}

			if (is_pg_running(local_data_directory))
			{
				if (runtime_options.force == false)
				{
					log_error(_("specified data directory \"%s\" appears to contain a running PostgreSQL instance"),
							  local_data_directory);
					log_hint(_("use -F/--force to create \"recovery.conf\" anyway"));
					exit(ERR_BAD_CONFIG);
				}

				node_is_running = true;

				if (runtime_options.dry_run == true)
				{
					log_warning(_("\"recovery.conf\" would be created in an active data directory"));
				}
				else
				{
					log_warning(_("creating \"recovery.conf\" in an active data directory"));
				}
			}
			break;
		default:
			break;
	}

	/* check connection */
	source_conn = establish_db_connection_by_params(&source_conninfo, true);

	/* determine node for primary_conninfo */

	if (runtime_options.upstream_node_id != UNKNOWN_NODE_ID)
	{
		upstream_node_id = runtime_options.upstream_node_id;
	}
	else
	{
		/* if --upstream-node-id not specifically supplied, get primary node id */
		upstream_node_id = get_primary_node_id(source_conn);

		if (upstream_node_id == NODE_NOT_FOUND)
		{
			log_error(_("unable to determine primary node for this replication cluster"));
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}

		log_debug("primary node determined as: %i", upstream_node_id);
	}

	/* attempt to retrieve upstream node record */
	record_status = get_node_record(source_conn,
									upstream_node_id,
									&upstream_node_record);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve node record for upstream node %i"), upstream_node_id);

		if (record_status == RECORD_ERROR)
		{
			log_detail("%s", PQerrorMessage(source_conn));
		}

		exit(ERR_BAD_CONFIG);
	}

	/* attempt to retrieve local node record */
	record_status = get_node_record(source_conn,
									config_file_options.node_id,
									&local_node_record);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve node record for local node %i"), config_file_options.node_id);

		if (record_status == RECORD_ERROR)
		{
			log_detail("%s", PQerrorMessage(source_conn));
		}
		else
		{
			log_hint(_("standby must be registered before a new recovery.conf file can be created"));
		}

		exit(ERR_BAD_CONFIG);
	}

	PQfinish(source_conn);


	/* connect to upstream (which could be different to source) */

	upstream_conn = establish_db_connection(upstream_node_record.conninfo, false);
	if (PQstatus(upstream_conn) != CONNECTION_OK)
	{
		log_error(_("unable to connect to upstream node \"%s\" (ID: %i)"),
				  upstream_node_record.node_name,
 				  upstream_node_id);
		exit(ERR_BAD_CONFIG);
	}

	/* Set the application name to this node's name */
	if (config_file_options.node_name[0] != '\0')
		param_set(&recovery_conninfo, "application_name", config_file_options.node_name);

	/* Set the replication user from the primary node record */
	param_set(&recovery_conninfo, "user", upstream_node_record.repluser);

	initialize_conninfo_params(&recovery_conninfo, false);

	/* We ignore any application_name set in the primary's conninfo */
	parse_conninfo_string(upstream_node_record.conninfo, &recovery_conninfo, NULL, true);

	/* check that a replication connection can be made (--force = override) */
	upstream_repl_conn = establish_db_connection_by_params(&recovery_conninfo, false);

	if (PQstatus(upstream_repl_conn) != CONNECTION_OK)
	{
		if (runtime_options.force == false)
		{
			log_error(_("unable to initiate replication connection to upstream node \"%s\" (ID: %i)"),
					  upstream_node_record.node_name,
					  upstream_node_id);
			PQfinish(upstream_conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/* if replication slots are in use, perform some checks */
	if (config_file_options.use_replication_slots == true)
	{
		PQExpBufferData msg;
		t_replication_slot slot_info = T_REPLICATION_SLOT_INITIALIZER;

		record_status = get_slot_record(upstream_conn, local_node_record.slot_name, &slot_info);

		/* check if replication slot exists*/
		if (record_status == RECORD_FOUND)
		{
			if (slot_info.active == true)
			{
				initPQExpBuffer(&msg);

				appendPQExpBuffer(&msg,
								  _("an active replication slot named \"%s\" already exists on upstream node \"%s\" (ID: %i)"),
								  local_node_record.slot_name,
								  upstream_node_record.node_name,
								  upstream_node_id);

				if (runtime_options.force == false && runtime_options.dry_run == false)
				{
					log_error("%s", msg.data);
					log_hint(_("use -F/--force to continue anyway"));
					termPQExpBuffer(&msg);
					PQfinish(upstream_conn);
					exit(ERR_BAD_CONFIG);
				}

				log_warning("%s", msg.data);
				termPQExpBuffer(&msg);
			}
			else
			{
				log_info(_("an inactive replication slot for this node exists on the upstream node"));
			}
		}
		/* if not, if check one can and should be created */
		else
		{
			get_node_replication_stats(upstream_conn, UNKNOWN_SERVER_VERSION_NUM, &upstream_node_record);

		    if (upstream_node_record.max_replication_slots > upstream_node_record.total_replication_slots)
			{
				slot_creation_required = true;
			}
			else
			{
				initPQExpBuffer(&msg);

				appendPQExpBuffer(&msg,
								  _("insufficient free replication slots on upstream node \"%s\" (ID: %i)"),
								  upstream_node_record.node_name,
								  upstream_node_id);

				if (runtime_options.force == false && runtime_options.dry_run == false)
				{
					log_error("%s", msg.data);
					log_hint(_("use -F/--force to continue anyway"));
					termPQExpBuffer(&msg);
					PQfinish(upstream_conn);
					exit(ERR_BAD_CONFIG);
				}

				log_warning("%s", msg.data);
				termPQExpBuffer(&msg);
			}
		}
	}

	/* check if recovery.conf exists */

	maxpath_snprintf(recovery_file_path, "%s/%s", local_data_directory, RECOVERY_COMMAND_FILE);

	if (stat(recovery_file_path, &st) == -1)
	{
		if (errno != ENOENT)
		{
			log_error(_("unable to check for existing \"recovery.conf\" file in \"%s\""),
					  local_data_directory);
			log_detail("%s", strerror(errno));
			exit(ERR_BAD_CONFIG);
		}
	}
	else
	{
		if (runtime_options.force == false)
		{
			log_error(_("\"recovery.conf\" already exists in \"%s\""),
					  local_data_directory);
			log_hint(_("use -F/--force to overwrite an existing \"recovery.conf\" file"));
			exit(ERR_BAD_CONFIG);
		}

		if (runtime_options.dry_run == true)
		{
			log_warning(_("the existing \"recovery.conf\" file would be overwritten"));
		}
		else
		{
			log_warning(_("the existing \"recovery.conf\" file will be overwritten"));
		}
	}

	if (runtime_options.dry_run == true)
	{
		char		recovery_conf_contents[MAXLEN] = "";
		create_recovery_file(&local_node_record, &recovery_conninfo, recovery_conf_contents, false);

		log_info(_("would create \"recovery.conf\" file in \"%s\""), local_data_directory);
		log_detail(_("\n%s"), recovery_conf_contents);
	}
	else
	{
		if (!create_recovery_file(&local_node_record, &recovery_conninfo, local_data_directory, true))
		{
			log_error(_("unable to create \"recovery.conf\""));
		}
		else
		{
			log_notice(_("\"recovery.conf\" created as \"%s\""), recovery_file_path);

			if (node_is_running == true)
			{
				log_hint(_("node must be restarted for the new file to take effect"));
			}
		}
	}

	/* add replication slot, if required */
	if (slot_creation_required == true)
	{
		if (runtime_options.dry_run == true)
		{
			log_info(_("would create replication slot \"%s\" on upstream node \"%s\" (ID: %i)"),
					 local_node_record.slot_name,
					 upstream_node_record.node_name,
					 upstream_node_id);
		}
		else
		{
			PQExpBufferData msg;
			initPQExpBuffer(&msg);

			if (create_replication_slot(upstream_conn,
										local_node_record.slot_name,
										UNKNOWN_SERVER_VERSION_NUM,
										&msg) == false)
			{
				log_error("%s", msg.data);
				PQfinish(upstream_conn);
				termPQExpBuffer(&msg);
				exit(ERR_BAD_CONFIG);
			}

			termPQExpBuffer(&msg);

			log_notice(_("replication slot \"%s\" created on upstream node \"%s\" (ID: %i)"),
					   local_node_record.slot_name,
					   upstream_node_record.node_name,
					   upstream_node_id);
		}
	}


	PQfinish(upstream_conn);

	return;
}


/*
 * do_standby_register()
 *
 * Event(s):
 *  - standby_register
 *  - standby_register_sync
 */
/*  XXX check --upstream-node-id works when re-registering */

void
do_standby_register(void)
{
	PGconn	   *conn = NULL;
	PGconn	   *primary_conn = NULL;

	bool		record_created = false;
	t_node_info node_record = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status = RECORD_NOT_FOUND;

	PQExpBufferData details;

	/* so we can pass info about the primary to event notification scripts */
	t_event_info event_info = T_EVENT_INFO_INITIALIZER;
	t_node_info primary_node_record = T_NODE_INFO_INITIALIZER;
	int primary_node_id = UNKNOWN_NODE_ID;

	bool		dry_run_ok = true;

	log_info(_("connecting to local node \"%s\" (ID: %i)"),
			 config_file_options.node_name,
			 config_file_options.node_id);

	conn = establish_db_connection_quiet(config_file_options.conninfo);

	/*
	 * If unable to connect, and --force not provided, wait up to --wait-start
	 * seconds (default: 0) for the node to become reachable.
	 *
	 * Not that if --force provided, we don't wait for the node to start, as
	 * the normal use case will be re-registering an existing node, or
	 * registering an inactive/not-yet-extant one; we'll do the
	 * error handling for those cases in the next code block
	 */
	if (PQstatus(conn) != CONNECTION_OK && runtime_options.force == false)
	{
		bool		conn_ok = false;
		int			timer = 0;

		for (;;)
		{
			if (timer == runtime_options.wait_start)
				break;

			sleep(1);

			log_verbose(LOG_INFO, _("%i of %i connection attempts"),
						timer + 1,
						runtime_options.wait_start);

			conn = establish_db_connection_quiet(config_file_options.conninfo);

			if (PQstatus(conn) == CONNECTION_OK)
			{
				conn_ok = true;
				break;
			}

			timer++;
		}

		if (conn_ok == true)
		{
			log_info(_("connected to local node \"%s\" (ID: %i) after %i seconds"),
					 config_file_options.node_name,
					 config_file_options.node_id,
					 timer);
		}
	}

	/*
	 * If still unable to connect, continue only if -F/--force provided,
	 * and primary connection parameters provided.
	 */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		if (runtime_options.force == false)
		{
			log_error(_("unable to connect to local node \"%s\" (ID: %i)"),
					  config_file_options.node_name,
					  config_file_options.node_id);
			log_detail("%s",
					   PQerrorMessage(conn));
			log_hint(_("to register a standby which is not running, provide primary connection parameters and use option -F/--force"));

			exit(ERR_BAD_CONFIG);
		}

		if (runtime_options.connection_param_provided == false)
		{
			log_error(_("unable to connect to local node \"%s\" (ID: %i)"),
					  config_file_options.node_name,
					  config_file_options.node_id);
			log_hint(_("to register a standby which is not running, additionally provide the primary connection parameters"));
			exit(ERR_BAD_CONFIG);
		}
	}
	/* connection OK - check this is actually a standby */
	else
	{
		check_recovery_type(conn);
	}

	/* check if there is a primary in this cluster */
	log_info(_("connecting to primary database"));

	/* Normal case - we can connect to the local node */
	if (PQstatus(conn) == CONNECTION_OK)
	{
		primary_conn = get_primary_connection(conn, &primary_node_id, NULL);
	}

	/*
	 * otherwise user is forcing a registration of a (potentially) inactive (or
	 * not-yet-extant) node and must have supplied primary connection info
	 */
	else
	{
		primary_conn = establish_db_connection_by_params(&source_conninfo, false);
	}

	/*
	 * no amount of --force will make it possible to register the standby
	 * without a primary server to connect to
	 */
	if (PQstatus(primary_conn) != CONNECTION_OK)
	{
		log_error(_("unable to connect to the primary database"));
		log_hint(_("a primary node must be configured before registering a standby node"));
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * Populate "event_info" with info about the primary for event notifications
	 */
	record_status = get_node_record(primary_conn,
									primary_node_id,
									&primary_node_record);
	event_info.node_id = primary_node_id;
	event_info.node_name = primary_node_record.node_name;
	event_info.conninfo_str = primary_node_record.conninfo;

	/*
	 * Verify that standby and primary are supported and compatible server
	 * versions
	 *
	 * If the user is registering an inactive standby, we'll trust they know
	 * what they're doing
	 */
	if (PQstatus(conn) == CONNECTION_OK)
	{
		check_primary_standby_version_match(conn, primary_conn);
	}


	/*
	 * Check that an active node with the same node_name doesn't exist already
	 */

	record_status = get_node_record_by_name(primary_conn,
											config_file_options.node_name,
											&node_record);

	if (record_status == RECORD_FOUND)
	{
		if (node_record.active == true && node_record.node_id != config_file_options.node_id)
		{
			log_error(_("node %i exists already with node_name \"%s\""),
					  node_record.node_id,
					  config_file_options.node_name);
			PQfinish(primary_conn);
			if (PQstatus(conn) == CONNECTION_OK)
				PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/* Check if node record exists */

	record_status = get_node_record(primary_conn,
									config_file_options.node_id,
									&node_record);

	if (record_status == RECORD_FOUND && !runtime_options.force)
	{
		log_error(_("node %i is already registered"),
				  config_file_options.node_id);
		log_hint(_("use option -F/--force to overwrite an existing node record"));
		PQfinish(primary_conn);
		if (PQstatus(conn) == CONNECTION_OK)
			PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * If an upstream node is defined, check if that node exists and is active.
	 *
	 * If it doesn't exist, and --force set, create a minimal inactive record,
	 * in the assumption that the user knows what they are doing (usually some kind
	 * of provisioning where multiple servers are created in parallel) and will
	 * create the active record later.
	 */
	if (runtime_options.upstream_node_id != NO_UPSTREAM_NODE)
	{
		RecordStatus upstream_record_status = RECORD_NOT_FOUND;
		t_node_info upstream_node_record = T_NODE_INFO_INITIALIZER;

		upstream_record_status = get_node_record(primary_conn,
												 runtime_options.upstream_node_id,
												 &upstream_node_record);

		/* create placeholder upstream record if -F/--force set */
		if (upstream_record_status != RECORD_FOUND)
		{
			t_node_info placeholder_upstream_node_record = T_NODE_INFO_INITIALIZER;

			if (!runtime_options.force)
			{
				log_error(_("no record found for upstream node %i"),
						  runtime_options.upstream_node_id);
				/* footgun alert - only do this if you know what you're doing */
				log_hint(_("use option -F/--force to create a dummy upstream record"));
				PQfinish(primary_conn);
				if (PQstatus(conn) == CONNECTION_OK)
					PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}

			log_notice(_("creating placeholder record for upstream node %i"),
					   runtime_options.upstream_node_id);

			placeholder_upstream_node_record.node_id = runtime_options.upstream_node_id;
			placeholder_upstream_node_record.type = STANDBY;
			placeholder_upstream_node_record.upstream_node_id = NO_UPSTREAM_NODE;
			strncpy(placeholder_upstream_node_record.conninfo, runtime_options.upstream_conninfo, MAXLEN);
			placeholder_upstream_node_record.active = false;

			record_created = create_node_record(primary_conn,
												"standby register",
												&placeholder_upstream_node_record);

			/*
			 * It's possible, in the kind of scenario this functionality is
			 * intended to support, that there's a race condition where the
			 * node's actual record gets inserted, causing the insert of the
			 * placeholder record to fail. If this is the case, we don't worry
			 * about this insert failing; if not we bail out.
			 *
			 * TODO: teach create_node_record() to use ON CONFLICT DO NOTHING
			 * for 9.5 and later.
			 */
			if (record_created == false)
			{
				upstream_record_status = get_node_record(primary_conn,
														 runtime_options.upstream_node_id,
														 &placeholder_upstream_node_record);
				if (upstream_record_status != RECORD_FOUND)
				{
					log_error(_("unable to create placeholder record for upstream node %i"),
							  runtime_options.upstream_node_id);
					PQfinish(primary_conn);
					if (PQstatus(conn) == CONNECTION_OK)
						PQfinish(conn);
					exit(ERR_BAD_CONFIG);
				}

				log_info(_("a record for upstream node %i was already created"),
						 runtime_options.upstream_node_id);
			}
		}
		else if (node_record.active == false)
		{
			/*
			 * upstream node is inactive and --force not supplied - refuse to
			 * register
			 */
			if (!runtime_options.force)
			{
				log_error(_("record for upstream node %i is marked as inactive"),
						  runtime_options.upstream_node_id);
				log_hint(_("use option -F/--force to register a standby with an inactive upstream node"));
				PQfinish(primary_conn);
				if (PQstatus(conn) == CONNECTION_OK)
					PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}

			/*
			 * user is using the --force - notify about the potential footgun
			 */
			log_notice(_("registering node %i with inactive upstream node %i"),
					   config_file_options.node_id,
					   runtime_options.upstream_node_id);
		}
		/* check upstream node is accessible and this node is connected */
		else
		{
			PGconn	   *upstream_conn = NULL;

			upstream_conn = establish_db_connection(upstream_node_record.conninfo, false);

			if (PQstatus(upstream_conn) != CONNECTION_OK)
			{
				if (!runtime_options.force)
				{
					log_error(_("unable to connect to upstream node \"%s\" (ID: %i)"),
							  upstream_node_record.node_name,
							  upstream_node_record.node_id);
					log_hint(_("use -F/--force to continue anyway"));
					PQfinish(primary_conn);
					if (PQstatus(conn) == CONNECTION_OK)
						PQfinish(conn);
					exit(ERR_BAD_CONFIG);
				}

				log_warning(_("unable to connect to upstream node \"%s\" (ID: %i) but continuing anyway"),
							upstream_node_record.node_name,
							upstream_node_record.node_id);
			}
			else
			{
				/* check our standby is connected */
				if (is_downstream_node_attached(upstream_conn, config_file_options.node_name) == true)
				{
					log_verbose(LOG_INFO, _("local node is attached to specified upstream node %i"), runtime_options.upstream_node_id);
				}
				else
				{
					if (!runtime_options.force)
					{
						log_error(_("this node does not appear to be attached to upstream node \"%s\" (ID: %i)"),
								  upstream_node_record.node_name,
								  upstream_node_record.node_id);

						log_detail(_("no record for application name \"%s\" found in \"pg_stat_replication\""),
								   config_file_options.node_name);
						log_hint(_("use -F/--force to continue anyway"));
						PQfinish(primary_conn);
						if (PQstatus(conn) == CONNECTION_OK)
							PQfinish(conn);
						exit(ERR_BAD_CONFIG);
					}
					log_warning(_("this node does not appear to be attached to upstream node \"%s\" (ID: %i)"),
								config_file_options.node_name,
								config_file_options.node_id);
				}
				PQfinish(upstream_conn);
			}
		}
	}

	/*
	 * populate node record structure with current values set in repmgr.conf
	 * and/or the command line (this will overwrite  any existing values, which
	 * is what we want when updating the record)
	 */
	init_node_record(&node_record);
	node_record.type = STANDBY;

	/* if --upstream-node-id not provided, set to primary node id */
	if (node_record.upstream_node_id == UNKNOWN_NODE_ID)
	{
		node_record.upstream_node_id = primary_node_id;
	}

	/*
	 * If --upstream-node-id not provided, we're defaulting to the primary as
	 * upstream node. If local node is available, double-check that it's attached
	 * to the primary, in case --upstream-node-id was an accidental ommission.
	 *
	 * Currently we'll only do this for newly registered nodes.
	 */
	if (runtime_options.upstream_node_id == NO_UPSTREAM_NODE && PQstatus(conn) == CONNECTION_OK)
	{
		/* only do this if record does not exist */
		if (record_status != RECORD_FOUND)
		{
			log_warning(_("--upstream-node-id not supplied, assuming upstream node is primary (node ID %i)"),
						primary_node_id);

			/* check our standby is connected */
			if (is_downstream_node_attached(primary_conn, config_file_options.node_name) == true)
			{
				log_verbose(LOG_INFO, _("local node is attached to primary"));
			}
			else if (runtime_options.force == false)
			{
				log_error(_("local node not attached to primary node %i"), primary_node_id);
				/* TODO: 9.6 and later, display detail from pg_stat_wal_receiver */
				log_hint(_("specify the actual upstream node id with --upstream-node-id, or use -F/--force to continue anyway"));

				if (runtime_options.dry_run == true)
				{
					dry_run_ok = false;
				}
				else
				{
					PQfinish(primary_conn);
					PQfinish(conn);
					exit(ERR_BAD_CONFIG);
				}
			}
			else
			{
				log_warning(_("local node not attached to primary node %i"), primary_node_id);
				log_notice(_("-F/--force supplied, continuing anyway"));
			}
		}

	}

	if (runtime_options.dry_run == true)
	{
		PQfinish(primary_conn);
		if (PQstatus(conn) == CONNECTION_OK)
			PQfinish(conn);

		if (dry_run_ok == false)
		{
			log_warning(_("issue(s) encountered; see preceding log messages"));
			exit(ERR_BAD_CONFIG);
		}

		log_info(_("all prerequisites for \"standby register\" are met"));

		exit(SUCCESS);
	}

	/*
	 * node record exists - update it (at this point we have already
	 * established that -F/--force is in use)
	 */
	if (record_status == RECORD_FOUND)
	{
		record_created = update_node_record(primary_conn,
											"standby register",
											&node_record);
	}
	else
	{
		record_created = create_node_record(primary_conn,
											"standby register",
											&node_record);
	}

	initPQExpBuffer(&details);

	if (record_created == false)
	{
		appendPQExpBuffer(&details,
						  "standby registration failed");

		if (runtime_options.force == true)
			appendPQExpBuffer(&details,
							  " (-F/--force option was used)");

		create_event_notification_extended(
			primary_conn,
			&config_file_options,
			config_file_options.node_id,
			"standby_register",
			false,
			details.data,
			&event_info);

		termPQExpBuffer(&details);
		PQfinish(primary_conn);
		primary_conn = NULL;

		if (PQstatus(conn) == CONNECTION_OK)
			PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	appendPQExpBuffer(&details,
					  "standby registration succeeded");

	if (runtime_options.force == true)
		appendPQExpBuffer(&details,
						  " (-F/--force option was used)");


	/* Log the event */
	create_event_notification_extended(
		primary_conn,
		&config_file_options,
		config_file_options.node_id,
		"standby_register",
		true,
		details.data,
		&event_info);

	termPQExpBuffer(&details);

	/*
	 * If --wait-sync option set, wait for the records to synchronise
	 * (unless 0 seconds provided, which disables it, which is the same as
	 *  not providing the option). The default value is -1, which means
	 * no timeout.
	 */

	if (PQstatus(conn) == CONNECTION_OK &&
		runtime_options.wait_register_sync == true &&
		runtime_options.wait_register_sync_seconds != 0)
	{
		bool		sync_ok = false;
		int			timer = 0;
		RecordStatus node_record_status = RECORD_NOT_FOUND;
		t_node_info node_record_on_primary = T_NODE_INFO_INITIALIZER;
		t_node_info node_record_on_standby = T_NODE_INFO_INITIALIZER;

		node_record_status = get_node_record(primary_conn,
											 config_file_options.node_id,
											 &node_record_on_primary);

		if (node_record_status != RECORD_FOUND)
		{
			log_error(_("unable to retrieve node record from primary"));
			PQfinish(primary_conn);
			PQfinish(conn);
			exit(ERR_REGISTRATION_SYNC);
		}

		for (;;)
		{
			bool		records_match = true;

			/*
			 * If timeout set to a positive value, check if we've reached it and
			 * exit the loop
			 */
			if (runtime_options.wait_register_sync_seconds > 0 && runtime_options.wait_register_sync_seconds == timer)
				break;

			node_record_status = get_node_record(conn,
												 config_file_options.node_id,
												 &node_record_on_standby);

			if (node_record_status == RECORD_NOT_FOUND)
			{
				/* no record available yet on standby */
				records_match = false;
			}
			else if (node_record_status == RECORD_FOUND)
			{
				/* compare relevant fields */
				if (node_record_on_standby.upstream_node_id != node_record_on_primary.upstream_node_id)
					records_match = false;

				if (node_record_on_standby.type != node_record_on_primary.type)
					records_match = false;

				if (node_record_on_standby.priority != node_record_on_primary.priority)
					records_match = false;

				if (node_record_on_standby.active != node_record_on_primary.active)
					records_match = false;

				if (strcmp(node_record_on_standby.node_name, node_record_on_primary.node_name) != 0)
					records_match = false;

				if (strcmp(node_record_on_standby.conninfo, node_record_on_primary.conninfo) != 0)
					records_match = false;

				if (strcmp(node_record_on_standby.slot_name, node_record_on_primary.slot_name) != 0)
					records_match = false;

				if (records_match == true)
				{
					sync_ok = true;
					break;
				}
			}

			sleep(1);
			timer++;
		}

		/* Log the event */
		initPQExpBuffer(&details);

		if (sync_ok == false)
		{
			appendPQExpBuffer(&details,
							  _("node record was not synchronised after %i seconds"),
							  runtime_options.wait_register_sync_seconds);
		}
		else
		{
			appendPQExpBuffer(&details,
							  _("node record synchronised after %i seconds"),
							  timer);
		}

		create_event_notification_extended(
			primary_conn,
			&config_file_options,
			config_file_options.node_id,
			"standby_register_sync",
			sync_ok,
			details.data,
			&event_info);

		if (sync_ok == false)
		{
			log_error("%s", details.data);
			termPQExpBuffer(&details);
			PQfinish(primary_conn);
			PQfinish(conn);
			exit(ERR_REGISTRATION_SYNC);
		}

		log_info(_("node record on standby synchronised from primary"));
		log_detail("%s", details.data);
		termPQExpBuffer(&details);
	}


	PQfinish(primary_conn);

	if (PQstatus(conn) == CONNECTION_OK)
		PQfinish(conn);

	log_info(_("standby registration complete"));
	log_notice(_("standby node \"%s\" (id: %i) successfully registered"),
			   config_file_options.node_name, config_file_options.node_id);
	return;
}


/*
 * do_standby_unregister()
 *
 * Event(s):
 *  - standby_unregister
 */
void
do_standby_unregister(void)
{
	PGconn	   *conn = NULL;
	PGconn	   *primary_conn = NULL;

	int			target_node_id = UNKNOWN_NODE_ID;
	t_node_info node_info = T_NODE_INFO_INITIALIZER;

	bool		node_record_deleted = false;

	log_info(_("connecting to local standby"));
	conn = establish_db_connection(config_file_options.conninfo, true);

	/* check if there is a primary in this cluster */
	log_info(_("connecting to primary database"));

	primary_conn = get_primary_connection(conn, NULL, NULL);

	if (PQstatus(primary_conn) != CONNECTION_OK)
	{
		log_error(_("unable to connect to primary server"));
		log_detail("%s", PQerrorMessage(conn));
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * if --node-id was specified, unregister that node rather than the
	 * current one - this enables inactive nodes to be unregistered.
	 */
	if (runtime_options.node_id != UNKNOWN_NODE_ID)
		target_node_id = runtime_options.node_id;
	else
		target_node_id = config_file_options.node_id;

	/* Check node exists and is really a standby */

	if (get_node_record(primary_conn, target_node_id, &node_info) != RECORD_FOUND)
	{
		log_error(_("no record found for node %i"), target_node_id);
		PQfinish(primary_conn);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	if (node_info.type != STANDBY)
	{
		log_error(_("node %i is not a standby server"), target_node_id);
		PQfinish(primary_conn);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Now unregister the standby */
	log_notice(_("unregistering node %i"), target_node_id);
	node_record_deleted = delete_node_record(primary_conn,
											 target_node_id);

	if (node_record_deleted == false)
	{
		PQfinish(primary_conn);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	/* Log the event */
	create_event_notification(primary_conn,
							  &config_file_options,
							  target_node_id,
							  "standby_unregister",
							  true,
							  NULL);

	PQfinish(primary_conn);
	PQfinish(conn);

	log_info(_("standby unregistration complete"));

	return;
}


/*
 * do_standby_promote()
 *
 * Event(s):
 *  - standby_promote
 */
void
do_standby_promote(void)
{
	PGconn	   *conn = NULL;
	PGconn	   *current_primary_conn = NULL;

	RecoveryType recovery_type = RECTYPE_UNKNOWN;

	int			existing_primary_id = UNKNOWN_NODE_ID;

	conn = establish_db_connection(config_file_options.conninfo, true);

	log_verbose(LOG_INFO, _("connected to standby, checking its state"));

	/* Verify that standby is a supported server version */
	check_server_version(conn, "standby", true, NULL);

	/* Check we are in a standby node */
	recovery_type = get_recovery_type(conn);

	if (recovery_type != RECTYPE_STANDBY)
	{
		if (recovery_type == RECTYPE_PRIMARY)
		{
			log_error(_("STANDBY PROMOTE can only be executed on a standby node"));
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}
		else
		{
			log_error(_("connection to node lost"));
			PQfinish(conn);
			exit(ERR_DB_CONN);
		}
	}

	/* check that there's no existing primary */
	current_primary_conn = get_primary_connection_quiet(conn, &existing_primary_id, NULL);

	if (PQstatus(current_primary_conn) == CONNECTION_OK)
	{
		log_error(_("this cluster already has an active primary server"));

		if (existing_primary_id != UNKNOWN_NODE_ID)
		{
			t_node_info primary_rec;

			get_node_record(conn, existing_primary_id, &primary_rec);

			log_detail(_("current primary is %s (node_id: %i)"),
					   primary_rec.node_name,
					   existing_primary_id);
		}

		PQfinish(current_primary_conn);
		PQfinish(conn);
		exit(ERR_PROMOTION_FAIL);
	}

	PQfinish(current_primary_conn);

	_do_standby_promote_internal(conn);
}


static void
_do_standby_promote_internal(PGconn *conn)
{
	char		script[MAXLEN];
	int			r,
				i;
	bool		promote_success = false;
	PQExpBufferData details;

	RecoveryType recovery_type = RECTYPE_UNKNOWN;

	t_node_info local_node_record = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status = RECORD_NOT_FOUND;
	char		data_dir[MAXPGPATH];

	get_node_config_directory(data_dir);

	/* fetch local node record so we can add detail in log messages */
	record_status = get_node_record(conn,
									config_file_options.node_id,
									&local_node_record);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve record for node %i"),
				  config_file_options.node_id);
		PQfinish(conn);
		exit(ERR_BAD_CONFIG);
	}


	/*
	 * Promote standby to primary.
	 *
	 * `pg_ctl promote` returns immediately and (prior to 10.0) has no -w
	 * option so we can't be sure when or if the promotion completes. For now
	 * we'll poll the server until the default timeout (60 seconds)
	 */

	get_server_action(ACTION_PROMOTE, script, (char *) data_dir);

	log_notice(_("promoting standby to primary"));
	log_detail(_("promoting server \"%s\" (ID: %i) using \"%s\""),
			   local_node_record.node_name,
			   local_node_record.node_id,
			   script);

	r = system(script);
	if (r != 0)
	{
		log_error(_("unable to promote server from standby to primary"));
		exit(ERR_PROMOTION_FAIL);
	}

	for (i = 0; i < config_file_options.promote_check_timeout; i += config_file_options.promote_check_interval)
	{
		recovery_type = get_recovery_type(conn);

		if (recovery_type == RECTYPE_PRIMARY)
		{
			promote_success = true;
			break;
		}
		sleep(config_file_options.promote_check_interval);
	}

	if (promote_success == false)
	{
		if (recovery_type == RECTYPE_STANDBY)
		{
			log_error(_("STANDBY PROMOTE failed, node is still a standby"));
			PQfinish(conn);
			exit(ERR_PROMOTION_FAIL);
		}
		else
		{
			log_error(_("connection to node lost"));
			PQfinish(conn);
			exit(ERR_DB_CONN);
		}
	}

	log_verbose(LOG_INFO, _("standby promoted to primary after %i second(s)"), i);

	/* update node information to reflect new status */
	if (update_node_record_set_primary(conn, config_file_options.node_id) == false)
	{
		initPQExpBuffer(&details);
		appendPQExpBuffer(&details,
						  _("unable to update node record for node %i"),
						  config_file_options.node_id);

		log_error("%s", details.data);

		create_event_notification(NULL,
								  &config_file_options,
								  config_file_options.node_id,
								  "standby_promote",
								  false,
								  details.data);

		exit(ERR_DB_QUERY);
	}


	initPQExpBuffer(&details);
	appendPQExpBuffer(&details,
					  _("server \"%s\" (ID: %i) was successfully promoted to primary"),
					  local_node_record.node_name,
					  local_node_record.node_id);

	log_notice(_("STANDBY PROMOTE successful"));
	log_detail("%s", details.data);

	/* Log the event */
	create_event_notification(conn,
							  &config_file_options,
							  config_file_options.node_id,
							  "standby_promote",
							  true,
							  details.data);

	termPQExpBuffer(&details);

	return;
}


/*
 * Follow a new primary.
 *
 * Node must be running. To start an inactive node and point it at a
 * new primary, use "repmgr node rejoin".
 *
 * TODO: enable provision of new primary's conninfo parameters, which
 * will be necessary if the primary's information has changed, but
 * was not replicated to the current standby.
 */

void
do_standby_follow(void)
{
	PGconn	   *local_conn = NULL;

	PGconn	   *primary_conn = NULL;
	int			primary_node_id = UNKNOWN_NODE_ID;
	t_node_info primary_node_record = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status = RECORD_NOT_FOUND;
	/* so we can pass info about the primary to event notification scripts */
	t_event_info event_info = T_EVENT_INFO_INITIALIZER;

	int			timer = 0;
	int			server_version_num = UNKNOWN_SERVER_VERSION_NUM;

	PQExpBufferData follow_output;
	bool		success = false;
	int			follow_error_code = SUCCESS;

	uint64		local_system_identifier = UNKNOWN_SYSTEM_IDENTIFIER;
	t_conninfo_param_list repl_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;
	PGconn	   *repl_conn = NULL;
	t_system_identification primary_identification = T_SYSTEM_IDENTIFICATION_INITIALIZER;

	log_verbose(LOG_DEBUG, "do_standby_follow()");

	local_conn = establish_db_connection(config_file_options.conninfo, false);

	if (PQstatus(local_conn) != CONNECTION_OK)
	{
		log_hint(_("use \"repmgr node rejoin\" to re-add an inactive node to the replication cluster"));
		exit(ERR_DB_CONN);
	}

	log_verbose(LOG_INFO, _("connected to local node"));

	/* check this is a standby */
	check_recovery_type(local_conn);

	/* sanity-checks for 9.3 */
	server_version_num = get_server_version(local_conn, NULL);

	if (server_version_num < 90400)
		check_93_config();

	/*
	 * Attempt to connect to primary.
	 *
	 * If --wait provided, loop for up `primary_follow_timeout` seconds
	 * before giving up
	 */

	for (timer = 0; timer < config_file_options.primary_follow_timeout; timer++)
	{
		primary_conn = get_primary_connection_quiet(local_conn,
													&primary_node_id,
													NULL);
		if (PQstatus(primary_conn) == CONNECTION_OK || runtime_options.wait == false)
		{
			break;
		}
		sleep(1);
	}

	PQfinish(local_conn);

	if (PQstatus(primary_conn) != CONNECTION_OK)
	{
		log_error(_("unable to determine primary node"));

		if (runtime_options.wait == true)
		{
			log_detail(_("no primary appeared after %i seconds"),
					   config_file_options.primary_follow_timeout);
			log_hint(_("alter \"primary_follow_timeout\" in \"repmgr.conf\" to change this value"));
		}

		exit(ERR_FOLLOW_FAIL);
	}

	if (runtime_options.dry_run == true)
	{
		log_info(_("connected to node %i, checking for current primary"), primary_node_id);
	}
	else
	{
		log_verbose(LOG_INFO, _("connected to node %i, checking for current primary"), primary_node_id);
	}

	record_status = get_node_record(primary_conn, primary_node_id, &primary_node_record);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to find record for new upstream node %i"),
				  runtime_options.upstream_node_id);
		PQfinish(primary_conn);
		exit(ERR_FOLLOW_FAIL);
	}

	/*
	 * Populate "event_info" with info about the primary for event notifications
	 */
	event_info.node_id = primary_node_id;
	event_info.node_name = primary_node_record.node_name;
	event_info.conninfo_str = primary_node_record.conninfo;


	if (runtime_options.dry_run == true)
	{
		log_info(_("primary node is \"%s\" (ID: %i)"),
				 primary_node_record.node_name,
				 primary_node_id);
	}
	else
	{
		log_verbose(LOG_INFO, ("primary node is \"%s\" (ID: %i)"),
					primary_node_record.node_name,
					primary_node_id);
	}

	/* if replication slots in use, check at least one free slot is available */

	if (config_file_options.use_replication_slots)
	{
		int free_slots = get_free_replication_slot_count(primary_conn);
		if (free_slots < 0)
		{
			log_error(_("unable to determine number of free replication slots on the primary"));
			PQfinish(primary_conn);
			exit(ERR_FOLLOW_FAIL);
		}

		if (free_slots == 0)
		{
			log_error(_("no free replication slots available on the primary"));
			log_hint(_("consider increasing \"max_replication_slots\""));
			PQfinish(primary_conn);
			exit(ERR_FOLLOW_FAIL);
		}
		else if (runtime_options.dry_run == true)
		{
			log_info(_("replication slots in use, %i free slots on the primary"),
					 free_slots);
		}

	}

	/* XXX check this is not current upstream anyway */
	/* check replication connection */
	initialize_conninfo_params(&repl_conninfo, false);

	conn_to_param_list(primary_conn, &repl_conninfo);

	if (strcmp(param_get(&repl_conninfo, "user"), primary_node_record.repluser) != 0)
	{
		param_set(&repl_conninfo, "user", primary_node_record.repluser);
		param_set(&repl_conninfo, "dbname", "replication");
	}
	param_set(&repl_conninfo, "replication", "1");

	repl_conn = establish_db_connection_by_params(&repl_conninfo, false);
	if (PQstatus(repl_conn) != CONNECTION_OK)
	{
		log_error(_("unable to establish a replication connection to the primary node"));
		PQfinish(primary_conn);
		exit(ERR_FOLLOW_FAIL);
	}
	else if (runtime_options.dry_run == true)
	{
		log_info(_("replication connection to primary node was successful"));
	}


	/* check system_identifiers match */
	local_system_identifier = get_system_identifier(config_file_options.data_directory);
	success = identify_system(repl_conn, &primary_identification);

	if (success == false)
	{
		log_error(_("unable to query the primary node's system identification"));
		PQfinish(primary_conn);
		PQfinish(repl_conn);
		exit(ERR_FOLLOW_FAIL);
	}

	if (primary_identification.system_identifier != local_system_identifier)
	{
		log_error(_("this node is not part of the primary node's replication cluster"));
		log_detail(_("this node's system identifier is %lu, primary node's system identifier is %lu"),
				   local_system_identifier,
				   primary_identification.system_identifier);
		PQfinish(primary_conn);
		PQfinish(repl_conn);
		exit(ERR_FOLLOW_FAIL);
	}
	else if (runtime_options.dry_run == true)
	{
		log_info(_("local and primary system identifiers match"));
		log_detail(_("system identifier is %lu"), local_system_identifier);
	}

	/* TODO: check timelines */

	PQfinish(repl_conn);
	free_conninfo_params(&repl_conninfo);

	if (runtime_options.dry_run == true)
	{
		log_info(_("prerequisites for executing STANDBY FOLLOW are met"));
		exit(SUCCESS);
	}

	initPQExpBuffer(&follow_output);

	success = do_standby_follow_internal(primary_conn,
										 &primary_node_record,
										 &follow_output,
										 &follow_error_code);

	/* unable to restart the standby */
	if (success == false)
	{
		create_event_notification_extended(
			primary_conn,
			&config_file_options,
			config_file_options.node_id,
			"standby_follow",
			success,
			follow_output.data,
			&event_info);

		PQfinish(primary_conn);

		log_notice(_("STANDBY FOLLOW failed"));
		if (strlen( follow_output.data ))
			log_detail("%s", follow_output.data);

		termPQExpBuffer(&follow_output);
		exit(follow_error_code);
	}

	termPQExpBuffer(&follow_output);

	initPQExpBuffer(&follow_output);

	/*
	 * Wait up to "standby_follow_timeout" seconds for standby to connect to
	 * upstream.
	 * For 9.6 and later, we could check pg_stat_wal_receiver on the local node.
	 */

	/* assume success, necessary if standby_follow_timeout is zero */
	success = true;

	for (timer = 0; timer < config_file_options.standby_follow_timeout; timer++)
	{
		success = is_downstream_node_attached(primary_conn, config_file_options.node_name);
		if (success == true)
			break;

		log_verbose(LOG_DEBUG, "sleeping %i of max %i seconds waiting for standby to attach to primary",
					timer + 1,
					config_file_options.standby_follow_timeout);
		sleep(1);
	}

	if (success == true)
	{
		log_notice(_("STANDBY FOLLOW successful"));
		appendPQExpBuffer(&follow_output,
						  "standby attached to upstream node \"%s\" (node ID: %i)",
						  primary_node_record.node_name,
						  primary_node_id);
	}
	else
	{
		log_error(_("STANDBY FOLLOW failed"));
		appendPQExpBuffer(&follow_output,
						  "standby did not attach to upstream node \"%s\" (node ID: %i) after %i seconds",
						  primary_node_record.node_name,
						  primary_node_id,
						  config_file_options.standby_follow_timeout);

	}

	log_detail("%s", follow_output.data);

	create_event_notification_extended(
		primary_conn,
		&config_file_options,
		config_file_options.node_id,
		"standby_follow",
		success,
		follow_output.data,
		&event_info);

	PQfinish(primary_conn);

	termPQExpBuffer(&follow_output);

	if (success == false)
		exit(ERR_FOLLOW_FAIL);

	return;
}


/*
 * Perform the actuall "follow" operation; this is executed by
 * "node rejoin" too.
 *
 * For PostgreSQL 9.3, ensure check_93_config() was called before calling
 * this function.
 */
bool
do_standby_follow_internal(PGconn *primary_conn, t_node_info *primary_node_record, PQExpBufferData *output, int *error_code)
{
	t_node_info local_node_record = T_NODE_INFO_INITIALIZER;
	int			original_upstream_node_id = UNKNOWN_NODE_ID;
	t_node_info original_upstream_node_record = T_NODE_INFO_INITIALIZER;

	RecordStatus record_status = RECORD_NOT_FOUND;
	char	   *errmsg = NULL;

	bool		remove_old_replication_slot = false;
	/*
	 * Fetch our node record so we can write application_name, if set, and to
	 * get the upstream node ID, which we'll need to know if replication slots
	 * are in use and we want to delete the old slot.
	 */
	record_status = get_node_record(primary_conn,
									config_file_options.node_id,
									&local_node_record);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve record for node %i"),
				  config_file_options.node_id);

		*error_code = ERR_FOLLOW_FAIL;
		return false;
	}

	/*
	 * If replication slots are in use, we'll need to create a slot on the new
	 * primary
	 */

	if (config_file_options.use_replication_slots)
	{
		int			primary_server_version_num = get_server_version(primary_conn, NULL);

		/*
		 * Here we add a sanity check for the "slot_name" field - it's possible
		 * the node was initially registered with "use_replication_slots=false"
		 * but the configuration was subsequently changed, leaving the field NULL.
		 *
		 * To avoid annoying failures we can just update the node record and proceed.
		 */

		if (!strlen(local_node_record.slot_name))
		{
			create_slot_name(local_node_record.slot_name, config_file_options.node_id);

			log_notice(_("setting node %i's slot name to \"%s\""),
					   config_file_options.node_id,
					   local_node_record.slot_name);

			update_node_record_slot_name(primary_conn, config_file_options.node_id, local_node_record.slot_name);
		}


		if (create_replication_slot(primary_conn,
									local_node_record.slot_name,
									primary_server_version_num,
									output) == false)
		{
			log_error("%s", output->data);

			return false;
		}
	}

	/* Initialise connection parameters to write as `primary_conninfo` */
	initialize_conninfo_params(&recovery_conninfo, false);

	/* We ignore any application_name set in the primary's conninfo */
	parse_conninfo_string(primary_node_record->conninfo, &recovery_conninfo, &errmsg, true);

	{
		t_conninfo_param_list local_node_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;
		bool		parse_success;

		initialize_conninfo_params(&local_node_conninfo, false);

		parse_success = parse_conninfo_string(local_node_record.conninfo, &local_node_conninfo, &errmsg, false);

		if (parse_success == false)
		{
			/*
			 * this shouldn't happen, but if it does we'll plough on
			 * regardless
			 */
			log_warning(_("unable to parse conninfo string \"%s\":\n  %s"),
						local_node_record.conninfo, errmsg);
		}
		else
		{
			char	   *application_name = param_get(&local_node_conninfo, "application_name");

			if (application_name != NULL && strlen(application_name))
				param_set(&recovery_conninfo, "application_name", application_name);
		}

		free_conninfo_params(&local_node_conninfo);

		/*
		 * store the original upstream node id so we can delete the
		 * replication slot, if exists
		 */
		if (local_node_record.upstream_node_id != UNKNOWN_NODE_ID)
		{
			original_upstream_node_id = local_node_record.upstream_node_id;
		}
		else
		{
			original_upstream_node_id = primary_node_record->node_id;
		}


		if (config_file_options.use_replication_slots && runtime_options.host_param_provided == false && original_upstream_node_id != UNKNOWN_NODE_ID)
		{
			remove_old_replication_slot = true;
		}
	}

	/* Fetch original upstream's record */
	if (remove_old_replication_slot == true)
	{
		PGconn	    *local_conn = NULL;
		RecordStatus upstream_record_status = RECORD_NOT_FOUND;

		/* abort if local connection not available */
		local_conn = establish_db_connection(config_file_options.conninfo, true);

		upstream_record_status = get_node_record(local_conn,
												 original_upstream_node_id,
												 &original_upstream_node_record);
		PQfinish(local_conn);

		if (upstream_record_status != RECORD_FOUND)
		{
			log_warning(_("unable to retrieve node record for old upstream node %i"),
						original_upstream_node_id);
			log_detail(_("replication slot will need to be removed manually"));
		}
	}

	/* Set the application name to this node's name */
	param_set(&recovery_conninfo, "application_name", config_file_options.node_name);

	/* Set the replication user from the primary node record */
	param_set(&recovery_conninfo, "user", primary_node_record->repluser);

	log_notice(_("setting node %i's primary to node %i"),
			   config_file_options.node_id, primary_node_record->node_id);

	if (!create_recovery_file(&local_node_record, &recovery_conninfo, config_file_options.data_directory, true))
	{
		*error_code = ERR_FOLLOW_FAIL;
		return false;
	}

	/*
	 * start/restart the service
	 */

	{
		char		server_command[MAXLEN] = "";
		bool		server_up = is_server_available(config_file_options.conninfo);
		char	   *action = NULL;
		bool		success;

		PQExpBufferData output_buf;
		initPQExpBuffer(&output_buf);

		if (server_up == true)
		{
			/* no "service_restart_command" defined - stop and start using pg_ctl*/
			if (config_file_options.service_restart_command[0] == '\0')
			{
				action = "stopp"; /* sic */
				get_server_action(ACTION_STOP_WAIT, server_command, config_file_options.data_directory);

				/* if translation needed, generate messages in the preceding if/else */
				log_notice(_("%sing server using \"%s\""),
						   action,
						   server_command);

				success = local_command(server_command, &output_buf);

				if (success == false)
				{
					log_error(_("unable to %s server"), action);

					*error_code = ERR_NO_RESTART;
					return false;
				}

				action = "start";
				get_server_action(ACTION_START, server_command, config_file_options.data_directory);

				/* if translation needed, generate messages in the preceding if/else */
				log_notice(_("%sing server using \"%s\""),
						   action,
						   server_command);

				success = local_command(server_command, &output_buf);

				if (success == false)
				{
					log_error(_("unable to %s server"), action);

					*error_code = ERR_NO_RESTART;
					return false;
				}

			}
			else
			{
				action = "restart";
				get_server_action(ACTION_RESTART, server_command, config_file_options.data_directory);

				/* if translation needed, generate messages in the preceding if/else */
				log_notice(_("%sing server using \"%s\""),
						   action,
						   server_command);

				success = local_command(server_command, &output_buf);

				if (success == false)
				{
					log_error(_("unable to %s server"), action);

					*error_code = ERR_NO_RESTART;
					return false;
				}

			}
		}
		else
		{

			action = "start";
			get_server_action(ACTION_START, server_command, config_file_options.data_directory);

			/* if translation needed, generate messages in the preceding if/else */
			log_notice(_("%sing server using \"%s\""),
					   action,
					   server_command);

			success = local_command(server_command, &output_buf);

			if (success == false)
			{
				log_error(_("unable to %s server"), action);

				*error_code = ERR_NO_RESTART;
				return false;
			}
		}
	}

	/*
	 * If replication slots are in use, and an inactive one for this node
	 * exists on the former upstream, drop it.
	 *
	 * XXX check if former upstream is current primary?
	 */

	if (remove_old_replication_slot == true)
	{
		if (original_upstream_node_record.node_id != UNKNOWN_NODE_ID)
		{
			PGconn	   *old_upstream_conn = establish_db_connection_quiet(original_upstream_node_record.conninfo);

			if (PQstatus(old_upstream_conn) != CONNECTION_OK)
			{
				log_warning(_("unable to connect to old upstream node %i to remove replication slot"),
							original_upstream_node_id);
				log_hint(_("if reusing this node, you should manually remove any inactive replication slots"));
			}
			else
			{
				drop_replication_slot_if_exists(old_upstream_conn,
												original_upstream_node_id,
												local_node_record.slot_name);
				PQfinish(old_upstream_conn);
			}
		}
	}

	/*
	 * It's possible this node was an inactive primary - update the relevant
	 * fields to ensure it's marked as an active standby
	 */
	if (update_node_record_status(primary_conn,
								  config_file_options.node_id,
								  "standby",
								  primary_node_record->node_id,
								  true) == false)
	{
		appendPQExpBuffer(output,
						  _("unable to update upstream node"));
		return false;
	}


	appendPQExpBuffer(output,
					  _("node %i is now attached to node %i"),
					  config_file_options.node_id,
					  primary_node_record->node_id);

	return true;
}


/*
 * Perform a switchover by:
 *  - stopping current primary node
 *  - promoting this standby node to primary
 *  - forcing previous primary node to follow this node
 *
 * Caveat:
 *  - repmgrd must not be running, otherwise it may
 *    attempt a failover
 *    (TODO: find some way of notifying repmgrd of planned
 *     activity like this)
 *
 * TODO:
 *  - make connection test timeouts/intervals configurable (see below)
 */


void
do_standby_switchover(void)
{
	PGconn	   *local_conn = NULL;
	PGconn	   *remote_conn = NULL;

	t_node_info local_node_record = T_NODE_INFO_INITIALIZER;

	/* the remote server is the primary to be demoted */
	char		remote_conninfo[MAXCONNINFO] = "";
	char		remote_host[MAXLEN] = "";
	int			remote_node_id = UNKNOWN_NODE_ID;
	t_node_info remote_node_record = T_NODE_INFO_INITIALIZER;

	RecordStatus record_status = RECORD_NOT_FOUND;
	RecoveryType recovery_type = RECTYPE_UNKNOWN;
	PQExpBufferData remote_command_str;
	PQExpBufferData command_output;
	PQExpBufferData node_rejoin_options;

	int			r,
				i;
	bool		command_success = false;
	bool		shutdown_success = false;

	/* this flag will use to generate the final message generated */
	bool		switchover_success = true;

	XLogRecPtr	remote_last_checkpoint_lsn = InvalidXLogRecPtr;
	ReplInfo	replication_info = T_REPLINFO_INTIALIZER;

	/* store list of configuration files on the demotion candidate */
	KeyValueList remote_config_files = {NULL, NULL};

	/* store list of sibling nodes if --siblings-follow specified */
	NodeInfoList sibling_nodes = T_NODE_INFO_LIST_INITIALIZER;
	int			reachable_sibling_node_count = 0;
	int			reachable_sibling_nodes_with_slot_count = 0;
	int			unreachable_sibling_node_count = 0;

	/* number of free replication slots required on promotion candidate */
	int			min_required_free_slots = 0;

	t_event_info event_info = T_EVENT_INFO_INITIALIZER;

	/*
	 * SANITY CHECKS
	 *
	 * We'll be doing a bunch of operations on the remote server (primary to
	 * be demoted) - careful checks needed before proceding.
	 */

	local_conn = establish_db_connection(config_file_options.conninfo, true);

	record_status = get_node_record(local_conn, config_file_options.node_id, &local_node_record);
	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve node record for node %i"),
				  config_file_options.node_id);

		PQfinish(local_conn);

		exit(ERR_DB_QUERY);
	}

	if (!is_streaming_replication(local_node_record.type))
	{
		log_error(_("switchover can only performed with streaming replication"));
		PQfinish(local_conn);
		exit(ERR_BAD_CONFIG);
	}

	if (runtime_options.dry_run == true)
	{
		log_notice(_("checking switchover on node \"%s\" (ID: %i) in --dry-run mode"),
				   local_node_record.node_name,
				   local_node_record.node_id);
	}
	else
	{
		log_notice(_("executing switchover on node \"%s\" (ID: %i)"),
				   local_node_record.node_name,
				   local_node_record.node_id);
	}

	/* Check that this is a standby */
	recovery_type = get_recovery_type(local_conn);
	if (recovery_type != RECTYPE_STANDBY)
	{
		log_error(_("switchover must be executed from the standby node to be promoted"));
		if (recovery_type == RECTYPE_PRIMARY)
		{
			log_detail(_("this node (ID: %i) is the primary"),
					   local_node_record.node_id);
		}
		PQfinish(local_conn);

		exit(ERR_SWITCHOVER_FAIL);
	}

	/* check remote server connection and retrieve its record */
	remote_conn = get_primary_connection(local_conn, &remote_node_id, remote_conninfo);

	if (PQstatus(remote_conn) != CONNECTION_OK)
	{
		log_error(_("unable to connect to current primary node"));
		log_hint(_("check that the cluster is correctly configured and this standby is registered"));
		PQfinish(local_conn);
		exit(ERR_DB_CONN);
	}

	record_status = get_node_record(remote_conn, remote_node_id, &remote_node_record);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve node record for node %i"),
				  remote_node_id);

		PQfinish(local_conn);
		PQfinish(remote_conn);

		exit(ERR_DB_QUERY);
	}

	/*
	 * Check that there's no exclusive backups running on the primary.
	 * We don't want to end up damaging the backup and also leaving the server in an
	 * state where there's control data saying it's in backup mode but there's no
	 * backup_label in PGDATA.
	 * If the DBA wants to do the switchover anyway, he should first stop the
	 * backup that's running.
	 */
	if (!server_not_in_exclusive_backup_mode(remote_conn))
	{
		log_error(_("can't perform a switchover while primary server is in exclusive backup mode"));
		log_hint(_("stop backup before attempting the switchover"));

		PQfinish(remote_conn);

		exit(ERR_SWITCHOVER_FAIL);
	}

	/*
	 * Check this standby is attached to the demotion candidate
	 * TODO:
	 *  - check application_name in pg_stat_replication
	 */

	if (local_node_record.upstream_node_id != remote_node_record.node_id)
	{
		log_error(_("local node %i is not a downstream of demotion candidate primary %i"),
				  local_node_record.node_id,
				  remote_node_record.node_id);

		PQfinish(local_conn);
		PQfinish(remote_conn);

		exit(ERR_BAD_CONFIG);
	}

	log_verbose(LOG_DEBUG, "remote node name is \"%s\"", remote_node_record.node_name);

	/* this will fill the %p event notification parameter */
	event_info.node_id = remote_node_record.node_id;

	/* keep a running total of how many nodes will require a replication slot */
	if (remote_node_record.slot_name[0] != '\0')
	{
		min_required_free_slots++;
	}
	/*
	 * If --force-rewind specified, check pg_rewind can be used, and
	 * pre-emptively fetch the list of configuration files which should be
	 * archived
	 */

	if (runtime_options.force_rewind_used == true)
	{
		PQExpBufferData reason;
		PQExpBufferData msg;

		initPQExpBuffer(&reason);

		if (can_use_pg_rewind(remote_conn, config_file_options.data_directory, &reason) == false)
		{
			log_error(_("--force-rewind specified but pg_rewind cannot be used"));
			log_detail("%s", reason.data);
			termPQExpBuffer(&reason);
			PQfinish(local_conn);
			PQfinish(remote_conn);

			exit(ERR_BAD_CONFIG);
		}
		termPQExpBuffer(&reason);

		initPQExpBuffer(&msg);
		appendPQExpBuffer(&msg,
						  _("prerequisites for using pg_rewind are met"));

		if (runtime_options.dry_run == true)
		{
			log_info("%s", msg.data);
		}
		else
		{
			log_verbose(LOG_INFO, "%s", msg.data);
		}
		termPQExpBuffer(&msg);

		get_datadir_configuration_files(remote_conn, &remote_config_files);
	}


	/*
	 * Check that we can connect by SSH to the remote (current primary) server
	 */
	get_conninfo_value(remote_conninfo, "host", remote_host);

	r = test_ssh_connection(remote_host, runtime_options.remote_user);

	if (r != 0)
	{
		log_error(_("unable to connect via SSH to host \"%s\", user \"%s\""),
				  remote_host, runtime_options.remote_user);
		PQfinish(remote_conn);
		PQfinish(local_conn);

		exit(ERR_BAD_CONFIG);
	}
	else
	{
		PQExpBufferData msg;

		initPQExpBuffer(&msg);

		appendPQExpBuffer(&msg,
						  _("SSH connection to host \"%s\" succeeded"),
						  remote_host);

		if (runtime_options.dry_run == true)
		{
			log_info("%s", msg.data);
		}
		else
		{
			log_verbose(LOG_INFO, "%s", msg.data);
		}

		termPQExpBuffer(&msg);
	}

	/* check remote repmgr binary can be found */
	initPQExpBuffer(&remote_command_str);
	make_remote_repmgr_path(&remote_command_str, &remote_node_record);

	appendPQExpBuffer(&remote_command_str, "--version 2>/dev/null && echo \"1\" || echo \"0\"");
	initPQExpBuffer(&command_output);
	command_success = remote_command(remote_host,
									 runtime_options.remote_user,
									 remote_command_str.data,
									 &command_output);

	termPQExpBuffer(&remote_command_str);

	if (command_success == false || command_output.data[0] == '0')
	{
		PQExpBufferData hint;

		log_error(_("unable to execute \"%s\" on \"%s\""),
				  progname(), remote_host);

		if (strlen(command_output.data) > 2)
			log_detail("%s", command_output.data);

		termPQExpBuffer(&command_output);

		initPQExpBuffer(&hint);
		appendPQExpBuffer(&hint,
						  _("check \"pg_bindir\" is set to the correct path in \"repmgr.conf\"; current value: "));

		if (strlen(config_file_options.pg_bindir))
		{
			appendPQExpBuffer(&hint,
							  "\"%s\"", config_file_options.pg_bindir);
		}
		else
		{
			appendPQExpBuffer(&hint,
							  "(not set)");
		}


		log_hint("%s", hint.data);

		termPQExpBuffer(&hint);

		PQfinish(remote_conn);
		PQfinish(local_conn);

		exit(ERR_BAD_CONFIG);
	}

	if (runtime_options.dry_run == true)
	{
		log_info(_("able to execute \"%s\" on remote host \"localhost\""), progname());
	}
	termPQExpBuffer(&command_output);

	/* check demotion candidate can make replication connection to promotion candidate */
	{
		initPQExpBuffer(&remote_command_str);
		make_remote_repmgr_path(&remote_command_str, &remote_node_record);
		appendPQExpBuffer(&remote_command_str,
						  "node check --remote-node-id=%i --replication-connection",
						  local_node_record.node_id);

		initPQExpBuffer(&command_output);

		command_success = remote_command(remote_host,
										 runtime_options.remote_user,
										 remote_command_str.data,
										 &command_output);

		termPQExpBuffer(&remote_command_str);

		if (command_success == true)
		{
			ConnectionStatus conn_status = parse_remote_node_replication_connection(command_output.data);

			switch(conn_status)
			{
				case CONN_OK:
					if (runtime_options.dry_run == true)
					{
						log_info(_("demotion candidate is able to make replication connection to promotion candidate"));
					}
					break;
				case CONN_BAD:
					log_error(_("demotion candidate is unable to make replication connection to promotion candidate"));
					exit(ERR_BAD_CONFIG);
					break;
				default:
					log_error(_("unable to determine whether demotion candidate is able to make replication connection to promotion candidate"));
					exit(ERR_BAD_CONFIG);
					break;
			}

			termPQExpBuffer(&command_output);
		}
	}

	/* check archive/replication status */
	{
		int			lag_seconds = 0;
		CheckStatus status = CHECK_STATUS_UNKNOWN;

		/* archive status - check when "archive_mode" is activated */

		if (guc_set(remote_conn, "archive_mode", "!=", "off"))
		{
			int			files = 0;
			int			threshold = 0;

			initPQExpBuffer(&remote_command_str);
			make_remote_repmgr_path(&remote_command_str, &remote_node_record);
			appendPQExpBuffer(&remote_command_str,
							  "node check --terse -LERROR --archive-ready --optformat");

			initPQExpBuffer(&command_output);

			command_success = remote_command(
											 remote_host,
											 runtime_options.remote_user,
											 remote_command_str.data,
											 &command_output);

			termPQExpBuffer(&remote_command_str);

			if (command_success == true)
			{
				status = parse_node_check_archiver(command_output.data, &files, &threshold);
			}

			termPQExpBuffer(&command_output);

			switch (status)
			{
				case CHECK_STATUS_UNKNOWN:
					{
						if (runtime_options.force == false)
						{
							log_error(_("unable to check number of pending archive files on demotion candidate \"%s\""),
									  remote_node_record.node_name);
							log_hint(_("use -F/--force to continue anyway"));
							PQfinish(remote_conn);
							PQfinish(local_conn);

							exit(ERR_SWITCHOVER_FAIL);
						}

						log_warning(_("unable to check number of pending archive files on demotion candidate \"%s\""),
									remote_node_record.node_name);
						log_notice(_("-F/--force set, continuing with switchover"));
					}
					break;

				case CHECK_STATUS_CRITICAL:
					{
						if (runtime_options.force == false)
						{
							log_error(_("number of pending archive files on demotion candidate \"%s\" is critical"),
									  remote_node_record.node_name);
							log_detail(_("%i pending archive files (critical threshold: %i)"),
									   files, threshold);
							log_hint(_("PostgreSQL will not shut down until all files are archived; use -F/--force to continue anyway"));
							PQfinish(remote_conn);
							PQfinish(local_conn);

							exit(ERR_SWITCHOVER_FAIL);
						}

						log_warning(_("number of pending archive files on demotion candidate \"%s\" is critical"),
									remote_node_record.node_name);
						log_detail(_("%i pending archive files (critical threshold: %i)"),
								   files, threshold);
						log_notice(_("-F/--force set, continuing with switchover"));
					}
					break;

				case CHECK_STATUS_WARNING:
					{
						log_warning(_("number of pending archive files on demotion candidate \"%s\" is warning"),
									remote_node_record.node_name);
						log_detail(_("%i pending archive files (warning threshold: %i)"),
								   files, threshold);
						log_hint(_("PostgreSQL will not shut down until all files are archived"));
					}
					break;

				case CHECK_STATUS_OK:
					{
						PQExpBufferData msg;

						initPQExpBuffer(&msg);

						appendPQExpBuffer(&msg,
										  _("%i pending archive files"),
										  files);

						if (runtime_options.dry_run == true)
						{
							log_info("%s", msg.data);
						}
						else
						{
							log_verbose(LOG_INFO, "%s", msg.data);
						}

						termPQExpBuffer(&msg);
					}
			}

		}
		else
		{
			char	   *msg = _("archive mode is \"off\"");

			if (runtime_options.dry_run == true)
			{
				log_info("%s", msg);
			}
			else
			{
				log_verbose(LOG_INFO, "%s", msg);
			}
		}

		/*
		 * check replication lag on promotion candidate (TODO: check on all
		 * nodes attached to demotion candidate)
		 */
		lag_seconds = get_replication_lag_seconds(local_conn);

		log_debug("lag is %i ", lag_seconds);

		if (lag_seconds >= config_file_options.replication_lag_critical)
		{
			if (runtime_options.force == false)
			{
				log_error(_("replication lag on this node is critical"));
				log_detail(_("lag is %i seconds (critical threshold: %i)"),
						   lag_seconds, config_file_options.replication_lag_critical);
				log_hint(_("PostgreSQL on the demotion candidate will not shut down until pending WAL is flushed to the standby; use -F/--force to continue anyway"));
				PQfinish(remote_conn);
				PQfinish(local_conn);

				exit(ERR_SWITCHOVER_FAIL);
			}

			log_warning(_("replication lag on this node is critical"));
			log_detail(_("lag is %i seconds (critical threshold: %i)"),
					   lag_seconds, config_file_options.replication_lag_critical);
			log_notice(_("-F/--force set, continuing with switchover"));
		}
		else if (lag_seconds >= config_file_options.replication_lag_warning)
		{
			log_warning(_("replication lag on this node is warning"));
			log_detail(_("lag is %i seconds (warning threshold: %i)"),
					   lag_seconds, config_file_options.replication_lag_warning);
		}
		else if (lag_seconds < 0)
		{
			if (runtime_options.force == false)
			{
				log_error(_("unable to check replication lag on local node"));
				log_hint(_("use -F/--force to continue anyway"));
				PQfinish(remote_conn);
				PQfinish(local_conn);

				exit(ERR_SWITCHOVER_FAIL);
			}

			log_warning(_("unable to check replication lag on local node"));
			log_notice(_("-F/--force set, continuing with switchover"));
		}
		/* replication lag is below warning threshold */
		else
		{
			PQExpBufferData msg;

			initPQExpBuffer(&msg);

			appendPQExpBuffer(&msg,
							  _("replication lag on this standby is %i seconds"),
							  lag_seconds);

			if (runtime_options.dry_run == true)
			{
				log_info("%s", msg.data);
			}
			else
			{
				log_verbose(LOG_INFO, "%s", msg.data);
			}

			termPQExpBuffer(&msg);
		}
	}

	PQfinish(remote_conn);

	/*
	 * populate local node record with current state of various replication-related
	 * values, so we can check for sufficient walsenders and replication slots
	 */
	get_node_replication_stats(local_conn, server_version_num, &local_node_record);

	/*
	 * If --siblings-follow specified, get list and check they're reachable
	 * (if not just issue a warning)
	 */
	get_active_sibling_node_records(local_conn,
									local_node_record.node_id,
									local_node_record.upstream_node_id,
									&sibling_nodes);

	if (runtime_options.siblings_follow == false)
	{
		if (sibling_nodes.node_count > 0)
		{
			log_warning(_("%i sibling nodes found, but option \"--siblings-follow\" not specified"),
						sibling_nodes.node_count);
			log_detail(_("these nodes will remain attached to the current primary"));
		}
	}
	else
	{
		char		host[MAXLEN] = "";
		NodeInfoListCell *cell;

		log_verbose(LOG_INFO, _("%i active sibling nodes found"),
					sibling_nodes.node_count);

		if (sibling_nodes.node_count == 0)
		{
			log_warning(_("option \"--sibling-nodes\" specified, but no sibling nodes exist"));
		}
		else
		{
			/* include walsender for promotion candidate in total */
			int			min_required_wal_senders = 1;
			int			available_wal_senders = local_node_record.max_wal_senders -
				local_node_record.attached_wal_receivers;

			for (cell = sibling_nodes.head; cell; cell = cell->next)
			{
				/* get host from node record */
				get_conninfo_value(cell->node_info->conninfo, "host", host);
				r = test_ssh_connection(host, runtime_options.remote_user);

				if (r != 0)
				{
					cell->node_info->reachable = false;
					unreachable_sibling_node_count++;
				}
				else
				{
					cell->node_info->reachable = true;
					reachable_sibling_node_count++;
					min_required_wal_senders++;

					if (cell->node_info->slot_name[0] != '\0')
					{
						reachable_sibling_nodes_with_slot_count++;
						min_required_free_slots++;
					}
				}
			}

			if (unreachable_sibling_node_count > 0)
			{
				if (runtime_options.force == false)
				{
					log_error(_("%i of %i sibling nodes unreachable via SSH:"),
							  unreachable_sibling_node_count,
							  sibling_nodes.node_count);
				}
				else
				{
					log_warning(_("%i of %i sibling nodes unreachable via SSH:"),
								unreachable_sibling_node_count,
								sibling_nodes.node_count);
				}

				/* display list of unreachable sibling nodes */
				for (cell = sibling_nodes.head; cell; cell = cell->next)
				{
					if (cell->node_info->reachable == true)
						continue;
					log_detail("  %s (ID: %i)",
							   cell->node_info->node_name,
							   cell->node_info->node_id);
				}

				if (runtime_options.force == false)
				{
					log_hint(_("use -F/--force to proceed in any case"));
					PQfinish(local_conn);
					exit(ERR_BAD_CONFIG);
				}

				if (runtime_options.dry_run == true)
				{
					log_detail(_("F/--force specified, would proceed anyway"));
				}
				else
				{
					log_detail(_("F/--force specified, proceeding anyway"));
				}
			}
			else
			{
				char	   *msg = _("all sibling nodes are reachable via SSH");

				if (runtime_options.dry_run == true)
				{
					log_info("%s", msg);
				}
				else
				{
					log_verbose(LOG_INFO, "%s", msg);
				}
			}

			/*
			 * check there are sufficient free walsenders - obviously there's potential
			 * for a later race condition if some walsenders come into use before the
			 * switchover operation gets around to attaching the sibling nodes, but
			 * this should catch any actual existing configuration issue.
			 */
			if (available_wal_senders < min_required_wal_senders)
			{
				if (runtime_options.force == false || runtime_options.dry_run == true)
				{
					log_error(_("insufficient free walsenders to attach all sibling nodes"));
					log_detail(_("at least %i walsenders required but only %i free walsenders on promotion candidate"),
							   min_required_wal_senders,
							   available_wal_senders);
					log_hint(_("increase parameter \"max_wal_senders\" or use -F/--force to proceed in any case"));

					if (runtime_options.dry_run == false)
					{
						PQfinish(local_conn);
						exit(ERR_BAD_CONFIG);
					}
				}
				else
				{
					log_warning(_("insufficient free walsenders to attach all sibling nodes"));
					log_detail(_("at least %i walsenders required but only %i free walsender(s) on promotion candidate"),
							   min_required_wal_senders,
							   available_wal_senders);
				}
			}
			else
			{
				if (runtime_options.dry_run == true)
				{
					log_info(_("%i walsenders required, %i available"),
							 min_required_wal_senders,
							 available_wal_senders);
				}
			}
		}
	}


	/*
	 * if replication slots are required by demotion candidate and/or siblings,
	 * check the promotion candidate has sufficient free slots
	 */

	if (min_required_free_slots > 0 )
	{
		int available_slots = local_node_record.max_replication_slots -
			local_node_record.active_replication_slots;

		log_debug("minimum of %i free slots (%i for siblings) required; %i available",
				  min_required_free_slots,
				  reachable_sibling_nodes_with_slot_count
				  , available_slots);

		if (available_slots < min_required_free_slots)
		{
			if (runtime_options.force == false || runtime_options.dry_run == true)
			{
				log_error(_("insufficient free replication slots to attach all nodes"));
				log_detail(_("at least %i additional replication slots required but only %i free slots available on promotion candidate"),
						   min_required_free_slots,
						   available_slots);
				log_hint(_("increase parameter \"max_replication_slots\" or use -F/--force to proceed in any case"));

				if (runtime_options.dry_run == false)
				{
					PQfinish(local_conn);
					exit(ERR_BAD_CONFIG);
				}
			}
		}
		else
		{
			if (runtime_options.dry_run == true)
			{
				log_info(_("%i replication slots required, %i available"),
						 min_required_free_slots,
						 available_slots);
			}
		}
	}


	/*
	 * Sanity checks completed - prepare for the switchover
	 */

	if (runtime_options.dry_run == true)
	{
		log_notice(_("local node \"%s\" (ID: %i) would be promoted to primary; "
					 "current primary \"%s\" (ID: %i) would be demoted to standby"),
				   local_node_record.node_name,
				   local_node_record.node_id,
				   remote_node_record.node_name,
				   remote_node_record.node_id);
	}
	else
	{
		log_notice(_("local node \"%s\" (ID: %i) will be promoted to primary; "
					 "current primary \"%s\" (ID: %i) will be demoted to standby"),
				   local_node_record.node_name,
				   local_node_record.node_id,
				   remote_node_record.node_name,
				   remote_node_record.node_id);
	}


	/*
	 * Stop the remote primary
	 *
	 * We'll issue the pg_ctl command but not force it not to wait; we'll
	 * check the connection from here - and error out if no shutdown is
	 * detected after a certain time.
	 */

	initPQExpBuffer(&remote_command_str);
	initPQExpBuffer(&command_output);

	make_remote_repmgr_path(&remote_command_str, &remote_node_record);

	if (runtime_options.dry_run == true)
	{
		appendPQExpBuffer(&remote_command_str,
						  "node service --terse -LERROR --list-actions --action=stop");

	}
	else
	{
		log_notice(_("stopping current primary node \"%s\" (ID: %i)"),
				   remote_node_record.node_name,
				   remote_node_record.node_id);
		appendPQExpBuffer(&remote_command_str,
						  "node service --action=stop --checkpoint");
	}

	/* XXX handle failure */

	(void) remote_command(remote_host,
						  runtime_options.remote_user,
						  remote_command_str.data,
						  &command_output);

	termPQExpBuffer(&remote_command_str);

	/*
	 * --dry-run ends here with display of command which would be used to shut
	 * down the remote server
	 */
	if (runtime_options.dry_run == true)
	{
		char		shutdown_command[MAXLEN] = "";

		strncpy(shutdown_command, command_output.data, MAXLEN);

		termPQExpBuffer(&command_output);

		string_remove_trailing_newlines(shutdown_command);

		log_info(_("following shutdown command would be run on node \"%s\":\n  \"%s\""),
				 remote_node_record.node_name,
				 shutdown_command);

		clear_node_info_list(&sibling_nodes);
		key_value_list_free(&remote_config_files);

		return;
	}

	termPQExpBuffer(&command_output);
	shutdown_success = false;

	/* loop for timeout waiting for current primary to stop */

	for (i = 0; i < config_file_options.reconnect_attempts; i++)
	{
		/* Check whether primary is available */
		PGPing		ping_res;

		log_info(_("checking primary status; %i of %i attempts"),
				 i + 1, config_file_options.reconnect_attempts);
		ping_res = PQping(remote_conninfo);

		log_debug("ping status is: %s", print_pqping_status(ping_res));

		/* database server could not be contacted */
		if (ping_res == PQPING_NO_RESPONSE || ping_res == PQPING_NO_ATTEMPT)
		{
			bool		command_success;

			/*
			 * remote server can't be contacted at protocol level - that
			 * doesn't necessarily mean it's shut down, so we'll ask its
			 * repmgr to check at data directory level, and if shut down also
			 * return the last checkpoint LSN.
			 */

			initPQExpBuffer(&remote_command_str);
			make_remote_repmgr_path(&remote_command_str, &remote_node_record);
			appendPQExpBuffer(&remote_command_str,
							  "node status --is-shutdown-cleanly");

			initPQExpBuffer(&command_output);

			command_success = remote_command(remote_host,
											 runtime_options.remote_user,
											 remote_command_str.data,
											 &command_output);

			termPQExpBuffer(&remote_command_str);

			if (command_success == true)
			{
				NodeStatus	status = parse_node_status_is_shutdown_cleanly(command_output.data, &remote_last_checkpoint_lsn);

				log_verbose(LOG_DEBUG, "remote node status is: %s", print_node_status(status));

				if (status == NODE_STATUS_DOWN && remote_last_checkpoint_lsn != InvalidXLogRecPtr)
				{
					shutdown_success = true;
					log_notice(_("current primary has been cleanly shut down at location %X/%X"),
							   format_lsn(remote_last_checkpoint_lsn));
					termPQExpBuffer(&command_output);

					break;
				}
				/* remote node did not shut down cleanly */
				else if (status == NODE_STATUS_UNCLEAN_SHUTDOWN)
				{
					if (!runtime_options.force)
					{
						log_error(_("current primary did not shut down cleanly, aborting"));
						log_hint(_("use -F/--force to promote current standby"));
						termPQExpBuffer(&command_output);
						exit(ERR_SWITCHOVER_FAIL);
					}
					log_error(_("current primary did not shut down cleanly, continuing anyway"));
					shutdown_success = true;
					break;
				}
				else if (status == NODE_STATUS_SHUTTING_DOWN)
				{
					log_info(_("remote node is still shutting down"));
				}
			}

			termPQExpBuffer(&command_output);
		}

		log_debug("sleeping %i seconds (\"reconnect_interval\") until next check",
				  config_file_options.reconnect_interval);
		sleep(config_file_options.reconnect_interval);
	}

	if (shutdown_success == false)
	{
		log_error(_("shutdown of the primary server could not be confirmed"));
		log_hint(_("check the primary server status before performing any further actions"));
		exit(ERR_SWITCHOVER_FAIL);
	}

	/* this is unlikely to happen, but check and handle gracefully anyway */
	if (PQstatus(local_conn) != CONNECTION_OK)
	{
		log_warning(_("connection to local node lost, reconnecting..."));
		local_conn = establish_db_connection(config_file_options.conninfo, false);

		if (PQstatus(local_conn) != CONNECTION_OK)
		{
			log_error(_("unable to reconnect to local node \"%s\""),
					  local_node_record.node_name);
			exit(ERR_DB_CONN);
		}
		log_verbose(LOG_INFO, _("successfully reconnected to local node"));
	}

	get_replication_info(local_conn, &replication_info);

	if (replication_info.last_wal_receive_lsn < remote_last_checkpoint_lsn)
	{
		log_warning(_("local node \"%s\" is behind shutdown primary \"%s\""),
					local_node_record.node_name,
					remote_node_record.node_name);
		log_detail(_("local node last receive LSN is %X/%X, primary shutdown checkpoint LSN is %X/%X"),
				   format_lsn(replication_info.last_wal_receive_lsn),
				   format_lsn(remote_last_checkpoint_lsn));

		if (runtime_options.always_promote == false)
		{
			log_notice(_("aborting switchover"));
			log_hint(_("use --always-promote to force promotion of standby"));
			PQfinish(local_conn);
			exit(ERR_SWITCHOVER_FAIL);
		}
	}

	/* promote standby (local node) */
	_do_standby_promote_internal(local_conn);


	/*
	 * if pg_rewind is requested, issue a checkpoint immediately after promoting
	 * the local node, as pg_rewind compares timelines on the basis of the value
	 * in pg_control, which is written at the first checkpoint, which might not
	 * occur immediately.
	 */
	if (runtime_options.force_rewind_used == true)
	{
		log_notice(_("issuing CHECKPOINT"));
		checkpoint(local_conn);
	}

	/*
	 * Execute `repmgr node rejoin` to create recovery.conf and start the
	 * remote server. Additionally execute "pg_rewind", if required and
	 * requested.
	 */
	initPQExpBuffer(&node_rejoin_options);
	if (replication_info.last_wal_receive_lsn < remote_last_checkpoint_lsn)
	{
		KeyValueListCell *cell = NULL;
		bool		first_entry = true;

		if (runtime_options.force_rewind_used == false)
		{
			log_error(_("new primary diverges from former primary and --force-rewind not provided"));
			log_hint(_("the former primary will need to be restored manually, or use \"repmgr node rejoin\""));
			termPQExpBuffer(&node_rejoin_options);
			PQfinish(local_conn);
			exit(ERR_SWITCHOVER_FAIL);
		}

		appendPQExpBuffer(&node_rejoin_options,
						  " --force-rewind");

		if (runtime_options.force_rewind_path[0] != '\0')
		{
			appendPQExpBuffer(&node_rejoin_options,
							  "=%s",
							  runtime_options.force_rewind_path);
		}
		appendPQExpBuffer(&node_rejoin_options,
						  " --config-files=");

		for (cell = remote_config_files.head; cell; cell = cell->next)
		{
			if (first_entry == false)
				appendPQExpBuffer(&node_rejoin_options, ",");
			else
				first_entry = false;

			appendPQExpBuffer(&node_rejoin_options, "%s", cell->key);
		}

		appendPQExpBuffer(&node_rejoin_options, " ");
	}

	key_value_list_free(&remote_config_files);

	initPQExpBuffer(&remote_command_str);
	make_remote_repmgr_path(&remote_command_str, &remote_node_record);

	appendPQExpBuffer(&remote_command_str,
					  "%s-d \\'%s\\' node rejoin",
					  node_rejoin_options.data,
					  local_node_record.conninfo);

	termPQExpBuffer(&node_rejoin_options);

	log_debug("executing:\n  %s", remote_command_str.data);
	initPQExpBuffer(&command_output);

	command_success = remote_command(remote_host,
									 runtime_options.remote_user,
									 remote_command_str.data,
									 &command_output);

	termPQExpBuffer(&remote_command_str);

	/* TODO: verify this node's record was updated correctly */

	if (command_success == false)
	{
		log_error(_("rejoin failed %i"), r);

		create_event_notification_extended(local_conn,
										   &config_file_options,
										   config_file_options.node_id,
										   "standby_switchover",
										   false,
										   command_output.data,
										   &event_info);
	}
	else
	{
		PQExpBufferData event_details;

		initPQExpBuffer(&event_details);

		appendPQExpBuffer(&event_details,
						  "node %i promoted to primary, node %i demoted to standby",
						  config_file_options.node_id,
						  remote_node_record.node_id);

		create_event_notification_extended(local_conn,
										   &config_file_options,
										   config_file_options.node_id,
										   "standby_switchover",
										   true,
										   event_details.data,
										   &event_info);
		termPQExpBuffer(&event_details);
	}

	termPQExpBuffer(&command_output);

	/*
	 * If --siblings-follow specified, attempt to make them follow the new
	 * primary
	 */
	if (runtime_options.siblings_follow == true && sibling_nodes.node_count > 0)
	{
		int			failed_follow_count = 0;
		char		host[MAXLEN] = "";
		NodeInfoListCell *cell = NULL;

		log_notice(_("executing STANDBY FOLLOW on %i of %i siblings"),
				   sibling_nodes.node_count - unreachable_sibling_node_count,
				   sibling_nodes.node_count);

		for (cell = sibling_nodes.head; cell; cell = cell->next)
		{
			bool		success = false;
			t_node_info sibling_node_record = T_NODE_INFO_INITIALIZER;

			/* skip nodes previously determined as unreachable */
			if (cell->node_info->reachable == false)
				continue;

			record_status = get_node_record(local_conn,
											cell->node_info->node_id,
											&sibling_node_record);

			initPQExpBuffer(&remote_command_str);
			make_remote_repmgr_path(&remote_command_str, &sibling_node_record);

			if (sibling_node_record.type == WITNESS)
			{
				appendPQExpBuffer(&remote_command_str,
								  "witness register -d \\'%s\\' --force 2>/dev/null && echo \"1\" || echo \"0\"",
								  local_node_record.conninfo);
			}
			else
			{
				appendPQExpBuffer(&remote_command_str,
								  "standby follow 2>/dev/null && echo \"1\" || echo \"0\"");
			}
			get_conninfo_value(cell->node_info->conninfo, "host", host);
			log_debug("executing:\n  %s", remote_command_str.data);

			initPQExpBuffer(&command_output);

			success = remote_command(host,
									 runtime_options.remote_user,
									 remote_command_str.data,
									 &command_output);

			termPQExpBuffer(&remote_command_str);

			if (success == false || command_output.data[0] == '0')
			{
				if (sibling_node_record.type == WITNESS)
				{
					log_warning(_("WITNESS REGISTER failed on node \"%s\""),
								cell->node_info->node_name);
				}
				else
				{
					log_warning(_("STANDBY FOLLOW failed on node \"%s\""),
								cell->node_info->node_name);
				}
				failed_follow_count++;
			}

			termPQExpBuffer(&command_output);
		}

		if (failed_follow_count == 0)
		{
			log_info(_("STANDBY FOLLOW successfully executed on all reachable sibling nodes"));
		}
		else
		{
			log_warning(_("execution of STANDBY FOLLOW failed on %i sibling nodes"),
						failed_follow_count);
		}

		/*
		 * TODO: double-check all expected nodes are in pg_stat_replication
		 * and entries in repmgr.nodes match
		 */
	}

	clear_node_info_list(&sibling_nodes);

	PQfinish(local_conn);

	/*
	 * Clean up remote node. It's possible that the standby is still starting up,
	 * so poll for a while until we get a connection.
	 */

	for (i = 0; i < config_file_options.standby_reconnect_timeout; i++)
	{
		remote_conn = establish_db_connection(remote_node_record.conninfo, false);

		if (PQstatus(remote_conn) == CONNECTION_OK)
			break;

		log_info(_("sleeping 1 second; %i of %i attempts (\"standby_reconnect_timeout\") to reconnect to demoted primary"),
				 i + 1,
				 config_file_options.standby_reconnect_timeout);
		sleep(1);
	}

	/* check new standby (old primary) is reachable */
	if (PQstatus(remote_conn) != CONNECTION_OK)
	{
		switchover_success = false;

		/* TODO: double-check whether new standby has attached */

		log_warning(_("switchover did not fully complete"));
		log_detail(_("node \"%s\" is now primary but node \"%s\" is not reachable"),
				   local_node_record.node_name,
				   remote_node_record.node_name);

		if (config_file_options.use_replication_slots == true)
		{
			log_hint(_("any inactive replication slots on the old primary will need to be dropped manually"));
		}
	}
	else
	{
		if (config_file_options.use_replication_slots == true)
		{
			drop_replication_slot_if_exists(remote_conn,
											remote_node_record.node_id,
											local_node_record.slot_name);
		}
		/* TODO warn about any inactive replication slots */

		log_notice(_("switchover was successful"));
		log_detail(_("node \"%s\" is now primary and node \"%s\" is attached as standby"),
				   local_node_record.node_name,
				   remote_node_record.node_name);

	}

	PQfinish(remote_conn);


	if (switchover_success == true)
	{
		log_notice(_("STANDBY SWITCHOVER has completed successfully"));
	}
	else
	{
		log_notice(_("STANDBY SWITCHOVER has completed with issues"));
		log_hint(_("see preceding log message(s) for details"));
		exit(ERR_SWITCHOVER_INCOMPLETE);
	}


	return;
}


static void
check_source_server()
{
	PGconn	   *superuser_conn = NULL;
	PGconn	   *privileged_conn = NULL;

	char		cluster_size[MAXLEN];
	char	   *connstr = NULL;

	t_node_info node_record = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status = RECORD_NOT_FOUND;
	ExtensionStatus extension_status = REPMGR_UNKNOWN;

	/* Attempt to connect to the upstream server to verify its configuration */
	log_verbose(LOG_DEBUG, "check_source_server()");
	log_info(_("connecting to source node"));

	connstr = param_list_to_string(&source_conninfo);
	log_detail(_("connection string is: %s"), connstr);
	pfree(connstr);

	source_conn = establish_db_connection_by_params(&source_conninfo, false);
	/*
	 * Unless in barman mode, exit with an error;
	 * establish_db_connection_by_params() will have already logged an error
	 * message
	 */
	if (PQstatus(source_conn) != CONNECTION_OK)
	{
		PQfinish(source_conn);
		source_conn = NULL;
		if (mode == barman)
			return;
		else
			exit(ERR_DB_CONN);
	}

	/*
	 * If a connection was established, perform some sanity checks on the
	 * provided upstream connection
	 */

	source_server_version_num = check_server_version(source_conn, "primary", true, NULL);

	if (get_cluster_size(source_conn, cluster_size) == false)
		exit(ERR_DB_QUERY);

	log_detail(_("current installation size is %s"),
			   cluster_size);

	/*
	 * If the upstream node is a standby, try to connect to the primary too so
	 * we can write an event record
	 */
	if (get_recovery_type(source_conn) == RECTYPE_STANDBY)
	{
		primary_conn = get_primary_connection(source_conn, NULL, NULL);

		if (PQstatus(primary_conn) != CONNECTION_OK)
		{
			log_error(_("unable to connect to primary node"));
			exit(ERR_BAD_CONFIG);
		}
	}
	else
	{
		primary_conn = source_conn;
	}

	/*
	 * Sanity-check that the primary node has a repmgr extension - if not
	 * present, fail with an error unless -F/--force is used (to enable repmgr
	 * to be used as a standalone clone tool)
	 */

	extension_status = get_repmgr_extension_status(primary_conn);

	if (extension_status != REPMGR_INSTALLED)
	{
		if (!runtime_options.force)
		{
			/* this is unlikely to happen */
			if (extension_status == REPMGR_UNKNOWN)
			{
				log_error(_("unable to determine status of \"repmgr\" extension"));
				log_detail("%s", PQerrorMessage(primary_conn));
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
				log_detail(_("repmgr extension is not available on the upstream node"));
			}

			log_hint(_("check that the upstream node is part of a repmgr cluster"));
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
		log_detail(_("STANDBY CLONE must be run with database superuser permissions"));
		log_hint(_("provide a database superuser name with -S/--superuser"));

		PQfinish(source_conn);
		source_conn = NULL;

		if (superuser_conn != NULL)
			PQfinish(superuser_conn);

		exit(ERR_BAD_CONFIG);
	}

	if (superuser_conn != NULL)
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
		if (is_pg_dir(local_data_directory) && runtime_options.force != true)
		{
			log_error(_("target data directory appears to be a PostgreSQL data directory"));
			log_detail(_("target data directory is \"%s\""), local_data_directory);
			log_hint(_("use -F/--force to overwrite the existing data directory"));
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/*
	 * Attempt to find the upstream node record
	 */
	if (runtime_options.upstream_node_id == NO_UPSTREAM_NODE)
		upstream_node_id = get_primary_node_id(source_conn);
	else
		upstream_node_id = runtime_options.upstream_node_id;

	log_debug("upstream_node_id determined as %i", upstream_node_id);

	if (upstream_node_id != UNKNOWN_NODE_ID)
	{
		record_status = get_node_record(source_conn, upstream_node_id, &node_record);
		if (record_status == RECORD_FOUND)
		{
			t_conninfo_param_list upstream_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;
			char	   *upstream_conninfo_user;

			initialize_conninfo_params(&upstream_conninfo, false);
			parse_conninfo_string(node_record.conninfo, &upstream_conninfo, NULL, false);

			strncpy(recovery_conninfo_str, node_record.conninfo, MAXLEN);
			strncpy(upstream_repluser, node_record.repluser, NAMEDATALEN);

			upstream_conninfo_user = param_get(&upstream_conninfo, "user");
			if (upstream_conninfo_user != NULL)
			{
				strncpy(upstream_user, upstream_conninfo_user, NAMEDATALEN);
			}
			else
			{
				get_conninfo_default_value("user", upstream_user, NAMEDATALEN);
			}

			log_verbose(LOG_DEBUG, "upstream_user is \"%s\"", upstream_user);

			upstream_conninfo_found = true;
		}

		/*
		 * check that there's no existing node record with the same name but
		 * different ID
		 */
		record_status = get_node_record_by_name(source_conn, config_file_options.node_name, &node_record);

		if (record_status == RECORD_FOUND && node_record.node_id != config_file_options.node_id)
		{
			log_error(_("another node (node_id: %i) already exists with node_name \"%s\""),
					  node_record.node_id,
					  config_file_options.node_name);
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/* disable configuration file options incompatible with 9.3 */
	if (source_server_version_num < 90400)
		check_93_config();

	check_upstream_config(source_conn, source_server_version_num, &node_record, true);
}


static void
check_source_server_via_barman()
{
	char		buf[MAXLEN] = "";
	char		barman_conninfo_str[MAXLEN] = "";
	t_conninfo_param_list barman_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;
	char	   *errmsg = NULL;
	bool		parse_success = false,
				command_success = false;
	char		where_condition[MAXLEN];
	PQExpBufferData command_output;
	PQExpBufferData repmgr_conninfo_buf;

	int			c = 0;

	get_barman_property(barman_conninfo_str, "conninfo", local_repmgr_tmp_directory);

	initialize_conninfo_params(&barman_conninfo, false);

	/*
	 * parse_conninfo_string() here will remove the upstream's
	 * `application_name`, if set
	 */
	parse_success = parse_conninfo_string(barman_conninfo_str, &barman_conninfo, &errmsg, true);

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

	if (upstream_node_id == UNKNOWN_NODE_ID)
	{
		maxlen_snprintf(where_condition, "type='primary' AND active IS TRUE");
	}
	else
	{
		maxlen_snprintf(where_condition, "node_id=%i", upstream_node_id);
	}

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

	upstream_conninfo_found = true;
	log_verbose(LOG_DEBUG,
				"upstream node conninfo string extracted via barman server: %s",
				recovery_conninfo_str);

	termPQExpBuffer(&command_output);
}


/*
 * check_upstream_config()
 *
 * Perform sanity check on upstream server configuration before starting cloning
 * process
 *
 * For PostreSQL 9.3, ensure check_93_config() is called before calling this.
 *
 * TODO:
 *  - check user is qualified to perform base backup
 */

static bool
check_upstream_config(PGconn *conn, int server_version_num, t_node_info *node_info, bool exit_on_error)
{
	int			i;
	bool		config_ok = true;
	char	   *wal_error_message = NULL;
	t_basebackup_options backup_options = T_BASEBACKUP_OPTIONS_INITIALIZER;
	bool		backup_options_ok = true;
	ItemList	backup_option_errors = {NULL, NULL};
	bool		xlog_stream = true;
	standy_clone_mode mode;

	/*
	 * Detecting the intended cloning mode
	 */
	mode = get_standby_clone_mode();

	/*
	 * Parse `pg_basebackup_options`, if set, to detect whether --xlog-method
	 * has been set to something other than `stream` (i.e. `fetch`), as this
	 * will influence some checks
	 */

	backup_options_ok = parse_pg_basebackup_options(
													config_file_options.pg_basebackup_options,
													&backup_options, server_version_num,
													&backup_option_errors);

	if (backup_options_ok == false)
	{
		if (exit_on_error == true)
		{
			log_error(_("error(s) encountered parsing \"pg_basebackup_options\""));
			print_error_list(&backup_option_errors, LOG_ERR);
			log_hint(_("\"pg_basebackup_options\" is: \"%s\""),
					 config_file_options.pg_basebackup_options);
			exit(ERR_BAD_CONFIG);
		}

		config_ok = false;
	}

	if (strlen(backup_options.xlog_method) && strcmp(backup_options.xlog_method, "stream") != 0)
		xlog_stream = false;

	/* Check that WAL level is set correctly */
	if (server_version_num < 90400)
	{
		i = guc_set(conn, "wal_level", "=", "hot_standby");
		wal_error_message = _("parameter \"wal_level\" must be set to \"hot_standby\"");
	}
	else
	{
		char	   *levels_pre96[] = {
			"hot_standby",
			"logical",
			NULL,
		};

		/*
		 * Note that in 9.6+, "hot_standby" and "archive" are accepted as
		 * aliases for "replica", but current_setting() will of course always
		 * return "replica"
		 */
		char	   *levels_96plus[] = {
			"replica",
			"logical",
			NULL,
		};

		char	  **levels;
		int			j = 0;

		if (server_version_num < 90600)
		{
			levels = (char **) levels_pre96;
			wal_error_message = _("parameter \"wal_level\" must be set to \"hot_standby\" or \"logical\"");
		}
		else
		{
			levels = (char **) levels_96plus;
			wal_error_message = _("parameter \"wal_level\" must be set to \"replica\" or \"logical\"");
		}

		do
		{
			i = guc_set(conn, "wal_level", "=", levels[j]);
			if (i)
			{
				break;
			}
			j++;
		} while (levels[j] != NULL);
	}

	if (i == 0 || i == -1)
	{
		if (i == 0)
		{
			log_error("%s", wal_error_message);
		}

		if (exit_on_error == true)
		{
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		config_ok = false;
	}

	if (config_file_options.use_replication_slots)
	{
		i = guc_set_typed(conn, "max_replication_slots", ">",
						  "0", "integer");
		if (i == 0 || i == -1)
		{
			if (i == 0)
			{
				log_error(_("parameter \"max_replication_slots\" must be set to at least 1 to enable replication slots"));
				log_hint(_("\"max_replication_slots\" should be set to at least the number of expected standbys"));
				if (exit_on_error == true)
				{
					PQfinish(conn);
					exit(ERR_BAD_CONFIG);
				}

				config_ok = false;
			}
		}
	}

	/*
	 * physical replication slots not available or not requested - check if
	 * there are any circumstances where `wal_keep_segments` should be set
	 */
	else if (mode != barman)
	{
		bool		check_wal_keep_segments = false;

		/*
		 * A non-zero `wal_keep_segments` value will almost certainly be
		 * required if pg_basebackup is being used with --xlog-method=fetch,
		 * *and* no restore command has been specified
		 */
		if (xlog_stream == false
			&& strcmp(config_file_options.restore_command, "") == 0)
		{
			check_wal_keep_segments = true;
		}

		if (check_wal_keep_segments == true)
		{
			i = guc_set_typed(conn, "wal_keep_segments", ">", "0", "integer");

			if (i == 0 || i == -1)
			{
				if (i == 0)
				{
					log_error(_("parameter \"wal_keep_segments\" on the upstream server must be be set to a non-zero value"));
					log_hint(_("Choose a value sufficiently high enough to retain enough WAL "
							   "until the standby has been cloned and started.\n "
							   "Alternatively set up WAL archiving using e.g. PgBarman and configure "
							   "'restore_command' in repmgr.conf to fetch WALs from there."));

					if (server_version_num >= 90400)
					{
						log_hint(_("In PostgreSQL 9.4 and later, replication slots can be used, which "
								   "do not require \"wal_keep_segments\" to be set "
								   "(set parameter \"use_replication_slots\" in repmgr.conf to enable)\n"
								   ));
					}
				}

				if (exit_on_error == true)
				{
					PQfinish(conn);
					exit(ERR_BAD_CONFIG);
				}

				config_ok = false;
			}
		}
	}


	/*
	 * If archive_mode is enabled, check that 'archive_command' is non empty
	 * (however it's not practical to check that it actually represents a
	 * valid command).
	 *
	 * From PostgreSQL 9.5, archive_mode can be one of 'off', 'on' or 'always'
	 * so for ease of backwards compatibility, rather than explicitly check
	 * for an enabled mode, check that it's not "off".
	 */

	if (guc_set(conn, "archive_mode", "!=", "off"))
	{
		i = guc_set(conn, "archive_command", "!=", "");

		if (i == 0 || i == -1)
		{
			if (i == 0)
				log_error(_("parameter \"archive_command\" must be set to a valid command"));

			if (exit_on_error == true)
			{
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}

			config_ok = false;
		}
	}


	/*
	 * Check that 'hot_standby' is on. This isn't strictly necessary for the
	 * primary server, however the assumption is that we'll be cloning
	 * standbys and thus copying the primary configuration; this way the
	 * standby will be correctly configured by default.
	 */

	i = guc_set(conn, "hot_standby", "=", "on");
	if (i == 0 || i == -1)
	{
		if (i == 0)
		{
			log_error(_("parameter 'hot_standby' must be set to 'on'"));
		}

		if (exit_on_error == true)
		{
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		config_ok = false;
	}

	i = guc_set_typed(conn, "max_wal_senders", ">", "0", "integer");
	if (i == 0 || i == -1)
	{
		if (i == 0)
		{
			log_error(_("parameter \"max_wal_senders\" must be set to be at least 1"));
			log_hint(_("\"max_wal_senders\" should be set to at least the number of expected standbys"));
		}

		if (exit_on_error == true)
		{
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		config_ok = false;
	}

	/*
	 * If using pg_basebackup, ensure sufficient replication connections can
	 * be made. There's no guarantee they'll still be available by the time
	 * pg_basebackup is executed, but there's nothing we can do about that.
	 */
	if (mode == pg_basebackup)
	{

		PGconn	  **connections;
		int			i;
		int			min_replication_connections = 1,
					possible_replication_connections = 0;

		t_conninfo_param_list repl_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;

		/*
		 * Make a copy of the connection parameter arrays, and append
		 * "replication"
		 */

		initialize_conninfo_params(&repl_conninfo, false);

		conn_to_param_list(conn, &repl_conninfo);

		param_set(&repl_conninfo, "replication", "1");

		if (runtime_options.replication_user[0] != '\0')
		{
			param_set(&repl_conninfo, "user", runtime_options.replication_user);
		}
		else if (upstream_repluser[0] != '\0')
		{
			param_set(&repl_conninfo, "user", upstream_repluser);
		}
		else if (node_info->repluser[0] != '\0')
		{
			param_set(&repl_conninfo, "user", node_info->repluser);
		}

		if (strcmp(param_get(&repl_conninfo, "user"), upstream_user) != 0)
		{
			param_set(&repl_conninfo, "dbname", "replication");
		}

		/*
		 * work out how many replication connections are required (1 or 2)
		 */

		if (xlog_stream == true)
			min_replication_connections += 1;

		log_verbose(LOG_NOTICE, "checking for available walsenders on source node (%i required)",
					min_replication_connections);

		connections = pg_malloc0(sizeof(PGconn *) * min_replication_connections);

		/*
		 * Attempt to create the minimum number of required concurrent
		 * connections
		 */
		for (i = 0; i < min_replication_connections; i++)
		{
			PGconn	   *replication_conn;

			replication_conn = establish_db_connection_by_params(&repl_conninfo, false);

			if (PQstatus(replication_conn) == CONNECTION_OK)
			{
				connections[i] = replication_conn;
				possible_replication_connections++;
			}
		}

		/* Close previously created connections */
		for (i = 0; i < possible_replication_connections; i++)
		{
			PQfinish(connections[i]);
		}

		pfree(connections);
		free_conninfo_params(&repl_conninfo);

		if (possible_replication_connections < min_replication_connections)
		{
			config_ok = false;

			/*
			 * XXX at this point we could check
			 * current_setting('max_wal_senders) - COUNT(*) FROM
			 * pg_stat_replication; if >= min_replication_connections we could
			 * infer possible authentication error / lack of permissions.
			 *
			 * Alternatively call PQconnectStart() and poll for
			 * presence/absence of CONNECTION_AUTH_OK ?
			 */
			log_error(_("unable to establish necessary replication connections"));

			log_hint(_("increase \"max_wal_senders\" by at least %i"),
					 min_replication_connections - possible_replication_connections);

			if (exit_on_error == true)
			{
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}
		}

		log_verbose(LOG_INFO, "sufficient walsenders available on source node (%i required)",
					min_replication_connections);
	}

	return config_ok;
}


/*
 * initialise_direct_clone()
 *
 * In pg_basebackup mode, configure the target data directory
 * if necessary, and fetch information about tablespaces and configuration
 * files.
 *
 * Event(s):
 * - standby_clone
 */
static void
initialise_direct_clone(t_node_info *node_record)
{
	/*
	 * Check the destination data directory can be used (in Barman mode, this
	 * directory will already have been created)
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
	 * pg_basebackup doesn't verify mappings, so any errors will not be
	 * caught. We'll do that here as a value-added service.
	 *
	 */

	if (config_file_options.tablespace_mapping.head != NULL)
	{
		if (source_server_version_num < 90400)
		{
			log_error(_("tablespace mapping not supported in PostgreSQL 9.3, ignoring"));
		}
		else
		{
			TablespaceListCell *cell;
			KeyValueList not_found = {NULL, NULL};
			int			total = 0,
						matched = 0;
			bool		success = false;


			for (cell = config_file_options.tablespace_mapping.head; cell; cell = cell->next)
			{
				char	   *old_dir_escaped = escape_string(source_conn, cell->old_dir);
				char		name[MAXLEN] = "";

				success = get_tablespace_name_by_location(source_conn, old_dir_escaped, name);
				pfree(old_dir_escaped);

				if (success == true)
				{
					matched++;
				}
				else
				{
					key_value_list_set(&not_found,
									   cell->old_dir,
									   "");
				}

				total++;
			}

			if (not_found.head != NULL)
			{
				PQExpBufferData detail;
				KeyValueListCell *kv_cell;

				log_error(_("%i of %i mapped tablespaces not found"),
						  total - matched, total);

				initPQExpBuffer(&detail);

				for (kv_cell = not_found.head; kv_cell; kv_cell = kv_cell->next)
				{
					appendPQExpBuffer(
						&detail,
						"  %s\n", kv_cell->key);
				}

				log_detail(_("following tablespaces not found:\n%s"),
						   detail.data);
				termPQExpBuffer(&detail);

				exit(ERR_BAD_CONFIG);
			}
		}
	}

	/*
	 * If replication slots requested, create appropriate slot on the source
	 * node; this must be done before pg_basebackup is called.
	 *
	 * Note: if the source node is different to the specified upstream node,
	 * we'll need to drop the slot and recreate it on the upstream.
	 *
	 * TODO: skip this for Pg10, and ensure temp slot option used
	 *
	 * Replication slots are not supported (and not very useful anyway) in
	 * Barman mode.
	 */

	if (config_file_options.use_replication_slots == true)
	{
		PGconn	   *superuser_conn = NULL;
		PGconn	   *privileged_conn = NULL;
		PQExpBufferData event_details;

		initPQExpBuffer(&event_details);

		get_superuser_connection(&source_conn, &superuser_conn, &privileged_conn);

		if (create_replication_slot(privileged_conn, node_record->slot_name, source_server_version_num, &event_details) == false)
		{
			log_error("%s", event_details.data);

			create_event_notification(primary_conn,
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

		log_verbose(LOG_INFO,
					_("replication slot \"%s\" created on source node"),
					node_record->slot_name);

		if (superuser_conn != NULL)
			PQfinish(superuser_conn);
	}

	return;
}


static int
run_basebackup(t_node_info *node_record)
{
	char		script[MAXLEN] = "";
	int			r = SUCCESS;
	PQExpBufferData params;
	TablespaceListCell *cell = NULL;
	t_basebackup_options backup_options = T_BASEBACKUP_OPTIONS_INITIALIZER;

	/*
	 * Parse the pg_basebackup_options provided in repmgr.conf - we'll want to
	 * check later whether certain options were set by the user
	 */
	parse_pg_basebackup_options(config_file_options.pg_basebackup_options,
								&backup_options,
								source_server_version_num,
								NULL);

	/* Create pg_basebackup command line options */

	initPQExpBuffer(&params);

	appendPQExpBuffer(&params, " -D %s", local_data_directory);

	/*
	 * conninfo string provided - pass it to pg_basebackup as the -d option
	 * (pg_basebackup doesn't require or want a database name, but for
	 * consistency with other applications accepts a conninfo string under
	 * -d/--dbname)
	 */
	if (runtime_options.conninfo_provided == true)
	{
		t_conninfo_param_list conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;
		char	   *conninfo_str = NULL;

		initialize_conninfo_params(&conninfo, false);

		/* string will already have been parsed */
		(void) parse_conninfo_string(runtime_options.dbname, &conninfo, NULL, false);

		if (runtime_options.replication_user[0] != '\0')
		{
			param_set(&conninfo, "user", runtime_options.replication_user);
		}
		else if (upstream_repluser[0] != '\0')
		{
			param_set(&conninfo, "user", upstream_repluser);
		}
		else
		{
			param_set(&conninfo, "user", node_record->repluser);
		}

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
		else if (strlen(upstream_repluser))
		{
			appendPQExpBuffer(&params, " -U %s", upstream_repluser);
		}
		else if (strlen(node_record->repluser))
		{
			appendPQExpBuffer(&params, " -U %s", node_record->repluser);
		}
		else if (strlen(runtime_options.username))
		{
			appendPQExpBuffer(&params, " -U %s", runtime_options.username);
		}

	}

	if (runtime_options.fast_checkpoint)
	{
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
	 * To ensure we have all the WALs needed during basebackup execution we
	 * stream them as the backup is taking place.
	 *
	 * From 9.6, if replication slots are in use, we'll have previously
	 * created a slot with reserved LSN, and will stream from that slot to
	 * avoid WAL buildup on the primary using the -S/--slot, which requires
	 * -X/--xlog-method=stream (from 10, -X/--wal-method=stream)
	 */
	if (!strlen(backup_options.xlog_method))
	{
		appendPQExpBuffer(&params, " -X stream");
	}

	/*
	 * From 9.6, pg_basebackup accepts -S/--slot, which forces WAL streaming
	 * to use the specified replication slot. If replication slot usage is
	 * specified, the slot will already have been created.
	 *
	 * NOTE: currently there's no way of disabling the --slot option while
	 * using --xlog-method=stream - it's hard to imagine a use case for this,
	 * so no provision has been made for doing it.
	 *
	 * NOTE: It's possible to set 'pg_basebackup_options' with an invalid
	 * combination of values for --wal-method (--xlog-method) and --slot -
	 * we're not checking that, just that we're not overriding any
	 * user-supplied values
	 */
	if (source_server_version_num >= 90600 && config_file_options.use_replication_slots)
	{
		bool		slot_add = true;

		/*
		 * Check whether 'pg_basebackup_options' in repmgr.conf has the --slot
		 * option set, or if --wal-method (--xlog-method) is set to a value
		 * other than "stream" (in which case we can't use --slot).
		 */
		if (strlen(backup_options.slot) || (strlen(backup_options.xlog_method) && strcmp(backup_options.xlog_method, "stream") != 0))
		{
			slot_add = false;
		}

		if (slot_add == true)
		{
			appendPQExpBuffer(&params, " -S %s", node_record->slot_name);
		}
	}

	maxlen_snprintf(script,
					"%s -l \"repmgr base backup\" %s %s",
					make_pg_path("pg_basebackup"),
					params.data,
					config_file_options.pg_basebackup_options);

	termPQExpBuffer(&params);

	log_info(_("executing:\n  %s"), script);

	/*
	 * As of 9.4, pg_basebackup only ever returns 0 or 1
	 */

	r = system(script);

	if (r != 0)
		return ERR_BAD_BASEBACKUP;

	/*
	 * If replication slots in use, check the created slot is on the correct
	 * node; the slot will initially get created on the source node, and will
	 * need to be dropped and recreated on the actual upstream node if these
	 * differ.
	 */
	if (config_file_options.use_replication_slots && upstream_node_id != UNKNOWN_NODE_ID)
	{

		PGconn	   *superuser_conn = NULL;
		PGconn	   *privileged_conn = NULL;

		t_node_info upstream_node_record = T_NODE_INFO_INITIALIZER;
		t_replication_slot slot_info = T_REPLICATION_SLOT_INITIALIZER;
		RecordStatus record_status = RECORD_NOT_FOUND;
		bool slot_exists_on_upstream = false;

		record_status = get_node_record(source_conn, upstream_node_id, &upstream_node_record);

		/*
		 * if there's no upstream record, there's no point in trying to create
		 * a replication slot on the designated upstream, as the assumption is
		 * it won't exist at this point.
		 */
		if (record_status != RECORD_FOUND)
		{
			log_warning(_("no record exists for designated upstream node %i"),
						upstream_node_id);
			log_hint(_("you'll need to create the replication slot (\"%s\") manually"),
					 node_record->slot_name);
		}
		else
		{
			PGconn	   *upstream_conn = NULL;

			upstream_conn = establish_db_connection(upstream_node_record.conninfo, true);

			record_status = get_slot_record(upstream_conn, node_record->slot_name, &slot_info);

			if (record_status == RECORD_FOUND)
			{
				log_verbose(LOG_INFO,
							_("replication slot \"%s\" aleady exists on upstream node %i"),
							node_record->slot_name,
							upstream_node_id);
				slot_exists_on_upstream = true;
			}
			else
			{
				PQExpBufferData event_details;

				log_notice(_("creating replication slot \"%s\" on upstream node %i"),
						   node_record->slot_name,
						   upstream_node_id);

				get_superuser_connection(&upstream_conn, &superuser_conn, &privileged_conn);

				initPQExpBuffer(&event_details);
				if (create_replication_slot(privileged_conn, node_record->slot_name, source_server_version_num, &event_details) == false)
				{
					log_error("%s", event_details.data);

					create_event_notification(
											  primary_conn,
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

				if (superuser_conn != NULL)
					PQfinish(superuser_conn);

				termPQExpBuffer(&event_details);
			}

			PQfinish(upstream_conn);
		}

		get_superuser_connection(&source_conn, &superuser_conn, &privileged_conn);

		if (slot_info.active == false)
		{
			if (slot_exists_on_upstream == false)
			{
				if (drop_replication_slot(source_conn, node_record->slot_name) == true)
				{
					log_notice(_("replication slot \"%s\" deleted on source node"), node_record->slot_name);
				}
				else
				{
					log_error(_("unable to delete replication slot \"%s\" on source node"), node_record->slot_name);
				}
			}
		}

		/*
		 * if replication slot is still active (shouldn't happen), emit a
		 * warning
		 */
		else
		{
			log_warning(_("replication slot \"%s\" is still active on source node"), node_record->slot_name);
		}

		if (superuser_conn != NULL)
			PQfinish(superuser_conn);

	}


	return SUCCESS;
}


static int
run_file_backup(t_node_info *node_record)
{
	int			r = SUCCESS,
				i;

	char		command[MAXLEN] = "";
	char		filename[MAXLEN] = "";
	char		buf[MAXLEN] = "";
	char		basebackups_directory[MAXLEN] = "";
	char		backup_id[MAXLEN] = "";
	char	   *p = NULL,
			   *q = NULL;
	TablespaceDataList tablespace_list = {NULL, NULL};
	TablespaceDataListCell *cell_t = NULL;

	PQExpBufferData tablespace_map;
	bool		tablespace_map_rewrite = false;

	if (mode == barman)
	{
		/*
		 * Locate Barman's base backups directory
		 */

		get_barman_property(basebackups_directory, "basebackups_directory", local_repmgr_tmp_directory);

		/*
		 * Read the list of backup files into a local file. In the process:
		 *
		 * - determine the backup ID; - check, and remove, the prefix; -
		 * detect tablespaces; - filter files in one list per tablespace;
		 */

		{
			FILE	   *fi;		/* input stream */
			FILE	   *fd;		/* output for data.txt */
			char		prefix[MAXLEN] = "";
			char		output[MAXLEN] = "";
			int			n = 0;

			maxlen_snprintf(command, "%s list-files --target=data %s latest",
							make_barman_ssh_command(barman_command_buf),
							config_file_options.barman_server);

			log_verbose(LOG_DEBUG, "executing:\n  %s", command);

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
				exit(ERR_BARMAN);
			}

			maxlen_snprintf(prefix, "%s/", basebackups_directory);

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
				if (!strcmp(backup_id, ""))
				{
					FILE	   *fi2;

					n = strcspn(p, "/");

					strncpy(backup_id, p, n);

					strncat(prefix, backup_id, MAXLEN - 1);
					strncat(prefix, "/", MAXLEN - 1);
					p = string_skip_prefix(backup_id, p);
					p = string_skip_prefix("/", p);

					/*
					 * Copy backup.info
					 */
					maxlen_snprintf(command,
									"rsync -a %s:%s/%s/backup.info %s",
									config_file_options.barman_host,
									basebackups_directory,
									backup_id,
									local_repmgr_tmp_directory);
					(void) local_command(
										 command,
										 NULL);

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
							source_server_version_num = strtol(q, NULL, 10);
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
		if (source_server_version_num >= 90500)
		{
			initPQExpBuffer(&tablespace_map);
		}

		/*
		 * As of Barman version 1.6.1, the file structure of a backup is as
		 * follows:
		 *
		 * base/ - base backup wals/ - WAL files associated to the backup
		 *
		 * base/<ID> - backup files
		 *
		 * here ID has the standard timestamp form yyyymmddThhmmss
		 *
		 * base/<ID>/backup.info - backup metadata, in text format
		 * base/<ID>/data        - data directory base/<ID>/<OID>       -
		 * tablespace with the given oid
		 */

		/*
		 * Copy all backup files from the Barman server
		 */
		maxlen_snprintf(command,
						"rsync --progress -a --files-from=%s %s:%s/%s/data %s",
						datadir_list_filename,
						config_file_options.barman_host,
						basebackups_directory,
						backup_id,
						local_data_directory);

		(void) local_command(
							 command,
							 NULL);

		unlink(datadir_list_filename);

		/*
		 * We must create some PGDATA subdirectories because they are not
		 * included in the Barman backup.
		 *
		 * See class RsyncBackupExecutor in the Barman source
		 * (barman/backup_executor.py) for a definitive list of excluded
		 * directories.
		 */
		{
			const char *const dirs[] = {
				/* Only from 10 */
				"pg_wal",
				/* Only from 9.5 */
				"pg_commit_ts",
				/* Only from 9.4 */
				"pg_dynshmem", "pg_logical", "pg_logical/snapshots", "pg_logical/mappings", "pg_replslot",
				/* Already in 9.3 */
				"pg_notify", "pg_serial", "pg_snapshots", "pg_stat", "pg_stat_tmp",
				"pg_subtrans", "pg_tblspc", "pg_twophase", "pg_xlog", 0
			};
			const int	vers[] = {
				100000,
				90500,
				90400, 90400, 90400, 90400, 90400,
				0, 0, 0, 0, 0,
				0, 0, 0, -100000
			};

			for (i = 0; dirs[i]; i++)
			{
				/* directory exists in newer versions than this server - skip */
				if (vers[i] > 0 && source_server_version_num < vers[i])
					continue;

				/*
				 * directory existed in earlier versions than this server but
				 * has been removed/renamed - skip
				 */
				if (vers[i] < 0 && source_server_version_num >= abs(vers[i]))
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


	for (cell_t = tablespace_list.head; cell_t; cell_t = cell_t->next)
	{
		bool		mapping_found = false;
		TablespaceListCell *cell = NULL;
		char	   *tblspc_dir_dest = NULL;

		/*
		 * Check if tablespace path matches one of the provided tablespace
		 * mappings
		 */
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
			log_debug(_("mapping source tablespace \"%s\" (OID %s) to \"%s\""),
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

			if (cell_t->f != NULL)	/* cell_t->f == NULL iff the tablespace is
									 * empty */
			{
				maxlen_snprintf(command,
								"rsync --progress -a --files-from=%s/%s.txt %s:%s/%s/%s %s",
								local_repmgr_tmp_directory,
								cell_t->oid,
								config_file_options.barman_host,
								basebackups_directory,
								backup_id,
								cell_t->oid,
								tblspc_dir_dest);
				(void) local_command(
									 command,
									 NULL);
				fclose(cell_t->f);
				maxlen_snprintf(filename,
								"%s/%s.txt",
								local_repmgr_tmp_directory,
								cell_t->oid);
				unlink(filename);
			}
		}


		/*
		 * If a valid mapping was provide for this tablespace, arrange for it
		 * to be remapped (if no tablespace mapping was provided, the link
		 * will be copied as-is by pg_basebackup and no action is required)
		 */
		if (mapping_found == true || mode == barman)
		{
			/* 9.5 and later - append to the tablespace_map file */
			if (source_server_version_num >= 90500)
			{
				tablespace_map_rewrite = true;
				appendPQExpBuffer(&tablespace_map,
								  "%s %s\n",
								  cell_t->oid,
								  tblspc_dir_dest);
			}

			/*
			 * Pre-9.5, we have to manipulate the symlinks in pg_tblspc/
			 * ourselves
			 */
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
					log_detail("%s", strerror(errno));
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
	 * For 9.5 and later, if tablespace remapping was requested, we'll need to
	 * rewrite the tablespace map file ourselves. The tablespace map file is
	 * read on startup and any links created by the backend; we could do this
	 * ourselves like for pre-9.5 servers, but it's better to rely on
	 * functionality the backend provides.
	 */

	if (source_server_version_num >= 90500 && tablespace_map_rewrite == true)
	{
		PQExpBufferData tablespace_map_filename;
		FILE	   *tablespace_map_file;

		initPQExpBuffer(&tablespace_map_filename);
		appendPQExpBuffer(&tablespace_map_filename, "%s/%s",
						  local_data_directory,
						  TABLESPACE_MAP);

		/*
		 * Unlink any existing file (it should be there, but we don't care if
		 * it isn't)
		 */
		if (unlink(tablespace_map_filename.data) < 0 && errno != ENOENT)
		{
			log_error(_("unable to remove tablespace_map file \"%s\""),
					  tablespace_map_filename.data);
			log_detail("%s", strerror(errno));

			r = ERR_BAD_BASEBACKUP;
			goto stop_backup;
		}

		tablespace_map_file = fopen(tablespace_map_filename.data, "w");
		if (tablespace_map_file == NULL)
		{
			log_error(_("unable to create tablespace_map file \"%s\""), tablespace_map_filename.data);

			r = ERR_BAD_BASEBACKUP;
			goto stop_backup;
		}

		if (fputs(tablespace_map.data, tablespace_map_file) == EOF)
		{
			log_error(_("unable to write to tablespace_map file \"%s\""), tablespace_map_filename.data);

			r = ERR_BAD_BASEBACKUP;
			goto stop_backup;
		}

		fclose(tablespace_map_file);
	}

stop_backup:

	if (mode == barman)
	{
		/* In Barman mode, remove local_repmgr_directory */
		rmtree(local_repmgr_tmp_directory, true);
	}


	/*
	 * if replication slots in use, create replication slot
	 */
	if (r == SUCCESS)
	{
		if (config_file_options.use_replication_slots == true)
		{
			bool slot_warning = false;
			if (runtime_options.no_upstream_connection == true)
			{
				slot_warning = true;
			}
			else
			{
				t_node_info upstream_node_record = T_NODE_INFO_INITIALIZER;
				t_replication_slot slot_info = T_REPLICATION_SLOT_INITIALIZER;
				RecordStatus record_status = RECORD_NOT_FOUND;
				PGconn	   *upstream_conn = NULL;

				record_status = get_node_record(source_conn, upstream_node_id, &upstream_node_record);

				if (record_status != RECORD_FOUND)
				{
					log_error(_("unable to retrieve node record for upstream node %i"), upstream_node_id);
					slot_warning = true;
				}
				else
				{
					upstream_conn = establish_db_connection(upstream_node_record.conninfo, false);
					if (PQstatus(upstream_conn) != CONNECTION_OK)
					{
						log_error(_("unable to connect to upstream node %i to create a replication slot"), upstream_node_id);
						slot_warning = true;
					}
					else
					{
						record_status = get_slot_record(upstream_conn, node_record->slot_name, &slot_info);

						if (record_status == RECORD_FOUND)
						{
							log_verbose(LOG_INFO,
										_("replication slot \"%s\" aleady exists on upstream node %i"),
										node_record->slot_name,
										upstream_node_id);
						}
						else
						{
							PQExpBufferData errmsg;

							initPQExpBuffer(&errmsg);

							if (create_replication_slot(upstream_conn, node_record->slot_name, source_server_version_num, &errmsg) == false)
							{
								log_error(_("unable to create replication slot on upstream node %i"), upstream_node_id);
								log_detail("%s", errmsg.data);
								slot_warning = true;
							}
							else
							{
								log_notice(_("replication slot \"%s\" created on upstream node \"%s\" (ID: %i)"),
										   node_record->slot_name,
										   upstream_node_record.node_name,
										   upstream_node_id );
							}
							termPQExpBuffer(&errmsg);
						}

						PQfinish(upstream_conn);
					}
				}
			}


			if (slot_warning == true)
			{
				log_warning(_("\"use_replication_slots\" specified but a replication slot could not be created"));
				log_hint(_("ensure a replication slot called \"%s\" is created on the upstream node (ID: %i)"),
						 node_record->slot_name,
						 upstream_node_id);
			}
		}
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
			(char *tablespace_data_barman,
			 TablespaceDataList *tablespace_list)
{
	/*
	 * Example: [('main', 24674, '/var/lib/postgresql/tablespaces/9.5/main'),
	 * ('alt', 24678, '/var/lib/postgresql/tablespaces/9.5/alt')]
	 */

	char		name[MAXLEN] = "";
	char		oid[MAXLEN] = "";
	char		location[MAXPGPATH] = "";
	char	   *p = tablespace_data_barman;
	int			i = 0;

	tablespace_list->head = NULL;
	tablespace_list->tail = NULL;

	p = string_skip_prefix("[", p);
	if (p == NULL)
		return -1;

	while (*p == '(')
	{
		p = string_skip_prefix("('", p);
		if (p == NULL)
			return -1;

		i = strcspn(p, "'");
		strncpy(name, p, i);
		name[i] = 0;

		p = string_skip_prefix("', ", p + i);
		if (p == NULL)
			return -1;

		i = strcspn(p, ",");
		strncpy(oid, p, i);
		oid[i] = 0;

		p = string_skip_prefix(", '", p + i);
		if (p == NULL)
			return -1;

		i = strcspn(p, "'");
		strncpy(location, p, i);
		location[i] = 0;

		p = string_skip_prefix("')", p + i);
		if (p == NULL)
			return -1;

		tablespace_data_append(tablespace_list, name, oid, location);

		if (*p == ']')
			break;

		p = string_skip_prefix(", ", p);
		if (p == NULL)
			return -1;
	}

	return SUCCESS;
}


void
get_barman_property(char *dst, char *name, char *local_repmgr_directory)
{
	PQExpBufferData command_output;
	char		buf[MAXLEN] = "";
	char		command[MAXLEN] = "";
	char	   *p = NULL;

	initPQExpBuffer(&command_output);

	maxlen_snprintf(command,
					"grep \"^\t%s:\" %s/show-server.txt",
					name, local_repmgr_tmp_directory);
	(void) local_command(command, &command_output);

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
copy_configuration_files(bool delete_after_copy)
{
	int			i,
				r;
	t_configfile_info *file = NULL;
	char	   *host = NULL;

	/* get host from upstream record */
	host = param_get(&recovery_conninfo, "host");

	if (host == NULL)
		host = runtime_options.host;

	log_notice(_("copying external configuration files from upstream node \"%s\""), host);

	for (i = 0; i < config_files.entries; i++)
	{
		PQExpBufferData dest_path;

		file = config_files.files[i];

		/*
		 * Skip files in the data directory - these will be copied during the
		 * main backup
		 */
		if (file->in_data_directory == true)
			continue;

		initPQExpBuffer(&dest_path);

		if (runtime_options.copy_external_config_files_destination == CONFIG_FILE_SAMEPATH)
		{
			appendPQExpBufferStr(&dest_path, file->filepath);
		}
		else
		{
			appendPQExpBuffer(&dest_path,
							  "%s/%s",
							  local_data_directory,
							  file->filename);
		}

		r = copy_remote_files(runtime_options.host, runtime_options.remote_user,
							  file->filepath, dest_path.data, false, source_server_version_num);

		/*
		 * TODO: collate errors into list
		 */

		if (WEXITSTATUS(r))
		{
			log_error(_("standby clone: unable to copy config file \"%s\""),
					  file->filename);
			log_hint(_("see preceding messages for details"));

			if (runtime_options.force == false)
				exit(ERR_BAD_RSYNC);
		}

		/*
		 * This is to check we can actually copy the files before running the
		 * main clone operation
		 */
		if (delete_after_copy == true)
		{
			/* this is very unlikely to happen, but log in case it does */
			if (unlink(dest_path.data) < 0 && errno != ENOENT)
			{
				log_warning(_("unable to delete %s"), dest_path.data);
				log_detail("%s", strerror(errno));
			}
		}

		termPQExpBuffer(&dest_path);
	}

	return;
}


static void
tablespace_data_append(TablespaceDataList *list, const char *name, const char *oid, const char *location)
{
	TablespaceDataListCell *cell = NULL;

	cell = (TablespaceDataListCell *) pg_malloc0(sizeof(TablespaceDataListCell));

	if (cell == NULL)
	{
		log_error(_("unable to allocate memory; terminating"));
		exit(ERR_OUT_OF_MEMORY);
	}

	cell->oid = pg_malloc(1 + strlen(oid));
	cell->name = pg_malloc(1 + strlen(name));
	cell->location = pg_malloc(1 + strlen(location));

	strncpy(cell->oid, oid, 1 + strlen(oid));
	strncpy(cell->name, name, 1 + strlen(name));
	strncpy(cell->location, location, 1 + strlen(location));

	if (list->tail)
		list->tail->next = cell;
	else
		list->head = cell;

	list->tail = cell;
}



/*
 * check_primary_standby_version_match()
 *
 * Check server versions of supplied connections are compatible for
 * replication purposes.
 *
 * Exits on error.
 */
static void
check_primary_standby_version_match(PGconn *conn, PGconn *primary_conn)
{
	char		standby_version[MAXVERSIONSTR] = "";
	int			standby_version_num = UNKNOWN_SERVER_VERSION_NUM;

	char		primary_version[MAXVERSIONSTR] = "";
	int			primary_version_num = UNKNOWN_SERVER_VERSION_NUM;

	standby_version_num = check_server_version(conn, "standby", true, standby_version);

	/* Verify that primary is a supported server version */
	primary_version_num = check_server_version(conn, "primary", false, primary_version);
	if (primary_version_num < 0)
	{
		PQfinish(conn);
		PQfinish(primary_conn);
		exit(ERR_BAD_CONFIG);
	}

	/* primary and standby version should match */
	if ((primary_version_num / 100) != (standby_version_num / 100))
	{
		PQfinish(conn);
		PQfinish(primary_conn);
		log_error(_("PostgreSQL versions on primary (%s) and standby (%s) must match"),
				  primary_version, standby_version);
		exit(ERR_BAD_CONFIG);
	}
}


static void
check_recovery_type(PGconn *conn)
{
	RecoveryType recovery_type = get_recovery_type(conn);

	if (recovery_type != RECTYPE_STANDBY)
	{
		if (recovery_type == RECTYPE_PRIMARY)
		{
			log_error(_("this node should be a standby (%s)"),
					  config_file_options.conninfo);
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}
		else
		{
			log_error(_("connection to node (%s) lost"),
					  config_file_options.conninfo);
			PQfinish(conn);
			exit(ERR_DB_CONN);
		}
	}
}


static void
drop_replication_slot_if_exists(PGconn *conn, int node_id, char *slot_name)
{
	t_replication_slot slot_info = T_REPLICATION_SLOT_INITIALIZER;
	RecordStatus record_status = get_slot_record(conn, slot_name, &slot_info);

	log_verbose(LOG_DEBUG, "attempting to delete slot \"%s\" on node %i",
				slot_name, node_id);

	if (record_status != RECORD_FOUND)
	{
		log_info(_("no slot record found for slot \"%s\" on node %i"),
				 slot_name, node_id);
	}
	else
	{
		if (slot_info.active == false)
		{
			if (drop_replication_slot(conn, slot_name) == true)
			{
				log_notice(_("replication slot \"%s\" deleted on node %i"), slot_name, node_id);
			}
			else
			{
				log_error(_("unable to delete replication slot \"%s\" on node %i"), slot_name, node_id);
			}
		}

		/*
		 * if active replication slot exists, call Houston as we have a
		 * problem
		 */
		else
		{
			log_warning(_("replication slot \"%s\" is still active on node %i"), slot_name, node_id);
		}
	}
}


/*
 * Creates a recovery.conf file for a standby
 *
 * A database connection pointer is required for escaping primary_conninfo
 * parameters. When cloning from Barman and --no-upstream-connection ) this
 * might not be available.
 */
bool
create_recovery_file(t_node_info *node_record, t_conninfo_param_list *recovery_conninfo, char *dest, bool as_file)
{
	PQExpBufferData recovery_file_buf;
	char		recovery_file_path[MAXPGPATH] = "";
	FILE	   *recovery_file;
	mode_t		um;

	/* create file in buffer */
	initPQExpBuffer(&recovery_file_buf);

	/* standby_mode = 'on' */
	appendPQExpBuffer(&recovery_file_buf,
					  "standby_mode = 'on'\n");

	/* primary_conninfo = '...' */

	/*
	 * the user specified --upstream-conninfo string - copy that
	 */
	if (strlen(runtime_options.upstream_conninfo))
	{
		char	   *escaped = escape_recovery_conf_value(runtime_options.upstream_conninfo);

		appendPQExpBuffer(&recovery_file_buf,
						  "primary_conninfo = '%s'\n",
						  escaped);

		free(escaped);
	}

	/*
	 * otherwise use the conninfo inferred from the upstream connection and/or
	 * node record
	 */
	else
	{
		write_primary_conninfo(&recovery_file_buf, recovery_conninfo);
	}

	/* recovery_target_timeline = 'latest' */
	appendPQExpBuffer(&recovery_file_buf,
					  "recovery_target_timeline = 'latest'\n");


	/* recovery_min_apply_delay = ... (optional) */
	if (config_file_options.recovery_min_apply_delay_provided == true)
	{
		appendPQExpBuffer(&recovery_file_buf,
						  "recovery_min_apply_delay = %s\n",
						  config_file_options.recovery_min_apply_delay);
	}

	/* primary_slot_name = '...' (optional, for 9.4 and later) */
	if (config_file_options.use_replication_slots)
	{
		appendPQExpBuffer(&recovery_file_buf,
						  "primary_slot_name = %s\n",
						  node_record->slot_name);
	}

	/*
	 * If restore_command is set, we use it as restore_command in
	 * recovery.conf
	 */
	if (config_file_options.restore_command[0] != '\0')
	{
		char	   *escaped = escape_recovery_conf_value(config_file_options.restore_command);

		appendPQExpBuffer(&recovery_file_buf,
						  "restore_command = '%s'\n",
						  escaped);
		free(escaped);
	}

	/* archive_cleanup_command (optional) */
	if (config_file_options.archive_cleanup_command[0] != '\0')
	{
		char	   *escaped = escape_recovery_conf_value(config_file_options.archive_cleanup_command);
		appendPQExpBuffer(&recovery_file_buf,
						  "archive_cleanup_command = '%s'\n",
						  escaped);
		free(escaped);
	}

	if (as_file == true)
	{
		maxpath_snprintf(recovery_file_path, "%s/%s", dest, RECOVERY_COMMAND_FILE);
		log_debug("create_recovery_file(): creating \"%s\"...",
				  recovery_file_path);

		/* Set umask to 0600 */
		um = umask((~(S_IRUSR | S_IWUSR)) & (S_IRWXG | S_IRWXO));
		recovery_file = fopen(recovery_file_path, "w");
		umask(um);

		if (recovery_file == NULL)
		{
			log_error(_("unable to create recovery.conf file at \"%s\""),
					  recovery_file_path);
			log_detail("%s", strerror(errno));

			return false;
		}

		log_debug("recovery file is:\n%s", recovery_file_buf.data);

		if (fputs(recovery_file_buf.data, recovery_file) == EOF)
		{
			log_error(_("unable to write to recovery file at \"%s\""), recovery_file_path);
			fclose(recovery_file);
			termPQExpBuffer(&recovery_file_buf);
			return false;
		}

		fclose(recovery_file);
	}
	else
	{
		maxlen_snprintf(dest, "%s", recovery_file_buf.data);
	}

	termPQExpBuffer(&recovery_file_buf);

	return true;
}


static void
write_primary_conninfo(PQExpBufferData *dest, t_conninfo_param_list *param_list)
{
	PQExpBufferData conninfo_buf;
	bool		application_name_provided = false;
	bool		password_provided = false;
	int			c;
	char	   *escaped = NULL;
	t_conninfo_param_list env_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;

	initialize_conninfo_params(&env_conninfo, true);

	initPQExpBuffer(&conninfo_buf);

	for (c = 0; c < param_list->size && param_list->keywords[c] != NULL; c++)
	{
		/*
		 * Skip empty settings and ones which don't make any sense in
		 * recovery.conf
		 */
		if (strcmp(param_list->keywords[c], "dbname") == 0 ||
			strcmp(param_list->keywords[c], "replication") == 0 ||
			(param_list->values[c] == NULL) ||
			(param_list->values[c] != NULL && param_list->values[c][0] == '\0'))
			continue;

		/* only include "password" if explicitly requested */
		if (strcmp(param_list->keywords[c], "password") == 0)
		{
			password_provided = true;
		}

		if (conninfo_buf.len != 0)
			appendPQExpBufferChar(&conninfo_buf, ' ');

		if (strcmp(param_list->keywords[c], "application_name") == 0)
			application_name_provided = true;

		appendPQExpBuffer(&conninfo_buf, "%s=", param_list->keywords[c]);
		appendConnStrVal(&conninfo_buf, param_list->values[c]);
	}

	/* "application_name" not provided - default to repmgr node name */
	if (application_name_provided == false)
	{
		if (strlen(config_file_options.node_name))
		{
			appendPQExpBuffer(&conninfo_buf, " application_name=");
			appendConnStrVal(&conninfo_buf, config_file_options.node_name);
		}
		else
		{
			appendPQExpBuffer(&conninfo_buf, " application_name=repmgr");
		}
	}

	/* no password provided explicitly  */
	if (password_provided == false)
	{
		if (config_file_options.use_primary_conninfo_password == true)
		{
			const char *password = param_get(&env_conninfo, "password");

			if (password != NULL)
			{
				appendPQExpBuffer(&conninfo_buf, " password=");
				appendConnStrVal(&conninfo_buf, password);
			}
		}
	}

	/* passfile provided as configuration option */
	if (config_file_options.passfile[0] != '\0')
	{
		/* check if the libpq we're using supports "passfile=" */
		if (has_passfile() == true)
		{
			appendPQExpBuffer(&conninfo_buf, " passfile=");
			appendConnStrVal(&conninfo_buf, config_file_options.passfile);
		}
	}

	escaped = escape_recovery_conf_value(conninfo_buf.data);

	appendPQExpBuffer(dest,
					  "primary_conninfo = '%s'\n", escaped);

	free(escaped);
	free_conninfo_params(&env_conninfo);
	termPQExpBuffer(&conninfo_buf);
}


static NodeStatus
parse_node_status_is_shutdown_cleanly(const char *node_status_output, XLogRecPtr *checkPoint)
{
	NodeStatus	node_status = NODE_STATUS_UNKNOWN;

	int			c = 0,
				argc_item = 0;
	char	  **argv_array = NULL;
	int			optindex = 0;

	/* We're only interested in these options */
	struct option node_status_options[] =
	{
		{"last-checkpoint-lsn", required_argument, NULL, 'L'},
		{"state", required_argument, NULL, 'S'},
		{NULL, 0, NULL, 0}
	};

	/* Don't attempt to tokenise an empty string */
	if (!strlen(node_status_output))
	{
		*checkPoint = InvalidXLogRecPtr;
		return node_status;
	}

	argc_item = parse_output_to_argv(node_status_output, &argv_array);

	/* Reset getopt's optind variable */
	optind = 0;

	/* Prevent getopt from emitting errors */
	opterr = 0;

	while ((c = getopt_long(argc_item, argv_array, "L:S:", node_status_options,
							&optindex)) != -1)
	{
		switch (c)
		{
				/* --last-checkpoint-lsn */
			case 'L':
				*checkPoint = parse_lsn(optarg);
				break;
				/* --state */
			case 'S':
				{
					if (strncmp(optarg, "RUNNING", MAXLEN) == 0)
					{
						node_status = NODE_STATUS_UP;
					}
					else if (strncmp(optarg, "SHUTDOWN", MAXLEN) == 0)
					{
						node_status = NODE_STATUS_DOWN;
					}
					else if (strncmp(optarg, "UNCLEAN_SHUTDOWN", MAXLEN) == 0)
					{
						node_status = NODE_STATUS_UNCLEAN_SHUTDOWN;
					}
					else if (strncmp(optarg, "UNKNOWN", MAXLEN) == 0)
					{
						node_status = NODE_STATUS_UNKNOWN;
					}
				}
				break;
		}
	}

	free_parsed_argv(&argv_array);

	return node_status;
}


static ConnectionStatus
parse_remote_node_replication_connection(const char *node_check_output)
{
	ConnectionStatus	conn_status = CONN_UNKNOWN;

	int			c = 0,
				argc_item = 0;
	char	  **argv_array = NULL;
	int			optindex = 0;

	/* We're only interested in these options */
	struct option node_check_options[] =
	{
		{"connection", required_argument, NULL, 'c'},
		{NULL, 0, NULL, 0}
	};

	/* Don't attempt to tokenise an empty string */
	if (!strlen(node_check_output))
	{
		return CONN_UNKNOWN;
	}

	argc_item = parse_output_to_argv(node_check_output, &argv_array);

	/* Reset getopt's optind variable */
	optind = 0;

	/* Prevent getopt from emitting errors */
	opterr = 0;

	while ((c = getopt_long(argc_item, argv_array, "L:S:", node_check_options,
							&optindex)) != -1)
	{
		switch (c)
		{

			/* --connection */
			case 'c':
				{
					if (strncmp(optarg, "OK", MAXLEN) == 0)
					{
						conn_status = CONN_OK;
					}
					else if (strncmp(optarg, "BAD", MAXLEN) == 0)
					{
						conn_status = CONN_BAD;
					}
					else if (strncmp(optarg, "UNKNOWN", MAXLEN) == 0)
					{
						conn_status = CONN_UNKNOWN;
					}
				}
				break;
		}
	}

	free_parsed_argv(&argv_array);

	return conn_status;
}


static CheckStatus
parse_node_check_archiver(const char *node_check_output, int *files, int *threshold)
{
	CheckStatus status = CHECK_STATUS_UNKNOWN;

	int			c = 0,
				argc_item = 0;
	char	  **argv_array = NULL;
	int			optindex = 0;

	/* We're only interested in these options */
	struct option node_check_options[] =
	{
		{"status", required_argument, NULL, 'S'},
		{"files", required_argument, NULL, 'f'},
		{"threshold", required_argument, NULL, 't'},
		{NULL, 0, NULL, 0}
	};

	*files = 0;
	*threshold = 0;

	/* Don't attempt to tokenise an empty string */
	if (!strlen(node_check_output))
	{
		return status;
	}

	argc_item = parse_output_to_argv(node_check_output, &argv_array);


	/* Reset getopt's optind variable */
	optind = 0;

	/* Prevent getopt from emitting errors */
	opterr = 0;

	while ((c = getopt_long(argc_item, argv_array, "f:S:t:", node_check_options,
							&optindex)) != -1)
	{
		switch (c)
		{
				/* --files */
			case 'f':
				*files = atoi(optarg);
				break;

			case 't':
				*threshold = atoi(optarg);
				break;

				/* --status */
			case 'S':
				{
					if (strncmp(optarg, "OK", MAXLEN) == 0)
					{
						status = CHECK_STATUS_OK;
					}
					else if (strncmp(optarg, "WARNING", MAXLEN) == 0)
					{
						status = CHECK_STATUS_WARNING;
					}
					else if (strncmp(optarg, "CRITICAL", MAXLEN) == 0)
					{
						status = CHECK_STATUS_CRITICAL;
					}
					else if (strncmp(optarg, "UNKNOWN", MAXLEN) == 0)
					{
						status = CHECK_STATUS_UNKNOWN;
					}
					else
					{
						status = CHECK_STATUS_UNKNOWN;
					}
				}
				break;
		}
	}

	free_parsed_argv(&argv_array);

	return status;
}




void
do_standby_help(void)
{
	print_help_header();

	printf(_("Usage:\n"));
	printf(_("    %s [OPTIONS] standby clone\n"), progname());
	printf(_("    %s [OPTIONS] standby register\n"), progname());
	printf(_("    %s [OPTIONS] standby unregister\n"), progname());
	printf(_("    %s [OPTIONS] standby promote\n"), progname());
	printf(_("    %s [OPTIONS] standby follow\n"), progname());
	printf(_("    %s [OPTIONS] standby switchover\n"), progname());

	puts("");

	printf(_("STANDBY CLONE\n"));
	puts("");
	printf(_("  \"standby clone\" clones a standby from the primary or an upstream node.\n"));
	puts("");
	printf(_("  -d, --dbname=conninfo               conninfo of the upstream node to use for cloning.\n"));
	printf(_("  -c, --fast-checkpoint               force fast checkpoint\n"));
	printf(_("  --copy-external-config-files[={samepath|pgdata}]\n" \
			 "                                      copy configuration files located outside the \n" \
			 "                                        data directory to the same path on the standby (default) or to the\n" \
			 "                                        PostgreSQL data directory\n"));
	printf(_("  --dry-run                           perform checks but don't actually clone the standby\n"));
	printf(_("  --no-upstream-connection            when using Barman, do not connect to upstream node\n"));
	printf(_("  -R, --remote-user=USERNAME          database server username for SSH operations (default: \"%s\")\n"), runtime_options.username);
	printf(_("  --replication-user                  user to make replication connections with (optional, not usually required)\n"));
	printf(_("  --upstream-conninfo                 \"primary_conninfo\" value to write in recovery.conf\n" \
			 "                                        when the intended upstream server does not yet exist\n"));
	printf(_("  --upstream-node-id                  ID of the upstream node to replicate from (optional, defaults to primary node)\n"));
	printf(_("  --without-barman                    do not use Barman even if configured\n"));
	printf(_("  --recovery-conf-only                create \"recovery.conf\" file for a previously cloned instance\n"));

	puts("");

	printf(_("STANDBY REGISTER\n"));
	puts("");
	printf(_("  \"standby register\" registers the standby node.\n"));
	puts("");
	printf(_("  -F, --force                         overwrite an existing node record, or if primary connection\n" \
			 "                                        parameters supplied, create record even if standby offline\n"));
	printf(_("  --upstream-node-id                  ID of the upstream node to replicate from (optional)\n"));
	printf(_("  --wait-start=VALUE                  wait for the standby to start (timeout in seconds, default %i)\n"), DEFAULT_WAIT_START);

	printf(_("  --wait-sync[=VALUE]                 wait for the node record to synchronise to the standby\n" \
			 "                                        (optional timeout in seconds)\n"));

	puts("");

	printf(_("STANDBY UNREGISTER\n"));
	puts("");
	printf(_("  \"standby unregister\" unregisters an inactive standby node.\n"));
	puts("");
	printf(_("  --node-id                           ID of node to unregister (optional, used when the node to\n" \
			 "                                         unregister is offline)\n"));
	puts("");

	printf(_("STANDBY PROMOTE\n"));
	puts("");
	printf(_("  \"standby promote\" promotes a standby node to primary.\n"));
	puts("");

	printf(_("STANDBY FOLLOW\n"));
	puts("");
	printf(_("  \"standby follow\" instructs a standby node to follow a new primary.\n"));
	puts("");
	printf(_("  --dry-run                           perform checks but don't actually follow the new primary\n"));
	printf(_("  -W, --wait                          wait for a primary to appear\n"));
	puts("");


	printf(_("STANDBY SWITCHOVER\n"));
	puts("");
	printf(_("  \"standby switchover\" promotes a standby node to primary, and demotes the previous primary to a standby.\n"));
	puts("");
	printf(_("  --always-promote                    promote standby even if behind original primary\n"));
	printf(_("  --dry-run                           perform checks etc. but don't actually execute switchover\n"));
	printf(_("  -F, --force                         ignore warnings and continue anyway\n"));
	printf(_("  --force-rewind[=VALUE]              use \"pg_rewind\" to reintegrate the old primary if necessary\n"));
	printf(_("                                        (9.3 and 9.4 - provide \"pg_rewind\" path)\n"));

	printf(_("  -R, --remote-user=USERNAME          database server username for SSH operations (default: \"%s\")\n"), runtime_options.username);
	printf(_("  --siblings-follow                   have other standbys follow new primary\n"));

	puts("");
}
