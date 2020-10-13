/*
 * repmgr-action-standby.c
 *
 * Implements standby actions for the repmgr command line utility
 *
 * Copyright (c) 2ndQuadrant, 2010-2020
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
	/* Optional pointer to a file containing a list of tablespace files to copy from Barman */
	FILE	   *fptr;
} TablespaceDataListCell;

typedef struct TablespaceDataList
{
	TablespaceDataListCell *head;
	TablespaceDataListCell *tail;
} TablespaceDataList;


typedef struct
{
	int			reachable_sibling_node_count;
	int			reachable_sibling_nodes_with_slot_count;
	int			unreachable_sibling_node_count;
	int			min_required_wal_senders;
	int			min_required_free_slots;
} SiblingNodeStats;

#define T_SIBLING_NODES_STATS_INITIALIZER { \
	0, \
	0, \
	0, \
	0, \
	0 \
}

static PGconn *primary_conn = NULL;
static PGconn *source_conn = NULL;

static char local_data_directory[MAXPGPATH] = "";

static bool upstream_conninfo_found = false;
static int	upstream_node_id = UNKNOWN_NODE_ID;

static t_conninfo_param_list recovery_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;
static char recovery_conninfo_str[MAXLEN] = "";
static char upstream_repluser[NAMEDATALEN] = "";
static char upstream_user[NAMEDATALEN] = "";

static int	source_server_version_num = UNKNOWN_SERVER_VERSION_NUM;

static t_configfile_list config_files = T_CONFIGFILE_LIST_INITIALIZER;

static standy_clone_mode mode = pg_basebackup;

/* used by barman mode */
static char local_repmgr_tmp_directory[MAXPGPATH] = "";
static char datadir_list_filename[MAXLEN] = "";
static char barman_command_buf[MAXLEN] = "";

/*
 * To enable "standby clone" to run with lowest possible user
 * privileges, we'll need to determine which actions need to
 * be run and which of the available users, which will be one
 * of the repmgr user, the replication user (if available) or
 * the superuser (if available).
 */
static t_user_type SettingsUser = REPMGR_USER;

static void _do_standby_promote_internal(PGconn *conn);
static void _do_create_replication_conf(void);

static void check_barman_config(void);
static void check_source_server(void);
static void check_source_server_via_barman(void);
static bool check_upstream_config(PGconn *conn, int server_version_num, t_node_info *node_info, bool exit_on_error);
static void check_primary_standby_version_match(PGconn *conn, PGconn *primary_conn);
static void check_recovery_type(PGconn *conn);

static void initialise_direct_clone(t_node_info *local_node_record, t_node_info *upstream_node_record);
static int	run_basebackup(t_node_info *node_record);
static int	run_file_backup(t_node_info *node_record);

static void copy_configuration_files(bool delete_after_copy);

static void tablespace_data_append(TablespaceDataList *list, const char *name, const char *oid, const char *location);

static void get_barman_property(char *dst, char *name, char *local_repmgr_directory);
static int	get_tablespace_data_barman(char *, TablespaceDataList *);
static char *make_barman_ssh_command(char *buf);

static bool create_recovery_file(t_node_info *node_record, t_conninfo_param_list *primary_conninfo, int server_version_num, char *dest, bool as_file);
static void write_primary_conninfo(PQExpBufferData *dest, t_conninfo_param_list *param_list);

static bool check_sibling_nodes(NodeInfoList *sibling_nodes, SiblingNodeStats *sibling_nodes_stats);
static bool check_free_wal_senders(int available_wal_senders, SiblingNodeStats *sibling_nodes_stats, bool *dry_run_success);
static bool check_free_slots(t_node_info *local_node_record, SiblingNodeStats *sibling_nodes_stats, bool *dry_run_success);

static void sibling_nodes_follow(t_node_info *local_node_record, NodeInfoList *sibling_nodes, SiblingNodeStats *sibling_nodes_stats);

static t_remote_error_type parse_remote_error(const char *error);
static CheckStatus parse_check_status(const char *status_str);

static NodeStatus parse_node_status_is_shutdown_cleanly(const char *node_status_output, XLogRecPtr *checkPoint);
static CheckStatus parse_node_check_archiver(const char *node_check_output, int *files, int *threshold, t_remote_error_type *remote_error);
static ConnectionStatus parse_remote_node_replication_connection(const char *node_check_output);
static bool parse_data_directory_config(const char *node_check_output, t_remote_error_type *remote_error);
static bool parse_replication_config_owner(const char *node_check_output);
static CheckStatus parse_db_connection(const char *db_connection);

/*
 * STANDBY CLONE
 *
 * Event(s):
 *  - standby_clone
 *
 * Parameters:
 *  --upstream-conninfo
 *  --upstream-node-id
 *  --no-upstream-connection
 *  -F/--force
 *  --dry-run
 *  -c/--fast-checkpoint
 *  --copy-external-config-files
 *  -R/--remote-user
 *  --replication-user (only required if no upstream record)
 *  --without-barman
 *  --replication-conf-only (--recovery-conf-only)
 *  --verify-backup (PostgreSQL 13 and later)
 */

void
do_standby_clone(void)
{
	PQExpBufferData event_details;
	int			r = 0;

	/* dummy node record */
	t_node_info local_node_record = T_NODE_INFO_INITIALIZER;
	t_node_info upstream_node_record = T_NODE_INFO_INITIALIZER;

	bool local_data_directory_provided = false;

	initialize_conninfo_params(&recovery_conninfo, false);

	/*
	 * --replication-conf-only provided - we'll handle that separately
	 */
	if (runtime_options.replication_conf_only == true)
	{
		return _do_create_replication_conf();
	}

	/*
	 * conninfo params for the actual upstream node (which might be different
	 * to the node we're cloning from) to write to recovery.conf
	 */

	mode = get_standby_clone_mode();

	/*
	 * Copy the provided data directory; if a configuration file was provided,
	 * use the (mandatory) value from that; if -D/--pgdata was provided, use
	 * that.
	 *
	 * Note that barman mode requires -D/--pgdata.
	 */

	get_node_data_directory(local_data_directory);
	if (local_data_directory[0] != '\0')
	{
		local_data_directory_provided = true;
		log_notice(_("destination directory \"%s\" provided"),
				   local_data_directory);
	}
	else
	{
		/*
		 * If a configuration file is provided, repmgr will error out after
		 * parsing it if no data directory is provided; this check is for
		 * niche use-cases where no configuration file is provided.
		 */
		log_error(_("no data directory provided"));
		log_hint(_("use -D/--pgdata to explicitly specify a data directory"));
		exit(ERR_BAD_CONFIG);
	}


	if (mode == barman)
	{
		/*
		 * Not currently possible to use --verify-backup with Barman
		 */
		if (runtime_options.verify_backup == true)
		{
			log_error(_("--verify-backup option cannot be used when cloning from Barman backups"));
			exit(ERR_BAD_CONFIG);
		}

		/*
		 * Sanity-check barman connection and installation;
		 * this will exit with ERR_BARMAN if problems found.
		 */
		check_barman_config();
	}

	init_node_record(&local_node_record);
	local_node_record.type = STANDBY;

	/*
	 * Initialise list of conninfo parameters which will later be used to
	 * create the "primary_conninfo" recovery parameter.
	 *
	 * We'll initialise it with the host settings specified on the command
	 * line. As it's possible the standby will be cloned from a node different
	 * to its intended upstream, we'll later attempt to fetch the upstream
	 * node record and overwrite the values set here with those from the
	 * upstream node record (excluding that record's application_name)
	 */

	copy_conninfo_params(&recovery_conninfo, &source_conninfo);


	/* Set the default application name to this node's name */
	if (config_file_options.node_id != UNKNOWN_NODE_ID)
	{
		char		application_name[MAXLEN] = "";

		param_set(&recovery_conninfo, "application_name", config_file_options.node_name);

		get_conninfo_value(config_file_options.conninfo, "application_name", application_name);
		if (strlen(application_name) && strncmp(application_name, config_file_options.node_name, sizeof(config_file_options.node_name)) != 0)
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
	 * fall back to connecting to the source node via Barman (if available).
	 */
	if (runtime_options.no_upstream_connection == false)
	{
		RecordStatus record_status = RECORD_NOT_FOUND;

		/*
		 * This connects to the source node and performs sanity checks, also
		 * sets "recovery_conninfo_str", "upstream_repluser", "upstream_user" and
		 * "upstream_node_id" and creates a connection handle in "source_conn".
		 *
		 * Will error out if source connection not possible and not in
		 * "barman" mode.
		 */
		check_source_server();

		if (runtime_options.verify_backup == true)
		{
			/*
			 * --verify-backup available for PostgreSQL 13 and later
			 */
			if (PQserverVersion(source_conn) < 130000)
			{
				log_error(_("--verify-backup available for PostgreSQL 13 and later"));
				exit(ERR_BAD_CONFIG);
			}
		}

		/* attempt to retrieve upstream node record */
		record_status = get_node_record(source_conn,
										upstream_node_id,
										&upstream_node_record);

		if (record_status != RECORD_FOUND)
		{
			log_error(_("unable to retrieve record for upstream node %i"),
					  upstream_node_id);
			exit(ERR_BAD_CONFIG);
		}

	}
	else
	{
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
			log_error(_("unable to parse conninfo string \"%s\" for upstream node"),
					  recovery_conninfo_str);
			log_detail("%s", errmsg);
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

		if (SettingsUser == REPMGR_USER)
		{
			privileged_conn = source_conn;
		}
		else
		{
			get_superuser_connection(&source_conn, &superuser_conn, &privileged_conn);
		}

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
			 * We don't do this during a --dry-run as it may introduce unexpected changes
			 * on the local node; during an actual clone operation, any problems with
			 * copying files will be detected early and the operation aborted before
			 * the actual database cloning commences.
			 *
			 * TODO: put the files in a temporary directory and move to their final
			 * destination once the database has been cloned.
			 */

			if (runtime_options.dry_run == false)
			{
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
		}


		if (superuser_conn != NULL)
			PQfinish(superuser_conn);
	}


	if (runtime_options.dry_run == true)
	{
		/*
		 * If replication slots in use, sanity-check whether we can create them
		 * with the available user permissions.
		 */
		if (config_file_options.use_replication_slots == true && PQstatus(source_conn) == CONNECTION_OK)
		{
			PQExpBufferData msg;
			bool success = true;

			initPQExpBuffer(&msg);

			/*
			 * "create_replication_slot()" knows about --dry-run mode and
			 * will perform checks but not actually create the slot.
			 */
			success = create_replication_slot(source_conn,
											  local_node_record.slot_name,
											  &upstream_node_record,
											  &msg);
			if (success == false)
			{
				log_error(_("prerequisites not met for creating a replication slot on upstream node %i"),
						  upstream_node_record.node_id);
				termPQExpBuffer(&msg);
				exit(ERR_BAD_CONFIG);
			}
			termPQExpBuffer(&msg);
		}

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

		PQfinish(source_conn);

		log_info(_("all prerequisites for \"standby clone\" are met"));

		exit(SUCCESS);
	}

	if (mode != barman)
	{
		initialise_direct_clone(&local_node_record, &upstream_node_record);
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
			/*
			 * In the case where a standby is being cloned from a node other than its
			 * intended upstream, We can't be sure of the source node's node_id. This
			 * is only required by "drop_replication_slot_if_exists()" to determine
			 * from the node's record whether it has a different replication user, and
			 * as in this case that would need to be supplied via "--replication-user"
			 * it's not a problem.
			 */
			drop_replication_slot_if_exists(source_conn, UNKNOWN_NODE_ID, local_node_record.slot_name);
		}

		log_error(_("unable to take a base backup of the primary server"));
		log_hint(_("data directory (\"%s\") may need to be cleaned up manually"),
				 local_data_directory);

		PQfinish(source_conn);
		exit(r);
	}

	/*
	 * Run pg_verifybackup here if requested, before any alterations are made
	 * to the data directory.
	 */
	if (mode == pg_basebackup && runtime_options.verify_backup == true)
	{
		PQExpBufferData command;
		int r;
		struct stat st;

		initPQExpBuffer(&command);

		make_pg_path(&command, "pg_verifybackup");

		/* check command actually exists */
		if (stat(command.data, &st) != 0)
		{
			log_error(_("unable to find expected binary \"%s\""), command.data);
			log_detail("%s", strerror(errno));
			exit(ERR_BAD_CONFIG);
		}

		appendPQExpBufferStr(&command, " ");

		/* Somewhat inconsistent, but pg_verifybackup doesn't accept a -D option  */
		appendShellString(&command,
						  local_data_directory);

		log_debug("executing:\n  %s", command.data);

		r = system(command.data);
		termPQExpBuffer(&command);

		if (r != 0)
		{
			log_error(_("unable to verify backup"));
			exit(ERR_BAD_BASEBACKUP);
		}

		log_verbose(LOG_INFO, _("backup successfully verified"));

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

	if (create_recovery_file(&local_node_record,
							 &recovery_conninfo,
							 source_server_version_num,
							 local_data_directory,
							 true) == false)
	{
		/* create_recovery_file() will log an error */
		if (source_server_version_num >= 120000)
		{
			log_notice(_("unable to write replication configuration; see preceding error messages"));
		}
		else
		{
			log_notice(_("unable to create recovery.conf; see preceding error messages"));
		}
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

	appendPQExpBufferStr(&event_details,
						 _("; backup method: "));

	switch (mode)
	{
		case pg_basebackup:
			appendPQExpBufferStr(&event_details, "pg_basebackup");
			break;
		case barman:
			appendPQExpBufferStr(&event_details, "barman");
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
	PQExpBufferData command;
	bool		command_ok = false;

	/*
	 * Check that there is at least one valid backup
	 */

	log_info(_("connecting to Barman server to verify backup for \"%s\""), config_file_options.barman_server);

	initPQExpBuffer(&command);

	appendPQExpBuffer(&command, "%s show-backup %s latest > /dev/null",
					  make_barman_ssh_command(barman_command_buf),
					  config_file_options.barman_server);

	command_ok = local_command(command.data, NULL);

	if (command_ok == false)
	{
		log_error(_("no valid backup for server \"%s\" was found in the Barman catalogue"),
				  config_file_options.barman_server);
		log_detail(_("command executed was:\n  %s"), command.data),
		log_hint(_("refer to the Barman documentation for more information"));

		termPQExpBuffer(&command);
		exit(ERR_BARMAN);
	}
	else if (runtime_options.dry_run == true)
	{
		log_info(_("valid backup for server \"%s\" found in the Barman catalogue"),
				 config_file_options.barman_server);
	}

	termPQExpBuffer(&command);

	/*
	 * Attempt to create data directory (unless --dry-run specified,
	 * in which case do nothing; warnings will be emitted elsewhere about
	 * any issues with the data directory)
	 */
	if (runtime_options.dry_run == false)
	{
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
	}

	/*
	 * Fetch server parameters from Barman
	 */
	log_info(_("connecting to Barman server to fetch server parameters"));

	initPQExpBuffer(&command);

	if (runtime_options.dry_run == true)
	{
		appendPQExpBuffer(&command, "%s show-server %s > /dev/null",
						  make_barman_ssh_command(barman_command_buf),
						  config_file_options.barman_server);
	}
	else
	{
		appendPQExpBuffer(&command, "%s show-server %s > %s/show-server.txt",
						  make_barman_ssh_command(barman_command_buf),
						  config_file_options.barman_server,
						  local_repmgr_tmp_directory);
	}

	command_ok = local_command(command.data, NULL);

	if (command_ok == false)
	{
		log_error(_("unable to fetch server parameters from Barman server"));
		log_detail(_("command executed was:\n  %s"), command.data),
		termPQExpBuffer(&command);
		exit(ERR_BARMAN);
	}
	else if (runtime_options.dry_run == true)
	{
		log_info(_("server parameters were successfully fetched from Barman server"));
	}

	termPQExpBuffer(&command);
}


/*
 * _do_create_replication_conf()
 *
 * Create replication configuration for a previously cloned instance.
 *
 * Prerequisites:
 *
 * - data directory must be provided, either explicitly or via
 *   repmgr.conf
 * - the instance should not be running
 * - an existing "recovery.conf" file can only be overwritten with
 *   -F/--force (Pg11 and earlier)
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
_do_create_replication_conf(void)
{
	t_node_info local_node_record = T_NODE_INFO_INITIALIZER;
	t_node_info upstream_node_record = T_NODE_INFO_INITIALIZER;

	RecordStatus record_status = RECORD_NOT_FOUND;
	char		recovery_file_path[MAXPGPATH + sizeof(RECOVERY_COMMAND_FILE)] = "";
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


	/* check connection */
	source_conn = establish_db_connection_by_params(&source_conninfo, true);

	/* Verify that source is a supported server version */
	(void) check_server_version(source_conn, "source node", true, NULL);

	/*
	 * Do some sanity checks on the data directory to make sure
	 * it contains a valid but dormant instance
	 */
	switch (check_dir(local_data_directory))
	{
		case DIR_ERROR:
			log_error(_("unable to access specified data directory \"%s\""), local_data_directory);
			log_detail("%s", strerror(errno));
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
			break;
		case DIR_NOENT:
			log_error(_("specified data directory \"%s\" does not exist"), local_data_directory);
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
			break;
		case DIR_EMPTY:
			log_error(_("specified data directory \"%s\" is empty"), local_data_directory);
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
			break;
		case DIR_NOT_EMPTY:
			/* Present but not empty */
			if (!is_pg_dir(local_data_directory))
			{
				log_error(_("specified data directory \"%s\" does not contain a PostgreSQL instance"), local_data_directory);
				PQfinish(source_conn);
				exit(ERR_BAD_CONFIG);
			}

			if (is_pg_running(local_data_directory))
			{
				if (runtime_options.force == false)
				{
					log_error(_("specified data directory \"%s\" appears to contain a running PostgreSQL instance"),
							  local_data_directory);

					if (PQserverVersion(source_conn) >= 120000)
					{
						log_hint(_("use -F/--force to create replication configuration anyway"));
					}
					else
					{
						log_hint(_("use -F/--force to create \"recovery.conf\" anyway"));
					}

					exit(ERR_BAD_CONFIG);
				}

				node_is_running = true;

				if (runtime_options.dry_run == true)
				{
					if (PQserverVersion(source_conn) >= 120000)
					{
						log_warning(_("replication configuration would be created in an active data directory"));
					}
					else
					{
						log_warning(_("\"recovery.conf\" would be created in an active data directory"));
					}
				}
				else
				{
					if (PQserverVersion(source_conn) >= 120000)
					{
						log_warning(_("creating replication configuration in an active data directory"));
					}
					else
					{
						log_warning(_("creating \"recovery.conf\" in an active data directory"));
					}
				}
			}
			break;
		default:
			break;
	}


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
			log_hint(_("standby must be registered before replication can be configured"));
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
			get_node_replication_stats(upstream_conn, &upstream_node_record);

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

	/* check if recovery.conf exists (Pg11 and earlier only) */
	if (PQserverVersion(upstream_conn) < 120000)
	{
		snprintf(recovery_file_path, sizeof(recovery_file_path),
				 "%s/%s",
				 local_data_directory,
				 RECOVERY_COMMAND_FILE);

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
	}

	if (runtime_options.dry_run == true)
	{
		char		recovery_conf_contents[MAXLEN] = "";
		create_recovery_file(&local_node_record,
							 &recovery_conninfo,
							 PQserverVersion(upstream_conn),
							 recovery_conf_contents,
							 false);

		if (PQserverVersion(upstream_conn) >= 120000)
		{
			log_info(_("following items would be added to \"postgresql.auto.conf\" in \"%s\""), local_data_directory);
		}
		else
		{
			log_info(_("would create \"recovery.conf\" file in \"%s\""), local_data_directory);
		}

		log_detail(_("\n%s"), recovery_conf_contents);
	}
	else
	{
		if (!create_recovery_file(&local_node_record,
								  &recovery_conninfo,
								  PQserverVersion(upstream_conn),
								  local_data_directory,
								  true))
		{
			if (PQserverVersion(upstream_conn) >= 120000)
			{
				log_error(_("unable to write replication configuration to \"postgresql.auto.conf\""));
			}
			else
			{
				log_error(_("unable to create \"recovery.conf\""));
			}
		}
		else
		{
			if (PQserverVersion(upstream_conn) >= 120000)
			{
				log_notice(_("replication configuration written to \"postgresql.auto.conf\""));
			}
			else
			{
				log_notice(_("\"recovery.conf\" created as \"%s\""), recovery_file_path);
			}

			if (node_is_running == true)
			{
				log_hint(_("node must be restarted for the new file to take effect"));
			}
		}
	}

	/* Pg12 and later: add standby.signal, if not already there */
	if (PQserverVersion(upstream_conn) >= 120000)
	{
		if (runtime_options.dry_run == true)
		{
			log_info(_("would write \"standby.signal\" file"));

		}
		else
		{
			if (write_standby_signal() == false)
			{
				log_error(_("unable to write \"standby.signal\" file"));
			}
		}
	}

	/* add replication slot, if required */
	if (slot_creation_required == true)
	{
		PQExpBufferData msg;
		initPQExpBuffer(&msg);

		if (runtime_options.dry_run == true)
		{
			/*
			 * In --dry-run mode this will check availability
			 * of a user who can create replication slots.
			 */
			// XXX check return value
			create_replication_slot(upstream_conn,
									local_node_record.slot_name,
									NULL,
									&msg);
			log_info(_("would create replication slot \"%s\" on upstream node \"%s\" (ID: %i)"),
					 local_node_record.slot_name,
					 upstream_node_record.node_name,
					 upstream_node_id);
		}
		else
		{

			if (create_replication_slot(upstream_conn,
										local_node_record.slot_name,
										NULL,
										&msg) == false)
			{
				log_error("%s", msg.data);
				PQfinish(upstream_conn);
				termPQExpBuffer(&msg);
				exit(ERR_BAD_CONFIG);
			}


			log_notice(_("replication slot \"%s\" created on upstream node \"%s\" (ID: %i)"),
					   local_node_record.slot_name,
					   upstream_node_record.node_name,
					   upstream_node_id);
		}
		termPQExpBuffer(&msg);

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
			log_detail("\n%s", PQerrorMessage(conn));
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
		if (runtime_options.connection_param_provided)
		{
			log_warning(_("database connection parameters not required when the standby to be registered is running"));
			log_detail(_("repmgr uses the \"conninfo\" parameter in \"repmgr.conf\" to connect to the standby"));
		}
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

		if (runtime_options.upstream_node_id == config_file_options.node_id)
		{
			log_error(_("provided node ID for --upstream-node-id (%i) is the same as the configured local node ID (%i)"),
					  runtime_options.upstream_node_id,
					  config_file_options.node_id);
			PQfinish(primary_conn);
			if (PQstatus(conn) == CONNECTION_OK)
				PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

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
				if (is_downstream_node_attached(upstream_conn, config_file_options.node_name, NULL) == NODE_ATTACHED)
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
								upstream_node_record.node_name,
								upstream_node_record.node_id);
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
			if (is_downstream_node_attached(primary_conn, config_file_options.node_name, NULL) == NODE_ATTACHED)
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
						  _("standby registration failed; provided upstream node ID was %i"),
						  node_record.upstream_node_id);

		if (runtime_options.force == true)
			appendPQExpBufferStr(&details,
								 _(" (-F/--force option was used)"));

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
					  _("standby registration succeeded; upstream node ID is %i"),
					  node_record.upstream_node_id);

	if (runtime_options.force == true)
		appendPQExpBufferStr(&details,
							 _(" (-F/--force option was used)"));


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

				if (strcmp(node_record_on_standby.location, node_record_on_primary.location) != 0)
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
	log_notice(_("standby node \"%s\" (ID: %i) successfully registered"),
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
		log_detail("\n%s", PQerrorMessage(conn));
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
	PGconn	   *local_conn = NULL;
	PGconn	   *current_primary_conn = NULL;

	RecoveryType recovery_type = RECTYPE_UNKNOWN;

	int			existing_primary_id = UNKNOWN_NODE_ID;

	RecordStatus record_status = RECORD_NOT_FOUND;
	t_node_info local_node_record = T_NODE_INFO_INITIALIZER;

	NodeInfoList sibling_nodes = T_NODE_INFO_LIST_INITIALIZER;
	SiblingNodeStats sibling_nodes_stats = T_SIBLING_NODES_STATS_INITIALIZER;
	int			available_wal_senders = 0;
	bool		dry_run_success = true;

	local_conn = establish_db_connection(config_file_options.conninfo, true);

	log_verbose(LOG_INFO, _("connected to standby, checking its state"));

	/* Verify that standby is a supported server version */
	(void) check_server_version(local_conn, "standby", true, NULL);

	/* Check we are in a standby node */
	recovery_type = get_recovery_type(local_conn);

	if (recovery_type != RECTYPE_STANDBY)
	{
		if (recovery_type == RECTYPE_PRIMARY)
		{
			log_error(_("STANDBY PROMOTE can only be executed on a standby node"));
			PQfinish(local_conn);
			exit(ERR_PROMOTION_FAIL);
		}
		else
		{
			log_error(_("unable to determine node's recovery state"));
			PQfinish(local_conn);
			exit(ERR_DB_CONN);
		}
	}
	else if (runtime_options.dry_run == true)
	{
		log_info(_("node is a standby"));
	}

	record_status = get_node_record(local_conn, config_file_options.node_id, &local_node_record);
	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve node record for node %i"),
				  config_file_options.node_id);

		PQfinish(local_conn);

		exit(ERR_DB_QUERY);
	}

	/*
	 * In PostgreSQL 12 and earlier, executing "pg_ctl ... promote" when WAL
	 * replay is paused and WAL is pending replay will mean the standby will
	 * not promote until replay is resumed.
	 *
	 * As that could happen at any time outside repmgr's control, we
	 * need to avoid leaving a "ticking timebomb" which might cause
	 * an unexpected status change in the replication cluster.
	 */
	if (PQserverVersion(local_conn) < 130000)
	{
		ReplInfo 	replication_info;
		bool 	 	replay_paused = false;

		init_replication_info(&replication_info);

		if (get_replication_info(local_conn, STANDBY, &replication_info) == false)
		{
			log_error(_("unable to retrieve replication information from local node"));
			PQfinish(local_conn);
			exit(ERR_PROMOTION_FAIL);
		}

		/*
		 * If the local node is recovering from archive, we can't tell
		 * whether there's still WAL which needs to be replayed, so
		 * we'll abort if WAL replay is paused.
		 */
		if (replication_info.receiving_streamed_wal == false)
		{
			/* just a simple check for paused WAL replay */
			replay_paused = is_wal_replay_paused(local_conn, false);
			if (replay_paused == true)
			{
				log_error(_("WAL replay is paused on this node"));
				log_detail(_("node is in archive recovery and is not safe to promote in this state"));
				log_detail(_("replay paused at %X/%X"),
						   format_lsn(replication_info.last_wal_replay_lsn));
			}
		}
		else
		{
			/* check that replay is pause *and* WAL is pending replay */
			replay_paused = is_wal_replay_paused(local_conn, true);
			if (replay_paused == true)
			{
				log_error(_("WAL replay is paused on this node but not all WAL has been replayed"));
				log_detail(_("replay paused at %X/%X; last WAL received is %X/%X"),
						   format_lsn(replication_info.last_wal_replay_lsn),
						   format_lsn(replication_info.last_wal_receive_lsn));
			}
		}

		if (replay_paused == true)
		{
			if (PQserverVersion(local_conn) >= 100000)
				log_hint(_("execute \"pg_wal_replay_resume()\" to unpause WAL replay"));
			else
				log_hint(_("execute \"pg_xlog_replay_resume()\" to npause WAL replay"));

			PQfinish(local_conn);
			exit(ERR_PROMOTION_FAIL);
		}
	}

	/* check that there's no existing primary */
	current_primary_conn = get_primary_connection_quiet(local_conn, &existing_primary_id, NULL);

	if (PQstatus(current_primary_conn) == CONNECTION_OK)
	{
		log_error(_("this replication cluster already has an active primary server"));

		if (existing_primary_id != UNKNOWN_NODE_ID)
		{
			t_node_info primary_rec;

			get_node_record(local_conn, existing_primary_id, &primary_rec);

			log_detail(_("current primary is \"%s\" (ID: %i)"),
					   primary_rec.node_name,
					   existing_primary_id);
		}

		PQfinish(current_primary_conn);
		PQfinish(local_conn);
		exit(ERR_PROMOTION_FAIL);
	}
	else if (runtime_options.dry_run == true)
	{
		log_info(_("no active primary server found in this replication cluster"));
	}

	PQfinish(current_primary_conn);

	/*
	 * populate local node record with current state of various replication-related
	 * values, so we can check for sufficient walsenders and replication slots
	 */
	get_node_replication_stats(local_conn, &local_node_record);

	available_wal_senders = local_node_record.max_wal_senders -
		local_node_record.attached_wal_receivers;


	/*
	 * Get list of sibling nodes; if --siblings-follow specified,
	 * check they're reachable; if not, the list will be used to warn
	 * about nodes which will not follow the new primary
	 */
	get_active_sibling_node_records(local_conn,
									local_node_record.node_id,
									local_node_record.upstream_node_id,
									&sibling_nodes);

	if (check_sibling_nodes(&sibling_nodes, &sibling_nodes_stats) == false)
	{
		PQfinish(local_conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * check there are sufficient free walsenders - obviously there's potential
	 * for a later race condition if some walsenders come into use before the
	 * promote operation gets around to attaching the sibling nodes, but
	 * this should catch any actual existing configuration issue (and if anyone's
	 * performing a promote in such an unstable environment, they only have
	 * themselves to blame).
	 */
	if (check_free_wal_senders(available_wal_senders, &sibling_nodes_stats, &dry_run_success) == false)
	{
		if (runtime_options.dry_run == false || runtime_options.force == false)
		{
			PQfinish(local_conn);
			exit(ERR_BAD_CONFIG);
		}
	}


	/*
	 * if replication slots are required by siblings,
	 * check the promotion candidate has sufficient free slots
	 */
	if (check_free_slots(&local_node_record, &sibling_nodes_stats, &dry_run_success) == false)
	{
		if (runtime_options.dry_run == false || runtime_options.force == false)
		{
			PQfinish(local_conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/*
	 * In --dry-run mode, note which promotion method will be used.
	 * For Pg12 and later, check whether pg_promote() can be executed.
	 */
	if (runtime_options.dry_run == true)
	{
		if (config_file_options.service_promote_command[0] != '\0')
		{
			log_info(_("node will be promoted using command defined in \"service_promote_command\""));
			log_detail(_("\"service_promote_command\" is \"%s\""),
					   config_file_options.service_promote_command);
		}
		else if (PQserverVersion(local_conn) >= 120000)
		{
			if (can_execute_pg_promote(local_conn) == false)
			{
				log_info(_("node will be promoted using \"pg_ctl promote\""));
				log_detail(_("user \"%s\" does not have permission to execute \"pg_promote()\""),
						   PQuser(local_conn));
			}
			else
			{
				log_info(_("node will be promoted using the \"pg_promote()\" function"));
			}
		}
		else
		{
			log_info(_("node will be promoted using \"pg_ctl promote\""));
		}
	}

	if (runtime_options.dry_run == true)
	{
		PQfinish(local_conn);

		if (dry_run_success == false)
		{
			log_error(_("prerequisites for executing STANDBY PROMOTE are *not* met"));
			log_hint(_("see preceding error messages"));
			exit(ERR_BAD_CONFIG);
		}
		log_info(_("prerequisites for executing STANDBY PROMOTE are met"));
		exit(SUCCESS);
	}

	_do_standby_promote_internal(local_conn);

	/*
	 * If --siblings-follow specified, attempt to make them follow the new
	 * primary
	 */
	if (runtime_options.siblings_follow == true && sibling_nodes.node_count > 0)
	{
		sibling_nodes_follow(&local_node_record, &sibling_nodes, &sibling_nodes_stats);
	}

	clear_node_info_list(&sibling_nodes);

	return;
}


static void
_do_standby_promote_internal(PGconn *conn)
{
	int			i;
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
	 * "pg_ctl promote: returns immediately and (prior to 10.0) has no -w
	 * option so we can't be sure when or if the promotion completes. For now
	 * we'll poll the server until the default timeout (60 seconds)
	 *
	 * For PostgreSQL 12+, use the pg_promote() function, unless one of
	 * "service_promote_command" or "use_pg_ctl_promote" is set.
	 */
	{
		bool use_pg_promote = false;


		if (PQserverVersion(conn) >= 120000)
		{
			use_pg_promote = true;

			if (config_file_options.service_promote_command[0] != '\0')
			{
				use_pg_promote = false;
			}
			else if (can_execute_pg_promote(conn) == false)
			{
				use_pg_promote = false;
				log_info(_("user \"%s\" does not have permission to execute \"pg_promote()\", falling back to \"pg_ctl promote\""),
						 PQuser(conn));
			}
		}

		log_notice(_("promoting standby to primary"));

		if (use_pg_promote == true)
		{
			log_detail(_("promoting server \"%s\" (ID: %i) using pg_promote()"),
					   local_node_record.node_name,
					   local_node_record.node_id);

			/*
			 * We'll check for promotion success ourselves, but will abort
			 * if some unrecoverable error prevented the function from being
			 * executed.
			 */
			if (!promote_standby(conn, false, 0))
			{
				log_error(_("unable to promote server from standby to primary"));
				exit(ERR_PROMOTION_FAIL);
			}
		}
		else
		{
			char		script[MAXLEN];
			int			r;

			get_server_action(ACTION_PROMOTE, script, (char *) data_dir);

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
		}
	}

	log_notice(_("waiting up to %i seconds (parameter \"promote_check_timeout\") for promotion to complete"),
			   config_file_options.promote_check_timeout);

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
			log_detail(_("node still in recovery after %i seconds"), config_file_options.promote_check_timeout);
			log_hint(_("the node may need more time to promote itself, check the PostgreSQL log for details"));
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
	t_node_info local_node_record = T_NODE_INFO_INITIALIZER;

	PGconn	   *primary_conn = NULL;
	int			primary_node_id = UNKNOWN_NODE_ID;

	PGconn	   *follow_target_conn = NULL;
	int			follow_target_node_id = UNKNOWN_NODE_ID;
	t_node_info follow_target_node_record = T_NODE_INFO_INITIALIZER;
	bool		follow_target_is_primary = true;

	RecordStatus record_status = RECORD_NOT_FOUND;
	/* so we can pass info about the primary to event notification scripts */
	t_event_info event_info = T_EVENT_INFO_INITIALIZER;

	int			timer = 0;

	PQExpBufferData follow_output;
	bool		success = false;
	int			follow_error_code = SUCCESS;

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

	/* attempt to retrieve local node record */
	record_status = get_node_record(local_conn,
									config_file_options.node_id,
									&local_node_record);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve record for local node %i"),
				  config_file_options.node_id);
		PQfinish(local_conn);
		exit(ERR_BAD_CONFIG);
	}

	/*
	 * --upstream-node-id provided - attempt to follow that node
	 */
	if (runtime_options.upstream_node_id != UNKNOWN_NODE_ID)
	{
		/* we can't follow ourselves */
		if (runtime_options.upstream_node_id == config_file_options.node_id)
		{
			log_error(_("provided --upstream-node-id %i is the current node"),
					  runtime_options.upstream_node_id);
			PQfinish(local_conn);
			exit(ERR_FOLLOW_FAIL);
		}

		follow_target_node_id = runtime_options.upstream_node_id;
		record_status = get_node_record(local_conn,
										follow_target_node_id,
										&follow_target_node_record);

		/* but we must follow a node which exists (=registered) */
		if (record_status != RECORD_FOUND)
		{
			log_error(_("unable to find record for intended upstream node %i"),
					  runtime_options.upstream_node_id);
			PQfinish(local_conn);
			exit(ERR_FOLLOW_FAIL);
		}
	}
	/*
	 * otherwise determine the current primary and attempt to follow that
	 */
	else
	{
		log_notice(_("attempting to find and follow current primary"));
	}

	/*
	 * Attempt to connect to follow target - if this was provided with --upstream-node-id,
	 * we'll connect to that, otherwise we'll attempt to find the current primary.
	 *
	 * If --wait provided, loop for up `primary_follow_timeout` seconds
	 * before giving up
	 *
	 * XXX add `upstream_follow_timeout` ?
	 */

	for (timer = 0; timer < config_file_options.primary_follow_timeout; timer++)
	{
		/* --upstream-node-id provided - connect to specified node*/
		if (follow_target_node_id != UNKNOWN_NODE_ID)
		{
			follow_target_conn = establish_db_connection_quiet(follow_target_node_record.conninfo);
		}
		/* attempt to find current primary node */
		else
		{
			follow_target_conn = get_primary_connection_quiet(local_conn,
															  &follow_target_node_id,
															  NULL);
		}

		if (PQstatus(follow_target_conn) == CONNECTION_OK || runtime_options.wait_provided == false)
		{
			break;
		}
		sleep(1);
	}

	/* unable to connect to the follow target */
	if (PQstatus(follow_target_conn) != CONNECTION_OK)
	{
		if (follow_target_node_id == UNKNOWN_NODE_ID)
		{
			log_error(_("unable to find a primary node"));
		}
		else
		{
			log_error(_("unable to connect to target node %i"), follow_target_node_id);
		}

		if (runtime_options.wait_provided == true)
		{
			if (follow_target_node_id == UNKNOWN_NODE_ID)
			{
				log_detail(_("no primary appeared after %i seconds"),
						   config_file_options.primary_follow_timeout);
			}
			else
			{
				log_detail(_("unable to connect to target node %i after %i seconds"),
						   follow_target_node_id,
						   config_file_options.primary_follow_timeout);
			}

			log_hint(_("alter \"primary_follow_timeout\" in \"repmgr.conf\" to change this value"));
		}

		PQfinish(local_conn);
		exit(ERR_FOLLOW_FAIL);
	}

	/* --upstream-node-id not provided - retrieve record for node determined as primary  */
	if (runtime_options.upstream_node_id == UNKNOWN_NODE_ID)
	{
		if (runtime_options.dry_run == true)
		{
			log_info(_("connected to node %i, checking for current primary"), follow_target_node_id);
		}
		else
		{
			log_verbose(LOG_INFO, _("connected to node %i, checking for current primary"), follow_target_node_id);
		}

		record_status = get_node_record(follow_target_conn,
										follow_target_node_id,
										&follow_target_node_record);

		if (record_status != RECORD_FOUND)
		{
			log_error(_("unable to find record for follow target node %i"),
					  follow_target_node_id);
			PQfinish(follow_target_conn);
			exit(ERR_FOLLOW_FAIL);
		}
	}

	/*
	 * Populate "event_info" with info about the node to follow for event notifications
	 *
	 * XXX need to differentiate between primary and non-primary?
	 */
	event_info.node_id = follow_target_node_id;
	event_info.node_name = follow_target_node_record.node_name;
	event_info.conninfo_str = follow_target_node_record.conninfo;

	/*
	 * Check whether follow target is in recovery, so we know later whether
	 * we'll need to open a connection to the primary to update the metadata.
	 * Also emit an informative message.
	 */
	{
		PQExpBufferData node_info_msg;
		RecoveryType recovery_type = RECTYPE_UNKNOWN;
		initPQExpBuffer(&node_info_msg);

		recovery_type = get_recovery_type(follow_target_conn);

		/*
		 * unlikely this will happen, but it's conceivable the follow target will
		 * have vanished since we last talked to it, or something
		 */
		if (recovery_type == RECTYPE_UNKNOWN)
		{
			log_error(_("unable to determine recovery type of follow target"));
			PQfinish(follow_target_conn);
			exit(ERR_FOLLOW_FAIL);
		}

		if (recovery_type == RECTYPE_PRIMARY)
		{
			follow_target_is_primary = true;
			appendPQExpBuffer(&node_info_msg,
							  _("follow target is primary node \"%s\" (ID: %i)"),
							  follow_target_node_record.node_name,
							  follow_target_node_id);
		}
		else
		{
			follow_target_is_primary = false;
			appendPQExpBuffer(&node_info_msg,
							  _("follow target is standby node \"%s\" (ID: %i)"),
							  follow_target_node_record.node_name,
							  follow_target_node_id);
		}

		if (runtime_options.dry_run == true)
		{
			log_info("%s", node_info_msg.data);
		}
		else
		{
			log_verbose(LOG_INFO, "%s", node_info_msg.data);
		}

		termPQExpBuffer(&node_info_msg);
	}

	/*
	 * if replication slots in use, check at least one free slot is available
	 * on the follow target
	 */

	if (config_file_options.use_replication_slots)
	{
		bool slots_available = check_replication_slots_available(follow_target_node_id,
																 follow_target_conn);
		if (slots_available == false)
		{
			PQfinish(follow_target_conn);
			PQfinish(local_conn);
			exit(ERR_FOLLOW_FAIL);
		}
	}

	/* XXX check this is not current upstream anyway */

	/* check if we can attach to the follow target */
	{
		PGconn	   *local_repl_conn = NULL;
		t_system_identification local_identification = T_SYSTEM_IDENTIFICATION_INITIALIZER;

		bool can_follow;
		XLogRecPtr local_xlogpos = get_node_current_lsn(local_conn);

		/* Check local replication connection - we want to execute IDENTIFY_SYSTEM
		 * to get the current timeline ID, which might not yet be written to
		 * pg_control.
		 *
		 * TODO: from 9.6, query "pg_stat_wal_receiver" via the existing local connection
		 */

		local_repl_conn = establish_replication_connection_from_conn(local_conn,
																	 local_node_record.repluser);
		if (PQstatus(local_repl_conn) != CONNECTION_OK)
		{
			log_error(_("unable to establish a replication connection to the local node"));

			PQfinish(local_conn);
			PQfinish(follow_target_conn);

			exit(ERR_FOLLOW_FAIL);
		}
		else if (runtime_options.dry_run == true)
		{
			log_info(_("replication connection to the local node was successful"));
		}

		success = identify_system(local_repl_conn, &local_identification);
		PQfinish(local_repl_conn);

		if (success == false)
		{
			log_error(_("unable to query the local node's system identification"));

			PQfinish(local_conn);

			PQfinish(follow_target_conn);

			exit(ERR_FOLLOW_FAIL);
		}

		can_follow = check_node_can_attach(local_identification.timeline,
										   local_xlogpos,
										   follow_target_conn,
										   &follow_target_node_record,
										   false);

		if (can_follow == false)
		{
			PQfinish(local_conn);
			PQfinish(follow_target_conn);
			exit(ERR_FOLLOW_FAIL);
		}
	}

	PQfinish(local_conn);

	/*
	 * Here we'll need a connection to the primary, if the upstream is not a primary.
	 */
	if (follow_target_is_primary == false)
	{
		/*
		 * We'll try and establish primary from follow target, in the assumption its node
		 * record is more up-to-date.
		 */
		primary_conn = get_primary_connection_quiet(follow_target_conn,
													&primary_node_id,
													NULL);

		/*
		 * If follow target is not primary and no other primary could be found,
		 * abort because we won't be able to update the node record.
		 */
		if (PQstatus(primary_conn) != CONNECTION_OK)
		{
			log_error(_("unable to determine the cluster primary"));
			log_detail(_("an active primary node is required for \"repmgr standby follow\""));
			PQfinish(follow_target_conn);
			exit(ERR_FOLLOW_FAIL);
		}
	}
	else
	{
		primary_conn = follow_target_conn;
	}

	if (runtime_options.dry_run == true)
	{
		log_info(_("prerequisites for executing STANDBY FOLLOW are met"));
		exit(SUCCESS);
	}

	initPQExpBuffer(&follow_output);

	success = do_standby_follow_internal(
		primary_conn,
		follow_target_conn,
		&follow_target_node_record,
		&follow_output,
		ERR_FOLLOW_FAIL,
		&follow_error_code);

	/* unable to restart the standby */
	if (success == false)
	{
		create_event_notification_extended(
			follow_target_conn,
			&config_file_options,
			config_file_options.node_id,
			"standby_follow",
			success,
			follow_output.data,
			&event_info);

		PQfinish(follow_target_conn);

		if (follow_target_is_primary == false)
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
		NodeAttached node_attached = is_downstream_node_attached(follow_target_conn,
																 config_file_options.node_name,
																 NULL);

		if (node_attached == NODE_ATTACHED)
		{
			success = true;
			break;
		}

		log_verbose(LOG_DEBUG, "sleeping %i of max %i seconds waiting for standby to attach to primary",
					timer + 1,
					config_file_options.standby_follow_timeout);
		sleep(1);
	}

	if (success == true)
	{
		log_notice(_("STANDBY FOLLOW successful"));
		appendPQExpBuffer(&follow_output,
						  "standby attached to upstream node \"%s\" (ID: %i)",
						  follow_target_node_record.node_name,
						  follow_target_node_id);
	}
	else
	{
		log_error(_("STANDBY FOLLOW failed"));
		appendPQExpBuffer(&follow_output,
						  "standby did not attach to upstream node \"%s\" (ID: %i) after %i seconds",
						  follow_target_node_record.node_name,
						  follow_target_node_id,
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

	termPQExpBuffer(&follow_output);

	PQfinish(follow_target_conn);

	if (follow_target_is_primary == false)
		PQfinish(primary_conn);

	if (success == false)
		exit(ERR_FOLLOW_FAIL);

	return;
}


/*
 * Perform the actual "follow" operation; this is executed by
 * "node rejoin" too.
 */
bool
do_standby_follow_internal(PGconn *primary_conn, PGconn *follow_target_conn, t_node_info *follow_target_node_record, PQExpBufferData *output, int general_error_code, int *error_code)
{
	t_node_info local_node_record = T_NODE_INFO_INITIALIZER;
	int			original_upstream_node_id = UNKNOWN_NODE_ID;
	t_node_info original_upstream_node_record = T_NODE_INFO_INITIALIZER;

	RecordStatus record_status = RECORD_NOT_FOUND;
	char	   *errmsg = NULL;

	bool		remove_old_replication_slot = false;

	/*
	 * Fetch our node record so we can write application_name, if set, and to
	 * get the current upstream node ID, which we'll need to know if replication
	 * slots are in use and we want to delete this node's slot on the current
	 * upstream.
	 */
	record_status = get_node_record(primary_conn,
									config_file_options.node_id,
									&local_node_record);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve record for node %i"),
				  config_file_options.node_id);

		*error_code = ERR_BAD_CONFIG;
		return false;
	}

	/*
	 * If replication slots are in use, we'll need to create a slot on the
	 * follow target
	 */

	if (config_file_options.use_replication_slots)
	{
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


		if (create_replication_slot(follow_target_conn,
									local_node_record.slot_name,
									NULL,
									output) == false)
		{
			log_error("%s", output->data);

			*error_code = general_error_code;

			return false;
		}
	}

	/*
	 * Store the original upstream node id so we can delete the
	 * replication slot, if it exists.
	 */
	if (local_node_record.upstream_node_id != UNKNOWN_NODE_ID)
	{
		original_upstream_node_id = local_node_record.upstream_node_id;
	}
	else
	{
		original_upstream_node_id = follow_target_node_record->node_id;
	}

	if (config_file_options.use_replication_slots && runtime_options.host_param_provided == false)
	{
		/*
		 * Only attempt to delete the old replication slot if the old upstream
		 * node is known and is different to the follow target node.
		 */
		if (original_upstream_node_id != UNKNOWN_NODE_ID
		 && original_upstream_node_id != follow_target_node_record->node_id)
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

	/* Initialise connection parameters to write as "primary_conninfo" */
	initialize_conninfo_params(&recovery_conninfo, false);

	/* We ignore any application_name set in the primary's conninfo */
	parse_conninfo_string(follow_target_node_record->conninfo, &recovery_conninfo, &errmsg, true);

	/* Set the application name to this node's name */
	param_set(&recovery_conninfo, "application_name", config_file_options.node_name);

	/* Set the replication user from the follow target node record */
	param_set(&recovery_conninfo, "user", follow_target_node_record->repluser);

	log_notice(_("setting node %i's upstream to node %i"),
			   config_file_options.node_id, follow_target_node_record->node_id);

	if (!create_recovery_file(&local_node_record,
							  &recovery_conninfo,
							  PQserverVersion(primary_conn),
							  config_file_options.data_directory,
							  true))
	{
		*error_code = general_error_code;
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

			if (PQserverVersion(primary_conn) >= 130000 && config_file_options.standby_follow_restart == false)
			{
				/* PostgreSQL 13 and later: we'll send SIGHUP via pg_ctl */
				get_server_action(ACTION_RELOAD, server_command, config_file_options.data_directory);

				success = local_command(server_command, &output_buf);

				if (success == true)
				{
					goto cleanup;
				}

				/* In the unlikley event that fails, we'll fall back to a restart */
				log_warning(_("unable to reload server configuration"));
			}

			if (config_file_options.service_restart_command[0] == '\0')
			{
				/* no "service_restart_command" defined - stop and start using pg_ctl */

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

cleanup:
	/*
	 * If replication slots are in use, and an inactive one for this node
	 * exists on the former upstream, drop it.
	 *
	 * Note that if this function is called by do_standby_switchover(), the
	 * "repmgr node rejoin" command executed on the demotion candidate may already
	 * have removed the slot, so there may be nothing to do.
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
								  follow_target_node_record->node_id,
								  true) == false)
	{
		appendPQExpBufferStr(output,
							 _("unable to update upstream node"));
		return false;
	}

	appendPQExpBuffer(output,
					  _("node %i is now attached to node %i"),
					  config_file_options.node_id,
					  follow_target_node_record->node_id);

	return true;
}


/*
 * Perform a switchover by:
 *
 *  - stopping current primary node
 *  - promoting this standby node to primary
 *  - forcing the previous primary node to follow this node
 *
 * Where running and not already paused, repmgrd will be paused (and
 * subsequently unpaused), unless --repmgrd-no-pause provided.
 *
 * Note that this operation can only be considered to have failed completely
 * ("ERR_SWITCHOVER_FAIL") in these situations:
 *
 *  - the prerequisites for a switchover are not met
 *  - the demotion candidate could not be shut down cleanly
 *  - the promotion candidate could not be promoted
 *
 * All other failures (demotion candidate did not connect to new primary etc.)
 * are considered partial failures ("ERR_SWITCHOVER_INCOMPLETE")
 *
 * TODO:
 *  - make connection test timeouts/intervals configurable (see below)
 */


void
do_standby_switchover(void)
{
	PGconn	   *local_conn = NULL;
	PGconn	   *superuser_conn = NULL;
	PGconn	   *remote_conn = NULL;

	t_node_info local_node_record = T_NODE_INFO_INITIALIZER;

	/* the remote server is the primary to be demoted */
	char		remote_conninfo[MAXCONNINFO] = "";
	char		remote_host[MAXLEN] = "";
	int			remote_node_id = UNKNOWN_NODE_ID;
	t_node_info remote_node_record = T_NODE_INFO_INITIALIZER;
	int 		remote_repmgr_version = UNKNOWN_REPMGR_VERSION_NUM;

	RecordStatus record_status = RECORD_NOT_FOUND;
	RecoveryType recovery_type = RECTYPE_UNKNOWN;
	PQExpBufferData remote_command_str;
	PQExpBufferData command_output;
	PQExpBufferData node_rejoin_options;
	PQExpBufferData errmsg;
	PQExpBufferData detailmsg;

	int			r,
				i;
	bool		command_success = false;
	bool		shutdown_success = false;
	bool		dry_run_success = true;

	/* this flag will use to generate the final message generated */
	bool		switchover_success = true;

	XLogRecPtr	remote_last_checkpoint_lsn = InvalidXLogRecPtr;
	ReplInfo	replication_info;

	/* store list of configuration files on the demotion candidate */
	KeyValueList remote_config_files = {NULL, NULL};

	NodeInfoList sibling_nodes = T_NODE_INFO_LIST_INITIALIZER;
	SiblingNodeStats sibling_nodes_stats = T_SIBLING_NODES_STATS_INITIALIZER;

	/* this will be calculated as max_wal_senders - COUNT(*) FROM pg_stat_replication */
	int			available_wal_senders = 0;

	t_event_info event_info = T_EVENT_INFO_INITIALIZER;

	/* used for handling repmgrd pause/unpause */
	NodeInfoList all_nodes = T_NODE_INFO_LIST_INITIALIZER;
	RepmgrdInfo **repmgrd_info = NULL;
	int			repmgrd_running_count = 0;

	/* number of free walsenders required on promotion candidate
	 * (at least one will be required for the demotion candidate)
	 */
	sibling_nodes_stats.min_required_wal_senders = 1;

	/*
	 * SANITY CHECKS
	 *
	 * We'll be doing a bunch of operations on the remote server (primary to
	 * be demoted) - careful checks needed before proceding.
	 */

	local_conn = establish_db_connection(config_file_options.conninfo, true);

	/* Verify that standby is a supported server version */
	(void) check_server_version(local_conn, "standby", true, NULL);

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

	/*
	 * If -S/--superuser option provided, check that a superuser connection can be made
	 * to the local database. We'll check the remote superuser connection later,
	 */

	if (runtime_options.superuser[0] != '\0')
	{
		if (runtime_options.dry_run == true)
		{
			log_info(_("validating connection to local database for superuser \"%s\""),
					 runtime_options.superuser);
		}

		superuser_conn = establish_db_connection_with_replacement_param(
			config_file_options.conninfo,
			"user",
			runtime_options.superuser, false);

		if (PQstatus(superuser_conn) != CONNECTION_OK)
		{
			log_error(_("unable to connect to local database \"%s\" as provided superuser \"%s\""),
					  PQdb(superuser_conn),
					  runtime_options.superuser);
			exit(ERR_BAD_CONFIG);
		}

		if (is_superuser_connection(superuser_conn, NULL) == false)
		{
			log_error(_("connection established to local database \"%s\" for provided superuser \"%s\" is not a superuser connection"),
					  PQdb(superuser_conn),
					  runtime_options.superuser);
			exit(ERR_BAD_CONFIG);
		}

		if (runtime_options.dry_run == true)
		{
			log_info(_("successfully established connection to local database \"%s\" for provided superuser \"%s\""),
					 PQdb(superuser_conn),
					 runtime_options.superuser);
		}

	}

	/*
	 * Warn if no superuser connection is available.
	 */
	if (superuser_conn == NULL && is_superuser_connection(local_conn, NULL) == false)
	{
		log_warning(_("no superuser connection available"));
		log_detail(_("it is recommended to perform switchover operations with a database superuser"));
		log_hint(_("provide the name of a superuser with -S/--superuser"));
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

	/*
	 * Check that the local replication configuration file is owned by the data
	 * directory owner.
	 *
	 * For PostgreSQL 11 and earlier, if PostgreSQL is not able to rename "recovery.conf",
	 * promotion will fail.
	 *
	 * For PostgreSQL 12 and later, promotion will not fail even if "postgresql.auto.conf"
	 * is owned by another user, but we'll check just in case, as it is indicative of a
	 * poorly configured setup. In any case we will need to check "postgresql.auto.conf" on
	 * the demotion candidate as the rejoin will fail if we are unable to to write to that.
	 */

	initPQExpBuffer(&errmsg);
	initPQExpBuffer(&detailmsg);

	if (check_replication_config_owner(PQserverVersion(local_conn),
									   config_file_options.data_directory,
									   &errmsg, &detailmsg) == false)
	{
		log_error("%s", errmsg.data);
		log_detail("%s", detailmsg.data);

		termPQExpBuffer(&errmsg);
		termPQExpBuffer(&detailmsg);

		PQfinish(local_conn);
		exit(ERR_BAD_CONFIG);
	}

	termPQExpBuffer(&errmsg);
	termPQExpBuffer(&detailmsg);

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
		log_error(_("unable to retrieve node record for current primary (node %i)"),
				  remote_node_id);

		PQfinish(local_conn);
		PQfinish(remote_conn);

		exit(ERR_DB_QUERY);
	}

	log_verbose(LOG_DEBUG, "remote node name is \"%s\"", remote_node_record.node_name);

	/*
	 * Check this standby is attached to the demotion candidate
	 */

	if (local_node_record.upstream_node_id != remote_node_record.node_id)
	{
		log_error(_("local node \"%s\" (ID: %i) is not a downstream of demotion candidate primary \"%s\" (ID: %i)"),
				  local_node_record.node_name,
				  local_node_record.node_id,
				  remote_node_record.node_name,
				  remote_node_record.node_id);

		if (local_node_record.upstream_node_id == UNKNOWN_NODE_ID)
			log_detail(_("local node has no registered upstream node"));
		else
			log_detail(_("registered upstream node ID is %i"),
					   local_node_record.upstream_node_id);

		log_hint(_("execute \"repmgr standby register --force\" to update the local node's metadata"));

		PQfinish(local_conn);
		PQfinish(remote_conn);

		exit(ERR_BAD_CONFIG);
	}

	if (is_downstream_node_attached(remote_conn, local_node_record.node_name, NULL) != NODE_ATTACHED)
	{
		log_error(_("local node \"%s\" (ID: %i) is not attached to demotion candidate \"%s\" (ID: %i)"),
				  local_node_record.node_name,
				  local_node_record.node_id,
				  remote_node_record.node_name,
				  remote_node_record.node_id);

		PQfinish(local_conn);
		PQfinish(remote_conn);

		exit(ERR_BAD_CONFIG);
	}

	/*
	 * In PostgreSQL 12 and earlier, check that WAL replay on the standby
	 * is *not* paused, as that could lead to unexpected behaviour when the
	 * standby is promoted.
	 *
	 * For switchover we'll mandate that WAL replay *must not* be paused.
	 * For a promote operation we can proceed if WAL replay is paused and
	 * there is no more available WAL to be replayed, as we can be sure the
	 * primary is down already, but in a switchover context there's
	 * potentially a window for more WAL to be received before we shut down
	 * the primary completely.
	 */

	if (PQserverVersion(local_conn) < 130000 && is_wal_replay_paused(local_conn, false) == true)
	{
		ReplInfo 	replication_info;
		init_replication_info(&replication_info);

		if (get_replication_info(local_conn, STANDBY, &replication_info) == false)
		{
			log_error(_("unable to retrieve replication information from local node"));
			PQfinish(local_conn);
			exit(ERR_SWITCHOVER_FAIL);
		}

		log_error(_("WAL replay is paused on this node and it is not safe to proceed"));
		log_detail(_("replay paused at %X/%X; last WAL received is %X/%X"),
				   format_lsn(replication_info.last_wal_replay_lsn),
				   format_lsn(replication_info.last_wal_receive_lsn));

		if (PQserverVersion(local_conn) >= 100000)
			log_hint(_("execute \"pg_wal_replay_resume()\" to unpause WAL replay"));
		else
			log_hint(_("execute \"pg_xlog_replay_resume()\" to unpause WAL replay"));

		PQfinish(local_conn);
		exit(ERR_SWITCHOVER_FAIL);
	}


	/*
	 * Check that there are no exclusive backups running on the primary.
	 * We don't want to end up damaging the backup and also leaving the server in an
	 * state where there's control data saying it's in backup mode but there's no
	 * backup_label in PGDATA.
	 * If the user wants to do the switchover anyway, they should first stop the
	 * backup that's running.
	 */
	if (server_in_exclusive_backup_mode(remote_conn) != BACKUP_STATE_NO_BACKUP)
	{
		log_error(_("unable to perform a switchover while primary server is in exclusive backup mode"));
		log_hint(_("stop backup before attempting the switchover"));

		PQfinish(local_conn);
		PQfinish(remote_conn);

		exit(ERR_SWITCHOVER_FAIL);
	}

	/* this will fill the %p event notification parameter */
	event_info.node_id = remote_node_record.node_id;

	/* keep a running total of how many nodes will require a replication slot */
	if (remote_node_record.slot_name[0] != '\0')
	{
		sibling_nodes_stats.min_required_free_slots++;
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
		appendPQExpBufferStr(&msg,
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

	/*
	 * Here we're executing an arbitrary repmgr command which is guaranteed to
	 * succeed if repmgr is executed. We'll extract the actual version number in the
	 * next step.
	 */
	appendPQExpBufferStr(&remote_command_str, "--version >/dev/null 2>&1 && echo \"1\" || echo \"0\"");
	initPQExpBuffer(&command_output);
	command_success = remote_command(remote_host,
									 runtime_options.remote_user,
									 remote_command_str.data,
									 config_file_options.ssh_options,
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
		appendPQExpBufferStr(&hint,
							 _("check \"pg_bindir\" is set to the correct path in \"repmgr.conf\"; current value: "));

		if (strlen(config_file_options.pg_bindir))
		{
			appendPQExpBuffer(&hint,
							  "\"%s\"", config_file_options.pg_bindir);
		}
		else
		{
			appendPQExpBufferStr(&hint,
								 "(not set)");
		}

		log_hint("%s", hint.data);

		termPQExpBuffer(&hint);

		PQfinish(remote_conn);
		PQfinish(local_conn);

		exit(ERR_BAD_CONFIG);
	}

	termPQExpBuffer(&command_output);

	/*
	 * Now we're sure the binary can be executed, fetch its version number.
	 */
	initPQExpBuffer(&remote_command_str);
	make_remote_repmgr_path(&remote_command_str, &remote_node_record);

	appendPQExpBufferStr(&remote_command_str, "--version 2>/dev/null");
	initPQExpBuffer(&command_output);
	command_success = remote_command(remote_host,
									 runtime_options.remote_user,
									 remote_command_str.data,
									 config_file_options.ssh_options,
									 &command_output);

	termPQExpBuffer(&remote_command_str);

	if (command_success == true)
	{
		remote_repmgr_version = parse_repmgr_version(command_output.data);
		if (remote_repmgr_version == UNKNOWN_REPMGR_VERSION_NUM)
		{
			log_error(_("unable to parse \"%s\"'s reported version on \"%s\""),
					  progname(), remote_host);
			PQfinish(remote_conn);
			PQfinish(local_conn);
			exit(ERR_BAD_CONFIG);
		}
		log_debug(_("\"%s\" version on \"%s\" is %i"),
				  progname(), remote_host, remote_repmgr_version );

	}
	else
	{
		log_error(_("unable to execute \"%s\" on \"%s\""),
				  progname(), remote_host);

		if (strlen(command_output.data) > 2)
			log_detail("%s", command_output.data);

		termPQExpBuffer(&command_output);

		PQfinish(remote_conn);
		PQfinish(local_conn);

		exit(ERR_BAD_CONFIG);
	}

	termPQExpBuffer(&command_output);

	/*
	 * Check if the expected remote repmgr.conf file exists
	 */
	initPQExpBuffer(&remote_command_str);

	appendPQExpBuffer(&remote_command_str,
					  "test -f %s && echo 1 || echo 0",
					  remote_node_record.config_file);
	initPQExpBuffer(&command_output);

	command_success = remote_command(remote_host,
									 runtime_options.remote_user,
									 remote_command_str.data,
									 config_file_options.ssh_options,
									 &command_output);

	termPQExpBuffer(&remote_command_str);

	if (command_success == false || command_output.data[0] == '0')
	{
		log_error(_("expected configuration file not found on the demotion candiate \"%s\" (ID: %i)"),
				  remote_node_record.node_name,
				  remote_node_record.node_id);
		log_detail(_("registered configuration file is \"%s\""),
				   remote_node_record.config_file);
		log_hint(_("ensure the configuration file is in the expected location, or re-register \"%s\" to update the configuration file location"),
				  remote_node_record.node_name);

		PQfinish(remote_conn);
		PQfinish(local_conn);

		termPQExpBuffer(&command_output);

		exit(ERR_BAD_CONFIG);
	}


	/*
	 * Sanity-check remote "data_directory" is correctly configured in repmgr.conf.
	 *
	 * This is important as we'll need to be able to run "repmgr node status" on the data
	 * directory after the remote (demotion candidate) has shut down.
	 */

	initPQExpBuffer(&remote_command_str);
	make_remote_repmgr_path(&remote_command_str, &remote_node_record);

	/*
	 * --data-directory-config is available from repmgr 4.3; it will fail
	 * if the remote repmgr is an earlier version, but the version should match
	 * anyway.
	 */
	appendPQExpBufferStr(&remote_command_str, "node check --data-directory-config --optformat -LINFO 2>/dev/null");

	initPQExpBuffer(&command_output);
	command_success = remote_command(remote_host,
									 runtime_options.remote_user,
									 remote_command_str.data,
									 config_file_options.ssh_options,
									 &command_output);

	termPQExpBuffer(&remote_command_str);

	if (command_success == false)
	{
		log_error(_("unable to execute \"%s node check --data-directory-config\" on \"%s\":"),
				  progname(), remote_host);
		log_detail("%s", command_output.data);

		PQfinish(remote_conn);
		PQfinish(local_conn);

		termPQExpBuffer(&command_output);

		exit(ERR_BAD_CONFIG);
	}

	/* check remote repmgr has the data directory correctly configured */
	{
		t_remote_error_type remote_error = REMOTE_ERROR_NONE;

		if (parse_data_directory_config(command_output.data, &remote_error) == false)
		{
			if (remote_error != REMOTE_ERROR_NONE)
			{
				log_error(_("unable to run data directory check on node \"%s\" (ID: %i)"),
							remote_node_record.node_name,
							remote_node_record.node_id);

				if (remote_error == REMOTE_ERROR_DB_CONNECTION)
				{
					PQExpBufferData ssh_command;

					/* can happen if the connection configuration is not consistent across nodes */
					log_detail(_("an error was encountered when attempting to connect to PostgreSQL on node \"%s\" (ID: %i)"),
							   remote_node_record.node_name,
							   remote_node_record.node_id);

					/* output a helpful hint to help diagnose the issue */
					initPQExpBuffer(&remote_command_str);
					make_remote_repmgr_path(&remote_command_str, &remote_node_record);

					appendPQExpBufferStr(&remote_command_str, "node check --db-connection");

					initPQExpBuffer(&ssh_command);

					make_remote_command(remote_host,
										runtime_options.remote_user,
										remote_command_str.data,
										config_file_options.ssh_options,
										&ssh_command);

					log_hint(_("diagnose with:\n  %s"), ssh_command.data);

					termPQExpBuffer(&remote_command_str);
					termPQExpBuffer(&ssh_command);
				}
				else if (remote_error == REMOTE_ERROR_CONNINFO_PARSE)
				{
					/* highly unlikely */
					log_detail(_("an error was encountered when parsing the \"conninfo\" parameter in \"rempgr.conf\" on node \"%s\" (ID: %i)"),
							   remote_node_record.node_name,
							   remote_node_record.node_id);
				}
				else
				{
					log_detail(_("an unknown error was encountered when attempting to connect to PostgreSQL on node \"%s\" (ID: %i)"),
							   remote_node_record.node_name,
							   remote_node_record.node_id);
				}
			}
			else
			{
				log_error(_("\"data_directory\" parameter in \"repmgr.conf\" on \"%s\" (ID: %i) is incorrectly configured"),
						  remote_node_record.node_name,
						  remote_node_record.node_id);

				log_hint(_("execute \"repmgr node check --data-directory-config\" on \"%s\" (ID: %i) to diagnose the issue"),
						 remote_node_record.node_name,
						 remote_node_record.node_id);

			}

			PQfinish(remote_conn);
			PQfinish(local_conn);

			termPQExpBuffer(&command_output);

			exit(ERR_BAD_CONFIG);
		}
	}

	termPQExpBuffer(&command_output);

	if (runtime_options.dry_run == true)
	{
		log_info(_("able to execute \"%s\" on remote host \"%s\""),
				 progname(),
				 remote_host);
	}

	/*
	 * If -S/--superuser option provided, check that a superuser connection can be made
	 * to the local database on the remote node.
	 */

	if (runtime_options.superuser[0] != '\0')
	{
		CheckStatus status = CHECK_STATUS_UNKNOWN;

		initPQExpBuffer(&remote_command_str);
		make_remote_repmgr_path(&remote_command_str, &remote_node_record);

		appendPQExpBuffer(&remote_command_str,
						  "node check --db-connection --superuser=%s --optformat -LINFO 2>/dev/null",
						  runtime_options.superuser);

		initPQExpBuffer(&command_output);
		command_success = remote_command(remote_host,
										 runtime_options.remote_user,
										 remote_command_str.data,
										 config_file_options.ssh_options,
										 &command_output);

		termPQExpBuffer(&remote_command_str);

		if (command_success == false)
		{
			log_error(_("unable to execute \"%s node check --db-connection\" on \"%s\":"),
					  progname(), remote_host);
			log_detail("%s", command_output.data);

			PQfinish(remote_conn);
			PQfinish(local_conn);

			termPQExpBuffer(&command_output);

			exit(ERR_BAD_CONFIG);
		}

		status = parse_db_connection(command_output.data);

		if (status != CHECK_STATUS_OK)
		{
			PQExpBufferData ssh_command;
			log_error(_("unable to connect locally as superuser \"%s\" on node \"%s\" (ID: %i)"),
					  runtime_options.superuser,
					  remote_node_record.node_name,
					  remote_node_record.node_id);

			/* output a helpful hint to help diagnose the issue */
			initPQExpBuffer(&remote_command_str);
			make_remote_repmgr_path(&remote_command_str, &remote_node_record);

			appendPQExpBuffer(&remote_command_str,
							  "node check --db-connection --superuser=%s",
							  runtime_options.superuser);

			initPQExpBuffer(&ssh_command);

			make_remote_command(remote_host,
								runtime_options.remote_user,
								remote_command_str.data,
								config_file_options.ssh_options,
								&ssh_command);

			log_hint(_("diagnose with:\n  %s"), ssh_command.data);

			termPQExpBuffer(&remote_command_str);
			termPQExpBuffer(&ssh_command);
			exit(ERR_DB_CONN);
		}



		termPQExpBuffer(&command_output);
	}

	/*
	 * For PostgreSQL 12 and later, check "postgresql.auto.conf" is owned by the
	 * correct user, otherwise the node will probably not be able to attach to
	 * the promotion candidate (and is a sign of bad configuration anyway) so we
	 * will complain vocally.
	 *
	 * We'll only do this if we've determined the remote repmgr binary is new
	 * enough to have the "node check --replication-config-owner" option.
	 */

	if (PQserverVersion(local_conn) >= 120000 && remote_repmgr_version >= 50100)
	{
		initPQExpBuffer(&remote_command_str);
		make_remote_repmgr_path(&remote_command_str, &remote_node_record);

		appendPQExpBufferStr(&remote_command_str, "node check --replication-config-owner --optformat -LINFO 2>/dev/null");

		initPQExpBuffer(&command_output);
		command_success = remote_command(remote_host,
										 runtime_options.remote_user,
										 remote_command_str.data,
										 config_file_options.ssh_options,
										 &command_output);

		termPQExpBuffer(&remote_command_str);

		if (command_success == false)
		{
			log_error(_("unable to execute \"%s node check --replication-config-owner\" on \"%s\":"),
					  progname(), remote_host);
			log_detail("%s", command_output.data);

			PQfinish(remote_conn);
			PQfinish(local_conn);

			termPQExpBuffer(&command_output);

			exit(ERR_BAD_CONFIG);
		}

		if (parse_replication_config_owner(command_output.data) == false)
		{
			log_error(_("\"%s\" file on \"%s\" has incorrect ownership"),
					  PG_AUTOCONF_FILENAME,
					  remote_node_record.node_name);

			log_hint(_("check the file has the same owner/group as the data directory"));

			PQfinish(remote_conn);
			PQfinish(local_conn);

			termPQExpBuffer(&command_output);

			exit(ERR_BAD_CONFIG);
		}

		termPQExpBuffer(&command_output);
	}


	/*
	 * populate local node record with current state of various replication-related
	 * values, so we can check for sufficient walsenders and replication slots
	 */
	get_node_replication_stats(local_conn, &local_node_record);

	available_wal_senders = local_node_record.max_wal_senders -
		local_node_record.attached_wal_receivers;

	/*
	 * Get list of sibling nodes; if --siblings-follow specified,
	 * check they're reachable; if not, the list will be used to warn
	 * about nodes which will remain attached to the demotion candidate
	 */
	get_active_sibling_node_records(local_conn,
									local_node_record.node_id,
									local_node_record.upstream_node_id,
									&sibling_nodes);

	if (check_sibling_nodes(&sibling_nodes, &sibling_nodes_stats) == false)
	{
		PQfinish(local_conn);
		exit(ERR_BAD_CONFIG);
	}


	/*
	 * check there are sufficient free walsenders - obviously there's potential
	 * for a later race condition if some walsenders come into use before the
	 * switchover operation gets around to attaching the sibling nodes, but
	 * this should catch any actual existing configuration issue (and if anyone's
	 * performing a switchover in such an unstable environment, they only have
	 * themselves to blame).
	 */
	if (check_free_wal_senders(available_wal_senders, &sibling_nodes_stats, &dry_run_success) == false)
	{
		if (runtime_options.dry_run == false)
		{
			PQfinish(local_conn);
			exit(ERR_BAD_CONFIG);
		}
	}


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
										 config_file_options.ssh_options,
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
			t_remote_error_type remote_error = REMOTE_ERROR_NONE;

			initPQExpBuffer(&remote_command_str);
			make_remote_repmgr_path(&remote_command_str, &remote_node_record);
			appendPQExpBufferStr(&remote_command_str,
								 "node check --terse -LERROR --archive-ready --optformat");

			initPQExpBuffer(&command_output);

			command_success = remote_command(remote_host,
											 runtime_options.remote_user,
											 remote_command_str.data,
											 config_file_options.ssh_options,
											 &command_output);

			termPQExpBuffer(&remote_command_str);

			if (command_success == true)
			{
				status = parse_node_check_archiver(command_output.data, &files, &threshold, &remote_error);
			}

			termPQExpBuffer(&command_output);

			switch (status)
			{
				case CHECK_STATUS_UNKNOWN:
					{
						if (runtime_options.force == false || remote_error == REMOTE_ERROR_DB_CONNECTION)
						{
							log_error(_("unable to check number of pending archive files on demotion candidate \"%s\""),
									  remote_node_record.node_name);

							if (remote_error == REMOTE_ERROR_DB_CONNECTION)
								log_detail(_("an error was encountered when attempting to connect to PostgreSQL on node \"%s\" (ID: %i)"),
										   remote_node_record.node_name,
										   remote_node_record.node_id);
							else
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

						log_warning(_("number of pending archive files on demotion candidate \"%s\" exceeds the critical threshold"),
									remote_node_record.node_name);
						log_detail(_("%i pending archive files (critical threshold: %i)"),
								   files, threshold);
						log_notice(_("-F/--force set, continuing with switchover"));
					}
					break;

				case CHECK_STATUS_WARNING:
					{
						log_warning(_("number of pending archive files on demotion candidate \"%s\" exceeds the warning threshold"),
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
		else if (lag_seconds == UNKNOWN_REPLICATION_LAG)
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
	 * if replication slots are required by demotion candidate and/or siblings,
	 * check the promotion candidate has sufficient free slots
	 */
	if (check_free_slots(&local_node_record, &sibling_nodes_stats, &dry_run_success) == false)
	{
		if (runtime_options.dry_run == false)
		{
			PQfinish(local_conn);
			exit(ERR_BAD_CONFIG);
		}
	}


	/*
	 * Attempt to pause all repmgrd instances, unless user explicitly
	 * specifies not to.
	 */
	if (runtime_options.repmgrd_no_pause == false)
	{
		NodeInfoListCell *cell = NULL;
		ItemList repmgrd_connection_errors = {NULL, NULL};
		int i = 0;
		int unreachable_node_count = 0;

		get_all_node_records(local_conn, &all_nodes);

		repmgrd_info = (RepmgrdInfo **) pg_malloc0(sizeof(RepmgrdInfo *) * all_nodes.node_count);

		for (cell = all_nodes.head; cell; cell = cell->next)
		{
			repmgrd_info[i] = pg_malloc0(sizeof(RepmgrdInfo));
			repmgrd_info[i]->node_id = cell->node_info->node_id;
			repmgrd_info[i]->pid = UNKNOWN_PID;
			repmgrd_info[i]->paused = false;
			repmgrd_info[i]->running = false;
			repmgrd_info[i]->pg_running = true;

			cell->node_info->conn = establish_db_connection_quiet(cell->node_info->conninfo);

			if (PQstatus(cell->node_info->conn) != CONNECTION_OK)
			{
				/*
				 * unable to connect; treat this as an error
				 */

				repmgrd_info[i]->pg_running = false;

				/*
				 * Only worry about unreachable nodes if they're marked as active
				 * in the repmgr metadata.
				 */
				if (cell->node_info->active == true)
				{
					unreachable_node_count++;

					item_list_append_format(&repmgrd_connection_errors,
											_("unable to connect to node \"%s\" (ID %i):\n%s"),
											cell->node_info->node_name,
											cell->node_info->node_id,
											PQerrorMessage(cell->node_info->conn));
				}

				PQfinish(cell->node_info->conn);
				cell->node_info->conn = NULL;

				i++;
				continue;
			}

			repmgrd_info[i]->running = repmgrd_is_running(cell->node_info->conn);
			repmgrd_info[i]->pid = repmgrd_get_pid(cell->node_info->conn);
			repmgrd_info[i]->paused = repmgrd_is_paused(cell->node_info->conn);

			if (repmgrd_info[i]->running == true)
				repmgrd_running_count++;

			i++;
		}

		if (unreachable_node_count > 0)
		{
			PQExpBufferData msg;
			PQExpBufferData detail;
			ItemListCell *cell;

			initPQExpBuffer(&msg);
			appendPQExpBuffer(&msg,
							  _("unable to connect to %i node(s), unable to pause all repmgrd instances"),
							  unreachable_node_count);

			initPQExpBuffer(&detail);

			for (cell = repmgrd_connection_errors.head; cell; cell = cell->next)
			{
				appendPQExpBuffer(&detail,
								  "  %s\n",
								  cell->string);
			}


			if (runtime_options.force == false)
			{
				log_error("%s", msg.data);
			}
			else
			{
				log_warning("%s", msg.data);
			}

			log_detail(_("following node(s) unreachable:\n%s"), detail.data);

			termPQExpBuffer(&msg);
			termPQExpBuffer(&detail);

			/* tell user about footgun */
			if (runtime_options.force == false)
			{
				log_hint(_("use -F/--force to continue anyway"));

				clear_node_info_list(&sibling_nodes);
				clear_node_info_list(&all_nodes);

				exit(ERR_SWITCHOVER_FAIL);
			}

		}

		/* pause repmgrd on all reachable nodes */
		if (repmgrd_running_count > 0)
		{
			i = 0;
			for (cell = all_nodes.head; cell; cell = cell->next)
			{

				/*
				 * Skip if node was unreachable
				 */
				if (repmgrd_info[i]->pg_running == false)
				{
					log_warning(_("node \"%s\" (ID %i) unreachable, unable to pause repmgrd"),
								cell->node_info->node_name,
								cell->node_info->node_id);
					i++;
					continue;
				}


				/*
				 * Skip if repmgrd not running on node
				 */
				if (repmgrd_info[i]->running == false)
				{
					log_warning(_("repmgrd not running on node \"%s\" (ID %i)"),
								cell->node_info->node_name,
								cell->node_info->node_id);
					i++;
					continue;
				}
				/*
				 * Skip if node is already paused. Note we won't unpause these, to
				 * leave the repmgrd instances in the cluster in the same state they
				 * were before the switchover.
				 */
				if (repmgrd_info[i]->paused == true)
				{
					PQfinish(cell->node_info->conn);
					cell->node_info->conn = NULL;
					i++;
					continue;
				}

				if (runtime_options.dry_run == true)
				{
					log_info(_("would pause repmgrd on node \"%s\" (ID %i)"),
							 cell->node_info->node_name,
							 cell->node_info->node_id);
				}
				else
				{
					/* XXX check result  */
					log_debug("pausing repmgrd on node \"%s\" (ID %i)",
							 cell->node_info->node_name,
							 cell->node_info->node_id);

					(void) repmgrd_pause(cell->node_info->conn, true);
				}

				PQfinish(cell->node_info->conn);
				cell->node_info->conn = NULL;
				i++;
			}
		}
		else
		{
			/* close all connections - we'll reestablish later */
			for (cell = all_nodes.head; cell; cell = cell->next)
			{
				if (cell->node_info->conn != NULL)
				{
					PQfinish(cell->node_info->conn);
					cell->node_info->conn = NULL;
				}
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
		appendPQExpBufferStr(&remote_command_str,
							 "node service --terse -LERROR --list-actions --action=stop");

	}
	else
	{
		log_notice(_("stopping current primary node \"%s\" (ID: %i)"),
				   remote_node_record.node_name,
				   remote_node_record.node_id);
		appendPQExpBufferStr(&remote_command_str,
							 "node service --action=stop --checkpoint");

		if (runtime_options.superuser[0] != '\0')
		{
			appendPQExpBuffer(&remote_command_str,
							  " --superuser=%s",
							  runtime_options.superuser);
		}
	}

	/* XXX handle failure */

	(void) remote_command(remote_host,
						  runtime_options.remote_user,
						  remote_command_str.data,
						  config_file_options.ssh_options,
						  &command_output);

	termPQExpBuffer(&remote_command_str);

	/*
	 * --dry-run ends here with display of command which would be used to shut
	 * down the remote server
	 */
	if (runtime_options.dry_run == true)
	{
		/* we use a buffer here as it will be modified by string_remove_trailing_newlines() */
		char		shutdown_command[MAXLEN] = "";

		strncpy(shutdown_command, command_output.data, MAXLEN);

		termPQExpBuffer(&command_output);

		string_remove_trailing_newlines(shutdown_command);

		log_info(_("following shutdown command would be run on node \"%s\":\n  \"%s\""),
				 remote_node_record.node_name,
				 shutdown_command);

		log_info(_("parameter \"shutdown_check_timeout\" is set to %i seconds"),
				 config_file_options.shutdown_check_timeout);

		clear_node_info_list(&sibling_nodes);

		key_value_list_free(&remote_config_files);

		if (dry_run_success == false)
		{
			log_error(_("prerequisites for executing STANDBY SWITCHOVER are *not* met"));
			log_hint(_("see preceding error messages"));
			exit(ERR_BAD_CONFIG);
		}

		log_info(_("prerequisites for executing STANDBY SWITCHOVER are met"));

		exit(SUCCESS);
	}

	termPQExpBuffer(&command_output);
	shutdown_success = false;

	/* loop for timeout waiting for current primary to stop */

	for (i = 0; i < config_file_options.shutdown_check_timeout; i++)
	{
		/* Check whether primary is available */
		PGPing		ping_res;

		log_info(_("checking for primary shutdown; %i of %i attempts (\"shutdown_check_timeout\")"),
				 i + 1, config_file_options.shutdown_check_timeout);

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
			appendPQExpBufferStr(&remote_command_str,
								 "node status --is-shutdown-cleanly");

			initPQExpBuffer(&command_output);

			command_success = remote_command(remote_host,
											 runtime_options.remote_user,
											 remote_command_str.data,
											 config_file_options.ssh_options,
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

		log_debug("sleeping 1 second until next check");
		sleep(1);
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
		log_detail("\n%s", PQerrorMessage(local_conn));
		PQfinish(local_conn);

		local_conn = establish_db_connection(config_file_options.conninfo, false);

		if (PQstatus(local_conn) != CONNECTION_OK)
		{
			log_error(_("unable to reconnect to local node \"%s\""),
					  local_node_record.node_name);
			exit(ERR_DB_CONN);
		}
		log_verbose(LOG_INFO, _("successfully reconnected to local node"));
	}

	init_replication_info(&replication_info);
	/*
	 * Compare standby's last WAL receive location with the primary's last
	 * checkpoint LSN. We'll loop for a while as it's possible the standby's
	 * walreceiver has not yet flushed all received WAL to disk.
	 */
	{
		bool notice_emitted = false;

		for (i = 0; i < config_file_options.wal_receive_check_timeout; i++)
		{
			get_replication_info(local_conn, STANDBY, &replication_info);
			if (replication_info.last_wal_receive_lsn >= remote_last_checkpoint_lsn)
				break;

			/*
			 * We'll only output this notice if it looks like we're going to have
			 * to wait for WAL to be flushed.
			 */
			if (notice_emitted == false)
			{
				log_notice(_("waiting up to %i seconds (parameter \"wal_receive_check_timeout\") for received WAL to flush to disk"),
						   config_file_options.wal_receive_check_timeout);

				notice_emitted = true;
			}

			log_info(_("sleeping %i of maximum %i seconds waiting for standby to flush received WAL to disk"),
					 i + 1, config_file_options.wal_receive_check_timeout);
			sleep(1);
		}
	}

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

	log_debug("local node last receive LSN is %X/%X, primary shutdown checkpoint LSN is %X/%X",
			  format_lsn(replication_info.last_wal_receive_lsn),
			  format_lsn(remote_last_checkpoint_lsn));

	/*
	 * Promote standby (local node).
	 *
	 * If PostgreSQL 12 or later, and -S/--superuser provided, we will provide
	 * a superuser connection so that pg_promote() can be used.
	 */

	if (PQserverVersion(local_conn) >= 120000 && superuser_conn != NULL)
	{
		_do_standby_promote_internal(superuser_conn);
	}
	else
	{
		_do_standby_promote_internal(local_conn);
	}


	/*
	 * If pg_rewind is requested, issue a checkpoint immediately after promoting
	 * the local node, as pg_rewind compares timelines on the basis of the value
	 * in pg_control, which is written at the first checkpoint, which might not
	 * occur immediately.
	 */
	if (runtime_options.force_rewind_used == true)
	{
		PGconn *checkpoint_conn = local_conn;
		if (superuser_conn != NULL)
		{
			checkpoint_conn = superuser_conn;
		}

		if (is_superuser_connection(checkpoint_conn, NULL) == true)
		{
			log_notice(_("issuing CHECKPOINT on node \"%s\" (ID: %i) "),
					   config_file_options.node_name,
					   config_file_options.node_id);
			checkpoint(superuser_conn);
		}
		else
		{
			log_warning(_("no superuser connection available, unable to issue CHECKPOINT"));
		}
	}

	/*
	 * Execute "repmgr node rejoin" to create recovery.conf and start the
	 * remote server. Additionally execute "pg_rewind", if required and
	 * requested.
	 */
	initPQExpBuffer(&node_rejoin_options);

	/*
	 * Don't wait for repmgr on the remote node to report the success
	 * of the rejoin operation - we'll check it from here.
	 */
	appendPQExpBufferStr(&node_rejoin_options,
						 " --no-wait");

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

		appendPQExpBufferStr(&node_rejoin_options,
						  " --force-rewind");

		if (runtime_options.force_rewind_path[0] != '\0')
		{
			appendPQExpBuffer(&node_rejoin_options,
							  "=%s",
							  runtime_options.force_rewind_path);
		}
		appendPQExpBufferStr(&node_rejoin_options,
							 " --config-files=");

		for (cell = remote_config_files.head; cell; cell = cell->next)
		{
			if (first_entry == false)
				appendPQExpBufferChar(&node_rejoin_options, ',');
			else
				first_entry = false;

			appendPQExpBufferStr(&node_rejoin_options, cell->key);
		}

		appendPQExpBufferChar(&node_rejoin_options, ' ');
	}

	key_value_list_free(&remote_config_files);

	initPQExpBuffer(&remote_command_str);
	make_remote_repmgr_path(&remote_command_str, &remote_node_record);

	/*
	 * Here we'll coerce the local node's connection string into
	 * "param=value" format, in case it's configured in URI format,
	 * to simplify escaping issues when passing the string to the
	 * remote node.
	 */
	{
		char	   *conninfo_normalized = normalize_conninfo_string(local_node_record.conninfo);

		appendPQExpBuffer(&remote_command_str,
						  "%s -d ",
						  node_rejoin_options.data);

		appendRemoteShellString(&remote_command_str,
								conninfo_normalized);

		appendPQExpBufferStr(&remote_command_str,
							 " node rejoin");

		pfree(conninfo_normalized);
	}

	termPQExpBuffer(&node_rejoin_options);

	log_debug("executing:\n  %s", remote_command_str.data);
	initPQExpBuffer(&command_output);

	command_success = remote_command(remote_host,
									 runtime_options.remote_user,
									 remote_command_str.data,
									 config_file_options.ssh_options,
									 &command_output);

	termPQExpBuffer(&remote_command_str);

	/* TODO: verify this node's record was updated correctly */

	if (command_success == false)
	{
		log_error(_("rejoin failed with error code %i"), r);

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
		standy_join_status join_success = check_standby_join(local_conn,
															 &local_node_record,
															 &remote_node_record);

		initPQExpBuffer(&event_details);

		switch (join_success) {
			case JOIN_FAIL_NO_PING:
				appendPQExpBuffer(&event_details,
								  _("node \"%s\" (ID: %i) promoted to primary, but demote node \"%s\" (ID: %i) did not beome available"),
								  config_file_options.node_name,
								  config_file_options.node_id,
								  remote_node_record.node_name,
								  remote_node_record.node_id);
				switchover_success = false;

				break;
			case JOIN_FAIL_NO_REPLICATION:
				appendPQExpBuffer(&event_details,
								  _("node \"%s\" (ID: %i) promoted to primary, but demote node \"%s\" (ID: %i) did not connect to the new primary"),
								  config_file_options.node_name,
								  config_file_options.node_id,
								  remote_node_record.node_name,
								  remote_node_record.node_id);
				switchover_success = false;
				break;
			case JOIN_SUCCESS:
				appendPQExpBuffer(&event_details,
								  _("node  \"%s\" (ID: %i) promoted to primary, node \"%s\" (ID: %i) demoted to standby"),
								  config_file_options.node_name,
								  config_file_options.node_id,
								  remote_node_record.node_name,
								  remote_node_record.node_id);
		}

		create_event_notification_extended(local_conn,
										   &config_file_options,
										   config_file_options.node_id,
										   "standby_switchover",
										   switchover_success,
										   event_details.data,
										   &event_info);
		if (switchover_success == true)
		{
			log_notice("%s", event_details.data);
		}
		else
		{
			log_error("%s", event_details.data);
		}
		termPQExpBuffer(&event_details);
	}

	termPQExpBuffer(&command_output);

	/*
	 * If --siblings-follow specified, attempt to make them follow the new
	 * primary
	 */
	if (runtime_options.siblings_follow == true && sibling_nodes.node_count > 0)
	{
		sibling_nodes_follow(&local_node_record, &sibling_nodes, &sibling_nodes_stats);
	}

	clear_node_info_list(&sibling_nodes);

	/*
	 * Clean up remote node (primary demoted to standby). It's possible that the node is
	 * still starting up, so poll for a while until we get a connection.
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
		log_detail(_("node \"%s\" (ID: %i) is now primary but node \"%s\" (ID: %i) is not reachable"),
				   local_node_record.node_name,
				   local_node_record.node_id,
				   remote_node_record.node_name,
				   remote_node_record.node_id);

		if (config_file_options.use_replication_slots == true)
		{
			log_hint(_("any inactive replication slots on the old primary will need to be dropped manually"));
		}
	}
	else
	{
		NodeAttached node_attached;

		/*
		 * We were able to connect to the former primary - attempt to drop
		 * this node's former replication slot, if it exists.
		 */
		if (config_file_options.use_replication_slots == true)
		{
			drop_replication_slot_if_exists(remote_conn,
											remote_node_record.node_id,
											local_node_record.slot_name);
		}


		/*
		 * Do a final check that the standby has connected - it's possible
		 * the standby became reachable but has not connected (or became disconnected).
		 */

		 node_attached = is_downstream_node_attached(local_conn,
													 remote_node_record.node_name,
													 NULL);
		if (node_attached == NODE_ATTACHED)
		{
			switchover_success = true;
			log_notice(_("switchover was successful"));
			log_detail(_("node \"%s\" is now primary and node \"%s\" is attached as standby"),
					   local_node_record.node_name,
					   remote_node_record.node_name);
		}
		else
		{
			log_notice(_("switchover is incomplete"));
			log_detail(_("node \"%s\" is now primary but node \"%s\" is not attached as standby"),
					   local_node_record.node_name,
					   remote_node_record.node_name);
			switchover_success = false;
		}

	}

	PQfinish(remote_conn);
	PQfinish(local_conn);

	/*
	 * Attempt to unpause all paused repmgrd instances, unless user explicitly
	 * specifies not to.
	 */
	if (runtime_options.repmgrd_no_pause == false)
	{
		if (repmgrd_running_count > 0)
		{
			ItemList repmgrd_unpause_errors = {NULL, NULL};
			NodeInfoListCell *cell = NULL;
			int i = 0;
			int error_node_count = 0;

			for (cell = all_nodes.head; cell; cell = cell->next)
			{

				if (repmgrd_info[i]->paused == true && runtime_options.repmgrd_force_unpause == false)
				{
					log_debug("repmgrd on node \"%s\" (ID %i) paused before switchover, --repmgrd-force-unpause not provided, not unpausing",
							  cell->node_info->node_name,
							  cell->node_info->node_id);

					i++;
					continue;
				}

				log_debug("unpausing repmgrd on node \"%s\" (ID %i)",
						  cell->node_info->node_name,
						  cell->node_info->node_id);

				cell->node_info->conn = establish_db_connection_quiet(cell->node_info->conninfo);

				if (PQstatus(cell->node_info->conn) == CONNECTION_OK)
				{
					if (repmgrd_pause(cell->node_info->conn, false) == false)
					{
						item_list_append_format(&repmgrd_unpause_errors,
												_("unable to unpause node \"%s\" (ID %i)"),
												cell->node_info->node_name,
												cell->node_info->node_id);
						error_node_count++;
					}
				}
				else
				{
					item_list_append_format(&repmgrd_unpause_errors,
											_("unable to connect to node \"%s\" (ID %i):\n%s"),
											cell->node_info->node_name,
											cell->node_info->node_id,
											PQerrorMessage(cell->node_info->conn));
					error_node_count++;
				}

				i++;
			}

			if (error_node_count > 0)
			{
				PQExpBufferData detail;
				ItemListCell *cell;

				initPQExpBuffer(&detail);

				for (cell = repmgrd_unpause_errors.head; cell; cell = cell->next)
				{
					appendPQExpBuffer(&detail,
									  "  %s\n",
									  cell->string);
				}

				log_warning(_("unable to unpause repmgrd on %i node(s)"),
							error_node_count);
				log_detail(_("errors encountered for following node(s):\n%s"), detail.data);
				log_hint(_("check node connection and status; unpause manually with \"repmgr service unpause\""));

				termPQExpBuffer(&detail);
			}
		}

		clear_node_info_list(&all_nodes);
	}

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
	char		cluster_size[MAXLEN];
	char	   *connstr = NULL;

	t_node_info upstream_node_record = T_NODE_INFO_INITIALIZER;
	RecordStatus record_status = RECORD_NOT_FOUND;
	ExtensionStatus extension_status = REPMGR_UNKNOWN;
	t_extension_versions extversions = T_EXTENSION_VERSIONS_INITIALIZER;

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

		exit(ERR_DB_CONN);
	}

	/*
	 * If a connection was established, perform some sanity checks on the
	 * provided upstream connection.
	 */

	source_server_version_num = check_server_version(source_conn, "primary", true, NULL);

	/*
	 * It's not essential to know the cluster size, but useful to sanity-check
	 * we can actually run a query before going any further.
	 */
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
	 * to be used as a standalone clone tool).
	 */

	extension_status = get_repmgr_extension_status(primary_conn, &extversions);

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

			if (extension_status == REPMGR_AVAILABLE)
			{
				log_error(_("repmgr extension is available but not installed in database \"%s\""),
						   param_get(&source_conninfo, "dbname"));
				log_hint(_("check that you are cloning from the database where \"repmgr\" is installed"));
			}
			else if (extension_status == REPMGR_UNAVAILABLE)
			{
				log_error(_("repmgr extension is not available on the upstream node"));
			}
			else if (extension_status == REPMGR_OLD_VERSION_INSTALLED)
			{
				log_error(_("an older version of the extension is installed on the upstream node"));
				log_detail(_("version %s is installed but newer version %s is available"),
						   extversions.installed_version,
						   extversions.default_version);
				log_hint(_("upgrade \"repmgr\" on the source node first"));
			}

			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}

		log_warning(_("repmgr extension not found on source node"));
	}
	else
	{
		/*
		 * If upstream is not a standby, retrieve its node records
		 * and attempt to connect to one; we'll then compare
		 * that node's system identifier to that of the source
		 * connection, to ensure we're cloning from a node which is
		 * part of the physical replication cluster. This is mainly
		 * to prevent cloning a standby from a witness server.
		 *
		 * Note that it doesn't matter if the node from the node record
		 * list is the same as the source node; also if the source node
		 * does not have any node records, there's not a lot we can do.
		 *
		 * This check will be only carried out on PostgreSQL 9.6 and
		 * later, as this is a precautionary check and we can retrieve the system
		 * identifier with a normal connection.
		 */

		if (runtime_options.dry_run == true)
		{
			log_info(_("\"repmgr\" extension is installed in database \"%s\""),
					 param_get(&source_conninfo, "dbname"));
		}

		if (get_recovery_type(source_conn) == RECTYPE_PRIMARY && PQserverVersion(source_conn) >= 90600)
		{
			uint64		source_system_identifier = system_identifier(source_conn);

			if (source_system_identifier != UNKNOWN_SYSTEM_IDENTIFIER)
			{
				NodeInfoList all_nodes = T_NODE_INFO_LIST_INITIALIZER;
				NodeInfoListCell *cell = NULL;
				get_all_node_records(source_conn, &all_nodes);

				log_debug("%i node records returned by source node", all_nodes.node_count);

				/* loop through its nodes table */

				for (cell = all_nodes.head; cell; cell = cell->next)
				{

					/* exclude the witness node, as its system identifier will be different, of course */
					if (cell->node_info->type == WITNESS)
						continue;

					cell->node_info->conn = establish_db_connection_quiet(cell->node_info->conninfo);
					if (PQstatus(cell->node_info->conn) == CONNECTION_OK)
					{
						uint64		test_system_identifier = system_identifier(cell->node_info->conn);
						PQfinish(cell->node_info->conn);
						cell->node_info->conn = NULL;

						if (test_system_identifier != UNKNOWN_SYSTEM_IDENTIFIER)
						{
							if (source_system_identifier != test_system_identifier)
							{
								log_error(_("source node's system identifier does not match other nodes in the replication cluster"));
								log_detail(_("source node's system identifier is %lu, replication cluster member \"%s\"'s system identifier is %lu"),
										   source_system_identifier,
										   cell->node_info->node_name,
										   test_system_identifier);
								log_hint(_("check that the source node is not a witness server"));
								PQfinish(source_conn);
								source_conn = NULL;

								exit(ERR_BAD_CONFIG);
							}
							/* identifiers match - our work here is done */
							break;
						}
					}
					else
					{
						PQfinish(cell->node_info->conn);
						cell->node_info->conn = NULL;
					}
				}
				clear_node_info_list(&all_nodes);
			}
		}
	}


	/*
	 * Check the local directory to see if it appears to be a PostgreSQL
	 * data directory.
	 *
	 * Note: a previous call to check_dir() will have checked whether it contains
	 * a running PostgreSQL instance.
	 */
	if (is_pg_dir(local_data_directory))
	{
		const char *msg = _("target data directory appears to be a PostgreSQL data directory");
		const char *hint = _("use -F/--force to overwrite the existing data directory");

		if (runtime_options.force == false && runtime_options.dry_run == false)
		{
			log_error("%s", msg);
			log_detail(_("target data directory is \"%s\""), local_data_directory);
			log_hint("%s", hint);
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}

		if (runtime_options.dry_run == true)
		{
			if (runtime_options.force == true)
			{
				log_warning("%s and will be overwritten", msg);
				log_detail(_("target data directory is \"%s\""), local_data_directory);

			}
			else
			{
				log_warning("%s", msg);
				log_detail(_("target data directory is \"%s\""), local_data_directory);
				log_hint("%s", hint);
			}
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
		t_node_info other_node_record = T_NODE_INFO_INITIALIZER;

		record_status = get_node_record(source_conn, upstream_node_id, &upstream_node_record);
		if (record_status == RECORD_FOUND)
		{
			t_conninfo_param_list upstream_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;
			char	   *upstream_conninfo_user;

			initialize_conninfo_params(&upstream_conninfo, false);
			parse_conninfo_string(upstream_node_record.conninfo, &upstream_conninfo, NULL, false);

			strncpy(recovery_conninfo_str, upstream_node_record.conninfo, MAXLEN);
			strncpy(upstream_repluser, upstream_node_record.repluser, NAMEDATALEN);

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
		 * Check that there's no existing node record with the same name but
		 * different ID.
		 */
		record_status = get_node_record_by_name(source_conn, config_file_options.node_name, &other_node_record);

		if (record_status == RECORD_FOUND && other_node_record.node_id != config_file_options.node_id)
		{
			log_error(_("another node (ID: %i) already exists with node_name \"%s\""),
					  other_node_record.node_id,
					  config_file_options.node_name);
			PQfinish(source_conn);
			exit(ERR_BAD_CONFIG);
		}
	}

	/* Check the source node is configured sufficiently to be able to clone from */
	check_upstream_config(source_conn, source_server_version_num, &upstream_node_record, true);

	/*
	 * Work out which users need to perform which tasks.
	 *
	 * Here we'll check the qualifications of the repmgr user as we have the
	 * connection open; replication and superuser connections will be opened
	 * when required and any errors will be raised at that point.
	 */

	/*
	 * If the user wants to copy configuration files located outside the
	 * data directory, we'll need to be able to query the upstream node's data
	 * directory location, which is available only to superusers or members
	 * of the appropriate role.
	 */
	if (runtime_options.copy_external_config_files == true)
	{
		/*
		 * This will check if the user is superuser or (from Pg10) is a member
		 * of "pg_read_all_settings"/"pg_monitor"
		 */
		if (connection_has_pg_monitor_role(source_conn, "pg_read_all_settings") == true)
		{
			SettingsUser = REPMGR_USER;
		}
		else if (runtime_options.superuser[0] != '\0')
		{
			SettingsUser = SUPERUSER;
		}
		else
		{
			log_error(_("--copy-external-config-files requires a user with permission to read the data directory on the source node"));

			if (PQserverVersion(source_conn) >= 100000)
			{
				log_hint(_("the repmgr user must be superuser or member of role \"pg_monitor\" or \"pg_read_all_settings\", or a superuser provided with -S/--superuser"));
			}
			else
			{
				log_hint(_("the repmgr user must be superuser, or a superuser provided with -S/--superuser"));
			}

			exit(ERR_BAD_CONFIG);
		}
	}

	/*
	 * To create replication slots, we'll need a user with the REPLICATION
	 * privilege, or a superuser.
	 */
	if (config_file_options.use_replication_slots == true)
	{
	}
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
 * TODO:
 *  - check user is qualified to perform base backup
 */

static bool
check_upstream_config(PGconn *conn, int server_version_num, t_node_info *upstream_node_record, bool exit_on_error)
{
	int			i;
	bool		config_ok = true;
	char	   *wal_error_message = NULL;
	t_basebackup_options backup_options = T_BASEBACKUP_OPTIONS_INITIALIZER;
	bool		backup_options_ok = true;
	ItemList	backup_option_errors = {NULL, NULL};
	bool		wal_method_stream = true;
	standy_clone_mode mode;
	bool		pg_setting_ok;

	/*
	 * Detecting the intended cloning mode
	 */
	mode = get_standby_clone_mode();

	/*
	 * Parse "pg_basebackup_options", if set, to detect whether --wal-method
	 * has been set to something other than `stream` (i.e. `fetch`), as this
	 * will influence some checks
	 */

	backup_options_ok = parse_pg_basebackup_options(config_file_options.pg_basebackup_options,
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

	if (strlen(backup_options.wal_method) && strcmp(backup_options.wal_method, "stream") != 0)
		wal_method_stream = false;

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

	if (config_file_options.use_replication_slots == true)
	{
		pg_setting_ok = get_pg_setting_int(conn, "max_replication_slots", &i);

		if (pg_setting_ok == false || i < 1)
		{
			if (pg_setting_ok == true)
			{
				log_error(_("parameter \"max_replication_slots\" must be set to at least 1 to enable replication slots"));
				log_detail(_("current value is %i"), i);
				log_hint(_("\"max_replication_slots\" should be set to at least the number of expected standbys"));
				if (exit_on_error == true)
				{
					PQfinish(conn);
					exit(ERR_BAD_CONFIG);
				}

				config_ok = false;
			}
		}

		if (pg_setting_ok == true && i > 0 && runtime_options.dry_run == true)
		{
			log_info(_("parameter \"max_replication_slots\" set to %i"), i);
		}
	}
	/*
	 * physical replication slots not available or not requested - check if
	 * there are any circumstances where "wal_keep_segments" should be set
	 */
	else if (mode != barman)
	{
		bool		check_wal_keep_segments = false;

		/*
		 * A non-zero "wal_keep_segments" value will almost certainly be
		 * required if pg_basebackup is being used with --xlog-method=fetch,
		 * *and* no restore command has been specified
		 */
		if (wal_method_stream == false
			&& strcmp(config_file_options.restore_command, "") == 0)
		{
			check_wal_keep_segments = true;
		}

		if (check_wal_keep_segments == true)
		{
			const char *wal_keep_parameter_name = "wal_keep_size";

			if (PQserverVersion(conn) < 130000)
				wal_keep_parameter_name = "wal_keep_segments";

			pg_setting_ok = get_pg_setting_int(conn, wal_keep_parameter_name, &i);

			if (pg_setting_ok == false || i < 1)
			{
				if (pg_setting_ok == true)
				{
					log_error(_("parameter \"%s\" on the upstream server must be be set to a non-zero value"),
							  wal_keep_parameter_name);
					log_hint(_("Choose a value sufficiently high enough to retain enough WAL "
							   "until the standby has been cloned and started.\n "
							   "Alternatively set up WAL archiving using e.g. PgBarman and configure "
							   "'restore_command' in repmgr.conf to fetch WALs from there."));
					log_hint(_("In PostgreSQL 9.4 and later, replication slots can be used, which "
							   "do not require \"%s\" to be set "
							   "(set parameter \"use_replication_slots\" in repmgr.conf to enable)\n"),
							 wal_keep_parameter_name);
				}

				if (exit_on_error == true)
				{
					PQfinish(conn);
					exit(ERR_BAD_CONFIG);
				}

				config_ok = false;
			}

			if (pg_setting_ok == true && i > 0 && runtime_options.dry_run == true)
			{
				log_info(_("parameter \"%s\" set to %i"),
						   wal_keep_parameter_name,
						   i);
			}
		}
	}


	if (config_file_options.use_replication_slots == false)
	{
		log_info(_("replication slot usage not requested;  no replication slot will be set up for this standby"));
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
			log_error(_("parameter \"hot_standby\" must be set to \"on\""));
		}

		if (exit_on_error == true)
		{
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		config_ok = false;
	}

	pg_setting_ok = get_pg_setting_int(conn, "max_wal_senders", &i);

	if (pg_setting_ok == false || i < 1)
	{
		if (pg_setting_ok == true)
		{
			log_error(_("parameter \"max_wal_senders\" must be set to be at least %i"), i);
			log_hint(_("\"max_wal_senders\" should be set to at least the number of expected standbys"));
		}

		if (exit_on_error == true)
		{
			PQfinish(conn);
			exit(ERR_BAD_CONFIG);
		}

		config_ok = false;
	}
	else if (pg_setting_ok == true && i > 0 && runtime_options.dry_run == true)
	{
		log_info(_("parameter \"max_wal_senders\" set to %i"), i);
	}

	/*
	 * If using pg_basebackup, ensure sufficient replication connections can
	 * be made. There's no guarantee they'll still be available by the time
	 * pg_basebackup is executed, but there's nothing we can do about that.
	 * This check is mainly intended to warn about missing replication permissions
	 * and/or lack of available walsenders.
	 */
	if (mode == pg_basebackup)
	{

		PGconn	  **connections;
		int			i;
		int			available_wal_senders;
		int			min_replication_connections = 1;
		int			possible_replication_connections = 0;
		t_conninfo_param_list repl_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;


		/*
		 * work out how many replication connections are required (1 or 2)
		 */

		if (wal_method_stream == true)
			min_replication_connections += 1;

		log_notice(_("checking for available walsenders on the source node (%i required)"),
				   min_replication_connections);

		/*
		 * check how many free walsenders are available
		 */
		get_node_replication_stats(conn, upstream_node_record);

		available_wal_senders = upstream_node_record->max_wal_senders -
			upstream_node_record->attached_wal_receivers;

		if (available_wal_senders < min_replication_connections)
		{
			log_error(_("insufficient free walsenders on the source node"));
			log_detail(_("%i free walsenders required, %i free walsenders available"),
					   min_replication_connections,
					   available_wal_senders);
			log_hint(_("increase \"max_wal_senders\" on the source node by at least %i"),
					 (upstream_node_record->attached_wal_receivers + min_replication_connections) - upstream_node_record->max_wal_senders);

			if (exit_on_error == true)
			{
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}
		}
		else if (runtime_options.dry_run == true)
		{
			log_info(_("sufficient walsenders available on the source node"));
			log_detail(_("%i required, %i available"),
					   min_replication_connections,
					   available_wal_senders);
		}


		/*
		 * Sufficient free walsenders appear to be available, check if
		 * we can connect to them. We check that the required number
		 * of connections can be made e.g. to rule out a very restrictive
		 * "CONNECTION LIMIT" setting.
		 */

		log_notice(_("checking replication connections can be made to the source server (%i required)"),
				   min_replication_connections);

		/*
		 * Make a copy of the connection parameter arrays, and append
		 * "replication".
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
		else if (upstream_node_record->repluser[0] != '\0')
		{
			param_set(&repl_conninfo, "user", upstream_node_record->repluser);
		}

		if (strcmp(param_get(&repl_conninfo, "user"), upstream_user) != 0)
		{
			param_set(&repl_conninfo, "dbname", "replication");
		}

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

			log_error(_("unable to establish necessary replication connections"));
			log_hint(_("check replication permissions on the source server"));

			if (exit_on_error == true)
			{
				PQfinish(conn);
				exit(ERR_BAD_CONFIG);
			}
		}

		if (runtime_options.dry_run == true)
		{
			log_info(_("required number of replication connections could be made to the source server"));
			log_detail(_("%i replication connections required"),
					   min_replication_connections);
		}
		else
		{
			log_verbose(LOG_INFO, _("sufficient replication connections could be made to the source server (%i required)"),
						min_replication_connections);
		}
	}

	/*
	 * Finally, add some checks for recommended settings
	 */

	{
		bool data_checksums = false;
		bool wal_log_hints = false;

		/* data_checksums available from PostgreSQL 9.3; can be read by any user */
		if (get_pg_setting_bool(conn, "data_checksums", &data_checksums) == false)
		{
			/* highly unlikely this will happen */
			log_error(_("unable to determine value for \"data_checksums\""));
			exit(ERR_BAD_CONFIG);
		}

		/* wal_log_hints available from PostgreSQL 9.4; can be read by any user */
		if (get_pg_setting_bool(conn, "wal_log_hints", &wal_log_hints) == false)
		{
			/* highly unlikely this will happen */
			log_error(_("unable to determine value for \"wal_log_hints\""));
			exit(ERR_BAD_CONFIG);
		}

		if (data_checksums == false && wal_log_hints == false)
		{
			log_warning(_("data checksums are not enabled and \"wal_log_hints\" is \"off\""));
			log_detail(_("pg_rewind requires \"wal_log_hints\" to be enabled"));
		}
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
initialise_direct_clone(t_node_info *local_node_record, t_node_info *upstream_node_record)
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
		PQExpBufferData event_details;

		initPQExpBuffer(&event_details);

		if (create_replication_slot(source_conn, local_node_record->slot_name, upstream_node_record, &event_details) == false)
		{
			log_error("%s", event_details.data);

			create_event_notification(primary_conn,
									  &config_file_options,
									  config_file_options.node_id,
									  "standby_clone",
									  false,
									  event_details.data);

			PQfinish(source_conn);

			exit(ERR_DB_QUERY);
		}

		termPQExpBuffer(&event_details);

		log_verbose(LOG_INFO,
					_("replication slot \"%s\" created on source node"),
					local_node_record->slot_name);
	}

	return;
}


static int
run_basebackup(t_node_info *node_record)
{
	PQExpBufferData params;
	PQExpBufferData script;

	int			r = SUCCESS;

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
		appendPQExpBufferStr(&params, " -c fast");
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
	if (!strlen(backup_options.wal_method))
	{
		appendPQExpBufferStr(&params, " -X stream");
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
		if (strlen(backup_options.slot) || (strlen(backup_options.wal_method) && strcmp(backup_options.wal_method, "stream") != 0))
		{
			slot_add = false;
		}

		if (slot_add == true)
		{
			appendPQExpBuffer(&params, " -S %s", node_record->slot_name);
		}
	}

	initPQExpBuffer(&script);
	make_pg_path(&script, "pg_basebackup");

	appendPQExpBuffer(&script,
					  " -l \"repmgr base backup\" %s %s",
					  params.data,
					  config_file_options.pg_basebackup_options);

	termPQExpBuffer(&params);

	log_info(_("executing:\n  %s"), script.data);

	/*
	 * As of 9.4, pg_basebackup only ever returns 0 or 1
	 */

	r = system(script.data);

	termPQExpBuffer(&script);

	if (r != 0)
		return ERR_BAD_BASEBACKUP;

	/* check connections are still available */
	(void)connection_ping_reconnect(primary_conn);

	if (source_conn != primary_conn)
		(void)connection_ping_reconnect(source_conn);

	/*
	 * If replication slots in use, check the created slot is on the correct
	 * node; the slot will initially get created on the source node, and will
	 * need to be dropped and recreated on the actual upstream node if these
	 * differ.
	 */
	if (config_file_options.use_replication_slots && upstream_node_id != UNKNOWN_NODE_ID)
	{
		t_node_info upstream_node_record = T_NODE_INFO_INITIALIZER;
		t_replication_slot slot_info = T_REPLICATION_SLOT_INITIALIZER;
		RecordStatus record_status = RECORD_NOT_FOUND;
		bool slot_exists_on_upstream = false;

		record_status = get_node_record(source_conn, upstream_node_id, &upstream_node_record);

		/*
		 * If there's no upstream record, there's no point in trying to create
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

			upstream_conn = establish_db_connection(upstream_node_record.conninfo, false);

			/*
			 * It's possible the upstream node is not yet running, in which case we'll
			 * have to rely on the user taking action to create the slot
			 */
			if (PQstatus(upstream_conn) != CONNECTION_OK)
			{
				log_warning(_("unable to connect to upstream node to create replication slot"));
				/*
				 * TODO: if slot creation also handled by "standby register", update warning
				 */
				log_hint(_("you may need to create the replication slot manually"));
			}
			else
			{
				record_status = get_slot_record(upstream_conn, node_record->slot_name, &slot_info);

				if (record_status == RECORD_FOUND)
				{
					log_verbose(LOG_INFO,
								_("replication slot \"%s\" already exists on upstream node %i"),
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

					initPQExpBuffer(&event_details);
					if (create_replication_slot(upstream_conn, node_record->slot_name, &upstream_node_record, &event_details) == false)
					{
						log_error("%s", event_details.data);

						create_event_notification(primary_conn,
												  &config_file_options,
												  config_file_options.node_id,
												  "standby_clone",
												  false,
												  event_details.data);

						PQfinish(source_conn);

						exit(ERR_DB_QUERY);
					}

					termPQExpBuffer(&event_details);
				}

				PQfinish(upstream_conn);
			}
		}

		if (slot_info.active == false)
		{
			if (slot_exists_on_upstream == false)
			{

				/* delete slot on source server */

				if (drop_replication_slot_if_exists(source_conn, UNKNOWN_NODE_ID, node_record->slot_name) == true)
				{
					log_notice(_("replication slot \"%s\" deleted on source node"),
							   node_record->slot_name);
				}
				else
				{
					log_error(_("unable to delete replication slot \"%s\" on source node"),
							  node_record->slot_name);
				}
			}
		}

		/*
		 * if replication slot is still active (shouldn't happen), emit a
		 * warning
		 */
		else
		{
			log_warning(_("replication slot \"%s\" is still active on source node"),
						node_record->slot_name);
		}
	}

	return SUCCESS;
}


/*
 * Perform a filesystem backup using rsync.
 *
 * From repmgr 4 this is only used for Barman backups.
 */
static int
run_file_backup(t_node_info *local_node_record)
{
	int			r = SUCCESS,
				i;

	char		command[MAXLEN] = "";
	char		filename[MAXLEN] = "";
	char		buf[MAXLEN] = "";
	char		basebackups_directory[MAXLEN] = "";
	char		backup_id[MAXLEN] = "";
	TablespaceDataList tablespace_list = {NULL, NULL};
	TablespaceDataListCell *cell_t = NULL;

	PQExpBufferData tablespace_map;
	bool		tablespace_map_rewrite = false;

	/* For the foreseeable future, no other modes are supported */
	Assert(mode == barman);
	if (mode == barman)
	{
		t_basebackup_options backup_options = T_BASEBACKUP_OPTIONS_INITIALIZER;

		Assert(source_server_version_num != UNKNOWN_SERVER_VERSION_NUM);

		/*
		 * Parse the pg_basebackup_options provided in repmgr.conf - we need to
		 * check if --waldir/--xlogdir was provided.
		 */
		parse_pg_basebackup_options(config_file_options.pg_basebackup_options,
									&backup_options,
									source_server_version_num,
									NULL);
		/*
		 * Locate Barman's base backups directory
		 */

		get_barman_property(basebackups_directory, "basebackups_directory", local_repmgr_tmp_directory);

		/*
		 * Read the list of backup files into a local file. In the process:
		 *
		 * - determine the backup ID
		 * - check, and remove, the prefix
		 * - detect tablespaces
		 * - filter files in one list per tablespace
		 */
		{
			FILE	   *fi;		/* input stream */
			FILE	   *fd;		/* output for data.txt */
			char		prefix[MAXLEN] = "";
			char		output[MAXLEN] = "";
			int			n = 0;
			char	   *p = NULL,
					   *q = NULL;

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
					log_error("unexpected output from \"barman list-files\"");
					log_detail("%s", output);
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

					if (p == NULL)
					{
						log_error("unexpected output from \"barman list-files\"");
						log_detail("%s", output);
						exit(ERR_BARMAN);
					}

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

					(void) local_command(command,
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
						if (cell_t->fptr == NULL)
						{
							maxlen_snprintf(filename, "%s/%s.txt", local_repmgr_tmp_directory, cell_t->oid);
							cell_t->fptr = fopen(filename, "w");
							if (cell_t->fptr == NULL)
							{
								log_error("cannot open file: %s", filename);
								exit(ERR_INTERNAL);
							}
						}
						fputs(q + 1, cell_t->fptr);
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
				/* Present in all versions from  9.3 */
				"pg_notify", "pg_serial", "pg_snapshots", "pg_stat", "pg_stat_tmp",
				"pg_subtrans", "pg_tblspc", "pg_twophase",
				/* Present from at least 9.3, but removed in 10 */
				"pg_xlog",
				/* Array delimiter */
				0
			};

			/*
			 * This array determines the major version each of the above directories
			 * first appears in; or if the value is negative, which from major version
			 * the directory does not appear in.
			 */
			const int	vers[] = {
				100000,
				90500,
				90400, 90400, 90400, 90400, 90400,
				0, 0, 0, 0, 0,
				0, 0, 0,
				-100000
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

				/*
				 * If --waldir/--xlogdir specified in "pg_basebackup_options",
				 * create a symlink rather than make a directory.
				 */
				if (strcmp(dirs[i], "pg_wal") == 0 || strcmp(dirs[i], "pg_xlog") == 0)
				{
					if (backup_options.waldir[0] != '\0')
					{
						if (create_pg_dir(backup_options.waldir, false) == false)
						{
							/* create_pg_dir() will log specifics */
							log_error(_("unable to create an empty directory for WAL files"));
							log_hint(_("see preceding error messages"));
							exit(ERR_BAD_CONFIG);
						}

						if (symlink(backup_options.waldir, filename) != 0)
						{
							log_error(_("could not create symbolic link \"%s\""), filename);
							exit(ERR_BAD_CONFIG);
						}
						continue;
					}
				}

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
			create_pg_dir(tblspc_dir_dest, false);

			if (cell_t->fptr != NULL)	/* cell_t->fptr == NULL iff the tablespace is
										 * empty */
			{
				/* close the file to ensure the contents are flushed to disk */
				fclose(cell_t->fptr);

				maxlen_snprintf(command,
								"rsync --progress -a --files-from=%s/%s.txt %s:%s/%s/%s %s",
								local_repmgr_tmp_directory,
								cell_t->oid,
								config_file_options.barman_host,
								basebackups_directory,
								backup_id,
								cell_t->oid,
								tblspc_dir_dest);
				(void) local_command(command,
									 NULL);
				maxlen_snprintf(filename,
								"%s/%s.txt",
								local_repmgr_tmp_directory,
								cell_t->oid);
				unlink(filename);
			}
		}


		/*
		 * If a valid mapping was provided for this tablespace, arrange for it
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
			fclose(tablespace_map_file);

			log_error(_("unable to write to tablespace_map file \"%s\""), tablespace_map_filename.data);

			r = ERR_BAD_BASEBACKUP;
			goto stop_backup;
		}

		fclose(tablespace_map_file);

		termPQExpBuffer(&tablespace_map_filename);
		termPQExpBuffer(&tablespace_map);
	}

stop_backup:

	if (mode == barman)
	{
		/*
		 * In Barman mode, remove local_repmgr_tmp_directory,
		 * which contains various temporary files containing Barman metadata.
		 */
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


				/* check connections are still available */
				(void)connection_ping_reconnect(primary_conn);

				if (source_conn != primary_conn)
					(void)connection_ping_reconnect(source_conn);

				(void)connection_ping_reconnect(source_conn);

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
						record_status = get_slot_record(upstream_conn, local_node_record->slot_name, &slot_info);

						if (record_status == RECORD_FOUND)
						{
							log_verbose(LOG_INFO,
										_("replication slot \"%s\" aleady exists on upstream node %i"),
										local_node_record->slot_name,
										upstream_node_id);
						}
						else
						{
							PQExpBufferData errmsg;
							bool success;

							initPQExpBuffer(&errmsg);
							success = create_replication_slot(upstream_conn,
															  local_node_record->slot_name,
															  &upstream_node_record,
															  &errmsg);
							if (success == false)
							{
								log_error(_("unable to create replication slot \"%s\" on upstream node %i"),
										  local_node_record->slot_name,
										  upstream_node_id);
								log_detail("%s", errmsg.data);
								slot_warning = true;
							}
							else
							{
								log_notice(_("replication slot \"%s\" created on upstream node \"%s\" (ID: %i)"),
										   local_node_record->slot_name,
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
						 local_node_record->slot_name,
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
get_tablespace_data_barman(char *tablespace_data_barman,
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
					"grep \"^[[:space:]]%s:\" %s/show-server.txt",
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



/*
 * Creates recovery configuration for a standby.
 *
 * A database connection pointer is required for escaping primary_conninfo
 * parameters. When cloning from Barman and --no-upstream-connection supplied,
 * this might not be available.
 */
static bool
create_recovery_file(t_node_info *node_record, t_conninfo_param_list *primary_conninfo, int server_version_num, char *dest, bool as_file)
{
	PQExpBufferData recovery_file_buf;
	PQExpBufferData primary_conninfo_buf;
	char		recovery_file_path[MAXPGPATH] = "";
	FILE	   *recovery_file;
	mode_t		um;

	KeyValueList recovery_config = {NULL, NULL};
	KeyValueListCell *cell = NULL;

	initPQExpBuffer(&primary_conninfo_buf);

	/* standby_mode = 'on' (Pg 11 and earlier) */
	if (server_version_num < 120000)
	{
		key_value_list_set(&recovery_config,
						   "standby_mode", "on");
	}

	/* primary_conninfo = '...' */
	write_primary_conninfo(&primary_conninfo_buf, primary_conninfo);
	key_value_list_set(&recovery_config,
					   "primary_conninfo", primary_conninfo_buf.data);

	/* recovery_target_timeline = 'latest' */
	key_value_list_set(&recovery_config,
					   "recovery_target_timeline", "latest");


	/* recovery_min_apply_delay = ... (optional) */
	if (config_file_options.recovery_min_apply_delay_provided == true)
	{
		key_value_list_set(&recovery_config,
						   "recovery_min_apply_delay", config_file_options.recovery_min_apply_delay);
	}

	/* primary_slot_name = '...' (optional, for 9.4 and later) */
	if (config_file_options.use_replication_slots)
	{
		key_value_list_set(&recovery_config,
						   "primary_slot_name", node_record->slot_name);
	}

	/*
	 * If restore_command is set, we use it as restore_command in
	 * recovery.conf
	 */
	if (config_file_options.restore_command[0] != '\0')
	{
		char	   *escaped = escape_recovery_conf_value(config_file_options.restore_command);

		key_value_list_set(&recovery_config,
						  "restore_command", escaped);
		free(escaped);
	}

	/* archive_cleanup_command (optional) */
	if (config_file_options.archive_cleanup_command[0] != '\0')
	{
		char	   *escaped = escape_recovery_conf_value(config_file_options.archive_cleanup_command);

		key_value_list_set(&recovery_config,
						  "archive_cleanup_command", escaped);
		free(escaped);
	}




	if (as_file == false)
	{
		/* create file in buffer */
		initPQExpBuffer(&recovery_file_buf);

		for (cell = recovery_config.head; cell; cell = cell->next)
		{
			appendPQExpBuffer(&recovery_file_buf,
							  "%s = '%s'\n",
							  cell->key, cell->value);
		}

		maxlen_snprintf(dest, "%s", recovery_file_buf.data);

		termPQExpBuffer(&recovery_file_buf);

		return true;
	}


	/*
	 * PostgreSQL 12 and later: modify postgresql.auto.conf
	 *
	 */
	if (server_version_num >= 120000)
	{

		if (modify_auto_conf(dest, &recovery_config) == false)
		{
			return false;
		}

		if (write_standby_signal() == false)
		{
			return false;
		}

		return true;
	}

	/*
	 * PostgreSQL 11 and earlier: write recovery.conf
	 */
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

	for (cell = recovery_config.head; cell; cell = cell->next)
	{
		initPQExpBuffer(&recovery_file_buf);
		appendPQExpBuffer(&recovery_file_buf,
						  "%s = '%s'\n",
						  cell->key, cell->value);

		log_debug("recovery.conf line: %s", recovery_file_buf.data);

		if (fputs(recovery_file_buf.data, recovery_file) == EOF)
		{
			log_error(_("unable to write to recovery file at \"%s\""), recovery_file_path);
			fclose(recovery_file);
			termPQExpBuffer(&recovery_file_buf);
			return false;
		}

		termPQExpBuffer(&recovery_file_buf);
	}


	fclose(recovery_file);

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
			appendPQExpBufferStr(&conninfo_buf, " application_name=");
			appendConnStrVal(&conninfo_buf, config_file_options.node_name);
		}
		else
		{
			appendPQExpBufferStr(&conninfo_buf, " application_name=repmgr");
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
				appendPQExpBufferStr(&conninfo_buf, " password=");
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
			appendPQExpBufferStr(&conninfo_buf, " passfile=");
			appendConnStrVal(&conninfo_buf, config_file_options.passfile);
		}
	}

	escaped = escape_recovery_conf_value(conninfo_buf.data);

	appendPQExpBufferStr(dest, escaped);

	free(escaped);
	free_conninfo_params(&env_conninfo);
	termPQExpBuffer(&conninfo_buf);
}


/*
 * For "standby promote" and "standby follow", check for sibling nodes.
 * If "--siblings-follow" was specified, fill the provided SiblingNodeStats
 * struct with some aggregate info about the nodes for later
 * decision making.
 */
static bool
check_sibling_nodes(NodeInfoList *sibling_nodes, SiblingNodeStats *sibling_nodes_stats)
{
	char		host[MAXLEN] = "";
	NodeInfoListCell *cell;
	int			r;

	/*
	 * If --siblings-follow not specified, warn about any extant
	 * siblings which will not follow the new primary
	 */

	if (runtime_options.siblings_follow == false)
	{
		if (sibling_nodes->node_count > 0)
		{
			PQExpBufferData nodes;
			NodeInfoListCell *cell;

			initPQExpBuffer(&nodes);

			for (cell = sibling_nodes->head; cell; cell = cell->next)
			{
				appendPQExpBuffer(&nodes,
								  "  %s (node ID: %i",
								  cell->node_info->node_name,
								  cell->node_info->node_id);

				if (cell->node_info->type == WITNESS)
				{
					appendPQExpBufferStr(&nodes,
										 ", witness server");
				}
				appendPQExpBufferChar(&nodes,
									  ')');
				if (cell->next)
					appendPQExpBufferStr(&nodes, "\n");
			}

			log_warning(_("%i sibling nodes found, but option \"--siblings-follow\" not specified"),
						sibling_nodes->node_count);
			log_detail(_("these nodes will remain attached to the current primary:\n%s"), nodes.data);

			termPQExpBuffer(&nodes);
		}

		return true;
	}

	log_verbose(LOG_INFO, _("%i active sibling nodes found"),
				sibling_nodes->node_count);

	if (sibling_nodes->node_count == 0)
	{
		log_warning(_("option \"--sibling-nodes\" specified, but no sibling nodes exist"));
		return true;
	}

	for (cell = sibling_nodes->head; cell; cell = cell->next)
	{
		/* get host from node record */
		get_conninfo_value(cell->node_info->conninfo, "host", host);
		r = test_ssh_connection(host, runtime_options.remote_user);

		if (r != 0)
		{
			cell->node_info->reachable = false;
			sibling_nodes_stats->unreachable_sibling_node_count++;
		}
		else
		{
			cell->node_info->reachable = true;
			sibling_nodes_stats->reachable_sibling_node_count++;
			sibling_nodes_stats->min_required_wal_senders++;

			if (cell->node_info->slot_name[0] != '\0')
			{
				sibling_nodes_stats->reachable_sibling_nodes_with_slot_count++;
				sibling_nodes_stats->min_required_free_slots++;
			}
		}
	}

	if (sibling_nodes_stats->unreachable_sibling_node_count > 0)
	{
		if (runtime_options.force == false)
		{
			log_error(_("%i of %i sibling nodes unreachable via SSH:"),
					  sibling_nodes_stats->unreachable_sibling_node_count,
					  sibling_nodes->node_count);
		}
		else
		{
			log_warning(_("%i of %i sibling nodes unreachable via SSH:"),
						sibling_nodes_stats->unreachable_sibling_node_count,
						sibling_nodes->node_count);
		}

		/* display list of unreachable sibling nodes */
		for (cell = sibling_nodes->head; cell; cell = cell->next)
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
			return false;
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

	return true;
}


static bool
check_free_wal_senders(int available_wal_senders, SiblingNodeStats *sibling_nodes_stats, bool *dry_run_success)
{
	if (available_wal_senders < sibling_nodes_stats->min_required_wal_senders)
	{
		if (runtime_options.force == false || runtime_options.dry_run == true)
		{
			log_error(_("insufficient free walsenders on promotion candidate"));
			log_detail(_("at least %i walsenders required but only %i free walsenders on promotion candidate"),
					   sibling_nodes_stats->min_required_wal_senders,
					   available_wal_senders);
			log_hint(_("increase parameter \"max_wal_senders\" or use -F/--force to proceed in any case"));

			if (runtime_options.dry_run == true)
			{
				*dry_run_success = false;
			}
			else
			{
				return false;
			}
		}
		else
		{
			log_warning(_("insufficient free walsenders on promotion candidate"));
			log_detail(_("at least %i walsenders required but only %i free walsender(s) on promotion candidate"),
					   sibling_nodes_stats->min_required_wal_senders,
					   available_wal_senders);
			return false;
		}
	}
	else
	{
		if (runtime_options.dry_run == true)
		{
			log_info(_("%i walsenders required, %i available"),
					 sibling_nodes_stats->min_required_wal_senders,
					 available_wal_senders);
		}
	}

	return true;
}


static bool
check_free_slots(t_node_info *local_node_record, SiblingNodeStats *sibling_nodes_stats, bool *dry_run_success)
{
	if (sibling_nodes_stats->min_required_free_slots > 0 )
	{
		int available_slots = local_node_record->max_replication_slots -
			local_node_record->active_replication_slots;

		log_debug("minimum of %i free slots (%i for siblings) required; %i available",
				  sibling_nodes_stats->min_required_free_slots,
				  sibling_nodes_stats->reachable_sibling_nodes_with_slot_count,
				  available_slots);

		if (available_slots < sibling_nodes_stats->min_required_free_slots)
		{
			if (runtime_options.force == false || runtime_options.dry_run == true)
			{
				log_error(_("insufficient free replication slots to attach all nodes"));
				log_detail(_("at least %i additional replication slots required but only %i free slots available on promotion candidate"),
						   sibling_nodes_stats->min_required_free_slots,
						   available_slots);
				log_hint(_("increase parameter \"max_replication_slots\" or use -F/--force to proceed in any case"));

				if (runtime_options.dry_run == true)
				{
					*dry_run_success = false;
				}
				else
				{
					return false;
				}
			}
		}
		else
		{
			if (runtime_options.dry_run == true)
			{
				log_info(_("%i replication slots required, %i available"),
						 sibling_nodes_stats->min_required_free_slots,
						 available_slots);
			}
		}
	}

	return true;
}


static void
sibling_nodes_follow(t_node_info *local_node_record, NodeInfoList *sibling_nodes, SiblingNodeStats *sibling_nodes_stats)
{
	int			failed_follow_count = 0;
	char		host[MAXLEN] = "";
	NodeInfoListCell *cell = NULL;
	PQExpBufferData remote_command_str;
	PQExpBufferData command_output;

	log_notice(_("executing STANDBY FOLLOW on %i of %i siblings"),
			   sibling_nodes->node_count - sibling_nodes_stats->unreachable_sibling_node_count,
			   sibling_nodes->node_count);

	for (cell = sibling_nodes->head; cell; cell = cell->next)
	{
		bool		success = false;

		/* skip nodes previously determined as unreachable */
		if (cell->node_info->reachable == false)
			continue;

		initPQExpBuffer(&remote_command_str);
		make_remote_repmgr_path(&remote_command_str, cell->node_info);

		if (cell->node_info->type == WITNESS)
		{
			PGconn *witness_conn = NULL;

			/* TODO: create "repmgr witness resync" or similar */
			appendPQExpBuffer(&remote_command_str,
							  "witness register -d \\'%s\\' --force 2>/dev/null && echo \"1\" || echo \"0\"",
							  local_node_record->conninfo);

			/*
			 * Notify the witness repmgrd about the new primary, as at this point it will be assuming
			 * a failover situation is in place. It will detect the new primary at some point, this
			 * just speeds up the process.
			 *
			 * In the unlikely event repmgrd is not running or not in use, this will have no effect.
			 */
			witness_conn = establish_db_connection_quiet(cell->node_info->conninfo);

			if (PQstatus(witness_conn) == CONNECTION_OK)
			{
				notify_follow_primary(witness_conn, local_node_record->node_id);
			}
			PQfinish(witness_conn);
		}
		else
		{
			appendPQExpBufferStr(&remote_command_str,
								 "standby follow 2>/dev/null && echo \"1\" || echo \"0\"");
		}
		get_conninfo_value(cell->node_info->conninfo, "host", host);
		log_debug("executing:\n  %s", remote_command_str.data);

		initPQExpBuffer(&command_output);

		success = remote_command(host,
								 runtime_options.remote_user,
								 remote_command_str.data,
								 config_file_options.ssh_options,
								 &command_output);

		termPQExpBuffer(&remote_command_str);

		if (success == false || command_output.data[0] == '0')
		{
			if (cell->node_info->type == WITNESS)
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



static t_remote_error_type
parse_remote_error(const char *error)
{
	if (error[0] == '\0')
		return REMOTE_ERROR_UNKNOWN;

	if (strcasecmp(error, "DB_CONNECTION") == 0)
		return REMOTE_ERROR_DB_CONNECTION;

	if (strcasecmp(error, "CONNINFO_PARSE") == 0)
		return REMOTE_ERROR_CONNINFO_PARSE;

	return REMOTE_ERROR_UNKNOWN;
}


static CheckStatus
parse_check_status(const char *status_str)
{
	CheckStatus status = CHECK_STATUS_UNKNOWN;

	if (strncmp(status_str, "OK", MAXLEN) == 0)
	{
		status = CHECK_STATUS_OK;
	}
	else if (strncmp(status_str, "WARNING", MAXLEN) == 0)
	{
		status = CHECK_STATUS_WARNING;
	}
	else if (strncmp(status_str, "CRITICAL", MAXLEN) == 0)
	{
		status = CHECK_STATUS_CRITICAL;
	}
	else if (strncmp(status_str, "UNKNOWN", MAXLEN) == 0)
	{
		status = CHECK_STATUS_UNKNOWN;
	}

	return status;
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
parse_node_check_archiver(const char *node_check_output, int *files, int *threshold, t_remote_error_type *remote_error)
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
		{"error", required_argument, NULL, 'E'},
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
				status = parse_check_status(optarg);
				break;
			case 'E':
				{
					*remote_error = parse_remote_error(optarg);
					status = CHECK_STATUS_UNKNOWN;
				}
				break;
		}
	}

	free_parsed_argv(&argv_array);

	return status;
}


static bool
parse_data_directory_config(const char *node_check_output, t_remote_error_type *remote_error)
{
	bool		config_ok = true;

	int			c = 0,
				argc_item = 0;
	char	  **argv_array = NULL;
	int			optindex = 0;

	/* We're only interested in these options */
	struct option node_check_options[] =
	{
		{"configured-data-directory", required_argument, NULL, 'C'},
		{"error", required_argument, NULL, 'E'},
		{NULL, 0, NULL, 0}
	};

	/* Don't attempt to tokenise an empty string */
	if (!strlen(node_check_output))
	{
		return false;
	}

	argc_item = parse_output_to_argv(node_check_output, &argv_array);

	/* Reset getopt's optind variable */
	optind = 0;

	/* Prevent getopt from emitting errors */
	opterr = 0;

	while ((c = getopt_long(argc_item, argv_array, "C:E:", node_check_options,
							&optindex)) != -1)
	{
		switch (c)
		{
			/* --configured-data-directory */
			case 'C':
				{
					/* we only care whether it's "OK" or not */
					if (strncmp(optarg, "OK", 2) != 0)
						config_ok = false;
				}
				break;
			case 'E':
				{
					*remote_error = parse_remote_error(optarg);
					config_ok = false;
				}
				break;
		}
	}
	free_parsed_argv(&argv_array);

	return config_ok;
}


static bool
parse_replication_config_owner(const char *node_check_output)
{
	bool		config_ok = true;

	int			c = 0,
				argc_item = 0;
	char	  **argv_array = NULL;
	int			optindex = 0;

	/* We're only interested in these options */
	struct option node_check_options[] =
	{
		{"replication-config-owner", required_argument, NULL, 'C'},
		{NULL, 0, NULL, 0}
	};

	/* Don't attempt to tokenise an empty string */
	if (!strlen(node_check_output))
	{
		return false;
	}

	argc_item = parse_output_to_argv(node_check_output, &argv_array);

	/* Reset getopt's optind variable */
	optind = 0;

	/* Prevent getopt from emitting errors */
	opterr = 0;

	while ((c = getopt_long(argc_item, argv_array, "C:", node_check_options,
							&optindex)) != -1)
	{
		switch (c)
		{
			/* --configured-data-directory */
			case 'C':
				{
					/* we only care whether it's "OK" or not */
					if (strncmp(optarg, "OK", 2) != 0)
						config_ok = false;
				}
				break;
		}
	}

	free_parsed_argv(&argv_array);

	return config_ok;
}


static CheckStatus
parse_db_connection(const char *db_connection)
{
	CheckStatus status = CHECK_STATUS_UNKNOWN;

	int			c = 0,
				argc_item = 0;
	char	  **argv_array = NULL;
	int			optindex = 0;

	/* We're only interested in this option */
	struct option node_check_options[] =
	{
		{"db-connection", required_argument, NULL, 'c'},
		{NULL, 0, NULL, 0}
	};

	/* Don't attempt to tokenise an empty string */
	if (!strlen(db_connection))
	{
		return false;
	}

	argc_item = parse_output_to_argv(db_connection, &argv_array);

	/* Reset getopt's optind variable */
	optind = 0;

	/* Prevent getopt from emitting errors */
	opterr = 0;

	while ((c = getopt_long(argc_item, argv_array, "c:", node_check_options,
							&optindex)) != -1)
	{
		switch (c)
		{
			/* --db-connection */
			case 'c':
				{
					status = parse_check_status(optarg);
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
	printf(_("  -S, --superuser=USERNAME            superuser to use, if repmgr user is not superuser\n"));
	printf(_("  --upstream-conninfo                 \"primary_conninfo\" value to write in recovery.conf\n" \
			 "                                        when the intended upstream server does not yet exist\n"));
	printf(_("  --upstream-node-id                  ID of the upstream node to replicate from (optional, defaults to primary node)\n"));
#if (PG_VERSION_NUM >= 130000)
	printf(_("  --verify-backup                     verify a cloned node using the \"pg_verifybackup\" utility\n"));
#endif
	printf(_("  --without-barman                    do not clone from Barman even if configured\n"));
	printf(_("  --replication-conf-only             generate replication configuration for a previously cloned instance\n"));

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
	printf(_("  --dry-run                           perform checks etc. but don't actually promote the node\n"));
	printf(_("  -F, --force                         ignore warnings and continue anyway\n"));
	printf(_("  --siblings-follow                   have other standbys follow new primary\n"));
	puts("");

	printf(_("STANDBY FOLLOW\n"));
	puts("");
	printf(_("  \"standby follow\" instructs a standby node to follow a new primary.\n"));
	puts("");
	printf(_("  --dry-run                           perform checks but don't actually follow the new primary\n"));
	printf(_("  --upstream-node-id                  node ID of the new primary\n"));
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
	printf(_("  -S, --superuser=USERNAME            superuser to use, if repmgr user is not superuser\n"));
	printf(_("  --repmgrd-no-pause                  don't pause repmgrd\n"));
	printf(_("  --siblings-follow                   have other standbys follow new primary\n"));

	puts("");

	printf(_("%s home page: <%s>\n"), "repmgr", REPMGR_URL);

}
