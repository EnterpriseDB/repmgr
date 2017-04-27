/*
 * repmgr-action-standby.c
 *
 * Implements standby actions for the repmgr command line utility
 *
 * Copyright (c) 2ndQuadrant, 2010-2017
 */

#include "repmgr.h"

#include "repmgr-client-global.h"
#include "repmgr-action-standby.h"



void
do_standby_clone(void)
{
	PGconn	   *primary_conn = NULL;
	PGconn	   *source_conn = NULL;
	PGresult   *res;

	int			server_version_num = -1;
	char		cluster_size[MAXLEN];

	/*
	 * conninfo params for the actual upstream node (which might be different
	 * to the node we're cloning from) to write to recovery.conf
	 */
	t_conninfo_param_list recovery_conninfo;
	char		recovery_conninfo_str[MAXLEN];
	bool		upstream_record_found = false;
	int		    upstream_node_id = UNKNOWN_NODE_ID;


	enum {
		barman,
		rsync,
		pg_basebackup
	}			mode;

	/* used by barman mode */
	char        datadir_list_filename[MAXLEN];
	char		local_repmgr_tmp_directory[MAXPGPATH];

	puts("standby clone");


	/*
	 * detecting the cloning mode
	 */
	if (runtime_options.rsync_only)
		mode = rsync;
	else if (strcmp(config_file_options.barman_server, "") != 0 && ! runtime_options.without_barman)
		mode = barman;
	else
		mode = pg_basebackup;

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

}
