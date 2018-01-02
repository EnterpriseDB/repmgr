/*
 * controldata.c
 * Copyright (c) 2ndQuadrant, 2010-2018
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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

#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "postgres_fe.h"

#include "repmgr.h"
#include "controldata.h"

static ControlFileInfo *get_controlfile(const char *DataDir);

uint64
get_system_identifier(const char *data_directory)
{
	ControlFileInfo *control_file_info = NULL;
	uint64		system_identifier = UNKNOWN_SYSTEM_IDENTIFIER;

	control_file_info = get_controlfile(data_directory);

	if (control_file_info->control_file_processed == true)
		system_identifier = control_file_info->control_file->system_identifier;
	else
		system_identifier = UNKNOWN_SYSTEM_IDENTIFIER;

	pfree(control_file_info->control_file);
	pfree(control_file_info);

	return system_identifier;
}

DBState
get_db_state(const char *data_directory)
{
	ControlFileInfo *control_file_info = NULL;
	DBState		state;

	control_file_info = get_controlfile(data_directory);

	if (control_file_info->control_file_processed == true)
		state = control_file_info->control_file->state;
	else
		/* if we were unable to parse the control file, assume DB is shut down */
		state = DB_SHUTDOWNED;

	pfree(control_file_info->control_file);
	pfree(control_file_info);

	return state;
}


extern XLogRecPtr
get_latest_checkpoint_location(const char *data_directory)
{
	ControlFileInfo *control_file_info = NULL;
	XLogRecPtr	checkPoint = InvalidXLogRecPtr;

	control_file_info = get_controlfile(data_directory);

	if (control_file_info->control_file_processed == false)
		return InvalidXLogRecPtr;

	checkPoint = control_file_info->control_file->checkPoint;

	pfree(control_file_info->control_file);
	pfree(control_file_info);

	return checkPoint;
}


int
get_data_checksum_version(const char *data_directory)
{
	ControlFileInfo *control_file_info = NULL;
	int			data_checksum_version = -1;

	control_file_info = get_controlfile(data_directory);

	if (control_file_info->control_file_processed == false)
	{
		data_checksum_version = -1;
	}
	else
	{
		data_checksum_version = (int) control_file_info->control_file->data_checksum_version;
	}

	pfree(control_file_info->control_file);
	pfree(control_file_info);

	return data_checksum_version;
}


const char *
describe_db_state(DBState state)
{
	switch (state)
	{
		case DB_STARTUP:
			return _("starting up");
		case DB_SHUTDOWNED:
			return _("shut down");
		case DB_SHUTDOWNED_IN_RECOVERY:
			return _("shut down in recovery");
		case DB_SHUTDOWNING:
			return _("shutting down");
		case DB_IN_CRASH_RECOVERY:
			return _("in crash recovery");
		case DB_IN_ARCHIVE_RECOVERY:
			return _("in archive recovery");
		case DB_IN_PRODUCTION:
			return _("in production");
	}
	return _("unrecognized status code");
}


/*
 * we maintain our own version of get_controlfile() as we need cross-version
 * compatibility, and also don't care if the file isn't readable.
 */
static ControlFileInfo *
get_controlfile(const char *DataDir)
{
	ControlFileInfo *control_file_info;
	int			fd;
	char		ControlFilePath[MAXPGPATH] = "";

	control_file_info = palloc0(sizeof(ControlFileInfo));
	control_file_info->control_file_processed = false;
	control_file_info->control_file = palloc0(sizeof(ControlFileData));

	snprintf(ControlFilePath, MAXPGPATH, "%s/global/pg_control", DataDir);

	if ((fd = open(ControlFilePath, O_RDONLY | PG_BINARY, 0)) == -1)
	{
		log_debug("could not open file \"%s\" for reading: %s",
				  ControlFilePath, strerror(errno));
		return control_file_info;
	}

	if (read(fd, control_file_info->control_file, sizeof(ControlFileData)) != sizeof(ControlFileData))
	{
		log_debug("could not read file \"%s\": %s",
				  ControlFilePath, strerror(errno));
		return control_file_info;
	}

	close(fd);

	control_file_info->control_file_processed = true;

	/*
	 * We don't check the CRC here as we're potentially checking a pg_control
	 * file from a different PostgreSQL version to the one repmgr was compiled
	 * against. However we're only interested in the first few fields, which
	 * should be constant across supported versions
	 *
	 */

	return control_file_info;
}
