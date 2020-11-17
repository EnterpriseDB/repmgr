/*
 * controldata.c - functions for reading the pg_control file
 *
 * The functions provided here enable repmgr to read a pg_control file
 * in a version-indepent way, even if the PostgreSQL instance is not
 * running. For that reason we can't use on the pg_control_*() functions
 * provided in PostgreSQL 9.6 and later.
 *
 * Copyright (c) 2ndQuadrant, 2010-2020
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

int
get_pg_version(const char *data_directory, char *version_string)
{
	char		PgVersionPath[MAXPGPATH] = "";
	FILE	   *fp = NULL;
	char	   *endptr = NULL;
	char		file_version_string[MAX_VERSION_STRING] = "";
	long		file_major, file_minor;
	int			ret;

	snprintf(PgVersionPath, MAXPGPATH, "%s/PG_VERSION", data_directory);

	fp = fopen(PgVersionPath, "r");

	if (fp == NULL)
	{
		log_warning(_("could not open file \"%s\" for reading"),
					PgVersionPath);
		log_detail("%s", strerror(errno));
		return UNKNOWN_SERVER_VERSION_NUM;
	}

	file_version_string[0] = '\0';

	ret = fscanf(fp, "%23s", file_version_string);
	fclose(fp);

	if (ret != 1 || endptr == file_version_string)
	{
		log_warning(_("unable to determine major version number from PG_VERSION"));

		return UNKNOWN_SERVER_VERSION_NUM;
	}

	file_major = strtol(file_version_string, &endptr, 10);
	file_minor = 0;

	if (*endptr == '.')
		file_minor = strtol(endptr + 1, NULL, 10);

	if (version_string != NULL)
		strncpy(version_string, file_version_string, MAX_VERSION_STRING);

	return ((int) file_major * 10000) + ((int) file_minor * 100);
}


uint64
get_system_identifier(const char *data_directory)
{
	ControlFileInfo *control_file_info = NULL;
	uint64		system_identifier = UNKNOWN_SYSTEM_IDENTIFIER;

	control_file_info = get_controlfile(data_directory);

	if (control_file_info->control_file_processed == true)
		system_identifier = control_file_info->system_identifier;

	pfree(control_file_info);

	return system_identifier;
}


bool
get_db_state(const char *data_directory, DBState *state)
{
	ControlFileInfo *control_file_info = NULL;
	bool control_file_processed;

	control_file_info = get_controlfile(data_directory);
	control_file_processed = control_file_info->control_file_processed;

	if (control_file_processed == true)
		*state = control_file_info->state;

	pfree(control_file_info);

	return control_file_processed;
}


XLogRecPtr
get_latest_checkpoint_location(const char *data_directory)
{
	ControlFileInfo *control_file_info = NULL;
	XLogRecPtr	checkPoint = InvalidXLogRecPtr;

	control_file_info = get_controlfile(data_directory);

	if (control_file_info->control_file_processed == true)
		checkPoint = control_file_info->checkPoint;

	pfree(control_file_info);

	return checkPoint;
}


int
get_data_checksum_version(const char *data_directory)
{
	ControlFileInfo *control_file_info = NULL;
	int			data_checksum_version = UNKNOWN_DATA_CHECKSUM_VERSION;

	control_file_info = get_controlfile(data_directory);

	if (control_file_info->control_file_processed == true)
		data_checksum_version = (int) control_file_info->data_checksum_version;

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


TimeLineID
get_timeline(const char *data_directory)
{
	ControlFileInfo *control_file_info = NULL;
	TimeLineID		 timeline = -1;

	control_file_info = get_controlfile(data_directory);

	timeline = (int) control_file_info->timeline;

	pfree(control_file_info);

	return timeline;
}


TimeLineID
get_min_recovery_end_timeline(const char *data_directory)
{
	ControlFileInfo *control_file_info = NULL;
	TimeLineID		 timeline = -1;

	control_file_info = get_controlfile(data_directory);

	timeline = (int) control_file_info->minRecoveryPointTLI;

	pfree(control_file_info);

	return timeline;
}


XLogRecPtr
get_min_recovery_location(const char *data_directory)
{
	ControlFileInfo *control_file_info = NULL;
	XLogRecPtr	minRecoveryPoint  = InvalidXLogRecPtr;

	control_file_info = get_controlfile(data_directory);

	minRecoveryPoint = control_file_info->minRecoveryPoint;

	pfree(control_file_info);

	return minRecoveryPoint;
}


/*
 * We maintain our own version of get_controlfile() as we need cross-version
 * compatibility, and also don't care if the file isn't readable.
 */
static ControlFileInfo *
get_controlfile(const char *DataDir)
{
	char		file_version_string[MAX_VERSION_STRING] = "";
	ControlFileInfo *control_file_info;
	int			fd, version_num;
	char		ControlFilePath[MAXPGPATH] = "";
	void	   *ControlFileDataPtr = NULL;
	int			expected_size = 0;

	control_file_info = palloc0(sizeof(ControlFileInfo));

	/* set default values */
	control_file_info->control_file_processed = false;
	control_file_info->system_identifier = UNKNOWN_SYSTEM_IDENTIFIER;
	control_file_info->state = DB_SHUTDOWNED;
	control_file_info->checkPoint = InvalidXLogRecPtr;
	control_file_info->data_checksum_version = -1;
	control_file_info->timeline = -1;
	control_file_info->minRecoveryPointTLI = -1;
	control_file_info->minRecoveryPoint = InvalidXLogRecPtr;

	/*
	 * Read PG_VERSION, as we'll need to determine which struct to read
	 * the control file contents into
	 */

	version_num = get_pg_version(DataDir, file_version_string);

	if (version_num == UNKNOWN_SERVER_VERSION_NUM)
	{
		log_warning(_("unable to determine server version number from PG_VERSION"));
		return control_file_info;
	}

	if (version_num < MIN_SUPPORTED_VERSION_NUM)
	{
		log_warning(_("data directory appears to be initialised for %s"),
					file_version_string);
		log_detail(_("minimum supported PostgreSQL version is %s"),
				   MIN_SUPPORTED_VERSION);
		return control_file_info;
	}

	snprintf(ControlFilePath, MAXPGPATH, "%s/global/pg_control", DataDir);

	if ((fd = open(ControlFilePath, O_RDONLY | PG_BINARY, 0)) == -1)
	{
		log_warning(_("could not open file \"%s\" for reading"),
					ControlFilePath);
		log_detail("%s", strerror(errno));
		return control_file_info;
	}

	if (version_num >= 120000)
	{
#if PG_ACTUAL_VERSION_NUM >= 120000
		expected_size = sizeof(ControlFileData12);
		ControlFileDataPtr = palloc0(expected_size);
#endif
	}
	else if (version_num >= 110000)
	{
		expected_size = sizeof(ControlFileData11);
		ControlFileDataPtr = palloc0(expected_size);
	}
	else if (version_num >= 90500)
	{
		expected_size = sizeof(ControlFileData95);
		ControlFileDataPtr = palloc0(expected_size);
	}
	else if (version_num >= 90400)
	{
		expected_size = sizeof(ControlFileData94);
		ControlFileDataPtr = palloc0(expected_size);
	}

	if (read(fd, ControlFileDataPtr, expected_size) != expected_size)
	{
		log_warning(_("could not read file \"%s\""),
					ControlFilePath);
		log_detail("%s", strerror(errno));

		close(fd);

		return control_file_info;
	}

	close(fd);

	control_file_info->control_file_processed = true;

	if (version_num >= 120000)
	{
#if PG_ACTUAL_VERSION_NUM >= 120000
		ControlFileData12 *ptr = (struct ControlFileData12 *)ControlFileDataPtr;
		control_file_info->system_identifier = ptr->system_identifier;
		control_file_info->state = ptr->state;
		control_file_info->checkPoint = ptr->checkPoint;
		control_file_info->data_checksum_version = ptr->data_checksum_version;
		control_file_info->timeline = ptr->checkPointCopy.ThisTimeLineID;
		control_file_info->minRecoveryPointTLI = ptr->minRecoveryPointTLI;
		control_file_info->minRecoveryPoint = ptr->minRecoveryPoint;
#else
		fprintf(stderr, "ERROR: please use a repmgr version built for PostgreSQL 12 or later\n");
		exit(ERR_BAD_CONFIG);
#endif
	}
	else if (version_num >= 110000)
	{
		ControlFileData11 *ptr = (struct ControlFileData11 *)ControlFileDataPtr;
		control_file_info->system_identifier = ptr->system_identifier;
		control_file_info->state = ptr->state;
		control_file_info->checkPoint = ptr->checkPoint;
		control_file_info->data_checksum_version = ptr->data_checksum_version;
		control_file_info->timeline = ptr->checkPointCopy.ThisTimeLineID;
		control_file_info->minRecoveryPointTLI = ptr->minRecoveryPointTLI;
		control_file_info->minRecoveryPoint = ptr->minRecoveryPoint;
	}
	else if (version_num >= 90500)
	{
		ControlFileData95 *ptr = (struct ControlFileData95 *)ControlFileDataPtr;
		control_file_info->system_identifier = ptr->system_identifier;
		control_file_info->state = ptr->state;
		control_file_info->checkPoint = ptr->checkPoint;
		control_file_info->data_checksum_version = ptr->data_checksum_version;
		control_file_info->timeline = ptr->checkPointCopy.ThisTimeLineID;
		control_file_info->minRecoveryPointTLI = ptr->minRecoveryPointTLI;
		control_file_info->minRecoveryPoint = ptr->minRecoveryPoint;
	}
	else if (version_num >= 90400)
	{
		ControlFileData94 *ptr = (struct ControlFileData94 *)ControlFileDataPtr;
		control_file_info->system_identifier = ptr->system_identifier;
		control_file_info->state = ptr->state;
		control_file_info->checkPoint = ptr->checkPoint;
		control_file_info->data_checksum_version = ptr->data_checksum_version;
		control_file_info->timeline = ptr->checkPointCopy.ThisTimeLineID;
		control_file_info->minRecoveryPointTLI = ptr->minRecoveryPointTLI;
		control_file_info->minRecoveryPoint = ptr->minRecoveryPoint;
	}

	pfree(ControlFileDataPtr);

	/*
	 * We don't check the CRC here as we're potentially checking a pg_control
	 * file from a different PostgreSQL version to the one repmgr was compiled
	 * against.
	 */

	return control_file_info;
}
