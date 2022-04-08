/*
 *
 * dirmod.c
 *	  directory handling functions
 *
 * Copyright (c) EnterpriseDB Corporation, 2010-2021
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
#include <dirent.h>
#include <signal.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <ftw.h>

/* NB: postgres_fe must be included BEFORE check_dir */
#include <libpq-fe.h>
#include <postgres_fe.h>

#include "dirutil.h"
#include "strutil.h"
#include "log.h"
#include "controldata.h"

static int	unlink_dir_callback(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf);

/* PID can be negative if backend is standalone */
typedef long pgpid_t;


/*
 * Check if a directory exists, and if so whether it is empty.
 *
 * This function is used for checking both the data directory
 * and tablespace directories.
 */
DataDirState
check_dir(const char *path)
{
	DIR		   *chkdir = NULL;
	struct dirent *file = NULL;
	int			result = DIR_EMPTY;

	errno = 0;

	chkdir = opendir(path);

	if (!chkdir)
		return (errno == ENOENT) ? DIR_NOENT : DIR_ERROR;

	while ((file = readdir(chkdir)) != NULL)
	{
		if (strcmp(".", file->d_name) == 0 ||
			strcmp("..", file->d_name) == 0)
		{
			/* skip this and parent directory */
			continue;
		}
		else
		{
			result = DIR_NOT_EMPTY;
			break;
		}
	}

	closedir(chkdir);

	if (errno != 0)
		return DIR_ERROR;				/* some kind of I/O error? */

	return result;
}


/*
 * Create directory with error log message when failing
 */
bool
create_dir(const char *path)
{
	char create_dir_path[MAXPGPATH];

	/* mkdir_p() may modify the supplied path */
	strncpy(create_dir_path, path, MAXPGPATH);

	if (mkdir_p(create_dir_path, 0700) == 0)
		return true;

	log_error(_("unable to create directory \"%s\""), create_dir_path);
	log_detail("%s", strerror(errno));

	return false;
}


bool
set_dir_permissions(const char *path, int server_version_num)
{
	struct stat stat_buf;
	bool no_group_access =
		(server_version_num != UNKNOWN_SERVER_VERSION_NUM) &&
		(server_version_num < 110000);
	/*
	 * At this point the path should exist, so this check is very
	 * much just-in-case.
	 */
	if (stat(path, &stat_buf) != 0)
	{
		if (errno == ENOENT)
		{
			log_warning(_("directory \"%s\" does not exist"), path);
		}
		else
		{
			log_warning(_("could not read permissions of directory \"%s\""),
						path);
			log_detail("%s", strerror(errno));
		}

		return false;
	}

	/*
	 * If mode is not 0700 or 0750, attempt to change.
	 */
	if ((no_group_access == true  && (stat_buf.st_mode & (S_IRWXG | S_IRWXO)))
	 || (no_group_access == false && (stat_buf.st_mode & (S_IWGRP | S_IRWXO))))
	{
		/*
		 * Currently we default to 0700.
		 * There is no facility to override this directly,
		 * but the user can manually create the directory with
		 * the desired permissions.
		 */

		if (chmod(path, 0700) != 0) {
			log_error(_("unable to change permissions of directory \"%s\""), path);
			log_detail("%s", strerror(errno));
			return false;
		}

		return true;
	}

	/* Leave as-is */
	return true;
}


/* function from initdb.c */
/* source adapted from FreeBSD /src/bin/mkdir/mkdir.c */

/*
 * this tries to build all the elements of a path to a directory a la mkdir -p
 * we assume the path is in canonical form, i.e. uses / as the separator
 * we also assume it isn't null.
 *
 * note that on failure, the path arg has been modified to show the particular
 * directory level we had problems with.
 */
int
mkdir_p(char *path, mode_t omode)
{
	struct stat sb;
	mode_t		numask,
				oumask;
	int			first,
				last,
				retval;
	char	   *p;

	p = path;
	oumask = 0;
	retval = 0;


	if (p[0] == '/')			/* Skip leading '/'. */
		++p;
	for (first = 1, last = 0; !last; ++p)
	{
		if (p[0] == '\0')
			last = 1;
		else if (p[0] != '/')
			continue;
		*p = '\0';
		if (!last && p[1] == '\0')
			last = 1;
		if (first)
		{
			/*
			 * POSIX 1003.2: For each dir operand that does not name an
			 * existing directory, effects equivalent to those caused by the
			 * following command shall occur:
			 *
			 * mkdir -p -m $(umask -S),u+wx $(dirname dir) && mkdir [-m mode]
			 * dir
			 *
			 * We change the user's umask and then restore it, instead of
			 * doing chmod's.
			 */
			oumask = umask(0);
			numask = oumask & ~(S_IWUSR | S_IXUSR);
			(void) umask(numask);
			first = 0;
		}
		if (last)
			(void) umask(oumask);

		/* check for pre-existing directory; ok if it's a parent */
		if (stat(path, &sb) == 0)
		{
			if (!S_ISDIR(sb.st_mode))
			{
				if (last)
					errno = EEXIST;
				else
					errno = ENOTDIR;
				retval = 1;
				break;
			}
		}
		else if (mkdir(path, last ? omode : S_IRWXU | S_IRWXG | S_IRWXO) < 0)
		{
			retval = 1;
			break;
		}
		if (!last)
			*p = '/';
	}
	if (!first && !last)
		(void) umask(oumask);
	return retval;
}


bool
is_pg_dir(const char *path)
{
	char		dirpath[MAXPGPATH] = "";
	struct stat sb;

	/* test pgdata */
	snprintf(dirpath, MAXPGPATH, "%s/PG_VERSION", path);
	if (stat(dirpath, &sb) == 0)
		return true;

	/* TODO: sanity check other files */

	return false;
}

/*
 * Attempt to determine if a PostgreSQL data directory is in use
 * by reading the pidfile. This is the same mechanism used by
 * "pg_ctl".
 *
 * This function will abort with appropriate log messages if a file error
 * is encountered, as the user will need to address the situation before
 * any further useful progress can be made.
 */
PgDirState
is_pg_running(const char *path)
{
	long		pid;
	FILE	   *pidf;

	char pid_file[MAXPGPATH];

	/* it's reasonable to assume the pidfile name will not change */
	snprintf(pid_file, MAXPGPATH, "%s/postmaster.pid", path);

	pidf = fopen(pid_file, "r");
	if (pidf == NULL)
	{
		/*
		 * No PID file - PostgreSQL shouldn't be running. From 9.3 (the
		 * earliest version we care about) removal of the PID file will
		 * cause the postmaster to shut down, so it's highly unlikely
		 * that PostgreSQL will still be running.
		 */
		if (errno == ENOENT)
		{
			return PG_DIR_NOT_RUNNING;
		}
		else
		{
			log_error(_("unable to open PostgreSQL PID file \"%s\""), pid_file);
			log_detail("%s", strerror(errno));
			exit(ERR_BAD_CONFIG);
		}
	}


	/*
	 * In the unlikely event we're unable to extract a PID from the PID file,
	 * log a warning but assume we're not dealing with a running instance
	 * as PostgreSQL should have shut itself down in these cases anyway.
	 */
	if (fscanf(pidf, "%ld", &pid) != 1)
	{
		/* Is the file empty? */
		if (ftell(pidf) == 0 && feof(pidf))
		{
			log_warning(_("PostgreSQL PID file \"%s\" is empty"), path);
		}
		else
		{
			log_warning(_("invalid data in PostgreSQL PID file \"%s\""), path);
		}

		fclose(pidf);

		return PG_DIR_NOT_RUNNING;
	}

	fclose(pidf);

	if (pid == getpid())
		return PG_DIR_NOT_RUNNING;

	if (pid == getppid())
		return PG_DIR_NOT_RUNNING;

	if (kill(pid, 0) == 0)
		return PG_DIR_RUNNING;

	return PG_DIR_NOT_RUNNING;
}


bool
create_pg_dir(const char *path, bool force)
{
	/* Check this directory can be used as a PGDATA dir */
	switch (check_dir(path))
	{
		case DIR_NOENT:
			/* Directory does not exist, attempt to create it. */
			log_info(_("creating directory \"%s\"..."), path);

			if (!create_dir(path))
			{
				log_error(_("unable to create directory \"%s\"..."),
						  path);
				return false;
			}
			break;
		case DIR_EMPTY:
			/*
			 * Directory exists but empty, fix permissions and use it.
			 *
			 * Note that at this point the caller might not know the server
			 * version number, so in this case "set_dir_permissions()" will
			 * accept 0750 as a valid setting. As this is invalid in Pg10 and
			 * earlier,  the caller should call "set_dir_permissions()" again
			 * when it has the number.
			 *
			 * We need to do the permissions check here in any case to catch
			 * fatal permissions early.
			 */
			log_info(_("checking and correcting permissions on existing directory \"%s\""),
					 path);

			if (!set_dir_permissions(path, UNKNOWN_SERVER_VERSION_NUM))
			{
				return false;
			}
			break;
		case DIR_NOT_EMPTY:
			/* exists but is not empty */
			log_warning(_("directory \"%s\" exists but is not empty"),
						path);

			if (is_pg_dir(path))
			{
				if (force == true)
				{
					log_notice(_("-F/--force provided - deleting existing data directory \"%s\""), path);
					nftw(path, unlink_dir_callback, 64, FTW_DEPTH | FTW_PHYS);

					/* recreate the directory ourselves to ensure permissions are correct */
					if (!create_dir(path))
					{
						log_error(_("unable to create directory \"%s\"..."),
								  path);
						return false;
					}

					return true;
				}

				return false;
			}
			else
			{
				if (force == true)
				{
					log_notice(_("deleting existing directory \"%s\""), path);
					nftw(path, unlink_dir_callback, 64, FTW_DEPTH | FTW_PHYS);

					/* recreate the directory ourselves to ensure permissions are correct */
					if (!create_dir(path))
					{
						log_error(_("unable to create directory \"%s\"..."),
								  path);
						return false;
					}

					return true;
				}
				return false;
			}
			break;
		case DIR_ERROR:
			log_error(_("could not access directory \"%s\"")
					  , path);
			log_detail("%s", strerror(errno));
			return false;
	}

	return true;
}



int
rmdir_recursive(const char *path)
{
	return nftw(path, unlink_dir_callback, 64, FTW_DEPTH | FTW_PHYS);
}

static int
unlink_dir_callback(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
	int			rv = remove(fpath);

	if (rv)
		perror(fpath);

	return rv;
}
