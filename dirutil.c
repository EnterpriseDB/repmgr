/*
 *
 * dirmod.c
 *	  directory handling functions
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

#include <unistd.h>
#include <dirent.h>
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

static int	unlink_dir_callback(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf);



/*
 * make sure the directory either doesn't exist or is empty
 * we use this function to check the new data directory and
 * the directories for tablespaces
 *
 * This is the same check initdb does on the new PGDATA dir
 *
 * Returns 0 if nonexistent, 1 if exists and empty, 2 if not empty,
 * or -1 if trouble accessing directory
 */
int
check_dir(char *path)
{
	DIR		   *chkdir;
	struct dirent *file;
	int			result = 1;

	errno = 0;

	chkdir = opendir(path);

	if (!chkdir)
		return (errno == ENOENT) ? 0 : -1;

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
			result = 2;			/* not empty */
			break;
		}
	}

#ifdef WIN32

	/*
	 * This fix is in mingw cvs (runtime/mingwex/dirent.c rev 1.4), but not in
	 * released version
	 */
	if (GetLastError() == ERROR_NO_MORE_FILES)
		errno = 0;
#endif

	closedir(chkdir);

	if (errno != 0)
		return -1;				/* some kind of I/O error? */

	return result;
}


/*
 * Create directory with error log message when failing
 */
bool
create_dir(char *path)
{
	if (mkdir_p(path, 0700) == 0)
		return true;

	log_error(_("unable to create directory \"%s\": %s"),
			  path, strerror(errno));

	return false;
}

bool
set_dir_permissions(char *path)
{
	return (chmod(path, 0700) != 0) ? false : true;
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

#ifdef WIN32
	/* skip network and drive specifiers for win32 */
	if (strlen(p) >= 2)
	{
		if (p[0] == '/' && p[1] == '/')
		{
			/* network drive */
			p = strstr(p + 2, "/");
			if (p == NULL)
				return 1;
		}
		else if (p[1] == ':' &&
				 ((p[0] >= 'a' && p[0] <= 'z') ||
				  (p[0] >= 'A' && p[0] <= 'Z')))
		{
			/* local drive */
			p += 2;
		}
	}
#endif

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
			 * following command shall occcur:
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
is_pg_dir(char *path)
{
	char		dirpath[MAXPGPATH];
	struct stat sb;

	/* test pgdata */
	snprintf(dirpath, MAXPGPATH, "%s/PG_VERSION", path);
	if (stat(dirpath, &sb) == 0)
		return true;

	/* TODO: sanity check other files */

	return false;
}


bool
create_pg_dir(char *path, bool force)
{
	bool		pg_dir = false;

	/* Check this directory could be used as a PGDATA dir */
	switch (check_dir(path))
	{
		case 0:
			/* dir not there, must create it */
			log_info(_("creating directory \"%s\"..."), path);

			if (!create_dir(path))
			{
				log_error(_("unable to create directory \"%s\"..."),
						  path);
				return false;
			}
			break;
		case 1:
			/* Present but empty, fix permissions and use it */
			log_info(_("checking and correcting permissions on existing directory %s"),
					 path);

			if (!set_dir_permissions(path))
			{
				log_error(_("unable to change permissions of directory \"%s\":\n  %s"),
						  path, strerror(errno));
				return false;
			}
			break;
		case 2:
			/* Present and not empty */
			log_warning(_("directory \"%s\" exists but is not empty"),
						path);

			pg_dir = is_pg_dir(path);

			if (pg_dir && force)
			{
				/* TODO: check DB state, if not running overwrite */

				if (false)
				{
					log_notice(_("deleting existing data directory \"%s\""), path);
					nftw(path, unlink_dir_callback, 64, FTW_DEPTH | FTW_PHYS);
				}
				/* Let it continue */
				break;
			}
			else if (pg_dir && !force)
			{
				log_hint(_("This looks like a PostgreSQL directory.\n"
						   "If you are sure you want to clone here, "
						   "please check there is no PostgreSQL server "
						   "running and use the -F/--force option"));
				return false;
			}

			return false;
		default:
			log_error(_("could not access directory \"%s\": %s"),
					  path, strerror(errno));
			return false;
	}
	return true;
}



int
rmdir_recursive(char *path)
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
