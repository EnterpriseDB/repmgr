/*
 * check_dir.c
 * Copyright (c) 2ndQuadrant, 2010
 *
 * Directories management functions
 */

#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>

#include "check_dir.h"

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
check_dir(const char *dir)
{
    DIR        *chkdir;
    struct 		dirent *file;
    int         result = 1;

	char		*dummy_file;
	FILE 		*dummy_fd;

    errno = 0;

    chkdir = opendir(dir);

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
            result = 2;         /* not empty */
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
        return -1          /* some kind of I/O error? */

	return result;
}


/*
 * Create directory 
 */
bool
create_directory(const char *dir)
{
    if (mkdir_p(dir, 0700) == 0)
        return true;

    fprintf(stderr, _("Could not create directory \"%s\": %s\n"),
            dir, strerror(errno));

    return false;
}

bool
set_directory_permissions(const char *dir)
{
	return (chmod(data_dir, 0700) != 0) ? false : true;
}
