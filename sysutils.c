/*
 * sysutils.c
 *
 * Copyright (c) 2ndQuadrant, 2010-2019
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

#include "repmgr.h"

static bool _local_command(const char *command, PQExpBufferData *outputbuf, bool simple, int *return_value);


/*
 * Execute a command locally. "outputbuf" should either be an
 * initialised PQexpbuffer, or NULL
 */
bool
local_command(const char *command, PQExpBufferData *outputbuf)
{
	return _local_command(command, outputbuf, false, NULL);
}

bool
local_command_return_value(const char *command, PQExpBufferData *outputbuf, int *return_value)
{
	return _local_command(command, outputbuf, false, return_value);
}


bool
local_command_simple(const char *command, PQExpBufferData *outputbuf)
{
	return _local_command(command, outputbuf, true, NULL);
}


static bool
_local_command(const char *command, PQExpBufferData *outputbuf, bool simple, int *return_value)
{
	FILE	   *fp = NULL;
	char		output[MAXLEN];
	int			retval = 0;
	bool		success;

	log_verbose(LOG_DEBUG, "executing:\n  %s", command);

	if (outputbuf == NULL)
	{
		retval = system(command);

		if (return_value != NULL)
			*return_value = WEXITSTATUS(retval);

		return (retval == 0) ? true : false;
	}

	fp = popen(command, "r");

	if (fp == NULL)
	{
		log_error(_("unable to execute local command:\n%s"), command);
		return false;
	}


	while (fgets(output, MAXLEN, fp) != NULL)
	{
		appendPQExpBuffer(outputbuf, "%s", output);

		if (!feof(fp) && simple == false)
		{
			break;
		}
	}

	retval = pclose(fp);

	/*  */
	success = (WEXITSTATUS(retval) == 0 || WEXITSTATUS(retval) == 141) ? true : false;

	log_verbose(LOG_DEBUG, "result of command was %i (%i)", WEXITSTATUS(retval), retval);

	if (return_value != NULL)
		*return_value = WEXITSTATUS(retval);

	if (outputbuf->data != NULL && outputbuf->data[0] != '\0')
		log_verbose(LOG_DEBUG, "local_command(): output returned was:\n%s", outputbuf->data);
	else
		log_verbose(LOG_DEBUG, "local_command(): no output returned");

	return success;
}


/*
 * Execute a command via ssh on the remote host.
 *
 * TODO: implement SSH calls using libssh2.
 */
bool
remote_command(const char *host, const char *user, const char *command, const char *ssh_options, PQExpBufferData *outputbuf)
{
	FILE	   *fp;
	char		ssh_command[MAXLEN] = "";
	PQExpBufferData ssh_host;

	char		output[MAXLEN] = "";

	initPQExpBuffer(&ssh_host);

	if (*user != '\0')
	{
		appendPQExpBuffer(&ssh_host, "%s@", user);
	}

	appendPQExpBuffer(&ssh_host, "%s", host);

	maxlen_snprintf(ssh_command,
					"ssh -o Batchmode=yes %s %s %s",
					ssh_options,
					ssh_host.data,
					command);

	termPQExpBuffer(&ssh_host);

	log_debug("remote_command():\n  %s", ssh_command);

	fp = popen(ssh_command, "r");

	if (fp == NULL)
	{
		log_error(_("unable to execute remote command:\n  %s"), ssh_command);
		return false;
	}

	if (outputbuf != NULL)
	{
		/* TODO: better error handling */
		while (fgets(output, MAXLEN, fp) != NULL)
		{
			appendPQExpBuffer(outputbuf, "%s", output);
		}
	}
	else
	{
		while (fgets(output, MAXLEN, fp) != NULL)
		{
			if (!feof(fp))
			{
				break;
			}
		}
	}

	pclose(fp);

	if (outputbuf != NULL)
	{
		if (outputbuf->data != NULL && outputbuf->data[0] != '\0')
			log_verbose(LOG_DEBUG, "remote_command(): output returned was:\n%s", outputbuf->data);
		else
			log_verbose(LOG_DEBUG, "remote_command(): no output returned");
	}

	return true;
}
