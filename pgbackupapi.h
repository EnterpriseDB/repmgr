/*
 * pgbackupapi.h
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
#include <curl/curl.h>
#include <json-c/json.h>

typedef struct operation_task {
        char *backup_id;
        char *destination_directory;
        char *operation_type;
	char *operation_id;
	char *operation_status;
        char *remote_ssh_command;
	char *host;
	char *node_name;
} operation_task;

//Default simplebuffer size in most of operations
#define MAX_BUFFER_LENGTH 72

//Callbacks to send/receive data from pg-backup-api endpoints
size_t receive_operations_cb(void *content, size_t size, size_t nmemb, char *buffer);
size_t receive_operation_id(void *content, size_t size, size_t nmemb, char *buffer);
size_t receive_operation_status(void *content, size_t size, size_t nmemb, char *buffer);

//Functions that implement the logic and know what to do and how to comunnicate wuth the API
CURLcode get_operations_on_server(CURL *curl, operation_task *task);
CURLcode create_new_task(CURL *curl, operation_task *task);
CURLcode get_status_of_operation(CURL *curl, operation_task *task);

//Helper to make simpler to read the handler where we set the URL
char * define_base_url(operation_task *task);
