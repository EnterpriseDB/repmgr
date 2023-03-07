/*
 * pgbackupapi.c
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

#include <string.h>

#include <curl/curl.h>
#include <json-c/json.h>

#include "repmgr.h"
#include "pgbackupapi.h"


size_t receive_operations_cb(void *content, size_t size, size_t nmemb, char *buffer) {
	short int max_chars_to_copy = MAX_BUFFER_LENGTH -2;
	short int i = 0;
	int operation_length = 0;
	json_object *value;

	json_object *root = json_tokener_parse(content);
	json_object *operations = json_object_object_get(root, "operations");

	operation_length = strlen(json_object_get_string(operations));
	if (operation_length < max_chars_to_copy) {
		max_chars_to_copy = operation_length;
	}

	strncpy(buffer, json_object_get_string(operations), max_chars_to_copy);

	fprintf(stdout, "Success! The following operations were found\n");
	for (i=0; i<json_object_array_length(operations); i++) {
		value = json_object_array_get_idx(operations, i);
		printf("%s\n", json_object_get_string(value));
	}
	return size * nmemb;
}

char * define_base_url(operation_task *task) {
	char *format = "http://%s:7480/servers/%s/operations";
	char *url = malloc(MAX_BUFFER_LENGTH);

	snprintf(url, MAX_BUFFER_LENGTH-1, format, task->host, task->node_name);

	//`url` is freed on the function that called this
	return url;
}

CURLcode get_operations_on_server(CURL *curl, operation_task *task) {
	char buffer[MAX_BUFFER_LENGTH];
	char *url = define_base_url(task);
	CURLcode ret;

	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, receive_operations_cb);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buffer);
	curl_easy_setopt(curl, CURLOPT_URL, url);

	ret = curl_easy_perform(curl);
	free(url);

	return ret;
}

size_t receive_operation_id(void *content, size_t size, size_t nmemb, char *buffer) {
	json_object *root = json_tokener_parse(content);
	json_object *operation = json_object_object_get(root, "operation_id");

	if (operation != NULL) {
		strncpy(buffer, json_object_get_string(operation), MAX_BUFFER_LENGTH-2);
	}

	return size * nmemb;
}


CURLcode create_new_task(CURL *curl, operation_task *task) {
	PQExpBufferData payload;
	char *url = define_base_url(task);
	CURLcode ret;
	json_object *root = json_object_new_object();
	struct curl_slist *chunk = NULL;

	json_object_object_add(root, "operation_type", json_object_new_string(task->operation_type));
	json_object_object_add(root, "backup_id", json_object_new_string(task->backup_id));
	json_object_object_add(root, "remote_ssh_command", json_object_new_string(task->remote_ssh_command));
	json_object_object_add(root, "destination_directory", json_object_new_string(task->destination_directory));

	initPQExpBuffer(&payload);
	appendPQExpBufferStr(&payload, json_object_to_json_string(root));

	chunk = curl_slist_append(chunk, "Content-type: application/json");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.data);
	curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
	//curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, receive_operation_id);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, task->operation_id);
	ret = curl_easy_perform(curl);
	free(url);
	termPQExpBuffer(&payload);

	return ret;
}


size_t receive_operation_status(void *content, size_t size, size_t nmemb, char *buffer) {
	json_object *root = json_tokener_parse(content);
	json_object *status = json_object_object_get(root, "status");
	if (status != NULL) {
		strncpy(buffer, json_object_get_string(status), MAX_BUFFER_LENGTH-2);
	}
	else {
		fprintf(stderr, "Incorrect reply received for that operation ID.\n");
		strcpy(buffer, "\0");
	}
	return size * nmemb;
}

CURLcode get_status_of_operation(CURL *curl, operation_task *task) {
	CURLcode ret;
	char *url = define_base_url(task);

	strcat(url, "/");
	strcat(url, task->operation_id);
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, receive_operation_status);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, task->operation_status);

	ret = curl_easy_perform(curl);
	free(url);

	return ret;
}
