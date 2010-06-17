/*
 * config.h
 * Copyright (c) 2ndQuadrant, 2010
 *
 */

void parse_config(char *cluster_name, int *node, char *service);
void parse_line(char *buff, char *name, char *value);
char *trim(char *s);
