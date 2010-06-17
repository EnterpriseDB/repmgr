/*
 * dbutils.h
 * Copyright (c) 2ndQuadrant, 2010
 *
 */

PGconn *establishDBConnection(const char *conninfo, const bool exit_on_error);
