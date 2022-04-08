-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION repmgr UPDATE" to load this file. \quit

-- This script is intentionally empty and exists to skip the CREATE FUNCTION
-- commands contained in the 4.2--4.3 and 4.3--4.4 extension upgrade scripts,
-- which reference C functions which no longer exist in 5.3 and later.
--
-- These functions will be explicitly created in the 5.2--5.3 extension
-- upgrade step with the correct C function references.

