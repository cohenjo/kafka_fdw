/*-------------------------------------------------------------------------
 *
 *                kafka foreign-data wrapper
 *
 * Copyright (c) 2013, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author:  Jony Vesterman Cohen <jony.cohenjo@gmail.com>
 *
 * IDENTIFICATION
 *                kafka_fdw/=sql/kafka_fdw.sql
 *
 *-------------------------------------------------------------------------
 */

CREATE FUNCTION kafka_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION kafka_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER kafka_fdw
  HANDLER kafka_fdw_handler
  VALIDATOR kafka_fdw_validator;
