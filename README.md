# Kafka Foreign Data Wrapper for PostgreSQL
Copyright (c) 2015, Jony Vesterman Cohen.

[![Build Status](https://travis-ci.org/cohenjo/kafka_fdw.svg?branch=master)](https://travis-ci.org/cohenjo/kafka_fdw)

The Kafka Foreign Data Wrapper (FDW) for PostgreSQL allows to consume and produce KAFKA messages as PostgreSQL tables.

kafka topic == foreign table.
consume == select
produce == insert

The FDW uses the librdkafka C client library.
https://github.com/edenhill/librdkafka
librdkafka is licensed under the 2-clause BSD license.

This FDW allows:
1. importing topics as foreign tables (by imort schema)
2. consuming topics using select statments.
3. producing  using insert statments.


CSV format - limitation: must end with ','

TODO List:
  1. consider moving to : rd_kafka_consume_callback for higher throughput
  2. look at: https://github.com/edenhill/librdkafka/blob/master/examples/rdkafka_performance.c from line 1180
