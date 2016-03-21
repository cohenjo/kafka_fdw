
set client_min_messages to error;
CREATE EXTENSION IF NOT EXISTS kafka_fdw;

CREATE SERVER kafka_server
FOREIGN DATA WRAPPER kafka_fdw
OPTIONS (address '127.0.0.1', port '9092');
-- OPTIONS (address '192.168.99.100', port '9092');

CREATE USER MAPPING FOR PUBLIC
SERVER kafka_server;


CREATE FOREIGN TABLE kafka_test_0 (message text, len int, offs int)
SERVER kafka_server OPTIONS (topic 'test', timeout '100', partition '0');


IMPORT FOREIGN SCHEMA test FROM SERVER kafka_server INTO test;

-- set client_min_messages to debug4;

insert into kafka_test_0 values ('hello world');
-- Kafka is A-Sync, make sure we have enough time to let the message arrive
SELECT pg_sleep(10);

select * from kafka_test_0;

drop foreign table kafka_test_0;
drop server kafka_server CASCADE;
DROP EXTENSION kafka_fdw;
