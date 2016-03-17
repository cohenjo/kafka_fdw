
CREATE EXTENSION kafka_fdw;

CREATE SERVER kafka_server
FOREIGN DATA WRAPPER kafka_fdw
OPTIONS (address '192.168.99.100', port '9092');

CREATE USER MAPPING FOR PUBLIC
SERVER kafka_server;


CREATE FOREIGN TABLE kafka_test_0 (message text, len int, offs int)
SERVER kafka_server OPTIONS (topic 'test', timeout '100', partition '0');


IMPORT FOREIGN SCHEMA test FROM SERVER kafka_server INTO test;

-- set client_min_messages to debug4;

select * from kafka_test_0;

drop foreign table kafka_test_0;
drop server kafka_server CASCADE;
DROP EXTENSION kafka_fdw;



CREATE FOREIGN TABLE kafka_testcsv_0 (word text,num int, len int, offs int)
SERVER kafka_server OPTIONS (timeout '100', topic 'testcsv', partition '0');

select * from kafka_testcsv_0 limit 1;

CREATE FOREIGN TABLE kafka_testcsv4_0 (word text,num int, len int, offs int)
SERVER kafka_server OPTIONS (timeout '100', topic 'testcsv4', partition '0');

select * from kafka_testcsv4_0 limit 1;
