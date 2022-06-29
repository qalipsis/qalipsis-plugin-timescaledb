CREATE SCHEMA events AUTHORIZATION qalipsis_user;
GRANT
ALL
ON SCHEMA events TO qalipsis_user;


CREATE SCHEMA meters AUTHORIZATION qalipsis_user;
GRANT
ALL
ON SCHEMA meters TO qalipsis_user;

ALTER
EXTENSION timescaledb_toolkit
UPDATE;