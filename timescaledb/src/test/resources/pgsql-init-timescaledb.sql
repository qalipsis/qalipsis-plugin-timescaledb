CREATE SCHEMA the_events AUTHORIZATION qalipsis_user;
GRANT ALL ON SCHEMA the_events TO qalipsis_user;

CREATE SCHEMA the_meters AUTHORIZATION qalipsis_user;
GRANT ALL ON SCHEMA the_meters TO qalipsis_user;

ALTER EXTENSION timescaledb_toolkit UPDATE;