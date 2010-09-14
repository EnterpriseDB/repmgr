CREATE TABLE repl_nodes (
  id 		integer	primary key,
  cluster   text	not null,
  conninfo	text	not null
);

drop table if exists repl_status;

CREATE TABLE repl_status(
  primary_node                   INTEGER NOT NULL,
  standby_node                   INTEGER NOT NULL,
  last_monitor_timestamp    	 TIMESTAMP WITH TIME ZONE NOT NULL,
  last_wal_primary_location      TEXT NOT NULL,
  last_wal_standby_location      TEXT NOT NULL,
  last_wal_standby_timestamp	 TIMESTAMP WITH TIME ZONE NOT NULL,
  replication_lag                BIGINT NOT NULL,
  apply_lag                      BIGINT NOT NULL
);
