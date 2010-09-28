drop table if exists repl_nodes;

/*
 * The table repl_nodes keeps information about all machines in
 * a cluster
 */
CREATE TABLE repl_nodes (
  id 		integer	primary key,
  cluster   text	not null,	-- Name to identify the cluster
  conninfo	text	not null 	
);

drop table if exists repl_status;

/*
 * Keeps monitor info about every node and their "position" relative 
 * to primary
 */
CREATE TABLE repl_monitor(
  primary_node                   INTEGER NOT NULL,
  standby_node                   INTEGER NOT NULL,
  last_monitor_time		    	 TIMESTAMP WITH TIME ZONE NOT NULL,
  last_wal_primary_location      TEXT NOT NULL,		
  last_wal_standby_location      TEXT NOT NULL,
  replication_lag                BIGINT NOT NULL, 
  apply_lag                      BIGINT NOT NULL  
);


/*
 * A useful view 
 */
CREATE VIEW repl_status AS
SELECT *, now() - (select max(last_monitor_time) from repl_monitor b
                    where b.primary_node = a.primary_node
                      and b.standby_node = a.standby_node)
  FROM repl_monitor;
