repmgrd's failover algorithm
============================

When implementing automatic failover, there are two factors which are critical in
ensuring the desired result is achieved:

  - has the master node genuinely failed?
  - which is the best node to promote to the new master?

This document outlines repmgrd's decision-making process during automatic failover
for standbys directly connected to the master node.


Master node failure detection
-----------------------------

If a `repmgrd` instance running on a PostgreSQL standby node is unable to connect to
the master node, this doesn't neccesarily mean that the master is down and a
failover is required. Factors such as network connectivity issues could mean that
even though the standby node is isolated, the replication cluster as a whole
is functioning correctly, and promoting the standby without further verification
could result in a "split-brain" situation.

In the event that `repmgrd` is unable to connect to the master node, it will attempt
to reconnect to the master server several times (as defined by the `reconnect_attempts`
parameter in `repmgr.conf`), with reconnection attempts  occuring at the interval
specified by `reconnect_interval`. This happens to verify that the master is definitively
not accessible (e.g. that connection was not lost due to a brief network glitch).

Appropriate values for these settings will depend very much on the replication
cluster environment. There will necessarily be a trade-off between the time it
takes to assume the master is not reachable, and the reliability of that conclusion.
A standby in a different physical location to the master will probably need a longer
check interval to rule out possible network issues, whereas one located in the same
rack with a direct connection between servers could perform the check very quickly.

Note that it's possible the master comes back online after this point is reached,
but before a new master has been selected; in this case it will be noticed
during the selection of a new master and no actual failover will take place.

Promotion candidate selection
-----------------------------

Once `repmgrd` has decided the master is definitively unreachable, following checks
will be carried out:

* attempts to connect to all other nodes in the cluster (including the witness
  node, if defined) to establish the state of the cluster, including their
  current LSN

* If less than half of the nodes are visible (from the viewpoint
  of this node), `repmgrd` will not take any further action. This is to ensure that
  e.g. if a replication cluster is spread over multiple data centres, a split-brain
  situation does not occur if there is a network failure between datacentres. Note
  that if nodes are split evenly between data centres, a witness server can be
  used to establish the "majority" data centre.

* `repmgrd` polls all visible servers and waits for each node to return a valid LSN;
  it updates the LSN previously  stored for this node if it has increased since
  the initial check

* once all LSNs have been retrieved, `repmgrd` will check for the highest LSN; if
  its own node has the highest LSN, it will attempt to promote itself (using the
  command defined in `promote_command` in `repmgr.conf`. Note that if using
  `repmgr standby promote` as the promotion command, and the original master becomes available
  before the promotion takes effect, `repmgr` will return an error and no promotion
  will take place, and `repmgrd` will resume monitoring as usual.

* if the node is not the promotion candidate, `repmgrd` will execute the
  `follow_command` defined in `repmgr.conf`. If using `repmgr standby follow` here,
  `repmgr` will attempt to detect the new master node and attach to that.




