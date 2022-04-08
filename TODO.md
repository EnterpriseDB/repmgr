TODO
====

This file contains a list of improvements which are desirable and/or have
been requested, and which we aim to address/implement when time and resources
permit.

It is *not* a roadmap and there's no guarantee of any item being implemented
within any given timeframe.


Enable suspension of repmgrd failover
-------------------------------------

When performing maintenance, e.g. a switchover, it's necessary to stop all
repmgrd nodes to prevent unintended failover; this is obviously inconvenient.
We'll need to implement some way of notifying each repmgrd to suspend automatic
failover until further notice.

Requested in GitHub #410 ( https://github.com/EnterpriseDB/repmgr/issues/410 )
