repmgr: Replication Manager for PostgreSQL
==========================================

`repmgr` is a suite of open-source tools to manage replication and failover
within a cluster of PostgreSQL servers. It enhances PostgreSQL's built-in
replication capabilities with utilities to set up standby servers, monitor
replication, and perform administrative tasks such as failover or switchover
operations.

`repmgr 4` is a complete rewrite of the existing `repmgr` codebase, allowing
the use of all of the latest features in PostgreSQL replication.

PostgreSQL 10, 9.6 and 9.5 are fully supported.
PostgreSQL 9.4 and 9.3 are supported, with some restrictions.

`repmgr` is distributed under the GNU GPL 3 and maintained by 2ndQuadrant.

### BDR support

`repmgr 4` supports monitoring of a two-node BDR 2.0 cluster on PostgreSQL 9.6
only. Note that BDR 2.0 is not publicly available; please contact 2ndQuadrant
for details. `repmgr 4` will support future public BDR releases.


Documentation
-------------

The main `repmgr` documentation is available here:

> [repmgr 4 documentation](https://repmgr.org/docs/4.0/index.html)

The `README` file for `repmgr` 3.x is available here:

> https://github.com/2ndQuadrant/repmgr/blob/REL3_3_STABLE/README.md


Files
------

 - `CONTRIBUTING.md`: details on how to contribute to `repmgr`
 - `COPYRIGHT`: Copyright information
 - `HISTORY`: Summary of changes in each `repmgr` release
 - `LICENSE`: GNU GPL3 details


Directories
-----------

 - `contrib/`: additional utilities
 - `doc/`: DocBook-based documentation files
 - `expected/`: expected regression test output
 - `scripts/`: example scripts
 - `sql/`: regression test input


Support and Assistance
----------------------

2ndQuadrant provides 24x7 production support for `repmgr`, including
configuration assistance, installation verification and training for
running a robust replication cluster. For further details see:

* http://2ndquadrant.com/en/support/

There is a mailing list/forum to discuss contributions or issues:

* http://groups.google.com/group/repmgr

The IRC channel #repmgr is registered with freenode.

Please report bugs and other issues to:

* https://github.com/2ndQuadrant/repmgr

Further information is available at http://www.repmgr.org/

We'd love to hear from you about how you use repmgr. Case studies and
news are always welcome. Send us an email at info@2ndQuadrant.com, or
send a postcard to

    repmgr
    c/o 2ndQuadrant
    7200 The Quorum
    Oxford Business Park North
    Oxford
    OX4 2JZ
    United Kingdom

Thanks from the repmgr core team.

* Ian Barwick
* Jaime Casanova
* Abhijit Menon-Sen
* Simon Riggs
* Cedric Villemain

Further reading
---------------

* http://blog.2ndquadrant.com/repmgr-3-2-is-here-barman-support-brand-new-high-availability-features/
* http://blog.2ndquadrant.com/improvements-in-repmgr-3-1-4/
* http://blog.2ndquadrant.com/managing-useful-clusters-repmgr/
* http://blog.2ndquadrant.com/easier_postgresql_90_clusters/
