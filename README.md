repmgr: Replication Manager for PostgreSQL
==========================================

`repmgr` is a suite of open-source tools to manage replication and failover
within a cluster of PostgreSQL servers. It enhances PostgreSQL's built-in
replication capabilities with utilities to set up standby servers, monitor
replication, and perform administrative tasks such as failover or switchover
operations.

The most recent `repmgr` version (5.2.1) supports all PostgreSQL versions from
9.5 to 13. PostgreSQL 9.4 is also supported, with some restrictions.

`repmgr` is distributed under the GNU GPL 3 and maintained by EnterpriseDB.

Documentation
-------------

The full `repmgr` documentation is available here:

> [repmgr documentation](https://repmgr.org/docs/current/index.html)

The old `README` file for `repmgr` 3.x is available here:

> https://github.com/EnterpriseDB/repmgr/blob/REL3_3_STABLE/README.md

Note that the `repmgr` 3.x series is no longer supported and contains known bugs;
please upgrade to the [current repmgr version](https://repmgr.org/docs/current/appendix-release-notes.html)
as soon as possible.

Versions
--------

For an overview of `repmgr` versions and PostgreSQL compatibility, see the
[repmgr compatibility matrix](https://repmgr.org/docs/current/install-requirements.html#INSTALL-COMPATIBILITY-MATRIX).

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

EnterpriseDB provides 24x7 production support for `repmgr`, including
configuration assistance, installation verification and training for
running a robust replication cluster. For further details see:

* [EDB Support Services](https://www.enterprisedb.com/support/postgresql-support-overview-get-the-most-out-of-postgresql)

There is a mailing list/forum to discuss contributions or issues:

* https://groups.google.com/group/repmgr

The IRC channel #repmgr is registered with freenode.

Please report bugs and other issues to:

* https://github.com/EnterpriseDB/repmgr

Further information is available at https://repmgr.org/

We'd love to hear from you about how you use repmgr. Case studies and
news are always welcome.

Thanks from the repmgr core team.

* Ian Barwick
* Jaime Casanova
* Abhijit Menon-Sen
* Simon Riggs
* Cedric Villemain

Further reading
---------------

* [repmgr documentation](https://repmgr.org/docs/current/index.html)
* [How to Automate PostgreSQL 12 Replication and Failover with repmgr - Part 1](https://www.2ndquadrant.com/en/blog/how-to-automate-postgresql-12-replication-and-failover-with-repmgr-part-1/)
* [How to Automate PostgreSQL 12 Replication and Failover with repmgr - Part 2](https://www.2ndquadrant.com/en/blog/how-to-automate-postgresql-12-replication-and-failover-with-repmgr-part-2/)
* [How to implement repmgr for PostgreSQL automatic failover](https://www.enterprisedb.com/postgres-tutorials/how-implement-repmgr-postgresql-automatic-failover)
