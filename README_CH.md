repmgr: Postgresql流复制管理器
==========================================


`repmgr`是一套用于Postgresql服务器集群流复制管理和故障转移的开源工具，一般用于搭建Postgresql的高可用集群。它通过通用组件设置多个备库、流复制监控、以及执行故障转移和故障切换等管理任务，极大的增强了Postgresql内置的流复制功能。

最新的`repmgr`支持从9.5到15的所有PostgreSQL版本，当然也支持PostgreSQL 9.4 ，但是会有一些限制。

`repmgr` 采用GNU GPL 3 开源，并且由EnterpriseDB 负责维护。

文档
-------------

完整的 `repmgr` 文档可以在这里获得：

> [repmgr文档](https://repmgr.org/docs/current/index.html)

版本
--------

关于`repmgr`版本和PostgreSQL兼容性可以查看如下文档：
[repmgr兼容性矩阵](https://repmgr.org/docs/current/install-requirements.html#INSTALL-COMPATIBILITY-MATRIX).

文件
------

 - `CONTRIBUTING.md`: 如何为 `repmgr`共享代码
 - `COPYRIGHT`: 版权信息
 - `HISTORY`: 每个 `repmgr` 发布版本的变更信息
 - `LICENSE`: GNU GPL3 细节文件


目录
-----------

 - `contrib/`: 额外的通用组件
 - `doc/`: 基于DocBook的文档文件
 - `expected/`: 回归测试的预期结果文件
 - `sql/`: 回归测试输入文件


支持和协助
----------------------
EnterpriseDB 为repmgr提供 24x7（全天候）产品服务，具体包括搭建运行高可用流复制集群的配置帮助、安装验证和培训。详细信息请参考如下链接内容：

* [EDB提供的服务](https://www.enterprisedb.com/support/postgresql-support-overview-get-the-most-out-of-postgresql)

如下是讨论问题或共享代码的邮件列表或论坛：

* https://groups.google.com/group/repmgr


IRC 频道 #repmgr 已注册到 freenode。

请将错误和其他问题提交到如下地址：

* https://github.com/EnterpriseDB/repmgr

想了解更多信息，请访问https://repmgr.org/


期待您向我们分享如何使用`repmgr`，并且欢迎分享`repmgr`的相关新闻和使用案例。

感谢repmgr的如下核心团队成员：

* Jaime Casanova
* Abhijit Menon-Sen
* Simon Riggs
* Cedric Villemain

延伸阅读
---------------

* [repmgr文档](https://repmgr.org/docs/current/index.html)
* [如何使用repmgr进行Postgresql自动流复制维护和自动故障转移-第一部分](https://www.2ndquadrant.com/en/blog/how-to-automate-postgresql-12-replication-and-failover-with-repmgr-part-1/)
* [如何使用repmgr进行Postgresql自动流复制维护和自动故障转移-第二部分](https://www.2ndquadrant.com/en/blog/how-to-automate-postgresql-12-replication-and-failover-with-repmgr-part-2/)
* [如何用repmgr实现Postgresql自动故障转移](https://www.enterprisedb.com/postgres-tutorials/how-implement-repmgr-postgresql-automatic-failover)
