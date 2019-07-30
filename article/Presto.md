## 背景
随着AI营销平台业务量的增长，数据报表需求越来越多，场景也越来越复杂，原有的烟囱式开发方式，不仅开发周期长，同时也浪费存储和计算资源。

在应用Presto之前，面临一系列的问题：

* 由于业务场景的复杂性，离线及实时报表数据存储在MySQL/Hive/Hbase/Kylin/Druid等多个存储引擎中，多个存储引擎之间比较难进行数据关联。
* 缺少一个数据查询服务，能够为不同的角色（例如数据分析师，开发，产品和运营等），提供支持标准SQL、交互式的数据查询功能，而无需关心具体的存储引擎。并能够对接odeon BI（公司级的BI产品，支持页面化的拖拽配置，通过SQL查询构建报表页面），方便快速的开发报表，降低开发周期。
* 数据查询来源于不同的部分和系统，不方便数据侧的统计及管理。

为了解决上述遇到的各类问题，同时参考了业界其他公司的大数据解决方案，我们调研了多个查询引擎，考虑到后续的定制化需求，最终选择Presto来实现数据查询服务。Presto以下特性能够满足我们的需求：
![3d81fdfa1db15b3b996058c1cd1f1ecb.png](en-resource://database/1606:1)

本文将介绍Presto的系统设计以及在AI营销平台的落地情况，内容包括：

* Presto系统设计，包括架构设计
* Presto Kylin/Druid Connector定制开发、条件下推优化
* 总结和展望


## Presto的设计
#### 架构设计
![66cfb96239174979a8c34b8d151ae270.png](en-resource://database/1608:1)
Presto包括Coordinator（包括元数据管理模块、SQL解析模块和分布式任务调度模块）、Worker（包括任务执行模块和Connector模块等）和客户端（包括jdbc和http两种连接方式）。主要模块具体功能和职责为：

**元数据管理模块：** 通过对不同底层数据存储引擎的API动态调用，映射到Presto内部的库表schema信息。

**SQL解析模块模块：** 对一条查询语句依次进行词法分析、语法分析、语义分析、执行计划生成、执行计划优化、执行计划分段的处理流程，如下图： 
![7e240826cd2c7d907e45e79ee697bc49.png](en-resource://database/1610:1)

* 通过目前流行的语言识别工具ANTLR4来构建词法分析器、语法分析器和树状分析器等各个模块，sql语句根据分词规则及语法规则组装成抽象语法树（AST）。
* 语义分析器主要用来绑定元数据，通过visitor模式遍历AST，将树种的表、字段绑定具体的元数据信息。例如某个字段A，绑定它关联的表，字段类型等。
* 执行计划生成负责将语义分析后的AST，转成逻辑执行计划，区分SQL不同语义，执行不同的逻辑操作。例如两表join操作转成逻辑执行计划如下图。执行计划优化是对执行计划优化的功能，例如外层语句的查询过滤条件，有些情况可以下推到具体的源表查询逻辑中，以减少数据量的获取，提高查询效率。优化规则可以在 PlanOptimizers和IterativeOptimizer找到。规则分两类，第一类都是通过 visitor 模式来对树进行改写；第二类的规则都是通过 pattern match 来触发。
![429ff749c78fc079a5cb8b0369c6e137.png](en-resource://database/1612:1)
* 执行计划分段的最重要目的就是能够以分片(splited)方式运输和执行在分布式节点上。分布式sql引擎相比于传统数据库引擎最大的区别之一就是并发度理论上可以无限横向扩展。例如上图中两个query plan的ProjectNode水平拆分到不同worker节点上运行，其它worker节点上通过网络传输拉取数据执行JoinNode的关联操作。

**SQL解析模块模块是Presto的核心部分，也是数据侧定制化重点修改部分**

**Connector模块：** Connector是Presto对外与其它存储或计算引擎交互的模块，主要实现获取元数据信息以及查询功能。遵循Java的SPI规范，基于接口编程，实现模块间的解耦，方便我们轻松自定义Connector。只需实现Kylin/Druid Connector即可实现Presto查询Kylin/Druid。


## Presto Kylin/Druid Connector定制开发、条件下推优化
由于Kylin和Druid存储设计及查询的特殊性，且语法并不完全遵循ANSI SQL标准，Presto官方并未提供Connector的实现。数据侧在定制开发和使用时，需要解决的问题包括：

1. 如果将Kylin/Druid只是作为存储，将计算全部交由Presto，查询时往往要拉取全量数据，能否满足性能要求？
2. 对于区间过滤条件，算数表达式如何实现下推优化？
3. 对于odeon BI 日期格式无法支持Druid如何进行适配？
4. 针对UDF无法下推的情况，如何优化查询效率？

为了解决上述问题，我们讨论了多种方式，决定采用MYSQL存储特定优化条件，并结合业务场景和需求，做了一些深度定制，最终解决方案如下：
![e1b92dd46aaebbc2412aa4aae99a12ca.png](en-resource://database/1614:1)

**针对问题1：** Presto在查询底层表的时候，会拉取计算所需字段的全量数据。但是对于Druid这样的存储系统，即使有过滤条件下推的优化，依然会拉取大量数据。例如Druid一张表存放的最细粒度是分钟，如果直接按小时维度查询，Druid会自动聚合分钟数据为小时并返回，而Presto查询则拉取全量分钟数据进行计算。数据量过大不仅影响了Presto的执行效率，也没有充分利用Druid本身的计算性能。数据侧最终通过采用将sql下推的方案，将部分计算交由Druid执行，减少了拉取的数据量，提高计算效率。执行原理详见下图：
![aa5868bda0c9fb63e5af0ff24d080b9f.png](en-resource://database/1616:1)
```
例1 统计2019-07-11号当天物料的下发，曝光和点击数
select img_url, sum(win), sum(impress), sum(click) from druid_table where __time between timestamp '2019-07-11 00:00:00' and timestamp '2019-07-11 23:59:59' group by img_url

例1优化前，Presto调用Druid查询sql
select img_url, win, impress, click from druid_table where __time between timestamp '2019-07-11 00:00:00' and timestamp '2019-07-11 23:59:59'
如果是Druid配置按分钟聚合数据，则相当于每分钟的数据都拉取到Presto进行计算

例1优化后，Presto调用Druid查询sql
select img_url, sum(win), sum(impress), sum(click) from druid_table where __time between timestamp '2019-07-11 00:00:00' and timestamp '2019-07-11 23:59:59' group by img_url

小结：时间范围内，每分钟数据已经聚合成天，查询时间从原有的分钟级降低到秒级
```

**针对问题2：** Presto在处理例如join on等条件时只能支持简单等值表达式的下推优化，过滤条件无法下推，导致全表数据拉取的情况。由于表达式复杂程度不定，Presto官方未对此场景做相关优化。数据侧最终通过支持简单加减乘除算数表达式逆推的方案，使得条件下推，提高计算效率。
```
例2 统计平台27，广告位37398在2019-02-23和2019-02-24平台收入比较（等值条件表达式支持下推）
select * from (select STATIS_DAILY day_time, plat_income from mysql_table where adunit_id = 37398 and plat_id = 27) a full join (select STATIS_DAILY day_time, plat_income from mysql_table where adunit_id = 37398 and plat_id = 27) b on b.day_time - 86400 = a.day_time where a.day_time = 1550851200;

例2中，外层过滤条件a.day_time = 1550851200，会下推到子查询a、b中，最终Presto在mysql中执行的查询如下：
a：select STATIS_DAILY day_time, plat_income from mysql_table where adunit_id = 37398 and plat_id = 27 and day_time = 1550851200
b：select STATIS_DAILY day_time, plat_income from mysql_table where adunit_id = 37398 and plat_id = 27 and day_time = 1550851200 + 86400

例3 统计平台27，广告位37398在2019-02-23和2019-02-24平台收入比较（范围表达式不支持下推）
select * from (select STATIS_DAILY day_time, plat_income from mysql_table where adunit_id = 37398 and plat_id = 27) a full join (select STATIS_DAILY day_time, plat_income from mysql_table where adunit_id = 37398 and plat_id = 27) b on b.day_time - 86400 = a.day_time where a.day_time >= 1550851200;

例3中，外层过滤条件a.day_time >= 1550851200，针对范围过滤条件，Presto不会下推到子查询b中，最终导致全时间范围数据拉取，最终Presto在mysql中执行的查询如下：
a：select STATIS_DAILY day_time, plat_income from mysql_table where adunit_id = 37398 and plat_id = 27 and day_time >= 1550851200
b：select STATIS_DAILY day_time, plat_income from mysql_table where adunit_id = 37398 and plat_id = 27

优化后b：select STATIS_DAILY day_time, plat_income from mysql_table where adunit_id = 37398 and plat_id = 27 and day_time >= 1550851200 + 86400

优化步骤：
    1.得到外层过滤条件a.day_time >= 1550851200
    2.得到关联条件b.day_time - 86400 = a.day_time替换过滤值，得到b.day_time - 86400 >= 1550851200
    3.进行表达式逆推，得到最终表b过滤添加b.day_time >= a.day_time + 86400
小结：减少了表b的查询数据量，查询时间从原有的分钟级降低到秒级
```

**针对问题3：** 由于Presto执行计划设计是提取表字段数据（详见问题1图），得到结果数据会和表字段类型进行匹配验证。经过SQL下推后odeon BI对Druid的日期UDF使用会报错，因此为了兼容做了一些特殊处理。
```
例4 获取分小时的平台广告位物料信息
select plat_id, format_datetime(__time,'yyyy年MM月dd日HH时'), ad_unit_id, img_url, title, desc from druid_table group by plat_id, format_datetime(__time,'yyyy年MM月dd日HH时'), ad_unit_id, img_url, title, desc limit 100

执行计划查询字段如下，Presto限制返回字段必须是表中类型字段，format_datetime(__time,'yyyy年MM月dd日HH时')则会提取出__time
plat_id, __time, ad_unit_id, img_url, title, desc

实际Presto SQL下推后查询Druid返回字段
plat_id, format_datetime(__time,'yyyy年MM月dd日HH时'), ad_unit_id, img_url, title, desc
这里format_datetime(__time,'yyyy年MM月dd日HH时')返回string类型，和__time的timestamp类型不一致，会导致查询失败，而BI这里需要返回字符串类型展示

修改逻辑：
    1.修改执行计划，提取plat_id, __time, ad_unit_id, img_url, title, desc等信息。
    2.下推SQL改造成：select plat_id, time_parse(format_datetime(__time,'yyyy年MM月dd日HH时'),'yyyy年MM月dd日HH时','+0800'), ad_unit_id, img_url, title, desc from druid_table group by plat_id, format_datetime(__time,'yyyy年MM月dd日HH时'), ad_unit_id, img_url, title, desc limit 100。为format_datetime增加了类型转换UDF time_parse，保证返回数据类型与表字段__time类型一致。
    3.返回日期经format_datetime转换成BI需要的格式。
```

**针对问题4：** 由于udf为Presto自定义的。在当前Presto架构下，为了下推udf需要逆向解析，并适配每个存储引擎自带的udf，非常难以实现，故官方未支持。但由于BI限制，很多需求场景需要该功能，数据侧决定通过定制化开发满足该需求。
```
例5 
select * from (select __time, img_url, sum(win), sum(impress), sum(click), sum(spend) from druid_table group by img_url) a where format_datetime(a.__time, 'yyyy-MM-dd HH') >= '2019-07-10 11';

优化前Presto在Druid中执行的查询如下
select __time, img_url, sum(win), sum(impress), sum(click), sum(spend) from druid_table group by img_url

优化后Presto在Druid中执行的查询如下
select __time, img_url, sum(win), sum(impress), sum(click), sum(spend) from druid_table group by img_url where format_datetime(__time, 'yyyy-MM-dd HH') >= '2019-07-10 11';

优化步骤：
    1.得到外层无法下推的udf，format_datetime(a.__time, 'yyyy-MM-dd HH') >= '2019-07-10 11'，存储到Mysql中
    2.在对Druid表进行查询时，判断表和列是否匹配。满足需求，则将过滤条件补充到查询SQL中format_datetime(__time, 'yyyy-MM-dd HH') >= '2019-07-10 11'
小结：将原先的全表数据拉取，添加了过滤条件，减少了数据量的摄入。
```


## 总结和展望
Presto从2018年10月开始开发，11月份正式投入生产。目前调度集群包括1台Coordinator和3台 Worker节点，并进行了高可用部署和节点监控。 自投入生产后，新的报表开发已迁移到Presto，明显提升数据交付时间；很多业务开发的数据查询需求也迁移到Presto，基本满足日常对数据查询的大部分使用场景。 同时我们也意识到这块还有很多可以挖掘和提升的点，未来我们可能会从这些方面进一步完善Presto的功能和提升用户体验：

* 由于定制化开发，造成sql编写有许多限制条件，后续我们会改善这部分，提高易用性
* 对查询sql进行优化，提升查询效率
* 提供查询监控功能，对不同用户查询进行监控，记录慢查询