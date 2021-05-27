# batch-data-pipeline-exercise

## 问题描述

给定两个数据集：“商品主数据”和“订单事件数据”，计算相应指标。

**商品主数据**

商品主数据（products）是每个源系统里的商品全量数据，每日更新，定义如下

|字段|类型|说明|
|---|---|---|
|id|字符串|商品ID，在每个源系统中是唯一的|
|title|字符串|商品标题|
|category|字符串|商品类别，可能会会变化|
|price|指导价格|可能会变化|

**订单事件数据**（增量数据，每日更新）

订单事件数据（order_events）包含了前一天内属性发生变化的订单数据。

|字段|类型|说明|
|---|---|---|
|id|字符串|订单ID|
|productId|字符串|商品ID，对应商品主数据里的ID。每个订单只包含同一个商品ID|
|amount|数字|商品数量|
|status|字符串|当前的当前状态。目前包括created，completed，deleted 状态，但以后还可能有其他状态|
|timestamp|时间戳|事件发生的时间|

注意，id 可能有重复，但 id 和 timestamp组合起来是唯一的。

计算如下指标：

1. 当前已创建未完成的订单数
2. 最近2年创建的订单中，订单创建数，按每个季度
3. 最近2年创建的订单中，订单创建数，按每个季度每个产品类别查看
4. 计算本月之前创建，但还未完成的订单数
5. 计算2年内，每个月当月创建，但没有完成的订单数

并通过BI工具创建对应的图表。

## 准备工作

安装如下工具

1. docker 和 docker-compose
2. 编辑器如 Visual Studio Code
3. PostgreSQL GUI 工具，比如搜索VS Code的相关扩展

熟悉以下编程知识

1. Python
2. SQL

## 快速链接

* Airflow http://localhost:8080
* Metabase http://localhost:8082