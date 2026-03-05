# 需求

## 背景

starrocks数据库中已有数据库和表，现在新版本需要将表结构的排序键和索引需要修改，但是表列完全不变。为了在不迁移数据的前提下能查询到历史数据，将新老两张表的数据通过union all创建的视图作为查询入口。

## 需求描述

db下已经有了新表和旧表多个表。两者表名前缀相同，后缀不同。将这两者分别通过union all的方式创建逻辑视图。建视图的查询语句要包括所有列，切新旧两表列完全对齐。同时根据配置传入的时间戳t0和类型（秒级时间戳或者时间）作为视图内两表的查询过滤条件，例如，旧表where t <= t0,新表where t > t0。注意大部分表的时间列名和类型通过默认值配置，少部分表需要通过额外的配置制定。注意过滤用的t0时间是全局的，所有表都适用，只是需要根据数据类型适配。

## 参考配置

```json
{
        "host": "10.107.29.99",
        "port": 30113,
        "username": "root",
        "password": "StarRocks!@2025#.",
        "database": "business"
"default_timestamp_column":{
	"column": "recordTimestamp",
	"type": "bigint",
},
  "timestamp_columns": {
    "datalake_report_log": {
      "column": "insertTime",
      "type": "datetime"
    },
    "report_audit_log": {
      "column": "insertTime",
      "type": "datetime"
    },
    "datalake_platform_log": {
      "column": "insertTime",
      "type": "datetime"
    },
    "asset_log": {
      "column": "insertTime",
      "type": "datetime"
    },
    "weak_log":{
      "column": "insertTime",
      "type": "datetime"
    }
  },
"t0": "2025-12-25 12:00:00"
}
```
