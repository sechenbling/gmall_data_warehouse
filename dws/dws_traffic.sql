-- 流量域会话粒度页面浏览最近1日汇总表
-- 建表语句
DROP TABLE IF EXISTS dws_traffic_session_page_view_1d;
CREATE EXTERNAL TABLE dws_traffic_session_page_view_1d
(
    `session_id`     STRING COMMENT '会话id',
    `mid_id`         string comment '设备id',
    `brand`          string comment '手机品牌',
    `model`          string comment '手机型号',
    `operate_system` string comment '操作系统',
    `version_code`   string comment 'app版本号',
    `channel`        string comment '渠道',
    `during_time_1d` BIGINT COMMENT '最近1日访问时长',
    `page_count_1d`  BIGINT COMMENT '最近1日访问页面数'
) COMMENT '流量域会话粒度页面浏览最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_traffic_session_page_view_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据装载
insert overwrite table dws_traffic_session_page_view_1d partition(dt='2020-06-14')
select
    session_id,
    mid_id,
    brand,
    model,
    operate_system,
    version_code,
    channel,
    sum(during_time),
    count(*)
from dwd_traffic_page_view_inc
where dt='2020-06-14'
group by session_id,mid_id,brand,model,operate_system,version_code,channel;

-- 流量域访客页面粒度页面浏览最近1日汇总表
-- 建表语句
DROP TABLE IF EXISTS dws_traffic_page_visitor_page_view_1d;
CREATE EXTERNAL TABLE dws_traffic_page_visitor_page_view_1d
(
    `mid_id`         STRING COMMENT '访客id',
    `brand`          string comment '手机品牌',
    `model`          string comment '手机型号',
    `operate_system` string comment '操作系统',
    `page_id`        STRING COMMENT '页面id',
    `during_time_1d` BIGINT COMMENT '最近1日浏览时长',
    `view_count_1d`  BIGINT COMMENT '最近1日访问次数'
) COMMENT '流量域访客页面粒度页面浏览最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_traffic_page_visitor_page_view_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据装载
insert overwrite table dws_traffic_page_visitor_page_view_1d partition(dt='2020-06-14')
select
    mid_id,
    brand,
    model,
    operate_system,
    page_id,
    sum(during_time),
    count(*)
from dwd_traffic_page_view_inc
where dt='2020-06-14'
group by mid_id,brand,model,operate_system,page_id;


-- 流量域访客页面粒度页面浏览最近n日汇总表
-- 建表语句
DROP TABLE IF EXISTS dws_traffic_page_visitor_page_view_nd;
CREATE EXTERNAL TABLE dws_traffic_page_visitor_page_view_nd
(
    `mid_id`          STRING COMMENT '访客id',
    `brand`           string comment '手机品牌',
    `model`           string comment '手机型号',
    `operate_system`  string comment '操作系统',
    `page_id`         STRING COMMENT '页面id',
    `during_time_7d`  BIGINT COMMENT '最近7日浏览时长',
    `view_count_7d`   BIGINT COMMENT '最近7日访问次数',
    `during_time_30d` BIGINT COMMENT '最近30日浏览时长',
    `view_count_30d`  BIGINT COMMENT '最近30日访问次数'
) COMMENT '流量域访客页面粒度页面浏览最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_traffic_page_visitor_page_view_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据装载
insert overwrite table dws_traffic_page_visitor_page_view_nd partition(dt='2020-06-14')
select
    mid_id,
    brand,
    model,
    operate_system,
    page_id,
    sum(if(dt>=date_add('2020-06-14',-6),during_time_1d,0)),
    sum(if(dt>=date_add('2020-06-14',-6),view_count_1d,0)),
    sum(during_time_1d),
    sum(view_count_1d)
from dws_traffic_page_visitor_page_view_1d
where dt>=date_add('2020-06-14',-29)
and dt<='2020-06-14'
group by mid_id,brand,model,operate_system,page_id;
