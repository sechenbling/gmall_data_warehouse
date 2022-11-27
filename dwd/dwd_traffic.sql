-- 流量域页面浏览事务事实表
-- 建表语句
DROP TABLE IF EXISTS dwd_traffic_page_view_inc;
CREATE EXTERNAL TABLE dwd_traffic_page_view_inc
(
    `province_id`    STRING COMMENT '省份id',
    `brand`          STRING COMMENT '手机品牌',
    `channel`        STRING COMMENT '渠道',
    `is_new`         STRING COMMENT '是否首次启动',
    `model`          STRING COMMENT '手机型号',
    `mid_id`         STRING COMMENT '设备id',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`        STRING COMMENT '会员id',
    `version_code`   STRING COMMENT 'app版本号',
    `page_item`      STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`   STRING COMMENT '上页类型',
    `page_id`        STRING COMMENT '页面ID ',
    `source_type`    STRING COMMENT '来源类型',
    `date_id`        STRING COMMENT '日期id',
    `view_time`      STRING COMMENT '跳入时间',
    `session_id`     STRING COMMENT '所属会话id',
    `during_time`    BIGINT COMMENT '持续时间毫秒'
) COMMENT '页面日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_page_view_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_page_view_inc partition (dt='2020-06-14')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    page_item,
    page_item_type,
    last_page_id,
    page_id,
    source_type,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') view_time,
    concat(mid_id,'-',last_value(session_start_point,true) over (partition by mid_id order by ts)) session_id,
    during_time
from
(
    select
        common.ar area_code,
        common.ba brand,
        common.ch channel,
        common.is_new is_new,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code,
        page.during_time,
        page.item page_item,
        page.item_type page_item_type,
        page.last_page_id,
        page.page_id,
        page.source_type,
        ts,
        if(page.last_page_id is null,ts,null) session_start_point
    from ods_log_inc
    where dt='2020-06-14'
    and page is not null
)log
left join
(
    select
        id province_id,
        area_code
    from ods_base_province_full
    where dt='2020-06-14'
)bp
on log.area_code=bp.area_code;

-- 流量域启动事务事实表
-- 建表语句
DROP TABLE IF EXISTS dwd_traffic_start_inc;
CREATE EXTERNAL TABLE dwd_traffic_start_inc
(
    `province_id`     STRING COMMENT '省份id',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备id',
    `operate_system`  STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员id',
    `version_code`    STRING COMMENT 'app版本号',
    `entry`           STRING COMMENT 'icon手机图标 notice 通知',
    `open_ad_id`      STRING COMMENT '广告页ID ',
    `date_id`         STRING COMMENT '日期id',
    `start_time`      STRING COMMENT '启动时间',
    `loading_time_ms` BIGINT COMMENT '启动加载时间',
    `open_ad_ms`      BIGINT COMMENT '广告总共播放时间',
    `open_ad_skip_ms` BIGINT COMMENT '用户跳过广告时点'
) COMMENT '启动日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_start_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_start_inc partition(dt='2020-06-14')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    entry,
    open_ad_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') action_time,
    loading_time,
    open_ad_ms,
    open_ad_skip_ms
from
(
    select
        common.ar area_code,
        common.ba brand,
        common.ch channel,
        common.is_new,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code,
        `start`.entry,
        `start`.loading_time,
        `start`.open_ad_id,
        `start`.open_ad_ms,
        `start`.open_ad_skip_ms,
        ts
    from ods_log_inc
    where dt='2020-06-14'
    and `start` is not null
)log
left join
(
    select
        id province_id,
        area_code
    from ods_base_province_full
    where dt='2020-06-14'
)bp
on log.area_code=bp.area_code;

-- 流量域动作事务事实表
-- 建表语句
DROP TABLE IF EXISTS dwd_traffic_action_inc;
CREATE EXTERNAL TABLE dwd_traffic_action_inc
(
    `province_id`      STRING COMMENT '省份id',
    `brand`            STRING COMMENT '手机品牌',
    `channel`          STRING COMMENT '渠道',
    `is_new`           STRING COMMENT '是否首次启动',
    `model`            STRING COMMENT '手机型号',
    `mid_id`           STRING COMMENT '设备id',
    `operate_system`   STRING COMMENT '操作系统',
    `user_id`          STRING COMMENT '会员id',
    `version_code`     STRING COMMENT 'app版本号',
    `during_time`      BIGINT COMMENT '持续时间毫秒',
    `page_item`        STRING COMMENT '目标id ',
    `page_item_type`   STRING COMMENT '目标类型',
    `last_page_id`     STRING COMMENT '上页类型',
    `page_id`          STRING COMMENT '页面id ',
    `source_type`      STRING COMMENT '来源类型',
    `action_id`        STRING COMMENT '动作id',
    `action_item`      STRING COMMENT '目标id ',
    `action_item_type` STRING COMMENT '目标类型',
    `date_id`          STRING COMMENT '日期id',
    `action_time`      STRING COMMENT '动作发生时间'
) COMMENT '动作日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_action_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_action_inc partition(dt='2020-06-14')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    during_time,
    page_item,
    page_item_type,
    last_page_id,
    page_id,
    source_type,
    action_id,
    action_item,
    action_item_type,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') action_time
from
(
    select
        common.ar area_code,
        common.ba brand,
        common.ch channel,
        common.is_new,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code,
        page.during_time,
        page.item page_item,
        page.item_type page_item_type,
        page.last_page_id,
        page.page_id,
        page.source_type,
        action.action_id,
        action.item action_item,
        action.item_type action_item_type,
        action.ts
    from ods_log_inc lateral view explode(actions) tmp as action
    where dt='2020-06-14'
    and actions is not null
)log
left join
(
    select
        id province_id,
        area_code
    from ods_base_province_full
    where dt='2020-06-14'
)bp
on log.area_code=bp.area_code;

-- 流量域曝光事务事实表
-- 建表语句
DROP TABLE IF EXISTS dwd_traffic_display_inc;
CREATE EXTERNAL TABLE dwd_traffic_display_inc
(
    `province_id`       STRING COMMENT '省份id',
    `brand`             STRING COMMENT '手机品牌',
    `channel`           STRING COMMENT '渠道',
    `is_new`            STRING COMMENT '是否首次启动',
    `model`             STRING COMMENT '手机型号',
    `mid_id`            STRING COMMENT '设备id',
    `operate_system`    STRING COMMENT '操作系统',
    `user_id`           STRING COMMENT '会员id',
    `version_code`      STRING COMMENT 'app版本号',
    `during_time`       BIGINT COMMENT 'app版本号',
    `page_item`         STRING COMMENT '目标id ',
    `page_item_type`    STRING COMMENT '目标类型',
    `last_page_id`      STRING COMMENT '上页类型',
    `page_id`           STRING COMMENT '页面ID ',
    `source_type`       STRING COMMENT '来源类型',
    `date_id`           STRING COMMENT '日期id',
    `display_time`      STRING COMMENT '曝光时间',
    `display_type`      STRING COMMENT '曝光类型',
    `display_item`      STRING COMMENT '曝光对象id ',
    `display_item_type` STRING COMMENT 'app版本号',
    `display_order`     BIGINT COMMENT '曝光顺序',
    `display_pos_id`    BIGINT COMMENT '曝光位置'
) COMMENT '曝光日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_display_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_display_inc partition(dt='2020-06-14')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    during_time,
    page_item,
    page_item_type,
    last_page_id,
    page_id,
    source_type,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') display_time,
    display_type,
    display_item,
    display_item_type,
    display_order,
    display_pos_id
from
(
    select
        common.ar area_code,
        common.ba brand,
        common.ch channel,
        common.is_new,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code,
        page.during_time,
        page.item page_item,
        page.item_type page_item_type,
        page.last_page_id,
        page.page_id,
        page.source_type,
        display.display_type,
        display.item display_item,
        display.item_type display_item_type,
        display.`order` display_order,
        display.pos_id display_pos_id,
        ts
    from ods_log_inc lateral view explode(displays) tmp as display
    where dt='2020-06-14'
    and displays is not null
)log
left join
(
    select
        id province_id,
        area_code
    from ods_base_province_full
    where dt='2020-06-14'
)bp
on log.area_code=bp.area_code;

-- 流量域错误事务事实表
-- 建表语句
DROP TABLE IF EXISTS dwd_traffic_error_inc;
CREATE EXTERNAL TABLE dwd_traffic_error_inc
(
    `province_id`     STRING COMMENT '地区编码',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备id',
    `operate_system`  STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员id',
    `version_code`    STRING COMMENT 'app版本号',
    `page_item`       STRING COMMENT '目标id ',
    `page_item_type`  STRING COMMENT '目标类型',
    `last_page_id`    STRING COMMENT '上页类型',
    `page_id`         STRING COMMENT '页面ID ',
    `source_type`     STRING COMMENT '来源类型',
    `entry`           STRING COMMENT 'icon手机图标  notice 通知',
    `loading_time`    STRING COMMENT '启动加载时间',
    `open_ad_id`      STRING COMMENT '广告页ID ',
    `open_ad_ms`      STRING COMMENT '广告总共播放时间',
    `open_ad_skip_ms` STRING COMMENT '用户跳过广告时点',
    `actions`         ARRAY<STRUCT<action_id:STRING,item:STRING,item_type:STRING,ts:BIGINT>> COMMENT '动作信息',
    `displays`        ARRAY<STRUCT<display_type :STRING,item :STRING,item_type :STRING,`order` :STRING,pos_id
                                   :STRING>> COMMENT '曝光信息',
    `date_id`         STRING COMMENT '日期id',
    `error_time`      STRING COMMENT '错误时间',
    `error_code`      STRING COMMENT '错误码',
    `error_msg`       STRING COMMENT '错误信息'
) COMMENT '错误日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_error_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 数据装载
set hive.cbo.enable=false;
set hive.execution.engine=mr;
insert overwrite table dwd_traffic_error_inc partition(dt='2020-06-14')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    page_item,
    page_item_type,
    last_page_id,
    page_id,
    source_type,
    entry,
    loading_time,
    open_ad_id,
    open_ad_ms,
    open_ad_skip_ms,
    actions,
    displays,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') error_time,
    error_code,
    error_msg
from
(
    select
        common.ar area_code,
        common.ba brand,
        common.ch channel,
        common.is_new,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code,
        page.during_time,
        page.item page_item,
        page.item_type page_item_type,
        page.last_page_id,
        page.page_id,
        page.source_type,
        `start`.entry,
        `start`.loading_time,
        `start`.open_ad_id,
        `start`.open_ad_ms,
        `start`.open_ad_skip_ms,
        actions,
        displays,
        err.error_code,
        err.msg error_msg,
        ts
    from ods_log_inc
    where dt='2020-06-14'
    and err is not null
)log
join
(
    select
        id province_id,
        area_code
    from ods_base_province_full
    where dt='2020-06-14'
)bp
on log.area_code=bp.area_code;
