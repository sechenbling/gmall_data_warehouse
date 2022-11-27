-- 交易域加购物车事务事实表 建表
DROP TABLE IF EXISTS dwd_trade_cart_add_inc;
CREATE EXTERNAL TABLE dwd_trade_cart_add_inc
(
    `id`               STRING COMMENT '编号',
    `user_id`          STRING COMMENT '用户id',
    `sku_id`           STRING COMMENT '商品id',
    `date_id`          STRING COMMENT '时间id',
    `create_time`      STRING COMMENT '加购时间',
    `source_id`        STRING COMMENT '来源类型ID',
    `source_type_code` STRING COMMENT '来源类型编码',
    `source_type_name` STRING COMMENT '来源类型名称',
    `sku_num`          BIGINT COMMENT '加购物车件数'
) COMMENT '交易域加购物车事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_add_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 交易域加购物车事务事实表 首日数据装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_cart_add_inc partition (dt)
select id,
       user_id,
       sku_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       source_id,
       source_type,
       dic.dic_name,
       sku_num,
       date_format(create_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.user_id,
                data.sku_id,
                data.create_time,
                data.source_id,
                data.source_type,
                data.sku_num
         from ods_cart_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) ci
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '24'
     ) dic
     on ci.source_type = dic.dic_code;
-- 交易域加购物车事务事实表 每日数据装载
insert overwrite table dwd_trade_cart_add_inc partition (dt = '2020-06-15')
select id,
       user_id,
       sku_id,
       date_id,
       create_time,
       source_id,
       source_type_code,
       source_type_name,
       sku_num
from (
         select data.id,
                data.user_id,
                data.sku_id,
                date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd')          date_id,
                date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') create_time,
                data.source_id,
                data.source_type                                                           source_type_code,
                if(type = 'insert', data.sku_num, data.sku_num - old['sku_num'])           sku_num
         from ods_cart_info_inc
         where dt = '2020-06-15'
           and (type = 'insert'
             or (type = 'update' and old['sku_num'] is not null and data.sku_num > cast(old['sku_num'] as int)))
     ) cart
         left join
     (
         select dic_code,
                dic_name source_type_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '24'
     ) dic
     on cart.source_type_code = dic.dic_code;

-- 交易域下单事务事实表
DROP TABLE IF EXISTS dwd_trade_order_detail_inc;
CREATE EXTERNAL TABLE dwd_trade_order_detail_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单id',
    `user_id`               STRING COMMENT '用户id',
    `sku_id`                STRING COMMENT '商品id',
    `province_id`           STRING COMMENT '省份id',
    `activity_id`           STRING COMMENT '参与活动规则id',
    `activity_rule_id`      STRING COMMENT '参与活动规则id',
    `coupon_id`             STRING COMMENT '使用优惠券id',
    `date_id`               STRING COMMENT '下单日期id',
    `create_time`           STRING COMMENT '下单时间',
    `source_id`             STRING COMMENT '来源编号',
    `source_type_code`      STRING COMMENT '来源类型编码',
    `source_type_name`      STRING COMMENT '来源类型名称',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '交易域下单明细事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_order_detail_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 交易域下单事务事实表 首日数据装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_detail_inc partition (dt)
select od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       source_id,
       source_type,
       dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount,
       date_format(create_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.create_time,
                data.source_id,
                data.source_type,
                data.sku_num,
                data.sku_num * data.order_price split_original_amount,
                data.split_total_amount,
                data.split_activity_amount,
                data.split_coupon_amount
         from ods_order_detail_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) od
         left join
     (
         select data.id,
                data.user_id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) oi
     on od.order_id = oi.id
         left join
     (
         select data.order_detail_id,
                data.activity_id,
                data.activity_rule_id
         from ods_order_detail_activity_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) act
     on od.id = act.order_detail_id
         left join
     (
         select data.order_detail_id,
                data.coupon_id
         from ods_order_detail_coupon_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) cou
     on od.id = cou.order_detail_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '24'
     ) dic
     on od.source_type = dic.dic_code;
-- 交易域下单事务事实表 每日数据装载
insert overwrite table dwd_trade_order_detail_inc partition (dt = '2020-06-15')
select od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       date_id,
       create_time,
       source_id,
       source_type,
       dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount
from (
         select data.id,
                data.order_id,
                data.sku_id,
                date_format(data.create_time, 'yyyy-MM-dd') date_id,
                data.create_time,
                data.source_id,
                data.source_type,
                data.sku_num,
                data.sku_num * data.order_price             split_original_amount,
                data.split_total_amount,
                data.split_activity_amount,
                data.split_coupon_amount
         from ods_order_detail_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) od
         left join
     (
         select data.id,
                data.user_id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) oi
     on od.order_id = oi.id
         left join
     (
         select data.order_detail_id,
                data.activity_id,
                data.activity_rule_id
         from ods_order_detail_activity_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) act
     on od.id = act.order_detail_id
         left join
     (
         select data.order_detail_id,
                data.coupon_id
         from ods_order_detail_coupon_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) cou
     on od.id = cou.order_detail_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '24'
     ) dic
     on od.source_type = dic.dic_code;

-- 交易域取消订单事务事实表
DROP TABLE IF EXISTS dwd_trade_cancel_detail_inc;
CREATE EXTERNAL TABLE dwd_trade_cancel_detail_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单id',
    `user_id`               STRING COMMENT '用户id',
    `sku_id`                STRING COMMENT '商品id',
    `province_id`           STRING COMMENT '省份id',
    `activity_id`           STRING COMMENT '参与活动规则id',
    `activity_rule_id`      STRING COMMENT '参与活动规则id',
    `coupon_id`             STRING COMMENT '使用优惠券id',
    `date_id`               STRING COMMENT '取消订单日期id',
    `cancel_time`           STRING COMMENT '取消订单时间',
    `source_id`             STRING COMMENT '来源编号',
    `source_type_code`      STRING COMMENT '来源类型编码',
    `source_type_name`      STRING COMMENT '来源类型名称',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '交易域取消订单明细事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cancel_detail_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 交易域取消订单事务事实表 首日数据装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_cancel_detail_inc partition (dt)
select od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       date_format(canel_time, 'yyyy-MM-dd') date_id,
       canel_time,
       source_id,
       source_type,
       dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount,
       date_format(canel_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.source_id,
                data.source_type,
                data.sku_num,
                data.sku_num * data.order_price split_original_amount,
                data.split_total_amount,
                data.split_activity_amount,
                data.split_coupon_amount
         from ods_order_detail_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) od
         join
     (
         select data.id,
                data.user_id,
                data.province_id,
                data.operate_time canel_time
         from ods_order_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
           and data.order_status = '1003'
     ) oi
     on od.order_id = oi.id
         left join
     (
         select data.order_detail_id,
                data.activity_id,
                data.activity_rule_id
         from ods_order_detail_activity_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) act
     on od.id = act.order_detail_id
         left join
     (
         select data.order_detail_id,
                data.coupon_id
         from ods_order_detail_coupon_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) cou
     on od.id = cou.order_detail_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '24'
     ) dic
     on od.source_type = dic.dic_code;
-- 交易域取消订单事务事实表 每日数据装载
insert overwrite table dwd_trade_cancel_detail_inc partition (dt = '2020-06-15')
select od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       date_format(canel_time, 'yyyy-MM-dd') date_id,
       canel_time,
       source_id,
       source_type,
       dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.source_id,
                data.source_type,
                data.sku_num,
                data.sku_num * data.order_price split_original_amount,
                data.split_total_amount,
                data.split_activity_amount,
                data.split_coupon_amount
         from ods_order_detail_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) od
         join
     (
         select data.id,
                data.user_id,
                data.province_id,
                data.operate_time canel_time
         from ods_order_info_inc
         where dt = '2020-06-15'
           and type = 'update'
           and data.order_status = '1003'
           and array_contains(map_keys(old), 'order_status')
     ) oi
     on order_id = oi.id
         left join
     (
         select data.order_detail_id,
                data.activity_id,
                data.activity_rule_id
         from ods_order_detail_activity_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) act
     on od.id = act.order_detail_id
         left join
     (
         select data.order_detail_id,
                data.coupon_id
         from ods_order_detail_coupon_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) cou
     on od.id = cou.order_detail_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '24'
     ) dic
     on od.source_type = dic.dic_code;

-- 交易域支付成功事务事实表
DROP TABLE IF EXISTS dwd_trade_pay_detail_suc_inc;
CREATE EXTERNAL TABLE dwd_trade_pay_detail_suc_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单id',
    `user_id`               STRING COMMENT '用户id',
    `sku_id`                STRING COMMENT '商品id',
    `province_id`           STRING COMMENT '省份id',
    `activity_id`           STRING COMMENT '参与活动规则id',
    `activity_rule_id`      STRING COMMENT '参与活动规则id',
    `coupon_id`             STRING COMMENT '使用优惠券id',
    `payment_type_code`     STRING COMMENT '支付类型编码',
    `payment_type_name`     STRING COMMENT '支付类型名称',
    `date_id`               STRING COMMENT '支付日期id',
    `callback_time`         STRING COMMENT '支付成功时间',
    `source_id`             STRING COMMENT '来源编号',
    `source_type_code`      STRING COMMENT '来源类型编码',
    `source_type_name`      STRING COMMENT '来源类型名称',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '应支付原始金额',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
    `split_payment_amount`  DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域成功支付事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_pay_detail_suc_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 交易域支付成功事务事实表 首日数据转载
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt)
select od.id,
       od.order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       payment_type,
       pay_dic.dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       source_id,
       source_type,
       src_dic.dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount,
       date_format(callback_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.source_id,
                data.source_type,
                data.sku_num,
                data.sku_num * data.order_price split_original_amount,
                data.split_total_amount,
                data.split_activity_amount,
                data.split_coupon_amount
         from ods_order_detail_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) od
         join
     (
         select data.user_id,
                data.order_id,
                data.payment_type,
                data.callback_time
         from ods_payment_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
           and data.payment_status = '1602'
     ) pi
     on od.order_id = pi.order_id
         left join
     (
         select data.id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) oi
     on od.order_id = oi.id
         left join
     (
         select data.order_detail_id,
                data.activity_id,
                data.activity_rule_id
         from ods_order_detail_activity_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) act
     on od.id = act.order_detail_id
         left join
     (
         select data.order_detail_id,
                data.coupon_id
         from ods_order_detail_coupon_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) cou
     on od.id = cou.order_detail_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '11'
     ) pay_dic
     on pi.payment_type = pay_dic.dic_code
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '24'
     ) src_dic
     on od.source_type = src_dic.dic_code;
-- 交易域支付成功事务事实表 每日数据转载
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt = '2020-06-15')
select od.id,
       od.order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       payment_type,
       pay_dic.dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       source_id,
       source_type,
       src_dic.dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.source_id,
                data.source_type,
                data.sku_num,
                data.sku_num * data.order_price split_original_amount,
                data.split_total_amount,
                data.split_activity_amount,
                data.split_coupon_amount
         from ods_order_detail_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) od
         join
     (
         select data.user_id,
                data.order_id,
                data.payment_type,
                data.callback_time
         from ods_payment_info_inc
         where dt = '2020-06-15'
           and type = 'update'
           and array_contains(map_keys(old), 'payment_status')
           and data.payment_status = '1602'
     ) pi
     on od.order_id = pi.order_id
         left join
     (
         select data.id,
                data.province_id
         from ods_order_info_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) oi
     on od.order_id = oi.id
         left join
     (
         select data.order_detail_id,
                data.activity_id,
                data.activity_rule_id
         from ods_order_detail_activity_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) act
     on od.id = act.order_detail_id
         left join
     (
         select data.order_detail_id,
                data.coupon_id
         from ods_order_detail_coupon_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) cou
     on od.id = cou.order_detail_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '11'
     ) pay_dic
     on pi.payment_type = pay_dic.dic_code
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '24'
     ) src_dic
     on od.source_type = src_dic.dic_code;

-- 交易域退单事务事实表
DROP TABLE IF EXISTS dwd_trade_order_refund_inc;
CREATE EXTERNAL TABLE dwd_trade_order_refund_inc
(
    `id`                      STRING COMMENT '编号',
    `user_id`                 STRING COMMENT '用户ID',
    `order_id`                STRING COMMENT '订单ID',
    `sku_id`                  STRING COMMENT '商品ID',
    `province_id`             STRING COMMENT '地区ID',
    `date_id`                 STRING COMMENT '日期ID',
    `create_time`             STRING COMMENT '退单时间',
    `refund_type_code`        STRING COMMENT '退单类型编码',
    `refund_type_name`        STRING COMMENT '退单类型名称',
    `refund_reason_type_code` STRING COMMENT '退单原因类型编码',
    `refund_reason_type_name` STRING COMMENT '退单原因类型名称',
    `refund_reason_txt`       STRING COMMENT '退单原因描述',
    `refund_num`              BIGINT COMMENT '退单件数',
    `refund_amount`           DECIMAL(16, 2) COMMENT '退单金额'
) COMMENT '交易域退单事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_order_refund_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
-- 交易域退单事务事实表 首日数据装载
insert overwrite table dwd_trade_order_refund_inc partition (dt)
select ri.id,
       user_id,
       order_id,
       sku_id,
       province_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       refund_type,
       type_dic.dic_name,
       refund_reason_type,
       reason_dic.dic_name,
       refund_reason_txt,
       refund_num,
       refund_amount,
       date_format(create_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.user_id,
                data.order_id,
                data.sku_id,
                data.refund_type,
                data.refund_num,
                data.refund_amount,
                data.refund_reason_type,
                data.refund_reason_txt,
                data.create_time
         from ods_order_refund_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) ri
         left join
     (
         select data.id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) oi
     on ri.order_id = oi.id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '15'
     ) type_dic
     on ri.refund_type = type_dic.dic_code
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '13'
     ) reason_dic
     on ri.refund_reason_type = reason_dic.dic_code;
-- 交易域退单事务事实表 每日数据装载
insert overwrite table dwd_trade_order_refund_inc partition (dt = '2020-06-15')
select ri.id,
       user_id,
       order_id,
       sku_id,
       province_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       refund_type,
       type_dic.dic_name,
       refund_reason_type,
       reason_dic.dic_name,
       refund_reason_txt,
       refund_num,
       refund_amount
from (
         select data.id,
                data.user_id,
                data.order_id,
                data.sku_id,
                data.refund_type,
                data.refund_num,
                data.refund_amount,
                data.refund_reason_type,
                data.refund_reason_txt,
                data.create_time
         from ods_order_refund_info_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) ri
         left join
     (
         select data.id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-15'
           and type = 'update'
           and data.order_status = '1005'
           and array_contains(map_keys(old), 'order_status')
     ) oi
     on ri.order_id = oi.id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '15'
     ) type_dic
     on ri.refund_type = type_dic.dic_code
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '13'
     ) reason_dic
     on ri.refund_reason_type = reason_dic.dic_code;

-- 交易域退款成功事务事实表
DROP TABLE IF EXISTS dwd_trade_refund_pay_suc_inc;
CREATE EXTERNAL TABLE dwd_trade_refund_pay_suc_inc
(
    `id`                STRING COMMENT '编号',
    `user_id`           STRING COMMENT '用户ID',
    `order_id`          STRING COMMENT '订单编号',
    `sku_id`            STRING COMMENT 'SKU编号',
    `province_id`       STRING COMMENT '地区ID',
    `payment_type_code` STRING COMMENT '支付类型编码',
    `payment_type_name` STRING COMMENT '支付类型名称',
    `date_id`           STRING COMMENT '日期ID',
    `callback_time`     STRING COMMENT '支付成功时间',
    `refund_num`        DECIMAL(16, 2) COMMENT '退款件数',
    `refund_amount`     DECIMAL(16, 2) COMMENT '退款金额'
) COMMENT '交易域提交退款成功事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_refund_pay_suc_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
-- 交易域退款成功事务事实表 首日数据装载
insert overwrite table dwd_trade_refund_pay_suc_inc partition (dt)
select rp.id,
       user_id,
       rp.order_id,
       rp.sku_id,
       province_id,
       payment_type,
       dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       refund_num,
       total_amount,
       date_format(callback_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.payment_type,
                data.callback_time,
                data.total_amount
         from ods_refund_payment_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
           and data.refund_status = '1602'
     ) rp
         left join
     (
         select data.id,
                data.user_id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) oi
     on rp.order_id = oi.id
         left join
     (
         select data.order_id,
                data.sku_id,
                data.refund_num
         from ods_order_refund_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) ri
     on rp.order_id = ri.order_id
         and rp.sku_id = ri.sku_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '11'
     ) dic
     on rp.payment_type = dic.dic_code;
-- 交易域退款成功事务事实表 每日数据装载
insert overwrite table dwd_trade_refund_pay_suc_inc partition (dt = '2020-06-15')
select rp.id,
       user_id,
       rp.order_id,
       rp.sku_id,
       province_id,
       payment_type,
       dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       refund_num,
       total_amount
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.payment_type,
                data.callback_time,
                data.total_amount
         from ods_refund_payment_inc
         where dt = '2020-06-15'
           and type = 'update'
           and array_contains(map_keys(old), 'refund_status')
           and data.refund_status = '1602'
     ) rp
         left join
     (
         select data.id,
                data.user_id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-15'
           and type = 'update'
           and data.order_status = '1006'
           and array_contains(map_keys(old), 'order_status')
     ) oi
     on rp.order_id = oi.id
         left join
     (
         select data.order_id,
                data.sku_id,
                data.refund_num
         from ods_order_refund_info_inc
         where dt = '2020-06-15'
           and type = 'update'
           and data.refund_status = '0705'
           and array_contains(map_keys(old), 'refund_status')
     ) ri
     on rp.order_id = ri.order_id
         and rp.sku_id = ri.sku_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '11'
     ) dic
     on rp.payment_type = dic.dic_code;

-- 交易域购物车 周期快照事实表
DROP TABLE IF EXISTS dwd_trade_cart_full;
CREATE EXTERNAL TABLE dwd_trade_cart_full
(
    `id`       STRING COMMENT '编号',
    `user_id`  STRING COMMENT '用户id',
    `sku_id`   STRING COMMENT '商品id',
    `sku_name` STRING COMMENT '商品名称',
    `sku_num`  BIGINT COMMENT '加购物车件数'
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 交易域购物车周期快照事实表 数据装载
insert overwrite table dwd_trade_cart_full partition (dt = '2020-06-14')
select id,
       user_id,
       sku_id,
       sku_name,
       sku_num
from ods_cart_info_full
where dt = '2020-06-14'
  and is_ordered = '0';
