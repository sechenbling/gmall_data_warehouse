-- 用户域用户粒度登录历史至今汇总表
-- 建表语句
DROP TABLE IF EXISTS dws_user_user_login_td;
CREATE EXTERNAL TABLE dws_user_user_login_td
(
    `user_id`         STRING COMMENT '用户id',
    `login_date_last` STRING COMMENT '末次登录日期',
    `login_count_td`  BIGINT COMMENT '累计登录次数'
) COMMENT '用户域用户粒度登录历史至今汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_user_user_login_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 首日装载
insert overwrite table dws_user_user_login_td partition(dt='2020-06-14')
select
    u.id,
    nvl(login_date_last,date_format(create_time,'yyyy-MM-dd')),
    nvl(login_count_td,1)
from
(
    select
        id,
        create_time
    from dim_user_zip
    where dt='9999-12-31'
)u
left join
(
    select
        user_id,
        max(dt) login_date_last,
        count(*) login_count_td
    from dwd_user_login_inc
    group by user_id
)l
on u.id=l.user_id;
-- 每日装载
insert overwrite table dws_user_user_login_td partition(dt='2020-06-15')
select
    nvl(old.user_id,new.user_id),
    if(new.user_id is null,old.login_date_last,'2020-06-15'),
    nvl(old.login_count_td,0)+nvl(new.login_count_1d,0)
from
(
    select
        user_id,
        login_date_last,
        login_count_td
    from dws_user_user_login_td
    where dt=date_add('2020-06-15',-1)
)old
full outer join
(
    select
        user_id,
        count(*) login_count_1d
    from dwd_user_login_inc
    where dt='2020-06-15'
    group by user_id
)new
on old.user_id=new.user_id;
