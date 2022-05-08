--开发测试，设置运行模式为本地模式
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.tasks.max=10;
set hive.exec.mode.local.auto.inputbytes.max=50000000;

--指标一：订单类型统计
with tmp as (
    select product_id,count(1) as total from hudi.tbl_hudi_didi group by product_id
)
select
    case product_id
        when 1 then '滴滴专车'
        when 2 then '滴滴企业专车'
        when 3 then '滴滴快车'
        when 4 then '滴滴企业快车'
    end as order_type,
    total
from tmp;

--指标二：订单时效性统计
with tmp as (
select type,count(1) as total from hudi.tbl_hudi_didi group by type
)
select
    case type
        when 0 then '实时'
        when 1 then '预约'
    end as order_type,
    total
from tmp;

--指标三：订单交通类型统计
with tmp as (
    select traffic_type,count(1) as total from hudi.tbl_hudi_didi group by traffic_type
)
select
    case traffic_type
        when 0 then '普通散客'
        when 1 then '企业时租'
        when 2 then '企业接机套餐'
        when 3 then '企业接机套餐'
        when 4 then '拼车'
        when 5 then '接机'
        when 6 then '送机'
        else '未知'
        end as traffic_type,
        total
    from tmp;

--指标四：订单价格统计，先将价格划分区间，在统计，使用when函数和sum函数
select
    sum(
        case when pre_total_fee between 0 and 15 then 1 else 0 end
    ) as 0_15,
    sum(
        case when pre_total_fee between 16 and 30 then 1 else 0 end
    ) as 16_30,
    sum(
        case when pre_total_fee between 31 and 50 then 1 else 0 end
    ) as 31_50,
    sum(
        case when pre_total_fee between 51 and 100 then 1 else 0 end
    ) as 51_100,
    sum(
        case when pre_total_fee > 100 then 1 else 0 end
    ) as 100_

from hudi.tbl_hudi_didi;

