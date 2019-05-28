
/*ad_query_list*/

/*super_up*/
select
  s.suid, s.uid, su.pay_point, su.pay_free, s.create_at
from
  ad_super_up s
join
  (
    select
      suid, sum(pay_point) as pay_point, sum(pay_free) as pay_free
    from
      ad_super_up_point
    group by
      suid
  ) su
on
  s.suid = su.suid
where
  s.status = 2

/*power_up*/
select
  p.pu_id, p.uid, pu.pay_point, pu.pay_free, p.created_at as create_at
from
  ad_power_up p
join
  (
    select
      pu_id, sum(pay_point) as pay_point, sum(pay_free) as pay_free
    from
      ad_power_up_point
    group by
      pu_id
  ) pu
on
  p.pu_id = pu.pu_id

/*super_up_shop*/
select
  s.suid, s.uid, sus.pay_point, sus.pay_free, s.create_at
from
  super_up_shop s
join
  (
    select
      sus_id, sum(pay_point) as pay_point, sum(pay_free) as pay_free
    from
      ad_super_up_shop_point
    group by
      sus_id
  ) sus
on
  s.sus_id = sus.sus_id
where
  s.status = 2

/*ad_keyword*/
select
  uid, request_date, sum(pay_point) as pay_point, sum(pay_free) as pay_free
from
  ad_keyword_bid
where
  status IN ('success', 'stop')
group by
  uid, request_date

/*super_keyword*/
select
  uid, created_at, sum(pay_point) as pay_point, sum(pay_free) as pay_free
from
  ad_super_keyword
where
  status = 2
group by
  uid, created_at

/*today_deal_plus*/
select
  uid, created_at as create_at, sum(pay_point) as pay_point, sum(pay_free) as pay_free
from
  ad_today_deal_plus
where
  status = 2
group by
  uid, created_at

/*today_deal*/
/* 투데이 딜 = 홈 추천 PLUS */
select
  uid, created_at as create_at, sum(pay_point) as pay_point, sum(pay_free) as pay_free
from
  ad_today_deal
where
  status = 2
group by
  uid, created_at

/*curation*/
/* 큐레이션 = 기획전 (메인 전면배너) */
select
  uid, created_at as create_at, sum(pay_point) as pay_point, sum(pay_free) as pay_free
from
  ad_curation_charge
where
  status = 2 and cid = 1000020
group by
  uid, created_at

/*shop_popular*/
select
  uid, created_at as create_at, sum(pay_point) as pay_point, sum(pay_free) as pay_free
from
  ad_shop_popular
where
  status = 2
group by
  uid, created_at

/*product_popular*/
select
  uid, created_at as create_at, sum(pay_point) as pay_point, sum(pay_free) as pay_free
from
  ad_product_popular
where
  status = 2
group by
  uid, created_at

/*category*/
select
  uid, created_at as create_at, sum(pay_point) as pay_point, sum(pay_free) as pay_free
from
  ad_category
where
  status = 2
group by
  uid, created_at

/*up_plus*/
select
  uid, created_at as create_at, sum(pay_point) as pay_point, sum(pay_free) as pay_free
from
  ad_up_plus
where
  status = 2
group by
  uid, created_at