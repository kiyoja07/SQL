
-- 이벤트 참여 유저 리스트

-- 상품 등록

select uid,
	case
		when first_register_at in ('2019-08-17', '2019-08-18') then 4000
		when first_register_at in ('2019-08-24', '2019-08-25') then 8000
		when first_register_at in ('2019-08-31', '2019-09-01') then 6000
		when first_register_at in ('2019-09-07', '2019-09-08') then 10000
    end as reward_point
from (
    -- 유저별 최초 상품 등록 시간
    select p.uid, date_format(min(create_date), '%Y-%m-%d') as first_register_at -- 첫 등록
    from product_info p
    join (
        -- 이벤트 기간 내 신규 가입한 유저
        select id
        from user
        where join_date between '2019-08-12' and '2019-09-08' and status = 0 -- 이벤트 기간 : 8/12(월) ~ 9/8(일)
    ) u
    on p.uid = u.id
    where left(p.category_id, 3) in ('310', '320', '400', '410', '700') -- 대상 카테고리 : 여성의류, 남성의류, 패션잡화, 뷰티/미용, 스포츠/레저
    group by 1
    having first_register_at in ('2019-08-17', '2019-08-18', '2019-08-24', '2019-08-25', '2019-08-31', '2019-09-01', '2019-09-07', '2019-09-08') -- 등록 지정 기간
) register_users


-- 번개페이, 송금으로 판매

select user.uid,
    case
        when min(sell.paid_at) between '2019-08-12' and '2019-08-18' then 2000
        when min(sell.paid_at) between '2019-08-19' and '2019-08-25' then 4000
        when min(sell.paid_at) between '2019-08-26' and '2019-09-01' then 3000
        when min(sell.paid_at) between '2019-09-02' and '2019-09-08' then 5000
    end as reward_point
from (
    -- 이벤트 대상 유저의 등록 상품
    select p.uid, p.id as pid, p.create_date
    from product_info p
    join (
        -- 이벤트 기간 내 신규 가입한 유저
        select id
        from user
        where join_date between '2019-08-12' and '2019-09-08' and status = 0 -- 이벤트 기간 : 8/12(월) ~ 9/8(일)
    ) u
    on p.uid = u.id
    where create_date between '2019-08-12' and '2019-09-08' -- 이벤트 기간에 등록한 상품
) user
join (
    -- 번개 송금으로 판매
    select pid, paid_at, status, updated_at
    from wire_transfer
    where paid_at between '2019-08-12' and '2019-09-08' and  -- 이벤트 기간에 결제한 상품
        status = 'transfer_completed' and updated_at <= '2019-09-22 23:59:59' -- 9/22까지 거래 완료

    union 

    -- 번개 페이로 판매
    select i.pid, o.order_done_date, o.order_status_cd, o.update_date
    from order_mast o
    join order_item i
    on o.id = i.order_mast_id
    where o.order_done_date between '2019-08-12' and '2019-09-08' and  -- 이벤트 기간에 결제한 상품 
        o.order_status_cd = 'purchase_confirm' and o.update_date <= '2019-09-22 23:59:59' -- 9/22까지 거래 완료
) sell
on user.pid = sell.pid
where sell.paid_at <= date_add(user.create_date, interval 14 day)  -- 등록 후 2주 이내 결제
group by 1


-- 번개페이, 송금으로 구매

select uid,
    case
        when min(paid_at) between '2019-08-12' and '2019-08-18' then 2000
        when min(paid_at) between '2019-08-19' and '2019-08-25' then 4000
        when min(paid_at) between '2019-08-26' and '2019-09-01' then 3000
        when min(paid_at) between '2019-09-02' and '2019-09-08' then 5000
    end as reward_point
from (
    -- 이벤트 기간 내 신규 가입한 유저
    select id
    from user
    where join_date between '2019-08-12' and '2019-09-08' and status = 0 -- 이벤트 기간 : 8/12(월) ~ 9/8(일)
) user
join (
    -- 번개 송금으로 구매
    select uid, paid_at, status, updated_at
    from wire_transfer
    where paid_at between '2019-08-12' and '2019-09-08' and  -- 이벤트 기간에 결제한 상품 
        status = 'transfer_completed' and updated_at <= '2019-09-22 23:59:59' -- 9/22까지 거래 완료

    union

    -- 번개 페이로 구매
    select buyer_id, order_done_date, order_status_cd, update_date
    from order_mast
    where order_done_date between '2019-08-12' and '2019-09-08' and  -- 이벤트 기간에 결제한 상품 
        order_status_cd = 'purchase_confirm' and update_date <= '2019-09-22 23:59:59' -- 9/22까지 거래 완료
) buy
on user.id = buy.uid
group by 1



