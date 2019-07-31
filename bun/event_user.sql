
-- 상품 등록

-- 유저의 상품 등록 기간에 따른 포인트 지급액
select uid,
	case
		when first_register_at in ('2019-08-17', '2019-08-18') then 4000
		when first_register_at in ('2019-08-24', '2019-08-25') then 8000
		when first_register_at in ('2019-08-31', '2019-09-01') then 6000
		when first_register_at in ('2019-09-07', '2019-09-08') then 10000
		end as reward_point
from (
    -- 유저별 최초 상품 등록 시간
    select p.uid, date_format(min(create_date), '%Y-%m-%d') as first_register_at
    from product_info p
    join (
        -- 이벤트 기간 내 신규 가입한 유저
        select id
        from user
        where join_date between '2019-08-12' and '2019-09-08' and -- 이벤트 기간 : 8/12(월) ~ 9/8(일)
            status = 0
    ) u
    on p.uid = u.id
    where left(p.category, 3) in ('310', '320', '400', '410', '700') -- 여성의류, 남성의류, 패션잡화, 뷰티/미용, 스포츠/레저
    group by 1
    having first_register_at in ('2019-08-17', '2019-08-18', '2019-08-24', '2019-08-25', '2019-08-31', '2019-09-01', '2019-09-07', '2019-09-08') -- 등록 지정 기간
) register_users



