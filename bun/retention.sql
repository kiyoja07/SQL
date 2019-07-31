


-- 검색 기준 유저 리텐션

with user_search as (
	-- 전체 검색 로그
	select date_trunc('day', updated) as date, viewer_uid as uid
	from item_search_log
)
,new_user_search as (
	-- 가입일의 검색 로그
	select search.date, joined.device, search.uid
	from user_search as search
	join user_for_stats as joined
	on search.uid = joined.uid and search.date = date_trunc('day', joined.join_date)
),
new_user_count as (
	-- 가입일의 검색 유저 수
	select date, device, count(distinct uid) as new_user
	from new_user_search
	group by 1, 2
)


select date, device, period,
	joined_users, retained_users, retention
from (
	select new_user.date, new_user.device, datediff(day, new_user.date, search.date) AS period,
		max(new_user_count.new_user) as joined_users,
		count(distinct search.uid) as retained_users,
		count(distinct search.uid) / max(new_user_count.new_user)::float as retention
	from new_user_search new_user
	left join user_search search
	on new_user.uid = search.uid and new_user.date <= search.date and
		(new_user.date + interval '30 days') >= search.date
	left join new_user_count
	on new_user.date = new_user_count.date
	group by 1, 2, 3
)
where period is not null
order by 1, 2, 3



-- 첫 구매월 기준 구매 금액 리텐션

-- 전체 구매 데이터
WITH bunp_all AS (
SELECT buyer_uid, date_trunc('month', updated_at) AS month_at, 
    sum(seller_pid_price) AS bunp_amount
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price > 0 AND seller_pid_price < 3000000
GROUP BY 1, 2
),
-- 첫 구매월 데이터 추출
first_bunp AS (
SELECT buyer_uid, month_at, bunp_amount
FROM (
    SELECT buyer_uid, month_at, bunp_amount,
        ROW_NUMBER () OVER (PARTITION by buyer_uid ORDER BY month_at) AS _num
    FROM bunp_all
    )
WHERE _num = 1
),
first_bunp_month AS (
SELECT month_at, SUM(bunp_amount) AS bunp_amount_month
FROM first_bunp
GROUP BY 1
)



SELECT to_char(first_at, 'YYYY-MM') as fisrt_at, _period, amount_retention
FROM (
    SELECT first.month_at AS first_at,
        datediff(month, first.month_at, _all.month_at) AS _period,
        sum(_all.bunp_amount) / max(first_month.bunp_amount_month)::FLOAT AS amount_retention
    FROM first_bunp first
    LEFT JOIN bunp_all _all
    ON first.buyer_uid = _all.buyer_uid AND first.month_at <= _all.month_at
    LEFT JOIN first_bunp_month first_month
    ON first_month.month_at = first.month_at
    GROUP BY 1, 2
)



-- 첫 구매월 기준 구매 횟수 리텐션

-- 번프 전체 구매 내역 리스트
WITH bunp_all AS (
SELECT complite.bunp_month,
    json_extract_path_text(params, 'buyer_uid') AS buyer_uid,
    COUNT(bunp.bunp_id) AS bunp_count
FROM (
    SELECT DISTINCT date_trunc('month', updated) AS bunp_month,
        json_extract_path_text(params, 'bunp_id') AS bunp_id
    FROM bunp
    WHERE log_type = 'complite' 
) complite
JOIN bunp
ON complite.bunp_id = bunp.bunp_id
WHERE bunp.log_type = 'make'
GROUP BY 1, 2
),
-- 첫 구매월 
first_bunp AS (
SELECT bunp_month, buyer_uid, bunp_count
FROM (
    SELECT bunp_month, buyer_uid, bunp_count,
        ROW_NUMBER() OVER (PARTITION by buyer_uid ORDER BY bunp_month) AS _num
    FROM bunp_all
    )
WHERE _num = 1
),
first_bunp_month AS (
SELECT bunp_month, SUM(bunp_count) AS bunp_count_month
FROM first_bunp
GROUP BY 1
)


SELECT to_char(first_at, 'YYYY-MM') AS fisrt_at, _period, count_retention
FROM (
    SELECT first.bunp_month AS first_at,
        datediff(month, first.bunp_month, _all.bunp_month) AS _period,
        sum(_all.bunp_count) / max(first_month.bunp_count_month)::FLOAT AS count_retention
    FROM first_bunp first
    LEFT JOIN bunp_all _all
    ON first.buyer_uid = _all.buyer_uid AND first.bunp_month <= _all.bunp_month
    LEFT JOIN first_bunp_month first_month
    ON first_month.bunp_month = first.bunp_month
GROUP BY 1, 2)




-- 첫 판매월 기준 판매 금액 리텐션

-- 전체 판매 데이터
WITH bunp_all AS (
SELECT seller_uid, date_trunc('month', updated_at) AS month_at, 
    sum(seller_pid_price) AS bunp_amount
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price > 0 AND seller_pid_price < 3000000
GROUP BY 1, 2
),
-- 첫 구매월 데이터 추출
first_bunp AS (
SELECT seller_uid, month_at, bunp_amount
FROM (
    SELECT seller_uid, month_at, bunp_amount,
        ROW_NUMBER () OVER (PARTITION by seller_uid ORDER BY month_at) AS _num
    FROM bunp_all
    )
WHERE _num = 1
),
first_bunp_month AS (
SELECT month_at, SUM(bunp_amount) AS bunp_amount_month
FROM first_bunp
GROUP BY 1
)


SELECT to_char(first_at, 'YYYY-MM') as fisrt_at, _period, amount_retention
FROM (
    SELECT first.month_at AS first_at,
        datediff(month, first.month_at, _all.month_at) AS _period,
        sum(_all.bunp_amount) / max(first_month.bunp_amount_month)::FLOAT AS amount_retention
    FROM first_bunp first
    LEFT JOIN bunp_all _all
    ON first.seller_uid = _all.seller_uid AND first.month_at <= _all.month_at
    LEFT JOIN first_bunp_month first_month
    ON first_month.month_at = first.month_at
GROUP BY 1, 2)




-- 첫 판매월 기준 판매 횟수 리텐션

-- 번프 전체 판매 내역 리스트
WITH bunp_all AS (
SELECT complite.bunp_month,
    json_extract_path_text(params, 'seller_uid') AS seller_uid,
    COUNT(bunp.bunp_id) AS bunp_count
FROM (
    SELECT DISTINCT date_trunc('month', updated) AS bunp_month,
        json_extract_path_text(params, 'bunp_id') AS bunp_id
    FROM bunp
    WHERE log_type = 'complite' 
) complite
JOIN bunp
ON complite.bunp_id = bunp.bunp_id
WHERE bunp.log_type = 'make'
GROUP BY 1, 2
),
-- 첫 판매월 
first_bunp AS (
SELECT bunp_month, seller_uid, bunp_count
FROM (
    SELECT bunp_month, seller_uid, bunp_count,
        ROW_NUMBER() OVER (PARTITION by seller_uid ORDER BY bunp_month) AS _num
    FROM bunp_all
    )
WHERE _num = 1
),
first_bunp_month AS (
SELECT bunp_month, SUM(bunp_count) AS bunp_count_month
FROM first_bunp
GROUP BY 1
)



SELECT to_char(first_at, 'YYYY-MM') AS fisrt_at, _period, count_retention
FROM (
    SELECT first.bunp_month AS first_at,
        datediff(month, first.bunp_month, _all.bunp_month) AS _period,
        sum(_all.bunp_count) / max(first_month.bunp_count_month)::FLOAT AS count_retention
    FROM first_bunp first
    LEFT JOIN bunp_all _all
    ON first.seller_uid = _all.seller_uid AND first.bunp_month <= _all.bunp_month
    LEFT JOIN first_bunp_month first_month
    ON first_month.bunp_month = first.bunp_month
    GROUP BY 1, 2
)



-- 가입월 기준 구매 금액 리텐션

-- 전체 구매 데이터
WITH bunp_all AS (
SELECT buyer_uid, date_trunc('month', updated_at) AS month_at, 
    sum(seller_pid_price) AS bunp_amount
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price > 0 AND seller_pid_price < 3000000
GROUP BY 1, 2
),
-- 가입월 거래 데이터 추출
joined_bunp AS (
SELECT joined.uid, joined.joined_at, bunp_all.bunp_amount
FROM (
-- 월별 가입자 추출
    SELECT uid, date_trunc('month', updated) AS joined_at 
    FROM user_join_log
) joined
JOIN bunp_all
ON joined.uid = bunp_all.buyer_uid AND joined.joined_at = bunp_all.month_at
),
joined_bunp_month AS (
SELECT joined_at, SUM(bunp_amount) AS bunp_amount_month
FROM joined_bunp
GROUP BY 1
)



SELECT to_char(joined_at, 'YYYY-MM') as joined_at, _period, amount_retention
FROM (
    SELECT joined.joined_at AS joined_at,
        datediff(month, joined.joined_at, _all.month_at) AS _period,
        sum(_all.bunp_amount) / max(joined_month.bunp_amount_month)::FLOAT AS amount_retention
    FROM joined_bunp joined
    LEFT JOIN bunp_all _all
    ON joined.uid = _all.buyer_uid AND joined.joined_at <= _all.month_at
    LEFT JOIN joined_bunp_month joined_month
    ON joined_month.joined_at = joined.joined_at
    GROUP BY 1, 2
)





-- 가입월 기준 구매 횟수 리텐션

-- 번프 전체 판매 내역 리스트
WITH bunp_all AS (
SELECT complite.bunp_month,
    json_extract_path_text(params, 'buyer_uid') AS buyer_uid,
    COUNT(bunp.bunp_id) AS bunp_count
FROM (
    SELECT DISTINCT date_trunc('month', updated) AS bunp_month,
        json_extract_path_text(params, 'bunp_id') AS bunp_id
    FROM bunp
    WHERE log_type = 'complite' 
) complite
JOIN bunp
ON complite.bunp_id = bunp.bunp_id
WHERE bunp.log_type = 'make'
GROUP BY 1, 2
),
-- 가입월 판매 데이터
joined_bunp AS (
SELECT joined.uid, joined.joined_at, bunp_all.bunp_count
FROM (
-- 월별 가입자
    SELECT uid, date_trunc('month', updated) AS joined_at 
    FROM user_join_log
) joined
JOIN bunp_all
ON joined.uid = bunp_all.buyer_uid AND joined.joined_at = bunp_all.bunp_month
),
joined_bunp_month AS (
SELECT joined_at, SUM(bunp_count) AS bunp_count_month
FROM joined_bunp
GROUP BY 1
)



SELECT to_char(joined_at, 'YYYY-MM') AS joined_at, _period, count_retention
FROM (
    SELECT joined.joined_at AS joined_at,
        datediff(month, joined.joined_at, _all.bunp_month) AS _period,
        sum(_all.bunp_count) / max(joined_month.bunp_count_month)::FLOAT AS count_retention
    FROM joined_bunp joined
    LEFT JOIN bunp_all _all
    ON joined.uid = _all.buyer_uid AND joined.joined_at <= _all.bunp_month
    LEFT JOIN joined_bunp_month joined_month
    ON joined_month.joined_at = joined.joined_at
    GROUP BY 1, 2
)



-- 가입월 기준 판매 금액 리텐션

-- 전체 판매 데이터
WITH bunp_all AS (
SELECT seller_uid, date_trunc('month', updated_at) AS month_at, 
    sum(seller_pid_price) AS bunp_amount
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price > 0 AND seller_pid_price < 3000000
GROUP BY 1, 2
),
-- 가입월 거래 데이터 추출
joined_bunp AS (
SELECT joined.uid, joined.joined_at, bunp_all.bunp_amount
FROM (
-- 월별 가입자 추출
    SELECT uid, date_trunc('month', updated) AS joined_at 
    FROM user_join_log
) joined
JOIN bunp_all
ON joined.uid = bunp_all.seller_uid AND joined.joined_at = bunp_all.month_at
),
joined_bunp_month AS (
SELECT joined_at, SUM(bunp_amount) AS bunp_amount_month
FROM joined_bunp
GROUP BY 1
)



SELECT to_char(joined_at, 'YYYY-MM') as joined_at, _period, amount_retention
FROM (
    SELECT joined.joined_at AS joined_at,
        datediff(month, joined.joined_at, _all.month_at) AS _period,
        sum(_all.bunp_amount) / max(joined_month.bunp_amount_month)::FLOAT AS amount_retention
    FROM joined_bunp joined
    LEFT JOIN bunp_all _all
    ON joined.uid = _all.seller_uid AND joined.joined_at <= _all.month_at
    LEFT JOIN joined_bunp_month joined_month
    ON joined_month.joined_at = joined.joined_at
    GROUP BY 1, 2
)




-- 가입월 기준 판매 횟수 리텐션

-- 번프 전체 판매 내역 리스트
WITH bunp_all AS (
SELECT complite.bunp_month,
    json_extract_path_text(params, 'seller_uid') AS seller_uid,
    COUNT(bunp.bunp_id) AS bunp_count
FROM (
    SELECT DISTINCT date_trunc('month', updated) AS bunp_month,
        json_extract_path_text(params, 'bunp_id') AS bunp_id
    FROM bunp
    WHERE log_type = 'complite' 
) complite
JOIN bunp
ON complite.bunp_id = bunp.bunp_id
WHERE bunp.log_type = 'make'
GROUP BY 1, 2
),
-- 가입월 판매 데이터
joined_bunp AS (
SELECT joined.uid, joined.joined_at, bunp_all.bunp_count
FROM (
-- 월별 가입자
    SELECT uid, date_trunc('month', updated) AS joined_at 
    FROM user_join_log
) joined
JOIN bunp_all
ON joined.uid = bunp_all.seller_uid AND joined.joined_at = bunp_all.bunp_month
),
joined_bunp_month AS (
SELECT joined_at, SUM(bunp_count) AS bunp_count_month
FROM joined_bunp
GROUP BY 1
)



SELECT to_char(joined_at, 'YYYY-MM') AS joined_at, _period, count_retention
FROM (
    SELECT joined.joined_at AS joined_at,
        datediff(month, joined.joined_at, _all.bunp_month) AS _period,
        sum(_all.bunp_count) / max(joined_month.bunp_count_month)::FLOAT AS count_retention
    FROM joined_bunp joined
    LEFT JOIN bunp_all _all
    ON joined.uid = _all.seller_uid AND joined.joined_at <= _all.bunp_month
    LEFT JOIN joined_bunp_month joined_month
    ON joined_month.joined_at = joined.joined_at
    GROUP BY 1, 2
)


----------------------------------------------------------
----------------------------------------------------------


-- 첫 번개송금 기준 번개 송금 구매 금액 리텐션

-- 전체 번개 송금 데이터
WITH transfer_all AS (
SELECT uid, DATE_trunc('month', updated_at) AS month_at, 
    SUM(product_price) AS amount
FROM wire_transfer
WHERE status = 'transfer_completed'
GROUP BY 1, 2
),
-- 첫 구매 번개 송금 데이터
transfer_first AS (
SELECT uid, month_at, amount
FROM (
    SELECT uid, month_at, amount,
        ROW_NUMBER () OVER (PARTITION by uid ORDER BY month_at) AS _num
    FROM transfer_all
    )
WHERE _num = 1
),
transfer_first_month AS (
SELECT month_at, SUM(amount) AS amount
FROM transfer_first
GROUP BY 1
)

SELECT to_char(first_at, 'YYYY-MM') AS fisrt_at, period, amount_retention
FROM (
    SELECT first.month_at AS first_at,
        datediff(month, first.month_at, _all.month_at) AS period,
        SUM(_all.amount) / max(first_month.amount)::FLOAT AS amount_retention
    FROM transfer_first first
    LEFT JOIN transfer_all _all
    ON first.uid = _all.uid AND first.month_at <= _all.month_at
    LEFT JOIN transfer_first_month first_month
    ON first_month.month_at = first.month_at
    GROUP BY 1, 2
)




-- 첫 번개 송금 기준 번개 송금 구매 횟수 리텐션

-- 전체 번개 송금 데이터
WITH transfer_all AS (
SELECT uid, DATE_trunc('month', updated_at) AS month_at, 
    count(distinct id) AS transfer_count
FROM wire_transfer
WHERE status = 'transfer_completed'
GROUP BY 1, 2
),
-- 첫 구매 번개 송금 데이터
transfer_first AS (
SELECT uid, month_at, transfer_count
FROM (
    SELECT uid, month_at, transfer_count,
        ROW_NUMBER () OVER (PARTITION by uid ORDER BY month_at) AS _num
    FROM transfer_all
    )
WHERE _num = 1
),
transfer_first_month AS (
SELECT month_at, SUM(transfer_count) AS transfer_count
FROM transfer_first
GROUP BY 1
)

SELECT to_char(first_at, 'YYYY-MM') AS fisrt_at, period, count_retention
FROM (
    SELECT first.month_at AS first_at,
        datediff(month, first.month_at, _all.month_at) AS period,
        SUM(_all.transfer_count) / max(first_month.transfer_count)::FLOAT AS count_retention
    FROM transfer_first first
    LEFT JOIN transfer_all _all
    ON first.uid = _all.uid AND first.month_at <= _all.month_at
    LEFT JOIN transfer_first_month first_month
    ON first_month.month_at = first.month_at
    GROUP BY 1, 2
)



-- 가입월 기준 번개 송금 구매 금액 리텐션

-- 전체 번개 송금 데이터
WITH transfer_all AS (
SELECT uid, DATE_trunc('month', updated_at) AS month_at, 
    SUM(product_price) AS amount
FROM wire_transfer
WHERE status = 'transfer_completed'
GROUP BY 1, 2
),
transfer_joined AS (
SELECT joined.uid, joined.joined_at, transfer_all.amount
FROM (
-- 월별 가입자 추출
    SELECT uid, date_trunc('month', updated) AS joined_at 
    FROM user_join_log
) joined
JOIN transfer_all
ON joined.uid = transfer_all.uid AND joined.joined_at = transfer_all.month_at
),
transfer_joined_month AS (
SELECT joined_at, SUM(amount) AS amount
FROM transfer_joined
GROUP BY 1
)



SELECT to_char(joined_at, 'YYYY-MM') AS joined_at, _period, amount_retention
FROM (
    SELECT joined.joined_at AS joined_at,
        datediff(month, joined.joined_at, _all.month_at) AS _period,
        sum(_all.amount) / max(joined_month.amount)::FLOAT AS amount_retention
    FROM transfer_joined joined
    LEFT JOIN transfer_all _all
    ON joined.uid = _all.uid AND joined.joined_at <= _all.month_at
    LEFT JOIN transfer_joined_month joined_month
    ON joined_month.joined_at = joined.joined_at
    GROUP BY 1, 2
)




-- 가입월 기준 번개 송금 구매 횟수 리텐션

-- 전체 번개 송금 데이터
WITH transfer_all AS (
SELECT uid, DATE_trunc('month', updated_at) AS month_at, 
    count(distinct id) AS transfer_count
FROM wire_transfer
WHERE status = 'transfer_completed'
GROUP BY 1, 2
),
transfer_joined AS (
SELECT joined.uid, joined.joined_at, transfer_all.transfer_count
FROM (
-- 월별 가입자 추출
    SELECT uid, date_trunc('month', updated) AS joined_at 
    FROM user_join_log
) joined
JOIN transfer_all
ON joined.uid = transfer_all.uid AND joined.joined_at = transfer_all.month_at
),
transfer_joined_month AS (
SELECT joined_at, SUM(transfer_count) AS transfer_count
FROM transfer_joined
GROUP BY 1
)



SELECT to_char(joined_at, 'YYYY-MM') AS joined_at, _period, count_retention
FROM (
    SELECT joined.joined_at AS joined_at,
        datediff(month, joined.joined_at, _all.month_at) AS _period,
        sum(_all.transfer_count) / max(joined_month.transfer_count)::FLOAT AS count_retention
    FROM transfer_joined joined
    LEFT JOIN transfer_all _all
    ON joined.uid = _all.uid AND joined.joined_at <= _all.month_at
    LEFT JOIN transfer_joined_month joined_month
    ON joined_month.joined_at = joined.joined_at
    GROUP BY 1, 2
)




-- 첫 번개 송금 기준 번개 송금 판매 금액 리텐션

-- 전체 번개 송금 제품의 판매자 데이터
WITH transfer_all AS (
SELECT p.uid, t.month_at, sum(t.amount) as amount
FROM (
    SELECT pid, DATE_trunc('month', updated_at) AS month_at, product_price AS amount
    FROM wire_transfer
    WHERE status = 'transfer_completed'
) t
JOIN product_info_for_stats p
ON t.pid = p.pid
group by 1, 2
),
-- 첫 판매 번개 송금 데이터
transfer_first AS (
SELECT uid, month_at, amount
FROM (
    SELECT uid, month_at, amount,
        ROW_NUMBER () OVER (PARTITION by uid ORDER BY month_at) AS _num
    FROM transfer_all
    )
WHERE _num = 1
),
transfer_first_month AS (
SELECT month_at, SUM(amount) AS amount
FROM transfer_first
GROUP BY 1
)

SELECT to_char(first_at, 'YYYY-MM') AS fisrt_at, period, amount_retention
FROM (
    SELECT first.month_at AS first_at,
        datediff(month, first.month_at, _all.month_at) AS period,
        SUM(_all.amount) / max(first_month.amount)::FLOAT AS amount_retention
    FROM transfer_first first
    LEFT JOIN transfer_all _all
    ON first.uid = _all.uid AND first.month_at <= _all.month_at
    LEFT JOIN transfer_first_month first_month
    ON first_month.month_at = first.month_at
    GROUP BY 1, 2
)




-- 첫 번개 송금 기준 번개 송금 입금 횟수 리텐션

-- 전체 번개 송금 제품의 판매자 데이터
WITH transfer_all AS (
SELECT p.uid, t.month_at, count(t.id) as transfer_count
FROM (
    SELECT pid, DATE_trunc('month', updated_at) AS month_at, id
    FROM wire_transfer
    WHERE status = 'transfer_completed'
) t
JOIN product_info_for_stats p
ON t.pid = p.pid
group by 1, 2
),
-- 첫 판매 번개 송금 데이터
transfer_first AS (
SELECT uid, month_at, transfer_count
FROM (
    SELECT uid, month_at, transfer_count,
        ROW_NUMBER () OVER (PARTITION by uid ORDER BY month_at) AS _num
    FROM transfer_all
    )
WHERE _num = 1
),
transfer_first_month AS (
SELECT month_at, SUM(transfer_count) AS transfer_count
FROM transfer_first
GROUP BY 1
)

SELECT to_char(first_at, 'YYYY-MM') AS fisrt_at, period, count_retention
FROM (
    SELECT first.month_at AS first_at,
        datediff(month, first.month_at, _all.month_at) AS period,
        SUM(_all.transfer_count) / max(first_month.transfer_count)::FLOAT AS count_retention
    FROM transfer_first first
    LEFT JOIN transfer_all _all
    ON first.uid = _all.uid AND first.month_at <= _all.month_at
    LEFT JOIN transfer_first_month first_month
    ON first_month.month_at = first.month_at
    GROUP BY 1, 2
)






-- 가입월 기준 번개 송금 구매 입금액 리텐션

-- 전체 번개 송금 데이터
WITH transfer_all AS (
SELECT p.uid, t.month_at, sum(t.amount) as amount
FROM (
    SELECT pid, DATE_trunc('month', updated_at) AS month_at, product_price AS amount
    FROM wire_transfer
    WHERE status = 'transfer_completed'
) t
JOIN product_info_for_stats p
ON t.pid = p.pid
group by 1, 2
),
transfer_joined AS (
SELECT joined.uid, joined.joined_at, transfer_all.amount
FROM (
-- 월별 가입자 추출
    SELECT uid, date_trunc('month', updated) AS joined_at 
    FROM user_join_log
) joined
JOIN transfer_all
ON joined.uid = transfer_all.uid AND joined.joined_at = transfer_all.month_at
),
transfer_joined_month AS (
SELECT joined_at, SUM(amount) AS amount
FROM transfer_joined
GROUP BY 1
)



SELECT to_char(joined_at, 'YYYY-MM') AS joined_at, _period, amount_retention
FROM (
    SELECT joined.joined_at AS joined_at,
        datediff(month, joined.joined_at, _all.month_at) AS _period,
        sum(_all.amount) / max(joined_month.amount)::FLOAT AS amount_retention
    FROM transfer_joined joined
    LEFT JOIN transfer_all _all
    ON joined.uid = _all.uid AND joined.joined_at <= _all.month_at
    LEFT JOIN transfer_joined_month joined_month
    ON joined_month.joined_at = joined.joined_at
    GROUP BY 1, 2
)




-- 가입월 기준 번개 페이 판매 금액 리텐션

-- 전체 번개페이 데이터
WITH pay_all AS (
SELECT item.seller_id AS uid, mast.month_at, 
    COUNT(distinct mast.id) AS pay_count
FROM (
SELECT id, date_trunc('month', deposit_done_date) AS month_at, total_price
FROM order_mast
WHERE order_status_cd = 'purchase_confirm' AND deposit_done_date IS NOT NULL
) mast
JOIN order_item item
ON mast.id = item.order_mast_id
GROUP BY 1, 2
),
-- 첫 번개 페이 구매 데이터
pay_joined AS (
SELECT joined.uid, joined.joined_at, pay_all.pay_count
FROM (
-- 월별 가입자 추출
    SELECT uid, date_trunc('month', updated) AS joined_at 
    FROM user_join_log
    ) joined
JOIN pay_all
ON joined.uid = pay_all.uid AND joined.joined_at = pay_all.month_at
),
pay_first_month AS (
SELECT joined_at, SUM(pay_count) AS pay_count
FROM pay_joined
GROUP BY 1
)




SELECT to_char(joined_at, 'YYYY-MM') AS joined_at, period, count_retention
FROM (
    SELECT first.joined_at AS joined_at,
        datediff(month, first.joined_at, _all.month_at) AS period,
        SUM(_all.pay_count) / max(first_month.pay_count)::FLOAT AS count_retention
    FROM pay_joined first
    LEFT JOIN pay_all _all
    ON first.uid = _all.uid AND first.joined_at <= _all.month_at
    LEFT JOIN pay_first_month first_month
    ON first_month.joined_at = first.joined_at
    GROUP BY 1, 2
)


----------------------------------------------------------
----------------------------------------------------------




-- 첫 번개 페이 기준 번개 페이 구매 금액 리텐션

-- 전체 번개페이 데이터
WITH pay_all AS (
SELECT buyer_id AS uid, date_trunc('month', deposit_done_date) AS month_at, 
    SUM(total_price) AS amount
FROM order_mast
WHERE order_status_cd = 'purchase_confirm' and deposit_done_date is not null
GROUP BY 1, 2
),
-- 첫 번개 페이 구매 데이터
pay_first AS (
SELECT uid, MONTH_at, amount
FROM (
    SELECT uid, MONTH_at, amount,
        ROW_NUMBER () OVER (PARTITION by uid ORDER BY month_at) AS _num
    FROM pay_all
    )
WHERE _num = 1
),
pay_first_month AS (
SELECT month_at, SUM(amount) AS amount
FROM pay_first
GROUP BY 1
)



SELECT to_char(first_at, 'YYYY-MM') AS fisrt_at, period, amount_retention
FROM (
    SELECT first.month_at AS first_at,
        datediff(month, first.month_at, _all.month_at) AS period,
        SUM(_all.amount) / max(first_month.amount)::FLOAT AS amount_retention
    FROM pay_first first
    LEFT JOIN pay_all _all
    ON first.uid = _all.uid AND first.month_at <= _all.month_at
    LEFT JOIN pay_first_month first_month
    ON first_month.month_at = first.month_at
    GROUP BY 1, 2
)




-- 첫 번개 페이 기준 번개 페이 구매 횟수 리텐션

-- 전체 번개페이 데이터
WITH pay_all AS (
SELECT buyer_id AS uid, date_trunc('month', deposit_done_date) AS month_at, 
    COUNT(distinct id) AS pay_count
FROM order_mast
WHERE order_status_cd = 'purchase_confirm' and deposit_done_date is not null
GROUP BY 1, 2
),
-- 첫 번개 페이 구매 데이터
pay_first AS (
SELECT uid, MONTH_at, pay_count
FROM (
    SELECT uid, MONTH_at, pay_count,
        ROW_NUMBER () OVER (PARTITION by uid ORDER BY month_at) AS _num
    FROM pay_all
    )
WHERE _num = 1
),
pay_first_month AS (
SELECT month_at, SUM(pay_count) AS pay_count
FROM pay_first
GROUP BY 1
)



SELECT to_char(first_at, 'YYYY-MM') AS fisrt_at, period, amount_retention
FROM (
    SELECT first.month_at AS first_at,
        datediff(month, first.month_at, _all.month_at) AS period,
        SUM(_all.pay_count) / max(first_month.pay_count)::FLOAT AS amount_retention
    FROM pay_first first
    LEFT JOIN pay_all _all
    ON first.uid = _all.uid AND first.month_at <= _all.month_at
    LEFT JOIN pay_first_month first_month
    ON first_month.month_at = first.month_at
    GROUP BY 1, 2
)




-- 가입월 기준 번개 페이 구매 금액 리텐션

-- 전체 번개페이 데이터
WITH pay_all AS (
SELECT buyer_id AS uid, date_trunc('month', deposit_done_date) AS month_at, 
    SUM(total_price) AS amount
FROM order_mast
WHERE order_status_cd = 'purchase_confirm' AND deposit_done_date IS NOT NULL
GROUP BY 1, 2
),
-- 첫 번개 페이 구매 데이터
pay_joined AS (
SELECT joined.uid, joined.joined_at, pay_all.amount
FROM (
-- 월별 가입자 추출
    SELECT uid, date_trunc('month', updated) AS joined_at 
    FROM user_join_log
    ) joined
JOIN pay_all
ON joined.uid = pay_all.uid AND joined.joined_at = pay_all.month_at
),
pay_first_month AS (
SELECT joined_at, SUM(amount) AS amount
FROM pay_joined
GROUP BY 1
)



SELECT to_char(joined_at, 'YYYY-MM') AS joined_at, period, amount_retention
FROM (
    SELECT joined.joined_at AS joined_at,
        datediff(month, joined.joined_at, _all.month_at) AS period,
        SUM(_all.amount) / max(first_month.amount)::FLOAT AS amount_retention
    FROM pay_joined joined
    LEFT JOIN pay_all _all
    ON joined.uid = _all.uid AND joined.joined_at <= _all.month_at
    LEFT JOIN pay_first_month first_month
    ON first_month.joined_at = joined.joined_at
    GROUP BY 1, 2
)



-- 가입월 기준 번개 페이 구매 횟수 리텐션

-- 전체 번개페이 데이터
WITH pay_all AS (
SELECT buyer_id AS uid, date_trunc('month', deposit_done_date) AS month_at, 
    COUNT(distinct id) AS pay_count
FROM order_mast
WHERE order_status_cd = 'purchase_confirm' AND deposit_done_date IS NOT NULL
GROUP BY 1, 2
),
-- 첫 번개 페이 구매 데이터
pay_joined AS (
SELECT joined.uid, joined.joined_at, pay_all.pay_count
FROM (
-- 월별 가입자 추출
    SELECT uid, date_trunc('month', updated) AS joined_at 
    FROM user_join_log
    ) joined
JOIN pay_all
ON joined.uid = pay_all.uid AND joined.joined_at = pay_all.month_at
),
pay_first_month AS (
SELECT joined_at, SUM(pay_count) AS pay_count
FROM pay_joined
GROUP BY 1
)



SELECT to_char(joined_at, 'YYYY-MM') AS joined_at, period, amount_retention
FROM (
    SELECT joined.joined_at AS joined_at,
        datediff(month, joined.joined_at, _all.month_at) AS period,
        SUM(_all.pay_count) / max(first_month.pay_count)::FLOAT AS amount_retention
    FROM pay_joined joined
    LEFT JOIN pay_all _all
    ON joined.uid = _all.uid AND joined.joined_at <= _all.month_at
    LEFT JOIN pay_first_month first_month
    ON first_month.joined_at = joined.joined_at
    GROUP BY 1, 2
)





-- 첫 번개 페이 기준 번개 페이 판매 금액 리텐션

-- 전체 번개페이 데이터
WITH pay_all AS (
SELECT item.seller_id AS uid, mast.month_at, 
    SUM(mast.total_price) AS amount
FROM (
SELECT id, date_trunc('month', deposit_done_date) AS month_at, total_price
FROM order_mast
WHERE order_status_cd = 'purchase_confirm' AND deposit_done_date IS NOT NULL
) mast
JOIN order_item item
ON mast.id = item.order_mast_id
GROUP BY 1, 2
),
-- 첫 번개 페이 판매 데이터
pay_first AS (
SELECT uid, month_at, amount
FROM (
    SELECT uid, month_at, amount,
        ROW_NUMBER () OVER (PARTITION by uid ORDER BY month_at) AS _num
    FROM pay_all
    )
WHERE _num = 1
),
pay_first_month AS (
SELECT month_at, SUM(amount) AS amount
FROM pay_first
GROUP BY 1
)



SELECT to_char(first_at, 'YYYY-MM') AS fisrt_at, period, amount_retention
FROM (
    SELECT first.month_at AS first_at,
        datediff(month, first.month_at, _all.month_at) AS period,
        SUM(_all.amount) / max(first_month.amount)::FLOAT AS amount_retention
    FROM pay_first first
    LEFT JOIN pay_all _all
    ON first.uid = _all.uid AND first.month_at <= _all.month_at
    LEFT JOIN pay_first_month first_month
    ON first_month.month_at = first.month_at
    GROUP BY 1, 2
)





-- 첫 번개 페이 기준 번개 페이 판매 횟수 리텐션

-- 전체 번개페이 데이터
WITH pay_all AS (
SELECT item.seller_id AS uid, mast.month_at, 
    COUNT(distinct mast.id) AS pay_count
FROM (
SELECT id, date_trunc('month', deposit_done_date) AS month_at, total_price
FROM order_mast
WHERE order_status_cd = 'purchase_confirm' AND deposit_done_date IS NOT NULL
) mast
JOIN order_item item
ON mast.id = item.order_mast_id
GROUP BY 1, 2
),
-- 첫 번개 페이 판매 데이터
pay_first AS (
SELECT uid, month_at, pay_count
FROM (
    SELECT uid, month_at, pay_count,
        ROW_NUMBER () OVER (PARTITION by uid ORDER BY month_at) AS _num
    FROM pay_all
    )
WHERE _num = 1
),
pay_first_month AS (
SELECT month_at, SUM(pay_count) AS pay_count
FROM pay_first
GROUP BY 1
)



SELECT to_char(first_at, 'YYYY-MM') AS fisrt_at, period, count_retention
FROM (
    SELECT first.month_at AS first_at,
        datediff(month, first.month_at, _all.month_at) AS period,
        SUM(_all.pay_count) / max(first_month.pay_count)::FLOAT AS count_retention
    FROM pay_first first
    LEFT JOIN pay_all _all
    ON first.uid = _all.uid AND first.month_at <= _all.month_at
    LEFT JOIN pay_first_month first_month
    ON first_month.month_at = first.month_at
    GROUP BY 1, 2
)





-- 가입월 기준 번개 페이 판매 금액 리텐션

-- 전체 번개페이 데이터
WITH pay_all AS (
SELECT item.seller_id AS uid, mast.month_at, 
    SUM(mast.total_price) AS amount
FROM (
SELECT id, date_trunc('month', deposit_done_date) AS month_at, total_price
FROM order_mast
WHERE order_status_cd = 'purchase_confirm' AND deposit_done_date IS NOT NULL
) mast
JOIN order_item item
ON mast.id = item.order_mast_id
GROUP BY 1, 2
),
-- 첫 번개 페이 구매 데이터
pay_joined AS (
SELECT joined.uid, joined.joined_at, pay_all.amount
FROM (
-- 월별 가입자 추출
    SELECT uid, date_trunc('month', updated) AS joined_at 
    FROM user_join_log
    ) joined
JOIN pay_all
ON joined.uid = pay_all.uid AND joined.joined_at = pay_all.month_at
),
pay_first_month AS (
SELECT joined_at, SUM(amount) AS amount
FROM pay_joined
GROUP BY 1
)




SELECT to_char(joined_at, 'YYYY-MM') AS joined_at, period, amount_retention
FROM (
    SELECT joined.joined_at AS joined_at,
        datediff(month, joined.joined_at, _all.month_at) AS period,
        SUM(_all.amount) / max(first_month.amount)::FLOAT AS amount_retention
    FROM pay_joined joined
    LEFT JOIN pay_all _all
    ON joined.uid = _all.uid AND joined.joined_at <= _all.month_at
    LEFT JOIN pay_first_month first_month
    ON first_month.joined_at = joined.joined_at
    GROUP BY 1, 2
)




-- 가입월 기준 번개 페이 판매 금액 리텐션

-- 전체 번개페이 데이터
WITH pay_all AS (
SELECT item.seller_id AS uid, mast.month_at, 
    COUNT(distinct mast.id) AS pay_count
FROM (
SELECT id, date_trunc('month', deposit_done_date) AS month_at, total_price
FROM order_mast
WHERE order_status_cd = 'purchase_confirm' AND deposit_done_date IS NOT NULL
) mast
JOIN order_item item
ON mast.id = item.order_mast_id
GROUP BY 1, 2
),
-- 첫 번개 페이 구매 데이터
pay_joined AS (
SELECT joined.uid, joined.joined_at, pay_all.pay_count
FROM (
-- 월별 가입자 추출
    SELECT uid, date_trunc('month', updated) AS joined_at 
    FROM user_join_log
    ) joined
JOIN pay_all
ON joined.uid = pay_all.uid AND joined.joined_at = pay_all.month_at
),
pay_first_month AS (
SELECT joined_at, SUM(pay_count) AS pay_count
FROM pay_joined
GROUP BY 1
)




SELECT to_char(joined_at, 'YYYY-MM') AS joined_at, period, count_retention
FROM (
    SELECT first.joined_at AS joined_at,
        datediff(month, first.joined_at, _all.month_at) AS period,
        SUM(_all.pay_count) / max(first_month.pay_count)::FLOAT AS count_retention
    FROM pay_joined first
    LEFT JOIN pay_all _all
    ON first.uid = _all.uid AND first.joined_at <= _all.month_at
    LEFT JOIN pay_first_month first_month
    ON first_month.joined_at = first.joined_at
    GROUP BY 1, 2
)
