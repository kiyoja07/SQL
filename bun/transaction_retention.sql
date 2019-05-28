

-- 첫 구매월 기준 구매 리텐션

-- 전체 구매 데이터
WITH bunp_all AS (
SELECT buyer_uid, date_trunc('month', updated_at) AS month_at, 
    sum(seller_pid_price) AS bunp_amount, COUNT(id) AS bunp_count
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price < 3000000
GROUP BY 1, 2
HAVING bunp_amount > 0
),
-- 첫 구매월 데이터 추출
first_bunp AS (
SELECT buyer_uid, month_at, bunp_amount, bunp_count
FROM (
    SELECT buyer_uid, month_at, bunp_amount, bunp_count,
        ROW_NUMBER () OVER (PARTITION by buyer_uid ORDER BY month_at) AS _num
    FROM bunp_all
    )
WHERE _num = 1
)

SELECT to_char(first_at, 'YYYY-MM') as fisrt_at, _period, count_retention, amount_retention
FROM (
SELECT first.month_at AS first_at, _all.month_at AS bunp_at,
    datediff(month, first.month_at, _all.month_at) AS _period,
    count(DISTINCT _all.buyer_uid) AS users,
    sum(_all.bunp_count) / sum(first.bunp_count)::FLOAT AS count_retention,
    sum(_all.bunp_amount) / sum(first.bunp_amount)::FLOAT AS amount_retention
FROM first_bunp first
LEFT JOIN bunp_all _all
ON first.buyer_uid = _all.buyer_uid AND first.month_at <= _all.month_at
GROUP BY 1, 2)



-- 가입월 기준 구매 리텐션

-- 전체 구매 데이터
WITH bunp_all AS (
SELECT buyer_uid, date_trunc('month', updated_at) AS month_at, 
    sum(seller_pid_price) AS bunp_amount, COUNT(id) AS bunp_count
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price < 3000000
GROUP BY 1, 2
HAVING bunp_amount > 0
),
-- 가입월 거래 데이터 추출
joined_bunp AS (
SELECT joined.uid, joined.joined_at, bunp_all.bunp_amount, bunp_all.bunp_count
FROM (
-- 월별 가입자 추출
    SELECT uid, date_trunc('month', updated) AS joined_at 
    FROM user_join_log
) joined
JOIN bunp_all
ON joined.uid = bunp_all.buyer_uid AND joined.joined_at = bunp_all.month_at
)

SELECT to_char(joined_at, 'YYYY-MM') as joined_at, _period, count_retention, amount_retention
FROM (
SELECT joined.joined_at AS joined_at, _all.month_at AS bunp_at,
    COUNT(DISTINCT joined.uid) AS users,
    datediff(month, joined.joined_at, _all.month_at) AS _period,
    sum(_all.bunp_count) / sum(joined.bunp_count)::FLOAT AS count_retention,
    sum(_all.bunp_amount) / sum(joined.bunp_amount)::FLOAT AS amount_retention
FROM joined_bunp joined
LEFT JOIN bunp_all _all
ON joined.uid = _all.buyer_uid AND joined.joined_at <= _all.month_at
GROUP BY 1, 2)








WITH bunp_info AS (
SELECT complite.bunp_month,
    json_extract_path_text(params, 'buyer_uid') AS buyer,
    -- json_extract_path_text(params, 'seller_uid') AS seller,
    SUM(json_extract_path_text(params, 'seller_pid_price')) AS bunp_price
FROM (
    SELECT DISTINCT date_trunc('month', updated) AS bunp_month,
        json_extract_path_text(params, 'bunp_id') AS bunp_id
    FROM bunp
    WHERE log_type = 'complite' 
    -- AND updated >= '2019-01-01 00:00:00'
) complite
LEFT JOIN bunp
ON complite.bunp_id = bunp.bunp_id
WHERE bunp.log_type = 'make'
GROUP BY 1,2
),
-- 유저의 가입월의 번프 거래액
new_user_bunp AS (
SELECT DISTINCT date_trunc('month', user_join_log.updated) AS joined_month, 
    user_join_log.uid,
    bunp_info.bunp_price
FROM user_join_log
LEFT JOIN bunp_info
ON user_join_log.uid = bunp_info.buyer
)


SELECT new_user.joined_month, (bunp_info.bunp_month - new_user.joined_month) AS period,
--     count(DISTINCT new_user.uid) AS new_user,
    count(DISTINCT bunp_info.buyer) / count(DISTINCT new_user.uid)::FLOAT AS bunp_user_retention,
--     sum(new_user.bupn_price) AS new_user_amount,
    sum(bunp_info.bunp_price) / sum(new_user.bunp_price):: FLOAT AS bunp_amount_retention
FROM new_user_bunp new_user
LEFT JOIN bunp_info
ON new_user.uid = bunp_info.buyer AND new_user.joined_month < bunp_info.bunp_month
GROUP BY 1, 2


