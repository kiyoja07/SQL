

-- 등록 & 불가

with register as (
-- 월별 등록된 상품
SELECT date_trunc('month', updated) AS MONTH_at, COUNT(DISTINCT pid) AS registered
FROM product_register_history
GROUP BY 1
),
sold as (
-- 월별 예약, 삭제, 판매 완료된 제품
-- status IN (1, 2, 3)
SELECT date_trunc('month', updated) AS MONTH_at, COUNT(DISTINCT pid) AS sold
FROM product_status_change_log
WHERE status IN (1, 2, 3)
GROUP BY 1
)

select register.month_at, register.registered, sold.sold
from register
full outer join sold
on register.month_at = sold.month_at
order by 1




-- status IN (1, 2, 3) 인 상품의 최초 월별 상품 수
WITH ordered AS (
SELECT updated, pid,
    ROW_NUMBER () OVER (PARTITION by pid ORDER BY updated) AS _num
FROM (
    SELECT updated, pid
    FROM product_status_change_log
    WHERE status IN (1, 2, 3)
    )
)

SELECT date_trunc('month', updated) AS MONTH_at, COUNT(DISTINCT pid) AS sell
FROM ordered
WHERE _num = 1
GROUP BY 1
ORDER BY 1


-- 재판매
-- 월별 다시 판매 중으로 바꾼 상품
-- status가 (1, 2, 3) -> 0
SELECT DATE_trunc('month', updated) AS month_at, COUNT(DISTINCT p.pid) AS re_sell
FROM product_status_change_log p
JOIN (
    SELECT DATE_trunc('month', updated) AS month_at, pid
    FROM product_status_change_log
    WHERE status IN (1, 2, 3)
) s
ON p.pid = s.pid AND DATE_trunc('month', p.updated)> s.month_at AND p.status = 0
GROUP BY 1
ORDER BY 1


-- 재판매 불가
-- 판매 불가 status에서 다시 판매 불가 status로 바뀐 제품 
-- status가 (1, 2, 3) -> (1, 2, 3)
SELECT DATE_trunc('month', updated) AS month_at, COUNT(DISTINCT p.pid) AS re_sell
FROM product_status_change_log p
JOIN (
    SELECT DATE_trunc('month', updated) AS month_at, pid
    FROM product_status_change_log
    WHERE status IN (1, 2, 3)
) s
ON p.pid = s.pid AND DATE_trunc('month', p.updated)> s.month_at AND p.status IN (1, 2, 3)
GROUP BY 1
ORDER BY 1


-- noChange
-- status의 change가 없는 제품의 등록월별 현재 판매불가 상품
SELECT DATE_trunc('month', i.register_date) AS month_at, COUNT(DISTINCT i.pid)
FROM product_info_for_stats i
LEFT JOIN product_status_change_log c
ON i.pid = c.pid
WHERE c.pid IS NULL and i.status in (1, 2, 3)
GROUP BY 1
ORDER BY 1



-- 검증
-- 현재의 status별 제품 수
SELECT status, COUNT(DISTINCT pid)
FROM product_info_for_stats
GROUP BY 1
ORDER BY 1




-- 사용 안함, 쿼리 합침
-- 최초 등록
-- 월별 등록된 상품
SELECT date_trunc('month', updated) AS MONTH_at, COUNT(DISTINCT pid) AS registered
FROM product_register_history
GROUP BY 1
ORDER BY 1

-- 사용 안함, 참고 용
-- status = 0 인 상품의 최초 월별 상품 수
WITH ordered AS (
SELECT updated, pid,
    ROW_NUMBER () OVER (PARTITION by pid ORDER BY updated) AS _num
FROM (
    SELECT updated, pid
    FROM product_status_change_log
    WHERE status = 0
)
)

SELECT date_trunc('month', updated) AS MONTH_at, COUNT(DISTINCT pid) AS sell
FROM ordered
WHERE _num = 1
GROUP BY 1
ORDER BY 1


-- 사용 안함, 쿼리 합침
-- 판매 불가
-- 월별 예약, 삭제, 판매 완료된 제품
-- status IN (1, 2, 3)
SELECT date_trunc('month', updated) AS MONTH_at, COUNT(DISTINCT pid) AS sold
FROM product_status_change_log
WHERE status IN (1, 2, 3)
GROUP BY 1
ORDER BY 1


-- noChange
-- 사용 안 함
-- status의 change가 없는 제품의 등록월별 현재 status
SELECT DATE_trunc('month', i.register_date) AS MONTH_at, i.status, COUNT(DISTINCT i.pid)
FROM product_info_for_stats i
LEFT JOIN product_status_change_log c
ON i.pid = c.pid
WHERE c.pid IS NULL
GROUP BY 1, 2
ORDER BY 1, 2