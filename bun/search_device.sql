

-- 디바이스별 키워드 대비 검색량
-- 비회원(uid = -1) 제외
-- Redshift 2019년 4월 데이터
 
 
-- 전체 키워드 대비 검색량
SELECT viewer_device,
    COUNT(DISTINCT keyword) AS COUNT_keyword, COUNT(id) AS COUNT_search,
    count(id) / count(DISTINCT keyword)::FLOAT AS total_search_per_keyword
FROM item_search_log
WHERE viewer_device IN ('i', 'a', 'w') AND viewer_uid > -1 AND
    updated >= '2019-04-01 00:00:00' AND updated < '2019-05-01 00:00:00'
GROUP BY 1



-- 전체 키워드 대비 검색의 유저 성, 연령, 전문판매자
SELECT i.viewer_device, u.age, u.gender, u.bizlicense, 
    COUNT(DISTINCT keyword) AS COUNT_keyword, COUNT(id) AS COUNT_search
FROM item_search_log i
LEFT JOIN USER_for_stats u
ON i.viewer_uid = u.uid
WHERE i.viewer_device IN ('i', 'a', 'w') AND i.viewer_uid > -1 AND
    i.updated >= '2019-04-01 00:00:00' AND i.updated < '2019-05-01 00:00:00'
GROUP BY 1, 2, 3, 4


-- 유저별 키워드 대비 검색량
SELECT viewer_device, 
    count(keyword) AS count_keyword, sum(count_search) AS count_search,
    sum(count_search) / count(keyword)::FLOAT AS search_per_keyword_per_user
FROM (
SELECT viewer_uid, viewer_device, keyword,
    COUNT(id) AS COUNT_search
FROM item_search_log
WHERE viewer_device IN ('i', 'a', 'w') AND viewer_uid > -1 AND
    updated >= '2019-04-01 00:00:00' AND updated < '2019-05-01 00:00:00'
GROUP BY 1, 2, 3)
GROUP BY 1


-- 1회 검색한 키워드를 제외한 유저별 키워드 대비 검색량
SELECT viewer_device, 
    count(keyword) AS count_keyword, sum(count_search) AS count_search,
    sum(count_search) / count(keyword)::FLOAT AS search_per_keyword_per_user
FROM (
SELECT viewer_uid, viewer_device, keyword,
    COUNT(id) AS COUNT_search
FROM item_search_log
WHERE viewer_device IN ('i', 'a', 'w') AND viewer_uid > -1 AND
    updated >= '2019-04-01 00:00:00' AND updated < '2019-05-01 00:00:00'
GROUP BY 1, 2, 3)
WHERE count_search > 1
GROUP BY 1


-- 유저별 전체 (검색한) 키워드 대비 1회 검색한 키워드의 비율
SELECT viewer_device,
    sum(CASE WHEN count_search = 1 THEN 1 ELSE 0 END) AS one_search,
    count(viewer_uid) AS total_search,
    one_search / total_search::FLOAT AS one_per_total_search
FROM (
SELECT viewer_uid, viewer_device, keyword,
    COUNT(id) AS COUNT_search
FROM item_search_log
WHERE viewer_device IN ('i', 'a', 'w') AND viewer_uid > -1 AND
    updated >= '2019-04-01 00:00:00' AND updated < '2019-05-01 00:00:00'
GROUP BY 1, 2, 3)
GROUP BY 1



-- 유저별 전체 (검색한) 키워드 대비 1회 검색한 키워드의 비율 & 전문 상점 여부
SELECT i.viewer_device, u.bizlicense,
    sum(CASE WHEN i.count_search = 1 THEN 1 ELSE 0 END) AS one_search,
    count(i.viewer_uid) AS total_search,
    one_search / total_search::FLOAT AS one_per_total_search
FROM (
SELECT viewer_uid, viewer_device, keyword,
    COUNT(id) AS COUNT_search
FROM item_search_log
WHERE viewer_device IN ('i', 'a', 'w') AND viewer_uid > -1 AND
    updated >= '2019-04-01 00:00:00' AND updated < '2019-05-01 00:00:00'
GROUP BY 1, 2, 3) i
LEFT JOIN user_for_stats u
ON i.viewer_uid = u.uid
GROUP BY 1, 2



-- 키워드 다양성 
SELECT viewer_device,
    count(DISTINCT keyword) AS unique_keyword,
    count(keyword) AS total_keyword,
    unique_keyword / total_keyword::FLOAT AS total_per_one
FROM (
SELECT viewer_uid, viewer_device, keyword,
	COUNT(id) AS COUNT_search
FROM item_search_log
WHERE viewer_device IN ('i', 'a', 'w') AND
    updated >= '2019-04-01 00:00:00' AND updated < '2019-05-01 00:00:00'
GROUP BY 1, 2, 3)
GROUP BY 1

