
-- https://medium.com/daangn/%ED%8C%8C%EC%9B%8C%EC%9C%A0%EC%A0%80-%EC%BB%A4%EB%B8%8C-%EC%8A%A4%EB%A7%88%EC%9D%BC-%EC%BB%A4%EB%B8%8C-5762ae5854e7?fbclid=IwAR0UGnmHm2-EHu_DvsYfjb59JDwGE7HdY66fAL5fNwiQ8yy6yXb9KvClB98


SELECT
  t.launch_count,
  count(*) AS user_count
FROM
  (
    SELECT
    v.udid, count(*) AS launch_count
    from(
      SELECT
        DISTINCT
        date(updated),
        json_extract_path_text(json_data, 'udid', true) AS udid
      FROM
        super_log
      WHERE
        updated BETWEEN '2018-12-01 00:00:00' AND '2018-12-31 23:59:59'
      AND
        log_name = 'launch'
    ) v
    GROUP BY
    v.udid
  ) t
GROUP BY
  t.launch_count