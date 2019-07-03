
-- 차단 사용자 목록
SELECT DISTINCT uid
FROM 
(
SELECT uid
FROM user_auth_token
WHERE policy_id=2

UNION ALL

SELECT ud.uid
FROM user_device AS ud
  LEFT OUTER JOIN device_group_exception AS dge ON ud.udid_str=dge.udid AND ud.uid=dge.uid
  LEFT OUTER JOIN (SELECT gps.udid
                   FROM device_group_block_policy_status AS gps
                     JOIN block_history_device_group AS gh ON gps.udid=gh.udid
                   WHERE gps.policy_id=2
                     AND gh.is_active=1
                     AND gh.policy_type1_category IN ('거래사기')) AS blocked_udid ON ud.udid_str=blocked_udid.udid
WHERE ud.`status` = 0
  AND ud.udid_str != ''
  AND dge.uid IS NULL
  AND blocked_udid.udid IS NOT NULL
) AS t


-- 상담센터 거래관련 신고건의 신고 대상 uid
select distinct i.value
from help_extra_info i
join (
    select discussion_id
    from help_discussion
    where status = 1 and category_id in 
        ('transaction',
        'transaction-bad-item',
        'transaction-bunpay',
        'transaction-buyer',
        'transaction-etc',
        'transaction-no-paid',
        'transaction-no-ship',
        'transaction-no-ship-fee',
        'transaction-refund',
        'transaction-seller',
        'transaction-trade',
        'etc-join')
) d
on i.discussion_id = d.discussion_id
where i.key_name = 'targetUid'