# MarinaDB

SELECT PRODUCT_ORDER.USER_ID
      ,USER.USER_NM
      ,USER_PROFILE_IMG.IMG_URL
      ,SUM(CASE WHEN PRODUCT_ORDER.ORDER_STATUS = 'COMPLETE' THEN 1 ELSE 0 END) AS COMPLETE_CNT
      ,SUM(CASE WHEN PRODUCT_ORDER.ORDER_STATUS = 'SHIPPING' THEN 1 ELSE 0 END) AS SHIPPING_CNT
      ,SUM(CASE WHEN PRODUCT_ORDER.ORDER_STATUS = 'READY' THEN 1 ELSE 0 END) AS READY_CNT
FROM PRODUCT_ORDER
JOIN USER
ON PRODUCT_ORDER.USER_ID = USER.USER_ID
LEFT JOIN USER_PROFILE_IMG
ON PRODUCT_ORDER.USER_ID = USER_PROFILE_IMG.USER_ID
WHERE PRODUCT_ORDER.USER_ID = 'user@gmail.com'
    and PRODUCT_ORDER.REGISTERED_AT BETWEEN DATE('2018-12-31 23:59:59') - interval 90 day and '2018-12-31 23:59:59'
;