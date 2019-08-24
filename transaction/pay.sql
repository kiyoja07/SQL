
/*
	sql 분석 기초 작업
*/

create database homework;
use homework;

create table user_usage (
user_id char(20) PRIMARY KEY,
user_type char(20),
amount_type_1 integer,
cnt_type_1 integer,
amount_type_2 integer,
cnt_type_2 char(20) 
);
			
create table _transaction (
user_id char(20),
gender char(20),
age integer,
svc_type char(20), 
_date datetime,
tid integer PRIMARY KEY, 
amount integer,
FOREIGN KEY (user_id) REFERENCES user_usage (user_id)
);

/*
	3번
    user_usage data에서 
    금액 혹은 건수의 구간대별 통계, 2개 쿼리
*/

-- 기본 통계 보기
select min(amount_type_1), avg(amount_type_1), max(amount_type_1), stddev(amount_type_1), 
	min(cnt_type_1), avg(cnt_type_1), max(cnt_type_1) , stddev(cnt_type_1),
    min(amount_type_2), avg(amount_type_2), max(amount_type_2) , stddev(amount_type_2),
    min(cnt_type_2), avg(cnt_type_2), max(cnt_type_2), stddev(cnt_type_2)
from user_usage
;

-- type_1과 type_2 모두에서 cnt보다 amount의 편차가 크다
-- amount를 기준으로 구간대를 나누는 것 보다 cnt를 기준으로 구간대를 나누면 구간대의 amount의 차이가 더 뚜렷할 것이다.
-- cnt를 기준으로 amount를 살펴본다.

-- cnt의 type1과 type2를 더해서 sum_cnt를 기준으로 랭킹을 매겨서 amount를 더 자세히 살펴보자

select rank () over (order by sum_cnt ) as ranking_amount, user_id, sum_cnt, sum_amount, amount_type_1, amount_type_2, cnt_type_1,  cnt_type_2
from (select user_id, amount_type_1  + amount_type_2 as sum_amount , amount_type_1, amount_type_2,
cnt_type_1 + cnt_type_2 as sum_cnt, cnt_type_1,  cnt_type_2
from user_usage) t
;

-- sum_cnt가 증가할 수록 sum_amount가 함께 증가한다.
-- 하지만 기하급수적으로 증가하는 것 같지는 않다. 
-- 거래 건당 평균 금액(amount / cnt)을 살펴보면 확인할 수 있을 것이다.
-- cnt에 대한 사전 지식이 없다. 기본 적으로 cnt를 기준으로 백분위로 나누어서 구간대를 설정해보자. cnt 구간의 증가에 따른 변화를 확인해볼 수 있을 것이다.

with raw as (
select truncate(percent_rank () over (order by sum_cnt ), 1) as percent, user_id, sum_cnt, sum_amount, amount_type_1, amount_type_2, cnt_type_1,  cnt_type_2
from (select user_id, amount_type_1  + amount_type_2 as sum_amount , amount_type_1, amount_type_2,
cnt_type_1 + cnt_type_2 as sum_cnt, cnt_type_1,  cnt_type_2
from user_usage) t
)
select if(percent = 1, 0.9, percent) as 'percent / 100', 
	sum(amount_type_1) / sum(cnt_type_1) ,  sum(amount_type_2) / sum(cnt_type_2)
from raw
group by 1
;

-- sum_cnt, 즉 type_1과 type_2의 건 수의 합이 증가하더라도, type_1과 type_2의 평균 거래 금액은 크게 변하지 않는다.
-- 평균 거래 금액은 type_2가 type_1보다 훨씬 크다.

-- 많은 사회과학 데이터에서 볼 수 있듯이 user_usage 데이터도 정규분포를 가진다고 가정한다.
-- 정규분포의 구간에 따라 데이터를 살펴본다.
-- 앞서 cnt를 기준으로 구간을 나누어 보았기 때문에, 이번에는 amount를 기준으로 구간을 나눠본다.
-- 앞서 거래 건당 금액을 살펴보았다. 이번에는 유저의 행동도 살펴보자. 구간대에 따른 유저의 평균 거래 건을 살펴본다.
-- amount가 정규분포를 가진다고 가정하고, 표준편차에 의한 구간을 나누어 본다.

with raw as (
select  std(amount_type_1  + amount_type_2) as _std, avg(amount_type_1  + amount_type_2) as _avg
from user_usage
)
select case
	when u.amount_type_1  + u.amount_type_2 < r._avg - r._std then 'less_than_-sigma'
    when u.amount_type_1  + u.amount_type_2 >= r._avg - r._std  and u.amount_type_1  + u.amount_type_2 < r._avg then 'less_than_avg'
    when u.amount_type_1  + u.amount_type_2 < r._avg + r._std  and u.amount_type_1  + u.amount_type_2 >= r._avg then 'greater_than_avg'
    else 'greater_than_+sigma'
    end as 'category', 
	count(u.user_id),
    sum(u.cnt_type_1) / count(u.user_id), 
    sum(u.cnt_type_2) / count(u.user_id)
from raw r, user_usage u  
group by category
;

-- type_1과 type_2의 거래 금액의 합을 구간으로 하여 구간에 분포하는 데이터를 산출하였다.
-- less than avg 구역에 유저 1.5만 중 1.2만이 있다. 구간 값이 커질 수록 유저 수가 큰 폭으로 작아진다.
-- log를 취해서 구간을 보정해본다.

with raw as (
select  std(log(amount_type_1  + amount_type_2)) as _std, avg(log(amount_type_1  + amount_type_2)) as _avg
from user_usage
)
select case
	when log(u.amount_type_1  + u.amount_type_2) < r._avg - r._std then 'less_than_-sigma'
    when log(u.amount_type_1  + u.amount_type_2) >= r._avg - r._std  and log(u.amount_type_1  + u.amount_type_2) < r._avg then 'less_than_avg'
    when log(u.amount_type_1  + u.amount_type_2) < r._avg + r._std  and log(u.amount_type_1  + u.amount_type_2) >= r._avg then 'greater_than_avg'
    else 'greater_than_+sigma'
    end as 'category', 
    count(u.user_id),
    sum(u.cnt_type_1) / count(u.user_id), 
    sum(u.cnt_type_2) / count(u.user_id)
from raw r, user_usage u  
group by category
;

-- 유저 수를 보면 -sigma ~ +sigma 구간에 많은 유저들이 분포해있다. 구간 보정 효과가 있다.
-- 금액에 따른 log 정규분포에 의한 구간대에서 유저당 거래 건수는 type_2가 훨씬 크다.
-- 앞 선 데이터에서도 type_2의 거래 건당 평균 금액이 컸다.
-- type_1은 기본적인 거래, type_2는 특별한 거래라고 생각해볼 수 있다.

/*
	4번
    transaction data에서 
    svc_type_before, svc_type_after, avg_date_diff, tx_cnt
*/

with tx_id_date as (
select row_number () over (partition by user_id order by _date) as tx_num, user_id, _date, svc_type
from _transaction
)
select p.svc_type as svc_type_before, a.svc_type as svc_type_after, avg(datediff(a._date, p._date)) as avg_date_diff, count(a.tx_num)as tx_count
from tx_id_date p
join tx_id_date a
on (p.tx_num = a.tx_num - 1 and p.user_id = a.user_id)
group by 1, 2 
order by 1, 2
;

