create table counsel (
    counsel_times INT,
    id VARCHAR(255) NOT NULL,
    teacher INT,
    satisfied_score INT,
    PRIMARY KEY (counsel_times, id, teacher, satisfied_score)
);

select *
from counsel
;

create table invest (
	id varchar(255) not null, 
    _month int, 
    krw int,
	primary key (id, _month, krw)
);

select *
from invest
;


# uuid,age,kor,eng,math,dangi_login_times,total_video_watch_minute

create table result (
	id varchar(255) not null,
    age int, 
    kor int,
    eng int,
    math int,
    login_times int, 
    total_video_watch_minute int, 
	primary key (id, age, kor, eng, math, login_times, total_video_watch_minute)
);

select *
from result
;

# test_num,uuid,kor,eng,math
create table test (
	test_num int,
    id varchar(255) not null,
    kor int,
    eng int,
    math int,
    primary key (test_num, id, kor, eng, math)
);

select *
from test
;

show tables;


#drop table counsel;

select distinct c.id, sum(r.login_times) as sum_login_times 
from counsel c
inner join result r on c.id = r.id
where r.age > 19
group by c.id
having sum_login_times > 1000
#order by
;

select distinct t.id, t.avg_kor, max(c.satisfied_score) as max, min(c.satisfied_score) as min
from counsel c
inner join (select distinct id, avg(kor) as avg_kor
	from test
	group by id
	having avg_kor >= 80) t
on c.id = t.id
group by t.id
union
select distinct t.id, t.avg_kor, max(c.satisfied_score) as max, min(c.satisfied_score) as min
from counsel c
inner join (select distinct id, avg(kor) as avg_kor
	from test
	group by id
	having avg_kor < 60) t
on c.id = t.id
group by t.id
;












