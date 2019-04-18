-- SQLite

-- When the number of rows is odd

create table weight_odd (
    id VARCHAR(255) PRIMARY KEY,
    nick_name VARCHAR(255),
    weight_kg INTEGER
);

insert into weight_odd values ('1', 'ohs' ,77);
insert into weight_odd values ('2', 'sita', 79);
insert into weight_odd values ('3', 'ace', 85);
insert into weight_odd values ('4', 'bread', 64);
insert into weight_odd values ('5', 'coke', 66);
insert into weight_odd values ('6', 'dream', 78);
insert into weight_odd values ('7', 'eve', 80);
insert into weight_odd values ('8', 'five', 69);
insert into weight_odd values ('9', 'gold', 75);

-- calculate the median from the weight_odd

SELECT weight_kg
FROM weight_odd
ORDER BY weight_kg
LIMIT 1
OFFSET (SELECT COUNT(*)
        FROM weight_odd) / 2
;

SELECT "-------------"
;

SELECT weight_kg
FROM weight_odd
ORDER BY weight_kg
;


-- When the number of rows is even

create table weight_even (
    id VARCHAR(255) PRIMARY KEY,
    nick_name VARCHAR(255),
    weight_kg INTEGER
);

insert into weight_even values ('1', 'ohs' ,77);
insert into weight_even values ('2', 'sita', 79);
insert into weight_even values ('3', 'ace', 85);
insert into weight_even values ('4', 'bread', 64);
insert into weight_even values ('5', 'coke', 66);
insert into weight_even values ('6', 'dream', 78);
insert into weight_even values ('7', 'eve', 80);
insert into weight_even values ('8', 'five', 69);
insert into weight_even values ('9', 'gold', 75);
insert into weight_even values ('10', 'hunger', 72);

-- calculate the median from the weight_even

SELECT AVG(weight_kg)
FROM (SELECT weight_kg
      FROM weight_even
      ORDER BY weight_kg
      LIMIT 2
      OFFSET (SELECT (COUNT(*) - 1) / 2
              FROM weight_even))
;

SELECT "-------------"
;

SELECT weight_kg
FROM weight_even
ORDER BY weight_kg
;


-- combine odd and even cases

-- calculate odd case

SELECT AVG(weight_kg)
FROM (SELECT weight_kg
      FROM weight_odd
      ORDER BY weight_kg
      LIMIT 2 - (SELECT COUNT(*) FROM weight_odd) % 2    -- LIMIT : odd 1, even 2
      OFFSET (SELECT (COUNT(*) - 1) / 2
              FROM weight_odd))
;

-- calculate even case

SELECT AVG(weight_kg)
FROM (SELECT weight_kg
      FROM weight_even
      ORDER BY weight_kg
      LIMIT 2 - (SELECT COUNT(*) FROM weight_even) % 2    -- LIMIT : odd 1, even 2
      OFFSET (SELECT (COUNT(*) - 1) / 2
              FROM weight_even))
;