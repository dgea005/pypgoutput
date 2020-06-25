# py-pgoutput
python pgoutput reader

Decodes messages from the pgoutput Postgres plugin.

Messages specified here: https://www.postgresql.org/docs/10/protocol-logicalrep-message-formats.html 


```
docker-compose up

psql -h localhost -p 5432 -U test test

CREATE PUBLICATION pub FOR TABLE test_table;

DROP PUBLICATION pub;

ALTER PUBLICATION pub ADD TABLE test_table_2;

SELECT * FROM pg_create_logical_replication_slot('my_slot', 'pgoutput');

```

## useful queries

```
SELECT * FROM pg_publication;
SELECT * FROM pg_replication_slots;
```

## start
```
python3 get_replication_records.py
```

## test queries

```
insert into test_table (created) 
select created 
from (
    select now() AS "created", 
    generate_series(1, 5)
) x RETURNING *;

insert into test_table_2 (id, created) 
select id, created 
from (
    select generate_series(1, 5) AS "id", 
           now() AS "created"
) x RETURNING *;

truncate test_table;


update test_table set created = now() where id = (select min(id) from test_table) returning *;
update test_table set id = (select max(id)+10 from test_table) where id = (select min(id) from test_table) returning *;
delete from test_table where id = (select max(id) from test_table) returning *;


```
