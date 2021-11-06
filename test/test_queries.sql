\set autocommit off;

BEGIN;

INSERT INTO test_table (id, created) VALUES (4, '2011-01-01 12:00:00');
INSERT INTO test_table (id, created) VALUES (5, '2012-01-01 12:00:00');
INSERT INTO test_table (created, id) VALUES ('2014-01-01 12:00:00', 6);
UPDATE test_table set created = '2013-01-01 12:00:00' WHERE id = 5 ;
DELETE FROM test_table where id = 4;
COMMIT;

BEGIN;
TRUNCATE test_table;
COMMIT;
