CREATE TABLE public.test_table (id integer primary key, created timestamptz not null);
CREATE TABLE public.test_table_2 (id integer primary key, created timestamptz not null);
CREATE PUBLICATION pub FOR TABLE test_table;
SELECT * FROM pg_create_logical_replication_slot('my_slot', 'pgoutput');
