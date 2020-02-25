
CREATE TABLE public.test_table (id serial primary key, created timestamptz not null);

ALTER TABLE public.test_table OWNER TO admin;

-- CREATE PUBLICATION pub FOR TABLE test_table;

-- SELECT * FROM pg_create_logical_replication_slot('my_slot', 'pgoutput');

