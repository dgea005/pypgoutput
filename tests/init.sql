

CREATE OR REPLACE FUNCTION public.updated_at_trigger ()  RETURNS trigger
  VOLATILE
AS $body$
BEGIN
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$body$ LANGUAGE plpgsql;


CREATE TABLE public.test (id integer primary key, updated_at timestamptz not null);


CREATE TRIGGER zzz_updated_at_trigger
    BEFORE INSERT OR UPDATE
    ON public.test
    FOR EACH ROW
    EXECUTE PROCEDURE public.updated_at_trigger();

CREATE TABLE public.test_table_2 (id integer primary key, updated_at timestamptz not null);


CREATE TRIGGER zzz_updated_at_trigger
    BEFORE INSERT OR UPDATE
    ON public.test_table_2
    FOR EACH ROW
    EXECUTE PROCEDURE public.updated_at_trigger();
