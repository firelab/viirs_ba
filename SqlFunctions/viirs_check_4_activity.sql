-- Function: viirs_check_4_activity(timestamp without time zone, interval)

-- DROP FUNCTION viirs_check_4_activity(timestamp without time zone, interval);

CREATE OR REPLACE FUNCTION viirs_check_4_activity(
    varchar(200),
    timestamp without time zone,
    interval)
  RETURNS void AS
$BODY$
DECLARE
  schema varchar(200) := $1;
  collection timestamp without time zone := $2;
  recent interval := $3;
BEGIN
EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_collections SET active = FALSE where age($1, last_update) > $2'
   USING collection, recent ;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_check_4_activity(varchar(200), timestamp without time zone, interval)
  OWNER TO postgres;
