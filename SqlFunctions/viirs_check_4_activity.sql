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
EXECUTE 'UPDATE $1.fire_collections SET active = FALSE where age($2, last_update) > $3'
   USING schema, collection, recent ;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_check_4_activity(varchar(200), timestamp without time zone, interval)
  OWNER TO postgres;
