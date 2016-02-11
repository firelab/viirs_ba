-- Function: viirs_check_4_activity(timestamp without time zone, interval)

-- DROP FUNCTION viirs_check_4_activity(timestamp without time zone, interval);

CREATE OR REPLACE FUNCTION viirs_check_4_activity(
    timestamp without time zone,
    interval)
  RETURNS void AS
$BODY$
DECLARE
  collection timestamp without time zone := $1;
  recent interval := $2;
BEGIN
UPDATE fire_collections SET active = FALSE where  age(collection, last_update) > recent;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_check_4_activity(timestamp without time zone, interval)
  OWNER TO postgres;
