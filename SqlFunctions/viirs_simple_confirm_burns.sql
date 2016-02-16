-- Function: public.viirs_simple_confirm_burns(timestamp without time zone, interval, integer)

-- DROP FUNCTION public.viirs_simple_confirm_burns(timestamp without time zone, interval, integer);

CREATE OR REPLACE FUNCTION public.viirs_simple_confirm_burns(
    timestamp without time zone,
    interval,
    integer)
  RETURNS void AS
$BODY$
  DECLARE
    collection TIMESTAMP := $1;
    recent_interval INTERVAL := $2;
    distance INTEGER := $3;
BEGIN
UPDATE threshold_burned
SET confirmed_burn = TRUE
FROM(
SELECT a.* FROM public.threshold_burned a 
LEFT JOIN public.active_fire b 
ON ST_DWithin(ST_Transform(a.geom, 102008), ST_Transform(b.geom, 102008), distance)
WHERE a.collection_date = collection 
        AND
        b.collection_date >= collection - recent_interval 
        AND
        b.collection_date <= collection) AS subquery
WHERE threshold_burned.fid = subquery.fid;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION public.viirs_simple_confirm_burns(timestamp without time zone, interval, integer)
  OWNER TO postgres;
