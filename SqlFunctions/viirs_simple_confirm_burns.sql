-- Function: public.viirs_simple_confirm_burns(timestamp without time zone, interval, integer)

-- DROP FUNCTION public.viirs_simple_confirm_burns(timestamp without time zone, interval, integer);

CREATE OR REPLACE FUNCTION public.viirs_simple_confirm_burns(
    varchar(200),
    timestamp without time zone,
    interval,
    integer)
  RETURNS void AS
$BODY$
  DECLARE
    schema varchar(200) := $1;
    collection TIMESTAMP := $2;
    recent_interval INTERVAL := $3;
    distance INTEGER := $4;
BEGIN
    query := 'UPDATE threshold_burned ' ||
        'SET confirmed_burn = TRUE ' || 
        'FROM(' ||
            'SELECT a.* FROM $1.threshold_burned a ' ||
            'LEFT JOIN $1.active_fire b ' || 
            'ON ST_DWithin(ST_Transform(a.geom, 102008), ST_Transform(b.geom, 102008), $2)' ||
        'WHERE a.collection_date = $3 ' || 
            'AND b.collection_date >= $3 - $4 ' || 
            'AND b.collection_date <= $3) AS subquery ' || 
        'WHERE threshold_burned.fid = subquery.fid';
    EXECUTE query USING schema, distance, collection, recent_interval ;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION public.viirs_simple_confirm_burns(varchar(200), timestamp without time zone, interval, integer)
  OWNER TO postgres;
