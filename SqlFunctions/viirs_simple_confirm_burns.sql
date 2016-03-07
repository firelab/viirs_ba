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
    query text ; 
BEGIN
    -- subquery returns every row from threshold burned which meets the 
    -- temporal criteria, whether it meets the spatial criteria or not.
    -- every row returned by the subquery has its confirmed flag set 
    -- to true
    -- this has been identified as a bottleneck. One potential reason is 
    -- that the active fire and threshold_burned tables become very large and
    -- there is no index on the collection_date field on either one.
    query := 'UPDATE ' || quote_ident(schema) || '.threshold_burned ' ||
        'SET confirmed_burn = TRUE ' || 
        'FROM(' ||
            'SELECT a.* FROM ' || quote_ident(schema) || '.threshold_burned a ' ||
            'LEFT JOIN ' || quote_ident(schema) || '.active_fire b ' || 
            'ON ST_DWithin(ST_Transform(a.geom, 102008), ST_Transform(b.geom, 102008), $1)' ||
        'WHERE a.collection_date = $2 ' || 
            'AND b.collection_date >= $2 - $3 ' || 
            'AND b.collection_date <= $2) AS subquery ' || 
        'WHERE threshold_burned.fid = subquery.fid';
    EXECUTE query USING distance, collection, recent_interval ;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION public.viirs_simple_confirm_burns(varchar(200), timestamp without time zone, interval, integer)
  OWNER TO postgres;
