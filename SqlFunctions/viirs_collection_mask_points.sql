CREATE OR REPLACE FUNCTION viirs_collection_mask_points(
    varchar(200),
    text,
    text,
    text,
    text,
    timestamp without time zone)
  RETURNS void AS
$BODY$
DECLARE
  schema varchar(200) := $1;
  point_tbl text := $2 ;
  landcover_schema text := $3;
  no_burn_table text := $4 ; 
  no_burn_geom text := $5 ; 
  collection := $6 ;
  no_burn_res real ;
  dumint int ;  

BEGIN
  -- reproject and index the points
  PERFORM viirs_collection_nlcd_geom(schema, point_tbl, dumint, collection) ;

  -- Populate the masked column
  EXECUTE 'UPDATE ' || quote_ident(schema) || '.'||quote_ident(point_tbl)|| ' a '  || 
           'SET masked=TRUE ' ||
           'FROM ' ||quote_ident(landcover_schema)||'.'||quote_ident(no_burn_table)||' nb ' || 
           'WHERE a.geom_nlcd && nb.rast AND ' ||
        'ST_DWithin(a.geom_nlcd, nb.geom, $1) AND ' ||
        'collection_date = $2' 
    USING  no_burn_res, collection ; 
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_collection_mask_points(varchar(200),text,text,text,text,timestamp without time zone)
  OWNER TO postgres;

