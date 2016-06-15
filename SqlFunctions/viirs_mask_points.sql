CREATE OR REPLACE FUNCTION viirs_mask_points(
    varchar(200),
    text,
    text,
    text,
    text)
  RETURNS void AS
$BODY$
DECLARE
  schema varchar(200) := $1;
  point_tbl text := $2 
  landcover_schema text := $3;
  no_burn_table text := $4 ; 
  no_burn_geom text := $5 ; 
  no_burn_res real ;
  dumint int ;  

BEGIN
  -- delete and recreate masked column
  EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.' ||
           quote_ident(point_tbl) || 
          ' DROP COLUMN IF EXISTS masked ' ;
  EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.' ||
           quote_ident(point_tbl) || 
          ' ADD COLUMN masked boolean DEFAULT FALSE' ;

  -- determine resolution of "no-burn" mask
  EXECUTE 'SELECT scale_x/2 FROM raster_columns WHERE r_table_schema = ' || 
      quote_literal(landcover_schema) || 
      ' AND r_table_name = ' || quote_literal(no_burn_table) || 
      ' AND r_raster_column = ' || quote_literal('rast') INTO no_burn_res ;

  -- determine the srid of the landcover mask for projection
  EXECUTE 'SELECT ST_SRID(rast) FROM ' || 
     quote_ident(landcover_schema)||'.'||quote_ident(no_burn_table)||' nb ' || 
     'LIMIT 1' INTO dumint ; 
        
  -- Populate the masked column
  EXECUTE 'UPDATE ' || quote_ident(schema) || '.'||quote_ident(points_tbl)|| ' a '  || 
           'SET masked=TRUE ' ||
           'FROM ' ||quote_ident(landcover_schema)||'.'||quote_ident(no_burn_table)||' nb ' || 
           'WHERE ST_Transform(a.geom, $1) && nb.rast AND ' ||
        'ST_DWithin(ST_Transform(a.geom, $1), nb.geom, $2)' 
    USING dumint, no_burn_res ; 
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_mask_points(varchar(200),text,text,text,text)
  OWNER TO postgres;

