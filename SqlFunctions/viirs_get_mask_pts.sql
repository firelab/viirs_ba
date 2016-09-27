-- given a schema/table/column containing raster data, creates a 
-- geometry multipoint column contaning pixel centers for only those 
-- points where the pixel value == 1. (mask is true) 
CREATE OR REPLACE FUNCTION viirs_get_mask_pts(schema text, tbl text, rast text, geom text, srid int, mask_val int = 1) 
   RETURNS void AS
$BODY$
  BEGIN

	EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.' ||
	                          quote_ident(tbl) || 
	                          ' DROP COLUMN IF EXISTS ' || quote_ident(geom); 

	EXECUTE 'ALTER TABLE '  || quote_ident(schema) || '.' ||
	                          quote_ident(tbl) || 
	                          ' ADD COLUMN ' || quote_ident(geom) || 
	                          ' Geometry(MultiPoint, ' || srid || ')' ;

	EXECUTE 'WITH ' || 
	  'p as (SELECT rid, ST_Multi(ST_Collect((pixel).geom)) as multi_p ' ||
		'FROM (SELECT rid, ST_PixelAsCentroids(' || quote_ident(rast) ||
		       ') as pixel ' || 
		'FROM ' || quote_ident(schema) || '.' || quote_ident(tbl) || 
		') dummy ' || 
		'WHERE (pixel).val=' || mask_val || ' ' || 
		'GROUP BY rid) ' || 
	  'UPDATE ' || quote_ident(schema) || '.' || quote_ident(tbl) || ' a ' ||
	  'SET ' || quote_ident(geom) || ' = p.multi_p ' || 
	  'FROM p ' || 
	  'WHERE p.rid = a.rid ' ;

        EXECUTE 'CREATE INDEX ' || quote_ident('idx_' || schema || '_' || tbl || '_' || geom) || ' ON ' || 
             quote_ident(schema) || '.' || quote_ident(tbl) || 
             ' USING gist (' || quote_ident(geom) || ')';

   END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_get_mask_pts(schema text, tbl text, rast text, geom text, srid int, mask_val int )
  OWNER to postgres ;
