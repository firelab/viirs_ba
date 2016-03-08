CREATE OR REPLACE FUNCTION viirs_rasterize(schema text, gt_schema text, gt_table text) 
   RETURNS void AS
$BODY$
    BEGIN

	EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(schema) || '.fire_events_raster' ; 

	EXECUTE 'CREATE TABLE ' || quote_ident(schema) || '.fire_events_raster AS ' ||
	  'SELECT rid, ' ||
	     'ST_MapAlgebra(' ||
		'ST_Union(ST_AsRaster(geom_nlcd, rast, ' || quote_literal('8BUI') ||')), '
		'ST_AddBand(ST_MakeEmptyRaster(rast), ' || quote_literal('8BUI') || '::text), ' ||
		quote_literal('[rast1]') || ', ' || 
		quote_literal('8BUI') || ', ' || 
		quote_literal('SECOND') || ') rast ' ||
	  'FROM ' || quote_ident(schema) || '.fire_events a, ' || 
	       quote_ident(gt_schema) || '.' || quote_ident(gt_table) || ' b ' || 
	  'WHERE ST_Intersects(geom_nlcd, rast) ' || 
	  'GROUP BY rid, rast' ;  

	EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster ' ||
	    'SET rast = ST_SetBandNoDataValue(rast, 3.)' ;
	      
    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_rasterize(schema text,gt_schema text, gt_table text)
  OWNER to postgres ;
