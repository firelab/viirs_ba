CREATE OR REPLACE FUNCTION viirs_rasterize_375(schema text, gt_schema text, gt_table text, distance float) 
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
		quote_literal('SECOND') || ') rast_375 ' ||
	  'FROM ' || quote_ident(schema) || '.fire_events a, ' || 
	       quote_ident(gt_schema) || '.' || quote_ident(gt_table) || ' b ' || 
	  'WHERE ST_Intersects(geom_nlcd, rast) AND ' ||
	        'ST_DWithin(a.geom_nlcd, b.geom, $1) AND ' ||
	        'pixel_size = 375 ' ||  
	  'GROUP BY rid, rast' USING distance;  

	EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster ' ||
	    'SET rast_375 = ST_SetBandNoDataValue(rast_375, 3.)' ;
	      
    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_rasterize_375(schema text,gt_schema text, gt_table text, distance float)
  OWNER to postgres ;

CREATE OR REPLACE FUNCTION viirs_rasterize_750(schema text, gt_schema text, gt_table text, distance float) 
   RETURNS void AS
$BODY$
    BEGIN

	EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events_raster ' ||
	        'DROP COLUMN IF EXISTS rast_750'  ; 

	EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events_raster ' ||
	        'ADD COLUMN rast_750 raster'  ; 


    CREATE TEMPORARY TABLE newrasters (rid integer, rast_750 raster) ; 

    EXECUTE  'INSERT INTO newrasters ' || 
      'SELECT b.rid, ST_MapAlgebra(' ||
		    'ST_Union(ST_AsRaster(geom_nlcd, empty_rast_750.rast, '|| 
                         quote_literal('8BUI') || ')), empty_rast_750.rast, ' ||
		    quote_literal('[rast1]') || ', ' || 
		    quote_literal('8BUI') || ', ' || 
		    quote_literal('SECOND') || ') as rast_750 ' ||
      'FROM ' || quote_ident(schema) || '.fire_events a, ' || 
           quote_ident(gt_schema) || '.' || quote_ident(gt_table) || ' b, ' ||
           '(SELECT rid, ' || 
	           'St_SetSRID(ST_AddBand(ST_MakeEmptyRaster(ST_Width(rast)/2, ' ||
		                                  'ST_Height(rast)/2, ' ||
		                                  'ST_UpperLeftX(rast), ' ||
		                                  'ST_UpperLeftY(rast), 750), ' ||
		             quote_literal('8BUI')||'::text), ST_SRID(rast)) as rast ' ||
            'FROM ' || quote_ident(gt_schema)||'.'||quote_ident(gt_table)||') empty_rast_750 ' ||
      'WHERE ST_Intersects(geom_nlcd, b.rast) AND ' ||
	            'ST_DWithin(a.geom_nlcd, b.geom, $1) AND ' ||
	            'b.rid = empty_rast_750.rid AND ' ||
	            'pixel_size = 750  ' ||
      'GROUP BY b.rid, empty_rast_750.rast ' USING distance;
    
    EXECUTE 'LOCK TABLE ' || quote_ident(schema) || '.fire_events_raster ' ||
          'IN EXCLUSIVE MODE' ; 
    
    EXECUTE 'UPDATE  ' || quote_ident(schema) || '.fire_events_raster me '
      'SET rast_750 = newrasters.rast_750 ' ||
      'FROM newrasters ' ||
      'WHERE newrasters.rid = me.rid' ; 

    EXECUTE 'INSERT INTO ' || quote_ident(schema) || '.fire_events_raster ' ||
       '(rid, rast_750) ' || 
       'SELECT newrasters.rid, newrasters.rast_750 ' || 
       'FROM newrasters ' || 
       'LEFT OUTER JOIN ' || quote_ident(schema) || '.fire_events_raster me ' ||
          'ON (newrasters.rid = me.rid) ' || 
       'WHERE me.rid IS NULL' ;

    EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster ' ||
	    'SET rast_750 = ST_SetBandNoDataValue(rast_750, 3.) ' ||
            'WHERE rast_750 IS NOT NULL' ;

    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_rasterize_750(schema text,gt_schema text, gt_table text, distance float)
  OWNER to postgres ;
