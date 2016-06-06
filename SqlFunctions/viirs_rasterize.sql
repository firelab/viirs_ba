-- Functions to rasterize a polygon table, given a raster table to 
-- which the result should be aligned, optionally filtering by the
-- geometry objects in a third table. This function produces a
-- new table called "schema".fire_events_raster and populates it 
-- with the result.
-- The operation assumes that the input geometry is a table containing
-- viirs fire events, which may be a mixture of 375m and 750m pixels.
-- The code in this file assumes that rasterization occurs in three phases: 
-- 1] Rasterization of the 375m pixels in a newly created table, aligned to 
--     the specified raster (assumed to be defined at 375m resolution).
-- 2] Rasterization of the 750m pixels in a new column in the above table, where
--     the same row covers the same extent but in two different resolutions.
-- 3] Merging the output of the above two operations by performing a logical OR, 
--     storing the results into a third column.
--
-- "schema"."tbl"           : the geometry table to rasterize.
-- "gt_schema"."rast_table" : the raster table to which the result should be aligned
-- "gt_schema"."geom_table" : the table containing "ground truth" by which the 
--                            input geometry is filtered.
-- distance                 : the maximum distance a candidate geometry may be from 
--                          : a geometry in "gt_schema"."geom_table". Pass -1 to turn
--                          : filtering off.
--
CREATE OR REPLACE FUNCTION viirs_rasterize_375(schema text, tbl text,
                           gt_schema text, 
                           rast_table text, 
                           geom_table text,
                           distance float) 
   RETURNS void AS
$BODY$
    DECLARE 
       dist_clause text ; 
       filter_tbl  text ;
    BEGIN

	EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(schema) || '.fire_events_raster' ;
	
	IF distance <> -1 THEN 
	  dist_clause := 'ST_DWithin(a.geom_nlcd, c.geom, $1) AND ' ||
	                 'ST_Intersects(c.geom, b.rast) AND ' ;
          filter_tbl := ', '||quote_ident(gt_schema)||'.'||quote_ident(geom_table)||' c ' ;
	ELSE
	  dist_clause := ' ' ;
	  filter_tbl := ' ' ;
	END IF ; 

        EXECUTE 'CREATE TABLE ' || quote_ident(schema) || '.fire_events_raster AS ' ||
          'SELECT b.rid, ' ||
	     'ST_MapAlgebra(' ||
	        'ST_Union(ST_AsRaster(geom_nlcd, b.rast, ' || quote_literal('8BUI') ||')), '
		'ST_AddBand(ST_MakeEmptyRaster(b.rast), ' || quote_literal('8BUI') || '::text), ' ||
		quote_literal('[rast1]') || ', ' || 
		quote_literal('8BUI') || ', ' || 
		quote_literal('SECOND') || ') rast_375 ' ||
	  'FROM ' || quote_ident(schema)||'.'||quote_ident(tbl)|| ' a, ' || 
	        quote_ident(gt_schema) || '.' || quote_ident(rast_table) || ' b ' || 
	        filter_tbl || 
	  'WHERE ST_Intersects(a.geom_nlcd, b.rast) AND ' ||
	        dist_clause ||
	        'pixel_size = 375 ' ||  
	  'GROUP BY b.rid, b.rast' USING distance;  

	EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster ' ||
	    'SET rast_375 = ST_SetBandNoDataValue(rast_375, 3.)' ;
	      
    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_rasterize_375(schema text, tbl text, gt_schema text, 
                          rast_table text, geom_table text, distance float)
  OWNER to postgres ;

CREATE OR REPLACE FUNCTION viirs_rasterize_750(schema text, tbl text,
                           gt_schema text, 
                           rast_table text, 
                           geom_table text, 
                           distance float) 
   RETURNS void AS
$BODY$
    DECLARE 
        dist_clause text ; 
        filter_tbl text ;
    BEGIN

	EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events_raster ' ||
	        'DROP COLUMN IF EXISTS rast_750'  ; 

	EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events_raster ' ||
	        'ADD COLUMN rast_750 raster'  ; 

    DISCARD TEMP ;
    CREATE TEMPORARY TABLE newrasters (rid integer, rast_750 raster) ; 
    
    IF distance <> -1 THEN 
        dist_clause := 'ST_DWithin(a.geom_nlcd, c.geom, $1) AND ' ||
                       'ST_Intersects(c.geom, b.rast) AND ' ;
        filter_tbl := quote_ident(gt_schema)||'.'||quote_ident(geom_table)||' c, ' ;
    ELSE
        dist_clause := ' ' ;
        filter_tbl := ' ' ;
    END IF ; 

    EXECUTE  'INSERT INTO newrasters ' || 
      'SELECT b.rid, ST_MapAlgebra(' ||
		    'ST_Union(ST_AsRaster(a.geom_nlcd, empty_rast_750.rast, '|| 
                         quote_literal('8BUI') || ')), empty_rast_750.rast, ' ||
		    quote_literal('[rast1]') || ', ' || 
		    quote_literal('8BUI') || ', ' || 
		    quote_literal('SECOND') || ') as rast_750 ' ||
      'FROM ' || quote_ident(schema)||'.'||quote_ident(tbl)||' a, ' || 
           quote_ident(gt_schema) || '.' || quote_ident(rast_table) || ' b, ' ||
           filter_tbl || 
           '(SELECT rid, ' || 
	           'St_SetSRID(ST_AddBand(ST_MakeEmptyRaster(ST_Width(rast)/2, ' ||
		                                  'ST_Height(rast)/2, ' ||
		                                  'ST_UpperLeftX(rast), ' ||
		                                  'ST_UpperLeftY(rast), 750), ' ||
		             quote_literal('8BUI')||'::text), ST_SRID(rast)) as rast ' ||
            'FROM ' || quote_ident(gt_schema)||'.'||quote_ident(rast_table)||') empty_rast_750 ' ||
      'WHERE ST_Intersects(a.geom_nlcd, b.rast) AND ' ||
	            dist_clause ||
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
ALTER FUNCTION viirs_rasterize_750(schema text, tbl text, 
               gt_schema text, rast_table text, geom_table text, distance float)
  OWNER to postgres ;

CREATE OR REPLACE FUNCTION viirs_rasterize_merge(schema text, col text) 
   RETURNS void AS
$BODY$
   BEGIN
   
   EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events_raster ' ||
          'DROP COLUMN IF EXISTS ' || quote_ident(col) ;

   EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events_raster ' || 
          'ADD COLUMN ' || quote_ident(col) || ' raster' ;

   EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster '||
          'SET ' || quote_ident(col) || '=rast_375 ' ||
          'WHERE rast_375 IS NOT NULL and rast_750 IS NULL'; 

   EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster ' || 
          'SET ' || quote_ident(col) || '=ST_Rescale(rast_750, 375., -375) '  ||
          'WHERE rast_375 IS NULL and rast_750 IS NOT NULL' ; 

   EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster ' || 
          'SET ' || quote_ident(col) || '=ST_SetBandNoDataValue(' ||
             'ST_MapAlgebra(rast_375, ST_Rescale(rast_750,375.,-375.), ' ||
                     quote_literal('(([rast1]=1) OR ([rast2]=1))::int') ||', '|| 
                     quote_literal('8BUI') ||','||
                     quote_literal('FIRST') || '), 3.) ' || 
           'WHERE rast_375 IS NOT NULL and rast_750 IS NOT NULL' ;     
   END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_rasterize_merge(schema text, col text)
  OWNER to postgres ;

--
-- viirs_rasterize_filter masks the merged raster. The mask to apply to
-- the raster is specified by the mask_schema and mask_tbl parameters.
-- The data to mask is specified by rast_schema and rast_col (the table 
-- name "fire_events_raster" is assumed.)
--
-- Masked data is put in the "rast" column.
-- 

CREATE OR REPLACE FUNCTION viirs_rasterize_filter(
                                rast_schema text, rast_col text,
                                mask_schema text, mask_tbl text) 
   RETURNS void AS
$BODY$
   BEGIN
   
   EXECUTE 'ALTER TABLE ' || quote_ident(rast_schema) || '.fire_events_raster ' ||
          'DROP COLUMN IF EXISTS rast'  ;

   EXECUTE 'ALTER TABLE ' || quote_ident(rast_schema) || '.fire_events_raster ' || 
          'ADD COLUMN rast raster' ;
          
   EXECUTE 'WITH mask AS (' || 
       'SELECT a.rid, ST_Union(' ||
           'ST_MapAlgebra(a.' || quote_ident(rast_col) || ',b.rast,' || 
              quote_literal('([rast1]=1 AND [rast2]=1)::int') || '),' || 
              quote_literal('MAX') || ') as rast ' || 
        'FROM ' || quote_ident(rast_schema) || 
                  '.fire_events_raster a, ' || 
                  quote_ident(mask_schema) || '.' || 
                  quote_ident(mask_tbl) || ' b ' || 
        'WHERE ST_Contains(a.' || quote_ident(rast_col)|| ',b.rast) ' || 
        'GROUP BY a.rid) ' || 
     'UPDATE ' || quote_ident(rast_schema)||'.fire_events_raster a ' ||
     'SET rast = mask.rast ' || 
     'FROM mask ' || 
     'WHERE a.rid=mask.rid' ;

   END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_rasterize_filter(schema text, col text, 
                                   gt_schema text, mask_tbl text)
OWNER to postgres ;
