CREATE OR REPLACE FUNCTION viirs_mask_sum(schema text, gt_schema text, gt_table text) 
   RETURNS void AS
$BODY$
    BEGIN
	EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(schema) || '.mask_sum' ; 

	EXECUTE 'CREATE TABLE ' || quote_ident(schema) || '.mask_sum AS ' || 
	   'SELECT a.rid, ST_MapAlgebra(a.rast, b.rast, ' || 
		 quote_literal('[rast1]+[rast2]') || ', ' || 
		 quote_literal('8BUI') || '::text, ' ||  
		 quote_literal('FIRST') || ', ' || 
		 quote_literal('[rast2]') || ',' || 
		 quote_literal('[rast1]') || ',' || 
		 quote_literal('0') || ') rast ' || 
	   'FROM ' || quote_ident(gt_schema) || '.' || quote_ident(gt_table)|| ' a, ' || 
		   quote_ident(schema)||'.fire_events_raster b ' || 
	   'WHERE a.rid = b.rid ' ;

   
    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_mask_sum(schema text, gt_schema text, gt_table text)
  OWNER to postgres ;

