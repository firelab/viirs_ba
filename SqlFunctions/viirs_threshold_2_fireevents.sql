-- Function: viirs_threshold_2_fireevents(timestamp without time zone, interval, integer)

-- DROP FUNCTION viirs_threshold_2_fireevents(timestamp without time zone, interval, integer);

CREATE OR REPLACE FUNCTION viirs_threshold_2_fireevents(
    varchar(200),
    timestamp without time zone,
    interval,
    integer)
  RETURNS void AS
$BODY$
DECLARE 
  schema varchar(200) := $1 ;
  collection timestamp without time zone := $2; 
  recent interval := $3;
  distance integer := $4; 
  added record ; 
  confirm_query text ; 
  confirm_point text ; 
  insert_confirmed text ; 
  update_collection text;
BEGIN

  RAISE NOTICE 'Interval = %', recent ;
  
  -- This will return one row for each confirmed "threshold_burned" point in the 
  -- specified collection, paired with exactly one fire collection via exactly one 
  -- fire event with a source of "ActiveFire" meeting the spatiotemporal criteria. 
  confirm_query := 'SELECT t_fid, fe_fid, fc.fid as fc_fid ' || 
    'FROM ' || quote_ident(schema) || '.fire_collections fc, ' ||
               quote_ident(schema) || '.fire_events fe, ' || 
        '(SELECT t.fid as t_fid, MAX(fe.fid) AS fe_fid ' || 
         'FROM ' || quote_ident(schema) || '.fire_events fe, ' || 
             quote_ident(schema) || '.fire_collections fc, ' ||
             quote_ident(schema) || '.threshold_burned t ' ||
         'WHERE ' ||
             -- glue and seed criteria
             'fe.collection_id = fc.fid AND ' || 
             'fe.source = ' || quote_literal('ActiveFire') || ' AND ' || 
             't.collection_date = $1 AND ' || 

             -- temporal criterion
             'fc.last_update >= $1 - $2 AND ' ||
             'fc.last_update <= $1 AND ' || 

             -- spatial criterion
             'ST_DWithin(ST_Transform(t.geom, 102008), fe.geom, $3) AND ' || 
             
             -- mask out nonburnable
             '(NOT masked) ' ||

        'GROUP BY t.fid) confirmed ' ||
     'WHERE fe.fid = fe_fid AND ' ||
        'fe.collection_id = fc.fid' ;



    
  insert_confirmed := 'INSERT INTO ' || quote_ident(schema) || '.fire_events ' ||
      '(latitude, longitude, geom, source, collection_id, ' ||
       'collection_date, pixel_size, band_i_m) ' ||
      'SELECT latitude, longitude, ST_Transform(geom, 102008), ' || 
        quote_literal('Threshold') || ', ' || 
        'fc_fid, collection_date, pixel_size, band_i_m ' ||
      'FROM confirmed_pts cp, ' || 
            quote_ident(schema) || '.threshold_burned t ' ||
      'WHERE t.fid = cp.t_fid'  ;

  confirm_point := 'UPDATE ' || quote_ident(schema) || '.threshold_burned t ' || 
      'SET confirmed_burn = TRUE ' || 
      'FROM confirmed_pts cp ' || 
      'WHERE t.fid = cp.t_fid' ; 


  EXECUTE 'CREATE TEMPORARY TABLE confirmed_pts AS ' || confirm_query
      USING collection, recent, distance ; 
      
  EXECUTE 'SELECT count(*) as c FROM confirmed_pts' INTO added ; 

  RAISE NOTICE 'adding % points.', added.c ; 
  EXECUTE insert_confirmed ; 
  EXECUTE confirm_point ;     
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_threshold_2_fireevents(varchar(200), timestamp without time zone, interval, integer)
  OWNER TO postgres;
