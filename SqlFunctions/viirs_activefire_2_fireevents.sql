-- Function: viirs_activefire_2_fireevents(timestamp without time zone, interval, integer)

-- DROP FUNCTION viirs_activefire_2_fireevents(timestamp without time zone, interval, integer);

CREATE OR REPLACE FUNCTION viirs_activefire_2_fireevents(
    varchar(200),
    timestamp without time zone,
    interval,
    integer,
    text DEFAULT NULL,
    text DEFAULT NULL
    )
  RETURNS void AS
$BODY$
DECLARE
  schema varchar(200) = $1; 
  collection timestamp without time zone := $2; 
  recent interval := $3;
  distance integer := $4; 
  lm_schema text := $5 ; 
  lm_table  text:= $6 ;  
--   a_row active_fire%rowtype;
  a_row RECORD;
  ret RECORD;
  dumrec RECORD;
  dumint integer;
  query_str text;
  tempFID integer;
  select_collection text ; 
  append_point_to_collection text ; 
  update_existing_collection text  ;
  create_new_collection text ; 
  insert_first_point text  ;
  loop_query text ;
  
BEGIN
  -- selects currently active collections, to which the fire point should belong.
  select_collection := 'SELECT * from (SELECT fe.fid as fe_fid, ' || 
                       'fe.geom, fc.fid as fc_fid ' || 
                   'FROM ' || quote_ident(schema) || '.fire_events fe, ' || 
                              quote_ident(schema) || '.fire_collections fc ' || 
                   'WHERE fe.collection_id = fc.fid ' ||
                     'AND fc.last_update >= $1 - $2 ' || 
                     'AND fc.last_update <= $1) as tf ' ||  
    'WHERE ST_DWithin(ST_Transform($4, 102008), tf.geom, $3) LIMIT 1' ;
    
  append_point_to_collection := 'INSERT INTO ' || quote_ident(schema) || '.fire_events ' ||
        '(latitude, longitude, geom, source, collection_id, ' ||
        'collection_date, pixel_size, band_i_m) ' || 
        'VALUES($1, $2, ' ||
        'ST_Transform($3, 102008), ' ||
        quote_literal('ActiveFire') ||
        ' , $4, $5, $6, $7)';

  update_existing_collection := 'UPDATE ' || quote_ident(schema) || '.fire_collections ' ||
      'SET last_update = $1 ' || 
      'WHERE $2 = ' || quote_ident(schema) || '.fire_collections.fid';

  create_new_collection := 'INSERT INTO ' || quote_ident(schema) || '.fire_collections '||
     '(initial_date, last_update, active, initial_fid) ' || 
     'VALUES($1, $1, TRUE, $2) ' ||
     'RETURNING fid as collectionFid';
     
  insert_first_point := 'INSERT INTO ' || quote_ident(schema) || '.fire_events ' ||
      '(latitude, longitude, geom, source, collection_id, ' || 
      'collection_date, pixel_size, band_i_m) ' || 
      'VALUES($1, $2, ' ||
      'ST_Transform($3, 102008), ' ||
      quote_literal('ActiveFire') || 
      ', $4, $5, $6, $7)';    


      
  -- loops over all candidate active fire pixels from the specified collection.
  loop_query := 'SELECT a.* FROM ' || quote_ident(schema)||'.active_fire a ' ||
                'WHERE collection_date = $1 AND NOT masked' ; 

  -- masks active fire points in the current collection by the landmask
  IF lm_schema IS NOT NULL THEN
    PERFORM viirs_collection_mask_points(schema, 'active_fire', lm_schema, 
                             lm_table, 'geom', collection) ; 
  END IF ; 
  
  FOR a_row IN EXECUTE loop_query USING collection 
  LOOP
  EXECUTE select_collection INTO dumrec USING collection, recent, distance, a_row.geom ;
  IF dumrec IS NOT NULL THEN 
    RAISE NOTICE 'found a match' ;
    EXECUTE append_point_to_collection USING a_row.latitude, a_row.longitude, a_row.geom, dumrec.fc_fid,
        a_row.collection_date, a_row.pixel_size, a_row.band_i_m ; 
    EXECUTE update_existing_collection USING a_row.collection_date, dumrec.fc_fid ; 
  ELSE
    EXECUTE create_new_collection INTO tempFID USING a_row.collection_date, a_row.fid ; 
    EXECUTE insert_first_point USING a_row.latitude, a_row.longitude, a_row.geom, tempFID,
        a_row.collection_date, a_row.pixel_size, a_row.band_i_m ; 
  END IF;
  END LOOP;
return;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_activefire_2_fireevents(varchar(200), timestamp without time zone, interval, integer, text, text)
  OWNER TO postgres;
