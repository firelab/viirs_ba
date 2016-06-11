-- Function: viirs_activefire_2_fireevents(timestamp without time zone, interval, integer)

-- DROP FUNCTION viirs_activefire_2_fireevents(timestamp without time zone, interval, integer);

CREATE OR REPLACE FUNCTION viirs_activefire_2_fireevents(
    varchar(200),
    timestamp without time zone,
    interval,
    integer,
    text,
    text,
    text)
  RETURNS void AS
$BODY$
DECLARE
  schema varchar(200) = $1; 
  collection timestamp without time zone := $2; 
  recent interval := $3;
  distance integer := $4; 
  landcover_schema text := $5;
  no_burn_table text := $6 ; 
  no_burn_geom text := $7 ; 
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
  no_burn_res real ; 
  
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

  -- determine resolution of "no-burn" mask
  EXECUTE 'SELECT scale_x/2 FROM raster_columns WHERE r_table_schema = ' || 
      quote_literal(landcover_schema) || 
      ' AND r_table_name = ' || quote_literal(no_burn_table) || 
      ' AND r_raster_column = ' || quote_literal('rast') INTO no_burn_res ;


  -- apply the "no-burn" mask here
  -- select out the points to work with and compare against mask
  EXECUTE 'CREATE TEMPORARY TABLE current_active_fire AS (' ||
      'SELECT * FROM ' || quote_ident(schema) || '.active_fire ' || 
      'WHERE collection_date = $1)' USING collection ;
      
  ALTER TABLE current_active_fire ADD COLUMN masked boolean DEFAULT False ; 
  EXECUTE 'SELECT ST_SRID(rast) FROM ' || 
     quote_ident(landcover_schema)||'.'||quote_ident(no_burn_table)||' nb ' || 
     'LIMIT 1' INTO dumint ; 
  
  EXECUTE 'UPDATE current_active_fire ' ||
      'SET masked = TRUE ' ||
      'FROM '||quote_ident(landcover_schema)||'.'||quote_ident(no_burn_table)||' nb ' ||
      'WHERE ST_Transform(current_active_fire.geom, $1) && nb.rast AND ' ||
        'ST_DWithin(ST_Transform(current_active_fire.geom, $1), nb.geom, $2)'
    USING dumint, no_burn_res ; 
      
  -- loops over all candidate active fire pixels from the specified collection.
  loop_query := 'SELECT a.* FROM current_active_fire a WHERE NOT masked' ; 

  FOR a_row IN EXECUTE loop_query 
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
ALTER FUNCTION viirs_activefire_2_fireevents(varchar(200), timestamp without time zone, interval, integer)
  OWNER TO postgres;
