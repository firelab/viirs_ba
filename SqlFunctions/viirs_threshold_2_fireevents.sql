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
  a_row RECORD;
  dumrec RECORD;
  loop_query text ; 
  confirm_query text ; 
  confirm_point text ; 
  insert_confirmed text ; 
  update_collection text;
BEGIN
  -- Query selects all candidate burned area detections from a particular 
  -- satellite scene (collection_date).
  loop_query := 'SELECT a.* FROM ' || quote_ident(schema) || '.threshold_burned a ' ||
    'WHERE collection_date = $1 ';
  
  -- Subquery (in parens) selects all ActiveFire points from all fire groups 
  -- (collections) which meet the temporal criteria.
  -- Main query checks to see if there is at least one point from the subquery
  -- which also matches the spatial criteria.
  -- While there is no index on the last_update field, the number of entries in the
  -- fire_collections table is comparatively small.
  -- In essense, this performs the confirmation a second time. This is necessary 
  -- in order to identify a fire_collection.fid to assign the candidate point to.
  confirm_query := 'SELECT * from (SELECT fe.fid as fe_fid,  ' ||
           'fe.geom, fc.fid as fc_fid ' || 
      'FROM ' || quote_ident(schema) || '.fire_events fe, ' || 
                 quote_ident(schema) || '.fire_collections fc ' ||
      'WHERE fe.collection_id = fc.fid ' ||
        'AND fc.last_update >= $1 - $2 ' ||
        'AND fc.last_update <= $1 ' ||
        'AND fe.source = ' || quote_literal('ActiveFire') || ') as tf ' ||
    'WHERE ST_DWithin(ST_Transform($4, 102008), tf.geom, $3) LIMIT 1';
    
  insert_confirmed := 'INSERT INTO ' || quote_ident(schema) || '.fire_events ' ||
      '(latitude, longitude, geom, source, collection_id, ' ||
       'collection_date, pixel_size, band_i_m) ' ||
      'VALUES($1, $2, ' ||
             'ST_Multi(ST_Transform($3, 102008)), ' || 
             quote_literal('Threshold') ||
             ', $4, $5, $6, $7)';

  update_collection :=  'UPDATE ' || quote_ident(schema) || '.fire_collections ' ||
      'SET last_update = $1 ' || 
      'WHERE $2 = ' || quote_ident(schema) || '.fire_collections.fid' ;

  confirm_point := 'UPDATE ' || quote_ident(schema) || '.threshold_burned ' ||
        'SET confirmed_burn = TRUE ' || 
        'WHERE fid = $1';

      
  FOR a_row IN EXECUTE loop_query USING collection 
  LOOP
  EXECUTE confirm_query INTO dumrec USING collection, recent, distance, a_row.geom ;
  IF dumrec IS NOT NULL THEN
    RAISE NOTICE 'found a match' ;
    EXECUTE insert_confirmed USING a_row.latitude, a_row.longitude, a_row.geom, 
          dumrec.fc_fid, a_row.collection_date, a_row.pixel_size, a_row.band_i_m ; 
    EXECUTE update_collection USING a_row.collection_date, dumrec.fc_fid ; 
    EXECUTE confirm_point USING a_row.fid ;
  END IF;
  END LOOP;
return;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_threshold_2_fireevents(varchar(200), timestamp without time zone, interval, integer)
  OWNER TO postgres;
