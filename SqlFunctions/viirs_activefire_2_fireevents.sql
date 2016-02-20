-- Function: viirs_activefire_2_fireevents(timestamp without time zone, interval, integer)

-- DROP FUNCTION viirs_activefire_2_fireevents(timestamp without time zone, interval, integer);

CREATE OR REPLACE FUNCTION viirs_activefire_2_fireevents(
    varchar(200),
    timestamp without time zone,
    interval,
    integer)
  RETURNS void AS
$BODY$
DECLARE
  schema varchar(200) = $1; 
  collection timestamp without time zone := $2; 
  recent interval := $3;
  distance integer := $4; 
--   a_row active_fire%rowtype;
  a_row RECORD;
  ret RECORD;
  dumrec RECORD;
  dumint integer;
  query_str text;
  tempFID integer;
BEGIN
  select_collection := 'SELECT * from (SELECT fe.fid as fe_fid, ' || 
                       'fe.geom, fc.fid as fc_fid ' || 
                   'FROM $1.fire_events fe, $1.fire_collections fc ' || 
                   'WHERE fe.collection_id = fc.fid ' ||
                     'AND fc.last_update >= $2 - $3 ' || 
                     'AND fc.last_update <= $2 ' || 
                     'AND fc.active = TRUE) as tf ' ||  
    'WHERE ST_DWithin(ST_Transform(a_row.geom, 102008), tf.geom, $4) LIMIT 1' ;
    
  append_point_to_collection := 'INSERT INTO $1.fire_events ' ||
        '(latitude, longitude, geom, source, collection_id, ' ||
        'collection_date, pixel_size, band_i_m) ' || 
        'VALUES(a_row.latitude, a_row.longitude, ' ||
        'ST_Multi(ST_Transform(a_row.geom, 102008)), ' ||
        quote_literal('ActiveFire') ||
        ' , dumrec.fc_fid, a_row.collection_date, a_row.pixel_size, a_row.band_i_m)';

  update_existing_collection := 'UPDATE $1.fire_collections ' ||
      'SET last_update = a_row.collection_date ' || 
      'WHERE dumrec.fc_fid = fire_collections.fid';

  create_new_collection := 'INSERT INTO $1.fire_collections '||
     '(initial_date, last_update, active, initial_fid) ' || 
     'VALUES(a_row.collection_date, a_row.collection_date, TRUE, a_row.fid) ' ||
     'RETURNING fid as collectionFid';
     
  insert_first_point := 'INSERT INTO $1.fire_events ' ||
      '(latitude, longitude, geom, source, collection_id, ' || 
      'collection_date, pixel_size, band_i_m) ' || 
      'VALUES(a_row.latitude, a_row.longitude, ' ||
      'ST_Multi(ST_Transform(a_row.geom, 102008)), ' ||
      quote_literal('ActiveFire') || 
      ', tempFID, a_row.collection_date, a_row.pixel_size, a_row.band_i_m)';    



  FOR a_row IN SELECT a.* FROM active_fire a  
    WHERE collection_date = collection
  LOOP
  EXECUTE select_collection INTO dumrec USING schema, collection, recent, distance ;
  IF EXISTS (EXECUTE select_collection USING schema, collection, recent, distance) THEN 
    RAISE NOTICE 'found a match' ;
    EXECUTE append_point_to_collection USING schema ; 
    EXECUTE update_existing_collection USING schema ; 
  ELSE
    EXECUTE create_new_collection INTO tempFID USING schema ; 
    EXECUTE insert_first_point USING schema; 
  END IF;
  END LOOP;
return;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_activefire_2_fireevents(varchar(200), timestamp without time zone, interval, integer)
  OWNER TO postgres;
