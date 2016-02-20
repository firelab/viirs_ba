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
BEGIN
  loop_query := 'SELECT a.* FROM $1.threshold_burned a ' ||
    'WHERE collection_date = $2 ' ||
      'AND confirmed_burn = TRUE';
      
  confirm_query := 'SELECT * from (SELECT fe.fid as fe_fid,  ' ||
           'fe.geom, fc.fid as fc_fid ' || 
      'FROM $1.fire_events fe, $1.fire_collections fc ' ||
      'WHERE fe.collection_id = fc.fid ' ||
        'AND fc.last_update >= $2 - $3 ' ||
        'AND fc.last_update <= $2 ' ||
        'AND fc.active = TRUE) as tf ' ||
    'WHERE ST_DWithin(ST_Transform(a_row.geom, 102008), tf.geom, $4) LIMIT 1';
    
  insert_confirmed := 'INSERT INTO $1.fire_events ' ||
      '(latitude, longitude, geom, source, collection_id, ' ||
       'collection_date, pixel_size, band_i_m) ' ||
      'VALUES(a_row.latitude, a_row.longitude, ' ||
             'ST_Multi(ST_Transform(a_row.geom, 102008)), ' || 
             quote_literal('Threshold') ||
             ', dumrec.fc_fid, a_row.collection_date, a_row.pixel_size, a_row.band_i_m)';

  update_collection :=  'UPDATE $1.fire_collections ' ||
      'SET last_update = a_row.collection_date ' || 
      'WHERE dumrec.fc_fid = fire_collections.fid' ;

      
  FOR a_row IN EXECUTE loop_query USING schema, collection 
  LOOP
  EXECUTE confirm_query INTO dumrec USING schema, collection, recent, distance ;
  IF EXISTS (EXECUTE confirm_query USING schema, collection, recent, distance) THEN
    RAISE NOTICE 'found a match' ;
    EXECUTE insert_confirmed USING schema ; 
    EXECUTE update_collection USING schema ; 
  END IF;
  END LOOP;
return;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_threshold_2_fireevents(varchar(200), timestamp without time zone, interval, integer)
  OWNER TO postgres;
